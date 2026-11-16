// Package job 提供 recon 进程内基于注入时钟的对账调度器。
package job

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/recon/biz"
)

var (
	shanghai     = time.FixedZone("Asia/Shanghai", 8*3600)
	errNilRunner = errors.New("recon job: nil runner")
	defaultTick  = time.Second
)

// Runner 抽象每日定时与单批次重试的执行目标。
type Runner interface {
	RunDaily(ctx context.Context) error
	RunBatch(ctx context.Context, in biz.RunBatchInput) (*biz.RunBatchOutput, error)
}

type scheduledRetry struct {
	channel  string
	billDate string
	fireAt   time.Time
}

// Scheduler 是注入时钟的对账调度器，实现 server.Group 与 biz.RetryScheduler。
type Scheduler struct {
	runner Runner
	clk    clock.Clock
	hour   int
	minute int
	tick   time.Duration

	mu          sync.Mutex
	nextDailyAt time.Time
	retries     []scheduledRetry
	started     bool
	stopped     bool

	ctx    context.Context
	cancel context.CancelFunc
	quit   chan struct{}
	done   chan struct{}
	once   sync.Once

	panics atomic.Uint64
}

// NewScheduler 创建调度器。runAt 格式为 "HH:MM"，默认 "10:00"。
// tick 为 nil 或 <=0 时默认 1s（只用于 Serve 循环检查注入时钟）。
func NewScheduler(runner Runner, clk clock.Clock, runAt string, tick *time.Duration) *Scheduler {
	if clk == nil {
		clk = clock.Real{}
	}
	h, m := parseRunAt(runAt)
	interval := defaultTick
	if tick != nil && *tick > 0 {
		interval = *tick
	}
	now := clk.Now().In(shanghai)
	todayTarget := time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, shanghai)
	if now.After(todayTarget) {
		todayTarget = todayTarget.AddDate(0, 0, 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		runner:      runner,
		clk:         clk,
		hour:        h,
		minute:      m,
		tick:        interval,
		nextDailyAt: todayTarget,
		ctx:         ctx,
		cancel:      cancel,
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// ScheduleRetry 注册一次单批次延迟重试（用于 BILL_NOT_READY）。
func (s *Scheduler) ScheduleRetry(channel, billDate string, delay time.Duration) {
	fireAt := s.clk.Now().In(shanghai).Add(delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.retries {
		if s.retries[i].channel == channel && s.retries[i].billDate == billDate {
			s.retries[i].fireAt = fireAt
			return
		}
	}
	s.retries = append(s.retries, scheduledRetry{
		channel:  channel,
		billDate: billDate,
		fireAt:   fireAt,
	})
}

// Step 检查当前注入时钟是否到达每日定时或单批次重试时间，若到达则同步执行。
func (s *Scheduler) Step(ctx context.Context) error {
	if s.runner == nil {
		return errNilRunner
	}
	now := s.clk.Now().In(shanghai)

	var runDaily bool
	var dueRetries []scheduledRetry

	s.mu.Lock()
	if !now.Before(s.nextDailyAt) {
		runDaily = true
		next := s.nextDailyAt.AddDate(0, 0, 1)
		for !next.After(now) {
			next = next.AddDate(0, 0, 1)
		}
		s.nextDailyAt = next
	}
	if len(s.retries) > 0 {
		kept := s.retries[:0]
		for _, r := range s.retries {
			if !now.Before(r.fireAt) {
				dueRetries = append(dueRetries, r)
			} else {
				kept = append(kept, r)
			}
		}
		s.retries = kept
	}
	s.mu.Unlock()

	var firstErr error
	if runDaily {
		if err := s.runner.RunDaily(ctx); err != nil {
			firstErr = err
		}
	}
	for _, r := range dueRetries {
		if _, err := s.runner.RunBatch(ctx, biz.RunBatchInput{
			Channel:  r.channel,
			BillDate: r.billDate,
			Force:    false,
		}); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Serve 启动定时检查循环，直到 Stop。
func (s *Scheduler) Serve() error {
	if s.runner == nil {
		return errNilRunner
	}
	s.mu.Lock()
	if s.started || s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()
	defer close(s.done)

	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	for {
		select {
		case <-s.quit:
			return nil
		case <-ticker.C:
		}
		select {
		case <-s.quit:
			return nil
		default:
		}
		s.stepSafe()
	}
}

// Stop 停止调度器，等待当前在途步骤结束。
func (s *Scheduler) Stop(ctx context.Context) error {
	s.once.Do(func() { close(s.quit) })
	s.cancel()
	s.mu.Lock()
	s.stopped = true
	started := s.started
	s.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Panics 返回调度步恢复 panic 的次数。
func (s *Scheduler) Panics() uint64 {
	return s.panics.Load()
}

func (s *Scheduler) stepSafe() {
	defer func() {
		if r := recover(); r != nil {
			s.panics.Add(1)
			slog.Error("recon scheduler panicked", "recover", r)
		}
	}()
	if err := s.Step(s.ctx); err != nil {
		slog.Error("recon scheduler step failed", "error", err)
	}
}

func parseRunAt(s string) (int, int) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 {
		return 10, 0
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 10, 0
	}
	return h, m
}
