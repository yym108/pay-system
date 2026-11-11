// Package job 放 payment 进程内的定时任务。
//
// 本包不 import 任何 service/ 包：批次函数由 cmd/payment 装配时传入（如 (*biz.Payments).RunQueryBatch）。
package job

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/obs"
)

var errNilBatch = errors.New("job: nil batch")

// Loop 每隔 interval 调用一次 batch，同一时刻最多一个批次在跑。
// 实现 server.Group：Serve 阻塞到 Stop；Stop 等在途批次结束。
type Loop struct {
	name     string
	interval time.Duration
	batch    func(context.Context) error

	// ctx 是批次的上下文。Stop 一开始就取消，批次据此不再启动新工作。
	ctx    context.Context
	cancel context.CancelFunc

	quit     chan struct{}
	quitOnce sync.Once
	done     chan struct{}

	mu      sync.Mutex
	started bool
	stopped bool

	panics atomic.Uint64
}

// NewLoop 组装一个定时任务。interval 小于等于 0 时用 1 秒。batch 为 nil 时 Serve 返回错误。
func NewLoop(name string, interval time.Duration, batch func(context.Context) error) *Loop {
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Loop{
		name:     name,
		interval: interval,
		batch:    batch,
		ctx:      ctx,
		cancel:   cancel,
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Serve 每个间隔执行一次批次，直到 Stop。正常停止返回 nil。
// Stop 先于 Serve 调用时直接返回 nil；同一个 Loop 只运行一次，再次调用也返回 nil。
func (l *Loop) Serve() error {
	if l.batch == nil {
		return errNilBatch
	}
	l.mu.Lock()
	if l.started || l.stopped {
		l.mu.Unlock()
		return nil
	}
	l.started = true
	l.mu.Unlock()
	defer close(l.done)

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.quit:
			return nil
		case <-ticker.C:
		}
		// 两个 case 同时就绪时 select 随机选，再看一次 quit，停止后不再开新批次。
		select {
		case <-l.quit:
			return nil
		default:
		}
		l.runOnce()
	}
}

// Stop 不再开始新批次，并立即取消批次的 ctx：在途批次据此不再启动新任务，
// 已经开始的单个任务自带超时，不随之取消。之后等在途批次结束返回 nil；ctx 先到期时返回 ctx.Err()。
func (l *Loop) Stop(ctx context.Context) error {
	l.quitOnce.Do(func() { close(l.quit) })
	l.cancel()
	l.mu.Lock()
	l.stopped = true
	started := l.started
	l.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Panics 返回批次 panic 被恢复的次数（进程内计数，由 OPS-03 暴露）。
func (l *Loop) Panics() uint64 {
	return l.panics.Load()
}

func (l *Loop) runOnce() {
	defer func() {
		if r := recover(); r != nil {
			l.panics.Add(1)
			// 不记录 panic 值，它可能带业务数据。
			slog.ErrorContext(l.ctx, "job batch panicked", "job", l.name)
		}
	}()
	batchCtx, endBatch := obs.StartBatch(l.ctx, l.name)
	defer endBatch()
	if err := l.batch(batchCtx); err != nil {
		slog.WarnContext(l.ctx, "job batch failed",
			"job", l.name,
			"err_code", strconv.Itoa(int(errno.From(err).Code)),
		)
	}
}
