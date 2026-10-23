package biz

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
)

// TimerScheduler 是进程内调度器：每个任务一个 time.AfterFunc，等待时长按注入时钟计算。
// 任务不落盘，进程退出即丢失；重启后由 Service.Recover 从 mock_txn 重建。
// 任务 panic 会被恢复并计数，不影响其他任务。并发安全。
type TimerScheduler struct {
	clk    clock.Clock
	mu     sync.Mutex
	stop   bool
	nextID uint64
	timers map[uint64]*time.Timer
	// running 计数已登记但还没结束的任务（含等待中和执行中）。
	running sync.WaitGroup
	panics  atomic.Int64
}

// NewTimerScheduler 创建调度器。退出前调用 Stop。
func NewTimerScheduler(clk clock.Clock) *TimerScheduler {
	return &TimerScheduler{clk: clk, timers: map[uint64]*time.Timer{}}
}

// Schedule 在 due 执行 task；due 不晚于当前时间时立即执行。Stop 之后调用被忽略。
func (s *TimerScheduler) Schedule(due time.Time, task func()) {
	if task == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop {
		return
	}
	id := s.nextID
	s.nextID++
	s.running.Add(1)
	s.timers[id] = time.AfterFunc(due.Sub(s.clk.Now()), func() {
		// 谁把 id 从表里删掉，谁负责 Done：这里或 Stop，只会有一方。
		s.mu.Lock()
		if _, ok := s.timers[id]; !ok {
			s.mu.Unlock()
			return
		}
		delete(s.timers, id)
		s.mu.Unlock()
		defer s.running.Done()
		s.run(task)
	})
}

func (s *TimerScheduler) run(task func()) {
	defer func() {
		if p := recover(); p != nil {
			s.panics.Add(1)
			slog.Error("scheduled task panicked", slog.String("panic", fmt.Sprint(p)))
		}
	}()
	task()
}

// Stop 取消所有未开始的任务，并等正在执行的任务结束。可以重复调用。
func (s *TimerScheduler) Stop() {
	s.mu.Lock()
	s.stop = true
	for id, t := range s.timers {
		t.Stop()
		delete(s.timers, id)
		s.running.Done()
	}
	s.mu.Unlock()
	s.running.Wait()
}

// Pending 返回还没开始执行的任务数。
func (s *TimerScheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.timers)
}

// Panics 返回任务 panic 的次数。
func (s *TimerScheduler) Panics() int64 {
	return s.panics.Load()
}
