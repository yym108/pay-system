package biz

import (
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
)

const waitLimit = 2 * time.Second

func newTimer(t *testing.T) (*TimerScheduler, *clock.Fake) {
	t.Helper()
	clk := &clock.Fake{}
	clk.Set(baseTime)
	s := NewTimerScheduler(clk)
	t.Cleanup(s.Stop)
	return s, clk
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(waitLimit):
		t.Fatalf("%s did not happen within %v", what, waitLimit)
	}
}

// 已过期的 due 立即执行；未到期的按 due 与注入时钟之差等待。
func TestTimerSchedulerRunsTasks(t *testing.T) {
	s, clk := newTimer(t)
	overdue := make(chan struct{})
	soon := make(chan struct{})
	s.Schedule(clk.Now().Add(-time.Hour), func() { close(overdue) })
	s.Schedule(clk.Now().Add(20*time.Millisecond), func() { close(soon) })
	waitClosed(t, overdue, "overdue task")
	waitClosed(t, soon, "due task")
	timeout := time.After(waitLimit)
	for s.Pending() != 0 {
		select {
		case <-timeout:
			t.Fatalf("pending = %d", s.Pending())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Stop 取消未到期的任务；Stop 之后的 Schedule 被忽略；重复 Stop 安全。
func TestTimerSchedulerStopCancelsPending(t *testing.T) {
	s, clk := newTimer(t)
	ran := make(chan struct{}, 2)
	s.Schedule(clk.Now().Add(time.Hour), func() { ran <- struct{}{} })
	if s.Pending() != 1 {
		t.Fatalf("pending = %d", s.Pending())
	}
	s.Stop()
	if s.Pending() != 0 {
		t.Fatalf("pending after stop = %d", s.Pending())
	}
	// Stop 之后的 Schedule 在加锁判断 stop 后直接返回，不创建定时器，所以无需等待即可断言。
	s.Schedule(clk.Now().Add(-time.Hour), func() { ran <- struct{}{} })
	if s.Pending() != 0 {
		t.Fatalf("pending after schedule on stopped = %d", s.Pending())
	}
	s.Stop()
	if len(ran) != 0 || s.Pending() != 0 {
		t.Fatal("task ran after stop")
	}
}

// Stop 等正在执行的任务结束后才返回，退出时不丢正在写库的完成任务。
func TestTimerSchedulerStopWaitsForRunningTask(t *testing.T) {
	s, clk := newTimer(t)
	started := make(chan struct{})
	release := make(chan struct{})
	s.Schedule(clk.Now(), func() {
		close(started)
		<-release
	})
	waitClosed(t, started, "task start")
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while task was running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	waitClosed(t, stopped, "Stop")
}

// 任务 panic 被恢复并计数，不影响后续任务。
func TestTimerSchedulerRecoversPanic(t *testing.T) {
	s, clk := newTimer(t)
	s.Schedule(clk.Now(), func() { panic("boom") })
	after := make(chan struct{})
	s.Schedule(clk.Now().Add(20*time.Millisecond), func() { close(after) })
	waitClosed(t, after, "task after panic")
	if s.Panics() != 1 {
		t.Fatalf("panics = %d", s.Panics())
	}
}
