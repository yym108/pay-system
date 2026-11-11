package job

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
)

const testInterval = 5 * time.Millisecond

// pollFor 用真实时钟每 2 毫秒检查一次 ready，within 内成立返回 true。
func pollFor(within time.Duration, ready func() bool) bool {
	clk := clock.Real{}
	deadline := clk.Now().Add(within)
	for !clk.Now().After(deadline) {
		if ready() {
			return true
		}
		timer := time.NewTimer(2 * time.Millisecond)
		<-timer.C
	}
	return ready()
}

func serveAsync(l *Loop) <-chan error {
	out := make(chan error, 1)
	go func() { out <- l.Serve() }()
	return out
}

func waitServe(t *testing.T, served <-chan error) error {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-served:
		return err
	case <-timer.C:
		t.Fatal("Serve did not return")
		return nil
	}
}

func stopWithin(t *testing.T, l *Loop, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return l.Stop(ctx)
}

// PAY-05 规则 1：按间隔反复调用批次，Stop 之后 Serve 返回 nil，不再调用。
func TestLoopRunsBatchUntilStop(t *testing.T) {
	var calls atomic.Int32
	l := NewLoop("t", testInterval, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	served := serveAsync(l)
	if !pollFor(5*time.Second, func() bool { return calls.Load() >= 3 }) {
		t.Fatalf("calls = %d", calls.Load())
	}
	if err := stopWithin(t, l, 2*time.Second); err != nil {
		t.Fatalf("stop = %v", err)
	}
	if err := waitServe(t, served); err != nil {
		t.Fatalf("serve = %v", err)
	}
	after := calls.Load()
	if err := l.Serve(); err != nil {
		t.Fatalf("serve after stop = %v", err)
	}
	if calls.Load() != after {
		t.Fatalf("ran after stop: %d -> %d", after, calls.Load())
	}
}

// NFR-REL-05：批次 panic 被恢复并计数，循环继续运行。
func TestLoopRecoversPanic(t *testing.T) {
	var calls atomic.Int32
	l := NewLoop("t", testInterval, func(context.Context) error {
		if calls.Add(1) == 1 {
			panic("batch boom")
		}
		return nil
	})
	served := serveAsync(l)
	if !pollFor(5*time.Second, func() bool { return calls.Load() >= 2 }) {
		t.Fatalf("calls = %d", calls.Load())
	}
	if err := stopWithin(t, l, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := waitServe(t, served); err != nil {
		t.Fatal(err)
	}
	if l.Panics() != 1 {
		t.Fatalf("panics = %d", l.Panics())
	}
}

// 批次返回错误只记日志，下一次照常执行。
func TestLoopBatchErrorKeepsRunning(t *testing.T) {
	var calls atomic.Int32
	l := NewLoop("t", testInterval, func(context.Context) error {
		calls.Add(1)
		return errors.New("db down")
	})
	served := serveAsync(l)
	if !pollFor(5*time.Second, func() bool { return calls.Load() >= 3 }) {
		t.Fatalf("calls = %d", calls.Load())
	}
	if err := stopWithin(t, l, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := waitServe(t, served); err != nil {
		t.Fatal(err)
	}
}

// 优雅退出：Stop 立即取消批次 ctx（批次据此不再启动新任务），但仍等在途批次结束才返回 nil。
func TestLoopStopWaitsForBatch(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	l := NewLoop("t", testInterval, func(ctx context.Context) error {
		calls.Add(1)
		once.Do(func() { close(entered) })
		// 模拟已开始、自带超时的任务：看到取消后仍要等它们结束。
		<-ctx.Done()
		close(canceled)
		<-gate
		return nil
	})
	served := serveAsync(l)
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- stopWithin(t, l, 5*time.Second) }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-canceled:
	case <-timer.C:
		t.Fatal("batch ctx not canceled by Stop")
	}
	if pollFor(30*time.Millisecond, func() bool { return len(stopped) > 0 }) {
		t.Fatal("Stop returned while batch running")
	}
	close(gate)
	if err := <-stopped; err != nil {
		t.Fatalf("stop = %v", err)
	}
	if err := waitServe(t, served); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

// 在途任务比 Stop 的期限还长：Stop 到期返回 ctx.Err()，批次结束后 Serve 返回 nil。
func TestLoopStopDeadlineCancelsBatch(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	l := NewLoop("t", testInterval, func(ctx context.Context) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		<-gate
		return ctx.Err()
	})
	served := serveAsync(l)
	<-entered
	if err := stopWithin(t, l, 20*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		close(gate)
		t.Fatalf("stop = %v", err)
	}
	close(gate)
	if err := waitServe(t, served); err != nil {
		t.Fatalf("serve = %v", err)
	}
}

// 服务器可能在 Serve 启动前调用 Stop：立即返回 nil，之后的 Serve 也直接返回 nil。
func TestLoopStopBeforeServe(t *testing.T) {
	var calls atomic.Int32
	l := NewLoop("t", testInterval, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if err := stopWithin(t, l, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := waitServe(t, serveAsync(l)); err != nil {
		t.Fatal(err)
	}
	if err := stopWithin(t, l, time.Second); err != nil {
		t.Fatalf("second stop = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

// 没有批次函数是装配错误，Serve 返回错误；间隔非正时用 1 秒。
func TestLoopMisconfigured(t *testing.T) {
	if err := NewLoop("t", testInterval, nil).Serve(); err == nil {
		t.Fatal("nil batch served")
	}
	if got := NewLoop("t", 0, func(context.Context) error { return nil }).interval; got != time.Second {
		t.Fatalf("interval = %s", got)
	}
}

// PAY-05 规则 1：查单补偿每秒执行一次。
func TestNewQueryCompensate(t *testing.T) {
	l := NewQueryCompensate(func(context.Context) error { return nil })
	if l.interval != QueryCompensateInterval || QueryCompensateInterval != time.Second || l.name != "query_compensate" {
		t.Fatalf("loop = %s %s", l.name, l.interval)
	}
}

// REF-04 规则 1：退款卡单恢复每 10 秒执行一次，日志字段 job 为 refund_recover。
func TestNewRefundRecover(t *testing.T) {
	l := NewRefundRecover(func(context.Context) error { return nil })
	if l.interval != RefundRecoverInterval || RefundRecoverInterval != 10*time.Second || l.name != "refund_recover" {
		t.Fatalf("loop = %s %s", l.name, l.interval)
	}
}

// MQ-02：投递每 200ms，积压指标每 10 秒。account 不注册这两个循环。
func TestNewOutboxLoops(t *testing.T) {
	deliver := NewOutboxDeliver(func(context.Context) error { return nil })
	if deliver.name != "outbox_deliver" || deliver.interval != OutboxDeliverInterval || OutboxDeliverInterval != 200*time.Millisecond {
		t.Fatalf("deliver = %s %s", deliver.name, deliver.interval)
	}
	gauge := NewOutboxGauge(func(context.Context) error { return nil })
	if gauge.name != "outbox_gauge" || gauge.interval != OutboxGaugeInterval || OutboxGaugeInterval != 10*time.Second {
		t.Fatalf("gauge = %s %s", gauge.name, gauge.interval)
	}
}

// NTF-02 规则 1：通知发送每秒扫描一次。重试间隔不在这个循环上。
func TestNewNotifySend(t *testing.T) {
	l := NewNotifySend(func(context.Context) error { return nil })
	if l.name != "notify_send" || l.interval != NotifySendInterval || NotifySendInterval != time.Second {
		t.Fatalf("loop = %s %s", l.name, l.interval)
	}
}

// TRF-04：循环名 transfer_recover，间隔用传入的配置；小于等于 0 时 NewLoop 改成 1 秒。
func TestNewTransferRecover(t *testing.T) {
	l := NewTransferRecover(2*time.Second, func(context.Context) error { return nil })
	if l.name != "transfer_recover" || l.interval != 2*time.Second {
		t.Fatalf("loop = %s %s", l.name, l.interval)
	}
	if got := NewTransferRecover(0, func(context.Context) error { return nil }).interval; got != time.Second {
		t.Fatalf("non-positive interval = %s", got)
	}
}
