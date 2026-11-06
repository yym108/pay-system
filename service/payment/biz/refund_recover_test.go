package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

const recoverRefundNo = "RF0000000000000000401"

// recoverFreezer 记录冻结调用。gate 不为 nil 时每次调用等它关闭，用来观察并发度。
type recoverFreezer struct {
	mu     sync.Mutex
	status string
	err    error
	panics bool
	gate   chan struct{}
	calls  []string
	active int
	peak   int
	ctxErr error
	hasDL  bool
}

func (f *recoverFreezer) FreezeRefund(ctx context.Context, _, refundNo string, _ money.Amount) (RefundFreeze, error) {
	f.mu.Lock()
	f.calls = append(f.calls, refundNo)
	f.active++
	if f.active > f.peak {
		f.peak = f.active
	}
	f.ctxErr = ctx.Err()
	_, f.hasDL = ctx.Deadline()
	status, err, gate, boom := f.status, f.err, f.gate, f.panics
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	if boom {
		panic("freeze boom")
	}
	if gate != nil {
		<-gate
	}
	if err != nil {
		return RefundFreeze{}, err
	}
	return RefundFreeze{Status: status}, nil
}

func (f *recoverFreezer) snapshot() (calls []string, active, peak int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...), f.active, f.peak
}

type recoverUnit struct {
	p        *Payments
	orders   *memOrders
	clk      *clock.Fake
	freezer  *recoverFreezer
	channel  *fakeRefunder
	rapplier *refundRecordingApplier
}

// newRecoverUnit 在内存存储里放一笔 INIT 退款，创建和更新时间都是当前时钟。
func newRecoverUnit(t *testing.T) recoverUnit {
	t.Helper()
	orders := newRefundOrders()
	p := newRefundPayments(orders, nil)
	clk, ok := p.clk.(*clock.Fake)
	if !ok {
		t.Fatal("clock is not fake")
	}
	u := recoverUnit{
		p: p, orders: orders, clk: clk,
		freezer:  &recoverFreezer{status: freezeStatusFrozen},
		channel:  &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_R04"}},
		rapplier: &refundRecordingApplier{},
	}
	p.UseRefund(u.freezer, u.channel, u.rapplier, testChannelNotifyURL)
	orders.refunds = append(orders.refunds, u.row())
	return u
}

func (u recoverUnit) row() RefundOrder {
	now := u.clk.Now().Truncate(time.Millisecond)
	return RefundOrder{
		RefundNo: recoverRefundNo, MerchantID: "M100002", OutRefundNo: "R04", PayNo: "PA_REF_1",
		Amount: 1000, Currency: "CNY", Status: refundStatusInit, Channel: channelMockpay,
		CreatedAt: now, UpdatedAt: now,
	}
}

func (u recoverUnit) stored(t *testing.T) RefundOrder {
	t.Helper()
	rows := copiedRefunds(u.orders)
	if len(rows) != 1 {
		t.Fatalf("refunds = %+v", rows)
	}
	return rows[0]
}

// REF-04 规则 1：从 REF-02 第 1 步重新执行。冻结成功后 CAS 为 PROCESSING、写 channel_txn、提交渠道一次。
// 同一行再执行一次（例如另一个实例拿到了同一行）：冻结幂等，CAS 未命中，不再提交渠道。
func TestRecoverRefundResubmitsFromFreeze(t *testing.T) {
	u := newRecoverUnit(t)
	row := u.row()
	u.clk.Advance(refundRecoverIdle)
	u.p.recoverRefund(context.Background(), row)
	if got := u.stored(t); got.Status != refundStatusProcessing || got.Version != 1 {
		t.Fatalf("refund = %+v", got)
	}
	req := u.channel.got()
	if u.channel.callCount() != 1 || req.ReqNo != recoverRefundNo || req.OrigReqNo != "PA_REF_1" || req.Amount != 1000 {
		t.Fatalf("channel calls=%d req=%+v", u.channel.callCount(), req)
	}
	txns := copiedTxns(u.orders)
	if len(txns) != 1 || txns[0].BizNo != recoverRefundNo || txns[0].ChannelTxnNo != "CH_R04" {
		t.Fatalf("txns = %+v", txns)
	}

	u.p.recoverRefund(context.Background(), row)
	calls, _, _ := u.freezer.snapshot()
	if len(calls) != 2 || u.channel.callCount() != 1 || len(copiedTxns(u.orders)) != 1 || u.rapplier.calls != 0 {
		t.Fatalf("again freeze=%d channel=%d txns=%d apply=%d", len(calls), u.channel.callCount(), len(copiedTxns(u.orders)), u.rapplier.calls)
	}
}

// REF-04 规则 2 的边界：创建后恰好 1 小时仍从冻结重新执行；超过 1 小时直接按 REF-03 第 2 条置 FAILED
// （REFUND_TIMEOUT，source = JOB），不再冻结、不调用渠道。
func TestRecoverRefundTimeoutBoundary(t *testing.T) {
	cases := []struct {
		name    string
		age     time.Duration
		timeout bool
	}{
		{"exactly 1h", refundRecoverGiveUpAfter, false},
		{"1h + 1ms", refundRecoverGiveUpAfter + time.Millisecond, true},
		{"2h", 2 * time.Hour, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := newRecoverUnit(t)
			u.freezer.err = errno.ErrUnavailable
			row := u.row()
			u.clk.Advance(c.age)
			u.p.recoverRefund(context.Background(), row)
			calls, _, _ := u.freezer.snapshot()
			if c.timeout {
				in := u.rapplier.got()
				if len(calls) != 0 || u.rapplier.calls != 1 || in.RefundNo != recoverRefundNo || in.Result != payResultFailed ||
					in.FailCode != failCodeRefundTimeout || in.Source != paySourceJob || in.FailMsg != "" {
					t.Fatalf("freeze=%d apply=%d in=%+v", len(calls), u.rapplier.calls, in)
				}
			} else if len(calls) != 1 || u.rapplier.calls != 0 {
				t.Fatalf("freeze=%d apply=%d", len(calls), u.rapplier.calls)
			}
			if u.channel.callCount() != 0 || len(copiedTxns(u.orders)) != 0 {
				t.Fatalf("channel=%d txns=%d", u.channel.callCount(), len(copiedTxns(u.orders)))
			}
		})
	}
}

// REF-04 规则 1：冻结仍然超时或不可用时保持 INIT，不写 channel_txn，不调用渠道，也不置失败。
// 冻结得到 40002 时与 REF-02 相同，交给 ApplyRefundResult 置 FAILED（INSUFFICIENT_BALANCE）。
func TestRecoverRefundFreezeOutcomes(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		failCode string
	}{
		{"unavailable", errno.ErrUnavailable, ""},
		{"account missing", errno.ErrAccountNotFound, ""},
		{"insufficient", errno.ErrInsufficientBalance, failCodeInsufficientBalance},
		{"freeze conflict", errno.ErrFreezeStatusConflict, failCodeFreezeConflict},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := newRecoverUnit(t)
			u.freezer.err = c.err
			u.clk.Advance(refundRecoverIdle)
			u.p.recoverRefund(context.Background(), u.row())
			if u.channel.callCount() != 0 || len(copiedTxns(u.orders)) != 0 {
				t.Fatalf("channel=%d txns=%d", u.channel.callCount(), len(copiedTxns(u.orders)))
			}
			if got := u.stored(t); got.Status != refundStatusInit {
				t.Fatalf("refund = %+v", got)
			}
			if c.failCode == "" {
				if u.rapplier.calls != 0 {
					t.Fatalf("apply = %d", u.rapplier.calls)
				}
				return
			}
			if in := u.rapplier.got(); u.rapplier.calls != 1 || in.FailCode != c.failCode || in.Result != payResultFailed {
				t.Fatalf("apply=%d in=%+v", u.rapplier.calls, in)
			}
		})
	}
}

// 超时置失败返回错误（例如与推进为 PROCESSING 并发时 INIT CAS 未命中的 10003）只记日志，不重试、不冻结。
func TestRecoverRefundTimeoutApplyError(t *testing.T) {
	u := newRecoverUnit(t)
	u.rapplier.err = errno.ErrInternal
	row := u.row()
	u.clk.Advance(2 * time.Hour)
	u.p.recoverRefund(context.Background(), row)
	calls, _, _ := u.freezer.snapshot()
	if u.rapplier.calls != 1 || len(calls) != 0 || u.channel.callCount() != 0 {
		t.Fatalf("apply=%d freeze=%d channel=%d", u.rapplier.calls, len(calls), u.channel.callCount())
	}
}

// NFR-REL-05：单个任务 panic 被恢复并计数，不影响调用方。
func TestRecoverRefundTaskRecoversPanic(t *testing.T) {
	u := newRecoverUnit(t)
	u.freezer.panics = true
	before := refundRecoverPanics.Load()
	u.p.runRefundRecoverTask(context.Background(), u.row())
	if got := refundRecoverPanics.Load(); got != before+1 {
		t.Fatalf("panics = %d, want %d", got, before+1)
	}
}

// 每个任务有自己的超时，不随批次 ctx 取消：已经开始的冻结不会因为进程停止被打断成未知结果。
func TestRecoverRefundTaskOutlivesBatchCancel(t *testing.T) {
	u := newRecoverUnit(t)
	u.freezer.err = errno.ErrUnavailable
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	u.p.runRefundRecoverTask(ctx, u.row())
	u.freezer.mu.Lock()
	defer u.freezer.mu.Unlock()
	if len(u.freezer.calls) != 1 || u.freezer.ctxErr != nil || !u.freezer.hasDL {
		t.Fatalf("calls=%v ctxErr=%v deadline=%v", u.freezer.calls, u.freezer.ctxErr, u.freezer.hasDL)
	}
}

func recoverRows(u recoverUnit, n int) []RefundOrder {
	rows := make([]RefundOrder, n)
	for i := range rows {
		rows[i] = u.row()
	}
	return rows
}

// 事务外最多 10 个并发恢复，全部行都会执行。
func TestDispatchRefundRecoverLimitsConcurrency(t *testing.T) {
	u := newRecoverUnit(t)
	u.freezer.err = errno.ErrUnavailable
	gate := make(chan struct{})
	u.freezer.gate = gate
	rows := recoverRows(u, 25)
	done := make(chan struct{})
	go func() {
		u.p.dispatchRefundRecover(context.Background(), rows)
		close(done)
	}()
	ok := pollFor(5*time.Second, func() bool {
		_, active, _ := u.freezer.snapshot()
		return active == refundRecoverConcurrency
	})
	close(gate)
	<-done
	calls, active, peak := u.freezer.snapshot()
	if !ok || len(calls) != len(rows) || active != 0 || peak != refundRecoverConcurrency {
		t.Fatalf("reached=%v calls=%d active=%d peak=%d", ok, len(calls), active, peak)
	}
}

// 优雅退出：批次 ctx 取消后，等并发槽的行不再启动，已在执行的任务跑完才返回。
func TestDispatchRefundRecoverStopsWaitingOnCancel(t *testing.T) {
	u := newRecoverUnit(t)
	u.freezer.err = errno.ErrUnavailable
	gate := make(chan struct{})
	u.freezer.gate = gate
	rows := recoverRows(u, refundRecoverConcurrency+5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		u.p.dispatchRefundRecover(ctx, rows)
		close(done)
	}()
	full := pollFor(5*time.Second, func() bool {
		_, active, _ := u.freezer.snapshot()
		return active == refundRecoverConcurrency
	})
	cancel()
	returned := pollFor(100*time.Millisecond, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	close(gate)
	<-done
	calls, active, _ := u.freezer.snapshot()
	if !full || len(calls) != refundRecoverConcurrency || active != 0 {
		t.Fatalf("full=%v calls=%d active=%d", full, len(calls), active)
	}
	if returned {
		t.Fatal("dispatch returned before in-flight tasks finished")
	}
}

// 批次 ctx 已取消时不再启动新任务，这些行留到下一轮。
func TestDispatchRefundRecoverSkipsAfterCancel(t *testing.T) {
	u := newRecoverUnit(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	u.p.dispatchRefundRecover(ctx, recoverRows(u, 3))
	if calls, _, _ := u.freezer.snapshot(); len(calls) != 0 {
		t.Fatalf("calls = %v", calls)
	}
}

// nil 的 Payments 和内存存储（不是 MySQL）返回 10003，不冻结。未装配冻结或渠道退款、批次 ctx 已取消
// 要在 MySQL 存储上才能区分，见 TestRunRefundRecoverBatchNotWiredMySQL。
func TestRunRefundRecoverBatchNotReady(t *testing.T) {
	var bare *Payments
	if err := bare.RunRefundRecoverBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil = %v", err)
	}
	u := newRecoverUnit(t)
	if err := u.p.RunRefundRecoverBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("mem store = %v", err)
	}
	if calls, _, _ := u.freezer.snapshot(); len(calls) != 0 {
		t.Fatalf("calls = %v", calls)
	}
}

// fixedRefundApplier 原样返回设定的退款单或错误。
type fixedRefundApplier struct {
	row   RefundOrder
	err   error
	calls atomic.Int32
}

func (a *fixedRefundApplier) ApplyRefundResult(context.Context, RefundResult) (RefundOrder, error) {
	a.calls.Add(1)
	return a.row, a.err
}

// recoverLogLevels 返回 msg 完全相同的日志行的级别，以及全部 ERROR 行数。
func recoverLogLevels(t *testing.T, buf *bytes.Buffer, msg string) (levels []string, errs int) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec.Level == "ERROR" {
			errs++
		}
		if rec.Msg == msg {
			levels = append(levels, rec.Level)
		}
	}
	return levels, errs
}

// 超时置失败的日志按结果区分（审查 L1、L2）：真的置为 FAILED（REFUND_TIMEOUT）记 WARN “timed out”；
// 另一路先到终态（SUCCESS 或其他原因 FAILED）只记 INFO，不记 “timed out”；返回 10003 时重读，
// 已不是 INIT 是正常竞争，记 INFO，没有 ERROR；仍是 INIT 或重读失败才记 ERROR。
func TestRecoverRefundTimeoutLogs(t *testing.T) {
	const (
		timedOut   = "refund recover timed out"
		superseded = "refund recover timeout superseded"
		lostRace   = "refund recover timeout lost race"
		timeoutErr = "refund recover timeout"
	)
	cases := []struct {
		name    string
		applied RefundOrder
		err     error
		stored  string // 重读时库里的状态；"" 表示不改
		readErr error
		msg     string
		level   string
		errs    int
	}{
		{"timed out", RefundOrder{Status: refundStatusFailed, FailCode: failCodeRefundTimeout}, nil, "", nil, timedOut, "WARN", 0},
		{"already success", RefundOrder{Status: payResultSuccess}, nil, "", nil, superseded, "INFO", 0},
		{"failed by channel", RefundOrder{Status: refundStatusFailed, FailCode: failCodeChannelFailed}, nil, "", nil, superseded, "INFO", 0},
		{"lost race to processing", RefundOrder{}, errno.ErrInternal, refundStatusProcessing, nil, lostRace, "INFO", 0},
		{"lost race to success", RefundOrder{}, errno.ErrInternal, payResultSuccess, nil, lostRace, "INFO", 0},
		{"still init", RefundOrder{}, errno.ErrInternal, "", nil, timeoutErr, "ERROR", 1},
		{"reread fails", RefundOrder{}, errno.ErrInternal, refundStatusProcessing, errors.New("db down"), timeoutErr, "ERROR", 1},
		{"not found", RefundOrder{}, errno.ErrOrderNotFound, "", nil, timeoutErr, "WARN", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := newRecoverUnit(t)
			applier := &fixedRefundApplier{row: c.applied, err: c.err}
			applier.row.RefundNo = recoverRefundNo
			u.p.UseRefund(u.freezer, u.channel, applier, testChannelNotifyURL)
			if c.stored != "" {
				u.orders.mu.Lock()
				u.orders.refunds[0].Status = c.stored
				u.orders.mu.Unlock()
			}
			u.orders.readErr = c.readErr
			row := u.row()
			u.clk.Advance(2 * time.Hour)
			logs := captureLog(t)
			u.p.recoverRefund(context.Background(), row)
			if applier.calls.Load() != 1 {
				t.Fatalf("apply = %d", applier.calls.Load())
			}
			levels, errs := recoverLogLevels(t, logs, c.msg)
			if len(levels) != 1 || levels[0] != c.level || errs != c.errs {
				t.Fatalf("%q levels=%v errors=%d logs=%s", c.msg, levels, errs, logs.String())
			}
			if c.msg != timedOut {
				if got, _ := recoverLogLevels(t, logs, timedOut); len(got) != 0 {
					t.Fatalf("timed out logged: %s", logs.String())
				}
			}
			if c.err == nil && u.orders.refundReads != 0 {
				t.Fatalf("reread without error: %d", u.orders.refundReads)
			}
		})
	}
}
