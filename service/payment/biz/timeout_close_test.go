package biz

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// expiryCloser 是可注入 panic、观察 ctx 和并发数的渠道关单假实现。
// gate 非空时 ClosePay 等它关闭才返回，用来让调用停在渠道里。
type expiryCloser struct {
	mu       sync.Mutex
	closes   []string
	queries  []string
	closeOut channelCloseResult
	queryOut channelQueryResult
	closeErr error
	panicOn  string
	ctxErr   error
	hasDL    bool
	gate     chan struct{}
	active   int
	peak     int
}

func (c *expiryCloser) ClosePay(ctx context.Context, reqNo string) (channelCloseResult, error) {
	c.mu.Lock()
	c.closes = append(c.closes, reqNo)
	c.ctxErr = ctx.Err()
	_, c.hasDL = ctx.Deadline()
	boom := c.panicOn == reqNo
	out, err, gate := c.closeOut, c.closeErr, c.gate
	c.active++
	if c.active > c.peak {
		c.peak = c.active
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()
	if gate != nil {
		<-gate
	}
	if boom {
		panic("close boom")
	}
	return out, err
}

func (c *expiryCloser) load() (closes, active, peak int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.closes), c.active, c.peak
}

func (c *expiryCloser) QueryPay(_ context.Context, reqNo string) (channelQueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, reqNo)
	return c.queryOut, nil
}

func (c *expiryCloser) calls() (closes, queries []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.closes...), append([]string(nil), c.queries...)
}

// addOrder 复制第一行为另一笔订单，单号和商户订单号不同，状态由调用方给出。
func addOrder(orders *memOrders, payNo, status string) PaymentOrder {
	orders.mu.Lock()
	defer orders.mu.Unlock()
	row := orders.rows[0]
	row.PayNo = payNo
	row.OutTradeNo = "OUT-" + payNo
	row.Status = status
	orders.rows = append(orders.rows, row)
	return row
}

func rowByPayNo(orders *memOrders, payNo string) PaymentOrder {
	orders.mu.Lock()
	defer orders.mu.Unlock()
	for _, row := range orders.rows {
		if row.PayNo == payNo {
			return row
		}
	}
	return PaymentOrder{}
}

func addChannelTxn(orders *memOrders, payNo, txnNo string) {
	orders.mu.Lock()
	defer orders.mu.Unlock()
	orders.txns = append(orders.txns, channelTxn{
		Channel: channelMockpay, TxnType: txnTypePay, BizNo: payNo,
		ChannelTxnNo: txnNo, Status: txnStatusProcessing, Amount: orders.rows[0].Amount,
	})
}

// PAY-07 规则 2：过期的 INIT 订单走 PAY-06 的本地 CAS，closed_at 用注入时钟截到毫秒，不访问渠道。
func TestTimeoutCloseInitUsesLocalCAS(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &expiryCloser{closeOut: channelCloseResult{Outcome: closeClosed}}
	p.UseClose(ch)
	p.applier = &recordingApplier{}
	p.closeExpiredOrders(context.Background(), []PaymentOrder{order})
	stored := rowByPayNo(orders, order.PayNo)
	want := p.clk.Now().Truncate(time.Millisecond)
	if stored.Status != payStatusClosed || stored.Version != order.Version+1 || !stored.ClosedAt.Equal(want) {
		t.Fatalf("stored = %+v", stored)
	}
	if closes, queries := ch.calls(); len(closes) != 0 || len(queries) != 0 {
		t.Fatalf("closes=%v queries=%v", closes, queries)
	}
}

// PAY-07 规则 2：PAYING 走 PAY-06 的渠道关单分支，写入结果的来源是 JOB。
// CLOSED、ALREADY_PAID、渠道 FAILED 三种结果分别交给 ApplyPayResult。
func TestTimeoutClosePayingAppliesWithSourceJob(t *testing.T) {
	finished := payAt()
	cases := []struct {
		name     string
		outcome  closeOutcome
		query    channelQueryResult
		result   string
		failCode string
		queries  int
	}{
		{name: "closed", outcome: closeClosed, result: payResultClosed},
		{name: "already paid", outcome: closeAlreadyPaid, query: channelQueryResult{Kind: queryFound, OK: true, Amount: 10000, FinishedAt: finished, ChannelTxnNo: "CH-T"}, result: payResultSuccess, queries: 1},
		{name: "txn failed", outcome: closeTxnFailed, result: payResultFailed, failCode: failCodeChannelFailed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, orders, order := seedInit(t)
			setRow(orders, func(row *PaymentOrder) { row.Status = payStatusPaying })
			addChannelTxn(orders, order.PayNo, "CH-T")
			ch := &expiryCloser{closeOut: channelCloseResult{Outcome: c.outcome}, queryOut: c.query}
			p.UseClose(ch)
			status := map[string]string{payResultClosed: payStatusClosed, payResultSuccess: payStatusSuccess, payResultFailed: payStatusFailed}[c.result]
			applier := &recordingApplier{order: PaymentOrder{PayNo: order.PayNo, Status: status}}
			p.applier = applier
			order.Status = payStatusPaying
			p.dispatchExpiredPaying(context.Background(), []PaymentOrder{order})
			if applier.calls != 1 || applier.in.Source != paySourceJob || applier.in.Result != c.result ||
				applier.in.FailCode != c.failCode || applier.in.BizNo != order.PayNo {
				t.Fatalf("apply calls=%d in=%+v", applier.calls, applier.in)
			}
			if c.result == payResultSuccess && (applier.in.Amount == nil || *applier.in.Amount != money.Amount(10000) || !applier.in.FinishedAt.Equal(finished)) {
				t.Fatalf("success in = %+v", applier.in)
			}
			closes, queries := ch.calls()
			if len(closes) != 1 || len(queries) != c.queries {
				t.Fatalf("closes=%v queries=%v", closes, queries)
			}
		})
	}
}

// PAY-07 规则 2：一条失败（渠道调用失败 10004，订单保持 PAYING）不影响同批后面的行；
// 失败的行不写库，下一轮再试。SUCCESS 行即使出现在批次里也只返回 30003，不访问渠道。
func TestTimeoutCloseFailureDoesNotStopBatch(t *testing.T) {
	p, orders, first := seedInit(t)
	setRow(orders, func(row *PaymentOrder) {
		row.Status = payStatusPaying
		row.Version = 2
	})
	first.Status = payStatusPaying
	first.Version = 2
	addChannelTxn(orders, first.PayNo, "CH-1")
	paid := addOrder(orders, "PA0000000000000000702", payStatusSuccess)
	second := addOrder(orders, "PA0000000000000000703", payStatusInit)
	ch := &expiryCloser{closeErr: errors.New("dial failed")}
	p.UseClose(ch)
	applier := &recordingApplier{}
	p.applier = applier
	p.closeExpiredOrders(context.Background(), []PaymentOrder{first, paid, second})
	if got := rowByPayNo(orders, first.PayNo); got.Status != payStatusPaying || got.Version != 2 {
		t.Fatalf("failed row changed: %+v", got)
	}
	if got := rowByPayNo(orders, paid.PayNo); got.Status != payStatusSuccess || got.Version != paid.Version {
		t.Fatalf("success row changed: %+v", got)
	}
	if got := rowByPayNo(orders, second.PayNo); got.Status != payStatusClosed {
		t.Fatalf("later row = %+v", got)
	}
	if closes, _ := ch.calls(); len(closes) != 1 || closes[0] != first.PayNo || applier.calls != 0 {
		t.Fatalf("closes=%v apply=%d", closes, applier.calls)
	}
}

// NFR-REL-05：单行 panic 被恢复并计数，同批后面的行照常关闭。
func TestTimeoutCloseRecoversRowPanic(t *testing.T) {
	p, orders, first := seedInit(t)
	setRow(orders, func(row *PaymentOrder) { row.Status = payStatusPaying })
	first.Status = payStatusPaying
	addChannelTxn(orders, first.PayNo, "CH-1")
	second := addOrder(orders, "PA0000000000000000704", payStatusInit)
	p.UseClose(&expiryCloser{panicOn: first.PayNo})
	p.applier = &recordingApplier{}
	before := timeoutCloseTaskPanics.Load()
	p.closeExpiredOrders(context.Background(), []PaymentOrder{first, second})
	if got := timeoutCloseTaskPanics.Load(); got != before+1 {
		t.Fatalf("panics = %d, want %d", got, before+1)
	}
	if got := rowByPayNo(orders, second.PayNo); got.Status != payStatusClosed {
		t.Fatalf("later row = %+v", got)
	}
}

// 优雅退出：批次 ctx 取消后不再开始新的行；已开始的行用自己的超时，不随批次 ctx 取消。
func TestTimeoutCloseStopsOnCancel(t *testing.T) {
	p, orders, first := seedInit(t)
	setRow(orders, func(row *PaymentOrder) { row.Status = payStatusPaying })
	first.Status = payStatusPaying
	addChannelTxn(orders, first.PayNo, "CH-1")
	second := addOrder(orders, "PA0000000000000000705", payStatusInit)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := &expiryCloser{closeOut: channelCloseResult{Outcome: closeClosed}}
	p.UseClose(ch)
	// 第一行进入渠道调用后取消批次，模拟 Stop 在批次中途到来。
	p.applier = applierFunc(func(context.Context, PayResult) (PaymentOrder, error) {
		cancel()
		return PaymentOrder{PayNo: first.PayNo, Status: payStatusClosed}, nil
	})
	p.closeExpiredOrders(ctx, []PaymentOrder{first, second})
	ch.mu.Lock()
	ctxErr, hasDL := ch.ctxErr, ch.hasDL
	ch.mu.Unlock()
	if ctxErr != nil || !hasDL {
		t.Fatalf("row ctx err=%v deadline=%v", ctxErr, hasDL)
	}
	if got := rowByPayNo(orders, second.PayNo); got.Status != payStatusInit {
		t.Fatalf("row started after cancel: %+v", got)
	}
	// 已取消的批次一行都不开始。
	p.closeExpiredOrders(ctx, []PaymentOrder{second})
	if got := rowByPayNo(orders, second.PayNo); got.Status != payStatusInit {
		t.Fatalf("row started on canceled batch: %+v", got)
	}
}

type applierFunc func(context.Context, PayResult) (PaymentOrder, error)

func (f applierFunc) ApplyPayResult(ctx context.Context, in PayResult) (PaymentOrder, error) {
	return f(ctx, in)
}

// 未装配关单、没有时钟或不是 MySQL 存储时 RunTimeoutCloseBatch 返回 10003，不访问渠道。
func TestRunTimeoutCloseBatchNotReady(t *testing.T) {
	var bare *Payments
	if err := bare.RunTimeoutCloseBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil = %v", err)
	}
	p, _, _ := seedInit(t)
	if err := p.RunTimeoutCloseBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("not wired = %v", err)
	}
	ch := &expiryCloser{}
	p.UseClose(ch)
	p.applier = &recordingApplier{}
	if err := p.RunTimeoutCloseBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("mem store = %v", err)
	}
	if closes, queries := ch.calls(); len(closes) != 0 || len(queries) != 0 {
		t.Fatalf("closes=%v queries=%v", closes, queries)
	}
}

// seedPaying 建 n 笔已有渠道流水号的 PAYING 订单，关单直接调用渠道；applier 并发安全地计数并返回 CLOSED。
func seedPaying(t *testing.T, n int) (*Payments, *memOrders, []PaymentOrder, *atomic.Int64) {
	t.Helper()
	p, orders, first := seedInit(t)
	setRow(orders, func(row *PaymentOrder) { row.Status = payStatusPaying })
	first.Status = payStatusPaying
	addChannelTxn(orders, first.PayNo, "CH-0")
	rows := []PaymentOrder{first}
	for i := 1; i < n; i++ {
		payNo := fmt.Sprintf("PA%019d", 7100+i)
		rows = append(rows, addOrder(orders, payNo, payStatusPaying))
		addChannelTxn(orders, payNo, "CH-"+strconv.Itoa(i))
	}
	applied := &atomic.Int64{}
	p.applier = applierFunc(func(_ context.Context, in PayResult) (PaymentOrder, error) {
		applied.Add(1)
		return PaymentOrder{PayNo: in.BizNo, Status: payStatusClosed}, nil
	})
	return p, orders, rows, applied
}

// PAY-07 规则 2（审查修复）：PAYING 以最多 10 个并发关闭，渠道挂起时同时在途的不超过 10 笔，
// 槽位空出后其余行照常处理。
func TestDispatchExpiredPayingLimitsConcurrency(t *testing.T) {
	p, _, rows, applied := seedPaying(t, 25)
	gate := make(chan struct{})
	ch := &expiryCloser{closeOut: channelCloseResult{Outcome: closeClosed}, gate: gate}
	p.UseClose(ch)
	done := make(chan struct{})
	go func() {
		p.dispatchExpiredPaying(context.Background(), rows)
		close(done)
	}()
	full := pollFor(5*time.Second, func() bool {
		_, active, _ := ch.load()
		return active == timeoutCloseConcurrency
	})
	close(gate)
	<-done
	closes, active, peak := ch.load()
	if !full || closes != len(rows) || active != 0 || peak != timeoutCloseConcurrency || applied.Load() != int64(len(rows)) {
		t.Fatalf("full=%v closes=%d active=%d peak=%d applied=%d", full, closes, active, peak, applied.Load())
	}
}

// 优雅退出（审查修复）：批次 ctx 取消后，等并发槽的行不再启动；在途的行用自己的超时跑完，分发等它们结束才返回。
func TestDispatchExpiredPayingStopsWaitingOnCancel(t *testing.T) {
	p, orders, rows, applied := seedPaying(t, timeoutCloseConcurrency+5)
	gate := make(chan struct{})
	ch := &expiryCloser{closeOut: channelCloseResult{Outcome: closeClosed}, gate: gate}
	p.UseClose(ch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		p.dispatchExpiredPaying(ctx, rows)
		close(done)
	}()
	full := pollFor(5*time.Second, func() bool {
		_, active, _ := ch.load()
		return active == timeoutCloseConcurrency
	})
	cancel()
	// 槽位都还被占着：取消必须让分发停在等待处，而不是等槽位空出来再启动下一个。
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
	closes, active, _ := ch.load()
	if !full || closes != timeoutCloseConcurrency || active != 0 || applied.Load() != timeoutCloseConcurrency {
		t.Fatalf("full=%v closes=%d active=%d applied=%d", full, closes, active, applied.Load())
	}
	if returned {
		t.Fatal("dispatch returned before in-flight rows finished")
	}
	ch.mu.Lock()
	ctxErr, hasDL := ch.ctxErr, ch.hasDL
	ch.mu.Unlock()
	if ctxErr != nil || !hasDL {
		t.Fatalf("row ctx err=%v deadline=%v", ctxErr, hasDL)
	}
	if got := rowByPayNo(orders, rows[len(rows)-1].PayNo); got.Status != payStatusPaying {
		t.Fatalf("waiting row changed: %+v", got)
	}
	// 已取消的批次一行都不启动。
	p.dispatchExpiredPaying(ctx, rows[len(rows)-1:])
	if closes, _, _ := ch.load(); closes != timeoutCloseConcurrency {
		t.Fatalf("closes after canceled batch = %d", closes)
	}
}

// NFR-REL-05（审查修复）：并发分发里单行 panic 在该行的 goroutine 内恢复并计数，其余行照常关闭。
func TestDispatchExpiredPayingRecoversPanic(t *testing.T) {
	p, _, rows, applied := seedPaying(t, 3)
	p.UseClose(&expiryCloser{closeOut: channelCloseResult{Outcome: closeClosed}, panicOn: rows[0].PayNo})
	before := timeoutCloseTaskPanics.Load()
	p.dispatchExpiredPaying(context.Background(), rows)
	if got := timeoutCloseTaskPanics.Load(); got != before+1 {
		t.Fatalf("panics = %d, want %d", got, before+1)
	}
	if applied.Load() != 2 {
		t.Fatalf("applied = %d, want 2", applied.Load())
	}
}

// 审查修复：失败按错误码计入 pay_timeout_close_failures_total，日志不带错误文本；
// 渠道调用失败 10004 记 WARN，渠道金额不符 50002 记 ERROR。
// CAS 未命中后重读到已是 CLOSED 的行记 "timeout close settled"，不计失败。
func TestTimeoutCloseFailureCountsAndLevels(t *testing.T) {
	logs := captureLog(t)
	p, orders, rows, _ := seedPaying(t, 1)
	unavailable := strconv.Itoa(int(errno.ErrUnavailable.Code))
	mismatch := strconv.Itoa(int(errno.ErrChannelAmountMismatch.Code))
	beforeUnavailable, beforeMismatch := timeoutCloseFailureCount(unavailable), timeoutCloseFailureCount(mismatch)

	p.UseClose(&expiryCloser{closeErr: errors.New("dial tcp 10.0.0.1: secret")})
	p.dispatchExpiredPaying(context.Background(), rows)
	if got := timeoutCloseFailureCount(unavailable); got != beforeUnavailable+1 {
		t.Fatalf("10004 failures = %d, want %d", got, beforeUnavailable+1)
	}
	if !strings.Contains(logs.String(), `"level":"WARN","msg":"timeout close failed"`) ||
		strings.Contains(logs.String(), "secret") {
		t.Fatalf("10004 log = %s", logs.String())
	}

	logs.Reset()
	p.UseClose(&expiryCloser{closeOut: channelCloseResult{Outcome: closeClosed}})
	p.applier = applierFunc(func(context.Context, PayResult) (PaymentOrder, error) {
		return PaymentOrder{}, errno.ErrChannelAmountMismatch
	})
	p.dispatchExpiredPaying(context.Background(), rows)
	if got := timeoutCloseFailureCount(mismatch); got != beforeMismatch+1 {
		t.Fatalf("50002 failures = %d, want %d", got, beforeMismatch+1)
	}
	if !strings.Contains(logs.String(), `"level":"ERROR","msg":"timeout close failed"`) ||
		!strings.Contains(logs.String(), `"err_code":"50002"`) {
		t.Fatalf("50002 log = %s", logs.String())
	}

	logs.Reset()
	scanned := addOrder(orders, "PA0000000000000000799", payStatusClosed)
	scanned.Status = payStatusInit
	p.closeExpiredOrders(context.Background(), []PaymentOrder{scanned})
	if !strings.Contains(logs.String(), `"msg":"timeout close settled"`) ||
		!strings.Contains(logs.String(), `"from_status":"INIT","to_status":"CLOSED"`) {
		t.Fatalf("settled log = %s", logs.String())
	}
	if got := timeoutCloseFailureCount(unavailable); got != beforeUnavailable+1 {
		t.Fatalf("settled row counted as failure: %d", got)
	}
}
