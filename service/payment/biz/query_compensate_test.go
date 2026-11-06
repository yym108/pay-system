package biz

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/sign"
)

const (
	queryPayNo    = "PA0000000000000000501"
	queryRefundNo = "RF0000000000000000501"
)

// fakeQuerier 记录每次查单。gate 不为 nil 时每次调用等它关闭，用来观察并发度。
type fakeQuerier struct {
	mu     sync.Mutex
	res    channelQueryResult
	err    error
	calls  []string
	panics bool
	gate   chan struct{}
	active int
	peak   int
	ctxErr error
	hasDL  bool
	dl     time.Time
}

func (f *fakeQuerier) QueryTxn(ctx context.Context, txnType, reqNo string) (channelQueryResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, txnType+":"+reqNo)
	f.active++
	if f.active > f.peak {
		f.peak = f.active
	}
	f.ctxErr = ctx.Err()
	f.dl, f.hasDL = ctx.Deadline()
	res, err, gate, boom := f.res, f.err, f.gate, f.panics
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	if boom {
		panic("query boom")
	}
	if gate != nil {
		<-gate
	}
	return res, err
}

func (f *fakeQuerier) snapshot() (calls []string, active, peak int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...), f.active, f.peak
}

type queryUnit struct {
	p        *Payments
	orders   *memOrders
	clk      *clock.Fake
	querier  *fakeQuerier
	applier  *recordingApplier
	closer   *countingCloser
	refunder *fakeRefunder
	rapplier *refundRecordingApplier
}

func newQueryUnit(t *testing.T) queryUnit {
	t.Helper()
	clk := &clock.Fake{}
	clk.Set(payAt())
	orders := &memOrders{}
	p := newPayments(nil, &stubIDs{}, clk, "", orders)
	u := queryUnit{
		p: p, orders: orders, clk: clk,
		querier:  &fakeQuerier{},
		applier:  &recordingApplier{order: PaymentOrder{PayNo: queryPayNo, Status: payStatusClosed}},
		closer:   &countingCloser{},
		refunder: &fakeRefunder{},
		rapplier: &refundRecordingApplier{},
	}
	p.UsePay(nil, u.applier, testChannelNotifyURL)
	p.UseClose(u.closer)
	p.UseRefund(nil, u.refunder, u.rapplier, testChannelNotifyURL)
	p.UseQuery(u.querier, QueryPolicy{})
	return u
}

func (u queryUnit) seedRefund(status string) {
	u.orders.mu.Lock()
	defer u.orders.mu.Unlock()
	u.orders.refunds = append(u.orders.refunds, RefundOrder{
		RefundNo: queryRefundNo, PayNo: queryPayNo, MerchantID: "M100002", OutRefundNo: "R05",
		Amount: 3000, Status: status, Channel: channelMockpay,
	})
	u.orders.txns = append(u.orders.txns, channelTxn{
		Channel: channelMockpay, TxnType: txnTypeRefund, BizNo: queryRefundNo,
		Amount: 3000, Status: txnStatusProcessing,
	})
}

func (u queryUnit) task(txnType, bizNo string, age time.Duration) queryTask {
	return queryTask{
		ID: 1, BizNo: bizNo, TxnType: txnType, QueryCount: 1, CreatedAt: u.clk.Now().Add(-age),
		NextQueryAt: u.clk.Now().Add(time.Hour),
	}
}

func found(status string, amount int64, hasAmount bool, finished time.Time, txnNo string) channelQueryResult {
	res := channelQueryResult{Kind: queryFound, Status: status, FinishedAt: finished, ChannelTxnNo: txnNo}
	if hasAmount {
		res.HasAmount = true
		res.Amount = money.Amount(amount)
	}
	res.OK = status == payStatusSuccess && hasAmount && !finished.IsZero()
	return res
}

func samePayResult(got, want PayResult) bool {
	if (got.Amount == nil) != (want.Amount == nil) {
		return false
	}
	if got.Amount != nil && *got.Amount != *want.Amount {
		return false
	}
	return got.BizNo == want.BizNo && got.Result == want.Result && got.Source == want.Source &&
		got.FailCode == want.FailCode && got.FailMsg == want.FailMsg && got.ChannelTxnNo == want.ChannelTxnNo &&
		got.ChannelStatusRaw == want.ChannelStatusRaw && got.FinishedAt.Equal(want.FinishedAt)
}

func sameRefundResult(got, want RefundResult) bool {
	if (got.Amount == nil) != (want.Amount == nil) {
		return false
	}
	if got.Amount != nil && *got.Amount != *want.Amount {
		return false
	}
	return got.RefundNo == want.RefundNo && got.Result == want.Result && got.Source == want.Source &&
		got.FailCode == want.FailCode && got.FailMsg == want.FailMsg && got.ChannelTxnNo == want.ChannelTxnNo &&
		got.ChannelStatusRaw == want.ChannelStatusRaw && got.FinishedAt.Equal(want.FinishedAt)
}

// pollFor 用真实时钟每 5 毫秒检查一次 ready，within 内成立返回 true。
func pollFor(within time.Duration, ready func() bool) bool {
	clk := clock.Real{}
	deadline := clk.Now().Add(within)
	for !clk.Now().After(deadline) {
		if ready() {
			return true
		}
		timer := time.NewTimer(5 * time.Millisecond)
		<-timer.C
	}
	return ready()
}

func amountPtr(n int64) *money.Amount {
	v := money.Amount(n)
	return &v
}

// PAY-05 规则 4：第 n 次抢占后的间隔取计划第 n 档（第 0 档是插入时的 10 秒），用完之后取 Tail，Tail 为 0 时每小时一次。
func TestQueryDelay(t *testing.T) {
	plan := []time.Duration{10 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute,
		2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute}
	cases := []struct {
		name   string
		policy QueryPolicy
		n      int
		want   time.Duration
	}{
		{"first claim", QueryPolicy{Backoff: plan}, 1, 10 * time.Second},
		{"second claim", QueryPolicy{Backoff: plan}, 2, 30 * time.Second},
		{"last step", QueryPolicy{Backoff: plan}, 7, 30 * time.Minute},
		{"after plan hourly", QueryPolicy{Backoff: plan}, 8, time.Hour},
		{"far after plan", QueryPolicy{Backoff: plan}, 500, time.Hour},
		{"custom tail", QueryPolicy{Backoff: plan, Tail: 2 * time.Second}, 9, 2 * time.Second},
		{"empty plan uses tail", QueryPolicy{}, 1, time.Hour},
		{"non positive step uses tail", QueryPolicy{Backoff: []time.Duration{time.Second, 0}}, 1, time.Hour},
		{"zero count uses step 0", QueryPolicy{Backoff: plan}, 0, 10 * time.Second},
	}
	for _, c := range cases {
		if got := queryDelay(c.policy, c.n); got != c.want {
			t.Errorf("%s: queryDelay(%d) = %s, want %s", c.name, c.n, got, c.want)
		}
	}
}

// PAY-05 规则 3：PAY 流水的分发表，每个出口都覆盖。NOT_FOUND 只在创建超过 60 秒后关单，关单走 PAY-06 的同一段代码，source = QUERY。
func TestQueryCompensatePayDispatch(t *testing.T) {
	finished := payAt().Add(-time.Second)
	cases := []struct {
		name        string
		res         channelQueryResult
		err         error
		age         time.Duration
		closeOut    channelCloseResult
		closeQuery  channelQueryResult
		want        *PayResult
		wantCloses  int
		wantQueries int
	}{
		{
			name: "success", res: found(payStatusSuccess, 10000, true, finished, "CH-Q"), age: time.Minute,
			want: &PayResult{BizNo: queryPayNo, Result: payResultSuccess, Source: paySourceQuery, Amount: amountPtr(10000), ChannelTxnNo: "CH-Q", FinishedAt: finished},
		},
		{name: "success without finished_at waits", res: found(payStatusSuccess, 10000, true, time.Time{}, "CH-Q"), age: time.Minute},
		{name: "success without amount waits", res: found(payStatusSuccess, 0, false, finished, "CH-Q"), age: time.Minute},
		{
			name: "failed with amount", res: found(payStatusFailed, 10000, true, finished, "CH-F"), age: time.Minute,
			want: &PayResult{BizNo: queryPayNo, Result: payResultFailed, FailCode: failCodeChannelFailed, Source: paySourceQuery, Amount: amountPtr(10000), ChannelTxnNo: "CH-F", FinishedAt: finished},
		},
		{
			name: "failed without amount", res: found(payStatusFailed, 0, false, time.Time{}, ""), age: time.Minute,
			want: &PayResult{BizNo: queryPayNo, Result: payResultFailed, FailCode: failCodeChannelFailed, Source: paySourceQuery},
		},
		{name: "processing", res: found(txnStatusProcessing, 10000, true, time.Time{}, "CH-P"), age: 2 * time.Hour},
		{
			name: "closed tombstone has no amount", res: found(payStatusClosed, 0, true, time.Time{}, "CH-C"), age: 2 * time.Minute,
			want: &PayResult{BizNo: queryPayNo, Result: payResultClosed, Source: paySourceQuery, ChannelTxnNo: "CH-C"},
		},
		{name: "call failed", res: channelQueryResult{}, age: 2 * time.Minute},
		{name: "local config error", res: channelQueryResult{}, err: errChannelConfig, age: 2 * time.Minute},
		{name: "not found at 60s", res: channelQueryResult{Kind: queryNotFound}, age: queryNotFoundGrace},
		{name: "not found young", res: channelQueryResult{Kind: queryNotFound}, age: 10 * time.Second},
		{
			name: "not found closes", res: channelQueryResult{Kind: queryNotFound}, age: queryNotFoundGrace + time.Millisecond,
			closeOut:   channelCloseResult{Outcome: closeClosed},
			want:       &PayResult{BizNo: queryPayNo, Result: payResultClosed, Source: paySourceQuery},
			wantCloses: 1,
		},
		{
			name: "not found already paid", res: channelQueryResult{Kind: queryNotFound}, age: 2 * time.Minute,
			closeOut:   channelCloseResult{Outcome: closeAlreadyPaid},
			closeQuery: found(payStatusSuccess, 10000, true, finished, "CH-LATE"),
			want:       &PayResult{BizNo: queryPayNo, Result: payResultSuccess, Source: paySourceQuery, Amount: amountPtr(10000), ChannelTxnNo: "CH-LATE", FinishedAt: finished},
			wantCloses: 1, wantQueries: 1,
		},
		{
			name: "not found already paid query fails", res: channelQueryResult{Kind: queryNotFound}, age: 2 * time.Minute,
			closeOut:   channelCloseResult{Outcome: closeAlreadyPaid},
			wantCloses: 1, wantQueries: 1,
		},
		{
			name: "not found channel failed", res: channelQueryResult{Kind: queryNotFound}, age: 2 * time.Minute,
			closeOut:   channelCloseResult{Outcome: closeTxnFailed},
			want:       &PayResult{BizNo: queryPayNo, Result: payResultFailed, FailCode: failCodeChannelFailed, Source: paySourceQuery},
			wantCloses: 1,
		},
		{
			name: "not found close call failed", res: channelQueryResult{Kind: queryNotFound}, age: 2 * time.Minute,
			wantCloses: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := newQueryUnit(t)
			u.querier.res, u.querier.err = c.res, c.err
			u.closer.closeOut, u.closer.queryOut = c.closeOut, c.closeQuery
			u.p.compensate(context.Background(), u.task(txnTypePay, queryPayNo, c.age))
			calls, _, _ := u.querier.snapshot()
			if len(calls) != 1 || calls[0] != txnTypePay+":"+queryPayNo {
				t.Fatalf("query calls = %v", calls)
			}
			if c.want == nil {
				if u.applier.calls != 0 {
					t.Fatalf("applied %+v", u.applier.in)
				}
			} else if u.applier.calls != 1 || !samePayResult(u.applier.in, *c.want) {
				t.Fatalf("apply calls=%d in=%+v want %+v", u.applier.calls, u.applier.in, *c.want)
			}
			if closes, queries := u.closer.calls(); closes != c.wantCloses || queries != c.wantQueries {
				t.Fatalf("closes=%d queries=%d", closes, queries)
			}
			if u.rapplier.calls != 0 || u.refunder.callCount() != 0 {
				t.Fatal("pay txn touched refund path")
			}
		})
	}
}

// PAY-05 规则 3：REFUND 流水的分发表。SUCCESS / FAILED 交给 ApplyRefundResult（source = QUERY）；
// NOT_FOUND 且创建超过 60 秒时按 REF-02 的同一段代码重新提交渠道退款。
func TestQueryCompensateRefundDispatch(t *testing.T) {
	finished := payAt().Add(-time.Second)
	cases := []struct {
		name         string
		res          channelQueryResult
		age          time.Duration
		refundStatus string
		refund       ChannelAcceptResult
		want         *RefundResult
		wantSubmit   int
		wantTxnNo    string
	}{
		{
			name: "success", res: found(payStatusSuccess, 3000, true, finished, "CH-RS"), age: time.Minute,
			want: &RefundResult{RefundNo: queryRefundNo, Result: payResultSuccess, Source: paySourceQuery, Amount: amountPtr(3000), ChannelTxnNo: "CH-RS", FinishedAt: finished},
		},
		{name: "success without finished_at waits", res: found(payStatusSuccess, 3000, true, time.Time{}, "CH-RS"), age: time.Minute},
		{
			name: "failed", res: found(payStatusFailed, 3000, true, finished, "CH-RF"), age: time.Minute,
			want: &RefundResult{RefundNo: queryRefundNo, Result: payResultFailed, Source: paySourceQuery, Amount: amountPtr(3000), ChannelTxnNo: "CH-RF", FinishedAt: finished},
		},
		{name: "processing", res: found(txnStatusProcessing, 3000, true, time.Time{}, "CH-RP"), age: time.Hour},
		{name: "closed waits", res: found(payStatusClosed, 0, true, time.Time{}, "CH-RC"), age: time.Hour},
		{name: "call failed", res: channelQueryResult{}, age: time.Hour},
		{name: "not found young", res: channelQueryResult{Kind: queryNotFound}, age: queryNotFoundGrace},
		{
			name: "not found resubmits ok", res: channelQueryResult{Kind: queryNotFound}, age: 2 * time.Minute,
			refund:     ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH-RE"},
			wantSubmit: 1, wantTxnNo: "CH-RE",
		},
		{
			name: "not found resubmit rejected", res: channelQueryResult{Kind: queryNotFound}, age: 2 * time.Minute,
			refund:     ChannelAcceptResult{Outcome: acceptRejected, Reason: "ORIG_NOT_SUCCESS"},
			want:       &RefundResult{RefundNo: queryRefundNo, Result: payResultFailed, FailCode: failCodeChannelRejected, FailMsg: "ORIG_NOT_SUCCESS", Source: paySourceSync},
			wantSubmit: 1,
		},
		{
			name: "not found resubmit unknown", res: channelQueryResult{Kind: queryNotFound}, age: 2 * time.Minute,
			wantSubmit: 1,
		},
		{
			name: "not found refund already terminal", res: channelQueryResult{Kind: queryNotFound}, age: 2 * time.Minute,
			refundStatus: payResultSuccess,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := newQueryUnit(t)
			status := c.refundStatus
			if status == "" {
				status = refundStatusProcessing
			}
			u.seedRefund(status)
			u.querier.res = c.res
			u.refunder.res = c.refund
			u.p.compensate(context.Background(), u.task(txnTypeRefund, queryRefundNo, c.age))
			calls, _, _ := u.querier.snapshot()
			if len(calls) != 1 || calls[0] != txnTypeRefund+":"+queryRefundNo {
				t.Fatalf("query calls = %v", calls)
			}
			if c.want == nil {
				if u.rapplier.calls != 0 {
					t.Fatalf("applied %+v", u.rapplier.got())
				}
			} else if u.rapplier.calls != 1 || !sameRefundResult(u.rapplier.got(), *c.want) {
				t.Fatalf("apply calls=%d in=%+v want %+v", u.rapplier.calls, u.rapplier.got(), *c.want)
			}
			if u.refunder.callCount() != c.wantSubmit {
				t.Fatalf("refund submits = %d", u.refunder.callCount())
			}
			if c.wantSubmit > 0 {
				req := u.refunder.got()
				if req.ReqNo != queryRefundNo || req.OrigReqNo != queryPayNo || req.Amount != 3000 || req.NotifyURL != testChannelNotifyURL {
					t.Fatalf("refund req = %+v", req)
				}
			}
			txn, _, _ := u.orders.channelTxnByBizNo(context.Background(), queryRefundNo)
			if txn.ChannelTxnNo != c.wantTxnNo {
				t.Fatalf("channel_txn_no = %q", txn.ChannelTxnNo)
			}
			if u.applier.calls != 0 {
				t.Fatal("refund txn touched pay path")
			}
			if closes, queries := u.closer.calls(); closes != 0 || queries != 0 {
				t.Fatalf("refund txn closed pay: %d %d", closes, queries)
			}
		})
	}
}

// 认不出的 txn_type 不调用任何推进路径。
func TestQueryCompensateUnknownType(t *testing.T) {
	u := newQueryUnit(t)
	u.querier.res = found(payStatusSuccess, 1, true, payAt(), "CH")
	u.p.compensate(context.Background(), u.task("TRANSFER", queryPayNo, time.Hour))
	if calls, _, _ := u.querier.snapshot(); len(calls) != 0 {
		t.Fatalf("queried unknown type: %v", calls)
	}
	if u.applier.calls != 0 || u.rapplier.calls != 0 || u.refunder.callCount() != 0 {
		t.Fatal("unknown type applied")
	}
}

// NFR-REL-05：单个任务 panic 被恢复并计数，不影响调用方。
func TestQueryTaskRecoversPanic(t *testing.T) {
	u := newQueryUnit(t)
	u.querier.panics = true
	before := queryTaskPanics.Load()
	u.p.runQueryTask(context.Background(), u.task(txnTypePay, queryPayNo, time.Minute))
	if got := queryTaskPanics.Load(); got != before+1 {
		t.Fatalf("panics = %d, want %d", got, before+1)
	}
}

// PAY-05 规则 2：每个任务有自己的超时，不随批次 ctx 取消；查单本身再由客户端限制在 3 秒。
func TestQueryTaskOutlivesBatchCancel(t *testing.T) {
	u := newQueryUnit(t)
	u.querier.res = found(txnStatusProcessing, 1, true, time.Time{}, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	u.p.runQueryTask(ctx, u.task(txnTypePay, queryPayNo, time.Minute))
	u.querier.mu.Lock()
	defer u.querier.mu.Unlock()
	if len(u.querier.calls) != 1 || u.querier.ctxErr != nil || !u.querier.hasDL {
		t.Fatalf("calls=%v ctxErr=%v deadline=%v", u.querier.calls, u.querier.ctxErr, u.querier.hasDL)
	}
}

// PAY-05 规则 2：事务外最多 10 个并发查单，全部任务都会执行。
func TestDispatchQueriesLimitsConcurrency(t *testing.T) {
	u := newQueryUnit(t)
	u.querier.res = found(txnStatusProcessing, 1, true, time.Time{}, "")
	gate := make(chan struct{})
	u.querier.gate = gate
	tasks := make([]queryTask, 25)
	for i := range tasks {
		tasks[i] = u.task(txnTypePay, queryPayNo, time.Minute)
		tasks[i].ID = int64(i + 1)
	}
	done := make(chan struct{})
	go func() {
		u.p.dispatchQueries(context.Background(), tasks)
		close(done)
	}()
	ok := pollFor(5*time.Second, func() bool {
		_, active, _ := u.querier.snapshot()
		return active == queryConcurrency
	})
	close(gate)
	<-done
	calls, active, peak := u.querier.snapshot()
	if !ok || len(calls) != len(tasks) || active != 0 || peak != queryConcurrency {
		t.Fatalf("reached=%v calls=%d active=%d peak=%d", ok, len(calls), active, peak)
	}
}

// PAY-05 规则 1：任务启动时抢占写入的 next_query_at 已到期，这一行可能已被别的实例接手，
// 不查单、不写库，只计数；还没到期的照常查单。
func TestQueryTaskSkipsReclaimableClaim(t *testing.T) {
	u := newQueryUnit(t)
	u.querier.res = found(txnStatusProcessing, 1, true, time.Time{}, "")
	task := u.task(txnTypePay, queryPayNo, time.Minute)
	task.NextQueryAt = u.clk.Now()
	before := queryClaimSkipped.Load()
	u.p.runQueryTask(context.Background(), task)
	if calls, _, _ := u.querier.snapshot(); len(calls) != 0 {
		t.Fatalf("queried reclaimable row: %v", calls)
	}
	if got := queryClaimSkipped.Load(); got != before+1 {
		t.Fatalf("skipped = %d, want %d", got, before+1)
	}
	task.NextQueryAt = u.clk.Now().Add(time.Millisecond)
	u.p.runQueryTask(context.Background(), task)
	if calls, _, _ := u.querier.snapshot(); len(calls) != 1 {
		t.Fatalf("calls = %v", calls)
	}
	if got := queryClaimSkipped.Load(); got != before+1 {
		t.Fatalf("skipped = %d, want %d", got, before+1)
	}
}

// 优雅退出：批次 ctx 取消后，等并发槽的任务不再启动，已在执行的任务跑完。
func TestDispatchQueriesStopsWaitingOnCancel(t *testing.T) {
	u := newQueryUnit(t)
	u.querier.res = found(txnStatusProcessing, 1, true, time.Time{}, "")
	gate := make(chan struct{})
	u.querier.gate = gate
	tasks := make([]queryTask, queryConcurrency+5)
	for i := range tasks {
		tasks[i] = u.task(txnTypePay, queryPayNo, time.Minute)
		tasks[i].ID = int64(i + 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		u.p.dispatchQueries(ctx, tasks)
		close(done)
	}()
	full := pollFor(5*time.Second, func() bool {
		_, active, _ := u.querier.snapshot()
		return active == queryConcurrency
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
	calls, active, _ := u.querier.snapshot()
	if !full || len(calls) != queryConcurrency || active != 0 {
		t.Fatalf("full=%v calls=%d active=%d", full, len(calls), active)
	}
	if returned {
		t.Fatal("dispatch returned before in-flight tasks finished")
	}
}

// 批次 ctx 已取消时不再启动新任务；已抢占的行等下一档。
func TestDispatchQueriesSkipsAfterCancel(t *testing.T) {
	u := newQueryUnit(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	u.p.dispatchQueries(ctx, []queryTask{u.task(txnTypePay, queryPayNo, time.Minute)})
	if calls, _, _ := u.querier.snapshot(); len(calls) != 0 {
		t.Fatalf("calls = %v", calls)
	}
}

// 未装配查单或不是 MySQL 存储时 RunQueryBatch 返回 10003，不调用渠道。
func TestRunQueryBatchNotReady(t *testing.T) {
	var bare *Payments
	if err := bare.RunQueryBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil = %v", err)
	}
	u := newQueryUnit(t)
	if err := u.p.RunQueryBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("mem store = %v", err)
	}
	u.p.UseQuery(nil, QueryPolicy{})
	if err := u.p.RunQueryBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("no querier = %v", err)
	}
	if calls, _, _ := u.querier.snapshot(); len(calls) != 0 {
		t.Fatalf("calls = %v", calls)
	}
}

// QueryTxn 按 txn_type 查单，带回状态、金额与完成时间；FAILED 带金额但 OK 为 false。
func TestChannelClientQueryTxn(t *testing.T) {
	clk := testClock()
	finished := payAt().Truncate(time.Millisecond)
	cases := []struct {
		txnType string
		body    string
		want    channelQueryResult
	}{
		{txnTypeRefund, `{"result":"OK","status":"SUCCESS","amount":3000,"channel_txn_no":"CH-R","finished_at":"` + finished.Format(channelTimeLayout) + `"}`,
			channelQueryResult{Kind: queryFound, OK: true, Status: payStatusSuccess, Amount: 3000, HasAmount: true, FinishedAt: finished, ChannelTxnNo: "CH-R"}},
		{txnTypePay, `{"result":"OK","status":"FAILED","amount":10001,"channel_txn_no":"CH-F","finished_at":"` + finished.Format(channelTimeLayout) + `"}`,
			channelQueryResult{Kind: queryFound, Status: payStatusFailed, Amount: 10001, HasAmount: true, FinishedAt: finished, ChannelTxnNo: "CH-F"}},
		{txnTypePay, `{"result":"OK","status":"CLOSED","amount":0,"channel_txn_no":"CH-C"}`,
			channelQueryResult{Kind: queryFound, Status: payStatusClosed, HasAmount: true, ChannelTxnNo: "CH-C"}},
		{txnTypeRefund, `{"result":"OK","status":"PROCESSING","channel_txn_no":"CH-P"}`,
			channelQueryResult{Kind: queryFound, Status: txnStatusProcessing, ChannelTxnNo: "CH-P"}},
		{txnTypeRefund, `{"result":"NOT_FOUND"}`, channelQueryResult{Kind: queryNotFound}},
		{txnTypePay, `{"result":"OK","status":"WEIRD"}`, channelQueryResult{}},
	}
	for _, c := range cases {
		var got channelQueryBody
		var path string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path = r.URL.Path
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &got)
			ts := r.Header.Get(sign.HeaderMockTimestamp)
			sig, err := sign.SignChannel([]byte(testChannelKey), sign.ChannelMessage{Timestamp: ts, Body: []byte(c.body)})
			if err != nil {
				t.Errorf("sign: %v", err)
				return
			}
			w.Header().Set(sign.HeaderMockTimestamp, ts)
			w.Header().Set(sign.HeaderMockSignature, sig)
			_, _ = w.Write([]byte(c.body))
		}))
		client := newTestClient(t, srv.URL, clk, testChannelKey)
		res, err := client.QueryTxn(context.Background(), c.txnType, "RF0000000000000000001")
		srv.Close()
		if err != nil {
			t.Fatal(err)
		}
		if path != channelQueryPath || got.TxnType != c.txnType || got.ReqNo != "RF0000000000000000001" {
			t.Fatalf("request path=%s body=%+v", path, got)
		}
		if res.Kind != c.want.Kind || res.OK != c.want.OK || res.Status != c.want.Status || res.Amount != c.want.Amount ||
			res.HasAmount != c.want.HasAmount || !res.FinishedAt.Equal(c.want.FinishedAt) || res.ChannelTxnNo != c.want.ChannelTxnNo {
			t.Fatalf("%s => %+v, want %+v", c.body, res, c.want)
		}
	}
	var nilClient *ChannelClient
	if _, err := nilClient.QueryTxn(context.Background(), txnTypePay, "PA1"); !errors.Is(err, errChannelConfig) {
		t.Fatalf("nil client = %v", err)
	}
}
