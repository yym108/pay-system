package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

type fakeFreezer struct {
	mu     sync.Mutex
	status string
	err    error
	calls  int
	last   struct {
		merchant string
		refundNo string
		amount   money.Amount
	}
}

func (f *fakeFreezer) FreezeRefund(_ context.Context, merchantID, refundNo string, amount money.Amount) (RefundFreeze, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.last.merchant = merchantID
	f.last.refundNo = refundNo
	f.last.amount = amount
	if f.err != nil {
		return RefundFreeze{}, f.err
	}
	return RefundFreeze{Status: f.status}, nil
}

func (f *fakeFreezer) snapshot() (merchant, refundNo string, amount money.Amount, calls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last.merchant, f.last.refundNo, f.last.amount, f.calls
}

type fakeRefunder struct {
	mu    sync.Mutex
	res   ChannelAcceptResult
	err   error
	calls int
	last  ChannelRefundRequest
}

func (f *fakeRefunder) Refund(_ context.Context, req ChannelRefundRequest) (ChannelAcceptResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.last = req
	if f.err != nil {
		return ChannelAcceptResult{}, f.err
	}
	return f.res, nil
}

func (f *fakeRefunder) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeRefunder) got() ChannelRefundRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

type refundRecordingApplier struct {
	mu    sync.Mutex
	last  RefundResult
	err   error
	calls int
}

func (a *refundRecordingApplier) ApplyRefundResult(_ context.Context, in RefundResult) (RefundOrder, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.last = in
	if a.err != nil {
		return RefundOrder{}, a.err
	}
	return RefundOrder{
		RefundNo: in.RefundNo,
		Status:   refundStatusFailed,
		FailCode: in.FailCode,
		FailMsg:  in.FailMsg,
	}, nil
}

func (a *refundRecordingApplier) got() RefundResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.last
}

func copiedTxns(m *memOrders) []channelTxn {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]channelTxn, len(m.txns))
	copy(out, m.txns)
	return out
}

func refundSubmitPayments(orders *memOrders) (*Payments, *clock.Fake) {
	clk := &clock.Fake{}
	clk.Set(feeAt(10, 0, 0, 0))
	ids := &stubIDs{prefix: refundNoPrefix, nos: []string{
		"RF0000000000000000001",
		"RF0000000000000000002",
		"RF0000000000000000003",
		"RF0000000000000000004",
	}}
	return newPayments(nil, ids, clk, "", orders), clk
}

// REF-02 验收：正常退款返回 PROCESSING，冻结 1 次，渠道受理 1 次，channel_txn 1 条。
func TestCreateRefundSubmitProcessing(t *testing.T) {
	orders := newRefundOrders()
	p, clk := refundSubmitPayments(orders)
	freezer := &fakeFreezer{status: freezeStatusFrozen}
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_RF_1"}}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	if err != nil || got.Status != refundStatusProcessing || got.RefundNo != "RF0000000000000000001" || got.Version != 1 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if refunded, _ := payRefunded(orders, "PA_REF_1"); refunded != 1000 {
		t.Fatalf("refunded = %d", refunded)
	}
	merchant, refundNo, amount, freezeCalls := freezer.snapshot()
	if freezeCalls != 1 || merchant != "M100002" || refundNo != got.RefundNo || amount != 1000 {
		t.Fatalf("freeze merchant=%s refund=%s amount=%d calls=%d", merchant, refundNo, amount, freezeCalls)
	}
	req := channel.got()
	if channel.callCount() != 1 || req.ReqNo != got.RefundNo || req.OrigReqNo != "PA_REF_1" || req.Amount != 1000 || req.NotifyURL != testChannelNotifyURL {
		t.Fatalf("channel = %+v calls=%d", req, channel.callCount())
	}
	txns := copiedTxns(orders)
	if len(txns) != 1 || txns[0].TxnType != txnTypeRefund || txns[0].Status != txnStatusProcessing ||
		txns[0].BizNo != got.RefundNo || txns[0].ChannelTxnNo != "CH_RF_1" || txns[0].Amount != 1000 {
		t.Fatalf("txns = %+v", txns)
	}
	wantNext := clk.Now().Truncate(time.Millisecond).Add(payNextQueryDelay)
	if !txns[0].NextQueryAt.Equal(wantNext) {
		t.Fatalf("next_query_at = %s want %s", txns[0].NextQueryAt, wantNext)
	}
	again, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	_, _, _, freezeCalls = freezer.snapshot()
	if err != nil || again.RefundNo != got.RefundNo || freezeCalls != 1 || channel.callCount() != 1 {
		t.Fatalf("again = %+v err=%v freeze=%d channel=%d", again, err, freezeCalls, channel.callCount())
	}
}

// REF-02 验收：Freeze 超时返回 INIT，不写 channel_txn，不调用渠道。
func TestCreateRefundFreezeUnavailable(t *testing.T) {
	orders := newRefundOrders()
	p, _ := refundSubmitPayments(orders)
	freezer := &fakeFreezer{err: errno.ErrUnavailable}
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_RF_1"}}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	if err != nil || got.Status != refundStatusInit || got.RefundNo == "" {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if channel.callCount() != 0 || len(copiedTxns(orders)) != 0 {
		t.Fatalf("channel=%d txns=%+v", channel.callCount(), copiedTxns(orders))
	}
	if refunded, _ := payRefunded(orders, "PA_REF_1"); refunded != 1000 {
		t.Fatalf("refunded = %d", refunded)
	}
	again, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	_, _, _, freezeCalls := freezer.snapshot()
	if err != nil || again.RefundNo != got.RefundNo || again.Status != refundStatusInit || freezeCalls != 1 {
		t.Fatalf("again = %+v err=%v freeze=%d", again, err, freezeCalls)
	}
}

func TestCreateRefundFreezeAccountMissing(t *testing.T) {
	for _, cause := range []error{errno.ErrAccountNotFound, errno.ErrAccountInactive} {
		orders := newRefundOrders()
		p, _ := refundSubmitPayments(orders)
		freezer := &fakeFreezer{err: cause}
		channel := &fakeRefunder{}
		p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
		_, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
		assertErrno(t, err, errno.ErrInternal)
		if channel.callCount() != 0 {
			t.Fatalf("channel calls = %d", channel.callCount())
		}
		rows := copiedRefunds(orders)
		if len(rows) != 1 || rows[0].Status != refundStatusInit {
			t.Fatalf("refunds = %+v", rows)
		}
	}
}

func TestCreateRefundApplyDelegates(t *testing.T) {
	orders := newRefundOrders()
	p, _ := refundSubmitPayments(orders)
	freezer := &fakeFreezer{}
	channel := &fakeRefunder{}
	applier := &refundRecordingApplier{}
	p.UseRefund(freezer, channel, applier, testChannelNotifyURL)

	freezer.err = errno.ErrInsufficientBalance
	got, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	assertErrno(t, err, errno.ErrInsufficientBalance)
	if got.Status != refundStatusFailed || got.FailCode != failCodeInsufficientBalance || applier.got().FailCode != failCodeInsufficientBalance || applier.got().RefundNo != got.RefundNo {
		t.Fatalf("balance = %+v apply=%+v", got, applier.got())
	}
	if channel.callCount() != 0 {
		t.Fatalf("channel calls = %d", channel.callCount())
	}

	freezer.err = errno.ErrFreezeStatusConflict
	got, err = p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R2", 1000))
	if err != nil || got.Status != refundStatusFailed || got.FailCode != failCodeFreezeConflict || applier.got().Result != payResultFailed || applier.got().Source != paySourceSync {
		t.Fatalf("conflict = %+v err=%v apply=%+v", got, err, applier.got())
	}

	freezer.err = nil
	freezer.status = freezeStatusFrozen
	channel.res = ChannelAcceptResult{Outcome: acceptRejected, Reason: "no"}
	got, err = p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R3", 1000))
	assertErrno(t, err, errno.ErrChannelRejected)
	if got.FailCode != failCodeChannelRejected || got.FailMsg != "no" || applier.got().FailMsg != "no" {
		t.Fatalf("rejected = %+v apply=%+v", got, applier.got())
	}
	if refunded, _ := payRefunded(orders, "PA_REF_1"); refunded != 3000 {
		t.Fatalf("refunded = %d", refunded)
	}
}

// 内存存储没有事务，生产 ApplyRefundResult 返回 10003：退款保持 INIT，额度不释放，不调用渠道。
func TestCreateRefundApplyWithoutTxStaysInit(t *testing.T) {
	orders := newRefundOrders()
	p, _ := refundSubmitPayments(orders)
	freezer := &fakeFreezer{err: errno.ErrInsufficientBalance}
	channel := &fakeRefunder{}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
	_, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	assertErrno(t, err, errno.ErrInternal)
	rows := copiedRefunds(orders)
	if len(rows) != 1 || rows[0].Status != refundStatusInit || len(copiedTxns(orders)) != 0 || channel.callCount() != 0 {
		t.Fatalf("refunds=%+v txns=%+v channel=%d", rows, copiedTxns(orders), channel.callCount())
	}
	if refunded, _ := payRefunded(orders, "PA_REF_1"); refunded != 1000 {
		t.Fatalf("refunded = %d", refunded)
	}
}

func TestCreateRefundChannelUnknown(t *testing.T) {
	orders := newRefundOrders()
	p, _ := refundSubmitPayments(orders)
	p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, &fakeRefunder{}, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	if err != nil || got.Status != refundStatusProcessing {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	txns := copiedTxns(orders)
	if len(txns) != 1 || txns[0].ChannelTxnNo != "" || txns[0].Status != txnStatusProcessing {
		t.Fatalf("txns = %+v", txns)
	}
}

func TestCreateRefundConfirmedStaysInit(t *testing.T) {
	orders := newRefundOrders()
	p, _ := refundSubmitPayments(orders)
	channel := &fakeRefunder{}
	p.UseRefund(&fakeFreezer{status: freezeStatusConfirmed}, channel, nil, testChannelNotifyURL)
	_, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	assertErrno(t, err, errno.ErrInternal)
	rows := copiedRefunds(orders)
	if len(rows) != 1 || rows[0].Status != refundStatusInit || channel.callCount() != 0 || len(copiedTxns(orders)) != 0 {
		t.Fatalf("refunds=%+v channel=%d", rows, channel.callCount())
	}
}

func TestCreateRefundChannelTxnConflictStaysInit(t *testing.T) {
	orders := newRefundOrders()
	orders.txns = append(orders.txns, channelTxn{BizNo: "RF0000000000000000001", TxnType: txnTypeRefund, Status: txnStatusProcessing})
	p, _ := refundSubmitPayments(orders)
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_RF_1"}}
	p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, channel, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	if err != nil || got.Status != refundStatusInit || got.RefundNo != "RF0000000000000000001" {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if channel.callCount() != 0 || len(copiedTxns(orders)) != 1 {
		t.Fatalf("channel=%d txns=%+v", channel.callCount(), copiedTxns(orders))
	}
}

func TestCreateRefundWithoutFreezerStaysInit(t *testing.T) {
	orders := newRefundOrders()
	p, _ := refundSubmitPayments(orders)
	got, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	if err != nil || got.Status != refundStatusInit {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if len(copiedTxns(orders)) != 0 {
		t.Fatalf("txns = %+v", copiedTxns(orders))
	}
}

func TestFreezeTimedOutRecognizesServerTransError(t *testing.T) {
	serverTimeout := kerrors.ErrRemoteOrNetwork.WithCause(remote.NewTransErrorWithMsg(remote.InternalError, kerrors.ErrRPCTimeout.Error()))
	if !freezeTimedOut(serverTimeout) || !freezeTimedOut(context.DeadlineExceeded) || !freezeTimedOut(kerrors.ErrRPCTimeout) {
		t.Fatal("timeout shapes")
	}
	if freezeTimedOut(errno.ErrAccountNotFound) || freezeTimedOut(errno.ErrInsufficientBalance) || freezeTimedOut(errors.New("connection refused")) {
		t.Fatal("business error or connection refused")
	}
}

func TestSubmitRefundUnavailableReturnsInitWhenCallerCancelled(t *testing.T) {
	p, _ := refundSubmitPayments(newRefundOrders())
	channel := &fakeRefunder{}
	p.UseRefund(&fakeFreezer{err: errno.ErrUnavailable}, channel, nil, testChannelNotifyURL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	row := RefundOrder{RefundNo: "RF0000000000000000001", MerchantID: "M100002", Amount: 1000, Status: refundStatusInit}
	got, err := p.submitRefund(ctx, row)
	if err != nil || got.Status != refundStatusInit || got.RefundNo != row.RefundNo || channel.callCount() != 0 {
		t.Fatalf("got = %+v err=%v channel=%d", got, err, channel.callCount())
	}
}
