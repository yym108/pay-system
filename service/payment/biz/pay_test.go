package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/sign"
)

const testChannelNotifyURL = "http://gateway:8080/callback/mockpay"

func seedInit(t *testing.T) (*Payments, *memOrders, PaymentOrder) {
	t.Helper()
	p, _, orders, _, _ := newPayTest(t, "merchant-demo")
	order, err := p.CreatePayment(context.Background(), payInput(strPtr("http://merchant-demo/notify"), nil))
	if err != nil {
		t.Fatal(err)
	}
	return p, orders, order
}

func wirePay(p *Payments, ch ChannelAcceptor, applier PayResultApplier) {
	p.UsePay(ch, applier, testChannelNotifyURL)
}

// UnimplementedPayResult 只留给测试。不写订单，返回 10003。
type UnimplementedPayResult struct{}

func (UnimplementedPayResult) ApplyPayResult(context.Context, PayResult) (PaymentOrder, error) {
	return PaymentOrder{}, errno.ErrInternal
}

func payReq(order PaymentOrder) *PayInput {
	return &PayInput{MerchantID: order.MerchantID, PayNo: order.PayNo, Channel: channelMockpay}
}

type countingChannel struct {
	mu       sync.Mutex
	calls    int
	last     ChannelAcceptRequest
	result   ChannelAcceptResult
	err      error
	block    func(context.Context)
	onAccept func()
}

func (c *countingChannel) Accept(ctx context.Context, req ChannelAcceptRequest) (ChannelAcceptResult, error) {
	c.mu.Lock()
	c.calls++
	c.last = req
	res, err, block, hook := c.result, c.err, c.block, c.onAccept
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	if block != nil {
		block(ctx)
	}
	return res, err
}

func (c *countingChannel) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *countingChannel) got() ChannelAcceptRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

type recordingApplier struct {
	in    PayResult
	order PaymentOrder
	err   error
	calls int
}

func (a *recordingApplier) ApplyPayResult(_ context.Context, in PayResult) (PaymentOrder, error) {
	a.calls++
	a.in = in
	return a.order, a.err
}

func setRow(orders *memOrders, fn func(*PaymentOrder)) {
	orders.mu.Lock()
	defer orders.mu.Unlock()
	fn(&orders.rows[0])
}

// PAY-03：请求为 nil 是 10001；channel 不是恰好 mockpay 是 30007，且不读库。
func TestPayNilAndUnsupportedChannel(t *testing.T) {
	ctx := context.Background()
	var bare *Payments
	if _, err := bare.Pay(ctx, nil); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("nil receiver nil request = %v", err)
	}
	if _, err := bare.Pay(ctx, &PayInput{Channel: "alipay"}); !errors.Is(err, errno.ErrChannelUnsupported) {
		t.Fatalf("nil receiver bad channel = %v", err)
	}

	p, orders, _ := seedInit(t)
	for _, channel := range []string{"", "MockPay", "mockpay ", "alipay"} {
		_, err := p.Pay(ctx, &PayInput{MerchantID: "M100002", PayNo: "PA0000000000000000001", Channel: channel})
		assertErrno(t, err, errno.ErrChannelUnsupported)
	}
	if orders.payNoReads != 0 {
		t.Fatalf("unsupported channel read pay_no %d times", orders.payNoReads)
	}
	if orders.rows[0].Status != payStatusInit {
		t.Fatalf("status = %s", orders.rows[0].Status)
	}
}

// PAY-03：expire_at == now+30s 为 30004，多 1 毫秒进入 PAYING。
func TestPayExpireBoundary(t *testing.T) {
	p, orders, order := seedInit(t)
	now := p.clk.Now().Truncate(time.Millisecond)
	setRow(orders, func(row *PaymentOrder) { row.ExpireAt = now.Add(payExpireMargin) })
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CTX-BOUNDARY"}}
	wirePay(p, ch, nil)
	if _, err := p.Pay(context.Background(), payReq(order)); !errors.Is(err, errno.ErrOrderExpired) {
		t.Fatalf("equal margin err = %v", err)
	}
	if ch.n() != 0 || orders.rows[0].Status != payStatusInit || len(orders.txns) != 0 {
		t.Fatalf("expired pay changed state calls=%d status=%s txns=%d", ch.n(), orders.rows[0].Status, len(orders.txns))
	}

	setRow(orders, func(row *PaymentOrder) { row.ExpireAt = now.Add(payExpireMargin + time.Millisecond) })
	got, err := p.Pay(context.Background(), payReq(order))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payStatusPaying || got.Channel != channelMockpay || got.Version != 1 || ch.n() != 1 {
		t.Fatalf("order=%+v calls=%d", got, ch.n())
	}
	if len(orders.txns) != 1 || orders.txns[0].ChannelTxnNo != "CTX-BOUNDARY" || orders.txns[0].Amount != order.Amount {
		t.Fatalf("txn = %+v", orders.txns)
	}
	if !orders.txns[0].NextQueryAt.Equal(now.Add(payNextQueryDelay)) || !orders.txns[0].CreatedAt.Equal(now) {
		t.Fatalf("txn times next=%s created=%s", orders.txns[0].NextQueryAt, orders.txns[0].CreatedAt)
	}
	req := ch.got()
	if req.ReqNo != order.PayNo || req.Amount != order.Amount || req.Subject != order.Subject || req.NotifyURL != testChannelNotifyURL {
		t.Fatalf("channel request = %+v", req)
	}
	if req.NotifyURL == order.NotifyURL {
		t.Fatal("channel notify url used the merchant url")
	}
}

// PAY-03：其他商户与不存在的 pay_no 都是 30001，订单保持 INIT。
func TestPayOtherMerchantAndMissing(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CTX"}}
	wirePay(p, ch, nil)
	ctx := context.Background()
	for _, merchantID := range []string{"M100099", ""} {
		_, err := p.Pay(ctx, &PayInput{MerchantID: merchantID, PayNo: order.PayNo, Channel: channelMockpay})
		assertErrno(t, err, errno.ErrOrderNotFound)
	}
	_, err := p.Pay(ctx, &PayInput{MerchantID: order.MerchantID, PayNo: "PA-missing", Channel: channelMockpay})
	assertErrno(t, err, errno.ErrOrderNotFound)
	if ch.n() != 0 || orders.rows[0].Status != payStatusInit || len(orders.txns) != 0 {
		t.Fatalf("leaked or mutated calls=%d status=%s", ch.n(), orders.rows[0].Status)
	}
}

// PAY-03：PAYING 再调一次不增加受理次数，也不再插入 channel_txn。
func TestPayPayingDoesNotAcceptAgain(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CTX-1"}}
	wirePay(p, ch, nil)
	first, err := p.Pay(context.Background(), payReq(order))
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Pay(context.Background(), payReq(order))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != payStatusPaying || second.PayNo != first.PayNo || second.Status != payStatusPaying {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if ch.n() != 1 || len(orders.txns) != 1 || orders.txns[0].ChannelTxnNo != "CTX-1" {
		t.Fatalf("calls=%d txns=%+v", ch.n(), orders.txns)
	}
}

func TestPayTerminalAndSuccess(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CTX"}}
	wirePay(p, ch, nil)
	for _, status := range []string{payStatusFailed, payStatusClosed} {
		setRow(orders, func(row *PaymentOrder) { row.Status = status })
		got, err := p.Pay(context.Background(), payReq(order))
		assertErrno(t, err, errno.ErrOrderStatusConflict)
		if got != (PaymentOrder{}) {
			t.Fatalf("%s carried order %+v", status, got)
		}
	}
	setRow(orders, func(row *PaymentOrder) { row.Status = payStatusSuccess })
	got, err := p.Pay(context.Background(), payReq(order))
	if err != nil || got.Status != payStatusSuccess {
		t.Fatalf("success = %+v err=%v", got, err)
	}
	if ch.n() != 0 || len(orders.txns) != 0 {
		t.Fatalf("terminal called channel calls=%d txns=%d", ch.n(), len(orders.txns))
	}
}

func TestPayCASMissStillInit(t *testing.T) {
	p, orders, order := seedInit(t)
	orders.forceMiss = true
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CTX"}}
	wirePay(p, ch, nil)
	got, err := p.Pay(context.Background(), payReq(order))
	if err != nil || got.Status != payStatusInit || ch.n() != 0 || len(orders.txns) != 0 {
		t.Fatalf("got=%+v err=%v calls=%d txns=%d", got, err, ch.n(), len(orders.txns))
	}
}

func TestPayCASMissRereadsPaying(t *testing.T) {
	p, orders, order := seedInit(t)
	orders.beforeCAS = func() {
		orders.mu.Lock()
		orders.rows[0].Status = payStatusPaying
		orders.rows[0].Channel = channelMockpay
		orders.mu.Unlock()
	}
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CTX"}}
	wirePay(p, ch, nil)
	got, err := p.Pay(context.Background(), payReq(order))
	if err != nil || got.Status != payStatusPaying || ch.n() != 0 || len(orders.txns) != 0 {
		t.Fatalf("got=%+v err=%v calls=%d txns=%d", got, err, ch.n(), len(orders.txns))
	}
}

func TestPayUnknownAndEmptyTxnNo(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingChannel{err: errors.New("dial tcp: connection refused")}
	wirePay(p, ch, nil)
	got, err := p.Pay(context.Background(), payReq(order))
	if err != nil || got.Status != payStatusPaying || len(orders.txns) != 1 || orders.txns[0].ChannelTxnNo != "" {
		t.Fatalf("unknown got=%+v err=%v txn=%+v", got, err, orders.txns)
	}

	p2, orders2, order2 := seedInit(t)
	ch2 := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK}}
	wirePay(p2, ch2, nil)
	got, err = p2.Pay(context.Background(), payReq(order2))
	if err != nil || got.Status != payStatusPaying || orders2.txns[0].ChannelTxnNo != "" {
		t.Fatalf("empty txn no got=%+v err=%v txn=%+v", got, err, orders2.txns)
	}
}

func TestPayDoesNotOverwriteTxnNo(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingChannel{
		result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "NEW"},
		onAccept: func() {
			orders.mu.Lock()
			orders.txns[0].ChannelTxnNo = "OLD"
			orders.mu.Unlock()
		},
	}
	wirePay(p, ch, nil)
	got, err := p.Pay(context.Background(), payReq(order))
	if err != nil || got.Status != payStatusPaying || orders.txns[0].ChannelTxnNo != "OLD" {
		t.Fatalf("got=%+v err=%v txn=%+v", got, err, orders.txns)
	}
}

func TestPayDuplicateTxnNoIsInternal(t *testing.T) {
	p, orders, order := seedInit(t)
	orders.txns = append(orders.txns, channelTxn{Channel: channelMockpay, BizNo: "OTHER", ChannelTxnNo: "CTX-DUP"})
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CTX-DUP"}}
	wirePay(p, ch, nil)
	_, err := p.Pay(context.Background(), payReq(order))
	assertErrno(t, err, errno.ErrInternal)
	if orders.rows[0].Status != payStatusPaying {
		t.Fatalf("status = %s", orders.rows[0].Status)
	}
	var ours string
	for _, txn := range orders.txns {
		if txn.BizNo == order.PayNo {
			ours = txn.ChannelTxnNo
		}
	}
	if ours != "" {
		t.Fatalf("overwrote txn no %q", ours)
	}
}

// PAY-03：假适配器收到 REJECTED 结构体。适配器返回的错误原样带出。
// 适配器成功写成 FAILED 且 error 为 nil 时，Pay 补上 30006。测试用空实现不改单并返回 10003。
func TestPayRejectedAdapter(t *testing.T) {
	p, orders, order := seedInit(t)
	failed := order
	failed.Status = payStatusFailed
	failed.FailCode = failCodeChannelRejected
	applier := &recordingApplier{order: failed, err: errno.ErrChannelRejected}
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptRejected, Reason: "ORDER_CLOSED"}}
	wirePay(p, ch, applier)
	got, err := p.Pay(context.Background(), payReq(order))
	assertErrno(t, err, errno.ErrChannelRejected)
	if got.Status != payStatusFailed || got.FailCode != failCodeChannelRejected {
		t.Fatalf("passed order = %+v", got)
	}
	if applier.calls != 1 || applier.in.BizNo != order.PayNo || applier.in.Result != payResultFailed ||
		applier.in.FailCode != failCodeChannelRejected || applier.in.FailMsg != "ORDER_CLOSED" ||
		applier.in.Source != paySourceSync || applier.in.Amount != nil {
		t.Fatalf("applier = %+v", applier)
	}
	if orders.rows[0].Status != payStatusPaying || orders.rows[0].FailCode != "" {
		t.Fatalf("store was changed by Pay itself: %+v", orders.rows[0])
	}

	p3, _, order3 := seedInit(t)
	failed3 := order3
	failed3.Status = payStatusFailed
	failed3.FailCode = failCodeChannelRejected
	failed3.FailMsg = "ORDER_CLOSED"
	applier3 := &recordingApplier{order: failed3}
	ch3 := &countingChannel{result: ChannelAcceptResult{Outcome: acceptRejected, Reason: "ORDER_CLOSED"}}
	wirePay(p3, ch3, applier3)
	got, err = p3.Pay(context.Background(), payReq(order3))
	assertErrno(t, err, errno.ErrChannelRejected)
	if got.PayNo != order3.PayNo || got.Status != payStatusFailed || got.FailMsg != "ORDER_CLOSED" {
		t.Fatalf("wrapped order = %+v", got)
	}
	if applier3.in.Amount != nil || applier3.in.Source != paySourceSync {
		t.Fatalf("applier = %+v", applier3.in)
	}

	p2, orders2, order2 := seedInit(t)
	ch2 := &countingChannel{result: ChannelAcceptResult{Outcome: acceptRejected, Reason: "ORDER_CLOSED"}}
	wirePay(p2, ch2, UnimplementedPayResult{})
	_, err = p2.Pay(context.Background(), payReq(order2))
	assertErrno(t, err, errno.ErrInternal)
	if orders2.rows[0].Status != payStatusPaying || orders2.rows[0].FailCode != "" || orders2.txns[0].Status != txnStatusProcessing {
		t.Fatalf("unimplemented adapter changed row=%+v txn=%+v", orders2.rows[0], orders2.txns)
	}
}

func TestPayNilChannelBeforeCAS(t *testing.T) {
	p, orders, order := seedInit(t)
	_, err := p.Pay(context.Background(), payReq(order))
	assertErrno(t, err, errno.ErrInternal)
	if orders.rows[0].Status != payStatusInit || len(orders.txns) != 0 {
		t.Fatalf("nil channel dirtied status=%s txns=%d", orders.rows[0].Status, len(orders.txns))
	}
}

func TestPayEmptyKeyAfterCAS(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingChannel{err: sign.ErrEmptyKey}
	wirePay(p, ch, nil)
	_, err := p.Pay(context.Background(), payReq(order))
	assertErrno(t, err, errno.ErrInternal)
	if orders.rows[0].Status != payStatusPaying || orders.txns[0].ChannelTxnNo != "" {
		t.Fatalf("row=%+v txn=%+v", orders.rows[0], orders.txns)
	}
}

func TestPayFaultPointAfterCommit(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CTX-FAULT"}}
	var sawPaying bool
	var callsAtPoint int
	if err := fault.Init("pay.after_tx1", fault.WithKill(func() {
		orders.mu.Lock()
		sawPaying = len(orders.rows) == 1 && orders.rows[0].Status == payStatusPaying && len(orders.txns) == 1
		orders.mu.Unlock()
		callsAtPoint = ch.n()
	})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fault.Init(""); err != nil {
			t.Errorf("clear fault: %v", err)
		}
	})
	wirePay(p, ch, nil)
	got, err := p.Pay(context.Background(), payReq(order))
	if err != nil || got.Status != payStatusPaying {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if !sawPaying || callsAtPoint != 0 || ch.n() != 1 {
		t.Fatalf("sawPaying=%v callsAtPoint=%d calls=%d", sawPaying, callsAtPoint, ch.n())
	}
}

func TestPayChannelTimeoutSelection(t *testing.T) {
	if PayChannelTimeout != 3*time.Second || PayChannelDialTimeout != 500*time.Millisecond {
		t.Fatalf("production timeout=%s dial=%s", PayChannelTimeout, PayChannelDialTimeout)
	}
	if (&Payments{}).payChannelTimeout() != PayChannelTimeout {
		t.Fatal("zero field must use the production timeout")
	}
	p := &Payments{channelTimeout: 40 * time.Millisecond}
	if p.payChannelTimeout() != 40*time.Millisecond {
		t.Fatalf("injected = %s", p.payChannelTimeout())
	}
	var hasDeadline bool
	ch := &countingChannel{block: func(ctx context.Context) {
		_, hasDeadline = ctx.Deadline()
	}}
	wired, _, order := seedInit(t)
	wirePay(wired, ch, nil)
	if _, err := wired.Pay(context.Background(), payReq(order)); err != nil || !hasDeadline {
		t.Fatalf("err=%v deadline=%v", err, hasDeadline)
	}
}

func TestAppPayNil(t *testing.T) {
	var app *App
	_, err := app.Pay(context.Background(), &PayInput{Channel: channelMockpay})
	assertErrno(t, err, errno.ErrInternal)
	_, err = (&App{}).Pay(context.Background(), &PayInput{Channel: channelMockpay})
	assertErrno(t, err, errno.ErrInternal)
}
