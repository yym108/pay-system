//go:build integration

package biz

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/kitex/client/callopt"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/sign"
	cbiz "github.com/yym108/pay-system/service/channel_mock/biz"         //nolint:depguard // GW-06 验收要打到真实渠道回调。生产代码不引用该包。
	cdal "github.com/yym108/pay-system/service/channel_mock/dal"         //nolint:depguard // 同上，只在本测试里读 mock_txn 并启动渠道服务。
	chandler "github.com/yym108/pay-system/service/channel_mock/handler" //nolint:depguard // 同上，进程内 HTTP 使用渠道 handler。
	"github.com/yym108/pay-system/service/gateway"                       //nolint:depguard // GW-06 验收要打到真实网关。生产代码不引用该包。
)

// gw06FinishedAt 是手写回调里的渠道完成时间，格式与网关解析一致。
const gw06FinishedAt = "2026-11-02T03:04:05.000+08:00"

// gwNotify 把网关回调接到本进程的 ApplyChannelNotify 与 ApplyRefundNotify。
// payment/biz 不能 import payment/handler，否则与 handler 对 biz 的引用成环。
// PAY、REFUND 分支的解析和错误转换与 handler.HandleChannelNotify 一致。
type gwNotify struct {
	p *Payments
}

func (g gwNotify) HandleChannelNotify(ctx context.Context, req *payment.HandleChannelNotifyRequest, _ ...callopt.Option) (*payment.HandleChannelNotifyResponse, error) {
	if g.p == nil {
		return nil, errno.ToBizStatus(errno.ErrInternal)
	}
	if req == nil || (req.TxnType != "PAY" && req.TxnType != "REFUND") {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	finished, err := time.Parse("2006-01-02T15:04:05.000Z07:00", req.FinishedAt)
	if err != nil || req.ReqNo == "" || (req.Status != "SUCCESS" && req.Status != "FAILED") {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	amt := money.Amount(req.Amount)
	var (
		kind    string
		callErr error
	)
	if req.TxnType == "REFUND" {
		kind, callErr = g.p.ApplyRefundNotify(ctx, RefundResult{
			RefundNo:     req.ReqNo,
			Result:       req.Status,
			Source:       paySourceNotify,
			ChannelTxnNo: req.ChannelTxnNo,
			Amount:       &amt,
			FinishedAt:   finished,
		})
	} else {
		kind, callErr = g.p.ApplyChannelNotify(ctx, PayResult{
			BizNo:        req.ReqNo,
			Result:       req.Status,
			Source:       paySourceNotify,
			ChannelTxnNo: req.ChannelTxnNo,
			Amount:       &amt,
			FinishedAt:   finished,
		})
	}
	if callErr != nil {
		_, rpcErr := errno.RPCExit(callErr, false)
		return nil, rpcErr
	}
	return &payment.HandleChannelNotifyResponse{Result_: kind}, nil
}

func startGateway(t *testing.T, p *Payments) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ln, err := gateway.Listen(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := gateway.NewEngine(ln)
	gateway.NewMerchant(
		gateway.WithClock(clock.Real{}),
		gateway.WithChannelKey([]byte(testChannelKey)),
		gateway.WithNotifyAPI(gwNotify{p: p}),
	).Register(srv)
	go func() { _ = srv.Run() }()
	waitListen(t, ln.Addr().String())
	t.Cleanup(func() {
		shut, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := srv.Shutdown(shut); err != nil {
			t.Errorf("shutdown gateway: %v", err)
		}
	})
	return "http://" + ln.Addr().String() + "/callback/mockpay"
}

func wireCallbackPay(t *testing.T, p *Payments, channelURL, callbackURL string) {
	t.Helper()
	client, err := NewChannelClient(ChannelClientConfig{
		BaseURL: channelURL, Key: []byte(testChannelKey), Clock: clock.Real{},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.UsePay(client, p, callbackURL)
}

func gw06Create(t *testing.T, env payEnv, merchantID, outTradeNo string, amount money.Amount) (*Payments, PaymentOrder) {
	t.Helper()
	p := openPayments(t, env, merchantID, 60, 60, feeAt(12, 0, 0, 0), "merchant-demo")
	notify := "http://merchant-demo/notify"
	order, err := p.CreatePayment(env.ctx, &CreatePaymentInput{
		MerchantID: merchantID, OutTradeNo: outTradeNo, Amount: amount, Subject: "demo", NotifyURL: &notify,
	})
	if err != nil {
		t.Fatal(err)
	}
	if order.Amount != amount {
		t.Fatalf("amount = %d", order.Amount)
	}
	return p, order
}

func postCallback(t *testing.T, url, reqNo, txnNo string, amount int64) (int, string) {
	t.Helper()
	body := []byte(`{"notify_id":"CN0000000123456789012","txn_type":"PAY","req_no":"` + reqNo + `","channel_txn_no":"` + txnNo + `","amount":` + strconv.FormatInt(amount, 10) + `,"status":"SUCCESS","finished_at":"` + gw06FinishedAt + `"}`)
	ts := strconv.FormatInt(clock.Real{}.Now().Unix(), 10)
	sig, err := sign.SignChannel([]byte(testChannelKey), sign.ChannelMessage{Timestamp: ts, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set(sign.HeaderMockTimestamp, ts)
	req.Header.Set(sign.HeaderMockSignature, sig)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw)
}

func waitUntil(t *testing.T, clk clock.Real, within time.Duration, ready func() bool) bool {
	t.Helper()
	deadline := clk.Now().Add(within)
	for {
		if ready() {
			return !clk.Now().After(deadline)
		}
		if clk.Now().After(deadline) {
			return false
		}
		timer := time.NewTimer(5 * time.Millisecond)
		<-timer.C
	}
}

func countBiz(t *testing.T, env payEnv, table, column, value string) int {
	t.Helper()
	var n int
	q := "SELECT COUNT(*) FROM " + table + " WHERE " + column + " = ?"
	if err := env.db.QueryRowContext(env.ctx, q, value).Scan(&n); err != nil { //nolint:gosec // 表名和列名只来自本文件的固定字面量。
		t.Fatal(testkit.Redact(err))
	}
	return n
}

// GW-06 验收：签名正确但 req_no 未知、金额不一致时返回 200 SUCCESS，订单不变，拒绝计数 +1。
func TestGW06RejectKeepsOrder(t *testing.T) {
	env := openPay(t)
	p, order := gw06Create(t, env, "M100080", "GW06-REJ", 10000)
	paying := acceptPaying(t, env.ctx, p, order, "MC-GW06-REJ")
	if paying.Status != payStatusPaying {
		t.Fatalf("status %s", paying.Status)
	}
	beforeOrder := loadPayment(t, env, "M100080", "GW06-REJ")
	beforeTxn := loadAppliedTxn(t, env, order.PayNo)
	gw := startGateway(t, p)

	unknown := "PA0000000000000000099"
	beforeMissing := gateway.CallbackRejected("30001")
	status, body := postCallback(t, gw, unknown, "MC-GW06-MISSING", 10000)
	if status != http.StatusOK || body != "SUCCESS" {
		t.Fatalf("unknown status %d body %q", status, body)
	}
	if gateway.CallbackRejected("30001") != beforeMissing+1 {
		t.Fatalf("30001 count %d", gateway.CallbackRejected("30001"))
	}
	if countBiz(t, env, "payment_order", "pay_no", unknown) != 0 || countBiz(t, env, "channel_txn", "biz_no", unknown) != 0 {
		t.Fatal("unknown req_no inserted a row")
	}
	if got := loadPayment(t, env, "M100080", "GW06-REJ"); got != beforeOrder {
		t.Fatalf("order changed after unknown\nbefore %+v\nafter  %+v", beforeOrder, got)
	}
	if got := loadAppliedTxn(t, env, order.PayNo); got != beforeTxn {
		t.Fatalf("txn changed after unknown\nbefore %+v\nafter  %+v", beforeTxn, got)
	}

	beforeMismatch := gateway.CallbackRejected("50002")
	status, body = postCallback(t, gw, order.PayNo, "MC-GW06-REJ", 1)
	if status != http.StatusOK || body != "SUCCESS" {
		t.Fatalf("mismatch status %d body %q", status, body)
	}
	if gateway.CallbackRejected("50002") != beforeMismatch+1 {
		t.Fatalf("50002 count %d", gateway.CallbackRejected("50002"))
	}
	if got := loadPayment(t, env, "M100080", "GW06-REJ"); got != beforeOrder {
		t.Fatalf("order changed after mismatch\nbefore %+v\nafter  %+v", beforeOrder, got)
	}
	gotTxn := loadAppliedTxn(t, env, order.PayNo)
	if gotTxn != beforeTxn || gotTxn.status != txnStatusProcessing || gotTxn.amount != 10000 || gotTxn.notifyCount != 0 {
		t.Fatalf("txn after mismatch %+v", gotTxn)
	}
	if rows := loadOutbox(t, env, order.PayNo); len(rows) != 0 {
		t.Fatalf("outbox %+v", rows)
	}
}

// listenChannel 启动 channel_mock。自动完成延迟固定为 50 毫秒，只用于本测试。
func listenChannel(t *testing.T) (base string, db *sqlx.DB, stop func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	schema, err := my.OpenSchema(ctx, "channel_mock")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer closeCancel()
		if err := schema.Close(closeCtx); err != nil {
			t.Errorf("close channel schema: %s", testkit.Redact(err))
		}
	})
	clk := clock.Real{}
	ids, err := idgen.New(4, clk)
	if err != nil {
		t.Fatal(err)
	}
	store := cdal.New(schema.DB)
	sched := cbiz.NewTimerScheduler(clk)
	notifier := cbiz.NewNotifier(store, ids, clk, sched, []byte(testChannelKey))
	svc := cbiz.New(store, ids, clk, sched, cbiz.Options{
		DelayMinMS: 50,
		DelayMaxMS: 50,
		OnFinished: notifier.Handle,
	})
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithExitWaitTime(time.Second),
		server.WithDisablePrintRoute(true),
	)
	chandler.New(svc, []byte(testChannelKey), clk).Register(srv)
	go func() { _ = srv.Run() }()
	waitListen(t, ln.Addr().String())
	t.Cleanup(func() {
		shut, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := srv.Shutdown(shut); err != nil {
			t.Errorf("shutdown channel http: %v", err)
		}
	})
	t.Cleanup(sched.Stop)
	return "http://" + ln.Addr().String(), schema.DB, sched.Stop
}

// GW-06 验收：Pay 返回 PAYING 后，channel_mock 自动完成并经回调，2 秒内订单变为 SUCCESS。
func TestGW06PaySucceedsWithinTwoSeconds(t *testing.T) {
	env := openPay(t)
	p, order := gw06Create(t, env, "M100081", "GW06-FAST", 10000)
	channelURL, mockDB, _ := listenChannel(t)
	callbackURL := startGateway(t, p)
	wireCallbackPay(t, p, channelURL, callbackURL)
	clk := clock.Real{}
	got, err := p.Pay(env.ctx, payReq(order))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payStatusPaying || got.PayNo != order.PayNo {
		t.Fatalf("pay = %+v", got)
	}
	ok := waitUntil(t, clk, 2*time.Second, func() bool {
		return loadPayment(t, env, "M100081", "GW06-FAST").status == payStatusSuccess
	})
	if !ok {
		t.Fatalf("not SUCCESS within 2s, order %+v txn %+v mock %+v",
			loadPayment(t, env, "M100081", "GW06-FAST"),
			loadAppliedTxn(t, env, order.PayNo),
			loadMockTxn(t, env.ctx, mockDB, order.PayNo))
	}
	stored := loadPayment(t, env, "M100081", "GW06-FAST")
	txn := loadAppliedTxn(t, env, order.PayNo)
	mock := loadMockTxn(t, env.ctx, mockDB, order.PayNo)
	if stored.count != 1 || stored.status != payStatusSuccess || stored.amount != 10000 || !stored.paidAt.Valid {
		t.Fatalf("order %+v", stored)
	}
	if txn.status != payStatusSuccess || txn.amount != stored.amount || txn.notifyCount != 1 || !txn.txnNo.Valid || txn.txnNo.String == "" {
		t.Fatalf("txn %+v", txn)
	}
	rows := loadOutbox(t, env, order.PayNo)
	if len(rows) != 1 || rows[0].eventType != "payment.succeeded" || rows[0].bizNo != order.PayNo {
		t.Fatalf("outbox %+v", rows)
	}
	if mock.count != 1 || mock.status != "SUCCESS" || mock.scenario != "NORMAL" || mock.notifySent != 1 || mock.amount != 10000 {
		t.Fatalf("mock %+v", mock)
	}
}

// GW-06 验收：DUP_NOTIFY 十次回调都得到 200 SUCCESS。订单只推进一次，outbox 恰好 1 条，notify_count=10。
func TestGW06DupNotifyTenCallbacks(t *testing.T) {
	env := openPay(t)
	const amount = money.Amount(10003)
	p, order := gw06Create(t, env, "M100082", "GW06-DUP", amount)
	channelURL, mockDB, _ := listenChannel(t)
	callbackURL := startGateway(t, p)
	wireCallbackPay(t, p, channelURL, callbackURL)
	got, err := p.Pay(env.ctx, payReq(order))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payStatusPaying || got.Amount != amount {
		t.Fatalf("pay = %+v", got)
	}
	clk := clock.Real{}
	ok := waitUntil(t, clk, 15*time.Second, func() bool {
		return loadAppliedTxn(t, env, order.PayNo).notifyCount == 10
	})
	if !ok {
		t.Fatalf("notify_count not 10, order %+v txn %+v mock %+v",
			loadPayment(t, env, "M100082", "GW06-DUP"),
			loadAppliedTxn(t, env, order.PayNo),
			loadMockTxn(t, env.ctx, mockDB, order.PayNo))
	}
	stored := loadPayment(t, env, "M100082", "GW06-DUP")
	txn := loadAppliedTxn(t, env, order.PayNo)
	mock := loadMockTxn(t, env.ctx, mockDB, order.PayNo)
	rows := loadOutbox(t, env, order.PayNo)
	if stored.count != 1 || stored.status != payStatusSuccess || stored.amount != 10003 {
		t.Fatalf("order %+v", stored)
	}
	if txn.status != payStatusSuccess || txn.amount != 10003 || txn.notifyCount != 10 {
		t.Fatalf("txn %+v", txn)
	}
	if len(rows) != 1 || rows[0].eventType != "payment.succeeded" || rows[0].bizNo != order.PayNo {
		t.Fatalf("outbox %+v", rows)
	}
	if mock.count != 1 || mock.scenario != "DUP_NOTIFY" || mock.notifySent != 10 || mock.status != "SUCCESS" || mock.amount != 10003 {
		t.Fatalf("mock %+v", mock)
	}
}
