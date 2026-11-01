//go:build integration

package biz

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/outbox"
	cbiz "github.com/yym108/pay-system/service/channel_mock/biz"         //nolint:depguard // 验收要打到真实关单实现。生产代码不引用该包。
	cdal "github.com/yym108/pay-system/service/channel_mock/dal"         //nolint:depguard // 同上，只在本测试里读 mock_txn 并启动渠道服务。
	chandler "github.com/yym108/pay-system/service/channel_mock/handler" //nolint:depguard // 同上，进程内 HTTP 使用渠道 handler。
)

// 关单场景由金额末两位决定：08 是 NEVER_FINISH，09 是 CLOSE_RACE。
const (
	amountNeverFinish = money.Amount(10008)
	amountCloseRace   = money.Amount(10009)
)

type mockHTTP struct {
	url   string
	db    *sqlx.DB
	hooks *atomic.Int32
	sched *cbiz.TimerScheduler
}

type mockTxnRow struct {
	count      int
	status     string
	scenario   string
	notifySent int
	amount     int64
}

func startMockHTTP(t *testing.T, clk *clock.Fake) *mockHTTP {
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
	ids, err := idgen.New(3, clk)
	if err != nil {
		t.Fatal(err)
	}
	env := &mockHTTP{db: schema.DB, hooks: &atomic.Int32{}}
	env.sched = cbiz.NewTimerScheduler(clk)
	t.Cleanup(env.sched.Stop)
	svc := cbiz.New(cdal.New(schema.DB), ids, clk, env.sched, cbiz.Options{
		DelayMinMS: 600000,
		DelayMaxMS: 600000,
		OnFinished: func(context.Context, cdal.Txn) { env.hooks.Add(1) },
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
	env.url = "http://" + ln.Addr().String()
	return env
}

func waitListen(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var last error
	for ctx.Err() == nil {
		dialer := &net.Dialer{Timeout: 50 * time.Millisecond}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		last = err
	}
	t.Fatalf("channel http not listening: %v", last)
}

func wireCloseChannel(t *testing.T, p *Payments, rawURL string, clk *clock.Fake) {
	t.Helper()
	client, err := NewChannelClient(ChannelClientConfig{BaseURL: rawURL, Key: []byte(testChannelKey), Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	p.UsePay(client, p, testChannelNotifyURL)
	p.UseClose(client)
}

func newClosePayments(t *testing.T, env payEnv, merchantID string, now time.Time, clk *clock.Fake) *Payments {
	t.Helper()
	insertMerchant(t, env, merchantID, 60, "ACTIVE")
	if _, err := env.svc.AppendFeeRate(env.ctx, merchantID, 60, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	ids, err := idgen.New(7, clk)
	if err != nil {
		t.Fatal(err)
	}
	return NewPayments(NewMerchants(env.db, nil, clk, 0), ids, clk, "merchant-demo", env.db)
}

func createCloseOrder(t *testing.T, env payEnv, p *Payments, merchantID, outTradeNo string, amount money.Amount) PaymentOrder {
	t.Helper()
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
	return order
}

func loadMockTxn(t *testing.T, ctx context.Context, db *sqlx.DB, reqNo string) mockTxnRow {
	t.Helper()
	var row mockTxnRow
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mock_txn WHERE txn_type = 'PAY' AND req_no = ?`, reqNo).Scan(&row.count)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if row.count == 0 {
		return row
	}
	err = db.QueryRowContext(ctx, `
SELECT status, scenario, notify_sent_count, amount
FROM mock_txn WHERE txn_type = 'PAY' AND req_no = ?`, reqNo).Scan(&row.status, &row.scenario, &row.notifySent, &row.amount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

func loadLocalTxnStatus(t *testing.T, env payEnv, bizNo string) (int, string) {
	t.Helper()
	var count int
	err := env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM channel_txn WHERE biz_no = ?`, bizNo).Scan(&count)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if count == 0 {
		return 0, ""
	}
	var status string
	err = env.db.QueryRowContext(env.ctx, `SELECT status FROM channel_txn WHERE biz_no = ?`, bizNo).Scan(&status)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return count, status
}

func ignoreConcurrentClose(err error) error {
	if err == nil || errors.Is(err, errno.ErrOrderStatusConflict) || errors.Is(err, errno.ErrUnavailable) || errors.Is(err, errno.ErrChannelRejected) {
		return nil
	}
	return err
}

// PAY-06 验收 1：NEVER_FINISH 关单后订单 CLOSED，渠道 mock_txn 为 CLOSED，之后不再有回调。
func TestCloseNeverFinishClosesChannelTxn(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100060"
	now := feeAt(9, 0, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	order := createCloseOrder(t, env, p, merchantID, "PAY06-NEVER", amountNeverFinish)
	paid, err := p.Pay(env.ctx, payReq(order))
	if err != nil || paid.Status != payStatusPaying {
		t.Fatalf("pay = %+v err=%v", paid, err)
	}
	closed, err := p.ClosePayment(env.ctx, closeReq(order))
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != payStatusClosed || closed.Amount != amountNeverFinish || !closed.ClosedAt.Equal(now) {
		t.Fatalf("close = %+v", closed)
	}
	stored := loadPayment(t, env, merchantID, "PAY06-NEVER")
	if stored.status != payStatusClosed || !stored.closedAt.Valid || !stored.closedAt.Time.Equal(now) || stored.amount != int64(amountNeverFinish) {
		t.Fatalf("order = %+v", stored)
	}
	row := loadMockTxn(t, env.ctx, mock.db, order.PayNo)
	if row.count != 1 || row.status != payStatusClosed || row.scenario != "NEVER_FINISH" || row.notifySent != 0 || row.amount != int64(amountNeverFinish) {
		t.Fatalf("mock = %+v", row)
	}
	if count, status := loadLocalTxnStatus(t, env, order.PayNo); count != 1 || status != payStatusClosed {
		t.Fatalf("channel_txn count=%d status=%s", count, status)
	}
	if mock.hooks.Load() != 0 || mock.sched.Pending() != 0 {
		t.Fatalf("hooks=%d pending=%d", mock.hooks.Load(), mock.sched.Pending())
	}
	if n := len(loadOutbox(t, env, order.PayNo)); n != 0 {
		t.Fatalf("outbox = %d", n)
	}
}

// PAY-06 验收 2：CLOSE_RACE 关单返回 30003，订单 SUCCESS，outbox 恰好 1 条 payment.succeeded。
func TestCloseRaceReturnsConflictAndOneSucceededEvent(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100061"
	now := feeAt(9, 30, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	order := createCloseOrder(t, env, p, merchantID, "PAY06-RACE", amountCloseRace)
	paid, err := p.Pay(env.ctx, payReq(order))
	if err != nil || paid.Status != payStatusPaying {
		t.Fatalf("pay = %+v err=%v", paid, err)
	}
	got, err := p.ClosePayment(env.ctx, closeReq(order))
	if !errors.Is(err, errno.ErrOrderStatusConflict) || got.Status != payStatusSuccess || got.Amount != amountCloseRace || got.PaidAt.IsZero() {
		t.Fatalf("close = %+v err=%v", got, err)
	}
	stored := loadPayment(t, env, merchantID, "PAY06-RACE")
	if stored.status != payStatusSuccess || !stored.paidAt.Valid || stored.amount != int64(amountCloseRace) {
		t.Fatalf("order = %+v", stored)
	}
	row := loadMockTxn(t, env.ctx, mock.db, order.PayNo)
	if row.count != 1 || row.status != payStatusSuccess || row.scenario != "CLOSE_RACE" || row.amount != int64(amountCloseRace) {
		t.Fatalf("mock = %+v", row)
	}
	if count, status := loadLocalTxnStatus(t, env, order.PayNo); count != 1 || status != payStatusSuccess {
		t.Fatalf("channel_txn count=%d status=%s", count, status)
	}
	events := loadOutbox(t, env, order.PayNo)
	if len(events) != 1 || events[0].eventType != outbox.EventPaymentSucceeded {
		t.Fatalf("outbox = %+v", events)
	}
	if mock.hooks.Load() != 1 {
		t.Fatalf("hooks = %d", mock.hooks.Load())
	}
}

// PAY-06 验收 3：INIT 上并发 Pay 与 Close，最终只能是 PAYING 或 CLOSED。
func TestCloseConcurrentWithPayStaysPayingOrClosed(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100062"
	now := feeAt(10, 0, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	order := createCloseOrder(t, env, p, merchantID, "PAY06-RACE-INIT", amountNeverFinish)
	start := make(chan struct{})
	var g errgroup.Group
	g.Go(func() error {
		<-start
		_, err := p.Pay(env.ctx, payReq(order))
		return ignoreConcurrentClose(err)
	})
	g.Go(func() error {
		<-start
		_, err := p.ClosePayment(env.ctx, closeReq(order))
		return ignoreConcurrentClose(err)
	})
	close(start)
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	stored := loadPayment(t, env, merchantID, "PAY06-RACE-INIT")
	row := loadMockTxn(t, env.ctx, mock.db, order.PayNo)
	count, status := loadLocalTxnStatus(t, env, order.PayNo)
	switch stored.status {
	case payStatusPaying:
		if row.count != 1 || row.status != "PROCESSING" || row.scenario != "NEVER_FINISH" || row.amount == 0 {
			t.Fatalf("paying mock = %+v", row)
		}
		if count != 1 || status != txnStatusProcessing {
			t.Fatalf("paying channel_txn count=%d status=%s", count, status)
		}
	case payStatusClosed:
		if row.count != 0 && (row.status != payStatusClosed || row.amount == 0) {
			t.Fatalf("order closed but mock = %+v", row)
		}
		if count != 0 && status != payStatusClosed {
			t.Fatalf("closed channel_txn count=%d status=%s", count, status)
		}
	default:
		t.Fatalf("status = %s mock=%+v local=%d %s", stored.status, row, count, status)
	}
}

// PAY-06：Pay 停在 pay.after_tx1、渠道尚未写入 mock_txn 时关单。
// 查无此单返回 10004，不插入 amount 0 占位。Pay 继续受理后订单不是 FAILED。
func TestCloseAtPayAfterTx1ReturnsUnavailableWithoutTombstone(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100063"
	now := feeAt(10, 30, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	order := createCloseOrder(t, env, p, merchantID, "PAY06-AFTER-TX1", amountNeverFinish)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePay := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releasePay)
	if err := fault.Init(faultPayAfterTx1, fault.WithKill(func() {
		close(entered)
		<-release
	})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fault.Init(""); err != nil {
			t.Errorf("clear fault: %v", err)
		}
	})

	type payResult struct {
		order PaymentOrder
		err   error
	}
	done := make(chan payResult, 1)
	go func() {
		got, err := p.Pay(env.ctx, payReq(order))
		done <- payResult{order: got, err: err}
	}()
	select {
	case <-entered:
	case res := <-done:
		t.Fatalf("pay finished before fault: %+v err=%v", res.order, res.err)
	case <-env.ctx.Done():
		t.Fatal(env.ctx.Err())
	}

	closed, closeErr := p.ClosePayment(env.ctx, closeReq(order))
	before := loadMockTxn(t, env.ctx, mock.db, order.PayNo)
	releasePay()
	res := <-done

	if !errors.Is(closeErr, errno.ErrUnavailable) || closed.PayNo != "" {
		t.Fatalf("close = %+v err=%v", closed, closeErr)
	}
	if before.count != 0 || before.amount != 0 {
		t.Fatalf("close inserted mock = %+v", before)
	}
	if errors.Is(res.err, errno.ErrChannelRejected) || res.err != nil || res.order.Status != payStatusPaying {
		t.Fatalf("pay = %+v err=%v", res.order, res.err)
	}
	stored := loadPayment(t, env, merchantID, "PAY06-AFTER-TX1")
	if stored.status != payStatusPaying || (stored.failCode.Valid && stored.failCode.String == failCodeChannelRejected) {
		t.Fatalf("order = %+v", stored)
	}
	row := loadMockTxn(t, env.ctx, mock.db, order.PayNo)
	if row.count != 1 || row.amount == 0 || row.amount != int64(amountNeverFinish) || row.status != txnStatusProcessing {
		t.Fatalf("mock = %+v", row)
	}
}
