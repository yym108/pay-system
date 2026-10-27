//go:build integration

package biz

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
)

func payOrder(t *testing.T, env payEnv, merchantID, outTradeNo string, now time.Time) (*Payments, PaymentOrder) {
	t.Helper()
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	notify := "http://merchant-demo/notify"
	order, err := p.CreatePayment(env.ctx, &CreatePaymentInput{
		MerchantID: merchantID, OutTradeNo: outTradeNo, Amount: 10000, Subject: "demo", NotifyURL: &notify,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p, order
}

type storedTxn struct {
	count                           int
	channel, txnType, bizNo, status string
	txnNo                           sql.NullString
	amount                          int64
	notifyCount, queryCount         int
	nextQuery, created, updated     time.Time
	version                         uint64
}

func loadTxn(t *testing.T, env payEnv, bizNo string) storedTxn {
	t.Helper()
	var row storedTxn
	err := env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM channel_txn WHERE biz_no = ?`, bizNo).Scan(&row.count)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if row.count == 0 {
		return row
	}
	err = env.db.QueryRowContext(env.ctx, `
SELECT channel, txn_type, biz_no, channel_txn_no, amount, status,
       notify_count, query_count, next_query_at, version, created_at, updated_at
FROM channel_txn WHERE biz_no = ?`, bizNo).Scan(
		&row.channel, &row.txnType, &row.bizNo, &row.txnNo, &row.amount, &row.status,
		&row.notifyCount, &row.queryCount, &row.nextQuery, &row.version, &row.created, &row.updated,
	)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

// PAY-03 验收 1：同一订单并发调用 Pay 20 次，只插入 1 条 channel_txn，渠道只受理 1 次。
func TestPayConcurrentOneChannelTxn(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100050"
	now := feeAt(12, 0, 0, 0)
	p, order := payOrder(t, env, merchantID, "PAY03-CONCURRENT", now)
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH-CONCURRENT"}}
	p.UsePay(ch, UnimplementedPayResult{}, testChannelNotifyURL)
	const n = 20
	in := payReq(order)
	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			got, err := p.Pay(env.ctx, in)
			if err != nil {
				return err
			}
			if got.Status != payStatusPaying || got.PayNo != order.PayNo || got.Channel != channelMockpay {
				return errPayResult(got)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	if ch.n() != 1 {
		t.Fatalf("channel accepts = %d", ch.n())
	}
	if req := ch.got(); req.ReqNo != order.PayNo || req.Amount != order.Amount || req.NotifyURL != testChannelNotifyURL || req.Subject != order.Subject {
		t.Fatalf("channel request = %+v", req)
	}
	row := loadTxn(t, env, order.PayNo)
	if row.count != 1 || row.status != txnStatusProcessing || row.txnNo.String != "CH-CONCURRENT" || row.amount != 10000 {
		t.Fatalf("txn = %+v", row)
	}
}

// PAY-03 验收 2：渠道返回 OK：接口返回 PAYING，订单为 PAYING，channel_txn 为 PROCESSING 且已回填 channel_txn_no。
func TestPayChannelOKFillsTxnNo(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100051"
	now := feeAt(13, 0, 0, 0)
	p, order := payOrder(t, env, merchantID, "PAY03-OK", now)
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH-OK"}}
	p.UsePay(ch, UnimplementedPayResult{}, testChannelNotifyURL)
	got, err := p.Pay(env.ctx, payReq(order))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payStatusPaying || got.Channel != channelMockpay || got.Amount != 10000 {
		t.Fatalf("response = %+v", got)
	}
	stored := loadPayment(t, env, merchantID, "PAY03-OK")
	if stored.status != payStatusPaying || !stored.channel.Valid || stored.channel.String != channelMockpay || stored.version != 1 {
		t.Fatalf("order = %+v", stored)
	}
	if !stored.updatedAt.Equal(now) {
		t.Fatalf("updated_at = %s", stored.updatedAt)
	}
	row := loadTxn(t, env, order.PayNo)
	if row.count != 1 || row.channel != channelMockpay || row.txnType != txnTypePay || row.status != txnStatusProcessing ||
		!row.txnNo.Valid || row.txnNo.String != "CH-OK" || row.amount != int64(order.Amount) ||
		row.notifyCount != 0 || row.queryCount != 0 || row.version != 0 ||
		!row.nextQuery.Equal(now.Add(payNextQueryDelay)) || !row.created.Equal(now) || !row.updated.Equal(now) {
		t.Fatalf("txn = %+v", row)
	}
	if ch.n() != 1 {
		t.Fatalf("accepts = %d", ch.n())
	}
}

// PAY-03 验收 3：假渠道客户端超过调用超时仍不响应：接口返回 PAYING，订单仍为 PAYING，channel_txn 仍为 PROCESSING、channel_txn_no 为空。
func TestPayChannelTimeoutStaysPaying(t *testing.T) {
	if PayChannelTimeout != 3*time.Second || PayChannelDialTimeout != 500*time.Millisecond {
		t.Fatalf("production timeout=%s dial=%s", PayChannelTimeout, PayChannelDialTimeout)
	}
	env := openPay(t)
	const merchantID = "M100052"
	now := feeAt(14, 0, 0, 0)
	p, order := payOrder(t, env, merchantID, "PAY03-TIMEOUT", now)
	ch := &countingChannel{block: func(ctx context.Context) { <-ctx.Done() }}
	p.UsePay(ch, UnimplementedPayResult{}, testChannelNotifyURL)
	p.channelTimeout = 40 * time.Millisecond
	if p.payChannelTimeout() != 40*time.Millisecond || (&Payments{}).payChannelTimeout() != 3*time.Second {
		t.Fatal("timeout selection drifted")
	}
	type result struct {
		order PaymentOrder
		err   error
	}
	done := make(chan result, 1)
	go func() {
		got, err := p.Pay(env.ctx, payReq(order))
		done <- result{got, err}
	}()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	var res result
	select {
	case <-timer.C:
		t.Fatal("Pay waited for the production 3s timeout; the injected timeout was not on the call path")
	case res = <-done:
	}
	if res.err != nil || res.order.Status != payStatusPaying {
		t.Fatalf("response = %+v err=%v", res.order, res.err)
	}
	stored := loadPayment(t, env, merchantID, "PAY03-TIMEOUT")
	if stored.status != payStatusPaying || stored.version != 1 {
		t.Fatalf("order = %+v", stored)
	}
	row := loadTxn(t, env, order.PayNo)
	if row.count != 1 || row.status != txnStatusProcessing || row.txnNo.Valid || row.amount != 10000 ||
		!row.nextQuery.Equal(now.Add(10*time.Second)) || !row.created.Equal(now) {
		t.Fatalf("txn = %+v", row)
	}
	if ch.n() != 1 {
		t.Fatalf("accepts = %d", ch.n())
	}
}

func errPayResult(got PaymentOrder) error {
	return &payResultError{status: got.Status, payNo: got.PayNo, channel: got.Channel}
}

type payResultError struct {
	status, payNo, channel string
}

func (e *payResultError) Error() string {
	return "pay result " + e.payNo + " " + e.status + " " + e.channel
}
