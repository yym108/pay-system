//go:build integration

package biz

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

func fenPtr(n int64) *money.Amount {
	v := money.Amount(n)
	return &v
}

func payToPaying(t *testing.T, env payEnv, merchantID, outTradeNo string, now time.Time, txnNo string) (*Payments, PaymentOrder) {
	t.Helper()
	p, order := payOrder(t, env, merchantID, outTradeNo, now)
	got := acceptPaying(t, env.ctx, p, order, txnNo)
	return p, got
}

// 同一发号器和时钟再下一笔。另开 Payments 会在同一毫秒撞上相同 pay_no。
func payAnother(t *testing.T, env payEnv, p *Payments, merchantID, outTradeNo, txnNo string) PaymentOrder {
	t.Helper()
	notify := "http://merchant-demo/notify"
	order, err := p.CreatePayment(env.ctx, &CreatePaymentInput{
		MerchantID: merchantID, OutTradeNo: outTradeNo, Amount: 10000, Subject: "demo", NotifyURL: &notify,
	})
	if err != nil {
		t.Fatal(err)
	}
	return acceptPaying(t, env.ctx, p, order, txnNo)
}

func acceptPaying(t *testing.T, ctx context.Context, p *Payments, order PaymentOrder, txnNo string) PaymentOrder {
	t.Helper()
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: txnNo}}
	p.UsePay(ch, nil, testChannelNotifyURL)
	got, err := p.Pay(ctx, payReq(order))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payStatusPaying || got.PayNo != order.PayNo {
		t.Fatalf("paying = %+v", got)
	}
	return got
}

type outboxRow struct {
	eventType, topic, msgKey, bizNo, status, payload string
}

func loadOutbox(t *testing.T, env payEnv, bizNo string) []outboxRow {
	t.Helper()
	rows, err := env.db.QueryContext(env.ctx, `
SELECT event_type, topic, msg_key, biz_no, status, payload
FROM outbox WHERE biz_no = ? ORDER BY id`, bizNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	var got []outboxRow
	for rows.Next() {
		var row outboxRow
		if err = rows.Scan(&row.eventType, &row.topic, &row.msgKey, &row.bizNo, &row.status, &row.payload); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		got = append(got, row)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return got
}

type appliedTxn struct {
	status              string
	amount              int64
	notifyCount         int
	version             uint64
	txnNo, raw          sql.NullString
	finished, nextQuery sql.NullTime
	updated             time.Time
}

func loadAppliedTxn(t *testing.T, env payEnv, bizNo string) appliedTxn {
	t.Helper()
	var row appliedTxn
	err := env.db.QueryRowContext(env.ctx, `
SELECT status, amount, notify_count, version, channel_txn_no, channel_status_raw,
       channel_finished_at, next_query_at, updated_at
FROM channel_txn WHERE biz_no = ?`, bizNo).Scan(
		&row.status, &row.amount, &row.notifyCount, &row.version, &row.txnNo, &row.raw,
		&row.finished, &row.nextQuery, &row.updated,
	)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

func applySuccess(ctx context.Context, p *Payments, payNo string, finished time.Time, source string) (PaymentOrder, error) {
	return p.ApplyPayResult(ctx, PayResult{
		BizNo: payNo, Result: payResultSuccess, Source: source,
		Amount: fenPtr(10000), ChannelStatusRaw: "SUCCESS", FinishedAt: finished,
	})
}

// PAY-04 验收 1：同一笔 SUCCESS、source=NOTIFY 调用 10 次，其中 5 次并发。订单只推进一次，outbox 恰好 1 条，notify_count=10。
func TestApplyNotifyDuplicateOneOutbox(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100060"
	now := feeAt(12, 0, 0, 0)
	finished := feeAt(15, 4, 5, 123000000)
	p, order := payToPaying(t, env, merchantID, "PAY04-DUP", now, "CH-DUP")
	apply := func() error {
		got, err := applySuccess(env.ctx, p, order.PayNo, finished, paySourceNotify)
		if err != nil {
			return err
		}
		if got.Status != payStatusSuccess || got.PayNo != order.PayNo || got.Amount != 10000 {
			return errPayResult(got)
		}
		return nil
	}
	var g errgroup.Group
	for i := 0; i < 5; i++ {
		g.Go(apply)
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := apply(); err != nil {
			t.Fatal(err)
		}
	}
	stored := loadPayment(t, env, merchantID, "PAY04-DUP")
	if stored.count != 1 || stored.status != payStatusSuccess || stored.amount != 10000 || stored.version != 2 ||
		!stored.paidAt.Valid || !stored.paidAt.Time.Equal(finished) || stored.paidAt.Time.Equal(now) {
		t.Fatalf("order = %+v", stored)
	}
	txn := loadAppliedTxn(t, env, order.PayNo)
	if txn.status != payStatusSuccess || txn.amount != 10000 || txn.notifyCount != 10 || txn.version != 1 ||
		!txn.txnNo.Valid || txn.txnNo.String != "CH-DUP" || txn.nextQuery.Valid ||
		!txn.finished.Valid || !txn.finished.Time.Equal(finished) {
		t.Fatalf("txn = %+v", txn)
	}
	rows := loadOutbox(t, env, order.PayNo)
	if len(rows) != 1 || rows[0].eventType != "payment.succeeded" || rows[0].topic != "pay.payment.succeeded" ||
		rows[0].msgKey != order.PayNo || rows[0].bizNo != order.PayNo || rows[0].status != "PENDING" {
		t.Fatalf("outbox = %+v", rows)
	}
	var payload struct {
		EventType string `json:"event_type"`
		Data      struct {
			PayNo      string `json:"pay_no"`
			MerchantID string `json:"merchant_id"`
			OutTradeNo string `json:"out_trade_no"`
			Channel    string `json:"channel"`
			Amount     int64  `json:"amount"`
			FeeAmount  int64  `json:"fee_amount"`
			PaidAt     string `json:"paid_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(rows[0].payload), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.EventType != "payment.succeeded" || payload.Data.PayNo != order.PayNo ||
		payload.Data.MerchantID != merchantID || payload.Data.OutTradeNo != "PAY04-DUP" ||
		payload.Data.Channel != channelMockpay || payload.Data.Amount != 10000 || payload.Data.FeeAmount != 60 ||
		payload.Data.PaidAt != "2026-10-17T15:04:05.123+08:00" {
		t.Fatalf("payload = %+v", payload)
	}
}

// PAY-04 验收 2：假渠道返回 REJECTED，reason 为 ORDER_CLOSED。订单 FAILED，返回 30006 且带订单。
func TestPayRejectedBecomesFailed(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100061"
	now := feeAt(13, 0, 0, 0)
	p, order := payOrder(t, env, merchantID, "PAY04-REJ", now)
	ch := &countingChannel{result: ChannelAcceptResult{Outcome: acceptRejected, Reason: "ORDER_CLOSED"}}
	p.UsePay(ch, nil, testChannelNotifyURL)
	got, err := p.Pay(env.ctx, payReq(order))
	assertErrno(t, err, errno.ErrChannelRejected)
	if got.PayNo != order.PayNo || got.Status != payStatusFailed || got.FailCode != failCodeChannelRejected || got.FailMsg != "ORDER_CLOSED" {
		t.Fatalf("response = %+v", got)
	}
	stored := loadPayment(t, env, merchantID, "PAY04-REJ")
	if stored.status != payStatusFailed || !stored.failCode.Valid || stored.failCode.String != failCodeChannelRejected ||
		!stored.failMsg.Valid || stored.failMsg.String != "ORDER_CLOSED" || stored.paidAt.Valid {
		t.Fatalf("order = %+v", stored)
	}
	txn := loadAppliedTxn(t, env, order.PayNo)
	if txn.status != payStatusFailed || txn.notifyCount != 0 || txn.nextQuery.Valid || txn.amount != 10000 {
		t.Fatalf("txn = %+v", txn)
	}
	if rows := loadOutbox(t, env, order.PayNo); len(rows) != 0 {
		t.Fatalf("outbox = %+v", rows)
	}
	again, err := p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order.PayNo, Result: payResultFailed, Source: paySourceSync,
		FailCode: failCodeChannelRejected, FailMsg: "ORDER_CLOSED",
	})
	if err != nil || again.Status != payStatusFailed || again.PayNo != order.PayNo {
		t.Fatalf("idempotent apply = %+v err=%v", again, err)
	}
	if loadAppliedTxn(t, env, order.PayNo).notifyCount != 0 {
		t.Fatal("SYNC duplicate incremented notify_count")
	}
}

// PAY-04 验收 3：回调金额与本地不一致返回 50002，订单仍是 PAYING，流水仍是 PROCESSING。
func TestApplyAmountMismatch(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100062"
	now := feeAt(14, 0, 0, 0)
	p, order := payToPaying(t, env, merchantID, "PAY04-AMT", now, "CH-AMT")
	before := callbackConflicts(channelMockpay)
	_, err := p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order.PayNo, Result: payResultSuccess, Source: paySourceNotify,
		Amount: fenPtr(10001), FinishedAt: now.Add(time.Hour),
	})
	assertErrno(t, err, errno.ErrChannelAmountMismatch)
	stored := loadPayment(t, env, merchantID, "PAY04-AMT")
	if stored.status != payStatusPaying || stored.paidAt.Valid || stored.version != 1 {
		t.Fatalf("order = %+v", stored)
	}
	txn := loadAppliedTxn(t, env, order.PayNo)
	if txn.status != txnStatusProcessing || txn.amount != 10000 || txn.notifyCount != 0 || txn.nextQuery.Valid == false {
		t.Fatalf("txn = %+v", txn)
	}
	if rows := loadOutbox(t, env, order.PayNo); len(rows) != 0 {
		t.Fatalf("outbox = %+v", rows)
	}
	if callbackConflicts(channelMockpay) != before {
		t.Fatal("amount mismatch incremented the conflict counter")
	}

	// 已是终态时仍先比金额：不一致是 50002，不是冲突，也不改单。
	order2 := payAnother(t, env, p, merchantID, "PAY04-AMT-TERM", "CH-AMT2")
	if _, err = p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order2.PayNo, Result: payResultFailed, Source: paySourceQuery, FailCode: failCodeChannelFailed,
	}); err != nil {
		t.Fatal(err)
	}
	before = callbackConflicts(channelMockpay)
	_, err = p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order2.PayNo, Result: payResultSuccess, Source: paySourceNotify,
		Amount: fenPtr(1), FinishedAt: now.Add(time.Hour),
	})
	assertErrno(t, err, errno.ErrChannelAmountMismatch)
	if loadPayment(t, env, merchantID, "PAY04-AMT-TERM").status != payStatusFailed {
		t.Fatal("terminal order changed on amount mismatch")
	}
	if callbackConflicts(channelMockpay) != before {
		t.Fatal("terminal amount mismatch incremented the conflict counter")
	}
}

// PAY-04 验收 4：先 FAILED 再 SUCCESS，订单保持 FAILED，冲突计数 +1，outbox 仍是 0。
func TestApplyConflictKeepsFailed(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100063"
	now := feeAt(15, 0, 0, 0)
	p, order := payToPaying(t, env, merchantID, "PAY04-CONF", now, "CH-CONF")
	before := callbackConflicts(channelMockpay)
	failed, err := p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order.PayNo, Result: payResultFailed, Source: paySourceNotify, FailMsg: "declined",
	})
	if err != nil || failed.Status != payStatusFailed || failed.FailCode != failCodeChannelFailed || failed.FailMsg != "declined" {
		t.Fatalf("failed = %+v err=%v", failed, err)
	}
	got, err := p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order.PayNo, Result: payResultSuccess, Source: paySourceNotify,
		Amount: fenPtr(10000), FinishedAt: now.Add(time.Hour),
	})
	if err != nil || got.Status != payStatusFailed || got.FailCode != failCodeChannelFailed || got.PayNo != order.PayNo {
		t.Fatalf("conflict = %+v err=%v", got, err)
	}
	if callbackConflicts(channelMockpay) != before+1 {
		t.Fatalf("conflicts = %d, want %d", callbackConflicts(channelMockpay), before+1)
	}
	stored := loadPayment(t, env, merchantID, "PAY04-CONF")
	if stored.status != payStatusFailed || stored.paidAt.Valid {
		t.Fatalf("order = %+v", stored)
	}
	txn := loadAppliedTxn(t, env, order.PayNo)
	if txn.status != payStatusFailed || txn.notifyCount != 1 {
		t.Fatalf("txn = %+v", txn)
	}
	if rows := loadOutbox(t, env, order.PayNo); len(rows) != 0 {
		t.Fatalf("outbox = %+v", rows)
	}
}

// PAY-04 验收 5：人为把订单改离 PAYING 后调用，返回 10003，channel_txn 仍是 PROCESSING。
func TestApplyOrderCASRollsBack(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100064"
	now := feeAt(16, 0, 0, 0)
	p, order := payToPaying(t, env, merchantID, "PAY04-CAS", now, "CH-CAS")
	before := loadTxn(t, env, order.PayNo)
	if _, err := env.db.ExecContext(env.ctx, `
UPDATE payment_order SET status = 'CLOSED', updated_at = updated_at WHERE pay_no = ? AND status = 'PAYING'`, order.PayNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err := applySuccess(env.ctx, p, order.PayNo, now.Add(time.Minute), paySourceNotify)
	assertErrno(t, err, errno.ErrInternal)
	stored := loadPayment(t, env, merchantID, "PAY04-CAS")
	if stored.status != payStatusClosed || stored.paidAt.Valid {
		t.Fatalf("order = %+v", stored)
	}
	txn := loadTxn(t, env, order.PayNo)
	if txn.status != txnStatusProcessing || txn.version != before.version || txn.notifyCount != before.notifyCount ||
		!txn.nextQuery.Equal(before.nextQuery) || txn.txnNo.String != before.txnNo.String {
		t.Fatalf("txn = %+v before = %+v", txn, before)
	}
	if rows := loadOutbox(t, env, order.PayNo); len(rows) != 0 {
		t.Fatalf("outbox = %+v", rows)
	}
}

// PAY-04：SUCCESS 的 paid_at 等于传入的渠道完成时间，不是注入时钟。
func TestApplySuccessPaidAtUsesChannelTime(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100065"
	now := feeAt(17, 0, 0, 0)
	finished := feeAt(18, 30, 1, 456000000)
	p, order := payToPaying(t, env, merchantID, "PAY04-PAID", now, "CH-PAID")
	got, err := p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order.PayNo, Result: payResultSuccess, Source: paySourceQuery,
		Amount: fenPtr(10000), ChannelTxnNo: "CH-SHOULD-NOT-REPLACE", ChannelStatusRaw: "TRADE_SUCCESS",
		FinishedAt: finished,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.PaidAt.Equal(finished) || got.PaidAt.Equal(now) || !got.UpdatedAt.Equal(now) || got.Status != payStatusSuccess {
		t.Fatalf("order = %+v", got)
	}
	stored := loadPayment(t, env, merchantID, "PAY04-PAID")
	if !stored.paidAt.Valid || !stored.paidAt.Time.Equal(finished) || !stored.updatedAt.Equal(now) {
		t.Fatalf("stored = %+v", stored)
	}
	txn := loadAppliedTxn(t, env, order.PayNo)
	if txn.notifyCount != 0 || !txn.finished.Valid || !txn.finished.Time.Equal(finished) ||
		txn.txnNo.String != "CH-PAID" || !txn.raw.Valid || txn.raw.String != "TRADE_SUCCESS" || txn.nextQuery.Valid {
		t.Fatalf("txn = %+v", txn)
	}
}

// PAY-04：CLOSED 不写 outbox。传了完成时间就用它；没传则 closed_at 用注入时钟，渠道完成时间保持空。
func TestApplyClosedNoOutbox(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100066"
	now := feeAt(19, 0, 0, 0)
	finished := feeAt(19, 40, 0, 0)
	p, order := payToPaying(t, env, merchantID, "PAY04-CLOSE", now, "CH-CLOSE")
	got, err := p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order.PayNo, Result: payResultClosed, Source: paySourceClose, FinishedAt: finished,
	})
	if err != nil || got.Status != payStatusClosed || !got.ClosedAt.Equal(finished) || got.ClosedAt.Equal(now) {
		t.Fatalf("closed = %+v err=%v", got, err)
	}
	if rows := loadOutbox(t, env, order.PayNo); len(rows) != 0 {
		t.Fatalf("outbox = %+v", rows)
	}
	txn := loadAppliedTxn(t, env, order.PayNo)
	if txn.status != payStatusClosed || txn.notifyCount != 0 || !txn.finished.Valid || !txn.finished.Time.Equal(finished) {
		t.Fatalf("txn = %+v", txn)
	}

	order2 := payAnother(t, env, p, merchantID, "PAY04-CLOSE-CLOCK", "CH-CLOSE2")
	got, err = p.ApplyPayResult(env.ctx, PayResult{
		BizNo: order2.PayNo, Result: payResultClosed, Source: paySourceJob,
	})
	if err != nil || !got.ClosedAt.Equal(now) || got.Status != payStatusClosed {
		t.Fatalf("clock closed = %+v err=%v", got, err)
	}
	stored := loadPayment(t, env, merchantID, "PAY04-CLOSE-CLOCK")
	if !stored.closedAt.Valid || !stored.closedAt.Time.Equal(now) || stored.paidAt.Valid {
		t.Fatalf("stored = %+v", stored)
	}
	txn = loadAppliedTxn(t, env, order2.PayNo)
	if txn.status != payStatusClosed || txn.finished.Valid || txn.notifyCount != 0 {
		t.Fatalf("txn = %+v", txn)
	}
	if rows := loadOutbox(t, env, order2.PayNo); len(rows) != 0 {
		t.Fatalf("outbox = %+v", rows)
	}
}

// PAY-04：同一终态的 QUERY 不加 notify_count，不写第二条 outbox。
func TestApplyQueryDuplicateNoNotify(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100067"
	now := feeAt(20, 0, 0, 0)
	finished := feeAt(20, 10, 0, 0)
	p, order := payToPaying(t, env, merchantID, "PAY04-QUERY", now, "CH-QUERY")
	if _, err := applySuccess(env.ctx, p, order.PayNo, finished, paySourceNotify); err != nil {
		t.Fatal(err)
	}
	got, err := applySuccess(env.ctx, p, order.PayNo, finished, paySourceQuery)
	if err != nil || got.Status != payStatusSuccess || got.PayNo != order.PayNo {
		t.Fatalf("query = %+v err=%v", got, err)
	}
	txn := loadAppliedTxn(t, env, order.PayNo)
	if txn.notifyCount != 1 || txn.status != payStatusSuccess || txn.version != 1 {
		t.Fatalf("txn = %+v", txn)
	}
	rows := loadOutbox(t, env, order.PayNo)
	if len(rows) != 1 || rows[0].eventType != "payment.succeeded" {
		t.Fatalf("outbox = %+v", rows)
	}
}
