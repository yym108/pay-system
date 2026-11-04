//go:build integration

package biz

import (
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
)

// REF-02 验收：MySQL 上正常退款为 PROCESSING，channel_txn 一条 REFUND，渠道受理 1 次。
func TestCreateRefundSubmitMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, "PA_REF_1", "OT_REF_1", payStatusSuccess, now)
	freezer := &fakeFreezer{status: freezeStatusFrozen}
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_RF_1"}}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_1", OutRefundNo: "R1", Amount: 1000,
	})
	if err != nil || got.Status != refundStatusProcessing || got.Amount != 1000 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if payRefundedAmount(t, env, "PA_REF_1") != 1000 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, "PA_REF_1"))
	}
	if channel.callCount() != 1 || channel.got().OrigReqNo != "PA_REF_1" || channel.got().ReqNo != got.RefundNo {
		t.Fatalf("channel = %+v calls=%d", channel.got(), channel.callCount())
	}
	var (
		txnType, status, txnNo, bizNo string
		amount                        int64
		nextQuery                     time.Time
	)
	err = env.db.QueryRowContext(env.ctx, `
SELECT txn_type, status, channel_txn_no, biz_no, amount, next_query_at
FROM channel_txn WHERE biz_no = ?`, got.RefundNo).Scan(&txnType, &status, &txnNo, &bizNo, &amount, &nextQuery)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if txnType != txnTypeRefund || status != txnStatusProcessing || txnNo != "CH_RF_1" || bizNo != got.RefundNo || amount != 1000 {
		t.Fatalf("txn type=%s status=%s no=%s biz=%s amount=%d", txnType, status, txnNo, bizNo, amount)
	}
	if !nextQuery.Equal(got.UpdatedAt.Add(payNextQueryDelay)) {
		t.Fatalf("next_query_at = %s updated = %s", nextQuery, got.UpdatedAt)
	}
	var n int
	if err = env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM channel_txn WHERE biz_no = ?`, got.RefundNo).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, _, _, freezeCalls := freezer.snapshot()
	if n != 1 || freezeCalls != 1 {
		t.Fatalf("rows=%d freeze=%d", n, freezeCalls)
	}
}

// REF-02 验收：Freeze 超时在 MySQL 上保持 INIT，没有 channel_txn。
func TestCreateRefundFreezeTimeoutMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, "PA_REF_1", "OT_REF_1", payStatusSuccess, now)
	channel := &fakeRefunder{}
	p.UseRefund(&fakeFreezer{err: errno.ErrUnavailable}, channel, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_1", OutRefundNo: "R1", Amount: 1000,
	})
	if err != nil || got.Status != refundStatusInit {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if payRefundedAmount(t, env, "PA_REF_1") != 1000 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, "PA_REF_1"))
	}
	var n int
	if err = env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM channel_txn`).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n != 0 || channel.callCount() != 0 {
		t.Fatalf("channel_txn=%d channel=%d", n, channel.callCount())
	}
}
