//go:build integration

package biz

import (
	"context"
	"testing"
	"time"

	accountbiz "github.com/yym108/pay-system/service/account/biz"
	accountdal "github.com/yym108/pay-system/service/account/dal"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

// accountBizFreezer 在测试里直接调用 account 的 Freeze，不起 Kitex。
type accountBizFreezer struct {
	svc *accountbiz.Service
}

func (a accountBizFreezer) FreezeRefund(ctx context.Context, merchantID, refundNo string, amount money.Amount) (RefundFreeze, error) {
	res, err := a.svc.Freeze(ctx, txnTypeRefund, refundNo, merchantID, amount)
	if err != nil {
		return RefundFreeze{}, err
	}
	return RefundFreeze{Status: res.Status}, nil
}

// REF-02 验收：真实账户库有 1 张 FROZEN 冻结单，金额等于退款金额，渠道流水为 PROCESSING。
func TestCreateRefundFreezeAccountMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, "PA_REF_1", "OT_REF_1", payStatusSuccess, now)

	my, err := testkit.Ready(env.ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	s0, err := my.OpenSchema(env.ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeAccountSchema(t, s0) })
	s1, err := my.OpenSchema(env.ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeAccountSchema(t, s1) })
	clk := &clock.Fake{}
	clk.Set(now)
	ids, err := idgen.New(9, clk)
	if err != nil {
		t.Fatal(err)
	}
	accounts := accountbiz.New(accountdal.New(s0.DB, s1.DB), ids, 8)
	if _, err = accounts.OpenAccount(env.ctx, "MERCHANT", merchantID, "MERCHANT_SETTLE", 0, 0); err != nil {
		t.Fatal(err)
	}
	res, err := s0.DB.ExecContext(env.ctx, `
UPDATE account SET balance = 100000
WHERE owner_type = 'MERCHANT' AND owner_id = ? AND account_type = 'MERCHANT_SETTLE'`, merchantID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("balance rows = %d err=%v", n, err)
	}
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_RF_1"}}
	p.UseRefund(accountBizFreezer{svc: accounts}, channel, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_1", OutRefundNo: "R1", Amount: 1000,
	})
	if err != nil || got.Status != refundStatusProcessing {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	var status, bizType, bizNo string
	var amount int64
	err = s0.DB.QueryRowContext(env.ctx, `
SELECT status, biz_type, biz_no, amount FROM account_freeze WHERE biz_no = ?`, got.RefundNo).
		Scan(&status, &bizType, &bizNo, &amount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if status != freezeStatusFrozen || bizType != txnTypeRefund || bizNo != got.RefundNo || amount != 1000 {
		t.Fatalf("freeze status=%s type=%s biz=%s amount=%d", status, bizType, bizNo, amount)
	}
	var freezeN int
	if err = s0.DB.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM account_freeze`).Scan(&freezeN); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if freezeN != 1 || channel.callCount() != 1 {
		t.Fatalf("freeze rows=%d channel=%d", freezeN, channel.callCount())
	}
	var txnN int
	if err = env.db.QueryRowContext(env.ctx, `
SELECT COUNT(*) FROM channel_txn WHERE biz_no = ? AND txn_type = 'REFUND' AND status = 'PROCESSING'`, got.RefundNo).Scan(&txnN); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if txnN != 1 {
		t.Fatalf("channel_txn = %d", txnN)
	}
}

func closeAccountSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}
