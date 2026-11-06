//go:build integration

package biz

import (
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
)

// REF-05 验收 1：越权查询返回 30001。
// 商户 B 用 A 的 refund_no、out_refund_no 查询都是 30001，与不存在的单号是同一个错误，不带退款单；
// A 的退款单、原支付单不变，没有写 outbox、channel_txn。
func TestREF05MerchantBCannotReadA(t *testing.T) {
	env := openPay(t)
	const merchantA = "M100090"
	const merchantB = "M100091"
	now := feeAt(16, 0, 0, 0)
	p := openPayments(t, env, merchantA, 60, 60, now, "merchant-demo")
	insertMerchant(t, env, merchantB, 60, "ACTIVE")
	insertRefundPay(t, env, merchantA, "PA_REF05_A", "OT_REF05_A", payStatusSuccess, now)
	created, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantA, PayNo: "PA_REF05_A", OutRefundNo: "REF05-A", Amount: 3000, Reason: "demo",
	})
	if err != nil || created.Status != refundStatusInit {
		t.Fatalf("created = %+v err=%v", created, err)
	}
	before := loadRefundRow(t, env, created.RefundNo)
	refundedBefore := payRefundedAmount(t, env, "PA_REF05_A")
	outboxBefore := countPayTable(t, env, "outbox")
	txnBefore := countPayTable(t, env, "channel_txn")

	_, errMissing := p.QueryRefund(env.ctx, &RefundQueryInput{MerchantID: merchantB, RefundNo: "RF9999999999999999999"})
	assertErrno(t, errMissing, errno.ErrOrderNotFound)
	for _, in := range []*RefundQueryInput{
		{MerchantID: merchantB, RefundNo: created.RefundNo},
		{MerchantID: merchantB, OutRefundNo: "REF05-A"},
		// 测试库排序规则不区分大小写，SQL 能匹配到 A 的行；服务端逐字节比较后仍是 30001。
		{MerchantID: "m100090", RefundNo: created.RefundNo},
		{MerchantID: "m100090", OutRefundNo: "REF05-A"},
		{MerchantID: merchantA, OutRefundNo: "ref05-a"},
		{MerchantID: merchantB, OutRefundNo: "NO-SUCH"},
	} {
		got, qerr := p.QueryRefund(env.ctx, in)
		assertErrno(t, qerr, errno.ErrOrderNotFound)
		if got.RefundNo != "" || got.Amount != 0 || qerr.Error() != errMissing.Error() {
			t.Fatalf("in=%+v got=%+v err=%v", in, got, qerr)
		}
	}

	after := loadRefundRow(t, env, created.RefundNo)
	if after.status != before.status || after.version != before.version || !after.updated.Equal(before.updated) ||
		after.status != refundStatusInit || after.version != 0 {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	if got := payRefundedAmount(t, env, "PA_REF05_A"); got != refundedBefore || got != 3000 {
		t.Fatalf("refunded_amount = %d, want %d", got, refundedBefore)
	}
	if countPayTable(t, env, "outbox") != outboxBefore || countPayTable(t, env, "channel_txn") != txnBefore {
		t.Fatal("query wrote outbox or channel_txn")
	}
}

// REF-05：本商户按两种单号都查到同一行；SUCCESS 带 succeeded_at，FAILED 带 fail_code、fail_msg，查询不改行。
func TestREF05ReadOwnRefundMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100092"
	now := feeAt(17, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, "PA_REF05_OWN", "OT_REF05_OWN", payStatusSuccess, now)
	created, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF05_OWN", OutRefundNo: "REF05-OWN", Amount: 2000,
	})
	if err != nil {
		t.Fatal(err)
	}
	byNo, err := p.QueryRefund(env.ctx, &RefundQueryInput{MerchantID: merchantID, RefundNo: created.RefundNo})
	if err != nil || byNo.RefundNo != created.RefundNo || byNo.OutRefundNo != "REF05-OWN" || byNo.PayNo != "PA_REF05_OWN" ||
		byNo.Amount != 2000 || byNo.Status != refundStatusInit || byNo.Version != 0 || !byNo.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("by refund_no = %+v err=%v", byNo, err)
	}
	byOut, err := p.QueryRefund(env.ctx, &RefundQueryInput{MerchantID: merchantID, OutRefundNo: "REF05-OWN"})
	if err != nil || byOut != byNo {
		t.Fatalf("by out_refund_no = %+v err=%v, want %+v", byOut, err, byNo)
	}

	succeeded := now.Add(time.Minute)
	if _, err = env.db.ExecContext(env.ctx,
		`UPDATE refund_order SET status = 'SUCCESS', succeeded_at = ?, version = 2 WHERE refund_no = ?`,
		succeeded, created.RefundNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err := p.QueryRefund(env.ctx, &RefundQueryInput{MerchantID: merchantID, RefundNo: created.RefundNo})
	if err != nil || got.Status != payStatusSuccess || !got.SucceededAt.Equal(succeeded) || got.Version != 2 || got.FailCode != "" {
		t.Fatalf("success = %+v err=%v", got, err)
	}
	if _, err = env.db.ExecContext(env.ctx,
		`UPDATE refund_order SET status = 'FAILED', succeeded_at = NULL, fail_code = 'CHANNEL_FAILED', fail_msg = 'mock fail', version = 3 WHERE refund_no = ?`,
		created.RefundNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err = p.QueryRefund(env.ctx, &RefundQueryInput{MerchantID: merchantID, OutRefundNo: "REF05-OWN"})
	if err != nil || got.Status != refundStatusFailed || got.FailCode != failCodeChannelFailed || got.FailMsg != "mock fail" ||
		!got.SucceededAt.IsZero() || got.Version != 3 {
		t.Fatalf("failed = %+v err=%v", got, err)
	}
	stored := loadRefundRow(t, env, created.RefundNo)
	if stored.status != refundStatusFailed || stored.version != 3 {
		t.Fatalf("stored = %+v", stored)
	}
	if countPayTable(t, env, "outbox") != 0 || countPayTable(t, env, "channel_txn") != 0 {
		t.Fatal("read path wrote a row")
	}
}
