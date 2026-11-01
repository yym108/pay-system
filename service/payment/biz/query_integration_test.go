//go:build integration

package biz

import (
	"testing"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// PAY-08 验收 1：用商户 B 查询商户 A 的 pay_no 返回 30001，A 的 status 与 version 不变。
func TestPAY08MerchantBCannotReadA(t *testing.T) {
	env := openPay(t)
	const merchantA = "M100080"
	const merchantB = "M100081"
	now := feeAt(14, 0, 0, 0)
	p := openPayments(t, env, merchantA, 60, 60, now, "merchant-demo")
	created, err := p.CreatePayment(env.ctx, &CreatePaymentInput{
		MerchantID: merchantA, OutTradeNo: "PAY08-A", Amount: 10000, Subject: "demo",
	})
	if err != nil {
		t.Fatal(err)
	}
	before := loadPayment(t, env, merchantA, "PAY08-A")
	outboxBefore := countPayTable(t, env, "outbox")
	txnBefore := countPayTable(t, env, "channel_txn")
	got, err := p.QueryPayment(env.ctx, &QueryInput{MerchantID: merchantB, PayNo: created.PayNo})
	assertErrno(t, err, errno.ErrOrderNotFound)
	if got.PayNo != "" || got.Amount != 0 {
		t.Fatalf("other merchant received an order: %+v", got)
	}
	after := loadPayment(t, env, merchantA, "PAY08-A")
	if after.status != before.status || after.version != before.version || after.status != payStatusInit || after.version != 0 ||
		after.amount != 10000 || !after.updatedAt.Equal(before.updatedAt) || after.payNo != created.PayNo {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	if countPayTable(t, env, "outbox") != outboxBefore || countPayTable(t, env, "channel_txn") != txnBefore {
		t.Fatal("query wrote outbox or channel_txn")
	}
}

func TestPAY08ReadOwnOrderAndMissingPayNo(t *testing.T) {
	env := openPay(t)
	const merchantA = "M100082"
	now := feeAt(15, 0, 0, 0)
	p := openPayments(t, env, merchantA, 60, 60, now, "merchant-demo")
	created, err := p.CreatePayment(env.ctx, &CreatePaymentInput{
		MerchantID: merchantA, OutTradeNo: "PAY08-OWN", Amount: money.Amount(10000), Subject: "demo",
	})
	if err != nil {
		t.Fatal(err)
	}
	byPay, err := p.QueryPayment(env.ctx, &QueryInput{MerchantID: merchantA, PayNo: created.PayNo})
	if err != nil || byPay.PayNo != created.PayNo || byPay.OutTradeNo != "PAY08-OWN" || byPay.Amount != 10000 ||
		byPay.Status != payStatusInit || byPay.Version != 0 || !byPay.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("by pay_no = %+v err=%v", byPay, err)
	}
	byOut, err := p.QueryPayment(env.ctx, &QueryInput{MerchantID: merchantA, OutTradeNo: "PAY08-OWN"})
	if err != nil || byOut.PayNo != created.PayNo || byOut.Amount != 10000 || byOut.Version != 0 || byOut.Status != payStatusInit {
		t.Fatalf("by out_trade_no = %+v err=%v", byOut, err)
	}
	_, errMissing := p.QueryPayment(env.ctx, &QueryInput{MerchantID: merchantA, PayNo: "PA999"})
	_, errOther := p.QueryPayment(env.ctx, &QueryInput{MerchantID: "M100083", PayNo: created.PayNo})
	assertErrno(t, errMissing, errno.ErrOrderNotFound)
	assertErrno(t, errOther, errno.ErrOrderNotFound)
	if errMissing.Error() != errOther.Error() {
		t.Fatalf("missing %v other %v", errMissing, errOther)
	}
	stored := loadPayment(t, env, merchantA, "PAY08-OWN")
	if stored.status != payStatusInit || stored.version != 0 || stored.amount != 10000 || !stored.updatedAt.Equal(now) {
		t.Fatalf("stored = %+v", stored)
	}
	if countPayTable(t, env, "outbox") != 0 || countPayTable(t, env, "channel_txn") != 0 {
		t.Fatal("read path wrote a row")
	}
}

func countPayTable(t *testing.T, env payEnv, table string) int {
	t.Helper()
	var query string
	switch table {
	case "outbox":
		query = `SELECT COUNT(*) FROM outbox`
	case "channel_txn":
		query = `SELECT COUNT(*) FROM channel_txn`
	default:
		t.Fatalf("unknown table %s", table)
	}
	var n int
	if err := env.db.QueryRowContext(env.ctx, query).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}
