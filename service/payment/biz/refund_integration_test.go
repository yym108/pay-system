//go:build integration

package biz

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

func insertRefundPay(t *testing.T, env payEnv, merchantID, payNo, outTradeNo, status string, now time.Time) {
	t.Helper()
	var paid any
	if status == payStatusSuccess {
		paid = now
	}
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO payment_order (
  pay_no, merchant_id, out_trade_no, amount, currency, subject, status,
  channel, fee_amount, refunded_amount, notify_url, expire_at,
  paid_at, version, created_at, updated_at
) VALUES (
  ?, ?, ?, 10000, 'CNY', 'demo', ?,
  'mockpay', 0, 0, '', ?,
  ?, 1, ?, ?
)`, payNo, merchantID, outTradeNo, status, now.Add(15*time.Minute), paid, now, now)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func payRefundedAmount(t *testing.T, env payEnv, payNo string) int64 {
	t.Helper()
	var n int64
	err := env.db.QueryRowContext(env.ctx, `SELECT refunded_amount FROM payment_order WHERE pay_no = ?`, payNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func refundOrderCount(t *testing.T, env payEnv, merchantID, payNo string) int {
	t.Helper()
	var n int
	err := env.db.QueryRowContext(env.ctx,
		`SELECT COUNT(*) FROM refund_order WHERE merchant_id = ? AND pay_no = ?`, merchantID, payNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func refundStatuses(t *testing.T, env payEnv, payNo string) []string {
	t.Helper()
	rows, err := env.db.QueryContext(env.ctx, `SELECT status FROM refund_order WHERE pay_no = ?`, payNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		out = append(out, status)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return out
}

// REF-01 验收：MySQL 上 6000 成功、5000 超额且额度仍是 6000；另一张单 6000 加 4000 占满。
func TestCreateRefundQuotaMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, "PA_REF_1", "OT_REF_1", payStatusSuccess, now)
	insertRefundPay(t, env, merchantID, "PA_REF_2", "OT_REF_2", payStatusSuccess, now)
	first, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_1", OutRefundNo: "R1", Amount: 6000,
	})
	if err != nil || first.Status != refundStatusInit || first.Amount != 6000 || first.PayNo != "PA_REF_1" {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	_, err = p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_1", OutRefundNo: "R2", Amount: 5000,
	})
	assertErrno(t, err, errno.ErrRefundExceeded)
	if got := payRefundedAmount(t, env, "PA_REF_1"); got != 6000 {
		t.Fatalf("refunded = %d", got)
	}
	if n := refundOrderCount(t, env, merchantID, "PA_REF_1"); n != 1 {
		t.Fatalf("rows = %d", n)
	}
	if _, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_2", OutRefundNo: "R3", Amount: 6000,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, OutTradeNo: "OT_REF_2", OutRefundNo: "R4", Amount: 4000,
	}); err != nil {
		t.Fatal(err)
	}
	if got := payRefundedAmount(t, env, "PA_REF_2"); got != 10000 {
		t.Fatalf("second refunded = %d", got)
	}
	if n := refundOrderCount(t, env, merchantID, "PA_REF_2"); n != 2 {
		t.Fatalf("second rows = %d", n)
	}
}

// REF-01 验收：MySQL 上相同 out_refund_no 返回同一 refund_no，不加额度；金额不同是 10006。
func TestCreateRefundIdempotentMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(11, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, "PA_REF_1", "OT_REF_1", payStatusSuccess, now)
	insertRefundPay(t, env, merchantID, "PA_REF_2", "OT_REF_2", payStatusSuccess, now)
	in := &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_1", OutRefundNo: "SAME", Amount: 3000, Reason: "demo",
	}
	first, err := p.CreateRefund(env.ctx, in)
	if err != nil || first.RefundNo == "" || first.Status != refundStatusInit {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	again, err := p.CreateRefund(env.ctx, in)
	if err != nil || again.RefundNo != first.RefundNo {
		t.Fatalf("again = %+v err=%v", again, err)
	}
	if got := payRefundedAmount(t, env, "PA_REF_1"); got != 3000 {
		t.Fatalf("refunded = %d", got)
	}
	if n := refundOrderCount(t, env, merchantID, "PA_REF_1"); n != 1 {
		t.Fatalf("rows = %d", n)
	}
	_, err = p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_1", OutRefundNo: "SAME", Amount: 2000,
	})
	assertErrno(t, err, errno.ErrIdempotentConflict)
	_, err = p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_2", OutRefundNo: "SAME", Amount: 3000,
	})
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if got := payRefundedAmount(t, env, "PA_REF_1"); got != 3000 {
		t.Fatalf("original refunded = %d", got)
	}
	if got := payRefundedAmount(t, env, "PA_REF_2"); got != 0 {
		t.Fatalf("other refunded = %d", got)
	}
	if n := refundOrderCount(t, env, merchantID, "PA_REF_1"); n != 1 {
		t.Fatalf("rows = %d", n)
	}
}

// REF-01 验收：MySQL 上 10 个并发各退 3000，恰好 3 笔 INIT，额度 9000，其余 30005。
func TestCreateRefundConcurrentMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(12, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, "PA_REF_1", "OT_REF_1", payStatusSuccess, now)
	const n = 10
	errs := make([]error, n)
	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			_, errs[i] = p.CreateRefund(env.ctx, &CreateRefundInput{
				MerchantID: merchantID, PayNo: "PA_REF_1",
				OutRefundNo: fmt.Sprintf("C%d", i), Amount: 3000,
			})
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
			continue
		}
		assertErrno(t, err, errno.ErrRefundExceeded)
	}
	if ok != 3 {
		t.Fatalf("successes = %d", ok)
	}
	if got := payRefundedAmount(t, env, "PA_REF_1"); got != 9000 {
		t.Fatalf("refunded = %d", got)
	}
	if n := refundOrderCount(t, env, merchantID, "PA_REF_1"); n != 3 {
		t.Fatalf("rows = %d", n)
	}
	for _, status := range refundStatuses(t, env, "PA_REF_1") {
		if status != refundStatusInit {
			t.Fatalf("status = %s", status)
		}
	}
}

// REF-01 验收：PAYING 返回 30003，不插入 refund_order。
func TestCreateRefundPayingMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(13, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, "PA_REF_PAY", "OT_REF_PAY", payStatusPaying, now)
	_, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: "PA_REF_PAY", OutRefundNo: "R1", Amount: money.MinPayAmount,
	})
	assertErrno(t, err, errno.ErrOrderStatusConflict)
	if n := refundOrderCount(t, env, merchantID, "PA_REF_PAY"); n != 0 {
		t.Fatalf("rows = %d", n)
	}
}
