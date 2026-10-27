//go:build integration

package biz

import (
	"database/sql"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

// PAY-02 验收 1：100 个并发相同请求只插入 1 行，全部返回同一个 pay_no。
func TestPAY02ConcurrentSameRequest(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100040"
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, merchantID, 80, 60, now, "merchant-demo")
	in := &CreatePaymentInput{
		MerchantID: merchantID, OutTradeNo: "CONCURRENT-1", Amount: 10000, Subject: "demo",
	}
	const n = 100
	orders := make([]PaymentOrder, n)
	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			got, err := p.CreatePayment(env.ctx, in)
			if err != nil {
				return err
			}
			orders[i] = got
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	payNo := orders[0].PayNo
	for i, got := range orders {
		if got.PayNo != payNo || got.FeeAmount != 60 || got.Status != "INIT" || got.Amount != 10000 {
			t.Fatalf("goroutine %d = %+v", i, got)
		}
	}
	row := loadPayment(t, env, merchantID, "CONCURRENT-1")
	if row.count != 1 || row.payNo != payNo || row.fee != 60 || row.status != "INIT" ||
		row.refunded != 0 || row.version != 0 || row.channel.Valid || row.paidAt.Valid ||
		row.closedAt.Valid || row.failCode.Valid || row.failMsg.Valid || row.notifyURL != "" ||
		!row.createdAt.Equal(now) || !row.expireAt.Equal(now.Add(15*time.Minute)) || !row.updatedAt.Equal(now) {
		t.Fatalf("stored = %+v", row)
	}
	again, err := p.CreatePayment(env.ctx, in)
	if err != nil || again.PayNo != payNo {
		t.Fatalf("replay = %+v err=%v", again, err)
	}
	if loadPayment(t, env, merchantID, "CONCURRENT-1").count != 1 || !loadPayment(t, env, merchantID, "CONCURRENT-1").updatedAt.Equal(now) {
		t.Fatal("replay updated the row")
	}
}

// PAY-02 验收 2：相同 out_trade_no、不同金额返回 10006。真实唯一索引。
func TestPAY02AmountConflict(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100041"
	now := feeAt(11, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	in := &CreatePaymentInput{MerchantID: merchantID, OutTradeNo: "SAME-1", Amount: 10000, Subject: "demo"}
	if _, err := p.CreatePayment(env.ctx, in); err != nil {
		t.Fatal(err)
	}
	in.Amount = 9999
	_, err := p.CreatePayment(env.ctx, in)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if row := loadPayment(t, env, merchantID, "SAME-1"); row.count != 1 || row.amount != 10000 {
		t.Fatalf("stored = %+v", row)
	}
}

// PAY-02 验收 4：库里的生效档是 60bp，merchant.fee_rate_bp 是 80，金额 10000 的手续费是 60。
func TestPAY02FeeUsesEffectiveTier(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100042"
	now := feeAt(12, 0, 0, 0)
	p := openPayments(t, env, merchantID, 80, 60, now, "merchant-demo")
	got, err := p.CreatePayment(env.ctx, &CreatePaymentInput{
		MerchantID: merchantID, OutTradeNo: "FEE-1", Amount: 10000, Subject: "demo",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.FeeAmount != 60 || merchantRate(t, env, merchantID) != 80 {
		t.Fatalf("fee = %d column = %d", got.FeeAmount, merchantRate(t, env, merchantID))
	}
	if _, err := idgen.Parse(got.PayNo); err != nil || got.PayNo[:2] != "PA" {
		t.Fatalf("pay_no = %s err=%v", got.PayNo, err)
	}
}

func TestPAY02DisabledAndBadNotify(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100043"
	now := feeAt(13, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	if _, err := env.db.ExecContext(env.ctx, `UPDATE merchant SET status = 'DISABLED' WHERE merchant_id = ?`, merchantID); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err := p.CreatePayment(env.ctx, &CreatePaymentInput{
		MerchantID: merchantID, OutTradeNo: "DIS-1", Amount: money.MinPayAmount, Subject: "demo",
	})
	assertErrno(t, err, errno.ErrMerchantInvalid)
	if loadPayment(t, env, merchantID, "DIS-1").count != 0 {
		t.Fatal("disabled merchant inserted")
	}
	zero := int32(0)
	_, err = p.CreatePayment(env.ctx, &CreatePaymentInput{
		MerchantID: merchantID, OutTradeNo: "DIS-2", Amount: 1, Subject: "demo", ExpireMinutes: &zero,
	})
	assertErrno(t, err, errno.ErrInvalidParam)
}

type storedPayment struct {
	count     int
	payNo     string
	amount    int64
	fee       int64
	refunded  int64
	status    string
	notifyURL string
	version   uint64
	createdAt time.Time
	expireAt  time.Time
	updatedAt time.Time
	channel   sql.NullString
	paidAt    sql.NullTime
	closedAt  sql.NullTime
	failCode  sql.NullString
	failMsg   sql.NullString
}

func loadPayment(t *testing.T, env payEnv, merchantID, outTradeNo string) storedPayment {
	t.Helper()
	var row storedPayment
	err := env.db.QueryRowContext(env.ctx, `
SELECT COUNT(*) FROM payment_order WHERE merchant_id = ? AND out_trade_no = ?`,
		merchantID, outTradeNo).Scan(&row.count)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if row.count == 0 {
		return row
	}
	err = env.db.QueryRowContext(env.ctx, `
SELECT pay_no, amount, fee_amount, refunded_amount, status, notify_url, version,
       created_at, expire_at, updated_at, channel, paid_at, closed_at, fail_code, fail_msg
FROM payment_order WHERE merchant_id = ? AND out_trade_no = ?`, merchantID, outTradeNo).Scan(
		&row.payNo, &row.amount, &row.fee, &row.refunded, &row.status, &row.notifyURL, &row.version,
		&row.createdAt, &row.expireAt, &row.updatedAt, &row.channel, &row.paidAt, &row.closedAt,
		&row.failCode, &row.failMsg,
	)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

func openPayments(t *testing.T, env payEnv, merchantID string, columnRate, tierRate int, now time.Time, hosts string) *Payments {
	t.Helper()
	insertMerchant(t, env, merchantID, columnRate, "ACTIVE")
	clk := &clock.Fake{}
	clk.Set(now)
	if _, err := env.svc.AppendFeeRate(env.ctx, merchantID, tierRate, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	ids, err := idgen.New(7, clk)
	if err != nil {
		t.Fatal(err)
	}
	return NewPayments(NewMerchants(env.db, nil, clk, 0), ids, clk, hosts, env.db)
}
