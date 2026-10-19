//go:build integration

package biz

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := testkit.Shutdown(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %s\n", testkit.Redact(err))
	}
	os.Exit(code)
}

type payEnv struct {
	ctx context.Context
	db  *sqlx.DB
	svc *Service
}

func openPay(t *testing.T) payEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	schema, err := my.OpenSchema(ctx, "payment")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer closeCancel()
		if err := schema.Close(closeCtx); err != nil {
			t.Errorf("close schema: %s", testkit.Redact(err))
		}
	})
	return payEnv{ctx: ctx, db: schema.DB, svc: New(schema.DB)}
}

func insertMerchant(t *testing.T, env payEnv, id string, rate int, status string) {
	t.Helper()
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO merchant (
  merchant_id, merchant_name, app_id, app_secret_cipher, fee_rate_bp, status
) VALUES (?, 'demo', ?, ?, ?, ?)`,
		id, "app_"+strings.ToLower(id), []byte("cipher"), rate, status)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func countRates(t *testing.T, env payEnv, merchantID string) int {
	t.Helper()
	var n int
	err := env.db.QueryRowContext(env.ctx,
		`SELECT COUNT(*) FROM merchant_fee_rate WHERE merchant_id = ?`, merchantID).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func merchantRate(t *testing.T, env payEnv, merchantID string) int {
	t.Helper()
	var rate int
	err := env.db.QueryRowContext(env.ctx,
		`SELECT fee_rate_bp FROM merchant WHERE merchant_id = ?`, merchantID).Scan(&rate)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return rate
}

func storedRate(t *testing.T, env payEnv, merchantID string) int {
	t.Helper()
	var rate int
	err := env.db.QueryRowContext(env.ctx,
		`SELECT fee_rate_bp FROM merchant_fee_rate WHERE merchant_id = ?`, merchantID).Scan(&rate)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return rate
}

// PAY-10 验收 1：已有一档后，更晚的时间追加成功，行数加 1，merchant.fee_rate_bp 不变。
func TestPAY10AppendLaterKeepsMerchantRate(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100010", 60, "ACTIVE")
	t0 := feeAt(10, 0, 0, 0)
	t1 := feeAt(11, 0, 0, 0)
	first, err := env.svc.AppendFeeRate(env.ctx, "M100010", 60, t0)
	if err != nil || first.Duplicated {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	if countRates(t, env, "M100010") != 1 || merchantRate(t, env, "M100010") != 60 {
		t.Fatal("first tier changed the merchant rate")
	}
	second, err := env.svc.AppendFeeRate(env.ctx, "M100010", 80, t1)
	if err != nil || second.Duplicated || second.RateBP != 80 {
		t.Fatalf("second = %+v err=%v", second, err)
	}
	if second.EffectiveFrom.UnixMilli() != t1.UnixMilli() {
		t.Fatalf("effective = %s", second.EffectiveFrom)
	}
	if countRates(t, env, "M100010") != 2 || merchantRate(t, env, "M100010") != 60 {
		t.Fatalf("rows=%d merchantRate=%d", countRates(t, env, "M100010"), merchantRate(t, env, "M100010"))
	}
}

// PAY-10 验收 2：同一生效时间再提交同一费率幂等成功；并发也只留 1 行。
func TestPAY10SameEffectiveTimeIdempotent(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100011", 60, "ACTIVE")
	t0 := feeAt(10, 0, 0, 0)
	first, err := env.svc.AppendFeeRate(env.ctx, "M100011", 60, t0)
	if err != nil || first.Duplicated {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	again, err := env.svc.AppendFeeRate(env.ctx, "M100011", 60, t0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !again.Duplicated || again.RateBP != 60 || again.MerchantID != "M100011" {
		t.Fatalf("again = %+v", again)
	}
	if countRates(t, env, "M100011") != 1 {
		t.Fatalf("rows = %d", countRates(t, env, "M100011"))
	}

	insertMerchant(t, env, "M100012", 45, "ACTIVE")
	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	got := make([]FeeRate, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = env.svc.AppendFeeRate(env.ctx, "M100012", 45, t0)
		}(i)
	}
	wg.Wait()
	fresh := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatal(testkit.Redact(errs[i]))
		}
		if got[i].RateBP != 45 {
			t.Fatalf("result %d = %+v", i, got[i])
		}
		if !got[i].Duplicated {
			fresh++
		}
	}
	if fresh != 1 || countRates(t, env, "M100012") != 1 || merchantRate(t, env, "M100012") != 45 {
		t.Fatalf("fresh=%d rows=%d merchant=%d", fresh, countRates(t, env, "M100012"), merchantRate(t, env, "M100012"))
	}
}

// PAY-10 验收 3：同一时间不同费率返回 10006，旧费率还在。
func TestPAY10SameTimeDifferentRate(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100013", 60, "ACTIVE")
	t0 := feeAt(10, 0, 0, 0)
	if _, err := env.svc.AppendFeeRate(env.ctx, "M100013", 60, t0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err := env.svc.AppendFeeRate(env.ctx, "M100013", 80, t0)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if countRates(t, env, "M100013") != 1 || storedRate(t, env, "M100013") != 60 {
		t.Fatalf("rows=%d rate=%d", countRates(t, env, "M100013"), storedRate(t, env, "M100013"))
	}
}

// PAY-10 验收 4：更早的时间返回 10001，不新增行。
func TestPAY10EarlierTimeRejected(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100014", 60, "ACTIVE")
	t0 := feeAt(10, 0, 0, 0)
	t1 := feeAt(11, 0, 0, 0)
	if _, err := env.svc.AppendFeeRate(env.ctx, "M100014", 60, t1); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err := env.svc.AppendFeeRate(env.ctx, "M100014", 60, t0)
	assertErrno(t, err, errno.ErrInvalidParam)
	if countRates(t, env, "M100014") != 1 {
		t.Fatalf("rows = %d", countRates(t, env, "M100014"))
	}
}

// PAY-10 验收 5：不存在的商户返回 20004。
func TestPAY10UnknownMerchant(t *testing.T) {
	env := openPay(t)
	_, err := env.svc.AppendFeeRate(env.ctx, "M100099", 60, feeAt(10, 0, 0, 0))
	assertErrno(t, err, errno.ErrMerchantInvalid)
	if countRates(t, env, "M100099") != 0 {
		t.Fatal("unknown merchant wrote a tier")
	}
}

func TestPAY10DisabledMerchantCanAppend(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100015", 10, "DISABLED")
	got, err := env.svc.AppendFeeRate(env.ctx, "M100015", 10, feeAt(10, 0, 0, 0))
	if err != nil || got.Duplicated || got.RateBP != 10 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if countRates(t, env, "M100015") != 1 || merchantRate(t, env, "M100015") != 10 {
		t.Fatal("disabled merchant was rejected or updated")
	}
}

// PAY-10 验收 6：按生效档核对订单手续费，只报告，不改 fee_amount。
func TestPAY10CheckOrderFees(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100016", 15, "ACTIVE")
	t0 := feeAt(10, 0, 0, 0)
	t1 := feeAt(11, 0, 0, 0)
	if _, err := env.svc.AppendFeeRate(env.ctx, "M100016", 0, t0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := env.svc.AppendFeeRate(env.ctx, "M100016", 60, t1); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	orders := []struct {
		payNo string
		at    time.Time
		fee   int64
	}{
		{payNo: "PA0000000000000000001", at: t0.Add(-time.Millisecond), fee: 0},
		{payNo: "PA0000000000000000002", at: t0, fee: 0},
		{payNo: "PA0000000000000000003", at: t1.Add(-time.Millisecond), fee: 0},
		{payNo: "PA0000000000000000004", at: t1, fee: 60},
		{payNo: "PA0000000000000000005", at: t1.Add(time.Millisecond), fee: 60},
		{payNo: "PA0000000000000000006", at: t1.Add(time.Millisecond), fee: 0},
	}
	for i, order := range orders {
		_, err := env.db.ExecContext(env.ctx, `
INSERT INTO payment_order (
  pay_no, merchant_id, out_trade_no, amount, subject, status, fee_amount, expire_at, created_at
) VALUES (?, 'M100016', ?, 10000, 'fee-check', 'INIT', ?, ?, ?)`,
			order.payNo, fmt.Sprintf("out-%d", i), order.fee, order.at.Add(15*time.Minute), order.at)
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
	got, err := env.svc.CheckOrderFees(env.ctx, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Scanned != 6 || got.Mismatched != 2 || len(got.Details) != 2 {
		t.Fatalf("scanned=%d mismatched=%d details=%d", got.Scanned, got.Mismatched, len(got.Details))
	}
	if got.Details[0].PayNo != orders[0].payNo || got.Details[0].RateBP != -1 || got.Details[0].ExpectedFee != 0 {
		t.Fatalf("no-rate detail = %+v", got.Details[0])
	}
	want, err := money.Fee(10000, 60)
	if err != nil {
		t.Fatal(err)
	}
	if got.Details[1].PayNo != orders[5].payNo || got.Details[1].RateBP != 60 || got.Details[1].ExpectedFee != want || got.Details[1].FeeAmount != 0 {
		t.Fatalf("mismatch detail = %+v", got.Details[1])
	}
	if got.Details[1].Amount != 10000 || got.Details[1].MerchantID != "M100016" {
		t.Fatalf("amounts = %+v", got.Details[1])
	}
	var fee int64
	if err := env.db.QueryRowContext(env.ctx,
		`SELECT fee_amount FROM payment_order WHERE pay_no = ?`, orders[5].payNo).Scan(&fee); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if fee != 0 {
		t.Fatalf("fee_amount changed to %d", fee)
	}
	if merchantRate(t, env, "M100016") != 15 {
		t.Fatal("check updated merchant.fee_rate_bp")
	}

	window, err := env.svc.CheckOrderFees(env.ctx, t0, t1)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if window.Scanned != 2 || window.Mismatched != 0 {
		t.Fatalf("window = %+v", window)
	}
	empty, err := env.svc.CheckOrderFees(env.ctx, t1, t1)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if empty.Scanned != 0 || empty.Mismatched != 0 {
		t.Fatalf("empty = %+v", empty)
	}
}

func TestPAY10MigrationBackfillDownUp(t *testing.T) {
	env := openPay(t)
	if countRates(t, env, "M100017") != 0 {
		t.Fatal("empty merchant backfill wrote a row")
	}
	if !indexExists(t, env) || !tableExists(t, env, "merchant_fee_rate") {
		t.Fatal("up did not create the fee table or index")
	}
	execMigration(t, env, "000002_merchant_fee_rate.down.sql")
	if tableExists(t, env, "merchant_fee_rate") || indexExists(t, env) {
		t.Fatal("down left the fee table or index")
	}
	if !tableExists(t, env, "payment_order") {
		t.Fatal("down dropped payment_order")
	}
	insertMerchant(t, env, "M100017", 60, "ACTIVE")
	var created time.Time
	if err := env.db.QueryRowContext(env.ctx,
		`SELECT created_at FROM merchant WHERE merchant_id = ?`, "M100017").Scan(&created); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	execMigration(t, env, "000002_merchant_fee_rate.up.sql")
	if !indexExists(t, env) || countRates(t, env, "M100017") != 1 || storedRate(t, env, "M100017") != 60 {
		t.Fatal("backfill did not copy the merchant rate")
	}
	var effective time.Time
	if err := env.db.QueryRowContext(env.ctx,
		`SELECT effective_from FROM merchant_fee_rate WHERE merchant_id = ?`, "M100017").Scan(&effective); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if effective.UnixMilli() != created.UnixMilli() {
		t.Fatalf("effective %s created %s", effective, created)
	}
}

func indexExists(t *testing.T, env payEnv) bool {
	t.Helper()
	var n int
	err := env.db.QueryRowContext(env.ctx, `
SELECT COUNT(*) FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = 'payment_order' AND index_name = 'idx_pay_merchant_created'`).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n > 0
}

func tableExists(t *testing.T, env payEnv, name string) bool {
	t.Helper()
	var n int
	err := env.db.QueryRowContext(env.ctx, `
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = ?`, name).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n == 1
}

func execMigration(t *testing.T, env payEnv, name string) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), "migrations", "payment", name))
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range sqlStatements(string(body)) {
		if _, err := env.db.ExecContext(env.ctx, stmt); err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
}

func sqlStatements(text string) []string {
	var b strings.Builder
	var out []string
	for _, line := range strings.Split(text, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "--") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
		if strings.HasSuffix(trim, ";") {
			stmt := strings.TrimSpace(b.String())
			stmt = strings.TrimSuffix(stmt, ";")
			out = append(out, strings.TrimSpace(stmt))
			b.Reset()
		}
	}
	if tail := strings.TrimSpace(b.String()); tail != "" {
		out = append(out, tail)
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func TestPAY10CheckOrderFeesCanceled(t *testing.T) {
	env := openPay(t)
	ctx, cancel := context.WithCancel(env.ctx)
	cancel()
	_, err := env.svc.CheckOrderFees(ctx, time.Time{}, time.Time{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}
