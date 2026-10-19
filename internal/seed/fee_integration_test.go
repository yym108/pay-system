//go:build integration

package seed

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
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

// PAY-10 验收 7：M100002 只有一档 60bp，生效时间等于 created_at；再播种不新增、不改费率、不改密文。
func TestPAY10SeedFeeRateIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	pay, err := my.OpenSchema(ctx, "payment")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, pay) })
	a0, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, a0) })
	a1, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, a1) })

	key := bytes.Repeat([]byte{0x11}, 32)
	cfg := Config{
		AccountDSN0:         a0.DSN(),
		AccountDSN1:         a1.DSN(),
		PaymentDSN:          pay.DSN(),
		MasterKey:           key,
		PlatformSubAccounts: DefaultPlatformSubAccounts,
		SecretFormat:        DefaultSecretFormat,
	}
	if _, err := Run(ctx, cfg); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	assertM100002Tier(t, ctx, pay.DB)
	cipher := merchantCipher(t, ctx, pay.DB)

	if _, err := Run(ctx, cfg); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	assertM100002Tier(t, ctx, pay.DB)
	if !bytes.Equal(cipher, merchantCipher(t, ctx, pay.DB)) {
		t.Fatal("重复种子改变了密文")
	}
	var tiers int
	if err := pay.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM merchant_fee_rate`).Scan(&tiers); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if tiers != 3 {
		t.Fatalf("fee tiers = %d, want 3", tiers)
	}
}

func assertM100002Tier(t *testing.T, ctx context.Context, db *sqlx.DB) {
	t.Helper()
	var n, rate int
	var effective, created time.Time
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(MIN(r.fee_rate_bp), -1)
FROM merchant_fee_rate r
WHERE r.merchant_id = 'M100002'`).Scan(&n, &rate)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n != 1 || rate != 60 {
		t.Fatalf("M100002 tiers=%d rate=%d", n, rate)
	}
	err = db.QueryRowContext(ctx, `
SELECT r.effective_from, m.created_at
FROM merchant_fee_rate r
JOIN merchant m ON m.merchant_id = r.merchant_id
WHERE r.merchant_id = 'M100002'`).Scan(&effective, &created)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !effective.Equal(created) {
		t.Fatalf("effective %s created %s", effective, created)
	}
}

func merchantCipher(t *testing.T, ctx context.Context, db *sqlx.DB) []byte {
	t.Helper()
	var cipher []byte
	err := db.QueryRowContext(ctx,
		`SELECT app_secret_cipher FROM merchant WHERE merchant_id = 'M100002'`).Scan(&cipher)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return cipher
}

func closeSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}
