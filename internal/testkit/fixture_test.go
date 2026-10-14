//go:build integration

package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/dbx"
)

// BASE-11：测试商户、测试账户按迁移列插入；非法组合被 CHECK 拒绝。这不是开户。
func TestInsertMerchantAndAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	my, err := Ready(ctx)
	if err != nil {
		t.Fatal(Redact(err))
	}
	pay, err := my.OpenSchema(ctx, "payment")
	if err != nil {
		t.Fatal(Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, pay) })
	acc, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, acc) })

	if _, err := my.OpenSchema(ctx, "../payment"); err == nil {
		t.Fatal("path-like service was accepted")
	}
	if _, err := my.OpenSchema(ctx, "not-a-service"); err == nil {
		t.Fatal("unknown service was accepted")
	}

	merchant := Merchant{MerchantID: "M100099", FeeRateBP: 60}
	if err := InsertMerchant(ctx, pay.DB, merchant); err != nil {
		t.Fatal(Redact(err))
	}
	var name, appID, status string
	var fee, version int
	var cipher []byte
	err = pay.DB.QueryRowContext(ctx, `
		SELECT merchant_name, app_id, app_secret_cipher, secret_version, fee_rate_bp, status
		FROM merchant WHERE merchant_id = ?`, "M100099").
		Scan(&name, &appID, &cipher, &version, &fee, &status)
	if err != nil {
		t.Fatal(Redact(err))
	}
	if name != "测试商户M100099" || appID != "app_m100099" || fee != 60 || version != 1 || status != "ACTIVE" || len(cipher) == 0 {
		t.Fatalf("merchant row name=%s app=%s fee=%d version=%d status=%s cipher=%d", name, appID, fee, version, status, len(cipher))
	}
	if err := InsertMerchant(ctx, pay.DB, merchant); err == nil || !dbx.IsDuplicate(err) {
		t.Fatalf("duplicate merchant err = %s", Redact(err))
	}

	account := Account{AccountNo: "AC9000000000000000001", OwnerID: "M100099", AccountType: "MERCHANT_SETTLE"}
	if err := InsertAccount(ctx, acc.DB, account); err != nil {
		t.Fatal(Redact(err))
	}
	var ownerType, direction, currency, accStatus string
	var subNo, overdraft int
	var balance, frozen int64
	err = acc.DB.QueryRowContext(ctx, `
		SELECT owner_type, currency, sub_no, balance_direction, balance, frozen_amount, allow_overdraft, status
		FROM account WHERE account_no = ?`, account.AccountNo).
		Scan(&ownerType, &currency, &subNo, &direction, &balance, &frozen, &overdraft, &accStatus)
	if err != nil {
		t.Fatal(Redact(err))
	}
	if ownerType != "MERCHANT" || currency != "CNY" || subNo != 0 || direction != "CREDIT" || balance != 0 || frozen != 0 || overdraft != 0 || accStatus != "ACTIVE" {
		t.Fatalf("account owner=%s currency=%s sub=%d direction=%s balance=%d frozen=%d overdraft=%d status=%s",
			ownerType, currency, subNo, direction, balance, frozen, overdraft, accStatus)
	}
	platform := Account{AccountNo: "AC1000000000000000003", OwnerID: "mockpay", AccountType: "CHANNEL_RECEIVABLE", SubNo: 2}
	if err := InsertAccount(ctx, acc.DB, platform); err != nil {
		t.Fatal(Redact(err))
	}
	err = acc.DB.QueryRowContext(ctx, `
		SELECT owner_type, sub_no, balance_direction, allow_overdraft
		FROM account WHERE account_no = ?`, platform.AccountNo).
		Scan(&ownerType, &subNo, &direction, &overdraft)
	if err != nil {
		t.Fatal(Redact(err))
	}
	if ownerType != "PLATFORM" || subNo != 2 || direction != "DEBIT" || overdraft != 1 {
		t.Fatalf("platform owner=%s sub=%d direction=%s overdraft=%d", ownerType, subNo, direction, overdraft)
	}
	bad := Account{
		AccountNo:        "AC9000000000000000002",
		OwnerType:        "MERCHANT",
		OwnerID:          "M100099",
		AccountType:      "FEE_INCOME",
		BalanceDirection: "CREDIT",
		AllowOverdraft:   1,
	}
	if err := InsertAccount(ctx, acc.DB, bad); err == nil || !dbx.IsCheckViolation(err) {
		t.Fatalf("illegal account err = %s", Redact(err))
	}
}

func closeSchema(t *testing.T, schema *Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", Redact(err))
	}
}
