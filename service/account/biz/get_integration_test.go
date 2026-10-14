//go:build integration

package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

func setAmount(t *testing.T, ctx context.Context, db *sqlx.DB, accountNo string, balance, frozen int64) {
	t.Helper()
	res, err := db.ExecContext(ctx,
		`UPDATE account SET balance = ?, frozen_amount = ? WHERE account_no = ?`,
		balance, frozen, accountNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n != 1 {
		t.Fatalf("updated = %d", n)
	}
}

func storedAmount(t *testing.T, ctx context.Context, db *sqlx.DB, accountNo string) (balance, frozen int64) {
	t.Helper()
	err := db.QueryRowContext(ctx,
		`SELECT balance, frozen_amount FROM account WHERE account_no = ?`, accountNo).Scan(&balance, &frozen)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return balance, frozen
}

// ACC-02 验收：冻结 300 后查询，available 减少 300，balance 不变。
// 300 分是 3.00 元，1000 分是 10.00 元。不实现 Freeze，用 SQL 写入余额和冻结。
func TestGetAccountMerchantAvailableAfterFreeze(t *testing.T) {
	env := openEnv(t)
	opened, err := env.svc.OpenAccount(env.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	const (
		balanceFen int64 = 1000
		frozenFen  int64 = 300
	)
	setAmount(t, env.ctx, env.s0.DB, opened.AccountNo, balanceFen, frozenFen)

	got, err := env.svc.GetAccount(env.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Balance != money.Amount(balanceFen) || got.FrozenAmount != money.Amount(frozenFen) || got.AvailableAmount != 700 {
		t.Fatalf("amounts balance=%d frozen=%d available=%d", got.Balance, got.FrozenAmount, got.AvailableAmount)
	}
	if got.AccountNo != opened.AccountNo || got.SubCount != 1 || got.Shard != 0 || got.SubNo != 0 {
		t.Fatalf("account = %+v", got)
	}
	if got.OwnerType != "MERCHANT" || got.OwnerID != "M100002" || got.AccountType != "MERCHANT_SETTLE" {
		t.Fatalf("owner = %+v", got)
	}
	if got.Currency != "CNY" || got.BalanceDirection != "CREDIT" || got.Status != "ACTIVE" {
		t.Fatalf("account = %+v", got)
	}
	storedBalance, storedFrozen := storedAmount(t, env.ctx, env.s0.DB, opened.AccountNo)
	if storedBalance != balanceFen || storedFrozen != frozenFen {
		t.Fatalf("query wrote balance=%d frozen=%d", storedBalance, storedFrozen)
	}
	if countOwner(t, env.ctx, env.s1.DB, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0) != 0 {
		t.Fatal("shard 1 has the merchant account")
	}
}

func TestGetAccountMerchantOtherShard(t *testing.T) {
	env := openEnv(t)
	opened, err := env.svc.OpenAccount(env.ctx, "MERCHANT", "M100001", "MERCHANT_SETTLE", 1, 0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	setAmount(t, env.ctx, env.s1.DB, opened.AccountNo, 80, 15)
	got, err := env.svc.GetAccount(env.ctx, "MERCHANT", "M100001", "MERCHANT_SETTLE")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Shard != 1 || got.Balance != 80 || got.FrozenAmount != 15 || got.AvailableAmount != 65 || got.SubCount != 1 {
		t.Fatalf("account = %+v", got)
	}
	if got.AccountNo != opened.AccountNo {
		t.Fatalf("account_no = %s", got.AccountNo)
	}
	if countOwner(t, env.ctx, env.s0.DB, "MERCHANT", "M100001", "MERCHANT_SETTLE", 0) != 0 {
		t.Fatal("shard 0 has M100001")
	}
}

// ACC-02 验收：平台渠道应收的汇总等于各子账户余额之和。
// 记账 Post 还不存在。入账本身是 ACC-03，这里用 SQL 写入余额。
func TestGetAccountPlatformChannelSum(t *testing.T) {
	env := openEnv(t)
	type row struct {
		shard, sub      int
		balance, frozen int64
	}
	rows := []row{
		{shard: 0, sub: 0, balance: 1000, frozen: 0},
		{shard: 0, sub: 1, balance: 2500, frozen: 100},
		{shard: 1, sub: 2, balance: 400, frozen: 50},
		{shard: 1, sub: 7, balance: 610, frozen: 0},
	}
	var wantBalance, wantFrozen int64
	for _, row := range rows {
		opened, err := env.svc.OpenAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", row.shard, row.sub)
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
		db := env.s0.DB
		if row.shard == 1 {
			db = env.s1.DB
		}
		setAmount(t, env.ctx, db, opened.AccountNo, row.balance, row.frozen)
		wantBalance += row.balance
		wantFrozen += row.frozen
	}
	fee, err := env.svc.OpenAccount(env.ctx, "PLATFORM", "PLATFORM", "FEE_INCOME", 0, 0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	setAmount(t, env.ctx, env.s0.DB, fee.AccountNo, 99999, 0)

	got, err := env.svc.GetAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	wantAvailable, err := money.Sub(money.Amount(wantBalance), money.Amount(wantFrozen))
	if err != nil {
		t.Fatal(err)
	}
	if got.Balance != money.Amount(wantBalance) || got.FrozenAmount != money.Amount(wantFrozen) || got.AvailableAmount != wantAvailable {
		t.Fatalf("sum balance=%d frozen=%d available=%d", got.Balance, got.FrozenAmount, got.AvailableAmount)
	}
	if got.SubCount != len(rows) || got.AccountNo != "" || got.Shard != 0 || got.SubNo != 0 {
		t.Fatalf("summary = %+v", got)
	}
	if got.OwnerType != "PLATFORM" || got.OwnerID != "mockpay" || got.AccountType != "CHANNEL_RECEIVABLE" {
		t.Fatalf("owner = %+v", got)
	}
	if got.Currency != "CNY" || got.BalanceDirection != "DEBIT" || got.Status != "ACTIVE" {
		t.Fatalf("summary = %+v", got)
	}

	feeGot, err := env.svc.GetAccount(env.ctx, "PLATFORM", "PLATFORM", "FEE_INCOME")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if feeGot.Balance != 99999 || feeGot.SubCount != 1 || feeGot.AccountNo != "" || feeGot.AvailableAmount != 99999 {
		t.Fatalf("fee = %+v", feeGot)
	}
}

func TestGetAccountPlatformOneShard(t *testing.T) {
	env := openEnv(t)
	opened, err := env.svc.OpenAccount(env.ctx, "PLATFORM", "PLATFORM", "TRANSIT", 1, 3)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	setAmount(t, env.ctx, env.s1.DB, opened.AccountNo, 42, 7)
	got, err := env.svc.GetAccount(env.ctx, "PLATFORM", "PLATFORM", "TRANSIT")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Balance != 42 || got.FrozenAmount != 7 || got.AvailableAmount != 35 || got.SubCount != 1 {
		t.Fatalf("transit = %+v", got)
	}
	if got.AccountNo != "" || got.BalanceDirection != "CREDIT" {
		t.Fatalf("transit = %+v", got)
	}
}

// ACC-02 规则：不存在返回 40001，且两个库都没有新行。
func TestGetAccountNotFoundWritesNothing(t *testing.T) {
	env := openEnv(t)
	before0 := countAll(t, env.ctx, env.s0.DB)
	before1 := countAll(t, env.ctx, env.s1.DB)

	_, err := env.svc.GetAccount(env.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE")
	if !errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatalf("merchant error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	_, err = env.svc.GetAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	if !errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatalf("platform error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	_, err = env.svc.GetAccount(env.ctx, "MERCHANT", "M10002", "MERCHANT_SETTLE")
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("invalid error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	_, err = env.svc.GetAccount(env.ctx, "MERCHANT", "M100002", "FEE_INCOME")
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("combo error = %s", testkit.Redact(err))
	}

	if countAll(t, env.ctx, env.s0.DB) != before0 || countAll(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("query inserted a row")
	}
}
