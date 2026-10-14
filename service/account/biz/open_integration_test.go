//go:build integration

package biz

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/service/account/dal"
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

type shardEnv struct {
	ctx   context.Context
	s0    *testkit.Schema
	s1    *testkit.Schema
	pools *dal.Pools
	svc   *Service
}

func openEnv(t *testing.T) shardEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	s0, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, s0) })
	s1, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, s1) })
	pools := dal.New(s0.DB, s1.DB)
	ids, err := idgen.New(3, stubClock{now: openTestTime})
	if err != nil {
		t.Fatal(err)
	}
	return shardEnv{ctx: ctx, s0: s0, s1: s1, pools: pools, svc: New(pools, ids, testSubCount)}
}

func closeSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}

func countAll(t *testing.T, ctx context.Context, db *sqlx.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM account").Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func countOwner(t *testing.T, ctx context.Context, db *sqlx.DB, ownerType, ownerID, accountType string, subNo int) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM account
		 WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND currency = 'CNY' AND sub_no = ?`,
		ownerType, ownerID, accountType, subNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func assertNoLeak(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	msg := err.Error()
	for _, part := range []string{"Duplicate", "SELECT", "INSERT", "@tcp(", "password", "root"} {
		if strings.Contains(msg, part) {
			t.Fatalf("error leaked internals: %s", testkit.Redact(err))
		}
	}
}

// ACC-01 验收：同一组合并发开户 50 次，只产生 1 个账户，50 次都返回同一 account_no。
func TestOpenAccountConcurrentSameNo(t *testing.T) {
	env := openEnv(t)
	const n = 50
	got := make([]Account, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			got[i], errs[i] = env.svc.OpenAccount(env.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
		}()
	}
	wg.Wait()

	var first string
	for i := range got {
		if errs[i] != nil {
			t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
		}
		assertNoLeak(t, errs[i])
		if got[i].AccountNo == "" || !strings.HasPrefix(got[i].AccountNo, "AC") || len(got[i].AccountNo) != 21 {
			t.Fatalf("account_no = %q", got[i].AccountNo)
		}
		if first == "" {
			first = got[i].AccountNo
		}
		if got[i].AccountNo != first {
			t.Fatalf("account_no = %s, want %s", got[i].AccountNo, first)
		}
		if got[i].OwnerType != "MERCHANT" || got[i].OwnerID != "M100002" || got[i].AccountType != "MERCHANT_SETTLE" {
			t.Fatalf("owner = %+v", got[i])
		}
		if got[i].Shard != 0 || got[i].SubNo != 0 || got[i].Currency != "CNY" || got[i].BalanceDirection != "CREDIT" {
			t.Fatalf("account = %+v", got[i])
		}
		if got[i].Balance != 0 || got[i].FrozenAmount != 0 || got[i].AvailableAmount != 0 || got[i].Status != "ACTIVE" {
			t.Fatalf("amounts = %+v", got[i])
		}
	}
	if countOwner(t, env.ctx, env.s0.DB, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0) != 1 {
		t.Fatal("shard 0 row count != 1")
	}
	if countOwner(t, env.ctx, env.s1.DB, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0) != 0 {
		t.Fatal("shard 1 has the merchant account")
	}
	assertStored(t, env.ctx, env.s0.DB, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, first, "CREDIT", 0, 0)
}

// ACC-01 验收：MERCHANT + FEE_INCOME 返回 10001；把 M100002 开在分片 1 返回 10001。
func TestOpenAccountRejectsComboAndWrongShard(t *testing.T) {
	env := openEnv(t)
	before0 := countAll(t, env.ctx, env.s0.DB)
	before1 := countAll(t, env.ctx, env.s1.DB)

	_, err := env.svc.OpenAccount(env.ctx, "MERCHANT", "M100002", "FEE_INCOME", 0, 0)
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("combo error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	_, err = env.svc.OpenAccount(env.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 1, 0)
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("shard error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)

	if countAll(t, env.ctx, env.s0.DB) != before0 || countAll(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("invalid open wrote a row")
	}
	if countOwner(t, env.ctx, env.s1.DB, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0) != 0 {
		t.Fatal("shard 1 has M100002")
	}
}

// ACC-01 规则：已存在则返回已有账户，不把余额和冻结改回 0。
func TestOpenAccountKeepsBalance(t *testing.T) {
	env := openEnv(t)
	first, err := env.svc.OpenAccount(env.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	res, err := env.s0.DB.ExecContext(env.ctx,
		`UPDATE account SET balance = 100, frozen_amount = 40 WHERE account_no = ?`, first.AccountNo)
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
	second, err := env.svc.OpenAccount(env.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if second.AccountNo != first.AccountNo || second.Balance != 100 || second.FrozenAmount != 40 || second.AvailableAmount != 60 {
		t.Fatalf("second = %+v", second)
	}
	if countOwner(t, env.ctx, env.s0.DB, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0) != 1 {
		t.Fatal("row count != 1")
	}
	var gotBalance, gotFrozen int64
	err = env.s0.DB.QueryRowContext(env.ctx,
		`SELECT balance, frozen_amount FROM account WHERE account_no = ?`, first.AccountNo).Scan(&gotBalance, &gotFrozen)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if gotBalance != 100 || gotFrozen != 40 {
		t.Fatalf("stored balance=%d frozen=%d", gotBalance, gotFrozen)
	}
}

func TestOpenAccountPlatformTypes(t *testing.T) {
	env := openEnv(t)
	channel, err := env.svc.OpenAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 0, 0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if channel.BalanceDirection != "DEBIT" || channel.Shard != 0 || channel.SubNo != 0 || channel.Balance != 0 {
		t.Fatalf("channel = %+v", channel)
	}
	assertStored(t, env.ctx, env.s0.DB, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 0, channel.AccountNo, "DEBIT", 1, 0)

	again, err := env.svc.OpenAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 0, 0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if again.AccountNo != channel.AccountNo {
		t.Fatalf("idempotent account_no = %s", again.AccountNo)
	}

	otherShard, err := env.svc.OpenAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 1, 7)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if otherShard.AccountNo == channel.AccountNo || otherShard.Shard != 1 || otherShard.SubNo != 7 {
		t.Fatalf("other shard = %+v", otherShard)
	}
	assertStored(t, env.ctx, env.s1.DB, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 7, otherShard.AccountNo, "DEBIT", 1, 0)

	fee, err := env.svc.OpenAccount(env.ctx, "PLATFORM", "PLATFORM", "FEE_INCOME", 0, 1)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if fee.BalanceDirection != "CREDIT" {
		t.Fatalf("fee = %+v", fee)
	}
	assertStored(t, env.ctx, env.s0.DB, "PLATFORM", "PLATFORM", "FEE_INCOME", 1, fee.AccountNo, "CREDIT", 1, 0)

	transit, err := env.svc.OpenAccount(env.ctx, "PLATFORM", "PLATFORM", "TRANSIT", 1, 0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	assertStored(t, env.ctx, env.s1.DB, "PLATFORM", "PLATFORM", "TRANSIT", 0, transit.AccountNo, "CREDIT", 1, 0)

	before0 := countAll(t, env.ctx, env.s0.DB)
	before1 := countAll(t, env.ctx, env.s1.DB)
	_, err = env.svc.OpenAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 0, testSubCount)
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("sub error = %s", testkit.Redact(err))
	}
	_, err = env.svc.OpenAccount(env.ctx, "PLATFORM", "mockpay", "FEE_INCOME", 0, 0)
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("fee owner error = %s", testkit.Redact(err))
	}
	if countAll(t, env.ctx, env.s0.DB) != before0 || countAll(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("rejected platform open wrote a row")
	}
}

func TestOpenAccountNumberConflict(t *testing.T) {
	env := openEnv(t)
	accountNo := predictAccountNo(t, openTestTime, 3, 0)
	_, err := env.s0.DB.ExecContext(env.ctx,
		`INSERT INTO account (
		   account_no, owner_type, owner_id, account_type, currency, sub_no,
		   balance_direction, balance, frozen_amount, allow_overdraft, status, version
		 ) VALUES (?, 'MERCHANT', 'M100004', 'MERCHANT_SETTLE', 'CNY', 0, 'CREDIT', 0, 0, 0, 'ACTIVE', 0)`,
		accountNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err = env.svc.OpenAccount(env.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	if countOwner(t, env.ctx, env.s0.DB, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0) != 0 {
		t.Fatal("conflict inserted the new owner")
	}
	if countOwner(t, env.ctx, env.s0.DB, "MERCHANT", "M100004", "MERCHANT_SETTLE", 0) != 1 {
		t.Fatal("existing owner missing")
	}
}

func assertStored(t *testing.T, ctx context.Context, db *sqlx.DB, ownerType, ownerID, accountType string, subNo int, accountNo, direction string, overdraft int, balance int64) {
	t.Helper()
	var (
		gotNo, gotDirection, status, currency string
		gotBalance, frozen, version           int64
		gotOverdraft                          int
	)
	err := db.QueryRowContext(ctx,
		`SELECT account_no, currency, balance_direction, balance, frozen_amount, allow_overdraft, status, version
		 FROM account
		 WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND sub_no = ?`,
		ownerType, ownerID, accountType, subNo).Scan(
		&gotNo, &currency, &gotDirection, &gotBalance, &frozen, &gotOverdraft, &status, &version)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if gotNo != accountNo || currency != "CNY" || gotDirection != direction || gotBalance != balance || frozen != 0 {
		t.Fatalf("stored no=%s currency=%s direction=%s balance=%d frozen=%d", gotNo, currency, gotDirection, gotBalance, frozen)
	}
	if gotOverdraft != overdraft || status != "ACTIVE" || version != 0 {
		t.Fatalf("overdraft=%d status=%s version=%d", gotOverdraft, status, version)
	}
}

func predictAccountNo(t *testing.T, at time.Time, worker, seq int) string {
	t.Helper()
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	ms := at.UnixMilli() - epoch.UnixMilli()
	if ms < 0 {
		t.Fatal("clock is before idgen epoch")
	}
	id := (ms << 22) | (int64(worker) << 12) | int64(seq)
	return fmt.Sprintf("AC%019d", id)
}
