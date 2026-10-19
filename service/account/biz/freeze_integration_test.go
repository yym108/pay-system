//go:build integration

package biz

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

func assertFreezeCode(t *testing.T, err error, want *errno.Error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	if err.Error() != want.Error() {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func countFreeze(t *testing.T, ctx context.Context, db *sqlx.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_freeze`).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func sumFrozenRows(t *testing.T, ctx context.Context, db *sqlx.DB, accountID uint64) (n int, sum int64) {
	t.Helper()
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM account_freeze WHERE account_id = ? AND status = 'FROZEN'`,
		accountID).Scan(&n, &sum)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n, sum
}

func insertFreezeRow(t *testing.T, ctx context.Context, db *sqlx.DB, freezeNo, bizType, bizNo string, accountID uint64, amount int64, status string) {
	t.Helper()
	res, err := db.ExecContext(ctx, `
INSERT INTO account_freeze (freeze_no, biz_type, biz_no, account_id, amount, status)
VALUES (?, ?, ?, ?, ?, ?)`, freezeNo, bizType, bizNo, accountID, amount, status)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n != 1 {
		t.Fatalf("inserted = %d", n)
	}
}

func loadFreeze(t *testing.T, ctx context.Context, db *sqlx.DB, bizType, bizNo string) (freezeNo string, accountID uint64, amount int64, status string, txnNo sql.NullString, version int64) {
	t.Helper()
	err := db.QueryRowContext(ctx, `
SELECT freeze_no, account_id, amount, status, txn_no, version
FROM account_freeze WHERE biz_type = ? AND biz_no = ?`, bizType, bizNo).
		Scan(&freezeNo, &accountID, &amount, &status, &txnNo, &version)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return freezeNo, accountID, amount, status, txnNo, version
}

func tightenPool(db *sqlx.DB, n int) {
	db.SetMaxOpenConns(n)
	db.SetMaxIdleConns(n)
}

// ACC-04 验收 1：可用余额 1000 分，100 个 goroutine 各冻结 20 分，恰好 50 次成功。
func TestFreezeConcurrentDistinctBizNo(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	tightenPool(db, 8)
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	const opening int64 = 1000
	setAmount(t, env.ctx, db, merchant.AccountNo, opening, 0)
	freeze1Before := countFreeze(t, env.ctx, env.s1.DB)
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)

	ctx, cancel := context.WithTimeout(env.ctx, 120*time.Second)
	defer cancel()
	const (
		n      = 100
		amount = money.Amount(20)
	)
	results := make([]FreezeResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = env.svc.Freeze(ctx, bizRefund, fmt.Sprintf("R%03d", i), "M100002", amount)
		}(i)
	}
	wg.Wait()

	okN, insuf := 0, 0
	seen := map[string]struct{}{}
	for i := range errs {
		if errs[i] == nil {
			if results[i].Duplicated || results[i].Status != statusFrozen || results[i].FreezeNo == "" {
				t.Fatalf("call %d result = %+v", i, results[i])
			}
			if _, ok := seen[results[i].FreezeNo]; ok {
				t.Fatalf("duplicate freeze_no %s", results[i].FreezeNo)
			}
			seen[results[i].FreezeNo] = struct{}{}
			parsed, err := idgen.Parse(results[i].FreezeNo)
			if err != nil || parsed.Prefix != "FZ" {
				t.Fatalf("freeze_no %s: %v", results[i].FreezeNo, err)
			}
			okN++
			continue
		}
		if errors.Is(errs[i], errno.ErrInsufficientBalance) {
			assertNoLeak(t, errs[i])
			insuf++
			continue
		}
		t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
	}
	if okN != 50 || insuf != 50 {
		t.Fatalf("ok=%d insufficient=%d", okN, insuf)
	}
	balance, frozen, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != opening || frozen != opening || status != statusActive || version != int64(okN) {
		t.Fatalf("balance=%d frozen=%d status=%s version=%d", balance, frozen, status, version)
	}
	rows, sum := sumFrozenRows(t, env.ctx, db, merchantID)
	if rows != okN || sum != opening {
		t.Fatalf("frozen rows=%d sum=%d", rows, sum)
	}
	if countFreeze(t, env.ctx, db) != okN {
		t.Fatalf("freeze rows=%d", countFreeze(t, env.ctx, db))
	}
	if countFreeze(t, env.ctx, env.s1.DB) != freeze1Before {
		t.Fatal("shard 1 gained a freeze")
	}
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatal("freeze wrote a ledger row")
	}
	var negative int
	err := db.QueryRowContext(env.ctx,
		`SELECT COUNT(*) FROM account WHERE allow_overdraft = 0 AND balance - frozen_amount < 0`).Scan(&negative)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if negative != 0 {
		t.Fatalf("negative available rows=%d", negative)
	}
}

// ACC-04 验收 2：同一 biz_no 重复冻结返回同一个 freeze_no，冻结金额只加一次。并发重复也一样。
func TestFreezeRepeatAndConcurrentSameBizNo(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 500, 0)
	const bizNo = "ACC04-SAME"
	first, err := env.svc.Freeze(env.ctx, bizRefund, bizNo, "M100002", 40)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if first.Duplicated || first.Status != statusFrozen || first.FreezeNo == "" {
		t.Fatalf("first = %+v", first)
	}
	second, err := env.svc.Freeze(env.ctx, bizRefund, bizNo, "M100002", 40)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !second.Duplicated || second.FreezeNo != first.FreezeNo || second.Status != statusFrozen {
		t.Fatalf("second = %+v, want duplicated %s", second, first.FreezeNo)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 500 || frozen != 40 || version != 1 {
		t.Fatalf("after replay balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, bizNo)
	if storedNo != first.FreezeNo || storedAccount != merchantID || storedAmount != 40 || storedStatus != statusFrozen || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("stored no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}

	tightenPool(db, 8)
	ctx, cancel := context.WithTimeout(env.ctx, 60*time.Second)
	defer cancel()
	const n = 16
	results := make([]FreezeResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = env.svc.Freeze(ctx, bizRefund, bizNo, "M100002", 40)
		}(i)
	}
	wg.Wait()
	fresh := 0
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
		}
		if results[i].FreezeNo != first.FreezeNo || results[i].Status != statusFrozen {
			t.Fatalf("call %d = %+v", i, results[i])
		}
		if !results[i].Duplicated {
			fresh++
		}
	}
	if fresh != 0 {
		t.Fatalf("concurrent replay fresh=%d", fresh)
	}
	balance, frozen, _, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 500 || frozen != 40 || version != 1 {
		t.Fatalf("after concurrent replay balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	if countFreeze(t, env.ctx, db) != 1 {
		t.Fatalf("freeze rows=%d", countFreeze(t, env.ctx, db))
	}
}

// ACC-04 验收 3：冻结与 Post 扣减混合并发，可用余额以条件更新和检查约束保持非负。
// ACC-10 还没做，这里用 SQL 断言，不调用 CheckBalance。
func TestFreezeMixedWithPost(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	tightenPool(db, 8)
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	const opening int64 = 1000
	setAmount(t, env.ctx, db, merchant.AccountNo, opening, 0)
	freeze1Before := countFreeze(t, env.ctx, env.s1.DB)
	txn1Before, entry1Before := ledgerCounts(t, env.ctx, env.s1.DB)

	ctx, cancel := context.WithTimeout(env.ctx, 120*time.Second)
	defer cancel()
	const (
		n      = 50
		amount = money.Amount(20)
	)
	freezeRes := make([]FreezeResult, n)
	freezeErr := make([]error, n)
	postRes := make([]PostResult, n)
	postErr := make([]error, n)
	rbErr := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			freezeRes[i], freezeErr[i] = env.svc.Freeze(ctx, bizRefund, fmt.Sprintf("MF%03d", i), "M100002", amount)
		}(i)
		go func(i int) {
			defer wg.Done()
			tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				postErr[i] = err
				return
			}
			postRes[i], err = env.svc.Post(ctx, tx, bizPay, fmt.Sprintf("MP%03d", i), "", []Entry{
				{AccountID: merchantID, Direction: directionDebit, Amount: amount},
				{AccountID: feeID, Direction: directionCredit, Amount: amount},
			})
			if err != nil {
				rbErr[i] = tx.Rollback()
				postErr[i] = err
				return
			}
			if commitErr := tx.Commit(); commitErr != nil {
				_ = tx.Rollback()
				postErr[i] = commitErr
			}
		}(i)
	}
	wg.Wait()

	freezeOK, postOK := 0, 0
	for i := range freezeErr {
		if rbErr[i] != nil {
			t.Fatalf("rollback %d: %s", i, testkit.Redact(rbErr[i]))
		}
		if freezeErr[i] == nil {
			if freezeRes[i].Duplicated || freezeRes[i].Status != statusFrozen {
				t.Fatalf("freeze %d = %+v", i, freezeRes[i])
			}
			freezeOK++
		} else if !errors.Is(freezeErr[i], errno.ErrInsufficientBalance) {
			t.Fatalf("freeze %d: %s", i, testkit.Redact(freezeErr[i]))
		} else {
			assertNoLeak(t, freezeErr[i])
		}
		if postErr[i] == nil {
			if postRes[i].Duplicated || postRes[i].TxnNo == "" {
				t.Fatalf("post %d = %+v", i, postRes[i])
			}
			postOK++
		} else if !errors.Is(postErr[i], errno.ErrInsufficientBalance) {
			t.Fatalf("post %d: %s", i, testkit.Redact(postErr[i]))
		} else {
			assertNoLeak(t, postErr[i])
		}
	}
	if freezeOK+postOK != 50 {
		t.Fatalf("freezeOK=%d postOK=%d", freezeOK, postOK)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != opening-int64(amount)*int64(postOK) || frozen != int64(amount)*int64(freezeOK) {
		t.Fatalf("balance=%d frozen=%d freezeOK=%d postOK=%d", balance, frozen, freezeOK, postOK)
	}
	if balance-frozen != 0 || version != int64(freezeOK+postOK) {
		t.Fatalf("available=%d version=%d", balance-frozen, version)
	}
	rows, sum := sumFrozenRows(t, env.ctx, db, merchantID)
	if rows != freezeOK || sum != frozen {
		t.Fatalf("frozen rows=%d sum=%d frozen=%d", rows, sum, frozen)
	}
	txnCount, entryCount := ledgerCounts(t, env.ctx, db)
	if txnCount != postOK || entryCount != postOK*2 {
		t.Fatalf("txn=%d entry=%d postOK=%d", txnCount, entryCount, postOK)
	}
	feeBalance, feeFrozen, _, feeVersion := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != int64(amount)*int64(postOK) || feeFrozen != 0 || feeVersion != int64(postOK) {
		t.Fatalf("fee balance=%d frozen=%d version=%d", feeBalance, feeFrozen, feeVersion)
	}
	if countFreeze(t, env.ctx, env.s1.DB) != freeze1Before {
		t.Fatal("shard 1 gained a freeze")
	}
	txn1, entry1 := ledgerCounts(t, env.ctx, env.s1.DB)
	if txn1 != txn1Before || entry1 != entry1Before {
		t.Fatal("shard 1 gained a ledger row")
	}
	var negative int
	err := db.QueryRowContext(env.ctx,
		`SELECT COUNT(*) FROM account WHERE allow_overdraft = 0 AND balance - frozen_amount < 0`).Scan(&negative)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if negative != 0 {
		t.Fatalf("negative available rows=%d", negative)
	}
}

// CF-14 出口：商户号、biz_type、biz_no、金额不合法时 10001，两个分片都不写冻结单。
func TestFreezeRejectsInvalidWritesNothing(t *testing.T) {
	env := openEnv(t)
	_, _, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	before0 := countFreeze(t, env.ctx, db)
	before1 := countFreeze(t, env.ctx, env.s1.DB)
	cases := []struct {
		name       string
		bizType    string
		bizNo      string
		merchantID string
		amount     money.Amount
	}{
		{name: "bad merchant", bizType: bizRefund, bizNo: "B1", merchantID: "M10000A", amount: 1},
		{name: "bad biz type", bizType: bizPay, bizNo: "B1", merchantID: "M100002", amount: 1},
		{name: "empty biz no", bizType: bizRefund, bizNo: "", merchantID: "M100002", amount: 1},
		{name: "long biz no", bizType: bizTransfer, bizNo: strings.Repeat("单", 65), merchantID: "M100002", amount: 1},
		{name: "zero amount", bizType: bizRefund, bizNo: "B1", merchantID: "M100002", amount: 0},
		{name: "negative amount", bizType: bizRefund, bizNo: "B1", merchantID: "M100002", amount: -5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := env.svc.Freeze(env.ctx, tc.bizType, tc.bizNo, tc.merchantID, tc.amount)
			assertFreezeCode(t, err, errno.ErrInvalidParam)
		})
	}
	if countFreeze(t, env.ctx, db) != before0 || countFreeze(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("invalid freeze wrote a row")
	}
}

// CF-14 出口：分片上没有商户账户，返回 40001，不写冻结单。
func TestFreezeAccountNotFound(t *testing.T) {
	env := openEnv(t)
	before0 := countFreeze(t, env.ctx, env.s0.DB)
	before1 := countFreeze(t, env.ctx, env.s1.DB)
	_, err := env.svc.Freeze(env.ctx, bizRefund, "MISSING", "M100003", 10)
	assertFreezeCode(t, err, errno.ErrAccountNotFound)
	_, err = env.svc.Freeze(env.ctx, bizTransfer, "MISSING", "M100004", 10)
	assertFreezeCode(t, err, errno.ErrAccountNotFound)
	if countFreeze(t, env.ctx, env.s0.DB) != before0 || countFreeze(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("missing account left a freeze")
	}
}

// CF-14 出口：非 ACTIVE 最终 40005，且优先于余额不足，不留冻结单。
func TestFreezeInactiveLeavesNothing(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 10, 0)
	res, err := db.ExecContext(env.ctx, `UPDATE account SET status = 'CLOSED' WHERE id = ? AND status = 'ACTIVE'`, merchantID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n != 1 {
		t.Fatalf("status update = %d", n)
	}
	_, err = env.svc.Freeze(env.ctx, bizRefund, "CLOSED-1", "M100002", 100)
	assertFreezeCode(t, err, errno.ErrAccountInactive)
	if errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatal("closed account with a short balance became 40002")
	}
	if countFreeze(t, env.ctx, db) != 0 {
		t.Fatal("inactive freeze left a row")
	}
	balance, frozen, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 10 || frozen != 0 || status != "CLOSED" || version != 0 {
		t.Fatalf("balance=%d frozen=%d status=%s version=%d", balance, frozen, status, version)
	}

	res, err = db.ExecContext(env.ctx, `UPDATE account SET status = 'FROZEN', balance = 80 WHERE id = ?`, merchantID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n, err = res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("reopen update n=%d err=%v", n, err)
	}
	_, err = env.svc.Freeze(env.ctx, bizTransfer, "FROZEN-1", "M100002", 1)
	assertFreezeCode(t, err, errno.ErrAccountInactive)
	if countFreeze(t, env.ctx, db) != 0 {
		t.Fatal("account FROZEN left a freeze row")
	}
	balance, frozen, status, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 80 || frozen != 0 || status != statusFrozen || version != 0 {
		t.Fatalf("frozen account balance=%d frozen=%d status=%s version=%d", balance, frozen, status, version)
	}
}

// CF-14 出口：可用余额不够返回 40002，不留冻结单，余额和冻结金额都不变。
func TestFreezeInsufficientLeavesNothing(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 100, 90)
	_, err := env.svc.Freeze(env.ctx, bizRefund, "SHORT-1", "M100002", 20)
	assertFreezeCode(t, err, errno.ErrInsufficientBalance)
	if countFreeze(t, env.ctx, db) != 0 {
		t.Fatal("insufficient freeze left a row")
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 100 || frozen != 90 || version != 0 {
		t.Fatalf("balance=%d frozen=%d version=%d", balance, frozen, version)
	}

	okRes, err := env.svc.Freeze(env.ctx, bizRefund, "EXACT-1", "M100002", 10)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if okRes.Duplicated || okRes.Status != statusFrozen {
		t.Fatalf("exact = %+v", okRes)
	}
	balance, frozen, _, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 100 || frozen != 100 || version != 1 {
		t.Fatalf("after exact balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	_, err = env.svc.Freeze(env.ctx, bizTransfer, "SHORT-2", "M100002", 1)
	assertFreezeCode(t, err, errno.ErrInsufficientBalance)
	if countFreeze(t, env.ctx, db) != 1 {
		t.Fatalf("freeze rows=%d", countFreeze(t, env.ctx, db))
	}
	balance, frozen, _, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 100 || frozen != 100 || version != 1 {
		t.Fatalf("after short balance=%d frozen=%d version=%d", balance, frozen, version)
	}
}

// CF-14 出口：已有 CONFIRMED 且金额、账户一致，返回已有结果，不增加冻结。
func TestFreezeConfirmedReplay(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 200, 15)
	const freezeNo = "FZCONFIRMED0000000001"
	insertFreezeRow(t, env.ctx, db, freezeNo, bizRefund, "CONF-1", merchantID, 30, statusConfirmed)
	got, err := env.svc.Freeze(env.ctx, bizRefund, "CONF-1", "M100002", 30)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !got.Duplicated || got.FreezeNo != freezeNo || got.Status != statusConfirmed {
		t.Fatalf("result = %+v", got)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 200 || frozen != 15 || version != 0 {
		t.Fatalf("balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	if countFreeze(t, env.ctx, db) != 1 {
		t.Fatal("confirmed replay inserted another row")
	}
	storedNo, storedAccount, storedAmount, storedStatus, _, _ := loadFreeze(t, env.ctx, db, bizRefund, "CONF-1")
	if storedNo != freezeNo || storedAccount != merchantID || storedAmount != 30 || storedStatus != statusConfirmed {
		t.Fatalf("row changed no=%s account=%d amount=%d status=%s", storedNo, storedAccount, storedAmount, storedStatus)
	}
}

// CF-14 出口：SQL 插入的 RELEASED 使 Freeze 返回 40004，冻结金额不变。
func TestFreezeReleasedConflict(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 200, 40)
	const freezeNo = "FZRELEASED00000000001"
	insertFreezeRow(t, env.ctx, db, freezeNo, bizTransfer, "REL-1", merchantID, 25, statusReleased)
	_, err := env.svc.Freeze(env.ctx, bizTransfer, "REL-1", "M100002", 25)
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 200 || frozen != 40 || version != 0 {
		t.Fatalf("balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	storedNo, storedAccount, storedAmount, storedStatus, _, _ := loadFreeze(t, env.ctx, db, bizTransfer, "REL-1")
	if storedNo != freezeNo || storedAccount != merchantID || storedAmount != 25 || storedStatus != statusReleased {
		t.Fatalf("row changed no=%s account=%d amount=%d status=%s", storedNo, storedAccount, storedAmount, storedStatus)
	}
	if countFreeze(t, env.ctx, db) != 1 {
		t.Fatal("released conflict inserted another row")
	}

	const otherNo = "FZRELEASED00000000002"
	insertFreezeRow(t, env.ctx, db, otherNo, bizRefund, "REL-2", merchantID, 25, statusReleased)
	_, err = env.svc.Freeze(env.ctx, bizRefund, "REL-2", "M100002", 80)
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	balance, frozen, _, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 200 || frozen != 40 || version != 0 {
		t.Fatalf("after different amount balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	storedNo, storedAccount, storedAmount, storedStatus, _, _ = loadFreeze(t, env.ctx, db, bizRefund, "REL-2")
	if storedNo != otherNo || storedAccount != merchantID || storedAmount != 25 || storedStatus != statusReleased {
		t.Fatalf("different amount row changed no=%s account=%d amount=%d status=%s", storedNo, storedAccount, storedAmount, storedStatus)
	}
	storedNo, storedAccount, storedAmount, storedStatus, _, _ = loadFreeze(t, env.ctx, db, bizTransfer, "REL-1")
	if storedNo != freezeNo || storedAccount != merchantID || storedAmount != 25 || storedStatus != statusReleased {
		t.Fatalf("first released row changed no=%s account=%d amount=%d status=%s", storedNo, storedAccount, storedAmount, storedStatus)
	}
	if countFreeze(t, env.ctx, db) != 2 {
		t.Fatalf("freeze rows=%d", countFreeze(t, env.ctx, db))
	}
}

// ACC-04：第一笔尚未提交时，16 个 goroutine 同时冻结同一个新 biz_no。
func TestFreezeConcurrentFirstInsert(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 1000, 0)
	ctx, cancel := context.WithTimeout(env.ctx, 60*time.Second)
	defer cancel()
	const (
		n      = 16
		amount = money.Amount(20)
		bizNo  = "ACC04-FIRST"
	)
	results := make([]FreezeResult, n)
	errs := make([]error, n)
	var ready sync.WaitGroup
	var wg sync.WaitGroup
	ready.Add(n)
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ready.Done()
			<-start
			results[i], errs[i] = env.svc.Freeze(ctx, bizRefund, bizNo, "M100002", amount)
		}(i)
	}
	ready.Wait()
	close(start)
	wg.Wait()

	var freezeNo string
	fresh, duplicated := 0, 0
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
		}
		if results[i].Status != statusFrozen || results[i].FreezeNo == "" {
			t.Fatalf("call %d = %+v", i, results[i])
		}
		if results[i].Duplicated {
			duplicated++
			continue
		}
		fresh++
		freezeNo = results[i].FreezeNo
	}
	if fresh != 1 || duplicated != n-1 {
		t.Fatalf("fresh=%d duplicated=%d", fresh, duplicated)
	}
	for i := range results {
		if results[i].FreezeNo != freezeNo {
			t.Fatalf("call %d freeze_no=%s, want %s", i, results[i].FreezeNo, freezeNo)
		}
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 1000 || frozen != int64(amount) || version != 1 {
		t.Fatalf("balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	if countFreeze(t, env.ctx, db) != 1 {
		t.Fatalf("freeze rows=%d", countFreeze(t, env.ctx, db))
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, bizNo)
	if storedNo != freezeNo || storedAccount != merchantID || storedAmount != int64(amount) || storedStatus != statusFrozen || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("stored no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}
}

// CF-14 出口：金额不一致或账户不一致返回 10006，旧行不变。
func TestFreezeIdempotentConflict(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 300, 5)
	insertFreezeRow(t, env.ctx, db, "FZAMOUNT0000000000001", bizRefund, "AMT-1", merchantID, 10, statusFrozen)
	_, err := env.svc.Freeze(env.ctx, bizRefund, "AMT-1", "M100002", 11)
	assertFreezeCode(t, err, errno.ErrIdempotentConflict)
	storedNo, storedAccount, storedAmount, storedStatus, _, _ := loadFreeze(t, env.ctx, db, bizRefund, "AMT-1")
	if storedNo != "FZAMOUNT0000000000001" || storedAccount != merchantID || storedAmount != 10 || storedStatus != statusFrozen {
		t.Fatalf("amount row changed no=%s account=%d amount=%d status=%s", storedNo, storedAccount, storedAmount, storedStatus)
	}

	insertFreezeRow(t, env.ctx, db, "FZACCOUNT000000000001", bizTransfer, "ACC-1", feeID, 10, statusConfirmed)
	_, err = env.svc.Freeze(env.ctx, bizTransfer, "ACC-1", "M100002", 10)
	assertFreezeCode(t, err, errno.ErrIdempotentConflict)
	storedNo, storedAccount, storedAmount, storedStatus, _, _ = loadFreeze(t, env.ctx, db, bizTransfer, "ACC-1")
	if storedNo != "FZACCOUNT000000000001" || storedAccount != feeID || storedAmount != 10 || storedStatus != statusConfirmed {
		t.Fatalf("account row changed no=%s account=%d amount=%d status=%s", storedNo, storedAccount, storedAmount, storedStatus)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 300 || frozen != 5 || version != 0 {
		t.Fatalf("balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	if countFreeze(t, env.ctx, db) != 2 {
		t.Fatalf("freeze rows=%d", countFreeze(t, env.ctx, db))
	}
}

// CF-14：TRANSFER 冻到商户所在分片。同一 biz_no 换 biz_type 是另一笔冻结。64 个码点的 biz_no 可以写入。
func TestFreezeTransferShardAndBizKey(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db1 := openOn(t, env, ownerMerchant, "M100001", typeMerchantSettle, 1, 0)
	setAmount(t, env.ctx, db1, merchant.AccountNo, 50, 0)
	before0 := countFreeze(t, env.ctx, env.s0.DB)
	bizNo := strings.Repeat("转", 64)
	first, err := env.svc.Freeze(env.ctx, bizTransfer, bizNo, "M100001", 15)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if first.Duplicated || first.Status != statusFrozen {
		t.Fatalf("transfer = %+v", first)
	}
	parsed, err := idgen.Parse(first.FreezeNo)
	if err != nil || parsed.Prefix != "FZ" {
		t.Fatalf("freeze_no %s: %v", first.FreezeNo, err)
	}
	second, err := env.svc.Freeze(env.ctx, bizRefund, bizNo, "M100001", 15)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if second.Duplicated || second.FreezeNo == first.FreezeNo || second.Status != statusFrozen {
		t.Fatalf("refund = %+v transfer=%s", second, first.FreezeNo)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db1, merchantID)
	if balance != 50 || frozen != 30 || version != 2 {
		t.Fatalf("balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	if countFreeze(t, env.ctx, db1) != 2 || countFreeze(t, env.ctx, env.s0.DB) != before0 {
		t.Fatal("freeze landed on the wrong shard")
	}
	rows, sum := sumFrozenRows(t, env.ctx, db1, merchantID)
	if rows != 2 || sum != 30 {
		t.Fatalf("rows=%d sum=%d", rows, sum)
	}
}
