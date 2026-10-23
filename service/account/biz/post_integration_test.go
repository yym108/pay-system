//go:build integration

package biz

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

func openOn(t *testing.T, env shardEnv, ownerType, ownerID, accountType string, shardNo, subNo int) (Account, uint64, *sqlx.DB) {
	t.Helper()
	acc, err := env.svc.OpenAccount(env.ctx, ownerType, ownerID, accountType, shardNo, subNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	db := env.s0.DB
	if shardNo == 1 {
		db = env.s1.DB
	}
	return acc, accountID(t, env.ctx, db, acc.AccountNo), db
}

func accountID(t *testing.T, ctx context.Context, db *sqlx.DB, accountNo string) uint64 {
	t.Helper()
	var id uint64
	err := db.QueryRowContext(ctx, `SELECT id FROM account WHERE account_no = ?`, accountNo).Scan(&id)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if id == 0 {
		t.Fatal("account id is 0")
	}
	return id
}

func ledgerCounts(t *testing.T, ctx context.Context, db *sqlx.DB) (txn int, entry int) {
	t.Helper()
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_txn`).Scan(&txn); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_entry`).Scan(&entry); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return txn, entry
}

func accountExists(t *testing.T, ctx context.Context, db *sqlx.DB, id uint64) bool {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM account WHERE id = ?`, id).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n != 0
}

func postCommitted(ctx context.Context, db *sqlx.DB, svc *Service, bizType, bizNo, memo string, entries []Entry) (PostResult, error) {
	var out PostResult
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		var postErr error
		out, postErr = svc.Post(ctx, tx, bizType, bizNo, memo, entries)
		return postErr
	})
	return out, err
}

func balanceOf(t *testing.T, ctx context.Context, db *sqlx.DB, id uint64) (balance, frozen int64, status string, version int64) {
	t.Helper()
	err := db.QueryRowContext(ctx,
		`SELECT balance, frozen_amount, status, version FROM account WHERE id = ?`, id).
		Scan(&balance, &frozen, &status, &version)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return balance, frozen, status, version
}

// ACC-03 验收 1：借贷不平、金额为 0、分录只有 1 条，返回 40003，凭证、分录和余额都不变。
func TestPostRejectsUnbalancedZeroAndSingle(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 80, 5)
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
	txn1Before, entry1Before := ledgerCounts(t, env.ctx, env.s1.DB)

	cases := []struct {
		name    string
		entries []Entry
	}{
		{name: "unbalanced", entries: []Entry{
			{AccountID: merchantID, Direction: directionDebit, Amount: 3},
			{AccountID: feeID, Direction: directionCredit, Amount: 2},
		}},
		{name: "amount zero", entries: []Entry{
			{AccountID: merchantID, Direction: directionDebit, Amount: 0},
			{AccountID: feeID, Direction: directionCredit, Amount: 0},
		}},
		{name: "one entry", entries: []Entry{
			{AccountID: merchantID, Direction: directionDebit, Amount: 1},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := postCommitted(env.ctx, db, env.svc, bizPay, "BAD-"+tc.name, "", tc.entries)
			if !errors.Is(err, errno.ErrUnbalancedEntries) {
				t.Fatalf("error = %s", testkit.Redact(err))
			}
			assertNoLeak(t, err)
			if errors.Is(err, errno.ErrAccountNotFound) {
				t.Fatal("validation returned 40001")
			}
		})
	}
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatalf("ledger txn=%d entry=%d", txnAfter, entryAfter)
	}
	txn1, entry1 := ledgerCounts(t, env.ctx, env.s1.DB)
	if txn1 != txn1Before || entry1 != entry1Before {
		t.Fatal("shard 1 changed")
	}
	balance, frozen, _, _ := balanceOf(t, env.ctx, db, merchantID)
	if balance != 80 || frozen != 5 {
		t.Fatalf("balance=%d frozen=%d", balance, frozen)
	}
}

// ACC-03 验收 2：同一 (biz_type, biz_no) 并发 100 次，只生成 1 张凭证。
func TestPostConcurrentSameBizNo(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	const opening int64 = 1000
	setAmount(t, env.ctx, db, merchant.AccountNo, opening, 0)
	const (
		n      = 100
		amount = money.Amount(10)
		bizNo  = "ACC03-SAME"
	)
	entries := []Entry{
		{AccountID: merchantID, Direction: directionDebit, Amount: amount},
		{AccountID: feeID, Direction: directionCredit, Amount: amount},
	}
	results := make([]PostResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = postCommitted(env.ctx, db, env.svc, bizPay, bizNo, "", entries)
		}(i)
	}
	wg.Wait()

	var txnNo string
	fresh, duplicated := 0, 0
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
		}
		if results[i].TxnNo == "" {
			t.Fatalf("call %d has empty txn_no", i)
		}
		if results[i].Duplicated {
			duplicated++
		} else {
			fresh++
			txnNo = results[i].TxnNo
		}
	}
	if fresh != 1 || duplicated != n-1 {
		t.Fatalf("fresh=%d duplicated=%d", fresh, duplicated)
	}
	for i := range results {
		if results[i].TxnNo != txnNo {
			t.Fatalf("txn_no = %s, want %s", results[i].TxnNo, txnNo)
		}
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != opening-int64(amount) || frozen != 0 || version != 1 {
		t.Fatalf("merchant balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	feeBalance, feeFrozen, _, feeVersion := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != int64(amount) || feeFrozen != 0 || feeVersion != 1 {
		t.Fatalf("fee balance=%d frozen=%d version=%d", feeBalance, feeFrozen, feeVersion)
	}
	txnCount, entryCount := ledgerCounts(t, env.ctx, db)
	if txnCount != 1 || entryCount != 2 {
		t.Fatalf("txn=%d entry=%d", txnCount, entryCount)
	}

	again, err := postCommitted(env.ctx, db, env.svc, bizPay, bizNo, "不同备注", []Entry{
		{AccountID: merchantID, Direction: directionDebit, Amount: 77},
		{AccountID: feeID, Direction: directionCredit, Amount: 77},
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !again.Duplicated || again.TxnNo != txnNo {
		t.Fatalf("replay = %+v, want duplicated %s", again, txnNo)
	}
	balance, _, _, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != opening-int64(amount) || version != 1 {
		t.Fatalf("replay changed balance=%d version=%d", balance, version)
	}
	var storedAmount int64
	var storedMemo string
	err = db.QueryRowContext(env.ctx, `SELECT amount, memo FROM ledger_txn WHERE txn_no = ?`, txnNo).Scan(&storedAmount, &storedMemo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if storedAmount != int64(amount) || storedMemo != "" {
		t.Fatalf("stored amount=%d memo=%q", storedAmount, storedMemo)
	}
	txnCount, entryCount = ledgerCounts(t, env.ctx, db)
	if txnCount != 1 || entryCount != 2 {
		t.Fatalf("after replay txn=%d entry=%d", txnCount, entryCount)
	}
}

// ACC-03 验收 3：1000 个 goroutine 对余额 500 的商户各借记 1，恰好 500 次成功、500 次 40002。
// 不用 dbx.WithTx。WithTx 把单次事务限制在 3 秒，包含等行锁的时间，1000 个等待者会被取消，计数就不是 500/500。
// 这里用 BeginTxx(READ COMMITTED)，父 ctx 超过 60 秒。Post 返回 nil 才 Commit，否则 Rollback。Post 自己不提交。
// 驱动的读超时也是 3 秒。同时堵在同一行锁上的连接如果太多，后面的 SELECT FOR UPDATE 会先被驱动打断。
// 所以只把这个测试库的打开连接数收到 8。1000 个 goroutine 仍然一起发起，多出来的在连接池里等父 ctx。
func TestPostConcurrentDebit500(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	const opening int64 = 500
	setAmount(t, env.ctx, db, merchant.AccountNo, opening, 0)
	txn1Before, entry1Before := ledgerCounts(t, env.ctx, env.s1.DB)

	ctx, cancel := context.WithTimeout(env.ctx, 120*time.Second)
	defer cancel()
	const n = 1000
	entries := []Entry{
		{AccountID: merchantID, Direction: directionDebit, Amount: 1},
		{AccountID: feeID, Direction: directionCredit, Amount: 1},
	}
	results := make([]PostResult, n)
	errs := make([]error, n)
	rbErrs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				errs[i] = err
				return
			}
			results[i], err = env.svc.Post(ctx, tx, bizPay, fmt.Sprintf("D%04d", i), "", entries)
			if err != nil {
				rbErrs[i] = tx.Rollback()
				errs[i] = err
				return
			}
			if commitErr := tx.Commit(); commitErr != nil {
				_ = tx.Rollback()
				errs[i] = commitErr
			}
		}(i)
	}
	wg.Wait()

	okN, insuf := 0, 0
	for i := range errs {
		if rbErrs[i] != nil {
			t.Fatalf("rollback %d: %s", i, testkit.Redact(rbErrs[i]))
		}
		if errs[i] == nil {
			if results[i].Duplicated || results[i].TxnNo == "" {
				t.Fatalf("call %d result = %+v", i, results[i])
			}
			okN++
			continue
		}
		if errors.Is(errs[i], errno.ErrInsufficientBalance) {
			insuf++
			continue
		}
		t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
	}
	if okN != int(opening) || insuf != n-int(opening) {
		t.Fatalf("ok=%d insufficient=%d", okN, insuf)
	}
	balance, frozen, _, _ := balanceOf(t, env.ctx, db, merchantID)
	if balance != 0 || frozen != 0 {
		t.Fatalf("merchant balance=%d frozen=%d", balance, frozen)
	}
	feeBalance, feeFrozen, _, _ := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != opening || feeFrozen != 0 {
		t.Fatalf("fee balance=%d frozen=%d", feeBalance, feeFrozen)
	}
	txnCount, entryCount := ledgerCounts(t, env.ctx, db)
	if txnCount != okN || entryCount != okN*2 {
		t.Fatalf("txn=%d entry=%d ok=%d", txnCount, entryCount, okN)
	}
	txn1, entry1 := ledgerCounts(t, env.ctx, env.s1.DB)
	if txn1 != txn1Before || entry1 != entry1Before {
		t.Fatal("shard 1 changed")
	}
	assertEntryReplay(t, env.ctx, db, map[uint64]money.Amount{
		merchantID: money.Amount(opening),
		feeID:      0,
	}, map[uint64]string{
		merchantID: directionCredit,
		feeID:      directionCredit,
	})
}

// ACC-03 验收 4：分录里放一个只存在于另一分片的 account_id，返回 40003，两个库都不新增凭证。
func TestPostRejectsOtherShardAccount(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db0 := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, _, db1 := openOn(t, env, ownerMerchant, "M100001", typeMerchantSettle, 1, 0)
	_, foreignID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeTransit, 1, 0)
	if accountExists(t, env.ctx, db0, foreignID) {
		t.Fatalf("account %d is visible on shard 0", foreignID)
	}
	setAmount(t, env.ctx, db0, merchant.AccountNo, 40, 0)
	txn0, entry0 := ledgerCounts(t, env.ctx, db0)
	txn1, entry1 := ledgerCounts(t, env.ctx, db1)
	_, err := postCommitted(env.ctx, db0, env.svc, bizTransfer, "CROSS-1", "", []Entry{
		{AccountID: merchantID, Direction: directionDebit, Amount: 5},
		{AccountID: foreignID, Direction: directionCredit, Amount: 5},
	})
	if !errors.Is(err, errno.ErrUnbalancedEntries) {
		t.Fatalf("error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	if errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatal("other shard returned 40001")
	}
	gotTxn0, gotEntry0 := ledgerCounts(t, env.ctx, db0)
	gotTxn1, gotEntry1 := ledgerCounts(t, env.ctx, db1)
	if gotTxn0 != txn0 || gotEntry0 != entry0 || gotTxn1 != txn1 || gotEntry1 != entry1 {
		t.Fatalf("ledger changed shard0=%d/%d shard1=%d/%d", gotTxn0, gotEntry0, gotTxn1, gotEntry1)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db0, merchantID)
	if balance != 40 || frozen != 0 || version != 0 {
		t.Fatalf("merchant balance=%d frozen=%d version=%d", balance, frozen, version)
	}
}

// ACC-03 验收 5：余额 10，借记 20，返回 40002，凭证、分录和余额都不变。
func TestPostInsufficientLeavesNothing(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 10, 0)
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
	_, err := postCommitted(env.ctx, db, env.svc, bizPay, "SHORT-1", "", []Entry{
		{AccountID: merchantID, Direction: directionDebit, Amount: 20},
		{AccountID: feeID, Direction: directionCredit, Amount: 20},
	})
	if !errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatalf("error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatalf("ledger txn=%d entry=%d", txnAfter, entryAfter)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 10 || frozen != 0 || version != 0 {
		t.Fatalf("balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	feeBalance, _, _, feeVersion := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != 0 || feeVersion != 0 {
		t.Fatalf("fee balance=%d version=%d", feeBalance, feeVersion)
	}
}

// ACC-03 验收 6：balance_after 等于从 SQL 写入的期初余额、按 ledger_entry.id 回放的结果。
func TestPostBalanceAfterMatchesReplay(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	fee, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	channel, channelID, _ := openOn(t, env, ownerPlatform, "mockpay", typeChannelReceivable, 0, 0)
	const (
		merchantOpen int64 = 1000
		feeOpen      int64 = 50
		channelOpen  int64 = 7
		frozen       int64 = 100
	)
	setAmount(t, env.ctx, db, merchant.AccountNo, merchantOpen, frozen)
	setAmount(t, env.ctx, db, fee.AccountNo, feeOpen, 0)
	setAmount(t, env.ctx, db, channel.AccountNo, channelOpen, 0)
	memo := "入账备注"
	first, err := postCommitted(env.ctx, db, env.svc, bizPay, "PAY-1", memo, []Entry{
		{AccountID: channelID, Direction: directionDebit, Amount: 100},
		{AccountID: merchantID, Direction: directionCredit, Amount: 94},
		{AccountID: feeID, Direction: directionCredit, Amount: 6},
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if first.Duplicated || first.TxnNo == "" {
		t.Fatalf("first = %+v", first)
	}
	_, err = postCommitted(env.ctx, db, env.svc, bizTransfer, "MOVE-1", "", []Entry{
		{AccountID: merchantID, Direction: directionDebit, Amount: 20},
		{AccountID: feeID, Direction: directionCredit, Amount: 20},
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err = postCommitted(env.ctx, db, env.svc, bizRefund, "BACK-1", "", []Entry{
		{AccountID: feeID, Direction: directionDebit, Amount: 10},
		{AccountID: merchantID, Direction: directionCredit, Amount: 10},
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	opening := map[uint64]money.Amount{
		merchantID: money.Amount(merchantOpen),
		feeID:      money.Amount(feeOpen),
		channelID:  money.Amount(channelOpen),
	}
	dirs := map[uint64]string{
		merchantID: directionCredit,
		feeID:      directionCredit,
		channelID:  directionDebit,
	}
	assertEntryReplay(t, env.ctx, db, opening, dirs)
	balance, gotFrozen, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 1084 || gotFrozen != frozen || version != 3 {
		t.Fatalf("merchant balance=%d frozen=%d version=%d", balance, gotFrozen, version)
	}
	feeBalance, _, _, feeVersion := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != 66 || feeVersion != 3 {
		t.Fatalf("fee balance=%d version=%d", feeBalance, feeVersion)
	}
	channelBalance, _, _, channelVersion := balanceOf(t, env.ctx, db, channelID)
	if channelBalance != 107 || channelVersion != 1 {
		t.Fatalf("channel balance=%d version=%d", channelBalance, channelVersion)
	}
	var storedMemo string
	var entryCount int
	var txnAmount int64
	err = db.QueryRowContext(env.ctx,
		`SELECT memo, entry_count, amount FROM ledger_txn WHERE txn_no = ?`, first.TxnNo).
		Scan(&storedMemo, &entryCount, &txnAmount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if storedMemo != memo || entryCount != 3 || txnAmount != 100 {
		t.Fatalf("memo=%q entry_count=%d amount=%d", storedMemo, entryCount, txnAmount)
	}
	txnCount, rows := ledgerCounts(t, env.ctx, db)
	if txnCount != 3 || rows != 7 {
		t.Fatalf("txn=%d entry=%d", txnCount, rows)
	}
}

func TestPostOverdraftFrozenAndInactive(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 100, 40)
	_, err := postCommitted(env.ctx, db, env.svc, bizPay, "FROZEN-SHORT", "", []Entry{
		{AccountID: merchantID, Direction: directionDebit, Amount: 70},
		{AccountID: feeID, Direction: directionCredit, Amount: 70},
	})
	if !errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatalf("frozen short error = %s", testkit.Redact(err))
	}
	okRes, err := postCommitted(env.ctx, db, env.svc, bizPay, "FROZEN-OK", "", []Entry{
		{AccountID: merchantID, Direction: directionDebit, Amount: 50},
		{AccountID: feeID, Direction: directionCredit, Amount: 50},
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if okRes.Duplicated {
		t.Fatal("frozen-ok duplicated")
	}
	balance, frozen, _, _ := balanceOf(t, env.ctx, db, merchantID)
	if balance != 50 || frozen != 40 {
		t.Fatalf("after guarded debit balance=%d frozen=%d", balance, frozen)
	}

	over, err := postCommitted(env.ctx, db, env.svc, bizPay, "OVER-1", "", []Entry{
		{AccountID: feeID, Direction: directionDebit, Amount: 80},
		{AccountID: merchantID, Direction: directionCredit, Amount: 80},
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if over.Duplicated {
		t.Fatal("overdraft duplicated")
	}
	feeBalance, feeFrozen, _, _ := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != 50-80 || feeFrozen != 0 {
		t.Fatalf("fee balance=%d frozen=%d", feeBalance, feeFrozen)
	}
	balance, frozen, _, _ = balanceOf(t, env.ctx, db, merchantID)
	if balance != 130 || frozen != 40 {
		t.Fatalf("merchant balance=%d frozen=%d", balance, frozen)
	}

	res, err := db.ExecContext(env.ctx, `UPDATE account SET status = 'FROZEN' WHERE id = ? AND status = 'ACTIVE'`, merchantID)
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
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
	_, err = postCommitted(env.ctx, db, env.svc, bizPay, "INACTIVE-1", "", []Entry{
		{AccountID: merchantID, Direction: directionCredit, Amount: 1},
		{AccountID: feeID, Direction: directionDebit, Amount: 1},
	})
	if !errors.Is(err, errno.ErrAccountInactive) {
		t.Fatalf("inactive error = %s", testkit.Redact(err))
	}
	assertNoLeak(t, err)
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatal("inactive post wrote a ledger row")
	}
	balance, frozen, status, _ := balanceOf(t, env.ctx, db, merchantID)
	if balance != 130 || frozen != 40 || status != "FROZEN" {
		t.Fatalf("inactive account balance=%d frozen=%d status=%s", balance, frozen, status)
	}
}

func assertEntryReplay(t *testing.T, ctx context.Context, db *sqlx.DB, opening map[uint64]money.Amount, direction map[uint64]string) {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT id, account_id, direction, amount, balance_after FROM ledger_entry ORDER BY id`)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	state := make(map[uint64]money.Amount, len(opening))
	for id, bal := range opening {
		state[id] = bal
	}
	var prev uint64
	seen := 0
	for rows.Next() {
		var (
			id           uint64
			accountID    uint64
			entryDir     string
			amount       int64
			balanceAfter int64
		)
		if err := rows.Scan(&id, &accountID, &entryDir, &amount, &balanceAfter); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if prev != 0 && id <= prev {
			t.Fatalf("entry id %d not after %d", id, prev)
		}
		prev = id
		dir, ok := direction[accountID]
		if !ok {
			t.Fatalf("unexpected account %d", accountID)
		}
		next, err := replayStep(dir, entryDir, state[accountID], money.Amount(amount))
		if err != nil {
			t.Fatal(err)
		}
		if next != money.Amount(balanceAfter) {
			t.Fatalf("entry %d account %d balance_after=%d replay=%d", id, accountID, balanceAfter, next)
		}
		state[accountID] = next
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if seen == 0 {
		t.Fatal("no entries to replay")
	}
	for id, want := range state {
		balance, _, _, _ := balanceOf(t, ctx, db, id)
		if money.Amount(balance) != want {
			t.Fatalf("account %d balance=%d replay=%d", id, balance, want)
		}
	}
}

func replayStep(balanceDirection, entryDirection string, current, amount money.Amount) (money.Amount, error) {
	switch balanceDirection {
	case directionCredit:
		if entryDirection == directionCredit {
			return money.Add(current, amount)
		}
		if entryDirection == directionDebit {
			return money.Sub(current, amount)
		}
	case directionDebit:
		if entryDirection == directionDebit {
			return money.Add(current, amount)
		}
		if entryDirection == directionCredit {
			return money.Sub(current, amount)
		}
	}
	return 0, errors.New("replay: bad direction")
}
