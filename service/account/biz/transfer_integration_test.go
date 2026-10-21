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

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

// ACC-07 验收 1：A、B 各 10000，A→B 与 B→A 各 500 次并发转 1，结束后 A+B=20000。
func TestTransferLocalCrossConcurrent(t *testing.T) {
	env := openEnv(t)
	payerShard, err := shard.Of("M100001")
	if err != nil || payerShard != 1 {
		t.Fatalf("M100001 shard = %d err=%v", payerShard, err)
	}
	payeeShard, err := shard.Of("M100003")
	if err != nil || payeeShard != 1 {
		t.Fatalf("M100003 shard = %d err=%v", payeeShard, err)
	}
	_, payerID, db := openSettle(t, env, "M100001", 10000, 0)
	_, payeeID, payeeDB := openSettle(t, env, "M100003", 10000, 0)
	if payeeDB != db {
		t.Fatal("payee opened on another shard")
	}
	// 至少 64。只抬本分片，另一分片收小，避免两个池加起来超过测试库的 max_connections。
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)
	env.s0.DB.SetMaxOpenConns(4)
	env.s0.DB.SetMaxIdleConns(4)

	ctx, cancel := context.WithTimeout(env.ctx, 180*time.Second)
	defer cancel()
	const each = 500
	results := make([]TransferLocalResult, each*2)
	errs := make([]error, each*2)
	// 两边都要锁同一对账户，实际串行。WithTx 的 3 秒含等连接。
	// 信号量在进 WithTx 之前，避免排队把这 3 秒耗尽。池仍然是 64。
	sem := make(chan struct{}, 32)
	var wg sync.WaitGroup
	wg.Add(each * 2)
	for i := 0; i < each; i++ {
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i], errs[i] = env.svc.TransferLocal(ctx, fmt.Sprintf("AB%04d", i), "M100001", "M100003", 1)
		}(i)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[each+i], errs[each+i] = env.svc.TransferLocal(ctx, fmt.Sprintf("BA%04d", i), "M100003", "M100001", 1)
		}(i)
	}
	wg.Wait()
	failed := 0
	sample := ""
	for i := range errs {
		if errs[i] != nil {
			failed++
			if sample == "" {
				sample = fmt.Sprintf("call %d: %s (%s)", i, testkit.Redact(errs[i]), transferErrDetail(errs[i]))
			}
			continue
		}
		if results[i].Status != statusSuccess || results[i].TxnNo == "" || results[i].FailCode != "" {
			t.Fatalf("call %d = %+v", i, results[i])
		}
	}
	if failed != 0 {
		t.Fatalf("%d calls failed, first %s", failed, sample)
	}
	assertLocalBatch(t, ctx, db, payerID, payeeID)
	assertShardEmpty(t, ctx, env.s0.DB)
}

// ACC-07 验收 2：同一 transfer_no 并发 50 次，只生成 1 张凭证，结果相同。
func TestTransferLocalSameNoConcurrent(t *testing.T) {
	env := openEnv(t)
	_, payerID, db := openSettle(t, env, "M100001", 10000, 0)
	_, payeeID, _ := openSettle(t, env, "M100003", 10000, 0)
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)
	ctx, cancel := context.WithTimeout(env.ctx, 120*time.Second)
	defer cancel()
	const n = 50
	results := make([]TransferLocalResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = env.svc.TransferLocal(ctx, "SAME-1", "M100001", "M100003", 40)
		}(i)
	}
	wg.Wait()
	first := results[0]
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
		}
		if results[i] != first {
			t.Fatalf("call %d = %+v, want %+v", i, results[i], first)
		}
	}
	if first.Status != statusSuccess || first.TxnNo == "" || first.FailCode != "" {
		t.Fatalf("result = %+v", first)
	}
	if countTxnBiz(t, ctx, db, bizTransfer, "SAME-1") != 1 {
		t.Fatal("voucher count != 1")
	}
	if countLocal(t, ctx, db, "SAME-1") != 1 {
		t.Fatal("branch count != 1")
	}
	balP, frP, _, _ := balanceOf(t, ctx, db, payerID)
	balE, frE, _, _ := balanceOf(t, ctx, db, payeeID)
	if balP != 9960 || balE != 10040 || frP != 0 || frE != 0 {
		t.Fatalf("payer=%d/%d payee=%d/%d", balP, frP, balE, frE)
	}
	assertNoOtherShard(t, ctx, env.s0.DB, "SAME-1")
}

// ACC-07 验收 3：余额不足失败后入账，同一 transfer_no 仍 FAILED，没有凭证。
func TestTransferLocalInsufficientStaysFailed(t *testing.T) {
	env := openEnv(t)
	payer, payerID, db := openSettle(t, env, "M100001", 100, 0)
	_, payeeID, _ := openSettle(t, env, "M100003", 0, 0)
	got, err := env.svc.TransferLocal(env.ctx, "LOW-1", "M100001", "M100003", 500)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	assertNoLeak(t, err)
	if got.Status != statusFailed || got.FailCode != failInsufficientBalance || got.TxnNo != "" {
		t.Fatalf("result = %+v", got)
	}
	if countTxnBiz(t, env.ctx, db, bizTransfer, "LOW-1") != 0 || countEntryBiz(t, env.ctx, db, bizTransfer, "LOW-1") != 0 {
		t.Fatal("insufficient balance wrote a voucher")
	}
	bal, frozen, _, _ := balanceOf(t, env.ctx, db, payerID)
	payeeBal, payeeFrozen, _, _ := balanceOf(t, env.ctx, db, payeeID)
	if bal != 100 || frozen != 0 || payeeBal != 0 || payeeFrozen != 0 {
		t.Fatalf("balances changed payer=%d/%d payee=%d/%d", bal, frozen, payeeBal, payeeFrozen)
	}
	row, version := mustLocal(t, env.ctx, db, "LOW-1")
	if row.Status != statusFailed || row.FailCode != failInsufficientBalance || row.TxnNo != "" || row.Amount != 500 || version != 0 {
		t.Fatalf("tombstone = %+v version=%d", row, version)
	}

	setAmount(t, env.ctx, db, payer.AccountNo, 10000, 0)
	again, err := env.svc.TransferLocal(env.ctx, "LOW-1", "M100001", "M100003", 500)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if again != got {
		t.Fatalf("replay = %+v, want %+v", again, got)
	}
	if countTxnBiz(t, env.ctx, db, bizTransfer, "LOW-1") != 0 {
		t.Fatal("replay wrote a voucher")
	}
	bal, frozen, _, _ = balanceOf(t, env.ctx, db, payerID)
	if bal != 10000 || frozen != 0 {
		t.Fatalf("replay changed balance=%d frozen=%d", bal, frozen)
	}
	assertNoOtherShard(t, env.ctx, env.s0.DB, "LOW-1")
}

// ACC-07 验收 4：双方不同分片、转给自己，返回 10001，没有写入。
func TestTransferLocalRejectsCrossShardAndSelf(t *testing.T) {
	env := openEnv(t)
	openSettle(t, env, "M100001", 1000, 0)
	openSettle(t, env, "M100002", 1000, 0)
	before0 := snapShard(t, env.ctx, env.s0.DB)
	before1 := snapShard(t, env.ctx, env.s1.DB)

	_, err := env.svc.TransferLocal(env.ctx, "CROSS-1", "M100001", "M100002", 1)
	assertErrno(t, err, errno.ErrInvalidParam)
	assertNoLeak(t, err)
	_, err = env.svc.TransferLocal(env.ctx, "SELF-1", "M100001", "M100001", 1)
	assertErrno(t, err, errno.ErrInvalidParam)
	assertNoLeak(t, err)

	if snapShard(t, env.ctx, env.s0.DB) != before0 || snapShard(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("invalid transfer wrote a row")
	}
}

// ACC-07：重复 SUCCESS 返回同一 txn_no，不产生第二张凭证。
func TestTransferLocalReplaySuccess(t *testing.T) {
	env := openEnv(t)
	_, payerID, db := openSettle(t, env, "M100001", 1000, 0)
	_, payeeID, _ := openSettle(t, env, "M100003", 0, 0)
	first, err := env.svc.TransferLocal(env.ctx, "ONCE-1", "M100001", "M100003", 250)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if first.Status != statusSuccess || first.TxnNo == "" || first.FailCode != "" {
		t.Fatalf("first = %+v", first)
	}
	balP, frP, _, verP := balanceOf(t, env.ctx, db, payerID)
	balE, frE, _, verE := balanceOf(t, env.ctx, db, payeeID)
	again, err := env.svc.TransferLocal(env.ctx, "ONCE-1", "M100001", "M100003", 250)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if again != first {
		t.Fatalf("replay = %+v, want %+v", again, first)
	}
	if countTxnBiz(t, env.ctx, db, bizTransfer, "ONCE-1") != 1 || countEntryBiz(t, env.ctx, db, bizTransfer, "ONCE-1") != 2 {
		t.Fatal("replay wrote another voucher")
	}
	gotP, gotFrP, _, gotVerP := balanceOf(t, env.ctx, db, payerID)
	gotE, gotFrE, _, gotVerE := balanceOf(t, env.ctx, db, payeeID)
	if gotP != balP || gotE != balE || gotFrP != frP || gotFrE != frE || gotVerP != verP || gotVerE != verE {
		t.Fatalf("replay changed accounts")
	}
	if balP != 750 || balE != 250 || frP != 0 || frE != 0 {
		t.Fatalf("balances payer=%d payee=%d", balP, balE)
	}
	assertTransferVoucher(t, env.ctx, db, "ONCE-1", first.TxnNo, 250, map[uint64]entryWant{
		payerID: {direction: directionDebit, amount: 250, after: 750},
		payeeID: {direction: directionCredit, amount: 250, after: 250},
	})
	if countFreezeAll(t, env.ctx, db) != 0 || countRole(t, env.ctx, db, "PAYEE") != 0 {
		t.Fatal("transfer wrote a freeze or PAYEE branch")
	}
	if countTxnBiz(t, env.ctx, db, bizTransferOut, "ONCE-1") != 0 || countTxnBiz(t, env.ctx, db, bizTransferIn, "ONCE-1") != 0 {
		t.Fatal("voucher type is not TRANSFER")
	}
}

// ACC-07：已有分支金额、付款方、收款方不一致，各返回 10006，旧行与余额不变。
func TestTransferLocalIdempotentConflict(t *testing.T) {
	env := openEnv(t)
	_, payerID, db := openSettle(t, env, "M100001", 5000, 0)
	_, payeeID, _ := openSettle(t, env, "M100003", 5000, 0)
	_, otherID, _ := openSettle(t, env, "M100005", 5000, 0)
	cases := []struct {
		name  string
		no    string
		payer string
		payee string
	}{
		{name: "amount", no: "CF-AMT", payer: "M100001", payee: "M100003"},
		{name: "payer", no: "CF-PAYER", payer: "M100005", payee: "M100003"},
		{name: "payee", no: "CF-PAYEE", payer: "M100001", payee: "M100005"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			insertLocalFixture(t, env.ctx, db, tc.no, "M100001", "M100003", 100, statusSuccess, "", "TX0000000000000000099")
			before := mustLocalRaw(t, env.ctx, db, tc.no)
			txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
			amount := money.Amount(100)
			if tc.name == "amount" {
				amount = 80
			}
			_, err := env.svc.TransferLocal(env.ctx, tc.no, tc.payer, tc.payee, amount)
			assertErrno(t, err, errno.ErrIdempotentConflict)
			assertNoLeak(t, err)
			if mustLocalRaw(t, env.ctx, db, tc.no) != before {
				t.Fatal("conflict changed the branch")
			}
			txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
			if txnAfter != txnBefore || entryAfter != entryBefore {
				t.Fatal("conflict wrote a voucher")
			}
		})
	}
	balP, frP, _, _ := balanceOf(t, env.ctx, db, payerID)
	balE, frE, _, _ := balanceOf(t, env.ctx, db, payeeID)
	balO, frO, _, _ := balanceOf(t, env.ctx, db, otherID)
	if balP != 5000 || balE != 5000 || balO != 5000 || frP != 0 || frE != 0 || frO != 0 {
		t.Fatalf("balances changed %d %d %d", balP, balE, balO)
	}
}

// ACC-07：账户不存在是 40001，且优先于非 ACTIVE。两个都在但一个非 ACTIVE 是 40005。都不写分支。
func TestTransferLocalAccountState(t *testing.T) {
	env := openEnv(t)
	db := env.s1.DB
	_, payerID, _ := openSettle(t, env, "M100001", 1000, 0)

	_, err := env.svc.TransferLocal(env.ctx, "MISS-PAYEE", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrAccountNotFound)
	_, err = env.svc.TransferLocal(env.ctx, "MISS-PAYER", "M100003", "M100001", 1)
	assertErrno(t, err, errno.ErrAccountNotFound)

	setAccountStatus(t, env.ctx, db, payerID, "FROZEN")
	_, err = env.svc.TransferLocal(env.ctx, "MISS-AND-FROZEN", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrAccountNotFound)
	if errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal("missing account became 40005")
	}
	setAccountStatus(t, env.ctx, db, payerID, statusActive)

	_, payeeID, _ := openSettle(t, env, "M100003", 1000, 0)
	setAccountStatus(t, env.ctx, db, payeeID, "CLOSED")
	_, err = env.svc.TransferLocal(env.ctx, "PAYEE-CLOSED", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrAccountInactive)
	setAccountStatus(t, env.ctx, db, payeeID, statusActive)
	setAccountStatus(t, env.ctx, db, payerID, "FROZEN")
	_, err = env.svc.TransferLocal(env.ctx, "PAYER-FROZEN", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrAccountInactive)

	if countLocalAll(t, env.ctx, db) != 0 || countTxnBiz(t, env.ctx, db, bizTransfer, "PAYEE-CLOSED") != 0 {
		t.Fatal("account state wrote a branch")
	}
	balP, frP, statusP, _ := balanceOf(t, env.ctx, db, payerID)
	balE, frE, statusE, _ := balanceOf(t, env.ctx, db, payeeID)
	if balP != 1000 || balE != 1000 || frP != 0 || frE != 0 || statusP != "FROZEN" || statusE != statusActive {
		t.Fatalf("accounts changed payer=%d %s payee=%d %s", balP, statusP, balE, statusE)
	}
}

// ACC-07：账面余额够、可用余额不够时 FAILED，balance 和 frozen 都不变。
func TestTransferLocalFrozenHoldsAvailable(t *testing.T) {
	env := openEnv(t)
	_, payerID, db := openSettle(t, env, "M100001", 1000, 800)
	_, payeeID, _ := openSettle(t, env, "M100003", 0, 0)
	got, err := env.svc.TransferLocal(env.ctx, "HOLD-1", "M100001", "M100003", 300)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Status != statusFailed || got.FailCode != failInsufficientBalance || got.TxnNo != "" {
		t.Fatalf("result = %+v", got)
	}
	bal, frozen, _, _ := balanceOf(t, env.ctx, db, payerID)
	payeeBal, payeeFrozen, _, _ := balanceOf(t, env.ctx, db, payeeID)
	if bal != 1000 || frozen != 800 || payeeBal != 0 || payeeFrozen != 0 {
		t.Fatalf("payer=%d/%d payee=%d/%d", bal, frozen, payeeBal, payeeFrozen)
	}
	if countTxnBiz(t, env.ctx, db, bizTransfer, "HOLD-1") != 0 {
		t.Fatal("frozen funds wrote a voucher")
	}
	row, _ := mustLocal(t, env.ctx, db, "HOLD-1")
	if row.Status != statusFailed || row.FailCode != failInsufficientBalance {
		t.Fatalf("branch = %+v", row)
	}
}

// ACC-07：已有 TRANSFER 凭证时仍提交 SUCCESS，回写那张 txn_no，不再改余额。
func TestTransferLocalDuplicateVoucherStillSucceeds(t *testing.T) {
	env := openEnv(t)
	_, payerID, db := openSettle(t, env, "M100001", 800, 0)
	_, payeeID, _ := openSettle(t, env, "M100003", 100, 0)
	posted, err := postCommitted(env.ctx, db, env.svc, bizTransfer, "DUP-V", "", []Entry{
		{AccountID: payerID, Direction: directionDebit, Amount: 80},
		{AccountID: payeeID, Direction: directionCredit, Amount: 80},
	})
	if err != nil || posted.Duplicated || posted.TxnNo == "" {
		t.Fatalf("post = %+v err=%v", posted, err)
	}
	balP, _, _, verP := balanceOf(t, env.ctx, db, payerID)
	balE, _, _, verE := balanceOf(t, env.ctx, db, payeeID)
	got, err := env.svc.TransferLocal(env.ctx, "DUP-V", "M100001", "M100003", 80)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Status != statusSuccess || got.TxnNo != posted.TxnNo || got.FailCode != "" {
		t.Fatalf("result = %+v, posted %s", got, posted.TxnNo)
	}
	if countTxnBiz(t, env.ctx, db, bizTransfer, "DUP-V") != 1 {
		t.Fatal("duplicate voucher inserted another txn")
	}
	gotP, _, _, gotVerP := balanceOf(t, env.ctx, db, payerID)
	gotE, _, _, gotVerE := balanceOf(t, env.ctx, db, payeeID)
	if gotP != balP || gotE != balE || gotVerP != verP || gotVerE != verE {
		t.Fatal("duplicate voucher changed balances")
	}
	if balP != 720 || balE != 180 {
		t.Fatalf("balances payer=%d payee=%d", balP, balE)
	}
}

// ACC-07：不选平台子账户。subCount 为 0 仍能转账。
func TestTransferLocalSubCountZero(t *testing.T) {
	env := openEnv(t)
	_, payerID, db := openSettle(t, env, "M100001", 50, 0)
	_, payeeID, _ := openSettle(t, env, "M100003", 10, 0)
	svc := New(env.pools, env.svc.ids, 0)
	got, err := svc.TransferLocal(env.ctx, "SUB0", "M100001", "M100003", 5)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Status != statusSuccess || got.TxnNo == "" {
		t.Fatalf("result = %+v", got)
	}
	balP, frP, _, _ := balanceOf(t, env.ctx, db, payerID)
	balE, frE, _, _ := balanceOf(t, env.ctx, db, payeeID)
	if balP != 45 || balE != 15 || frP != 0 || frE != 0 {
		t.Fatalf("payer=%d/%d payee=%d/%d", balP, frP, balE, frE)
	}
}

// transferErrDetail 只保留错误号，不把 SQL 打进失败输出。
func transferErrDetail(err error) string {
	if err == nil {
		return ""
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) && me != nil {
		return fmt.Sprintf("mysql %d", me.Number)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var ee *errno.Error
	if errors.As(err, &ee) && ee != nil {
		cause := ee.Unwrap()
		if cause == nil {
			return fmt.Sprintf("errno %d", ee.Code)
		}
		kind := transferErrDetail(cause)
		if kind == "plain" {
			msg := cause.Error()
			if len(msg) > 120 {
				msg = msg[:120]
			}
			lower := strings.ToLower(msg)
			if strings.Contains(lower, "select") || strings.Contains(lower, "insert") || strings.Contains(lower, "update") || strings.Contains(msg, "@") {
				return fmt.Sprintf("errno %d: %T", ee.Code, cause)
			}
			return fmt.Sprintf("errno %d: %T %q", ee.Code, cause, msg)
		}
		return fmt.Sprintf("errno %d: %s", ee.Code, kind)
	}
	return "plain"
}

func openSettle(t *testing.T, env shardEnv, merchantID string, balance, frozen int64) (Account, uint64, *sqlx.DB) {
	t.Helper()
	shardNo, err := shard.Of(merchantID)
	if err != nil {
		t.Fatal(err)
	}
	acc, id, db := openOn(t, env, ownerMerchant, merchantID, typeMerchantSettle, shardNo, 0)
	// 开户已经是 0/0。再写成同样的值时，MySQL 的 RowsAffected 是 0。
	if balance != 0 || frozen != 0 {
		setAmount(t, env.ctx, db, acc.AccountNo, balance, frozen)
	}
	return acc, id, db
}

func insertLocalFixture(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo, payer, payee string, amount int64, status, failCode, txnNo string) {
	t.Helper()
	var fail, txn any
	if failCode != "" {
		fail = failCode
	}
	if txnNo != "" {
		txn = txnNo
	}
	res, err := db.ExecContext(ctx, `
INSERT INTO transfer_branch (
  transfer_no, role, payer_merchant_id, payee_merchant_id, amount, status, fail_code, txn_no, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		transferNo, roleLocal, payer, payee, amount, status, fail, txn)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("insert branch n=%d err=%v", n, err)
	}
}

func mustLocal(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) (localBranch, int64) {
	t.Helper()
	var (
		payer, payee, status string
		fail, txn            sql.NullString
		amount, version      int64
	)
	err := db.QueryRowContext(ctx, `
SELECT payer_merchant_id, payee_merchant_id, amount, status, fail_code, txn_no, version
FROM transfer_branch WHERE transfer_no = ? AND role = ?`, transferNo, roleLocal).
		Scan(&payer, &payee, &amount, &status, &fail, &txn, &version)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return localBranch{
		Payer: payer, Payee: payee, Amount: money.Amount(amount),
		Status: status, FailCode: fail.String, TxnNo: txn.String,
	}, version
}

func mustLocalRaw(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) string {
	t.Helper()
	var (
		payer, payee, status string
		fail, txn            sql.NullString
		amount, version      int64
	)
	err := db.QueryRowContext(ctx, `
SELECT payer_merchant_id, payee_merchant_id, amount, status, fail_code, txn_no, version
FROM transfer_branch WHERE transfer_no = ? AND role = ?`, transferNo, roleLocal).
		Scan(&payer, &payee, &amount, &status, &fail, &txn, &version)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return fmt.Sprintf("%s %s %d %s %s %s %d", payer, payee, amount, status, fail.String, txn.String, version)
}

func countLocal(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM transfer_branch WHERE transfer_no = ? AND role = ?`, transferNo, roleLocal).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func countLocalAll(t *testing.T, ctx context.Context, db *sqlx.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM transfer_branch`).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func countRole(t *testing.T, ctx context.Context, db *sqlx.DB, role string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM transfer_branch WHERE role = ?`, role).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func countFreezeAll(t *testing.T, ctx context.Context, db *sqlx.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_freeze`).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

type shardSnap struct {
	accounts int
	txns     int
	entries  int
	branches int
	freezes  int
}

func snapShard(t *testing.T, ctx context.Context, db *sqlx.DB) shardSnap {
	t.Helper()
	var snap shardSnap
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM account`).Scan(&snap.accounts); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	snap.txns, snap.entries = ledgerCounts(t, ctx, db)
	snap.branches = countLocalAll(t, ctx, db)
	snap.freezes = countFreezeAll(t, ctx, db)
	return snap
}

func assertNoOtherShard(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) {
	t.Helper()
	if countLocal(t, ctx, db, transferNo) != 0 || countTxnBiz(t, ctx, db, bizTransfer, transferNo) != 0 {
		t.Fatal("other shard has this transfer")
	}
}

func assertShardEmpty(t *testing.T, ctx context.Context, db *sqlx.DB) {
	t.Helper()
	snap := snapShard(t, ctx, db)
	if snap != (shardSnap{}) {
		t.Fatalf("other shard = %+v", snap)
	}
}

func assertLocalBatch(t *testing.T, ctx context.Context, db *sqlx.DB, payerID, payeeID uint64) {
	t.Helper()
	balP, frP, _, _ := balanceOf(t, ctx, db, payerID)
	balE, frE, _, _ := balanceOf(t, ctx, db, payeeID)
	if balP != 10000 || balE != 10000 || balP+balE != 20000 || frP != 0 || frE != 0 {
		t.Fatalf("balances payer=%d/%d payee=%d/%d", balP, frP, balE, frE)
	}
	var txns, badTxn, otherBiz int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type = ?`, bizTransfer).Scan(&txns); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if txns != 1000 {
		t.Fatalf("vouchers = %d", txns)
	}
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM ledger_txn
WHERE biz_type = ? AND (amount <> 1 OR entry_count <> 2 OR memo <> '' OR currency <> 'CNY')`, bizTransfer).Scan(&badTxn)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if badTxn != 0 {
		t.Fatalf("bad vouchers = %d", badTxn)
	}
	if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type <> ?`, bizTransfer).Scan(&otherBiz); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if otherBiz != 0 {
		t.Fatalf("other biz_type = %d", otherBiz)
	}
	var entries, uneven int
	if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_entry`).Scan(&entries); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if entries != 2000 {
		t.Fatalf("entries = %d", entries)
	}
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM ledger_txn t
WHERE (SELECT COUNT(*) FROM ledger_entry e WHERE e.txn_no = t.txn_no) <> 2`).Scan(&uneven)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if uneven != 0 {
		t.Fatalf("uneven vouchers = %d", uneven)
	}
	assertLeg(t, ctx, db, payerID, directionDebit, 500, 10000)
	assertLeg(t, ctx, db, payerID, directionCredit, 500, 10000)
	assertLeg(t, ctx, db, payeeID, directionDebit, 500, 10000)
	assertLeg(t, ctx, db, payeeID, directionCredit, 500, 10000)
	var wrongDebit, wrongCredit int
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM ledger_entry e
JOIN transfer_branch b ON b.txn_no = e.txn_no AND b.role = ?
JOIN account a ON a.id = e.account_id
WHERE e.direction = ? AND a.owner_id <> b.payer_merchant_id`, roleLocal, directionDebit).Scan(&wrongDebit)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM ledger_entry e
JOIN transfer_branch b ON b.txn_no = e.txn_no AND b.role = ?
JOIN account a ON a.id = e.account_id
WHERE e.direction = ? AND a.owner_id <> b.payee_merchant_id`, roleLocal, directionCredit).Scan(&wrongCredit)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if wrongDebit != 0 || wrongCredit != 0 {
		t.Fatalf("wrong legs debit=%d credit=%d", wrongDebit, wrongCredit)
	}
	var branches, orphans int
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM transfer_branch
WHERE role = ? AND status = ? AND fail_code IS NULL AND txn_no IS NOT NULL AND amount = 1`, roleLocal, statusSuccess).Scan(&branches)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if branches != 1000 {
		t.Fatalf("branches = %d", branches)
	}
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM transfer_branch b
LEFT JOIN ledger_txn t ON t.txn_no = b.txn_no AND t.biz_type = ? AND t.biz_no = b.transfer_no
WHERE b.role = ? AND t.id IS NULL`, bizTransfer, roleLocal).Scan(&orphans)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if orphans != 0 {
		t.Fatalf("branches without voucher = %d", orphans)
	}
	if countRole(t, ctx, db, "PAYEE") != 0 || countFreezeAll(t, ctx, db) != 0 {
		t.Fatal("batch wrote PAYEE or freeze")
	}
	var ab, ba int
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM transfer_branch
WHERE payer_merchant_id = 'M100001' AND payee_merchant_id = 'M100003' AND amount = 1 AND status = ?`, statusSuccess).Scan(&ab)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	err = db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM transfer_branch
WHERE payer_merchant_id = 'M100003' AND payee_merchant_id = 'M100001' AND amount = 1 AND status = ?`, statusSuccess).Scan(&ba)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if ab != 500 || ba != 500 {
		t.Fatalf("directions ab=%d ba=%d", ab, ba)
	}
}

func assertLeg(t *testing.T, ctx context.Context, db *sqlx.DB, accountID uint64, direction string, wantN int, opening int64) {
	t.Helper()
	var n int
	var sum int64
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM ledger_entry
WHERE account_id = ? AND direction = ? AND amount = 1 AND biz_type = ?`, accountID, direction, bizTransfer).Scan(&n, &sum)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n != wantN || sum != int64(wantN) {
		t.Fatalf("account %d %s count=%d sum=%d", accountID, direction, n, sum)
	}
	var delta, last int64
	err = db.QueryRowContext(ctx, `
SELECT COALESCE(SUM(CASE WHEN direction = ? THEN amount ELSE -amount END), 0)
FROM ledger_entry WHERE account_id = ?`, directionCredit, accountID).Scan(&delta)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	err = db.QueryRowContext(ctx, `
SELECT balance_after FROM ledger_entry WHERE account_id = ? ORDER BY id DESC LIMIT 1`, accountID).Scan(&last)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	bal, _, _, _ := balanceOf(t, ctx, db, accountID)
	if last != bal || bal != opening+delta {
		t.Fatalf("account %d balance=%d last=%d opening+delta=%d", accountID, bal, last, opening+delta)
	}
}

func assertTransferVoucher(t *testing.T, ctx context.Context, db *sqlx.DB, bizNo, txnNo string, amount int64, want map[uint64]entryWant) {
	t.Helper()
	assertConfirmVoucher(t, ctx, db, bizTransfer, bizNo, txnNo, amount, want)
}
