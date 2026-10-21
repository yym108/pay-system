//go:build integration

package biz

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

// ACC-14 验收 1：Try 后 Confirm。收款余额增加 T，本分片对应在途减少 T。
// 在途从 0 开始，确认后为 -T，证明不会因为在途不足失败。重复 Confirm 只记一次账。
// ACC-10 未做，CheckBalance 用 SQL 断言代替，不调用核对函数。
func TestTransferInTryThenConfirm(t *testing.T) {
	env := openEnv(t)
	const payee = "M100002"
	const transferNo = "IN-CONFIRM-1"
	const opening int64 = 1000
	const amount = money.Amount(250)
	shardNo, err := shard.Of(payee)
	if err != nil || shardNo != 0 {
		t.Fatalf("shard = %d err=%v", shardNo, err)
	}
	_, merchantID, db := openSettle(t, env, payee, opening, 0)
	transitID := openTransitSub(t, env, transferNo, shardNo, 0)
	otherSub := otherTransitSub(t, transferNo)
	_, otherID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeTransit, shardNo, otherSub)
	insertLocalFixture(t, env.ctx, db, transferNo, "M100004", payee, int64(amount), statusSuccess, "", "TXLOCAL000000000000001")

	first, err := env.svc.TransferInTry(env.ctx, transferNo, payee, amount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if first.Status != statusTried || first.TxnNo != "" {
		t.Fatalf("try = %+v", first)
	}
	row := mustPayee(t, env.ctx, db, transferNo)
	if row.payee != payee || row.amount != int64(amount) || row.status != statusTried || row.txn != "" || row.version != 0 {
		t.Fatalf("branch = %+v", row)
	}
	assertBalances(t, env.ctx, db, merchantID, opening, 0, transitID, 0, 0)
	if countTxnBiz(t, env.ctx, db, bizTransferIn, transferNo) != 0 {
		t.Fatal("try wrote a voucher")
	}
	again, err := env.svc.TransferInTry(env.ctx, transferNo, payee, amount)
	if err != nil || again.Status != statusTried || again.TxnNo != "" {
		t.Fatalf("replay try = %+v err=%v", again, err)
	}
	if mustPayee(t, env.ctx, db, transferNo).version != 0 || countPayee(t, env.ctx, db, transferNo) != 1 {
		t.Fatal("replay try wrote")
	}

	got, err := env.svc.TransferInConfirm(env.ctx, transferNo, payee)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Status != statusConfirmed || got.TxnNo == "" {
		t.Fatalf("confirm = %+v", got)
	}
	row = mustPayee(t, env.ctx, db, transferNo)
	if row.status != statusConfirmed || row.txn != got.TxnNo || row.amount != int64(amount) || row.version != 1 {
		t.Fatalf("confirmed branch = %+v", row)
	}
	_, _, _, merchantVersion := balanceOf(t, env.ctx, db, merchantID)
	_, _, _, transitVersion := balanceOf(t, env.ctx, db, transitID)
	assertBalances(t, env.ctx, db, merchantID, opening+int64(amount), 0, transitID, -int64(amount), 0)
	if merchantVersion != 1 || transitVersion != 1 {
		t.Fatalf("versions merchant=%d transit=%d", merchantVersion, transitVersion)
	}
	_, _, _, otherVersion := balanceOf(t, env.ctx, db, otherID)
	otherBal, otherFrozen, _, _ := balanceOf(t, env.ctx, db, otherID)
	if otherBal != 0 || otherFrozen != 0 || otherVersion != 0 {
		t.Fatalf("other sub = %d/%d version=%d", otherBal, otherFrozen, otherVersion)
	}
	if countTxnBiz(t, env.ctx, db, bizTransferIn, transferNo) != 1 || countEntryBiz(t, env.ctx, db, bizTransferIn, transferNo) != 2 {
		t.Fatal("voucher count")
	}
	assertConfirmVoucher(t, env.ctx, db, bizTransferIn, transferNo, got.TxnNo, int64(amount), map[uint64]entryWant{
		transitID:  {direction: directionDebit, amount: int64(amount), after: -int64(amount)},
		merchantID: {direction: directionCredit, amount: int64(amount), after: opening + int64(amount)},
	})
	local := mustLocalRaw(t, env.ctx, db, transferNo)
	if local != "M100004 M100002 250 SUCCESS  TXLOCAL000000000000001 0" {
		t.Fatalf("local changed: %s", local)
	}

	second, err := env.svc.TransferInConfirm(env.ctx, transferNo, payee)
	if err != nil || second.Status != statusConfirmed || second.TxnNo != got.TxnNo {
		t.Fatalf("replay confirm = %+v err=%v", second, err)
	}
	if countTxnBiz(t, env.ctx, db, bizTransferIn, transferNo) != 1 || mustPayee(t, env.ctx, db, transferNo).version != 1 {
		t.Fatal("replay confirm wrote")
	}
	_, _, _, merchantVersion = balanceOf(t, env.ctx, db, merchantID)
	_, _, _, transitVersion = balanceOf(t, env.ctx, db, transitID)
	if merchantVersion != 1 || transitVersion != 1 {
		t.Fatal("replay confirm changed version")
	}
	assertBalances(t, env.ctx, db, merchantID, opening+int64(amount), 0, transitID, -int64(amount), 0)

	triedAgain, err := env.svc.TransferInTry(env.ctx, transferNo, payee, amount)
	if err != nil || triedAgain.Status != statusConfirmed || triedAgain.TxnNo != "" {
		t.Fatalf("try after confirm = %+v err=%v", triedAgain, err)
	}
	if countTxnBiz(t, env.ctx, db, bizTransferIn, transferNo) != 1 {
		t.Fatal("try after confirm wrote a voucher")
	}
	assertNoPayeeOnShard(t, env.ctx, env.s1.DB, transferNo)
	if countTxnBiz(t, env.ctx, env.s1.DB, bizTransferIn, transferNo) != 0 {
		t.Fatal("other shard has the voucher")
	}
}

// ACC-14 验收 2：Try 后 Cancel。双方余额不变，分支 CANCELLED。重复 Cancel 幂等。
// ACC-10 未做，CheckBalance 用 SQL 断言代替。
func TestTransferInTryThenCancel(t *testing.T) {
	env := openEnv(t)
	const payee = "M100002"
	const transferNo = "IN-CANCEL-1"
	const opening int64 = 800
	const amount = money.Amount(60)
	shardNo, _ := shard.Of(payee)
	_, merchantID, db := openSettle(t, env, payee, opening, 0)
	transitID := openTransitSub(t, env, transferNo, shardNo, 50)
	tried, err := env.svc.TransferInTry(env.ctx, transferNo, payee, amount)
	if err != nil || tried.Status != statusTried || tried.TxnNo != "" {
		t.Fatalf("try = %+v err=%v", tried, err)
	}
	got, err := env.svc.TransferInCancel(env.ctx, transferNo, payee, amount)
	if err != nil || got.Status != statusCancelled || got.TxnNo != "" {
		t.Fatalf("cancel = %+v err=%v", got, err)
	}
	row := mustPayee(t, env.ctx, db, transferNo)
	if row.status != statusCancelled || row.txn != "" || row.amount != int64(amount) || row.version != 1 || countPayee(t, env.ctx, db, transferNo) != 1 {
		t.Fatalf("branch = %+v", row)
	}
	assertBalances(t, env.ctx, db, merchantID, opening, 0, transitID, 50, 0)
	if countTxnBiz(t, env.ctx, db, bizTransferIn, transferNo) != 0 {
		t.Fatal("cancel wrote a voucher")
	}
	_, _, _, merchantVersion := balanceOf(t, env.ctx, db, merchantID)
	_, _, _, transitVersion := balanceOf(t, env.ctx, db, transitID)
	if merchantVersion != 0 || transitVersion != 0 {
		t.Fatalf("versions merchant=%d transit=%d", merchantVersion, transitVersion)
	}
	again, err := env.svc.TransferInCancel(env.ctx, transferNo, payee, amount+1)
	if err != nil || again.Status != statusCancelled || again.TxnNo != "" {
		t.Fatalf("replay = %+v err=%v", again, err)
	}
	row = mustPayee(t, env.ctx, db, transferNo)
	if row.version != 1 || row.amount != int64(amount) || countPayee(t, env.ctx, db, transferNo) != 1 {
		t.Fatalf("replay rewrote %+v", row)
	}
	assertBalances(t, env.ctx, db, merchantID, opening, 0, transitID, 50, 0)
	assertNoPayeeOnShard(t, env.ctx, env.s1.DB, transferNo)
}

// ACC-14 验收 3：没有行时先 Cancel，生成 CANCELLED 墓碑。随后 Try、Confirm 都是 40004，没有凭证。
// 墓碑不要求账户事先存在。ACC-10 未做，CheckBalance 用 SQL 断言代替。
func TestTransferInEmptyCancel(t *testing.T) {
	env := openEnv(t)
	const payee = "M100002"
	const transferNo = "IN-TOMB-1"
	const amount = money.Amount(15)
	shardNo, _ := shard.Of(payee)
	db := env.s0.DB
	if shardNo != 0 {
		db = env.s1.DB
	}
	got, err := env.svc.TransferInCancel(env.ctx, transferNo, payee, amount)
	if err != nil || got.Status != statusCancelled || got.TxnNo != "" {
		t.Fatalf("tombstone = %+v err=%v", got, err)
	}
	row := mustPayee(t, env.ctx, db, transferNo)
	if row.status != statusCancelled || row.payee != payee || row.amount != int64(amount) || row.txn != "" || row.version != 0 {
		t.Fatalf("tombstone row = %+v", row)
	}
	if countOwner(t, env.ctx, db, ownerMerchant, payee, typeMerchantSettle, 0) != 0 {
		t.Fatal("empty cancel created an account")
	}

	_, merchantID, _ := openSettle(t, env, payee, 100, 0)
	transitID := openTransitSub(t, env, transferNo, shardNo, 20)
	tryGot, err := env.svc.TransferInTry(env.ctx, transferNo, payee, amount+5)
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	assertEmptyIn(t, tryGot)
	assertNoLeak(t, err)
	confirmGot, err := env.svc.TransferInConfirm(env.ctx, transferNo, payee)
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	assertEmptyIn(t, confirmGot)
	row = mustPayee(t, env.ctx, db, transferNo)
	if row.status != statusCancelled || row.amount != int64(amount) || row.version != 0 || countPayee(t, env.ctx, db, transferNo) != 1 {
		t.Fatalf("late try rewrote %+v", row)
	}
	assertBalances(t, env.ctx, db, merchantID, 100, 0, transitID, 20, 0)
	if countTxnBiz(t, env.ctx, db, bizTransferIn, transferNo) != 0 || countEntryBiz(t, env.ctx, db, bizTransferIn, transferNo) != 0 {
		t.Fatal("tombstone path wrote a voucher")
	}
	assertNoPayeeOnShard(t, env.ctx, env.s1.DB, transferNo)
}

// ACC-14 验收 4：已 CONFIRMED 再 Cancel 返回 40004。分支、凭证和余额都不再变。
// ACC-10 未做，CheckBalance 用 SQL 断言代替。
func TestTransferInCancelAfterConfirm(t *testing.T) {
	env := openEnv(t)
	const payee = "M100002"
	const transferNo = "IN-CONF-CANCEL"
	const opening int64 = 400
	const amount = money.Amount(90)
	shardNo, _ := shard.Of(payee)
	_, merchantID, db := openSettle(t, env, payee, opening, 0)
	transitID := openTransitSub(t, env, transferNo, shardNo, 0)
	if _, err := env.svc.TransferInTry(env.ctx, transferNo, payee, amount); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	confirmed, err := env.svc.TransferInConfirm(env.ctx, transferNo, payee)
	if err != nil || confirmed.TxnNo == "" {
		t.Fatalf("confirm = %+v err=%v", confirmed, err)
	}
	got, err := env.svc.TransferInCancel(env.ctx, transferNo, payee, amount+1)
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	assertEmptyIn(t, got)
	assertNoLeak(t, err)
	row := mustPayee(t, env.ctx, db, transferNo)
	if row.status != statusConfirmed || row.txn != confirmed.TxnNo || row.version != 1 || countPayee(t, env.ctx, db, transferNo) != 1 {
		t.Fatalf("branch = %+v", row)
	}
	if countTxnBiz(t, env.ctx, db, bizTransferIn, transferNo) != 1 {
		t.Fatal("cancel removed or added a voucher")
	}
	assertBalances(t, env.ctx, db, merchantID, opening+int64(amount), 0, transitID, -int64(amount), 0)
}

// ACC-14 验收 5：同一 transfer_no，Try 与 Cancel 各 20 个 goroutine。最终 CANCELLED，没有凭证，余额不变。
// ACC-10 未做，CheckBalance 用 SQL 断言代替。
func TestTransferInTryCancelRace(t *testing.T) {
	env := openEnv(t)
	const payee = "M100002"
	const transferNo = "IN-RACE-1"
	const opening int64 = 500
	const amount = money.Amount(30)
	const n = 20
	shardNo, _ := shard.Of(payee)
	_, merchantID, db := openSettle(t, env, payee, opening, 0)
	transitID := openTransitSub(t, env, transferNo, shardNo, 0)
	// 40 个事务抢同一行。连接池放到 64，避免在 WithTx 的 3 秒里等连接。不在进事务前加信号量。
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)
	env.s1.DB.SetMaxOpenConns(4)
	env.s1.DB.SetMaxIdleConns(4)
	ctx, cancel := context.WithTimeout(env.ctx, 120*time.Second)
	defer cancel()

	results := make([]TransferInResult, n*2)
	errs := make([]error, n*2)
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = env.svc.TransferInTry(ctx, transferNo, payee, amount)
		}(i)
		go func(i int) {
			defer wg.Done()
			results[n+i], errs[n+i] = env.svc.TransferInCancel(ctx, transferNo, payee, amount)
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] == nil {
			if results[i].Status != statusTried || results[i].TxnNo != "" {
				t.Fatalf("try %d = %+v", i, results[i])
			}
			continue
		}
		if !errors.Is(errs[i], errno.ErrFreezeStatusConflict) {
			t.Fatalf("try %d: %s (%s)", i, testkit.Redact(errs[i]), transferErrDetail(errs[i]))
		}
		assertEmptyIn(t, results[i])
	}
	for i := n; i < n*2; i++ {
		if errs[i] != nil || results[i].Status != statusCancelled || results[i].TxnNo != "" {
			t.Fatalf("cancel %d = %+v err=%v", i, results[i], errs[i])
		}
	}
	row := mustPayee(t, ctx, db, transferNo)
	if row.status != statusCancelled || row.payee != payee || row.amount != int64(amount) || row.txn != "" {
		t.Fatalf("final = %+v", row)
	}
	if row.version != 0 && row.version != 1 {
		t.Fatalf("version = %d", row.version)
	}
	if countPayee(t, ctx, db, transferNo) != 1 || countTxnBiz(t, ctx, db, bizTransferIn, transferNo) != 0 {
		t.Fatal("race left a voucher or extra branch")
	}
	assertBalances(t, ctx, db, merchantID, opening, 0, transitID, 0, 0)
	late, err := env.svc.TransferInTry(ctx, transferNo, payee, amount+1)
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	assertEmptyIn(t, late)
	if mustPayee(t, ctx, db, transferNo).amount != int64(amount) {
		t.Fatal("late try rewrote the tombstone")
	}
	assertNoPayeeOnShard(t, ctx, env.s1.DB, transferNo)
}

// ACC-14：金额或收款商户不一致是 10006。账户不存在、非 ACTIVE、在途不存在、确认时停用，都不记账。
func TestTransferInConflictsAndAccounts(t *testing.T) {
	env := openEnv(t)
	const payee = "M100002"
	const other = "M100004"
	shardNo, err := shard.Of(payee)
	if err != nil || shardNo != 0 {
		t.Fatalf("shard = %d err=%v", shardNo, err)
	}
	if got, err := shard.Of(other); err != nil || got != shardNo {
		t.Fatalf("other shard = %d err=%v", got, err)
	}
	_, merchantID, db := openSettle(t, env, payee, 300, 0)
	_, otherID, _ := openSettle(t, env, other, 70, 0)

	missing, err := env.svc.TransferInTry(env.ctx, "IN-MISSING", "M100006", 10)
	assertErrno(t, err, errno.ErrAccountNotFound)
	assertEmptyIn(t, missing)
	if countPayee(t, env.ctx, db, "IN-MISSING") != 0 {
		t.Fatal("missing account wrote a branch")
	}

	closed, closedID, _ := openSettle(t, env, "M100008", 10, 0)
	setAccountStatus(t, env.ctx, db, closedID, statusFrozen)
	inactive, err := env.svc.TransferInTry(env.ctx, "IN-INACTIVE", "M100008", 10)
	assertErrno(t, err, errno.ErrAccountInactive)
	assertEmptyIn(t, inactive)
	if countPayee(t, env.ctx, db, "IN-INACTIVE") != 0 {
		t.Fatal("inactive try wrote a branch")
	}
	bal, frozen, status, version := balanceOf(t, env.ctx, db, closedID)
	if bal != 10 || frozen != 0 || status != statusFrozen || version != 0 {
		t.Fatalf("inactive account changed %d/%d %s %d", bal, frozen, status, version)
	}
	_ = closed

	if _, err = env.svc.TransferInTry(env.ctx, "IN-DIFF", payee, 40); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	amountGot, err := env.svc.TransferInTry(env.ctx, "IN-DIFF", payee, 41)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	assertEmptyIn(t, amountGot)
	payeeGot, err := env.svc.TransferInTry(env.ctx, "IN-DIFF", other, 40)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	assertEmptyIn(t, payeeGot)
	row := mustPayee(t, env.ctx, db, "IN-DIFF")
	if row.payee != payee || row.amount != 40 || row.status != statusTried || row.version != 0 {
		t.Fatalf("conflict rewrote %+v", row)
	}
	confirmMismatch, err := env.svc.TransferInConfirm(env.ctx, "IN-DIFF", other)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	assertEmptyIn(t, confirmMismatch)
	if countTxnBiz(t, env.ctx, db, bizTransferIn, "IN-DIFF") != 0 || mustPayee(t, env.ctx, db, "IN-DIFF").status != statusTried {
		t.Fatal("mismatch confirm posted")
	}
	assertBalances(t, env.ctx, db, merchantID, 300, 0, otherID, 70, 0)

	if _, err = env.svc.TransferInCancel(env.ctx, "IN-DIFF", payee, 40); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	late, err := env.svc.TransferInTry(env.ctx, "IN-DIFF", payee, 99)
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	assertEmptyIn(t, late)
	if mustPayee(t, env.ctx, db, "IN-DIFF").amount != 40 || mustPayee(t, env.ctx, db, "IN-DIFF").status != statusCancelled {
		t.Fatal("cancelled try compared amount")
	}

	if _, err = env.svc.TransferInTry(env.ctx, "IN-NO-TRANSIT", payee, 12); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	noTransit, err := env.svc.TransferInConfirm(env.ctx, "IN-NO-TRANSIT", payee)
	assertErrno(t, err, errno.ErrAccountNotFound)
	assertEmptyIn(t, noTransit)
	if mustPayee(t, env.ctx, db, "IN-NO-TRANSIT").status != statusTried || countTxnBiz(t, env.ctx, db, bizTransferIn, "IN-NO-TRANSIT") != 0 {
		t.Fatal("missing transit changed the branch")
	}

	const stoppedNo = "IN-STOPPED"
	transitID := openTransitSub(t, env, stoppedNo, shardNo, 0)
	if _, err = env.svc.TransferInTry(env.ctx, stoppedNo, payee, 20); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	setAccountStatus(t, env.ctx, db, merchantID, statusFrozen)
	stopped, err := env.svc.TransferInConfirm(env.ctx, stoppedNo, payee)
	assertErrno(t, err, errno.ErrAccountInactive)
	assertEmptyIn(t, stopped)
	if mustPayee(t, env.ctx, db, stoppedNo).status != statusTried || countTxnBiz(t, env.ctx, db, bizTransferIn, stoppedNo) != 0 {
		t.Fatal("inactive confirm changed the branch")
	}
	assertBalances(t, env.ctx, db, merchantID, 300, 0, transitID, 0, 0)
	setAccountStatus(t, env.ctx, db, merchantID, statusActive)

	const okNo = "IN-OTHER-PAYEE"
	okTransit := openTransitSub(t, env, okNo, shardNo, 0)
	if _, err = env.svc.TransferInTry(env.ctx, okNo, payee, 25); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	confirmed, err := env.svc.TransferInConfirm(env.ctx, okNo, payee)
	if err != nil || confirmed.TxnNo == "" {
		t.Fatalf("confirm = %+v err=%v", confirmed, err)
	}
	replay, err := env.svc.TransferInConfirm(env.ctx, okNo, other)
	if err != nil || replay.TxnNo != confirmed.TxnNo || replay.Status != statusConfirmed {
		t.Fatalf("other payee replay = %+v err=%v", replay, err)
	}
	if countTxnBiz(t, env.ctx, db, bizTransferIn, okNo) != 1 || mustPayee(t, env.ctx, db, okNo).payee != payee {
		t.Fatal("other payee confirm posted again")
	}
	assertBalances(t, env.ctx, db, merchantID, 325, 0, okTransit, -25, 0)
	otherBal, otherFrozen, _, _ := balanceOf(t, env.ctx, db, otherID)
	if otherBal != 70 || otherFrozen != 0 {
		t.Fatalf("other merchant = %d/%d", otherBal, otherFrozen)
	}

	ids, err := idgen.New(4, stubClock{now: openTestTime})
	if err != nil {
		t.Fatal(err)
	}
	bare := New(env.pools, nil, 0)
	bareGot, err := bare.TransferInTry(env.ctx, "IN-BARE", payee, 5)
	if err != nil || bareGot.Status != statusTried || bareGot.TxnNo != "" {
		t.Fatalf("bare try = %+v err=%v", bareGot, err)
	}
	bareCancel, err := bare.TransferInCancel(env.ctx, "IN-BARE-CANCEL", payee, 5)
	if err != nil || bareCancel.Status != statusCancelled {
		t.Fatalf("bare cancel = %+v err=%v", bareCancel, err)
	}
	if countTxnBiz(t, env.ctx, db, bizTransferIn, "IN-BARE") != 0 {
		t.Fatal("bare try posted")
	}
	low := New(env.pools, ids, 0)
	lowGot, err := low.TransferInConfirm(env.ctx, "IN-BARE", payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, lowGot)
	if mustPayee(t, env.ctx, db, "IN-BARE").status != statusTried || countTxnBiz(t, env.ctx, db, bizTransferIn, "IN-BARE") != 0 {
		t.Fatal("subCount 0 confirm wrote")
	}
	noID := New(env.pools, nil, testSubCount)
	noIDGot, err := noID.TransferInConfirm(env.ctx, "IN-BARE", payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, noIDGot)
	huge := New(env.pools, ids, int(math.MaxUint32)+1)
	hugeGot, err := huge.TransferInConfirm(env.ctx, "IN-BARE", payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, hugeGot)
	if mustPayee(t, env.ctx, db, "IN-BARE").status != statusTried {
		t.Fatal("huge subCount confirm wrote")
	}

	before := snapShard(t, env.ctx, db)
	_, err = env.svc.TransferInTry(env.ctx, "", payee, 1)
	assertErrno(t, err, errno.ErrInvalidParam)
	if snapShard(t, env.ctx, db) != before {
		t.Fatal("invalid try wrote")
	}
}

// ACC-14：Confirm 时商户行已被删掉，返回 40001，分支保持 TRIED。
func TestTransferInConfirmMissingMerchant(t *testing.T) {
	env := openEnv(t)
	const payee = "M100002"
	const transferNo = "IN-NO-MERCHANT"
	acc, merchantID, db := openSettle(t, env, payee, 20, 0)
	shardNo, _ := shard.Of(payee)
	openTransitSub(t, env, transferNo, shardNo, 0)
	if _, err := env.svc.TransferInTry(env.ctx, transferNo, payee, 8); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	res, err := db.ExecContext(env.ctx, `DELETE FROM account WHERE id = ?`, merchantID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("delete n=%d err=%v", n, err)
	}
	_ = acc
	got, err := env.svc.TransferInConfirm(env.ctx, transferNo, payee)
	assertErrno(t, err, errno.ErrAccountNotFound)
	assertEmptyIn(t, got)
	row := mustPayee(t, env.ctx, db, transferNo)
	if row.status != statusTried || row.version != 0 || countTxnBiz(t, env.ctx, db, bizTransferIn, transferNo) != 0 {
		t.Fatalf("branch = %+v", row)
	}
}

type storedPayee struct {
	payee   string
	amount  int64
	status  string
	txn     string
	version int64
}

func mustPayee(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) storedPayee {
	t.Helper()
	var (
		payer, fail, txn sql.NullString
		payee, status    string
		amount, version  int64
	)
	err := db.QueryRowContext(ctx, `
SELECT payer_merchant_id, payee_merchant_id, amount, status, fail_code, txn_no, version
FROM transfer_branch WHERE transfer_no = ? AND role = ?`, transferNo, rolePayee).
		Scan(&payer, &payee, &amount, &status, &fail, &txn, &version)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if payer.Valid || fail.Valid {
		t.Fatalf("payer or fail_code set payer=%v fail=%v", payer.String, fail.String)
	}
	return storedPayee{payee: payee, amount: amount, status: status, txn: txn.String, version: version}
}

func countPayee(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM transfer_branch WHERE transfer_no = ? AND role = ?`, transferNo, rolePayee).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func assertNoPayeeOnShard(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) {
	t.Helper()
	if countPayee(t, ctx, db, transferNo) != 0 || countTxnBiz(t, ctx, db, bizTransferIn, transferNo) != 0 {
		t.Fatal("other shard has this payee branch")
	}
}

func assertBalances(t *testing.T, ctx context.Context, db *sqlx.DB, merchantID uint64, merchantBal, merchantFrozen int64, transitID uint64, transitBal, transitFrozen int64) {
	t.Helper()
	bal, frozen, status, _ := balanceOf(t, ctx, db, merchantID)
	if bal != merchantBal || frozen != merchantFrozen || status == "" {
		t.Fatalf("merchant balance=%d frozen=%d status=%s", bal, frozen, status)
	}
	bal, frozen, status, _ = balanceOf(t, ctx, db, transitID)
	if bal != transitBal || frozen != transitFrozen || status == "" {
		t.Fatalf("transit balance=%d frozen=%d status=%s", bal, frozen, status)
	}
}

func openTransitSub(t *testing.T, env shardEnv, transferNo string, shardNo int, balance int64) uint64 {
	t.Helper()
	sub, err := shard.Sub(transferNo, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	acc, id, db := openOn(t, env, ownerPlatform, platformOwnerID, typeTransit, shardNo, sub)
	if balance != 0 {
		setAmount(t, env.ctx, db, acc.AccountNo, balance, 0)
	}
	return id
}

func otherTransitSub(t *testing.T, transferNo string) int {
	t.Helper()
	sub, err := shard.Sub(transferNo, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	return (sub + 1) % testSubCount
}
