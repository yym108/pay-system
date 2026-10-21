//go:build integration

package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

func assertCheckPass(t *testing.T, ctx context.Context, svc *Service, merchantID string, quiet bool) {
	t.Helper()
	got, err := svc.CheckBalance(ctx, merchantID, quiet)
	if err != nil {
		t.Fatalf("CheckBalance(%q, %v): %s", merchantID, quiet, testkit.Redact(err))
	}
	if !got.Passed || len(got.Failures) != 0 {
		t.Fatalf("CheckBalance(%q, %v) = %+v", merchantID, quiet, got)
	}
}

func postPay(t *testing.T, ctx context.Context, db *sqlx.DB, svc *Service, bizNo string, channelID, merchantID uint64, amount int64) {
	t.Helper()
	_, err := postCommitted(ctx, db, svc, bizPay, bizNo, "", []Entry{
		{AccountID: channelID, Direction: directionDebit, Amount: money.Amount(amount)},
		{AccountID: merchantID, Direction: directionCredit, Amount: money.Amount(amount)},
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func openChannel(t *testing.T, env shardEnv, shardNo int) uint64 {
	t.Helper()
	_, id, db := openOn(t, env, ownerPlatform, "mockpay", typeChannelReceivable, shardNo, 0)
	if shardNo == 0 && db != env.s0.DB || shardNo == 1 && db != env.s1.DB {
		t.Fatal("channel opened on the wrong shard")
	}
	return id
}

// ACC-10 验收 1：正常数据全量和商户模式都通过。不存在的商户是 40001，计数不变。
func TestCheckBalanceClean(t *testing.T) {
	env := openEnv(t)
	before := ledgerCheckFailures.Load()
	assertCheckPass(t, env.ctx, env.svc, "", false)
	assertCheckPass(t, env.ctx, env.svc, "", true)

	_, payerID, db1 := openSettle(t, env, "M100001", 0, 0)
	_, peerID, peerDB := openSettle(t, env, "M100005", 0, 0)
	if peerDB != db1 {
		t.Fatal("same-shard payee opened elsewhere")
	}
	_, payeeID, db0 := openSettle(t, env, "M100002", 0, 0)
	ch1 := openChannel(t, env, 1)
	ch0 := openChannel(t, env, 0)
	postPay(t, env.ctx, db1, env.svc, "FUND-PAYER", ch1, payerID, 1000)
	postPay(t, env.ctx, db1, env.svc, "FUND-PEER", ch1, peerID, 400)
	postPay(t, env.ctx, db0, env.svc, "FUND-PAYEE", ch0, payeeID, 200)

	frozen, err := env.svc.Freeze(env.ctx, bizRefund, "CLEAN-HOLD", "M100002", 30)
	if err != nil || frozen.Status != statusFrozen {
		t.Fatalf("freeze = %+v err=%v", frozen, err)
	}
	released, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, "CLEAN-HOLD", "M100002", 30)
	if err != nil || released.Status != statusReleased {
		t.Fatalf("release = %+v err=%v", released, err)
	}

	local, err := env.svc.TransferLocal(env.ctx, "TL-CLEAN-1", "M100001", "M100005", 50)
	if err != nil || local.Status != statusSuccess || local.TxnNo == "" {
		t.Fatalf("local = %+v err=%v", local, err)
	}

	const transferNo = "TF-CLEAN-1"
	const amount = money.Amount(80)
	openTransitSub(t, env, transferNo, 1, 0)
	openTransitSub(t, env, transferNo, 0, 0)
	hold, err := env.svc.Freeze(env.ctx, bizTransfer, transferNo, "M100001", amount)
	if err != nil || hold.Status != statusFrozen {
		t.Fatalf("transfer freeze = %+v err=%v", hold, err)
	}
	confirmed, err := env.svc.ConfirmFreeze(env.ctx, bizTransfer, transferNo, "M100001", "")
	if err != nil || confirmed.TxnNo == "" {
		t.Fatalf("confirm = %+v err=%v", confirmed, err)
	}
	tried, err := env.svc.TransferInTry(env.ctx, transferNo, "M100002", amount)
	if err != nil || tried.Status != statusTried {
		t.Fatalf("try = %+v err=%v", tried, err)
	}
	in, err := env.svc.TransferInConfirm(env.ctx, transferNo, "M100002")
	if err != nil || in.Status != statusConfirmed || in.TxnNo == "" {
		t.Fatalf("transfer in = %+v err=%v", in, err)
	}

	snap0 := snapShard(t, env.ctx, env.s0.DB)
	snap1 := snapShard(t, env.ctx, env.s1.DB)
	assertCheckPass(t, env.ctx, env.svc, "", false)
	assertCheckPass(t, env.ctx, env.svc, "M100001", false)
	// 在途已经回零。商户模式即使传入 true 也只核对该结算账户。
	assertCheckPass(t, env.ctx, env.svc, "M100002", true)
	assertCheckPass(t, env.ctx, env.svc, "", true)
	again, err := env.svc.CheckBalance(env.ctx, "", false)
	if err != nil || !again.Passed {
		t.Fatalf("repeat = %+v err=%v", again, err)
	}

	miss, err := env.svc.CheckBalance(env.ctx, "M100003", true)
	assertErrno(t, err, errno.ErrAccountNotFound)
	assertNoLeak(t, err)
	if miss.Passed || len(miss.Failures) != 0 {
		t.Fatalf("missing = %+v", miss)
	}
	if ledgerCheckFailures.Load() != before {
		t.Fatalf("counter = %d, want %d", ledgerCheckFailures.Load(), before)
	}
	if snapShard(t, env.ctx, env.s0.DB) != snap0 || snapShard(t, env.ctx, env.s1.DB) != snap1 {
		t.Fatal("check wrote rows")
	}
}

// ACC-10 验收 2：SQL 把余额加 1 分后指出该账户，计数加 1 一次。
func TestCheckBalanceTamperBalance(t *testing.T) {
	env := openEnv(t)
	acc, id, db := openSettle(t, env, "M100002", 0, 0)
	ch := openChannel(t, env, 0)
	postPay(t, env.ctx, db, env.svc, "FUND-TAMPER", ch, id, 100)
	res, err := db.ExecContext(env.ctx, `UPDATE account SET balance = balance + 1 WHERE id = ?`, id)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("tamper n=%d err=%v", n, err)
	}
	shardNo, err := shard.Of("M100002")
	if err != nil || shardNo != 0 {
		t.Fatalf("shard = %d err=%v", shardNo, err)
	}

	before := ledgerCheckFailures.Load()
	got, err := env.svc.CheckBalance(env.ctx, "", false)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Passed || len(got.Failures) == 0 {
		t.Fatalf("result = %+v", got)
	}
	var hit *BalanceFailure
	for i := range got.Failures {
		f := &got.Failures[i]
		if f.Check != "BALANCE_SUM" || f.AccountNo != acc.AccountNo {
			continue
		}
		hit = f
		break
	}
	if hit == nil || hit.Shard == nil || *hit.Shard != shardNo {
		t.Fatalf("failures = %+v", got.Failures)
	}
	diff, err := money.Sub(hit.Actual, hit.Expected)
	if err != nil || diff != 1 {
		t.Fatalf("expected %d actual %d err=%v", hit.Expected, hit.Actual, err)
	}
	failDelta(t, before, 1)
}

// ACC-10 验收 3：只完成付款确认时全量通过；静止时期望在途为 0。
// 篡改 PAYEE 为 CONFIRMED 且不记账后，收款方分片的预期在途对不上。
func TestCheckBalancePartialTransferAndTamperedPayee(t *testing.T) {
	env := openEnv(t)
	_, payerID, db1 := openSettle(t, env, "M100001", 0, 0)
	_, payeeID, db0 := openSettle(t, env, "M100002", 0, 0)
	ch1 := openChannel(t, env, 1)
	postPay(t, env.ctx, db1, env.svc, "FUND-PARTIAL", ch1, payerID, 1000)
	const transferNo = "TF-PART-1"
	const amount = money.Amount(80)
	openTransitSub(t, env, transferNo, 1, 0)
	hold, err := env.svc.Freeze(env.ctx, bizTransfer, transferNo, "M100001", amount)
	if err != nil || hold.Status != statusFrozen {
		t.Fatalf("freeze = %+v err=%v", hold, err)
	}
	confirmed, err := env.svc.ConfirmFreeze(env.ctx, bizTransfer, transferNo, "M100001", "")
	if err != nil || confirmed.TxnNo == "" {
		t.Fatalf("confirm = %+v err=%v", confirmed, err)
	}
	tried, err := env.svc.TransferInTry(env.ctx, transferNo, "M100002", amount)
	if err != nil || tried.Status != statusTried {
		t.Fatalf("try = %+v err=%v", tried, err)
	}
	if payeeBal, _, _, _ := balanceOf(t, env.ctx, db0, payeeID); payeeBal != 0 {
		t.Fatalf("try changed payee balance to %d", payeeBal)
	}

	before := ledgerCheckFailures.Load()
	assertCheckPass(t, env.ctx, env.svc, "", false)
	// 在途合计不是 0。商户模式忽略 expect_quiescent，不能因此失败。
	assertCheckPass(t, env.ctx, env.svc, "M100001", true)
	assertCheckPass(t, env.ctx, env.svc, "M100002", true)
	if ledgerCheckFailures.Load() != before {
		t.Fatal("passing checks incremented the counter")
	}

	quiet, err := env.svc.CheckBalance(env.ctx, "", true)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if quiet.Passed {
		t.Fatal("quiescent passed while transit is outstanding")
	}
	assertFailures(t, quiet.Failures, []BalanceFailure{{
		Check: "TRANSIT_ZERO", Expected: 0, Actual: amount,
	}})
	failDelta(t, before, 1)

	res, err := db0.ExecContext(env.ctx, `
UPDATE transfer_branch SET status = 'CONFIRMED'
WHERE transfer_no = ? AND role = ? AND status = 'TRIED'`, transferNo, rolePayee)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("tamper branch n=%d err=%v", n, err)
	}
	if countTxnBiz(t, env.ctx, db0, bizTransferIn, transferNo) != 0 {
		t.Fatal("status tamper posted a voucher")
	}

	got, err := env.svc.CheckBalance(env.ctx, "", false)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Passed {
		t.Fatal("tampered payee passed")
	}
	payeeShard, err := shard.Of("M100002")
	if err != nil || payeeShard != 0 {
		t.Fatalf("payee shard = %d", payeeShard)
	}
	var hit *BalanceFailure
	for i := range got.Failures {
		f := &got.Failures[i]
		if f.Check == "TRANSIT_EXPECTED" && f.Shard != nil && *f.Shard == payeeShard && f.AccountNo == "" {
			hit = f
			break
		}
	}
	if hit == nil || hit.Expected != -amount || hit.Actual != 0 {
		t.Fatalf("failures = %+v", got.Failures)
	}
	failDelta(t, before, 2)
}

// ACC-10 验收 4：并发记账和转账期间全量核对不误报。终态后在途合计为 0。
func TestCheckBalanceDuringConcurrentPostAndTransfer(t *testing.T) {
	env := openEnv(t)
	// 两个分片同时有确认和核对。加大池，让等待留在调用方的 120 秒里，而不是耗尽 WithTx 的 3 秒。
	env.s0.DB.SetMaxOpenConns(32)
	env.s0.DB.SetMaxIdleConns(32)
	env.s1.DB.SetMaxOpenConns(32)
	env.s1.DB.SetMaxIdleConns(32)

	_, payerID, db1 := openSettle(t, env, "M100001", 0, 0)
	_, peerID, _ := openSettle(t, env, "M100005", 0, 0)
	openSettle(t, env, "M100002", 0, 0)
	ch1 := openChannel(t, env, 1)
	postPay(t, env.ctx, db1, env.svc, "FUND-RACE-PAYER", ch1, payerID, 20000)
	postPay(t, env.ctx, db1, env.svc, "FUND-RACE-PEER", ch1, peerID, 5000)

	const crossN = 3
	const crossAmount = money.Amount(20)
	for i := 0; i < crossN; i++ {
		transferNo := "XR" + string(rune('0'+i))
		openTransitSub(t, env, transferNo, 1, 0)
		openTransitSub(t, env, transferNo, 0, 0)
		hold, err := env.svc.Freeze(env.ctx, bizTransfer, transferNo, "M100001", crossAmount)
		if err != nil || hold.Status != statusFrozen {
			t.Fatalf("freeze %s = %+v err=%v", transferNo, hold, err)
		}
		tried, err := env.svc.TransferInTry(env.ctx, transferNo, "M100002", crossAmount)
		if err != nil || tried.Status != statusTried {
			t.Fatalf("try %s = %+v err=%v", transferNo, tried, err)
		}
	}

	ctx, cancel := context.WithTimeout(env.ctx, 120*time.Second)
	defer cancel()
	before := ledgerCheckFailures.Load()

	const localN = 4
	const postN = 3
	const checkG = 4
	const checksEach = 3
	var wg sync.WaitGroup
	localErr := make([]error, localN*2)
	postErr := make([]error, postN)
	confirmErr := make([]error, crossN)
	inErr := make([]error, crossN)
	checkErr := make([]error, checkG*checksEach)
	checkPass := make([]bool, checkG*checksEach)

	wg.Add(localN*2 + postN + crossN*2 + checkG)
	for i := 0; i < localN; i++ {
		go func(i int) {
			defer wg.Done()
			res, err := env.svc.TransferLocal(ctx, "LA"+string(rune('0'+i)), "M100001", "M100005", 1)
			localErr[i] = err
			if err == nil && (res.Status != statusSuccess || res.TxnNo == "") {
				localErr[i] = errors.New("local not success")
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			res, err := env.svc.TransferLocal(ctx, "LB"+string(rune('0'+i)), "M100005", "M100001", 1)
			localErr[localN+i] = err
			if err == nil && (res.Status != statusSuccess || res.TxnNo == "") {
				localErr[localN+i] = errors.New("local not success")
			}
		}(i)
	}
	for i := 0; i < postN; i++ {
		go func(i int) {
			defer wg.Done()
			_, postErr[i] = postCommitted(ctx, db1, env.svc, bizPay, "RACE-POST-"+string(rune('0'+i)), "", []Entry{
				{AccountID: ch1, Direction: directionDebit, Amount: 1},
				{AccountID: payerID, Direction: directionCredit, Amount: 1},
			})
		}(i)
	}
	for i := 0; i < crossN; i++ {
		transferNo := "XR" + string(rune('0'+i))
		go func(i int, transferNo string) {
			defer wg.Done()
			res, err := env.svc.ConfirmFreeze(ctx, bizTransfer, transferNo, "M100001", "")
			confirmErr[i] = err
			if err == nil && res.TxnNo == "" {
				confirmErr[i] = errors.New("confirm missing txn")
			}
		}(i, transferNo)
		go func(i int, transferNo string) {
			defer wg.Done()
			res, err := env.svc.TransferInConfirm(ctx, transferNo, "M100002")
			inErr[i] = err
			if err == nil && (res.Status != statusConfirmed || res.TxnNo == "") {
				inErr[i] = errors.New("transfer in not confirmed")
			}
		}(i, transferNo)
	}
	for g := 0; g < checkG; g++ {
		go func(g int) {
			defer wg.Done()
			for n := 0; n < checksEach; n++ {
				idx := g*checksEach + n
				got, err := env.svc.CheckBalance(ctx, "", false)
				checkErr[idx] = err
				checkPass[idx] = err == nil && got.Passed && len(got.Failures) == 0
			}
		}(g)
	}
	wg.Wait()

	for i, err := range localErr {
		if err != nil {
			t.Fatalf("local %d: %s", i, testkit.Redact(err))
		}
	}
	for i, err := range postErr {
		if err != nil {
			t.Fatalf("post %d: %s", i, testkit.Redact(err))
		}
	}
	for i, err := range confirmErr {
		if err != nil {
			t.Fatalf("confirm %d: %s", i, testkit.Redact(err))
		}
	}
	for i, err := range inErr {
		if err != nil {
			t.Fatalf("transfer in %d: %s", i, testkit.Redact(err))
		}
	}
	for i, err := range checkErr {
		if err != nil || !checkPass[i] {
			t.Fatalf("check %d pass=%v err=%v", i, checkPass[i], err)
		}
	}
	if ledgerCheckFailures.Load() != before {
		t.Fatalf("counter moved by %d", ledgerCheckFailures.Load()-before)
	}
	assertCheckPass(t, ctx, env.svc, "", true)
	if ledgerCheckFailures.Load() != before {
		t.Fatal("final quiescent check incremented the counter")
	}
}
