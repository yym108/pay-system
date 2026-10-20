//go:build integration

package biz

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

// ACC-05 验收 1：冻结后解冻，frozen_amount 恢复。重复解冻幂等，不再减冻结，也不改余额。
func TestReleaseAfterFreezeRestoresFrozen(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	const (
		opening int64 = 1000
		already int64 = 10
		amount        = money.Amount(40)
	)
	setAmount(t, env.ctx, db, merchant.AccountNo, opening, already)
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
	freeze1Before := countFreeze(t, env.ctx, env.s1.DB)

	frozen, err := env.svc.Freeze(env.ctx, bizRefund, "ACC05-REL", "M100002", amount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if frozen.Duplicated || frozen.Status != statusFrozen || frozen.FreezeNo == "" {
		t.Fatalf("freeze = %+v", frozen)
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != opening || frozenAmount != already+int64(amount) || status != statusActive || version != 1 {
		t.Fatalf("after freeze balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}

	released, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, "ACC05-REL", "M100002", amount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if released.Duplicated || released.Status != statusReleased || released.FreezeNo != frozen.FreezeNo {
		t.Fatalf("release = %+v, freeze_no %s", released, frozen.FreezeNo)
	}
	balance, frozenAmount, status, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != opening || frozenAmount != already || status != statusActive || version != 2 {
		t.Fatalf("after release balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, "ACC05-REL")
	if storedNo != frozen.FreezeNo || storedAccount != merchantID || storedAmount != int64(amount) || storedStatus != statusReleased || txnNo.Valid || freezeVersion != 1 {
		t.Fatalf("stored no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}

	again, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, "ACC05-REL", "M100002", amount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !again.Duplicated || again.FreezeNo != frozen.FreezeNo || again.Status != statusReleased {
		t.Fatalf("replay = %+v", again)
	}
	balance, frozenAmount, status, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != opening || frozenAmount != already || status != statusActive || version != 2 {
		t.Fatalf("after replay balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion = loadFreeze(t, env.ctx, db, bizRefund, "ACC05-REL")
	if storedNo != frozen.FreezeNo || storedAccount != merchantID || storedAmount != int64(amount) || storedStatus != statusReleased || txnNo.Valid || freezeVersion != 1 {
		t.Fatalf("replay row no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}
	if countFreeze(t, env.ctx, db) != 1 || countFreeze(t, env.ctx, env.s1.DB) != freeze1Before {
		t.Fatal("release wrote an extra freeze row")
	}
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatal("release wrote a ledger row")
	}
}

// 同一张 FROZEN 单，16 个 goroutine 同时解冻：恰好 1 次真正减少冻结，其余幂等成功。
// 16 个 goroutine 抢同一张冻结单，各自走 ReleaseFreeze 自己的 WithTx。
// 单次事务上限 3 秒，含等连接和等行锁。把这个库的最大连接收到 8，父上下文给 60 秒。
// 这是测试设置，避免等待堆过驱动的 3 秒读超时。
func TestReleaseConcurrentSameFrozen(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	tightenPool(db, 8)
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 500, 0)
	const (
		n      = 16
		amount = money.Amount(40)
		bizNo  = "ACC05-SAME"
	)
	first, err := env.svc.Freeze(env.ctx, bizRefund, bizNo, "M100002", amount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	ctx, cancel := context.WithTimeout(env.ctx, 60*time.Second)
	defer cancel()
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
			results[i], errs[i] = env.svc.ReleaseFreeze(ctx, bizRefund, bizNo, "M100002", amount)
		}(i)
	}
	ready.Wait()
	close(start)
	wg.Wait()

	fresh := 0
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
		}
		if results[i].FreezeNo != first.FreezeNo || results[i].Status != statusReleased {
			t.Fatalf("call %d = %+v", i, results[i])
		}
		if !results[i].Duplicated {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh=%d", fresh)
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 500 || frozenAmount != 0 || status != statusActive || version != 2 {
		t.Fatalf("balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	storedNo, _, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, bizNo)
	if storedNo != first.FreezeNo || storedAmount != int64(amount) || storedStatus != statusReleased || txnNo.Valid || freezeVersion != 1 {
		t.Fatalf("stored no=%s amount=%d status=%s txn=%v version=%d", storedNo, storedAmount, storedStatus, txnNo, freezeVersion)
	}
	if countFreeze(t, env.ctx, db) != 1 {
		t.Fatalf("freeze rows=%d", countFreeze(t, env.ctx, db))
	}
}

// ACC-05 验收 2：对不存在的 biz_no 解冻成功并生成墓碑；随后冻结返回 40004，冻结金额不变。
func TestReleaseEmptyRollbackBlocksFreeze(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 200, 40)
	freeze1Before := countFreeze(t, env.ctx, env.s1.DB)
	const bizNo = "ACC05-TOMB"

	got, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, bizNo, "M100002", 15)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Duplicated || got.Status != statusReleased || got.FreezeNo == "" {
		t.Fatalf("tombstone = %+v", got)
	}
	parsed, err := idgen.Parse(got.FreezeNo)
	if err != nil || parsed.Prefix != "FZ" {
		t.Fatalf("freeze_no %s: %v", got.FreezeNo, err)
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 200 || frozenAmount != 40 || status != statusActive || version != 0 {
		t.Fatalf("after tombstone balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, bizNo)
	if storedNo != got.FreezeNo || storedAccount != merchantID || storedAmount != 15 || storedStatus != statusReleased || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("stored no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}

	_, err = env.svc.Freeze(env.ctx, bizRefund, bizNo, "M100002", 15)
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	balance, frozenAmount, status, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 200 || frozenAmount != 40 || status != statusActive || version != 0 {
		t.Fatalf("after late freeze balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion = loadFreeze(t, env.ctx, db, bizRefund, bizNo)
	if storedNo != got.FreezeNo || storedAccount != merchantID || storedAmount != 15 || storedStatus != statusReleased || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("late freeze changed no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}

	// 墓碑再解冻，金额不同也不比较。
	again, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, bizNo, "M100002", 99)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !again.Duplicated || again.FreezeNo != got.FreezeNo || again.Status != statusReleased {
		t.Fatalf("replay = %+v", again)
	}
	balance, frozenAmount, _, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 200 || frozenAmount != 40 || version != 0 {
		t.Fatalf("after replay balance=%d frozen=%d version=%d", balance, frozenAmount, version)
	}
	if countFreeze(t, env.ctx, db) != 1 || countFreeze(t, env.ctx, env.s1.DB) != freeze1Before {
		t.Fatal("tombstone wrote an extra row")
	}
}

// 16 个 goroutine 同时对一个新 biz_no 做空回滚：恰好 1 行墓碑，其余幂等成功。
// 和上面抢同一行的解冻一样，把这个库的最大连接收到 8，父上下文给 60 秒。这是测试设置。
func TestReleaseConcurrentEmptyRollback(t *testing.T) {
	env := openEnv(t)
	db := env.s0.DB
	tightenPool(db, 8)
	merchant, merchantID, _ := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 80, 5)
	ctx, cancel := context.WithTimeout(env.ctx, 60*time.Second)
	defer cancel()
	const (
		n      = 16
		amount = money.Amount(12)
		bizNo  = "ACC05-EMPTY"
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
			results[i], errs[i] = env.svc.ReleaseFreeze(ctx, bizRefund, bizNo, "M100002", amount)
		}(i)
	}
	ready.Wait()
	close(start)
	wg.Wait()

	var freezeNo string
	fresh := 0
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
		}
		if results[i].Status != statusReleased || results[i].FreezeNo == "" {
			t.Fatalf("call %d = %+v", i, results[i])
		}
		if results[i].Duplicated {
			continue
		}
		fresh++
		freezeNo = results[i].FreezeNo
	}
	if fresh != 1 {
		t.Fatalf("fresh=%d", fresh)
	}
	for i := range results {
		if results[i].FreezeNo != freezeNo {
			t.Fatalf("call %d freeze_no=%s, want %s", i, results[i].FreezeNo, freezeNo)
		}
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 80 || frozenAmount != 5 || status != statusActive || version != 0 {
		t.Fatalf("balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	if countFreeze(t, env.ctx, db) != 1 {
		t.Fatalf("freeze rows=%d", countFreeze(t, env.ctx, db))
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, bizNo)
	if storedNo != freezeNo || storedAccount != merchantID || storedAmount != int64(amount) || storedStatus != statusReleased || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("stored no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}
}

// ACC-05 验收 3：已确认的冻结单解冻返回 40004。金额不同、账户不同也是 40004，不再比较。
func TestReleaseConfirmedRejected(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 300, 15)
	const freezeNo = "FZCONFIRMED0000000001"
	insertFreezeRow(t, env.ctx, db, freezeNo, bizRefund, "CONF-REL", merchantID, 30, statusConfirmed)
	_, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, "CONF-REL", "M100002", 30)
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	_, err = env.svc.ReleaseFreeze(env.ctx, bizRefund, "CONF-REL", "M100002", 1)
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	if errors.Is(err, errno.ErrIdempotentConflict) {
		t.Fatal("confirmed amount mismatch became 10006")
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, "CONF-REL")
	if storedNo != freezeNo || storedAccount != merchantID || storedAmount != 30 || storedStatus != statusConfirmed || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("row changed no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}

	const otherNo = "FZCONFIRMED0000000002"
	insertFreezeRow(t, env.ctx, db, otherNo, bizTransfer, "CONF-OTHER", feeID, 30, statusConfirmed)
	_, err = env.svc.ReleaseFreeze(env.ctx, bizTransfer, "CONF-OTHER", "M100002", 30)
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	storedNo, storedAccount, storedAmount, storedStatus, _, freezeVersion = loadFreeze(t, env.ctx, db, bizTransfer, "CONF-OTHER")
	if storedNo != otherNo || storedAccount != feeID || storedAmount != 30 || storedStatus != statusConfirmed || freezeVersion != 0 {
		t.Fatalf("other row changed no=%s account=%d amount=%d status=%s version=%d", storedNo, storedAccount, storedAmount, storedStatus, freezeVersion)
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 300 || frozenAmount != 15 || status != statusActive || version != 0 {
		t.Fatalf("balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	feeBalance, feeFrozen, _, feeVersion := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != 0 || feeFrozen != 0 || feeVersion != 0 {
		t.Fatalf("fee balance=%d frozen=%d version=%d", feeBalance, feeFrozen, feeVersion)
	}
}

// CF-15 出口：FROZEN 金额不同，或冻结单指向别的账户，返回 10006。行保持 FROZEN，冻结金额不变。
func TestReleaseFrozenMismatch(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 300, 0)
	frozen, err := env.svc.Freeze(env.ctx, bizRefund, "AMT-REL", "M100002", 10)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err = env.svc.ReleaseFreeze(env.ctx, bizRefund, "AMT-REL", "M100002", 11)
	assertFreezeCode(t, err, errno.ErrIdempotentConflict)
	storedNo, storedAccount, storedAmount, storedStatus, _, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, "AMT-REL")
	if storedNo != frozen.FreezeNo || storedAccount != merchantID || storedAmount != 10 || storedStatus != statusFrozen || freezeVersion != 0 {
		t.Fatalf("amount row changed no=%s account=%d amount=%d status=%s version=%d", storedNo, storedAccount, storedAmount, storedStatus, freezeVersion)
	}
	balance, frozenAmount, _, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 300 || frozenAmount != 10 || version != 1 {
		t.Fatalf("after amount mismatch balance=%d frozen=%d version=%d", balance, frozenAmount, version)
	}

	insertFreezeRow(t, env.ctx, db, "FZACCOUNT000000000009", bizTransfer, "ACC-REL", feeID, 10, statusFrozen)
	_, err = env.svc.ReleaseFreeze(env.ctx, bizTransfer, "ACC-REL", "M100002", 10)
	assertFreezeCode(t, err, errno.ErrIdempotentConflict)
	storedNo, storedAccount, storedAmount, storedStatus, _, freezeVersion = loadFreeze(t, env.ctx, db, bizTransfer, "ACC-REL")
	if storedNo != "FZACCOUNT000000000009" || storedAccount != feeID || storedAmount != 10 || storedStatus != statusFrozen || freezeVersion != 0 {
		t.Fatalf("account row changed no=%s account=%d amount=%d status=%s version=%d", storedNo, storedAccount, storedAmount, storedStatus, freezeVersion)
	}
	balance, frozenAmount, _, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 300 || frozenAmount != 10 || version != 1 {
		t.Fatalf("after account mismatch balance=%d frozen=%d version=%d", balance, frozenAmount, version)
	}
	feeBalance, feeFrozen, _, feeVersion := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != 0 || feeFrozen != 0 || feeVersion != 0 {
		t.Fatalf("fee balance=%d frozen=%d version=%d", feeBalance, feeFrozen, feeVersion)
	}
}

// CF-15 出口：SQL 插入的 RELEASED 墓碑再解冻，金额和账户都不同也不比较，不改行，不改冻结。
func TestReleaseSQLTombstoneIgnoresAmount(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 200, 40)
	const freezeNo = "FZRELEASED00000000009"
	insertFreezeRow(t, env.ctx, db, freezeNo, bizTransfer, "SQL-REL", feeID, 25, statusReleased)
	got, err := env.svc.ReleaseFreeze(env.ctx, bizTransfer, "SQL-REL", "M100002", 80)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !got.Duplicated || got.FreezeNo != freezeNo || got.Status != statusReleased {
		t.Fatalf("result = %+v", got)
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizTransfer, "SQL-REL")
	if storedNo != freezeNo || storedAccount != feeID || storedAmount != 25 || storedStatus != statusReleased || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("row changed no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 200 || frozenAmount != 40 || status != statusActive || version != 0 {
		t.Fatalf("balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	feeBalance, feeFrozen, _, feeVersion := balanceOf(t, env.ctx, db, feeID)
	if feeBalance != 0 || feeFrozen != 0 || feeVersion != 0 {
		t.Fatalf("fee balance=%d frozen=%d version=%d", feeBalance, feeFrozen, feeVersion)
	}
}

// CF-15 出口：参数不合法不写行。
func TestReleaseRejectsInvalidWritesNothing(t *testing.T) {
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
			_, err := env.svc.ReleaseFreeze(env.ctx, tc.bizType, tc.bizNo, tc.merchantID, tc.amount)
			assertFreezeCode(t, err, errno.ErrInvalidParam)
		})
	}
	if countFreeze(t, env.ctx, db) != before0 || countFreeze(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("invalid release wrote a row")
	}
}

// CF-15 出口：账户不存在的空回滚返回 40001，两个分片都不新增冻结单。
func TestReleaseAccountNotFound(t *testing.T) {
	env := openEnv(t)
	before0 := countFreeze(t, env.ctx, env.s0.DB)
	before1 := countFreeze(t, env.ctx, env.s1.DB)
	_, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, "MISSING", "M100003", 10)
	assertFreezeCode(t, err, errno.ErrAccountNotFound)
	_, err = env.svc.ReleaseFreeze(env.ctx, bizTransfer, "MISSING", "M100004", 10)
	assertFreezeCode(t, err, errno.ErrAccountNotFound)
	if countFreeze(t, env.ctx, env.s0.DB) != before0 || countFreeze(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("missing account left a freeze")
	}
}

// CF-15 出口：账户不是 ACTIVE 时，解冻仍然成功，frozen_amount 减少，冻结单变为 RELEASED。不改余额。
func TestReleaseInactiveAccount(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 100, 0)
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
	first, err := env.svc.Freeze(env.ctx, bizRefund, "CLOSED-REL", "M100002", 30)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	second, err := env.svc.Freeze(env.ctx, bizTransfer, "FROZEN-REL", "M100002", 20)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	res, err := db.ExecContext(env.ctx, `UPDATE account SET status = 'CLOSED' WHERE id = ? AND status = 'ACTIVE'`, merchantID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("close n=%d err=%v", n, err)
	}
	got, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, "CLOSED-REL", "M100002", 30)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal("closed account became 40005")
	}
	if got.Duplicated || got.FreezeNo != first.FreezeNo || got.Status != statusReleased {
		t.Fatalf("closed release = %+v", got)
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 100 || frozenAmount != 20 || status != "CLOSED" || version != 3 {
		t.Fatalf("after closed release balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}

	res, err = db.ExecContext(env.ctx, `UPDATE account SET status = 'FROZEN' WHERE id = ?`, merchantID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n, err = res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("status update n=%d err=%v", n, err)
	}
	got, err = env.svc.ReleaseFreeze(env.ctx, bizTransfer, "FROZEN-REL", "M100002", 20)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Duplicated || got.FreezeNo != second.FreezeNo || got.Status != statusReleased {
		t.Fatalf("frozen-account release = %+v", got)
	}
	balance, frozenAmount, status, version = balanceOf(t, env.ctx, db, merchantID)
	if balance != 100 || frozenAmount != 0 || status != statusFrozen || version != 4 {
		t.Fatalf("after frozen-account release balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	_, _, _, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizTransfer, "FROZEN-REL")
	if storedStatus != statusReleased || txnNo.Valid || freezeVersion != 1 {
		t.Fatalf("status=%s txn=%v version=%d", storedStatus, txnNo, freezeVersion)
	}
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatal("inactive release wrote a ledger row")
	}
}

// CF-15 出口：账户更新影响 0 行时返回 10003 并回滚，冻结单保持 FROZEN，不改成 40002。
func TestReleaseAccountUpdateMissRollsBack(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 100, 0)
	frozen, err := env.svc.Freeze(env.ctx, bizRefund, "SHORT-REL", "M100002", 40)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	res, err := db.ExecContext(env.ctx, `UPDATE account SET frozen_amount = 10 WHERE id = ?`, merchantID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("shrink n=%d err=%v", n, err)
	}
	_, err = env.svc.ReleaseFreeze(env.ctx, bizRefund, "SHORT-REL", "M100002", 40)
	assertFreezeCode(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) || errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal("short frozen became a business rejection")
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db, merchantID)
	if balance != 100 || frozenAmount != 10 || status != statusActive || version != 1 {
		t.Fatalf("balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, "SHORT-REL")
	if storedNo != frozen.FreezeNo || storedAccount != merchantID || storedAmount != 40 || storedStatus != statusFrozen || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("rollback lost no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}
}

// CF-15：TRANSFER 解冻落在商户所在分片。64 个码点的 biz_no 可以解开。不改余额，也不写凭证。
func TestReleaseTransferShard(t *testing.T) {
	env := openEnv(t)
	merchant, merchantID, db1 := openOn(t, env, ownerMerchant, "M100001", typeMerchantSettle, 1, 0)
	setAmount(t, env.ctx, db1, merchant.AccountNo, 50, 0)
	before0 := countFreeze(t, env.ctx, env.s0.DB)
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db1)
	bizNo := strings.Repeat("转", 64)
	frozen, err := env.svc.Freeze(env.ctx, bizTransfer, bizNo, "M100001", 15)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err := env.svc.ReleaseFreeze(env.ctx, bizTransfer, bizNo, "M100001", 15)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Duplicated || got.FreezeNo != frozen.FreezeNo || got.Status != statusReleased {
		t.Fatalf("release = %+v freeze=%s", got, frozen.FreezeNo)
	}
	balance, frozenAmount, status, version := balanceOf(t, env.ctx, db1, merchantID)
	if balance != 50 || frozenAmount != 0 || status != statusActive || version != 2 {
		t.Fatalf("balance=%d frozen=%d status=%s version=%d", balance, frozenAmount, status, version)
	}
	if countFreeze(t, env.ctx, db1) != 1 || countFreeze(t, env.ctx, env.s0.DB) != before0 {
		t.Fatal("release landed on the wrong shard")
	}
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db1)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatal("transfer release wrote a ledger row")
	}
}
