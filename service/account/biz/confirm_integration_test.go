//go:build integration

package biz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

// ACC-06 验收 1：余额 9940、冻结 3000，REFUND 确认后余额 6940、冻结 0，渠道应收减少 3000。
func TestConfirmRefund(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "RF-3000"
		channel = "mockpay"
		opening = int64(9940)
		amount  = int64(3000)
		credit  = int64(10000)
	)
	ready := prepareFrozen(t, env, "M100002", bizRefund, bizNo, channel, opening, amount)
	setAmount(t, env.ctx, ready.db, ready.credit.AccountNo, credit, 0)
	otherBefore := snapAccounts(t, env.ctx, ready.other)
	_, merchantFrozen, _, merchantVersion := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	if merchantFrozen != amount || merchantVersion != 1 {
		t.Fatalf("before confirm frozen=%d version=%d", merchantFrozen, merchantVersion)
	}
	_, _, _, creditVersion := balanceOf(t, env.ctx, ready.db, ready.creditPK)

	got, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", channel)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Duplicated || got.FreezeNo != ready.freeze.FreezeNo || got.TxnNo == "" {
		t.Fatalf("result = %+v freeze=%s", got, ready.freeze.FreezeNo)
	}
	balance, frozen, status, version := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	if balance != 6940 || frozen != 0 || status != statusActive || version != merchantVersion+1 {
		t.Fatalf("merchant balance=%d frozen=%d status=%s version=%d", balance, frozen, status, version)
	}
	creditBalance, creditFrozen, creditStatus, creditVer := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	if creditBalance != 7000 || creditFrozen != 0 || creditStatus != statusActive || creditVer != creditVersion+1 {
		t.Fatalf("channel balance=%d frozen=%d status=%s version=%d", creditBalance, creditFrozen, creditStatus, creditVer)
	}
	assertConfirmVoucher(t, env.ctx, ready.db, bizRefund, bizNo, got.TxnNo, amount, map[uint64]entryWant{
		ready.merchantPK: {direction: directionDebit, amount: amount, after: 6940},
		ready.creditPK:   {direction: directionCredit, amount: amount, after: 7000},
	})
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, ready.db, bizRefund, bizNo)
	if storedNo != got.FreezeNo || storedAccount != ready.merchantPK || storedAmount != amount || storedStatus != statusConfirmed || !txnNo.Valid || txnNo.String != got.TxnNo || freezeVersion != 1 {
		t.Fatalf("freeze no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}
	if snapAccounts(t, env.ctx, ready.other) != otherBefore {
		t.Fatal("other shard changed")
	}
	if countTxnBiz(t, env.ctx, ready.other, bizRefund, bizNo) != 0 {
		t.Fatal("other shard has the voucher")
	}
}

// ACC-06 验收 2：TRANSFER 确认后商户余额与冻结各减 T，本分片在途增加 T，凭证是 TRANSFER_OUT。
func TestConfirmTransfer(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "TF-2500"
		opening = int64(8000)
		amount  = int64(2500)
	)
	ready := prepareFrozen(t, env, "M100001", bizTransfer, bizNo, "", opening, amount)
	otherBefore := snapAccounts(t, env.ctx, ready.other)
	_, _, _, merchantVersion := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	_, _, _, creditVersion := balanceOf(t, env.ctx, ready.db, ready.creditPK)

	got, err := env.svc.ConfirmFreeze(env.ctx, bizTransfer, bizNo, "M100001", "")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Duplicated || got.FreezeNo != ready.freeze.FreezeNo || got.TxnNo == "" {
		t.Fatalf("result = %+v", got)
	}
	balance, frozen, status, version := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	if balance != opening-amount || frozen != 0 || status != statusActive || version != merchantVersion+1 {
		t.Fatalf("merchant balance=%d frozen=%d status=%s version=%d", balance, frozen, status, version)
	}
	creditBalance, creditFrozen, _, creditVer := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	if creditBalance != amount || creditFrozen != 0 || creditVer != creditVersion+1 {
		t.Fatalf("transit balance=%d frozen=%d version=%d", creditBalance, creditFrozen, creditVer)
	}
	assertConfirmVoucher(t, env.ctx, ready.db, bizTransferOut, bizNo, got.TxnNo, amount, map[uint64]entryWant{
		ready.merchantPK: {direction: directionDebit, amount: amount, after: opening - amount},
		ready.creditPK:   {direction: directionCredit, amount: amount, after: amount},
	})
	if countTxnBiz(t, env.ctx, ready.db, bizTransfer, bizNo) != 0 {
		t.Fatal("freeze biz type was used as the voucher type")
	}
	storedNo, _, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, ready.db, bizTransfer, bizNo)
	if storedNo != got.FreezeNo || storedAmount != amount || storedStatus != statusConfirmed || !txnNo.Valid || txnNo.String != got.TxnNo || freezeVersion != 1 {
		t.Fatalf("freeze status=%s txn=%v version=%d", storedStatus, txnNo, freezeVersion)
	}
	if snapAccounts(t, env.ctx, ready.other) != otherBefore {
		t.Fatal("other shard changed")
	}
}

// ACC-06 验收 3：重复确认只记一次账。第二次 duplicated=true，单号不变。
func TestConfirmRepeat(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "RF-REPEAT"
		channel = "mockpay"
		opening = int64(4000)
		amount  = int64(1500)
	)
	ready := prepareFrozen(t, env, "M100002", bizRefund, bizNo, channel, opening, amount)
	setAmount(t, env.ctx, ready.db, ready.credit.AccountNo, 6000, 0)
	first, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", channel)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if first.Duplicated {
		t.Fatal("first confirm duplicated")
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	creditBalance, _, _, creditVersion := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	txnCount, entryCount := ledgerCounts(t, env.ctx, ready.db)

	second, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", channel)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !second.Duplicated || second.FreezeNo != first.FreezeNo || second.TxnNo != first.TxnNo {
		t.Fatalf("replay = %+v first = %+v", second, first)
	}
	balance2, frozen2, _, version2 := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	credit2, _, _, creditVersion2 := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	txn2, entry2 := ledgerCounts(t, env.ctx, ready.db)
	if balance2 != balance || frozen2 != frozen || version2 != version || credit2 != creditBalance || creditVersion2 != creditVersion || txn2 != txnCount || entry2 != entryCount {
		t.Fatalf("replay changed balance=%d frozen=%d version=%d credit=%d txn=%d entry=%d", balance2, frozen2, version2, credit2, txn2, entry2)
	}
}

// ACC-06 验收 3：16 个 goroutine 同时确认同一条 FROZEN，恰好 1 次记账。
func TestConfirmConcurrent(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "RF-RACE"
		channel = "mockpay"
		opening = int64(9940)
		amount  = int64(3000)
		n       = 16
	)
	ready := prepareFrozen(t, env, "M100002", bizRefund, bizNo, channel, opening, amount)
	setAmount(t, env.ctx, ready.db, ready.credit.AccountNo, 10000, 0)
	tightenPool(ready.db, 8)
	ctx, cancel := context.WithTimeout(env.ctx, 120*time.Second)
	defer cancel()

	results := make([]ConfirmResult, n)
	errs := make([]error, n)
	var readyN sync.WaitGroup
	var wg sync.WaitGroup
	readyN.Add(n)
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			readyN.Done()
			<-start
			results[i], errs[i] = env.svc.ConfirmFreeze(ctx, bizRefund, bizNo, "M100002", channel)
		}(i)
	}
	readyN.Wait()
	close(start)
	wg.Wait()

	fresh := 0
	var freezeNo, txnNo string
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("call %d: %s", i, testkit.Redact(errs[i]))
		}
		if results[i].FreezeNo == "" || results[i].TxnNo == "" {
			t.Fatalf("call %d = %+v", i, results[i])
		}
		if freezeNo == "" {
			freezeNo = results[i].FreezeNo
			txnNo = results[i].TxnNo
		}
		if results[i].FreezeNo != freezeNo || results[i].TxnNo != txnNo {
			t.Fatalf("call %d nos = %s %s", i, results[i].FreezeNo, results[i].TxnNo)
		}
		if !results[i].Duplicated {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh=%d", fresh)
	}
	balance, frozen, _, _ := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	if balance != 6940 || frozen != 0 {
		t.Fatalf("balance=%d frozen=%d", balance, frozen)
	}
	creditBalance, _, _, _ := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	if creditBalance != 7000 {
		t.Fatalf("channel balance=%d", creditBalance)
	}
	if countTxnBiz(t, env.ctx, ready.db, bizRefund, bizNo) != 1 {
		t.Fatal("voucher count is not 1")
	}
	if countEntryBiz(t, env.ctx, ready.db, bizRefund, bizNo) != 2 {
		t.Fatal("entry count is not 2")
	}
}

// ACC-06 验收 4：RELEASED（含墓碑）和不存在的 biz_no 确认返回 40004，不插入、不记账。
func TestConfirmReleasedAndMissing(t *testing.T) {
	env := openEnv(t)
	const channel = "mockpay"
	merchant, merchantPK, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 1000, 0)
	frozen, err := env.svc.Freeze(env.ctx, bizRefund, "REL-LIVE", "M100002", 300)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	released, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, "REL-LIVE", "M100002", 300)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if released.FreezeNo != frozen.FreezeNo || released.Status != statusReleased {
		t.Fatalf("release = %+v", released)
	}
	balance, frozenAmount, _, version := balanceOf(t, env.ctx, db, merchantPK)
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
	_, err = env.svc.ConfirmFreeze(env.ctx, bizRefund, "REL-LIVE", "M100002", channel)
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	balance2, frozen2, _, version2 := balanceOf(t, env.ctx, db, merchantPK)
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if balance2 != balance || frozen2 != frozenAmount || version2 != version || txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatalf("released confirm changed balance=%d frozen=%d version=%d txn=%d", balance2, frozen2, version2, txnAfter)
	}
	storedNo, _, _, storedStatus, txnNo, _ := loadFreeze(t, env.ctx, db, bizRefund, "REL-LIVE")
	if storedNo != frozen.FreezeNo || storedStatus != statusReleased || txnNo.Valid {
		t.Fatalf("released row status=%s txn=%v", storedStatus, txnNo)
	}

	tomb, err := env.svc.ReleaseFreeze(env.ctx, bizRefund, "REL-TOMB", "M100002", 20)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if tomb.Status != statusReleased || tomb.Duplicated {
		t.Fatalf("tombstone = %+v", tomb)
	}
	rowsBefore := countFreeze(t, env.ctx, db)
	_, err = env.svc.ConfirmFreeze(env.ctx, bizRefund, "REL-TOMB", "M100002", channel)
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	if countFreeze(t, env.ctx, db) != rowsBefore {
		t.Fatal("tombstone confirm inserted a row")
	}
	storedNo, _, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, "REL-TOMB")
	if storedNo != tomb.FreezeNo || storedAmount != 20 || storedStatus != statusReleased || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("tombstone changed no=%s amount=%d status=%s txn=%v version=%d", storedNo, storedAmount, storedStatus, txnNo, freezeVersion)
	}

	missingBefore := countFreeze(t, env.ctx, db)
	missingOther := countFreeze(t, env.ctx, env.s1.DB)
	_, err = env.svc.ConfirmFreeze(env.ctx, bizTransfer, "NO-SUCH", "M100002", "")
	assertFreezeCode(t, err, errno.ErrFreezeStatusConflict)
	if countFreeze(t, env.ctx, db) != missingBefore || countFreeze(t, env.ctx, env.s1.DB) != missingOther {
		t.Fatal("missing confirm inserted a freeze")
	}
	var n int
	if err = db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM account_freeze WHERE biz_no = 'NO-SUCH'`).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n != 0 {
		t.Fatal("missing biz_no inserted a row")
	}
	txnAfter, entryAfter = ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatal("rejected confirm wrote a voucher")
	}
	balance2, frozen2, _, version2 = balanceOf(t, env.ctx, db, merchantPK)
	if balance2 != balance || frozen2 != frozenAmount || version2 != version {
		t.Fatalf("missing confirm changed balance=%d frozen=%d version=%d", balance2, frozen2, version2)
	}
}

// ACC-06：可用余额为 0 时确认仍然成功。这是为了证明没有误用普通扣款条件。
func TestConfirmFullHold(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "RF-FULL"
		channel = "mockpay"
		amount  = int64(3000)
	)
	ready := prepareFrozen(t, env, "M100002", bizRefund, bizNo, channel, amount, amount)
	setAmount(t, env.ctx, ready.db, ready.credit.AccountNo, 10000, 0)
	got, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", channel)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Duplicated || errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatalf("full hold result = %+v err = %v", got, err)
	}
	balance, frozen, _, _ := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	if balance != 0 || frozen != 0 {
		t.Fatalf("balance=%d frozen=%d", balance, frozen)
	}
	creditBalance, _, _, _ := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	if creditBalance != 7000 {
		t.Fatalf("channel balance=%d", creditBalance)
	}
}

// ACC-06：参数不合法不写行。上下文已取消且参数不合法仍是 10001。
func TestConfirmInvalidWritesNothing(t *testing.T) {
	env := openEnv(t)
	before0 := countFreeze(t, env.ctx, env.s0.DB)
	before1 := countFreeze(t, env.ctx, env.s1.DB)
	txn0, entry0 := ledgerCounts(t, env.ctx, env.s0.DB)
	cases := []struct {
		bizType, bizNo, merchantID, channel string
	}{
		{bizTransfer, "B1", "M100002", "mockpay"},
		{bizRefund, "B1", "M100002", ""},
		{bizRefund, "B1", "M100002", strings.Repeat("渠", maxOwnerIDRunes+1)},
		{bizRefund, "B1", "BAD", "mockpay"},
		{bizPay, "B1", "M100002", "mockpay"},
		{bizRefund, "", "M100002", "mockpay"},
	}
	for _, tc := range cases {
		_, err := env.svc.ConfirmFreeze(env.ctx, tc.bizType, tc.bizNo, tc.merchantID, tc.channel)
		assertFreezeCode(t, err, errno.ErrInvalidParam)
	}
	canceled, cancel := context.WithCancel(env.ctx)
	cancel()
	_, err := env.svc.ConfirmFreeze(canceled, bizTransfer, "B1", "M100002", "x")
	assertFreezeCode(t, err, errno.ErrInvalidParam)
	if countFreeze(t, env.ctx, env.s0.DB) != before0 || countFreeze(t, env.ctx, env.s1.DB) != before1 {
		t.Fatal("invalid confirm wrote a freeze")
	}
	txn, entry := ledgerCounts(t, env.ctx, env.s0.DB)
	if txn != txn0 || entry != entry0 {
		t.Fatal("invalid confirm wrote a voucher")
	}
}

// ACC-06：FROZEN 但账户不是该商户结算账户时返回 10006，旧行不变。
func TestConfirmAccountMismatch(t *testing.T) {
	env := openEnv(t)
	merchant, merchantPK, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 500, 20)
	const freezeNo = "FZMISMATCH0000000001"
	insertFreezeRow(t, env.ctx, db, freezeNo, bizRefund, "BAD-ACCT", feeID, 20, statusFrozen)
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
	_, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, "BAD-ACCT", "M100002", "mockpay")
	assertFreezeCode(t, err, errno.ErrIdempotentConflict)
	if errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatal("wrong account became 40001")
	}
	storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, "BAD-ACCT")
	if storedNo != freezeNo || storedAccount != feeID || storedAmount != 20 || storedStatus != statusFrozen || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("row changed no=%s account=%d amount=%d status=%s txn=%v version=%d", storedNo, storedAccount, storedAmount, storedStatus, txnNo, freezeVersion)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantPK)
	if balance != 500 || frozen != 20 || version != 0 {
		t.Fatalf("merchant balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore {
		t.Fatal("mismatch wrote a voucher")
	}
}

// ACC-06：冻结金额大于当前 frozen_amount 时返回 10003，冻结单保持 FROZEN，无凭证。
func TestConfirmFrozenShort(t *testing.T) {
	env := openEnv(t)
	const bizNo = "RF-SHORT"
	merchant, merchantPK, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	k, err := shard.Sub(bizNo, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	credit, creditPK, _ := openOn(t, env, ownerPlatform, "mockpay", typeChannelReceivable, 0, k)
	setAmount(t, env.ctx, db, merchant.AccountNo, 1000, 100)
	setAmount(t, env.ctx, db, credit.AccountNo, 8000, 0)
	const freezeNo = "FZSHORT0000000000001"
	insertFreezeRow(t, env.ctx, db, freezeNo, bizRefund, bizNo, merchantPK, 5000, statusFrozen)
	_, err = env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", "mockpay")
	assertFreezeCode(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) || errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal("short frozen became a business rejection")
	}
	balance, frozen, status, version := balanceOf(t, env.ctx, db, merchantPK)
	if balance != 1000 || frozen != 100 || status != statusActive || version != 0 {
		t.Fatalf("merchant balance=%d frozen=%d status=%s version=%d", balance, frozen, status, version)
	}
	creditBalance, _, _, creditVersion := balanceOf(t, env.ctx, db, creditPK)
	if creditBalance != 8000 || creditVersion != 0 {
		t.Fatalf("channel balance=%d version=%d", creditBalance, creditVersion)
	}
	storedNo, _, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, bizNo)
	if storedNo != freezeNo || storedAmount != 5000 || storedStatus != statusFrozen || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("freeze changed amount=%d status=%s txn=%v version=%d", storedAmount, storedStatus, txnNo, freezeVersion)
	}
	if countTxnBiz(t, env.ctx, db, bizRefund, bizNo) != 0 {
		t.Fatal("short confirm left a voucher")
	}
}

// ACC-06：商户账户不是 ACTIVE 仍然确认，不返回 40005。
func TestConfirmInactiveMerchant(t *testing.T) {
	env := openEnv(t)
	const channel = "mockpay"
	merchant, merchantPK, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 200, 0)
	closedFreeze, err := env.svc.Freeze(env.ctx, bizRefund, "RF-CLOSED", "M100002", 40)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	frozenFreeze, err := env.svc.Freeze(env.ctx, bizTransfer, "TF-FROZEN-ACCT", "M100002", 50)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	closedK, err := shard.Sub("RF-CLOSED", testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	frozenK, err := shard.Sub("TF-FROZEN-ACCT", testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	channelAcc, channelPK, _ := openOn(t, env, ownerPlatform, channel, typeChannelReceivable, 0, closedK)
	_, transitPK, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeTransit, 0, frozenK)
	setAmount(t, env.ctx, db, channelAcc.AccountNo, 500, 0)
	setAccountStatus(t, env.ctx, db, merchantPK, "CLOSED")
	got, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, "RF-CLOSED", "M100002", channel)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Duplicated || got.FreezeNo != closedFreeze.FreezeNo || errors.Is(err, errno.ErrAccountInactive) {
		t.Fatalf("closed result = %+v", got)
	}
	balance, frozen, status, _ := balanceOf(t, env.ctx, db, merchantPK)
	if balance != 160 || frozen != 50 || status != "CLOSED" {
		t.Fatalf("after closed confirm balance=%d frozen=%d status=%s", balance, frozen, status)
	}
	channelBalance, _, _, _ := balanceOf(t, env.ctx, db, channelPK)
	if channelBalance != 460 {
		t.Fatalf("channel balance=%d", channelBalance)
	}

	setAccountStatus(t, env.ctx, db, merchantPK, statusFrozen)
	got, err = env.svc.ConfirmFreeze(env.ctx, bizTransfer, "TF-FROZEN-ACCT", "M100002", "")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.Duplicated || got.FreezeNo != frozenFreeze.FreezeNo {
		t.Fatalf("frozen-account result = %+v", got)
	}
	balance, frozen, status, _ = balanceOf(t, env.ctx, db, merchantPK)
	if balance != 110 || frozen != 0 || status != statusFrozen {
		t.Fatalf("after frozen-account confirm balance=%d frozen=%d status=%s", balance, frozen, status)
	}
	transitBalance, _, _, _ := balanceOf(t, env.ctx, db, transitPK)
	if transitBalance != 50 {
		t.Fatalf("transit balance=%d", transitBalance)
	}
}

// ACC-06：平台贷方不是 ACTIVE 时返回 40005，回滚，冻结单保持 FROZEN，无凭证。
func TestConfirmInactivePlatform(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "RF-PLAT"
		channel = "mockpay"
		amount  = int64(80)
	)
	ready := prepareFrozen(t, env, "M100002", bizRefund, bizNo, channel, 200, amount)
	setAmount(t, env.ctx, ready.db, ready.credit.AccountNo, 1000, 0)
	setAccountStatus(t, env.ctx, ready.db, ready.creditPK, "CLOSED")
	_, merchantFrozen, _, merchantVersion := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	_, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", channel)
	assertFreezeCode(t, err, errno.ErrAccountInactive)
	balance, frozen, status, version := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	if balance != 200 || frozen != merchantFrozen || status != statusActive || version != merchantVersion {
		t.Fatalf("merchant balance=%d frozen=%d status=%s version=%d", balance, frozen, status, version)
	}
	creditBalance, _, creditStatus, creditVersion := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	if creditBalance != 1000 || creditStatus != "CLOSED" || creditVersion != 0 {
		t.Fatalf("channel balance=%d status=%s version=%d", creditBalance, creditStatus, creditVersion)
	}
	_, _, _, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, ready.db, bizRefund, bizNo)
	if storedStatus != statusFrozen || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("freeze status=%s txn=%v version=%d", storedStatus, txnNo, freezeVersion)
	}
	if countTxnBiz(t, env.ctx, ready.db, bizRefund, bizNo) != 0 {
		t.Fatal("inactive platform left a voucher")
	}
}

// ACC-06：平台子账户不存在返回 40001，不写。另一分片的同号子账户不算。
func TestConfirmPlatformMissing(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "RF-NOPLAT"
		channel = "mockpay"
	)
	merchant, merchantPK, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 300, 0)
	k, err := shard.Sub(bizNo, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	other, otherPK, otherDB := openOn(t, env, ownerPlatform, channel, typeChannelReceivable, 1, k)
	setAmount(t, env.ctx, otherDB, other.AccountNo, 4000, 0)
	frozen, err := env.svc.Freeze(env.ctx, bizRefund, bizNo, "M100002", 100)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	txnBefore, entryBefore := ledgerCounts(t, env.ctx, db)
	_, err = env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", channel)
	assertFreezeCode(t, err, errno.ErrAccountNotFound)
	if errors.Is(err, errno.ErrUnbalancedEntries) {
		t.Fatal("missing platform became 40003")
	}
	balance, frozenAmount, _, version := balanceOf(t, env.ctx, db, merchantPK)
	if balance != 300 || frozenAmount != 100 || version != 1 {
		t.Fatalf("merchant balance=%d frozen=%d version=%d", balance, frozenAmount, version)
	}
	otherBalance, _, _, otherVersion := balanceOf(t, env.ctx, otherDB, otherPK)
	if otherBalance != 4000 || otherVersion != 0 {
		t.Fatalf("other shard channel balance=%d version=%d", otherBalance, otherVersion)
	}
	_, _, _, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, bizNo)
	if storedStatus != statusFrozen || txnNo.Valid || freezeVersion != 0 || frozen.FreezeNo == "" {
		t.Fatalf("freeze status=%s txn=%v version=%d", storedStatus, txnNo, freezeVersion)
	}
	txnAfter, entryAfter := ledgerCounts(t, env.ctx, db)
	if txnAfter != txnBefore || entryAfter != entryBefore || countTxnBiz(t, env.ctx, otherDB, bizRefund, bizNo) != 0 {
		t.Fatal("missing platform wrote a voucher")
	}
}

// ACC-06：已 CONFIRMED 再用另一个 channel 调用，仍返回第一次的 txn_no，另一个渠道余额不变。
func TestConfirmReplayIgnoresChannel(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo  = "RF-CHAN"
		first  = "mockpay"
		second = "otherpay"
		amount = int64(700)
	)
	ready := prepareFrozen(t, env, "M100002", bizRefund, bizNo, first, 2000, amount)
	setAmount(t, env.ctx, ready.db, ready.credit.AccountNo, 5000, 0)
	k, err := shard.Sub(bizNo, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	other, otherPK, _ := openOn(t, env, ownerPlatform, second, typeChannelReceivable, 0, k)
	setAmount(t, env.ctx, ready.db, other.AccountNo, 3000, 0)
	got, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", first)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	replay, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", second)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !replay.Duplicated || replay.FreezeNo != got.FreezeNo || replay.TxnNo != got.TxnNo {
		t.Fatalf("replay = %+v first = %+v", replay, got)
	}
	creditBalance, _, _, _ := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	otherBalance, _, _, otherVersion := balanceOf(t, env.ctx, ready.db, otherPK)
	if creditBalance != 4300 || otherBalance != 3000 || otherVersion != 0 {
		t.Fatalf("first channel=%d other=%d version=%d", creditBalance, otherBalance, otherVersion)
	}
	if countTxnBiz(t, env.ctx, ready.db, bizRefund, bizNo) != 1 {
		t.Fatal("replay wrote another voucher")
	}
}

// ACC-06：已 CONFIRMED 但 txn_no 为空返回 10003，不写凭证。
func TestConfirmEmptyTxnNo(t *testing.T) {
	env := openEnv(t)
	merchant, merchantPK, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 100, 40)
	const freezeNo = "FZEMPTYTXN0000000001"
	insertFreezeRow(t, env.ctx, db, freezeNo, bizRefund, "EMPTY-TXN", merchantPK, 40, statusConfirmed)
	_, err := env.svc.ConfirmFreeze(env.ctx, bizRefund, "EMPTY-TXN", "M100002", "mockpay")
	assertFreezeCode(t, err, errno.ErrInternal)
	_, _, storedAmount, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, db, bizRefund, "EMPTY-TXN")
	if storedAmount != 40 || storedStatus != statusConfirmed || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("row changed amount=%d status=%s txn=%v version=%d", storedAmount, storedStatus, txnNo, freezeVersion)
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantPK)
	if balance != 100 || frozen != 40 || version != 0 {
		t.Fatalf("balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	if countTxnBiz(t, env.ctx, db, bizRefund, "EMPTY-TXN") != 0 {
		t.Fatal("empty txn_no wrote a voucher")
	}
}

// ACC-06：冻结单仍是 FROZEN 但凭证已存在时返回 10003，不 CAS，不减冻结。
func TestConfirmVoucherAlreadyExists(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "RF-DUP"
		channel = "mockpay"
		amount  = int64(60)
	)
	ready := prepareFrozen(t, env, "M100002", bizRefund, bizNo, channel, 400, amount)
	setAmount(t, env.ctx, ready.db, ready.credit.AccountNo, 900, 0)
	_, err := ready.db.ExecContext(env.ctx, `
INSERT INTO ledger_txn (txn_no, biz_type, biz_no, amount, currency, entry_count, memo)
VALUES ('TXPLANTED00000000001', 'REFUND', ?, ?, 'CNY', 2, '')`, bizNo, amount)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err = env.svc.ConfirmFreeze(env.ctx, bizRefund, bizNo, "M100002", channel)
	assertFreezeCode(t, err, errno.ErrInternal)
	balance, frozen, _, version := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	if balance != 400 || frozen != amount || version != 1 {
		t.Fatalf("merchant balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	creditBalance, _, _, creditVersion := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	if creditBalance != 900 || creditVersion != 0 {
		t.Fatalf("channel balance=%d version=%d", creditBalance, creditVersion)
	}
	_, _, _, storedStatus, txnNo, freezeVersion := loadFreeze(t, env.ctx, ready.db, bizRefund, bizNo)
	if storedStatus != statusFrozen || txnNo.Valid || freezeVersion != 0 {
		t.Fatalf("freeze status=%s txn=%v version=%d", storedStatus, txnNo, freezeVersion)
	}
	if countTxnBiz(t, env.ctx, ready.db, bizRefund, bizNo) != 1 || countEntryBiz(t, env.ctx, ready.db, bizRefund, bizNo) != 0 {
		t.Fatal("duplicate confirm changed the planted voucher")
	}
}

// ACC-06：确认与解冻并发抢同一条 FROZEN。最终只能是一种终态，不能减两次。
func TestConfirmReleaseRace(t *testing.T) {
	env := openEnv(t)
	const (
		bizNo   = "RF-BOTH"
		channel = "mockpay"
		opening = int64(5000)
		amount  = int64(2000)
		n       = 8
	)
	ready := prepareFrozen(t, env, "M100002", bizRefund, bizNo, channel, opening, amount)
	setAmount(t, env.ctx, ready.db, ready.credit.AccountNo, 9000, 0)
	tightenPool(ready.db, 8)
	ctx, cancel := context.WithTimeout(env.ctx, 120*time.Second)
	defer cancel()

	confirmRes := make([]ConfirmResult, n)
	confirmErr := make([]error, n)
	releaseRes := make([]FreezeResult, n)
	releaseErr := make([]error, n)
	var readyN sync.WaitGroup
	var wg sync.WaitGroup
	readyN.Add(n * 2)
	wg.Add(n * 2)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			readyN.Done()
			<-start
			confirmRes[i], confirmErr[i] = env.svc.ConfirmFreeze(ctx, bizRefund, bizNo, "M100002", channel)
		}(i)
		go func(i int) {
			defer wg.Done()
			readyN.Done()
			<-start
			releaseRes[i], releaseErr[i] = env.svc.ReleaseFreeze(ctx, bizRefund, bizNo, "M100002", money.Amount(amount))
		}(i)
	}
	readyN.Wait()
	close(start)
	wg.Wait()

	for i := range confirmErr {
		if confirmErr[i] != nil && !errors.Is(confirmErr[i], errno.ErrFreezeStatusConflict) {
			t.Fatalf("confirm %d: %s", i, testkit.Redact(confirmErr[i]))
		}
		if releaseErr[i] != nil && !errors.Is(releaseErr[i], errno.ErrFreezeStatusConflict) {
			t.Fatalf("release %d: %s", i, testkit.Redact(releaseErr[i]))
		}
	}
	_, _, storedAmount, storedStatus, txnNo, _ := loadFreeze(t, env.ctx, ready.db, bizRefund, bizNo)
	balance, frozen, _, version := balanceOf(t, env.ctx, ready.db, ready.merchantPK)
	creditBalance, _, _, _ := balanceOf(t, env.ctx, ready.db, ready.creditPK)
	vouchers := countTxnBiz(t, env.ctx, ready.db, bizRefund, bizNo)
	if storedAmount != amount {
		t.Fatalf("amount changed to %d", storedAmount)
	}
	switch storedStatus {
	case statusConfirmed:
		if vouchers != 1 || !txnNo.Valid || balance != opening-amount || frozen != 0 || version != 2 || creditBalance != 7000 {
			t.Fatalf("confirmed balance=%d frozen=%d version=%d credit=%d vouchers=%d txn=%v", balance, frozen, version, creditBalance, vouchers, txnNo)
		}
	case statusReleased:
		if vouchers != 0 || txnNo.Valid || balance != opening || frozen != 0 || version != 2 || creditBalance != 9000 {
			t.Fatalf("released balance=%d frozen=%d version=%d credit=%d vouchers=%d txn=%v", balance, frozen, version, creditBalance, vouchers, txnNo)
		}
	default:
		t.Fatalf("status=%s balance=%d frozen=%d vouchers=%d", storedStatus, balance, frozen, vouchers)
	}
	freshConfirm, freshRelease := 0, 0
	for i := range confirmErr {
		if confirmErr[i] == nil && !confirmRes[i].Duplicated {
			freshConfirm++
		}
		if releaseErr[i] == nil && !releaseRes[i].Duplicated {
			freshRelease++
		}
	}
	if storedStatus == statusConfirmed && (freshConfirm != 1 || freshRelease != 0) {
		t.Fatalf("confirmed fresh confirm=%d release=%d", freshConfirm, freshRelease)
	}
	if storedStatus == statusReleased && (freshConfirm != 0 || freshRelease != 1) {
		t.Fatalf("released fresh confirm=%d release=%d", freshConfirm, freshRelease)
	}
}

// ACC-06：普通 Post 在商户有冻结时仍不改 frozen_amount，可用余额不足仍是 40002。
func TestPostStillGuardsFrozenAmount(t *testing.T) {
	env := openEnv(t)
	merchant, merchantPK, db := openOn(t, env, ownerMerchant, "M100002", typeMerchantSettle, 0, 0)
	_, feeID, _ := openOn(t, env, ownerPlatform, platformOwnerID, typeFeeIncome, 0, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, 200, 80)
	_, err := postCommitted(env.ctx, db, env.svc, bizPay, "STILL-SHORT", "", []Entry{
		{AccountID: merchantPK, Direction: directionDebit, Amount: 150},
		{AccountID: feeID, Direction: directionCredit, Amount: 150},
	})
	if !errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatalf("error = %s", testkit.Redact(err))
	}
	balance, frozen, _, version := balanceOf(t, env.ctx, db, merchantPK)
	if balance != 200 || frozen != 80 || version != 0 {
		t.Fatalf("after reject balance=%d frozen=%d version=%d", balance, frozen, version)
	}
	if _, err = postCommitted(env.ctx, db, env.svc, bizPay, "STILL-OK", "", []Entry{
		{AccountID: merchantPK, Direction: directionDebit, Amount: 100},
		{AccountID: feeID, Direction: directionCredit, Amount: 100},
	}); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	balance, frozen, _, version = balanceOf(t, env.ctx, db, merchantPK)
	if balance != 100 || frozen != 80 || version != 1 {
		t.Fatalf("after debit balance=%d frozen=%d version=%d", balance, frozen, version)
	}
}

type entryWant struct {
	direction string
	amount    int64
	after     int64
}

type frozenReady struct {
	db         *sqlx.DB
	other      *sqlx.DB
	merchant   Account
	merchantPK uint64
	credit     Account
	creditPK   uint64
	freeze     FreezeResult
}

func prepareFrozen(t *testing.T, env shardEnv, merchantID, bizType, bizNo, channel string, balance, amount int64) frozenReady {
	t.Helper()
	shardNo, err := shard.Of(merchantID)
	if err != nil {
		t.Fatal(err)
	}
	k, err := shard.Sub(bizNo, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	merchant, merchantPK, db := openOn(t, env, ownerMerchant, merchantID, typeMerchantSettle, shardNo, 0)
	setAmount(t, env.ctx, db, merchant.AccountNo, balance, 0)
	ownerID := channel
	accountType := typeChannelReceivable
	if bizType == bizTransfer {
		ownerID = platformOwnerID
		accountType = typeTransit
	}
	credit, creditPK, creditDB := openOn(t, env, ownerPlatform, ownerID, accountType, shardNo, k)
	if creditDB != db {
		t.Fatal("credit account opened on another shard")
	}
	frozen, err := env.svc.Freeze(env.ctx, bizType, bizNo, merchantID, money.Amount(amount))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if frozen.Duplicated || frozen.Status != statusFrozen || frozen.FreezeNo == "" {
		t.Fatalf("freeze = %+v", frozen)
	}
	other := env.s1.DB
	if shardNo == 1 {
		other = env.s0.DB
	}
	return frozenReady{
		db: db, other: other, merchant: merchant, merchantPK: merchantPK,
		credit: credit, creditPK: creditPK, freeze: frozen,
	}
}

func setAccountStatus(t *testing.T, ctx context.Context, db *sqlx.DB, id uint64, status string) {
	t.Helper()
	res, err := db.ExecContext(ctx, `UPDATE account SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("status update n=%d err=%v", n, err)
	}
}

func countTxnBiz(t *testing.T, ctx context.Context, db *sqlx.DB, bizType, bizNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type = ? AND biz_no = ?`, bizType, bizNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func countEntryBiz(t *testing.T, ctx context.Context, db *sqlx.DB, bizType, bizNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_entry WHERE biz_type = ? AND biz_no = ?`, bizType, bizNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func snapAccounts(t *testing.T, ctx context.Context, db *sqlx.DB) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT account_no, balance, frozen_amount, status, version FROM account ORDER BY account_no`)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	var b strings.Builder
	for rows.Next() {
		var (
			no, status      string
			balance, frozen int64
			version         int64
		)
		if err = rows.Scan(&no, &balance, &frozen, &status, &version); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		_, _ = fmt.Fprintf(&b, "%s %d %d %s %d\n", no, balance, frozen, status, version)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	txn, entry := ledgerCounts(t, ctx, db)
	_, _ = fmt.Fprintf(&b, "txn %d entry %d\n", txn, entry)
	return b.String()
}

func assertConfirmVoucher(t *testing.T, ctx context.Context, db *sqlx.DB, bizType, bizNo, txnNo string, amount int64, want map[uint64]entryWant) {
	t.Helper()
	var (
		gotTxn   string
		gotAmt   int64
		gotCount int
		memo     string
		currency string
	)
	err := db.QueryRowContext(ctx, `
SELECT txn_no, amount, entry_count, memo, currency
FROM ledger_txn WHERE biz_type = ? AND biz_no = ?`, bizType, bizNo).Scan(&gotTxn, &gotAmt, &gotCount, &memo, &currency)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if gotTxn != txnNo || gotAmt != amount || gotCount != len(want) || memo != "" || currency != currencyCNY {
		t.Fatalf("txn=%s amount=%d count=%d memo=%q currency=%s", gotTxn, gotAmt, gotCount, memo, currency)
	}
	rows, err := db.QueryContext(ctx, `
SELECT account_id, direction, amount, balance_after, biz_type, biz_no
FROM ledger_entry WHERE txn_no = ?`, txnNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	got := map[uint64]entryWant{}
	for rows.Next() {
		var (
			id         uint64
			direction  string
			amt, after int64
			rowType    string
			rowNo      string
		)
		if err = rows.Scan(&id, &direction, &amt, &after, &rowType, &rowNo); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if rowType != bizType || rowNo != bizNo {
			t.Fatalf("entry biz = %s %s", rowType, rowNo)
		}
		got[id] = entryWant{direction: direction, amount: amt, after: after}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if len(got) != len(want) {
		t.Fatalf("entries = %+v", got)
	}
	for id, w := range want {
		g, ok := got[id]
		if !ok || g != w {
			t.Fatalf("account %d entry = %+v want %+v", id, g, w)
		}
		var balance int64
		if err = db.QueryRowContext(ctx, `SELECT balance FROM account WHERE id = ?`, id).Scan(&balance); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if balance != w.after {
			t.Fatalf("account %d balance=%d balance_after=%d", id, balance, w.after)
		}
	}
}
