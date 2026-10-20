package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/account/dal"
)

// ACC-06：参数不合法返回 10001，不进事务、不发号、不写行。上下文已取消也先返回 10001。
func TestConfirmRejectsParams(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	ctx := context.Background()
	longChannel := strings.Repeat("渠", maxOwnerIDRunes+1)
	cases := []struct {
		name       string
		bizType    string
		bizNo      string
		merchantID string
		channel    string
	}{
		{name: "transfer channel", bizType: bizTransfer, bizNo: "B1", merchantID: "M100002", channel: "mockpay"},
		{name: "refund empty channel", bizType: bizRefund, bizNo: "B1", merchantID: "M100002", channel: ""},
		{name: "channel too long", bizType: bizRefund, bizNo: "B1", merchantID: "M100002", channel: longChannel},
		{name: "bad merchant", bizType: bizRefund, bizNo: "B1", merchantID: "M10002", channel: "mockpay"},
		{name: "bad biz type", bizType: bizPay, bizNo: "B1", merchantID: "M100002", channel: "mockpay"},
		{name: "empty biz no", bizType: bizRefund, bizNo: "", merchantID: "M100002", channel: "mockpay"},
		{name: "biz no 65", bizType: bizTransfer, bizNo: strings.Repeat("单", maxBizNoRunes+1), merchantID: "M100002", channel: ""},
		{name: "lower refund", bizType: "refund", bizNo: "B1", merchantID: "M100002", channel: "mockpay"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.ConfirmFreeze(ctx, tc.bizType, tc.bizNo, tc.merchantID, tc.channel)
			assertErrno(t, err, errno.ErrInvalidParam)
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.ConfirmFreeze(canceled, bizTransfer, "B1", "M100002", "mockpay")
	assertErrno(t, err, errno.ErrInvalidParam)
	_, err = (*Service)(nil).ConfirmFreeze(canceled, "", "B1", "M100002", "")
	assertErrno(t, err, errno.ErrInvalidParam)
	if clk.i != 0 {
		t.Fatalf("NewNo calls = %d", clk.i)
	}

	// 64 个码点和 32 个码点的 channel 只校验。nil db 说明已经过了校验，仍然不发号。
	_, err = svc.ConfirmFreeze(ctx, bizRefund, strings.Repeat("单", maxBizNoRunes), "M100002", strings.Repeat("渠", maxOwnerIDRunes))
	assertErrno(t, err, errno.ErrInternal)
	_, err = svc.ConfirmFreeze(ctx, bizTransfer, "B1", "M100001", "")
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil db issued an id")
	}
}

func TestConfirmNilAndCanceled(t *testing.T) {
	ctx := context.Background()
	_, err := (*Service)(nil).ConfirmFreeze(ctx, bizRefund, "B1", "M100002", "mockpay")
	assertErrno(t, err, errno.ErrInternal)

	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	// SA1012 不允许把 nil context 当参数。这里正是要证明 nil ctx 返回 10003。
	_, err = svc.ConfirmFreeze(nil, bizRefund, "B1", "M100002", "mockpay") //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)

	_, err = New(nil, newIDs(t, clk), testSubCount).ConfirmFreeze(ctx, bizRefund, "B1", "M100002", "mockpay")
	assertErrno(t, err, errno.ErrInternal)
	_, err = New(dal.New(nil, nil), nil, testSubCount).ConfirmFreeze(ctx, bizTransfer, "B1", "M100001", "")
	assertErrno(t, err, errno.ErrInternal)
	_, err = New(dal.New(nil, nil), newIDs(t, clk), 0).ConfirmFreeze(ctx, bizRefund, "B1", "M100002", "mockpay")
	assertErrno(t, err, errno.ErrInternal)
	_, err = New(dal.New(nil, nil), newIDs(t, clk), -1).ConfirmFreeze(ctx, bizRefund, "B1", "M100002", "mockpay")
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInvalidParam) {
		t.Fatal("subCount became 10001")
	}
	if clk.i != 0 {
		t.Fatal("nil dependency issued an id")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.ConfirmFreeze(canceled, bizRefund, "B1", "M100002", "mockpay")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
	if errors.Is(err, errno.ErrInternal) {
		t.Fatal("canceled context became 10003")
	}
	deadline, stop := context.WithDeadline(context.Background(), openTestTime)
	defer stop()
	_, err = svc.ConfirmFreeze(deadline, bizTransfer, "B1", "M100001", "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline", err)
	}
	if clk.i != 0 {
		t.Fatal("canceled request issued an id")
	}
}

// ACC-06：全额冻结走特殊更新。可用余额为 0 时不返回 40002，停用商户也不返回 40005。
func TestApplyPostFrozenDebit(t *testing.T) {
	merchant := activeAccount(1, directionCredit, 3000, 3000, overdraftNo)
	merchant.Status = "CLOSED"
	channel := activeAccount(2, directionDebit, 10000, 0, overdraftYes)
	fake := &fakePostTx{accounts: map[uint64]lockedAccount{1: merchant, 2: channel}}
	got, err := applyPost(context.Background(), fake, postRequest{
		TxnNo: "TX9", BizType: bizRefund, BizNo: "R1", Debit: 3000, FrozenAccountID: 1,
		Entries: []Entry{
			{AccountID: 1, Direction: directionDebit, Amount: 3000},
			{AccountID: 2, Direction: directionCredit, Amount: 3000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicated || got.TxnNo != "TX9" {
		t.Fatalf("result = %+v", got)
	}
	if _, ok := fake.deltas[1]; ok {
		t.Fatal("merchant used the ordinary debit update")
	}
	debit := fake.frozenDeltas[1]
	if debit.delta != -3000 || debit.release != 3000 {
		t.Fatalf("frozen debit = %+v", debit)
	}
	if fake.accounts[1].Balance != 0 || fake.accounts[1].FrozenAmount != 0 {
		t.Fatalf("merchant = %+v", fake.accounts[1])
	}
	if fake.accounts[2].Balance != 7000 || fake.deltas[2].delta != -3000 || fake.deltas[2].guard != nil {
		t.Fatalf("channel = %+v update = %+v", fake.accounts[2], fake.deltas[2])
	}
	if len(fake.entries) != 2 || fake.entries[0].BalanceAfter != 0 || fake.entries[1].BalanceAfter != 7000 {
		t.Fatalf("entries = %+v", fake.entries)
	}

	// 不设置选项时，全额冻结仍被普通借方拒绝，且不改冻结。
	plain := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 3000, 3000, overdraftNo),
		2: activeAccount(2, directionDebit, 10000, 0, overdraftYes),
	}}
	_, err = applyPost(context.Background(), plain, postRequest{
		TxnNo: "TX8", BizType: bizPay, BizNo: "P1", Debit: 3000,
		Entries: pairEntries(1, 2, 3000),
	})
	assertErrno(t, err, errno.ErrInsufficientBalance)
	if plain.accounts[1].FrozenAmount != 3000 || len(plain.frozenDeltas) != 0 || plain.updates != 0 {
		t.Fatal("ordinary post changed the hold or updated a balance")
	}
}

// ACC-06：特殊更新影响 0 行是 10003，不是 40002 或 40005。3819 同样是 10003。
func TestApplyPostFrozenDebitMiss(t *testing.T) {
	base := func() *fakePostTx {
		return &fakePostTx{accounts: map[uint64]lockedAccount{
			1: activeAccount(1, directionCredit, 3000, 3000, overdraftNo),
			2: activeAccount(2, directionDebit, 10000, 0, overdraftYes),
		}, forceZero: map[uint64]bool{1: true}}
	}
	req := postRequest{
		TxnNo: "TX7", BizType: bizRefund, BizNo: "R2", Debit: 3000, FrozenAccountID: 1,
		Entries: []Entry{
			{AccountID: 1, Direction: directionDebit, Amount: 3000},
			{AccountID: 2, Direction: directionCredit, Amount: 3000},
		},
	}
	short := base()
	short.reloadRow = map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 3000, 10, overdraftNo),
	}
	_, err := applyPost(context.Background(), short, req)
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) || errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal("short frozen became a business rejection")
	}
	if len(short.entries) != 0 || short.accounts[2].Balance != 10000 {
		t.Fatal("miss wrote the other leg")
	}

	gone := base()
	gone.reloadMissing = map[uint64]bool{1: true}
	_, err = applyPost(context.Background(), gone, req)
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrUnbalancedEntries) {
		t.Fatal("missing row became 40003")
	}

	still := base()
	_, err = applyPost(context.Background(), still, req)
	assertErrno(t, err, errno.ErrInternal)
	if len(still.entries) != 0 {
		t.Fatal("enough frozen still inserted entries")
	}

	check := &mysql.MySQLError{Number: 3819, Message: "Check constraint 'ck_account_available' is violated"}
	violated := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 3000, 3000, overdraftNo),
		2: activeAccount(2, directionDebit, 10000, 0, overdraftYes),
	}, updateErr: check}
	_, err = applyPost(context.Background(), violated, req)
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) || !errors.Is(err, check) {
		t.Fatalf("check error = %v", err)
	}
	if len(violated.entries) != 0 {
		t.Fatal("check violation inserted entries")
	}
}

// ACC-06：平台贷方不是 ACTIVE 时仍是 40005，且不更新。商户腿不因此先返回 40002。
func TestApplyPostFrozenDebitPlatformInactive(t *testing.T) {
	merchant := activeAccount(1, directionCredit, 3000, 3000, overdraftNo)
	platform := activeAccount(9, directionCredit, 0, 0, overdraftYes)
	platform.Status = "CLOSED"
	fake := &fakePostTx{accounts: map[uint64]lockedAccount{1: merchant, 9: platform}}
	_, err := applyPost(context.Background(), fake, postRequest{
		TxnNo: "TX6", BizType: bizTransferOut, BizNo: "T1", Debit: 3000, FrozenAccountID: 1,
		Entries: []Entry{
			{AccountID: 1, Direction: directionDebit, Amount: 3000},
			{AccountID: 9, Direction: directionCredit, Amount: 3000},
		},
	})
	assertErrno(t, err, errno.ErrAccountInactive)
	if fake.updates != 0 || len(fake.entries) != 0 || fake.accounts[1].FrozenAmount != 3000 {
		t.Fatal("inactive platform updated a balance")
	}
}

// ACC-06：没有冻结单或已 RELEASED 返回 40004，不调用记账。
func TestApplyConfirmMissingAndReleased(t *testing.T) {
	req := confirmRequest{BizType: bizRefund, BizNo: "B1", MerchantID: "M100002", Channel: "mockpay"}
	missing := &fakeConfirmTx{}
	post := &fakePostTx{}
	_, err := applyConfirm(context.Background(), newIDs(t, &stepClock{times: []time.Time{openTestTime}}), testSubCount, missing, post, req)
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	if post.inserts != 0 || post.locks != 0 || missing.confirmCalls != 0 {
		t.Fatal("missing freeze posted or confirmed")
	}

	released := &fakeConfirmTx{lockFound: true, locked: confirmRow{
		FreezeNo: "FZ0000000000000000001", AccountID: 4, Amount: 20, Status: statusReleased,
	}}
	post = &fakePostTx{}
	_, err = applyConfirm(context.Background(), newIDs(t, &stepClock{times: []time.Time{openTestTime}}), testSubCount, released, post, req)
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	if post.inserts != 0 || released.merchantCalls != 0 || released.confirmCalls != 0 {
		t.Fatal("released freeze posted")
	}
}

// ACC-06：已 CONFIRMED 但 txn_no 为空是 10003。有 txn_no 时忽略 channel，不再记账。
func TestApplyConfirmReplay(t *testing.T) {
	empty := &fakeConfirmTx{lockFound: true, locked: confirmRow{
		FreezeNo: "FZ0000000000000000002", AccountID: 4, Amount: 20, Status: statusConfirmed,
	}}
	post := &fakePostTx{}
	_, err := applyConfirm(context.Background(), nil, testSubCount, empty, post, confirmRequest{
		BizType: bizRefund, BizNo: "B1", MerchantID: "M100002", Channel: "mockpay",
	})
	assertErrno(t, err, errno.ErrInternal)
	if post.inserts != 0 || empty.confirmCalls != 0 {
		t.Fatal("empty txn_no posted")
	}

	replay := &fakeConfirmTx{lockFound: true, locked: confirmRow{
		FreezeNo: "FZ0000000000000000003", AccountID: 4, Amount: 20, Status: statusConfirmed, TxnNo: "TXOLD",
	}}
	post = &fakePostTx{}
	got, err := applyConfirm(context.Background(), nil, testSubCount, replay, post, confirmRequest{
		BizType: bizRefund, BizNo: "B1", MerchantID: "M100002", Channel: "other",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Duplicated || got.FreezeNo != "FZ0000000000000000003" || got.TxnNo != "TXOLD" {
		t.Fatalf("replay = %+v", got)
	}
	if post.inserts != 0 || replay.merchantCalls != 0 {
		t.Fatal("replay posted")
	}
}

// ACC-06：凭证已存在而冻结单仍是 FROZEN 时返回 10003，不 CAS。
func TestApplyConfirmDuplicateVoucher(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime}}
	db := &fakeConfirmTx{
		lockFound:     true,
		locked:        confirmRow{FreezeNo: "FZ0000000000000000004", AccountID: 4, Amount: 30, Status: statusFrozen},
		accountFound:  true,
		account:       freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		platformFound: true,
		platformID:    9,
		confirmOK:     true,
	}
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	post := &fakePostTx{insertErr: dup, txnFound: true, txnNo: "TXEXIST"}
	_, err := applyConfirm(context.Background(), newIDs(t, clk), testSubCount, db, post, confirmRequest{
		BizType: bizRefund, BizNo: "B1", MerchantID: "M100002", Channel: "mockpay",
	})
	assertErrno(t, err, errno.ErrInternal)
	if db.confirmCalls != 0 {
		t.Fatal("duplicate voucher ran CAS")
	}
	if post.locks != 0 || post.updates != 0 || len(post.entries) != 0 {
		t.Fatal("duplicate voucher changed balances")
	}
	if db.platformType != typeChannelReceivable || db.platformOwner != "mockpay" {
		t.Fatalf("platform lookup = %s %s sub %d", db.platformOwner, db.platformType, db.platformSub)
	}
	wantSub, err := shard.Sub("B1", testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	if db.platformSub != wantSub {
		t.Fatalf("sub = %d, want %d", db.platformSub, wantSub)
	}
}

// ACC-06：FROZEN 的其余出口。透支、错账户、缺平台、同 id、子账户个数、CAS 0 行。
func TestApplyConfirmFrozenBranches(t *testing.T) {
	ids := newIDs(t, stubClock{now: openTestTime})
	req := confirmRequest{BizType: bizRefund, BizNo: "B1", MerchantID: "M100002", Channel: "mockpay"}
	row := confirmRow{FreezeNo: "FZ0000000000000000005", AccountID: 4, Amount: 30, Status: statusFrozen}
	accounts := map[uint64]lockedAccount{
		4: activeAccount(4, directionCredit, 30, 30, overdraftNo),
		9: activeAccount(9, directionDebit, 100, 0, overdraftYes),
	}

	over := &fakeConfirmTx{
		lockFound: true, locked: row, accountFound: true,
		account: freezeAccount{ID: 4, AllowOverdraft: overdraftYes, Status: statusActive},
	}
	post := &fakePostTx{}
	_, err := applyConfirm(context.Background(), ids, testSubCount, over, post, req)
	assertErrno(t, err, errno.ErrInvalidParam)
	if post.inserts != 0 || over.confirmCalls != 0 {
		t.Fatal("overdraft posted")
	}

	mismatch := &fakeConfirmTx{
		lockFound: true, locked: row, accountFound: true,
		account: freezeAccount{ID: 8, AllowOverdraft: overdraftNo, Status: statusActive},
	}
	_, err = applyConfirm(context.Background(), ids, testSubCount, mismatch, post, req)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if mismatch.platformCalls != 0 {
		t.Fatal("mismatched account looked up the platform")
	}

	missingPlatform := &fakeConfirmTx{
		lockFound: true, locked: row, accountFound: true,
		account: freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
	}
	_, err = applyConfirm(context.Background(), ids, testSubCount, missingPlatform, post, req)
	assertErrno(t, err, errno.ErrAccountNotFound)
	if errors.Is(err, errno.ErrUnbalancedEntries) || post.inserts != 0 {
		t.Fatal("missing platform became 40003 or posted")
	}

	same := &fakeConfirmTx{
		lockFound: true, locked: row, accountFound: true,
		account:       freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		platformFound: true, platformID: 4,
	}
	_, err = applyConfirm(context.Background(), ids, testSubCount, same, post, req)
	assertErrno(t, err, errno.ErrInternal)
	if post.inserts != 0 || same.confirmCalls != 0 {
		t.Fatal("same account id posted")
	}

	badSub := &fakeConfirmTx{
		lockFound: true, locked: row, accountFound: true,
		account: freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
	}
	_, err = applyConfirm(context.Background(), ids, 0, badSub, post, req)
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, shard.ErrSubCount) || badSub.platformCalls != 0 {
		t.Fatalf("sub error = %v calls = %d", err, badSub.platformCalls)
	}

	unknown := &fakeConfirmTx{lockFound: true, locked: confirmRow{FreezeNo: "FZ0000000000000000006", Status: "OTHER"}}
	_, err = applyConfirm(context.Background(), ids, testSubCount, unknown, post, req)
	assertErrno(t, err, errno.ErrInternal)
	if unknown.merchantCalls != 0 {
		t.Fatal("unknown status continued")
	}

	noCAS := &fakeConfirmTx{
		lockFound: true, locked: row, accountFound: true,
		account:       freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		platformFound: true, platformID: 9,
	}
	post = &fakePostTx{accounts: cloneAccounts(accounts)}
	_, err = applyConfirm(context.Background(), ids, testSubCount, noCAS, post, req)
	assertErrno(t, err, errno.ErrInternal)
	if noCAS.confirmCalls != 1 || len(post.entries) == 0 {
		t.Fatal("CAS miss did not post before returning")
	}

	okDB := &fakeConfirmTx{
		lockFound: true, locked: row, accountFound: true,
		account:       freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: "CLOSED"},
		platformFound: true, platformID: 9, confirmOK: true,
	}
	post = &fakePostTx{accounts: cloneAccounts(accounts)}
	closed := post.accounts[4]
	closed.Status = "CLOSED"
	post.accounts[4] = closed
	got, err := applyConfirm(context.Background(), ids, testSubCount, okDB, post, req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicated || got.FreezeNo != row.FreezeNo || got.TxnNo == "" || okDB.confirmedTxn != got.TxnNo {
		t.Fatalf("result = %+v stored = %s", got, okDB.confirmedTxn)
	}
	if post.accounts[4].Balance != 0 || post.accounts[4].FrozenAmount != 0 {
		t.Fatalf("closed merchant = %+v", post.accounts[4])
	}
	if post.inserted.BizType != bizRefund || post.inserted.Memo != "" || post.inserted.BizNo != "B1" {
		t.Fatalf("txn = %+v", post.inserted)
	}

	transfer := &fakeConfirmTx{
		lockFound: true, locked: confirmRow{FreezeNo: "FZ0000000000000000007", AccountID: 4, Amount: 30, Status: statusFrozen},
		accountFound: true, account: freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		platformFound: true, platformID: 9, confirmOK: true,
	}
	post = &fakePostTx{accounts: map[uint64]lockedAccount{
		4: activeAccount(4, directionCredit, 30, 30, overdraftNo),
		9: activeAccount(9, directionCredit, 0, 0, overdraftYes),
	}}
	got, err = applyConfirm(context.Background(), ids, testSubCount, transfer, post, confirmRequest{
		BizType: bizTransfer, BizNo: "B1", MerchantID: "M100002",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicated || post.inserted.BizType != bizTransferOut || transfer.platformOwner != platformOwnerID || transfer.platformType != typeTransit {
		t.Fatalf("transfer txn = %+v owner = %s type = %s", post.inserted, transfer.platformOwner, transfer.platformType)
	}
	if post.accounts[9].Balance != 30 {
		t.Fatalf("transit balance = %d", post.accounts[9].Balance)
	}
}

func TestApplyConfirmClockRollback(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime, openTestTime.Add(-10 * time.Millisecond)}}
	ids := newIDs(t, clk)
	if _, err := ids.NewNo(context.Background(), txnNoPrefix); err != nil {
		t.Fatal(err)
	}
	db := &fakeConfirmTx{
		lockFound:    true,
		locked:       confirmRow{FreezeNo: "FZ0000000000000000008", AccountID: 4, Amount: 30, Status: statusFrozen},
		accountFound: true, account: freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		platformFound: true, platformID: 9, confirmOK: true,
	}
	post := &fakePostTx{}
	_, err := applyConfirm(context.Background(), ids, testSubCount, db, post, confirmRequest{
		BizType: bizRefund, BizNo: "B1", MerchantID: "M100002", Channel: "mockpay",
	})
	assertErrno(t, err, errno.ErrUnavailable)
	if !errors.Is(err, idgen.ErrClockRollback) {
		t.Fatal("clock rollback cause lost")
	}
	if post.inserts != 0 || db.confirmCalls != 0 {
		t.Fatal("clock rollback posted")
	}
}

func TestConfirmSQLHelpers(t *testing.T) {
	if strings.Contains(lockFreezeSQL, "txn_no") {
		t.Fatal("lockFreezeSQL column list changed")
	}
	if !strings.Contains(lockConfirmSQL, "txn_no") || !strings.Contains(lockConfirmSQL, "FOR UPDATE") {
		t.Fatalf("confirm lock = %s", lockConfirmSQL)
	}
	if !strings.Contains(updateFrozenDebitSQL, "frozen_amount = frozen_amount - ?") || strings.Contains(updateFrozenDebitSQL, "status") {
		t.Fatalf("frozen debit sql = %s", updateFrozenDebitSQL)
	}
	if !strings.Contains(markConfirmedSQL, "status = 'CONFIRMED'") || !strings.Contains(markConfirmedSQL, "txn_no = ?") {
		t.Fatalf("confirm sql = %s", markConfirmedSQL)
	}
	var db sqlFreezeTx
	ctx := context.Background()
	if _, _, err := db.lockConfirm(ctx, bizRefund, "B1"); !errors.Is(err, errNilTx) {
		t.Fatalf("lockConfirm = %v", err)
	}
	if _, _, err := db.selectOwnerAccount(ctx, ownerPlatform, "mockpay", typeChannelReceivable, 0); !errors.Is(err, errNilTx) {
		t.Fatalf("selectOwnerAccount = %v", err)
	}
	if _, err := db.markConfirmed(ctx, bizRefund, "B1", "TX1", 1, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("markConfirmed = %v", err)
	}
}

func cloneAccounts(in map[uint64]lockedAccount) map[uint64]lockedAccount {
	out := make(map[uint64]lockedAccount, len(in))
	for id, row := range in {
		out[id] = row
	}
	return out
}

type fakeConfirmTx struct {
	lockFound     bool
	locked        confirmRow
	lockErr       error
	accountFound  bool
	account       freezeAccount
	accountErr    error
	merchantCalls int

	platformID    uint64
	platformFound bool
	platformErr   error
	platformCalls int
	platformOwner string
	platformType  string
	platformSub   int

	confirmCalls int
	confirmedTxn string
	confirmOK    bool
	confirmErr   error
}

func (f *fakeConfirmTx) lockConfirm(context.Context, string, string) (confirmRow, bool, error) {
	if f.lockErr != nil {
		return confirmRow{}, false, f.lockErr
	}
	return f.locked, f.lockFound, nil
}

func (f *fakeConfirmTx) selectMerchant(context.Context, string) (freezeAccount, bool, error) {
	f.merchantCalls++
	if f.accountErr != nil {
		return freezeAccount{}, false, f.accountErr
	}
	return f.account, f.accountFound, nil
}

func (f *fakeConfirmTx) selectOwnerAccount(_ context.Context, _, ownerID, accountType string, subNo int) (uint64, bool, error) {
	f.platformCalls++
	f.platformOwner = ownerID
	f.platformType = accountType
	f.platformSub = subNo
	if f.platformErr != nil {
		return 0, false, f.platformErr
	}
	return f.platformID, f.platformFound, nil
}

func (f *fakeConfirmTx) markConfirmed(_ context.Context, _, _, txnNo string, _ money.Amount, _ uint64) (bool, error) {
	f.confirmCalls++
	f.confirmedTxn = txnNo
	if f.confirmErr != nil {
		return false, f.confirmErr
	}
	return f.confirmOK, nil
}
