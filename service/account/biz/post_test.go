package biz

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/dal"
)

func assertErrno(t *testing.T, err error, want *errno.Error) {
	t.Helper()
	if want == nil {
		t.Fatal("want is nil")
	}
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %s", err, want.Error())
	}
	if err.Error() != want.Error() {
		t.Fatalf("Error() = %q, want %q", err.Error(), want.Error())
	}
}

func pairEntries(debitID, creditID uint64, amount money.Amount) []Entry {
	return []Entry{
		{AccountID: debitID, Direction: directionDebit, Amount: amount},
		{AccountID: creditID, Direction: directionCredit, Amount: amount},
	}
}

func activeAccount(id uint64, direction string, balance, frozen money.Amount, overdraft int) lockedAccount {
	return lockedAccount{
		ID:               id,
		BalanceDirection: direction,
		Balance:          balance,
		FrozenAmount:     frozen,
		AllowOverdraft:   overdraft,
		Status:           statusActive,
		Currency:         currencyCNY,
	}
}

// ACC-03 规则：不碰库的校验失败即 40003，不发号。
func TestPostRejectsBeforeID(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	ctx := context.Background()
	longMemo := strings.Repeat("记", 256)
	memo255 := strings.Repeat("记", 255)
	biz64 := strings.Repeat("单", 64)
	cases := []struct {
		name    string
		bizType string
		bizNo   string
		memo    string
		entries []Entry
	}{
		{name: "unbalanced", bizType: bizPay, bizNo: "B1", entries: []Entry{
			{AccountID: 1, Direction: directionDebit, Amount: 3},
			{AccountID: 2, Direction: directionCredit, Amount: 2},
		}},
		{name: "amount zero", bizType: bizPay, bizNo: "B1", entries: []Entry{
			{AccountID: 1, Direction: directionDebit, Amount: 0},
			{AccountID: 2, Direction: directionCredit, Amount: 0},
		}},
		{name: "amount negative", bizType: bizPay, bizNo: "B1", entries: []Entry{
			{AccountID: 1, Direction: directionDebit, Amount: -1},
			{AccountID: 2, Direction: directionCredit, Amount: -1},
		}},
		{name: "one entry", bizType: bizPay, bizNo: "B1", entries: []Entry{
			{AccountID: 1, Direction: directionDebit, Amount: 1},
		}},
		{name: "zero entries", bizType: bizPay, bizNo: "B1"},
		{name: "eleven entries", bizType: bizPay, bizNo: "B1", entries: elevenBalanced()},
		{name: "same account", bizType: bizPay, bizNo: "B1", entries: []Entry{
			{AccountID: 7, Direction: directionDebit, Amount: 5},
			{AccountID: 7, Direction: directionCredit, Amount: 5},
		}},
		{name: "reversal", bizType: "REVERSAL", bizNo: "TX1", entries: pairEntries(1, 2, 5)},
		{name: "unknown biz type", bizType: "ADJUST", bizNo: "B1", entries: pairEntries(1, 2, 5)},
		{name: "lower biz type", bizType: "pay", bizNo: "B1", entries: pairEntries(1, 2, 5)},
		{name: "empty biz no", bizType: bizPay, bizNo: "", entries: pairEntries(1, 2, 5)},
		{name: "biz no 65", bizType: bizPay, bizNo: biz64 + "多", entries: pairEntries(1, 2, 5)},
		{name: "memo 256", bizType: bizPay, bizNo: "B1", memo: longMemo, entries: pairEntries(1, 2, 5)},
		{name: "bad direction", bizType: bizPay, bizNo: "B1", entries: []Entry{
			{AccountID: 1, Direction: "debit", Amount: 5},
			{AccountID: 2, Direction: directionCredit, Amount: 5},
		}},
		{name: "sum overflow", bizType: bizPay, bizNo: "B1", entries: []Entry{
			{AccountID: 1, Direction: directionDebit, Amount: money.Amount(math.MaxInt64)},
			{AccountID: 2, Direction: directionDebit, Amount: 1},
			{AccountID: 3, Direction: directionCredit, Amount: money.Amount(math.MaxInt64)},
			{AccountID: 4, Direction: directionCredit, Amount: 1},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Post(ctx, &sqlx.Tx{}, tc.bizType, tc.bizNo, tc.memo, tc.entries)
			assertErrno(t, err, errno.ErrUnbalancedEntries)
			if errors.Is(err, errno.ErrAccountNotFound) {
				t.Fatal("validation became 40001")
			}
		})
	}
	if clk.i != 0 {
		t.Fatalf("NewNo calls = %d", clk.i)
	}
	// 边界长度只校验，仍然不发号。nil tx 说明已经过了校验。
	_, err := svc.Post(ctx, nil, bizPay, biz64, memo255, pairEntries(1, 2, 1))
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("boundary check issued an id")
	}
	debit, err := validatePost(bizTransferOut, "OUT-1", "", pairEntries(1, 2, 9))
	if err != nil || debit != 9 {
		t.Fatalf("transfer out debit=%d err=%v", debit, err)
	}
	for _, bizType := range []string{bizRefund, bizTransfer, bizTransferIn} {
		if _, err := validatePost(bizType, "N1", "", pairEntries(1, 2, 1)); err != nil {
			t.Fatalf("%s: %v", bizType, err)
		}
	}
}

func elevenBalanced() []Entry {
	out := make([]Entry, 11)
	for i := range out {
		out[i] = Entry{AccountID: uint64(i + 1), Direction: directionDebit, Amount: 1}
	}
	return out
}

func TestPostNilAndCanceled(t *testing.T) {
	ctx := context.Background()
	entries := pairEntries(1, 2, 1)
	_, err := (*Service)(nil).Post(ctx, &sqlx.Tx{}, bizPay, "B1", "", entries)
	assertErrno(t, err, errno.ErrInternal)

	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	// SA1012 不允许把 nil context 当参数。这里正是要证明 nil ctx 返回 10003，而不是改成 TODO。
	_, err = svc.Post(nil, &sqlx.Tx{}, bizPay, "B1", "", entries) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)

	_, err = svc.Post(ctx, nil, bizPay, "B1", "", entries)
	assertErrno(t, err, errno.ErrInternal)

	_, err = New(dal.New(nil, nil), nil, testSubCount).Post(ctx, &sqlx.Tx{}, bizPay, "B1", "", entries)
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil tx issued an id")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.Post(canceled, &sqlx.Tx{}, bizPay, "B1", "", entries)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
	_, err = svc.Post(canceled, &sqlx.Tx{}, "REVERSAL", "B1", "", entries)
	assertErrno(t, err, errno.ErrUnbalancedEntries)
	if clk.i != 0 {
		t.Fatal("canceled request issued an id")
	}
}

func TestPostIDErrors(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime, openTestTime.Add(-10 * time.Millisecond)}}
	ids := newIDs(t, clk)
	if _, err := ids.NewNo(context.Background(), "TX"); err != nil {
		t.Fatal(err)
	}
	svc := New(dal.New(nil, nil), ids, testSubCount)
	_, err := svc.Post(context.Background(), &sqlx.Tx{}, bizPay, "B1", "", pairEntries(1, 2, 1))
	assertErrno(t, err, errno.ErrUnavailable)
	if !errors.Is(err, idgen.ErrClockRollback) {
		t.Fatal("clock rollback cause lost")
	}

	early := time.Date(2025, 12, 31, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	svc = New(dal.New(nil, nil), newIDs(t, stubClock{now: early}), testSubCount)
	_, err = svc.Post(context.Background(), &sqlx.Tx{}, bizPay, "B1", "", pairEntries(1, 2, 1))
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, idgen.ErrTimeRange) {
		t.Fatal("time range cause lost")
	}
	if strings.Contains(err.Error(), "idgen") || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("Error() leaked: %q", err.Error())
	}
}

func TestApplyPostDuplicateSkipsBalance(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'PAY-B1' for key 'uk_biz'"}
	fake := &fakePostTx{insertErr: dup, txnNo: "TX0000000000000000001", txnFound: true}
	got, err := applyPost(context.Background(), fake, postRequest{
		TxnNo:   "TX0000000000000000099",
		BizType: bizPay,
		BizNo:   "B1",
		Memo:    "changed",
		Entries: pairEntries(2, 1, 99),
		Debit:   99,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Duplicated || got.TxnNo != "TX0000000000000000001" {
		t.Fatalf("result = %+v", got)
	}
	if fake.updates != 0 || fake.locks != 0 || len(fake.entries) != 0 {
		t.Fatalf("duplicate locked=%d updated=%d entries=%d", fake.locks, fake.updates, len(fake.entries))
	}
	if fake.inserts != 1 {
		t.Fatalf("inserts = %d", fake.inserts)
	}
}

func TestApplyPostDuplicateMissIsInternal(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'TX' for key 'uk_txn_no'"}
	fake := &fakePostTx{insertErr: dup, txnFound: false}
	_, err := applyPost(context.Background(), fake, sampleRequest())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, dup) {
		t.Fatal("duplicate cause lost")
	}
	if fake.updates != 0 || fake.locks != 0 {
		t.Fatal("missing duplicate updated a balance")
	}

	text := errors.New("Error 1062: Duplicate entry")
	textFake := &fakePostTx{insertErr: text}
	_, err = applyPost(context.Background(), textFake, sampleRequest())
	assertErrno(t, err, errno.ErrInternal)
	if textFake.updates != 0 || textFake.locks != 0 {
		t.Fatal("text 1062 updated a balance")
	}

	selectErr := errors.New("SELECT txn password=secret")
	_, err = applyPost(context.Background(), &fakePostTx{insertErr: dup, selectErr: selectErr}, sampleRequest())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, selectErr) || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("select error leaked: %v", err)
	}
}

func TestApplyPostBalanceDirections(t *testing.T) {
	// CREDIT 账户借记减少，可用余额要扣掉冻结。
	credit := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 100, 30, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	got, err := applyPost(context.Background(), credit, postRequest{
		TxnNo: "TX1", BizType: bizPay, BizNo: "B1", Memo: "m", Debit: 40,
		Entries: []Entry{
			{AccountID: 2, Direction: directionCredit, Amount: 40},
			{AccountID: 1, Direction: directionDebit, Amount: 40},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicated || got.TxnNo != "TX1" {
		t.Fatalf("result = %+v", got)
	}
	if credit.lockOrder[0] != 1 || credit.lockOrder[1] != 2 {
		t.Fatalf("lock order = %v", credit.lockOrder)
	}
	assertUpdate(t, credit.deltas[1], -40, money.Amount(40))
	assertUpdate(t, credit.deltas[2], 40, 0)
	if credit.accounts[1].Balance != 60 || credit.accounts[1].FrozenAmount != 30 {
		t.Fatalf("credit account = %+v", credit.accounts[1])
	}
	if credit.accounts[2].Balance != 40 {
		t.Fatalf("fee balance = %d", credit.accounts[2].Balance)
	}
	if len(credit.entries) != 2 || credit.entries[0].BalanceAfter != 60 || credit.entries[1].BalanceAfter != 40 {
		t.Fatalf("entries = %+v", credit.entries)
	}
	if credit.inserted.Amount != 40 || credit.inserted.Currency != currencyCNY || credit.inserted.EntryCount != 2 || credit.inserted.Memo != "m" {
		t.Fatalf("txn = %+v", credit.inserted)
	}

	// DEBIT 账户借记增加，不检查可用余额。
	debit := &fakePostTx{accounts: map[uint64]lockedAccount{
		5: activeAccount(5, directionDebit, 8, 0, overdraftYes),
		9: activeAccount(9, directionCredit, 1, 0, overdraftYes),
	}}
	_, err = applyPost(context.Background(), debit, postRequest{
		TxnNo: "TX2", BizType: bizPay, BizNo: "B2", Debit: 3,
		Entries: pairEntries(5, 9, 3),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertUpdate(t, debit.deltas[5], 3, 0)
	if debit.accounts[5].Balance != 11 || debit.entries[0].BalanceAfter != 11 {
		t.Fatalf("debit account = %+v entries = %+v", debit.accounts[5], debit.entries)
	}

	// allow_overdraft = 1 时减少余额不看可用余额。
	over := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 5, 4, overdraftYes),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	_, err = applyPost(context.Background(), over, postRequest{
		TxnNo: "TX3", BizType: bizTransfer, BizNo: "B3", Debit: 10,
		Entries: pairEntries(1, 2, 10),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertUpdate(t, over.deltas[1], -10, 0)
	if over.accounts[1].Balance != -5 {
		t.Fatalf("overdraft balance = %d", over.accounts[1].Balance)
	}
}

func assertUpdate(t *testing.T, got balanceUpdate, delta money.Amount, guard money.Amount) {
	t.Helper()
	if got.delta != delta {
		t.Fatalf("delta = %d, want %d", got.delta, delta)
	}
	if guard == 0 {
		if got.guard != nil {
			t.Fatalf("guard = %d, want none", *got.guard)
		}
		return
	}
	if got.guard == nil || *got.guard != guard {
		t.Fatalf("guard = %v, want %d", got.guard, guard)
	}
}

func TestApplyPostRejectsAccountState(t *testing.T) {
	missing := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 10, 0, overdraftNo),
	}}
	_, err := applyPost(context.Background(), missing, sampleRequest())
	assertErrno(t, err, errno.ErrUnbalancedEntries)
	if errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatal("missing account returned 40001")
	}
	if missing.updates != 0 || len(missing.entries) != 0 {
		t.Fatal("missing account wrote")
	}

	inactive := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 100, 0, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	row := inactive.accounts[2]
	row.Status = "FROZEN"
	inactive.accounts[2] = row
	_, err = applyPost(context.Background(), inactive, sampleRequest())
	assertErrno(t, err, errno.ErrAccountInactive)
	if inactive.updates != 0 {
		t.Fatal("inactive account updated a balance")
	}

	short := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 10, 0, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	req := sampleRequest()
	req.Debit = 20
	req.Entries = pairEntries(1, 2, 20)
	_, err = applyPost(context.Background(), short, req)
	assertErrno(t, err, errno.ErrInsufficientBalance)
	if short.updates != 0 || len(short.entries) != 0 {
		t.Fatal("insufficient balance wrote")
	}

	frozen := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 100, 80, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	req = sampleRequest()
	req.Debit = 30
	req.Entries = pairEntries(1, 2, 30)
	_, err = applyPost(context.Background(), frozen, req)
	assertErrno(t, err, errno.ErrInsufficientBalance)

	usd := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 100, 0, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	row = usd.accounts[2]
	row.Currency = "USD"
	usd.accounts[2] = row
	_, err = applyPost(context.Background(), usd, sampleRequest())
	assertErrno(t, err, errno.ErrUnbalancedEntries)
	if usd.updates != 0 {
		t.Fatal("currency mismatch updated a balance")
	}

	badDirection := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, "OTHER", 100, 0, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	_, err = applyPost(context.Background(), badDirection, sampleRequest())
	assertErrno(t, err, errno.ErrInternal)
	if badDirection.updates != 0 {
		t.Fatal("bad balance direction updated a balance")
	}
}

// ACC-03：较小账户余额不足时，仍先返回较大账户的非 ACTIVE，且不更新余额。
func TestApplyPostInactiveBeforeInsufficient(t *testing.T) {
	fake := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 10, 0, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	row := fake.accounts[2]
	row.Status = "CLOSED"
	fake.accounts[2] = row
	req := sampleRequest()
	req.Debit = 20
	req.Entries = pairEntries(1, 2, 20)
	_, err := applyPost(context.Background(), fake, req)
	if !errors.Is(err, errno.ErrAccountInactive) {
		t.Fatalf("error = %v, want %s", err, errno.ErrAccountInactive.Error())
	}
	if fake.updates != 0 || len(fake.entries) != 0 {
		t.Fatal("inactive account updated a balance")
	}
}

func TestApplyPostZeroUpdate(t *testing.T) {
	base := func() *fakePostTx {
		return &fakePostTx{
			accounts: map[uint64]lockedAccount{
				1: activeAccount(1, directionCredit, 100, 0, overdraftNo),
				2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
			},
			forceZero: map[uint64]bool{1: true},
		}
	}
	inactive := base()
	inactive.reloadRow = map[uint64]lockedAccount{
		1: {ID: 1, Status: "CLOSED", Currency: currencyCNY, BalanceDirection: directionCredit, Balance: 100},
	}
	_, err := applyPost(context.Background(), inactive, sampleRequest())
	assertErrno(t, err, errno.ErrAccountInactive)
	if len(inactive.entries) != 0 {
		t.Fatal("zero update inserted entries")
	}

	low := base()
	low.reloadRow = map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 0, 0, overdraftNo),
	}
	_, err = applyPost(context.Background(), low, sampleRequest())
	assertErrno(t, err, errno.ErrInsufficientBalance)

	gone := base()
	gone.reloadMissing = map[uint64]bool{1: true}
	_, err = applyPost(context.Background(), gone, sampleRequest())
	assertErrno(t, err, errno.ErrUnbalancedEntries)

	same := base()
	_, err = applyPost(context.Background(), same, sampleRequest())
	assertErrno(t, err, errno.ErrInternal)
}

func TestApplyPostBalanceOverflow(t *testing.T) {
	fake := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, money.Amount(math.MaxInt64), 0, overdraftYes),
		2: activeAccount(2, directionDebit, 0, 0, overdraftYes),
	}}
	_, err := applyPost(context.Background(), fake, postRequest{
		TxnNo: "TX1", BizType: bizPay, BizNo: "B1", Debit: 1,
		Entries: []Entry{
			{AccountID: 2, Direction: directionCredit, Amount: 1},
			{AccountID: 1, Direction: directionCredit, Amount: 1},
		},
	})
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatal("overflow cause lost")
	}
	if fake.updates != 0 {
		t.Fatal("overflow updated a balance")
	}
}

func TestApplyPostHidesDriverError(t *testing.T) {
	secret := errors.New("INSERT INTO ledger_txn password=secret")
	_, err := applyPost(context.Background(), &fakePostTx{insertErr: secret}, sampleRequest())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, secret) {
		t.Fatal("cause lost")
	}

	check := &mysql.MySQLError{Number: 3819, Message: "Check constraint 'ck_account_available' is violated"}
	fake := &fakePostTx{accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 100, 0, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}, updateErr: check}
	_, err = applyPost(context.Background(), fake, sampleRequest())
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatal("check violation became 40002")
	}

	_, err = applyPost(context.Background(), &fakePostTx{insertErr: context.Canceled}, sampleRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	lockErr := &fakePostTx{lockErr: context.DeadlineExceeded, accounts: map[uint64]lockedAccount{
		1: activeAccount(1, directionCredit, 100, 0, overdraftNo),
		2: activeAccount(2, directionCredit, 0, 0, overdraftYes),
	}}
	_, err = applyPost(context.Background(), lockErr, sampleRequest())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	if errors.Is(err, errno.ErrAccountNotFound) || errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatal("context error became a business code")
	}
}

func TestPostSQLHelpers(t *testing.T) {
	query, args := buildEntryInsert([]ledgerEntryRow{
		{TxnNo: "TX1", AccountID: 2, Direction: directionCredit, Amount: 4, BalanceAfter: 4, BizType: bizPay, BizNo: "B1"},
		{TxnNo: "TX1", AccountID: 9, Direction: directionDebit, Amount: 4, BalanceAfter: -1, BizType: bizPay, BizNo: "B1"},
	})
	if strings.Count(query, "?") != 14 || strings.Contains(query, "B1") || strings.Contains(query, "TX1") {
		t.Fatalf("query = %s", query)
	}
	if len(args) != 14 || args[0] != "TX1" || args[7] != "TX1" {
		t.Fatalf("args = %#v", args)
	}
	if _, args := buildEntryInsert(nil); args != nil {
		t.Fatal("empty insert built args")
	}

	got, err := scanLocked(fakeScan{vals: []any{
		uint64(7), directionCredit, int64(100), int64(3), 0, statusActive, currencyCNY,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 7 || got.Balance != 100 || got.FrozenAmount != 3 || got.AllowOverdraft != 0 || got.Currency != currencyCNY {
		t.Fatalf("scanned = %+v", got)
	}
	if _, err := scanLocked(fakeScan{err: errors.New("SELECT boom")}); err == nil {
		t.Fatal("scan error lost")
	}

	var db sqlPostTx
	ctx := context.Background()
	if err := db.insertTxn(ctx, ledgerTxnInsert{}); !errors.Is(err, errNilTx) {
		t.Fatalf("insertTxn = %v", err)
	}
	if _, _, err := db.selectTxnNo(ctx, bizPay, "B1"); !errors.Is(err, errNilTx) {
		t.Fatalf("selectTxnNo = %v", err)
	}
	if _, _, err := db.lockAccount(ctx, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("lockAccount = %v", err)
	}
	guard := money.Amount(1)
	if _, err := db.updateBalance(ctx, 1, -1, &guard); !errors.Is(err, errNilTx) {
		t.Fatalf("updateBalance guard = %v", err)
	}
	if _, err := db.updateBalance(ctx, 1, 1, nil); !errors.Is(err, errNilTx) {
		t.Fatalf("updateBalance = %v", err)
	}
	if _, _, err := db.selectBalance(ctx, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("selectBalance = %v", err)
	}
	if _, _, err := db.reloadAccount(ctx, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("reloadAccount = %v", err)
	}
	if err := db.insertEntries(ctx, []ledgerEntryRow{{}}); !errors.Is(err, errNilTx) {
		t.Fatalf("insertEntries = %v", err)
	}
}

func sampleRequest() postRequest {
	return postRequest{
		TxnNo:   "TX1",
		BizType: bizPay,
		BizNo:   "B1",
		Debit:   1,
		Entries: pairEntries(1, 2, 1),
	}
}

type balanceUpdate struct {
	delta money.Amount
	guard *money.Amount
}

type fakePostTx struct {
	insertErr     error
	inserts       int
	inserted      ledgerTxnInsert
	txnNo         string
	txnFound      bool
	selectErr     error
	accounts      map[uint64]lockedAccount
	lockOrder     []uint64
	locks         int
	lockErr       error
	deltas        map[uint64]balanceUpdate
	updates       int
	updateErr     error
	forceZero     map[uint64]bool
	reloadRow     map[uint64]lockedAccount
	reloadMissing map[uint64]bool
	entries       []ledgerEntryRow
	entryErr      error
}

func (f *fakePostTx) insertTxn(_ context.Context, row ledgerTxnInsert) error {
	f.inserts++
	f.inserted = row
	return f.insertErr
}

func (f *fakePostTx) selectTxnNo(context.Context, string, string) (string, bool, error) {
	if f.selectErr != nil {
		return "", false, f.selectErr
	}
	return f.txnNo, f.txnFound, nil
}

func (f *fakePostTx) lockAccount(_ context.Context, accountID uint64) (lockedAccount, bool, error) {
	f.locks++
	f.lockOrder = append(f.lockOrder, accountID)
	if f.lockErr != nil {
		return lockedAccount{}, false, f.lockErr
	}
	row, ok := f.accounts[accountID]
	return row, ok, nil
}

func (f *fakePostTx) updateBalance(_ context.Context, accountID uint64, delta money.Amount, guard *money.Amount) (bool, error) {
	f.updates++
	if f.deltas == nil {
		f.deltas = map[uint64]balanceUpdate{}
	}
	copied := guard
	if guard != nil {
		v := *guard
		copied = &v
	}
	f.deltas[accountID] = balanceUpdate{delta: delta, guard: copied}
	if f.updateErr != nil {
		return false, f.updateErr
	}
	if f.forceZero[accountID] {
		return false, nil
	}
	row := f.accounts[accountID]
	next, err := money.Add(row.Balance, delta)
	if err != nil {
		return false, err
	}
	row.Balance = next
	f.accounts[accountID] = row
	return true, nil
}

func (f *fakePostTx) selectBalance(_ context.Context, accountID uint64) (money.Amount, bool, error) {
	row, ok := f.accounts[accountID]
	if !ok {
		return 0, false, nil
	}
	return row.Balance, true, nil
}

func (f *fakePostTx) reloadAccount(_ context.Context, accountID uint64) (lockedAccount, bool, error) {
	if f.reloadMissing[accountID] {
		return lockedAccount{}, false, nil
	}
	if row, ok := f.reloadRow[accountID]; ok {
		return row, true, nil
	}
	row, ok := f.accounts[accountID]
	return row, ok, nil
}

func (f *fakePostTx) insertEntries(_ context.Context, rows []ledgerEntryRow) error {
	if f.entryErr != nil {
		return f.entryErr
	}
	f.entries = append(f.entries, rows...)
	return nil
}
