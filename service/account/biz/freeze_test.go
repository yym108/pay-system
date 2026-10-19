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

// ACC-04：参数不合法返回 10001，不发号、不写行。
func TestFreezeRejectsBeforeID(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	ctx := context.Background()
	biz64 := strings.Repeat("单", 64)
	cases := []struct {
		name       string
		bizType    string
		bizNo      string
		merchantID string
		amount     money.Amount
	}{
		{name: "pay biz type", bizType: bizPay, bizNo: "B1", merchantID: "M100002", amount: 1},
		{name: "transfer out", bizType: bizTransferOut, bizNo: "B1", merchantID: "M100002", amount: 1},
		{name: "lower refund", bizType: "refund", bizNo: "B1", merchantID: "M100002", amount: 1},
		{name: "empty biz type", bizType: "", bizNo: "B1", merchantID: "M100002", amount: 1},
		{name: "empty biz no", bizType: bizRefund, bizNo: "", merchantID: "M100002", amount: 1},
		{name: "biz no 65", bizType: bizRefund, bizNo: biz64 + "多", merchantID: "M100002", amount: 1},
		{name: "amount zero", bizType: bizRefund, bizNo: "B1", merchantID: "M100002", amount: 0},
		{name: "amount negative", bizType: bizTransfer, bizNo: "B1", merchantID: "M100002", amount: -1},
		{name: "merchant short", bizType: bizRefund, bizNo: "B1", merchantID: "M10002", amount: 1},
		{name: "merchant long", bizType: bizRefund, bizNo: "B1", merchantID: "M1000002", amount: 1},
		{name: "merchant letter", bizType: bizRefund, bizNo: "B1", merchantID: "M10000A", amount: 1},
		{name: "merchant empty", bizType: bizRefund, bizNo: "B1", merchantID: "", amount: 1},
		{name: "merchant lower m", bizType: bizRefund, bizNo: "B1", merchantID: "m100002", amount: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Freeze(ctx, tc.bizType, tc.bizNo, tc.merchantID, tc.amount)
			assertErrno(t, err, errno.ErrInvalidParam)
		})
	}
	if clk.i != 0 {
		t.Fatalf("NewNo calls = %d", clk.i)
	}
	// 边界长度和最大正数只校验，nil db 说明已经过了校验，仍然不发号。
	_, err := svc.Freeze(ctx, bizRefund, biz64, "M100002", money.Amount(math.MaxInt64))
	assertErrno(t, err, errno.ErrInternal)
	_, err = svc.Freeze(ctx, bizTransfer, "B1", "M100001", 1)
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil db issued an id")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.Freeze(canceled, bizPay, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInvalidParam)
}

func TestFreezeNilAndCanceled(t *testing.T) {
	ctx := context.Background()
	_, err := (*Service)(nil).Freeze(ctx, bizRefund, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInternal)

	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	// SA1012 不允许把 nil context 当参数。这里正是要证明 nil ctx 返回 10003。
	_, err = svc.Freeze(nil, bizRefund, "B1", "M100002", 1) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)

	_, err = New(nil, newIDs(t, clk), testSubCount).Freeze(ctx, bizRefund, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInternal)
	_, err = New(dal.New(nil, nil), nil, testSubCount).Freeze(ctx, bizRefund, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil dependency issued an id")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.Freeze(canceled, bizRefund, "B1", "M100002", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
	if errors.Is(err, errno.ErrInternal) {
		t.Fatal("canceled context became 10003")
	}
	// openTestTime 早于本次运行日，截止时间已经过去，Err 是 DeadlineExceeded。不要再 cancel，否则会变成 Canceled。
	deadline, stop := context.WithDeadline(context.Background(), openTestTime)
	defer stop()
	_, err = svc.Freeze(deadline, bizTransfer, "B1", "M100001", 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline", err)
	}
	if clk.i != 0 {
		t.Fatal("canceled request issued an id")
	}
}

func TestFreezeIDErrors(t *testing.T) {
	// 分片池非 nil，发号才会执行。端口 1 没有服务，Begin 失败，不会真的写库。
	db, err := sqlx.Open("mysql", "freeze:unused@tcp(127.0.0.1:1)/none?timeout=100ms")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	clk := &stepClock{times: []time.Time{openTestTime, openTestTime.Add(-10 * time.Millisecond)}}
	svc := New(dal.New(db, db), newIDs(t, clk), testSubCount)
	_, err = svc.Freeze(context.Background(), bizRefund, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInternal)
	if strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), "unused") {
		t.Fatalf("Error() leaked: %q", err.Error())
	}
	_, err = svc.Freeze(context.Background(), bizRefund, "B2", "M100002", 1)
	assertErrno(t, err, errno.ErrUnavailable)
	if !errors.Is(err, idgen.ErrClockRollback) {
		t.Fatal("clock rollback cause lost")
	}
	if clk.i != 2 {
		t.Fatalf("NewNo calls = %d", clk.i)
	}

	earlyClk := &stepClock{times: []time.Time{time.Date(2025, 12, 31, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))}}
	early := New(dal.New(db, db), newIDs(t, earlyClk), testSubCount)
	_, err = early.Freeze(context.Background(), bizTransfer, "B1", "M100001", 1)
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, idgen.ErrTimeRange) {
		t.Fatal("time range cause lost")
	}
	if strings.Contains(err.Error(), "idgen") || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("Error() leaked: %q", err.Error())
	}
}

// CF-14 出口：allow_overdraft 不是 0，返回 10001，不插入冻结单。
// 真实库的 MERCHANT_SETTLE 插不出透支行，这条只走假实现。
func TestApplyFreezeOverdraft(t *testing.T) {
	fake := &fakeFreezeTx{accountFound: true, account: freezeAccount{
		ID: 7, Balance: 100, AllowOverdraft: overdraftYes, Status: statusActive,
	}}
	_, err := applyFreeze(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrInvalidParam)
	if fake.inserts != 0 || fake.addCalls != 0 {
		t.Fatalf("overdraft inserts=%d adds=%d", fake.inserts, fake.addCalls)
	}

	closed := &fakeFreezeTx{accountFound: true, account: freezeAccount{
		ID: 7, AllowOverdraft: overdraftYes, Status: "CLOSED",
	}}
	_, err = applyFreeze(context.Background(), closed, sampleFreeze())
	assertErrno(t, err, errno.ErrInvalidParam)
	if closed.inserts != 0 {
		t.Fatal("overdraft of a closed account inserted a freeze")
	}
}

// CF-14 出口：分片上没有商户账户，返回 40001，不写冻结单。
func TestApplyFreezeAccountMissing(t *testing.T) {
	fake := &fakeFreezeTx{}
	_, err := applyFreeze(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrAccountNotFound)
	if fake.inserts != 0 || fake.addCalls != 0 {
		t.Fatal("missing account wrote a freeze")
	}
}

func TestApplyFreezeSuccess(t *testing.T) {
	fake := &fakeFreezeTx{accountFound: true, account: freezeAccount{
		ID: 4, Balance: 1000, FrozenAmount: 10, AllowOverdraft: overdraftNo, Status: statusActive,
	}}
	req := sampleFreeze()
	got, err := applyFreeze(context.Background(), fake, req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicated || got.FreezeNo != req.FreezeNo || got.Status != statusFrozen {
		t.Fatalf("result = %+v", got)
	}
	if fake.inserts != 1 || fake.addCalls != 1 {
		t.Fatalf("inserts=%d adds=%d", fake.inserts, fake.addCalls)
	}
	if fake.inserted.Status != statusFrozen || fake.inserted.Amount != req.Amount || fake.inserted.AccountID != 4 {
		t.Fatalf("inserted = %+v", fake.inserted)
	}
	if fake.addedID != 4 || fake.addedAmount != req.Amount {
		t.Fatalf("added id=%d amount=%d", fake.addedID, fake.addedAmount)
	}
	if fake.account.Balance != 1000 {
		t.Fatal("freeze changed the in-memory balance")
	}
}

// CF-14 出口：1062 后同一事务读到 FROZEN 或 CONFIRMED，金额和账户一致，不再加冻结。
func TestApplyFreezeDuplicateMatch(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'REFUND-B1' for key 'uk_biz'"}
	for _, status := range []string{statusFrozen, statusConfirmed} {
		fake := &fakeFreezeTx{
			accountFound: true,
			account:      freezeAccount{ID: 4, Balance: 100, AllowOverdraft: overdraftNo, Status: statusActive},
			insertErr:    dup,
			freezeFound:  true,
			existing:     freezeRow{FreezeNo: "FZ0000000000000000001", AccountID: 4, Amount: 20, Status: status},
		}
		got, err := applyFreeze(context.Background(), fake, sampleFreeze())
		if err != nil {
			t.Fatal(err)
		}
		if !got.Duplicated || got.FreezeNo != "FZ0000000000000000001" || got.Status != status {
			t.Fatalf("status %s result = %+v", status, got)
		}
		if fake.addCalls != 0 {
			t.Fatalf("status %s increased frozen", status)
		}
	}
}

// CF-14 出口：已有 RELEASED，返回 40004。金额不同也不是 10006。
func TestApplyFreezeReleased(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'REFUND-B1' for key 'uk_biz'"}
	fake := &fakeFreezeTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, Balance: 100, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    dup,
		freezeFound:  true,
		existing:     freezeRow{FreezeNo: "FZ0000000000000000002", AccountID: 9, Amount: 99, Status: statusReleased},
	}
	_, err := applyFreeze(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	if errors.Is(err, errno.ErrIdempotentConflict) {
		t.Fatal("released freeze became 10006")
	}
	if fake.addCalls != 0 {
		t.Fatal("released freeze increased frozen")
	}
}

// CF-14 出口：FROZEN / CONFIRMED 的金额或账户不一致，返回 10006，不加冻结。
func TestApplyFreezeConflict(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'REFUND-B1' for key 'uk_biz'"}
	cases := []freezeRow{
		{FreezeNo: "FZ0000000000000000003", AccountID: 4, Amount: 21, Status: statusFrozen},
		{FreezeNo: "FZ0000000000000000004", AccountID: 8, Amount: 20, Status: statusConfirmed},
	}
	for _, row := range cases {
		fake := &fakeFreezeTx{
			accountFound: true,
			account:      freezeAccount{ID: 4, Balance: 100, AllowOverdraft: overdraftNo, Status: statusActive},
			insertErr:    dup,
			freezeFound:  true,
			existing:     row,
		}
		_, err := applyFreeze(context.Background(), fake, sampleFreeze())
		assertErrno(t, err, errno.ErrIdempotentConflict)
		if fake.addCalls != 0 {
			t.Fatalf("conflict row %+v increased frozen", row)
		}
	}
}

// CF-14 出口：1062 但按 (biz_type, biz_no) 读不到行，返回 10003。普通文本里的 1062 不算重复。
func TestApplyFreezeDuplicateMiss(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'FZ' for key 'uk_freeze_no'"}
	fake := &fakeFreezeTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    dup,
	}
	_, err := applyFreeze(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, dup) {
		t.Fatal("duplicate cause lost")
	}
	if fake.addCalls != 0 {
		t.Fatal("missing duplicate increased frozen")
	}

	text := errors.New("Error 1062: Duplicate entry")
	textFake := &fakeFreezeTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    text,
	}
	_, err = applyFreeze(context.Background(), textFake, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if textFake.addCalls != 0 || textFake.inserts != 1 {
		t.Fatal("text 1062 took the duplicate path")
	}

	wrapped := errors.Join(errors.New("wrap"), dup)
	wrapFake := &fakeFreezeTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    wrapped,
		freezeFound:  true,
		existing:     freezeRow{FreezeNo: "FZ0000000000000000005", AccountID: 4, Amount: 20, Status: statusFrozen},
	}
	got, err := applyFreeze(context.Background(), wrapFake, sampleFreeze())
	if err != nil || !got.Duplicated {
		t.Fatalf("wrapped 1062 result=%+v err=%v", got, err)
	}
}

// CF-14 出口：更新 0 行后重读。非 ACTIVE 优先于余额不足；仍够冻结则 10003，不重试。
func TestApplyFreezeZeroUpdate(t *testing.T) {
	base := func() *fakeFreezeTx {
		return &fakeFreezeTx{
			accountFound: true,
			account:      freezeAccount{ID: 4, Balance: 100, AllowOverdraft: overdraftNo, Status: statusActive},
			forceZero:    true,
			useReload:    true,
		}
	}
	inactive := base()
	inactive.reloaded = freezeAccount{ID: 4, Balance: 1, FrozenAmount: 0, Status: "CLOSED"}
	inactive.reloadFound = true
	_, err := applyFreeze(context.Background(), inactive, sampleFreeze())
	assertErrno(t, err, errno.ErrAccountInactive)
	if errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatal("inactive account became 40002")
	}
	if inactive.addCalls != 1 {
		t.Fatalf("adds=%d, want one attempt and no spin", inactive.addCalls)
	}

	low := base()
	low.reloaded = freezeAccount{ID: 4, Balance: 10, FrozenAmount: 0, Status: statusActive}
	low.reloadFound = true
	_, err = applyFreeze(context.Background(), low, sampleFreeze())
	assertErrno(t, err, errno.ErrInsufficientBalance)

	shortFrozen := base()
	shortFrozen.reloaded = freezeAccount{ID: 4, Balance: 100, FrozenAmount: 90, Status: statusActive}
	shortFrozen.reloadFound = true
	_, err = applyFreeze(context.Background(), shortFrozen, sampleFreeze())
	assertErrno(t, err, errno.ErrInsufficientBalance)

	same := base()
	same.reloaded = freezeAccount{ID: 4, Balance: 100, FrozenAmount: 0, Status: statusActive}
	same.reloadFound = true
	_, err = applyFreeze(context.Background(), same, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) || errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal("still sufficient became a business rejection")
	}
	if same.addCalls != 1 {
		t.Fatal("still sufficient retried the update")
	}

	gone := base()
	_, err = applyFreeze(context.Background(), gone, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)

	overflow := base()
	overflow.reloaded = freezeAccount{
		ID: 4, Balance: money.Amount(math.MaxInt64), FrozenAmount: money.Amount(math.MinInt64), Status: statusActive,
	}
	overflow.reloadFound = true
	_, err = applyFreeze(context.Background(), overflow, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatal("overflow cause lost")
	}
}

func TestApplyFreezeHidesDriverError(t *testing.T) {
	secret := errors.New("SELECT password=secret")
	_, err := applyFreeze(context.Background(), &fakeFreezeTx{selectErr: secret}, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, secret) || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("select error leaked: %v", err)
	}

	_, err = applyFreeze(context.Background(), &fakeFreezeTx{selectErr: context.Canceled}, sampleFreeze())
	if !errors.Is(err, context.Canceled) || errors.Is(err, errno.ErrInternal) {
		t.Fatalf("error = %v", err)
	}

	check := &mysql.MySQLError{Number: 3819, Message: "Check constraint 'ck_account_available' is violated"}
	fake := &fakeFreezeTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, Balance: 100, AllowOverdraft: overdraftNo, Status: statusActive},
		addErr:       check,
	}
	_, err = applyFreeze(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatal("check violation became 40002")
	}
	if !errors.Is(err, check) {
		t.Fatal("check cause lost")
	}

	readSecret := errors.New("SELECT account_freeze password=secret")
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	_, err = applyFreeze(context.Background(), &fakeFreezeTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    dup,
		readErr:      readSecret,
	}, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, readSecret) || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("read error leaked: %v", err)
	}
}

func TestFreezeUpdateSQLDoesNotTouchBalance(t *testing.T) {
	if !strings.Contains(addFrozenSQL, "frozen_amount = frozen_amount + ?") || !strings.Contains(addFrozenSQL, "version = version + 1") {
		t.Fatalf("update sql = %s", addFrozenSQL)
	}
	if strings.Contains(addFrozenSQL, "balance =") || strings.Contains(addFrozenSQL, "balance=") {
		t.Fatal("freeze assigns balance")
	}
	if strings.Contains(insertFreezeSQL, "balance") {
		t.Fatal("freeze insert mentions balance")
	}
}

func TestFreezeSQLHelpers(t *testing.T) {
	var db sqlFreezeTx
	ctx := context.Background()
	if _, _, err := db.selectMerchant(ctx, "M100002"); !errors.Is(err, errNilTx) {
		t.Fatalf("selectMerchant = %v", err)
	}
	if err := db.insertFreeze(ctx, freezeRow{}); !errors.Is(err, errNilTx) {
		t.Fatalf("insertFreeze = %v", err)
	}
	if _, _, err := db.selectFreeze(ctx, bizRefund, "B1"); !errors.Is(err, errNilTx) {
		t.Fatalf("selectFreeze = %v", err)
	}
	if _, err := db.addFrozen(ctx, 1, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("addFrozen = %v", err)
	}
	if _, _, err := db.reloadAccount(ctx, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("reloadAccount = %v", err)
	}

	got, err := scanFreezeAccount(fakeScan{vals: []any{uint64(7), int64(100), int64(3), 0, statusActive}})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 7 || got.Balance != 100 || got.FrozenAmount != 3 || got.AllowOverdraft != 0 || got.Status != statusActive {
		t.Fatalf("account = %+v", got)
	}
	if _, err := scanFreezeAccount(fakeScan{err: errors.New("SELECT boom")}); err == nil {
		t.Fatal("scan error lost")
	}
	row, err := scanFreezeRow(fakeScan{vals: []any{"FZ0000000000000000001", uint64(7), int64(20), statusFrozen}})
	if err != nil {
		t.Fatal(err)
	}
	if row.FreezeNo != "FZ0000000000000000001" || row.AccountID != 7 || row.Amount != 20 || row.Status != statusFrozen {
		t.Fatalf("freeze = %+v", row)
	}
}

func sampleFreeze() freezeRequest {
	return freezeRequest{
		FreezeNo:   "FZ0000000000000000099",
		BizType:    bizRefund,
		BizNo:      "B1",
		MerchantID: "M100002",
		Amount:     20,
	}
}

type fakeFreezeTx struct {
	account      freezeAccount
	accountFound bool
	selectErr    error

	insertErr error
	inserts   int
	inserted  freezeRow

	existing    freezeRow
	freezeFound bool
	readErr     error

	addCalls    int
	addedID     uint64
	addedAmount money.Amount
	addErr      error
	forceZero   bool

	reloaded    freezeAccount
	reloadFound bool
	reloadErr   error
	useReload   bool
}

func (f *fakeFreezeTx) selectMerchant(context.Context, string) (freezeAccount, bool, error) {
	if f.selectErr != nil {
		return freezeAccount{}, false, f.selectErr
	}
	return f.account, f.accountFound, nil
}

func (f *fakeFreezeTx) insertFreeze(_ context.Context, row freezeRow) error {
	f.inserts++
	f.inserted = row
	return f.insertErr
}

func (f *fakeFreezeTx) selectFreeze(context.Context, string, string) (freezeRow, bool, error) {
	if f.readErr != nil {
		return freezeRow{}, false, f.readErr
	}
	return f.existing, f.freezeFound, nil
}

func (f *fakeFreezeTx) addFrozen(_ context.Context, accountID uint64, amount money.Amount) (bool, error) {
	f.addCalls++
	f.addedID = accountID
	f.addedAmount = amount
	if f.addErr != nil {
		return false, f.addErr
	}
	if f.forceZero {
		return false, nil
	}
	return true, nil
}

func (f *fakeFreezeTx) reloadAccount(context.Context, uint64) (freezeAccount, bool, error) {
	if f.reloadErr != nil {
		return freezeAccount{}, false, f.reloadErr
	}
	if f.useReload {
		return f.reloaded, f.reloadFound, nil
	}
	return f.account, f.accountFound, nil
}
