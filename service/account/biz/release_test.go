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

// ACC-05：参数不合法返回 10001，不发号、不写行。上下文已取消也先返回 10001。
func TestReleaseRejectsBeforeID(t *testing.T) {
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
			_, err := svc.ReleaseFreeze(ctx, tc.bizType, tc.bizNo, tc.merchantID, tc.amount)
			assertErrno(t, err, errno.ErrInvalidParam)
		})
	}
	if clk.i != 0 {
		t.Fatalf("NewNo calls = %d", clk.i)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.ReleaseFreeze(canceled, bizPay, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInvalidParam)
	_, err = (*Service)(nil).ReleaseFreeze(canceled, "", "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInvalidParam)
	// 边界长度和最大正数只校验。nil db 说明已经过了校验，仍然不发号。
	_, err = svc.ReleaseFreeze(ctx, bizRefund, biz64, "M100002", money.Amount(math.MaxInt64))
	assertErrno(t, err, errno.ErrInternal)
	_, err = svc.ReleaseFreeze(ctx, bizTransfer, "B1", "M100001", 1)
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil db issued an id")
	}
}

func TestReleaseNilAndCanceled(t *testing.T) {
	ctx := context.Background()
	_, err := (*Service)(nil).ReleaseFreeze(ctx, bizRefund, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInternal)

	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	// SA1012 不允许把 nil context 当参数。这里正是要证明 nil ctx 返回 10003。
	_, err = svc.ReleaseFreeze(nil, bizRefund, "B1", "M100002", 1) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)

	_, err = New(nil, newIDs(t, clk), testSubCount).ReleaseFreeze(ctx, bizRefund, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInternal)
	_, err = New(dal.New(nil, nil), nil, testSubCount).ReleaseFreeze(ctx, bizRefund, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil dependency issued an id")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.ReleaseFreeze(canceled, bizRefund, "B1", "M100002", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
	if errors.Is(err, errno.ErrInternal) {
		t.Fatal("canceled context became 10003")
	}
	// openTestTime 早于本次运行日，截止时间已经过去，Err 是 DeadlineExceeded。不要再 cancel，否则会变成 Canceled。
	deadline, stop := context.WithDeadline(context.Background(), openTestTime)
	defer stop()
	_, err = svc.ReleaseFreeze(deadline, bizTransfer, "B1", "M100001", 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline", err)
	}
	if clk.i != 0 {
		t.Fatal("canceled request issued an id")
	}
}

func TestReleaseIDErrors(t *testing.T) {
	// 分片池非 nil，发号才会执行。端口 1 没有服务，Begin 失败，不会真的写库。
	db, err := sqlx.Open("mysql", "release:unused@tcp(127.0.0.1:1)/none?timeout=100ms")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	clk := &stepClock{times: []time.Time{openTestTime, openTestTime.Add(-10 * time.Millisecond)}}
	svc := New(dal.New(db, db), newIDs(t, clk), testSubCount)
	_, err = svc.ReleaseFreeze(context.Background(), bizRefund, "B1", "M100002", 1)
	assertErrno(t, err, errno.ErrInternal)
	if strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), "unused") {
		t.Fatalf("Error() leaked: %q", err.Error())
	}
	_, err = svc.ReleaseFreeze(context.Background(), bizRefund, "B2", "M100002", 1)
	assertErrno(t, err, errno.ErrUnavailable)
	if !errors.Is(err, idgen.ErrClockRollback) {
		t.Fatal("clock rollback cause lost")
	}
	if clk.i != 2 {
		t.Fatalf("NewNo calls = %d", clk.i)
	}

	earlyClk := &stepClock{times: []time.Time{time.Date(2025, 12, 31, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))}}
	early := New(dal.New(db, db), newIDs(t, earlyClk), testSubCount)
	_, err = early.ReleaseFreeze(context.Background(), bizTransfer, "B1", "M100001", 1)
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, idgen.ErrTimeRange) {
		t.Fatal("time range cause lost")
	}
	if strings.Contains(err.Error(), "idgen") || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("Error() leaked: %q", err.Error())
	}
}

// CF-15 出口：没有冻结单时插入 RELEASED 墓碑，不改账户。
func TestApplyReleaseTombstone(t *testing.T) {
	fake := &fakeReleaseTx{accountFound: true, account: freezeAccount{
		ID: 4, Balance: 1000, FrozenAmount: 10, AllowOverdraft: overdraftNo, Status: statusActive,
	}}
	req := sampleFreeze()
	got, err := applyRelease(context.Background(), fake, req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicated || got.FreezeNo != req.FreezeNo || got.Status != statusReleased {
		t.Fatalf("result = %+v", got)
	}
	if fake.inserts != 1 || fake.releaseCalls != 0 || fake.subCalls != 0 {
		t.Fatalf("inserts=%d releases=%d subs=%d", fake.inserts, fake.releaseCalls, fake.subCalls)
	}
	if fake.inserted.Status != statusReleased || fake.inserted.Amount != req.Amount || fake.inserted.AccountID != 4 || fake.inserted.FreezeNo != req.FreezeNo {
		t.Fatalf("inserted = %+v", fake.inserted)
	}
	if fake.account.Balance != 1000 || fake.account.FrozenAmount != 10 {
		t.Fatal("tombstone changed the in-memory account")
	}
}

// CF-15 出口：空回滚时透支账户返回 10001，不插入。真实库插不出这种商户账户。
func TestApplyReleaseTombstoneOverdraft(t *testing.T) {
	fake := &fakeReleaseTx{accountFound: true, account: freezeAccount{
		ID: 7, Balance: 100, AllowOverdraft: overdraftYes, Status: statusActive,
	}}
	_, err := applyRelease(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrInvalidParam)
	if fake.inserts != 0 || fake.subCalls != 0 {
		t.Fatalf("overdraft inserts=%d subs=%d", fake.inserts, fake.subCalls)
	}

	// 已有 FROZEN 且账户、金额一致时，不因为透支拒绝。空回滚才看这一列。
	frozen := &fakeReleaseTx{
		accountFound: true,
		account:      freezeAccount{ID: 7, AllowOverdraft: overdraftYes, Status: statusActive},
		lockFound:    true,
		locked:       freezeRow{FreezeNo: "FZ0000000000000000011", AccountID: 7, Amount: 20, Status: statusFrozen},
	}
	got, err := applyRelease(context.Background(), frozen, sampleFreeze())
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicated || got.FreezeNo != "FZ0000000000000000011" || got.Status != statusReleased {
		t.Fatalf("overdraft frozen result = %+v", got)
	}
	if frozen.inserts != 0 || frozen.subCalls != 1 {
		t.Fatalf("overdraft frozen inserts=%d subs=%d", frozen.inserts, frozen.subCalls)
	}
}

// CF-15 出口：空回滚找不到结算账户返回 40001，不插入。
func TestApplyReleaseTombstoneMissingAccount(t *testing.T) {
	fake := &fakeReleaseTx{}
	_, err := applyRelease(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrAccountNotFound)
	if fake.inserts != 0 || fake.subCalls != 0 {
		t.Fatal("missing account wrote a freeze")
	}
}

// CF-15 出口：1062 后再读。RELEASED 幂等；FROZEN 接着解冻；读不到包住 1062。普通文本里的 1062 不算重复。
func TestApplyReleaseDuplicate(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'REFUND-B1' for key 'uk_biz'"}
	released := &fakeReleaseTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, Balance: 100, FrozenAmount: 10, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    dup,
		useReread:    true,
		rereadFound:  true,
		reread:       freezeRow{FreezeNo: "FZ0000000000000000002", AccountID: 9, Amount: 99, Status: statusReleased},
	}
	got, err := applyRelease(context.Background(), released, sampleFreeze())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Duplicated || got.FreezeNo != "FZ0000000000000000002" || got.Status != statusReleased {
		t.Fatalf("released duplicate = %+v", got)
	}
	if released.subCalls != 0 || released.releaseCalls != 0 {
		t.Fatal("duplicate released changed the account")
	}

	frozen := &fakeReleaseTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, Balance: 100, FrozenAmount: 20, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    errors.Join(errors.New("wrap"), dup),
		useReread:    true,
		rereadFound:  true,
		reread:       freezeRow{FreezeNo: "FZ0000000000000000008", AccountID: 4, Amount: 20, Status: statusFrozen},
	}
	got, err = applyRelease(context.Background(), frozen, sampleFreeze())
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicated || got.FreezeNo != "FZ0000000000000000008" || got.Status != statusReleased {
		t.Fatalf("frozen duplicate = %+v", got)
	}
	if frozen.subCalls != 1 || frozen.inserts != 1 {
		t.Fatalf("frozen duplicate inserts=%d subs=%d", frozen.inserts, frozen.subCalls)
	}

	miss := &fakeReleaseTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    dup,
		useReread:    true,
	}
	_, err = applyRelease(context.Background(), miss, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, dup) {
		t.Fatal("duplicate cause lost")
	}
	if miss.subCalls != 0 {
		t.Fatal("missing duplicate changed the account")
	}

	text := &fakeReleaseTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    errors.New("Error 1062: Duplicate entry"),
	}
	_, err = applyRelease(context.Background(), text, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if text.locks != 1 || text.inserts != 1 || text.subCalls != 0 {
		t.Fatal("text 1062 took the duplicate path")
	}
}

// CF-15 出口：已是 RELEASED 时幂等成功，不再比较金额和账户，也不查结算账户。
func TestApplyReleaseAlreadyReleased(t *testing.T) {
	fake := &fakeReleaseTx{
		lockFound: true,
		locked:    freezeRow{FreezeNo: "FZ0000000000000000002", AccountID: 9, Amount: 99, Status: statusReleased},
	}
	got, err := applyRelease(context.Background(), fake, sampleFreeze())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Duplicated || got.FreezeNo != "FZ0000000000000000002" || got.Status != statusReleased {
		t.Fatalf("result = %+v", got)
	}
	if fake.selects != 0 || fake.inserts != 0 || fake.releaseCalls != 0 || fake.subCalls != 0 {
		t.Fatalf("selects=%d inserts=%d releases=%d subs=%d", fake.selects, fake.inserts, fake.releaseCalls, fake.subCalls)
	}
}

// CF-15 出口：已是 CONFIRMED 一律 40004，不再比较金额和账户。
func TestApplyReleaseConfirmed(t *testing.T) {
	fake := &fakeReleaseTx{
		lockFound: true,
		locked:    freezeRow{FreezeNo: "FZ0000000000000000006", AccountID: 8, Amount: 1, Status: statusConfirmed},
	}
	_, err := applyRelease(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	if errors.Is(err, errno.ErrIdempotentConflict) {
		t.Fatal("confirmed freeze became 10006")
	}
	if fake.selects != 0 || fake.releaseCalls != 0 || fake.subCalls != 0 {
		t.Fatal("confirmed freeze changed the account")
	}
}

// CF-15 出口：FROZEN 金额或账户不一致返回 10006。没有结算账户也对不上，不是 40001。
func TestApplyReleaseFrozenConflict(t *testing.T) {
	cases := []struct {
		name    string
		found   bool
		account freezeAccount
		row     freezeRow
	}{
		{
			name:    "amount",
			found:   true,
			account: freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
			row:     freezeRow{FreezeNo: "FZ0000000000000000003", AccountID: 4, Amount: 21, Status: statusFrozen},
		},
		{
			name:    "account",
			found:   true,
			account: freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
			row:     freezeRow{FreezeNo: "FZ0000000000000000004", AccountID: 8, Amount: 20, Status: statusFrozen},
		},
		{
			name: "missing settle",
			row:  freezeRow{FreezeNo: "FZ0000000000000000007", AccountID: 8, Amount: 20, Status: statusFrozen},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeReleaseTx{
				accountFound: tc.found,
				account:      tc.account,
				lockFound:    true,
				locked:       tc.row,
			}
			_, err := applyRelease(context.Background(), fake, sampleFreeze())
			assertErrno(t, err, errno.ErrIdempotentConflict)
			if errors.Is(err, errno.ErrAccountNotFound) {
				t.Fatal("frozen row without a settle account became 40001")
			}
			if fake.releaseCalls != 0 || fake.subCalls != 0 || fake.inserts != 0 {
				t.Fatal("conflict changed a row")
			}
		})
	}
}

// CF-15 出口：FROZEN 金额和账户一致时解冻。停用账户也解开。返回已有单号，不用新生成的号。
func TestApplyReleaseFrozenSuccess(t *testing.T) {
	for _, status := range []string{statusActive, "CLOSED", statusFrozen} {
		fake := &fakeReleaseTx{
			accountFound: true,
			account: freezeAccount{
				ID: 4, Balance: 1000, FrozenAmount: 20, AllowOverdraft: overdraftNo, Status: status,
			},
			lockFound: true,
			locked:    freezeRow{FreezeNo: "FZ0000000000000000001", AccountID: 4, Amount: 20, Status: statusFrozen},
		}
		req := sampleFreeze()
		req.FreezeNo = "FZNEW0000000000000099"
		got, err := applyRelease(context.Background(), fake, req)
		if err != nil {
			t.Fatal(err)
		}
		if got.Duplicated || got.FreezeNo != "FZ0000000000000000001" || got.Status != statusReleased {
			t.Fatalf("status %s result = %+v", status, got)
		}
		if fake.releaseCalls != 1 || fake.subCalls != 1 || fake.subID != 4 || fake.subAmount != 20 || fake.inserts != 0 {
			t.Fatalf("status %s releases=%d subs=%d id=%d amount=%d inserts=%d", status, fake.releaseCalls, fake.subCalls, fake.subID, fake.subAmount, fake.inserts)
		}
		if errors.Is(err, errno.ErrAccountInactive) {
			t.Fatal("inactive account became 40005")
		}
	}
}

// CF-15 出口：冻结单 CAS 影响 0 行后重读。RELEASED 幂等，CONFIRMED 为 40004，仍是 FROZEN 或读不到为 10003。
func TestApplyReleaseCASMiss(t *testing.T) {
	base := func() *fakeReleaseTx {
		return &fakeReleaseTx{
			accountFound:     true,
			account:          freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
			lockFound:        true,
			locked:           freezeRow{FreezeNo: "FZ0000000000000000001", AccountID: 4, Amount: 20, Status: statusFrozen},
			forceReleaseZero: true,
			useReread:        true,
		}
	}
	released := base()
	released.rereadFound = true
	released.reread = freezeRow{FreezeNo: "FZ0000000000000000001", AccountID: 4, Amount: 20, Status: statusReleased}
	got, err := applyRelease(context.Background(), released, sampleFreeze())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Duplicated || got.FreezeNo != "FZ0000000000000000001" || got.Status != statusReleased {
		t.Fatalf("released = %+v", got)
	}
	if released.subCalls != 0 || released.releaseCalls != 1 {
		t.Fatalf("released subs=%d releases=%d", released.subCalls, released.releaseCalls)
	}

	confirmed := base()
	confirmed.rereadFound = true
	confirmed.reread = freezeRow{FreezeNo: "FZ0000000000000000001", Status: statusConfirmed}
	_, err = applyRelease(context.Background(), confirmed, sampleFreeze())
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	if confirmed.subCalls != 0 {
		t.Fatal("confirmed miss decreased frozen")
	}

	still := base()
	still.rereadFound = true
	still.reread = freezeRow{FreezeNo: "FZ0000000000000000001", Status: statusFrozen}
	_, err = applyRelease(context.Background(), still, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if still.subCalls != 0 || still.releaseCalls != 1 {
		t.Fatal("still frozen retried the update")
	}

	gone := base()
	_, err = applyRelease(context.Background(), gone, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)

	other := base()
	other.rereadFound = true
	other.reread = freezeRow{Status: "OTHER"}
	_, err = applyRelease(context.Background(), other, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
}

// CF-15 出口：账户更新影响 0 行后重读。行没了或冻结仍不够都是 10003，不改成 40002，也不看 ACTIVE。
func TestApplyReleaseAccountMiss(t *testing.T) {
	base := func() *fakeReleaseTx {
		return &fakeReleaseTx{
			accountFound: true,
			account:      freezeAccount{ID: 4, Balance: 100, FrozenAmount: 20, AllowOverdraft: overdraftNo, Status: statusActive},
			lockFound:    true,
			locked:       freezeRow{FreezeNo: "FZ0000000000000000001", AccountID: 4, Amount: 20, Status: statusFrozen},
			forceSubZero: true,
			useReload:    true,
		}
	}
	short := base()
	short.reloaded = freezeAccount{ID: 4, Balance: 100, FrozenAmount: 19, Status: "CLOSED"}
	short.reloadFound = true
	_, err := applyRelease(context.Background(), short, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) || errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal("short frozen became a business rejection")
	}
	if short.subCalls != 1 || short.reloads != 1 {
		t.Fatalf("subs=%d reloads=%d, want one attempt and no spin", short.subCalls, short.reloads)
	}

	enough := base()
	enough.reloaded = freezeAccount{ID: 4, Balance: 100, FrozenAmount: 20, Status: statusActive}
	enough.reloadFound = true
	_, err = applyRelease(context.Background(), enough, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) {
		t.Fatal("still enough became 40002")
	}
	if enough.subCalls != 1 {
		t.Fatal("still enough retried the update")
	}

	gone := base()
	_, err = applyRelease(context.Background(), gone, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if gone.reloads != 1 {
		t.Fatal("missing account did not reload")
	}
}

// CF-15 出口：冻结单状态不是这三种时返回 10003。
func TestApplyReleaseOtherStatus(t *testing.T) {
	fake := &fakeReleaseTx{
		lockFound: true,
		locked:    freezeRow{FreezeNo: "FZ0000000000000000009", Status: "OTHER"},
	}
	_, err := applyRelease(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if fake.selects != 0 || fake.releaseCalls != 0 || fake.subCalls != 0 {
		t.Fatal("unknown status continued")
	}
}

func TestApplyReleaseHidesDriverError(t *testing.T) {
	secret := errors.New("SELECT password=secret")
	_, err := applyRelease(context.Background(), &fakeReleaseTx{lockErr: secret}, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, secret) || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("lock error leaked: %v", err)
	}

	_, err = applyRelease(context.Background(), &fakeReleaseTx{lockErr: context.Canceled}, sampleFreeze())
	if !errors.Is(err, context.Canceled) || errors.Is(err, errno.ErrInternal) {
		t.Fatalf("error = %v", err)
	}

	_, err = applyRelease(context.Background(), &fakeReleaseTx{selectErr: context.DeadlineExceeded}, sampleFreeze())
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errno.ErrInternal) {
		t.Fatalf("select error = %v", err)
	}

	check := &mysql.MySQLError{Number: 3819, Message: "Check constraint 'ck_account_frozen' is violated"}
	fake := &fakeReleaseTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: "CLOSED"},
		lockFound:    true,
		locked:       freezeRow{FreezeNo: "FZ0000000000000000001", AccountID: 4, Amount: 20, Status: statusFrozen},
		subErr:       check,
	}
	_, err = applyRelease(context.Background(), fake, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInsufficientBalance) || errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal("check violation became a business rejection")
	}
	if !errors.Is(err, check) {
		t.Fatal("check cause lost")
	}

	readSecret := errors.New("SELECT account_freeze password=secret")
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	_, err = applyRelease(context.Background(), &fakeReleaseTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		insertErr:    dup,
		useReread:    true,
		rereadErr:    readSecret,
	}, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, readSecret) || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("reread error leaked: %v", err)
	}

	reloadSecret := errors.New("SELECT account password=secret")
	_, err = applyRelease(context.Background(), &fakeReleaseTx{
		accountFound: true,
		account:      freezeAccount{ID: 4, AllowOverdraft: overdraftNo, Status: statusActive},
		lockFound:    true,
		locked:       freezeRow{FreezeNo: "FZ0000000000000000001", AccountID: 4, Amount: 20, Status: statusFrozen},
		forceSubZero: true,
		useReload:    true,
		reloadErr:    reloadSecret,
	}, sampleFreeze())
	assertErrno(t, err, errno.ErrInternal)
	if !errors.Is(err, reloadSecret) || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("reload error leaked: %v", err)
	}
}

func TestReleaseUpdateSQLDoesNotTouchBalance(t *testing.T) {
	if !strings.Contains(subFrozenSQL, "frozen_amount = frozen_amount - ?") || !strings.Contains(subFrozenSQL, "version = version + 1") || !strings.Contains(subFrozenSQL, "frozen_amount >= ?") {
		t.Fatalf("sub sql = %s", subFrozenSQL)
	}
	if strings.Contains(subFrozenSQL, "balance") || strings.Contains(subFrozenSQL, "ACTIVE") {
		t.Fatal("release update touches balance or requires ACTIVE")
	}
	if !strings.Contains(releaseFrozenSQL, "status = 'RELEASED'") || !strings.Contains(releaseFrozenSQL, "version = version + 1") || !strings.Contains(releaseFrozenSQL, "status = 'FROZEN'") {
		t.Fatalf("cas sql = %s", releaseFrozenSQL)
	}
	if strings.Contains(releaseFrozenSQL, "balance") || strings.Contains(releaseFrozenSQL, "txn_no") {
		t.Fatal("release cas touches balance or txn_no")
	}
	if !strings.Contains(lockFreezeSQL, "FOR UPDATE") {
		t.Fatal("release read does not lock the freeze row")
	}
}

func TestReleaseSQLHelpers(t *testing.T) {
	var db sqlFreezeTx
	ctx := context.Background()
	if _, _, err := db.lockFreeze(ctx, bizRefund, "B1"); !errors.Is(err, errNilTx) {
		t.Fatalf("lockFreeze = %v", err)
	}
	if _, err := db.markReleased(ctx, bizRefund, "B1", 1, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("markReleased = %v", err)
	}
	if _, err := db.subFrozen(ctx, 1, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("subFrozen = %v", err)
	}
	ok, err := affectedOne(fakeResult{n: 0})
	if err != nil || ok {
		t.Fatalf("zero = %v %v", ok, err)
	}
	ok, err = affectedOne(fakeResult{n: 1})
	if err != nil || !ok {
		t.Fatalf("one = %v %v", ok, err)
	}
	if _, err = affectedOne(fakeResult{n: 2}); err == nil {
		t.Fatal("two rows accepted")
	}
	if _, err = affectedOne(fakeResult{err: errors.New("rows")}); err == nil {
		t.Fatal("rows error lost")
	}
}

type fakeResult struct {
	n   int64
	err error
}

func (f fakeResult) LastInsertId() (int64, error) { return 0, nil }

func (f fakeResult) RowsAffected() (int64, error) { return f.n, f.err }

type fakeReleaseTx struct {
	account      freezeAccount
	accountFound bool
	selectErr    error
	selects      int

	locked    freezeRow
	lockFound bool
	lockErr   error
	locks     int

	useReread   bool
	reread      freezeRow
	rereadFound bool
	rereadErr   error

	insertErr error
	inserts   int
	inserted  freezeRow

	releaseCalls     int
	releaseErr       error
	forceReleaseZero bool

	subCalls     int
	subID        uint64
	subAmount    money.Amount
	subErr       error
	forceSubZero bool

	reloaded    freezeAccount
	reloadFound bool
	reloadErr   error
	useReload   bool
	reloads     int
}

func (f *fakeReleaseTx) lockFreeze(context.Context, string, string) (freezeRow, bool, error) {
	f.locks++
	if f.locks == 1 && f.lockErr != nil {
		return freezeRow{}, false, f.lockErr
	}
	if f.locks > 1 && f.useReread {
		if f.rereadErr != nil {
			return freezeRow{}, false, f.rereadErr
		}
		return f.reread, f.rereadFound, nil
	}
	return f.locked, f.lockFound, nil
}

func (f *fakeReleaseTx) selectMerchant(context.Context, string) (freezeAccount, bool, error) {
	f.selects++
	if f.selectErr != nil {
		return freezeAccount{}, false, f.selectErr
	}
	return f.account, f.accountFound, nil
}

func (f *fakeReleaseTx) insertFreeze(_ context.Context, row freezeRow) error {
	f.inserts++
	f.inserted = row
	return f.insertErr
}

func (f *fakeReleaseTx) markReleased(context.Context, string, string, money.Amount, uint64) (bool, error) {
	f.releaseCalls++
	if f.releaseErr != nil {
		return false, f.releaseErr
	}
	if f.forceReleaseZero {
		return false, nil
	}
	return true, nil
}

func (f *fakeReleaseTx) subFrozen(_ context.Context, accountID uint64, amount money.Amount) (bool, error) {
	f.subCalls++
	f.subID = accountID
	f.subAmount = amount
	if f.subErr != nil {
		return false, f.subErr
	}
	if f.forceSubZero {
		return false, nil
	}
	return true, nil
}

func (f *fakeReleaseTx) reloadAccount(context.Context, uint64) (freezeAccount, bool, error) {
	f.reloads++
	if f.reloadErr != nil {
		return freezeAccount{}, false, f.reloadErr
	}
	if f.useReload {
		return f.reloaded, f.reloadFound, nil
	}
	return f.account, f.accountFound, nil
}
