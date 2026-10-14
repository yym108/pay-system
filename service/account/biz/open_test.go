package biz

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/dal"
)

// openTestTime 晚于 idgen 纪元 2026-01-01T00:00:00+08:00。测试不调用 time.Now。
var openTestTime = time.Date(2026, 10, 15, 9, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))

const testSubCount = 8

type stubClock struct {
	now time.Time
}

func (c stubClock) Now() time.Time { return c.now }

// stepClock 按调用顺序返回时间。idgen 的 Clock 不是 pkg/clock。
type stepClock struct {
	mu    sync.Mutex
	times []time.Time
	i     int
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.times) == 0 {
		return time.Time{}
	}
	if c.i >= len(c.times) {
		return c.times[len(c.times)-1]
	}
	t := c.times[c.i]
	c.i++
	return t
}

func newIDs(t *testing.T, clk idgen.Clock) *idgen.Generator {
	t.Helper()
	ids, err := idgen.New(3, clk)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func assertInvalidParam(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("error = %v, want invalid parameter", err)
	}
	if err.Error() != errno.ErrInvalidParam.Error() {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func assertInternal(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("error = %v, want internal error", err)
	}
	msg := err.Error()
	if msg != errno.ErrInternal.Error() {
		t.Fatalf("Error() = %q", msg)
	}
	if strings.Contains(msg, "Duplicate") || strings.Contains(msg, "SELECT") || strings.Contains(msg, "password") || strings.Contains(msg, "@tcp(") {
		t.Fatalf("Error() leaked internals: %q", msg)
	}
}

// ACC-01 验收：MERCHANT + FEE_INCOME 以及其余非法组合返回 10001，不访问数据库。
func TestOpenAccountRejectsInvalid(t *testing.T) {
	svc := New(dal.New(nil, nil), newIDs(t, stubClock{now: openTestTime}), testSubCount)
	ctx := context.Background()
	cases := []struct {
		name                            string
		ownerType, ownerID, accountType string
		shard, subNo                    int
	}{
		{name: "merchant fee", ownerType: "MERCHANT", ownerID: "M100002", accountType: "FEE_INCOME", shard: 0, subNo: 0},
		{name: "merchant channel", ownerType: "MERCHANT", ownerID: "M100002", accountType: "CHANNEL_RECEIVABLE", shard: 0, subNo: 0},
		{name: "merchant transit", ownerType: "MERCHANT", ownerID: "M100002", accountType: "TRANSIT", shard: 0, subNo: 0},
		{name: "platform settle", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "MERCHANT_SETTLE", shard: 0, subNo: 0},
		{name: "bad merchant id", ownerType: "MERCHANT", ownerID: "M10002", accountType: "MERCHANT_SETTLE", shard: 0, subNo: 0},
		{name: "merchant wrong shard", ownerType: "MERCHANT", ownerID: "M100002", accountType: "MERCHANT_SETTLE", shard: 1, subNo: 0},
		{name: "merchant sub", ownerType: "MERCHANT", ownerID: "M100002", accountType: "MERCHANT_SETTLE", shard: 0, subNo: 1},
		{name: "merchant negative sub", ownerType: "MERCHANT", ownerID: "M100002", accountType: "MERCHANT_SETTLE", shard: 0, subNo: -1},
		{name: "channel empty owner", ownerType: "PLATFORM", ownerID: "", accountType: "CHANNEL_RECEIVABLE", shard: 0, subNo: 0},
		{name: "channel long owner", ownerType: "PLATFORM", ownerID: strings.Repeat("a", 33), accountType: "CHANNEL_RECEIVABLE", shard: 0, subNo: 0},
		{name: "channel shard 2", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "CHANNEL_RECEIVABLE", shard: 2, subNo: 0},
		{name: "channel negative shard", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "CHANNEL_RECEIVABLE", shard: -1, subNo: 0},
		{name: "channel sub 8", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "CHANNEL_RECEIVABLE", shard: 0, subNo: 8},
		{name: "channel negative sub", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "CHANNEL_RECEIVABLE", shard: 0, subNo: -1},
		{name: "fee wrong owner", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "FEE_INCOME", shard: 0, subNo: 0},
		{name: "fee lowercase owner", ownerType: "PLATFORM", ownerID: "platform", accountType: "FEE_INCOME", shard: 0, subNo: 0},
		{name: "transit wrong owner", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "TRANSIT", shard: 1, subNo: 0},
		{name: "transit shard 2", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "TRANSIT", shard: 2, subNo: 0},
		{name: "unknown type", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "SUSPENSE", shard: 0, subNo: 0},
		{name: "empty", ownerType: "", ownerID: "", accountType: "", shard: 0, subNo: 0},
		{name: "lower merchant", ownerType: "merchant", ownerID: "M100002", accountType: "MERCHANT_SETTLE", shard: 0, subNo: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.OpenAccount(ctx, tc.ownerType, tc.ownerID, tc.accountType, tc.shard, tc.subNo)
			assertInvalidParam(t, err)
			if tc.ownerID != "" && strings.Contains(err.Error(), tc.ownerID) {
				t.Fatal("error echoed owner id")
			}
		})
	}
}

func TestAccountSpec(t *testing.T) {
	settle, err := specFor("MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	if settle.BalanceDirection != "CREDIT" || settle.AllowOverdraft != 0 || settle.Currency != "CNY" {
		t.Fatalf("settle spec = %+v", settle)
	}
	channel, err := specFor("PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 1, 7, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	if channel.BalanceDirection != "DEBIT" || channel.AllowOverdraft != 1 {
		t.Fatalf("channel spec = %+v", channel)
	}
	owner32 := strings.Repeat("a", 32)
	if _, err := specFor("PLATFORM", owner32, "CHANNEL_RECEIVABLE", 0, 0, testSubCount); err != nil {
		t.Fatal(err)
	}
	for _, accountType := range []string{"FEE_INCOME", "TRANSIT"} {
		got, err := specFor("PLATFORM", "PLATFORM", accountType, 0, 0, testSubCount)
		if err != nil {
			t.Fatal(err)
		}
		if got.BalanceDirection != "CREDIT" || got.AllowOverdraft != 1 {
			t.Fatalf("%s spec = %+v", accountType, got)
		}
	}
	if _, err := specFor("PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 0, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := specFor("PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 0, 1, 1); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("subCount 1 sub 1 error = %v", err)
	}
}

func TestOpenAccountValidStopsBeforeDB(t *testing.T) {
	svc := New(dal.New(nil, nil), newIDs(t, stubClock{now: openTestTime}), testSubCount)
	ctx := context.Background()
	cases := []struct {
		name                            string
		ownerType, ownerID, accountType string
		shard, subNo                    int
	}{
		{name: "merchant shard 0", ownerType: "MERCHANT", ownerID: "M100002", accountType: "MERCHANT_SETTLE", shard: 0, subNo: 0},
		{name: "merchant shard 1", ownerType: "MERCHANT", ownerID: "M100001", accountType: "MERCHANT_SETTLE", shard: 1, subNo: 0},
		{name: "channel", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "CHANNEL_RECEIVABLE", shard: 1, subNo: 7},
		{name: "fee", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "FEE_INCOME", shard: 0, subNo: 0},
		{name: "transit", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "TRANSIT", shard: 1, subNo: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.OpenAccount(ctx, tc.ownerType, tc.ownerID, tc.accountType, tc.shard, tc.subNo)
			assertInternal(t, err)
		})
	}
}

func TestOpenAccountNilDeps(t *testing.T) {
	ctx := context.Background()
	ids := newIDs(t, stubClock{now: openTestTime})
	_, err := (*Service)(nil).OpenAccount(ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	assertInternal(t, err)

	_, err = New(nil, ids, testSubCount).OpenAccount(ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	assertInternal(t, err)

	_, err = New(dal.New(nil, nil), nil, testSubCount).OpenAccount(ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	assertInternal(t, err)
}

func TestOpenAccountContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := New(dal.New(nil, nil), newIDs(t, stubClock{now: openTestTime}), testSubCount)
	_, err := svc.OpenAccount(ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestOpenAccountIDTimeRange(t *testing.T) {
	early := time.Date(2025, 12, 31, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	svc := New(dal.New(nil, nil), newIDs(t, stubClock{now: early}), testSubCount)
	_, err := svc.OpenAccount(context.Background(), "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	assertInternal(t, err)
	if !errors.Is(err, idgen.ErrTimeRange) {
		t.Fatal("time range cause lost")
	}
}

func TestOpenAccountClockRollback(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime, openTestTime.Add(-10 * time.Millisecond)}}
	ids := newIDs(t, clk)
	if _, err := ids.NewNo(context.Background(), "AC"); err != nil {
		t.Fatal(err)
	}
	svc := New(dal.New(nil, nil), ids, testSubCount)
	_, err := svc.OpenAccount(context.Background(), "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	if !errors.Is(err, errno.ErrUnavailable) {
		t.Fatalf("error = %v, want unavailable", err)
	}
	if err.Error() != errno.ErrUnavailable.Error() {
		t.Fatalf("Error() = %q", err.Error())
	}
	if !errors.Is(err, idgen.ErrClockRollback) {
		t.Fatal("clock rollback cause lost")
	}
}

type fakeAccountTx struct {
	insertErr error
	inserts   int
	row       Account
	found     bool
	selectErr error
	selects   int
}

func (f *fakeAccountTx) insertAccount(context.Context, accountSpec, string) error {
	f.inserts++
	return f.insertErr
}

func (f *fakeAccountTx) selectAccount(context.Context, accountSpec) (Account, bool, error) {
	f.selects++
	return f.row, f.found, f.selectErr
}

func merchantSpec(t *testing.T) accountSpec {
	t.Helper()
	spec, err := specFor("MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0, testSubCount)
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestApplyOpenInsertsNewAccount(t *testing.T) {
	fake := &fakeAccountTx{}
	got, err := applyOpen(context.Background(), fake, merchantSpec(t), "AC0000000000000000011")
	if err != nil {
		t.Fatal(err)
	}
	if fake.inserts != 1 || fake.selects != 0 {
		t.Fatalf("inserts=%d selects=%d", fake.inserts, fake.selects)
	}
	if got.AccountNo != "AC0000000000000000011" || got.Balance != 0 || got.FrozenAmount != 0 || got.AvailableAmount != 0 {
		t.Fatalf("account = %+v", got)
	}
	if got.Status != "ACTIVE" || got.Shard != 0 || got.Currency != "CNY" || got.BalanceDirection != "CREDIT" {
		t.Fatalf("account = %+v", got)
	}
}

// ACC-01 规则：已存在则返回已有账户，不把余额改回 0。
func TestApplyOpenKeepsBalance(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'AC0000000000000000099' for key 'uk_account_no'"}
	fake := &fakeAccountTx{
		insertErr: dup,
		found:     true,
		row: Account{
			AccountNo:        "AC0000000000000000001",
			OwnerType:        "MERCHANT",
			OwnerID:          "M100002",
			AccountType:      "MERCHANT_SETTLE",
			SubNo:            0,
			Currency:         "CNY",
			BalanceDirection: "CREDIT",
			Balance:          100,
			FrozenAmount:     40,
			Status:           "ACTIVE",
		},
	}
	got, err := applyOpen(context.Background(), fake, merchantSpec(t), "AC0000000000000000099")
	if err != nil {
		t.Fatal(err)
	}
	if got.AccountNo != "AC0000000000000000001" || got.Balance != 100 || got.FrozenAmount != 40 || got.AvailableAmount != 60 {
		t.Fatalf("account = %+v", got)
	}
	if got.Shard != 0 {
		t.Fatalf("shard = %d", got.Shard)
	}
	if fake.selects != 1 {
		t.Fatalf("selects = %d", fake.selects)
	}
}

func TestApplyOpenAccountNoConflict(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'AC1' for key 'account.uk_account_no'"}
	spec := merchantSpec(t)
	cases := []struct {
		name string
		fake *fakeAccountTx
	}{
		{name: "missing row", fake: &fakeAccountTx{insertErr: dup, found: false}},
		{name: "other owner", fake: &fakeAccountTx{insertErr: dup, found: true, row: Account{OwnerID: "M100004"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := applyOpen(context.Background(), tc.fake, spec, "AC0000000000000000099")
			assertInternal(t, err)
			if !errors.Is(err, dup) {
				t.Fatal("duplicate cause lost")
			}
		})
	}
}

func TestApplyOpenHidesDriverError(t *testing.T) {
	secret := errors.New("dial tcp 10.0.0.1:3306 password=secret")
	_, err := applyOpen(context.Background(), &fakeAccountTx{insertErr: secret}, merchantSpec(t), "AC0000000000000000011")
	assertInternal(t, err)
	if !errors.Is(err, secret) {
		t.Fatal("cause lost")
	}

	text1062 := errors.New("Error 1062: Duplicate entry")
	_, err = applyOpen(context.Background(), &fakeAccountTx{insertErr: text1062}, merchantSpec(t), "AC0000000000000000011")
	assertInternal(t, err)

	selectErr := errors.New("SELECT failed password=secret")
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	_, err = applyOpen(context.Background(), &fakeAccountTx{insertErr: dup, selectErr: selectErr}, merchantSpec(t), "AC0000000000000000011")
	assertInternal(t, err)
	if !errors.Is(err, selectErr) {
		t.Fatal("select cause lost")
	}
}

func TestApplyOpenContext(t *testing.T) {
	_, err := applyOpen(context.Background(), &fakeAccountTx{insertErr: context.Canceled}, merchantSpec(t), "AC0000000000000000011")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	_, err = applyOpen(context.Background(), &fakeAccountTx{insertErr: context.DeadlineExceeded}, merchantSpec(t), "AC0000000000000000011")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	_, err = applyOpen(context.Background(), &fakeAccountTx{insertErr: errno.ErrInvalidParam}, merchantSpec(t), "AC0000000000000000011")
	assertInvalidParam(t, err)
}

func TestApplyOpenAvailableOverflow(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	fake := &fakeAccountTx{
		insertErr: dup,
		found:     true,
		row: Account{
			AccountNo:        "AC0000000000000000001",
			OwnerType:        "MERCHANT",
			OwnerID:          "M100002",
			AccountType:      "MERCHANT_SETTLE",
			Currency:         "CNY",
			BalanceDirection: "CREDIT",
			Balance:          money.Amount(math.MinInt64),
			FrozenAmount:     1,
			Status:           "ACTIVE",
		},
	}
	_, err := applyOpen(context.Background(), fake, merchantSpec(t), "AC0000000000000000099")
	assertInternal(t, err)
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatal("overflow cause lost")
	}
}

func TestSQLAccountTxNil(t *testing.T) {
	var tx sqlAccountTx
	if err := tx.insertAccount(context.Background(), merchantSpec(t), "AC0000000000000000011"); err == nil {
		t.Fatal("nil tx insert")
	}
	if _, _, err := tx.selectAccount(context.Background(), merchantSpec(t)); err == nil {
		t.Fatal("nil tx select")
	}
}
