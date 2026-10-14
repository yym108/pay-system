package biz

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/dal"
)

func assertNotFound(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatalf("error = %v, want account not found", err)
	}
	if err.Error() != errno.ErrAccountNotFound.Error() {
		t.Fatalf("Error() = %q", err.Error())
	}
}

// ACC-02 规则：类型与所有者不匹配时返回 10001，不访问数据库。
func TestGetAccountRejectsInvalid(t *testing.T) {
	svc := New(dal.New(nil, nil), newIDs(t, stubClock{now: openTestTime}), testSubCount)
	ctx := context.Background()
	cases := []struct {
		name                            string
		ownerType, ownerID, accountType string
	}{
		{name: "merchant fee", ownerType: "MERCHANT", ownerID: "M100002", accountType: "FEE_INCOME"},
		{name: "merchant channel", ownerType: "MERCHANT", ownerID: "M100002", accountType: "CHANNEL_RECEIVABLE"},
		{name: "merchant transit", ownerType: "MERCHANT", ownerID: "M100002", accountType: "TRANSIT"},
		{name: "platform settle", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "MERCHANT_SETTLE"},
		{name: "bad merchant id", ownerType: "MERCHANT", ownerID: "M10002", accountType: "MERCHANT_SETTLE"},
		{name: "merchant letters", ownerType: "MERCHANT", ownerID: "M10000A", accountType: "MERCHANT_SETTLE"},
		{name: "lower merchant", ownerType: "merchant", ownerID: "M100002", accountType: "MERCHANT_SETTLE"},
		{name: "channel empty owner", ownerType: "PLATFORM", ownerID: "", accountType: "CHANNEL_RECEIVABLE"},
		{name: "channel long owner", ownerType: "PLATFORM", ownerID: strings.Repeat("渠", 33), accountType: "CHANNEL_RECEIVABLE"},
		{name: "fee wrong owner", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "FEE_INCOME"},
		{name: "fee lower owner", ownerType: "PLATFORM", ownerID: "platform", accountType: "FEE_INCOME"},
		{name: "transit wrong owner", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "TRANSIT"},
		{name: "transit merchant owner", ownerType: "MERCHANT", ownerID: "PLATFORM", accountType: "TRANSIT"},
		{name: "unknown type", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "SUSPENSE"},
		{name: "empty", ownerType: "", ownerID: "", accountType: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.GetAccount(ctx, tc.ownerType, tc.ownerID, tc.accountType)
			assertInvalidParam(t, err)
			if tc.ownerID != "" && strings.Contains(err.Error(), tc.ownerID) {
				t.Fatal("error echoed owner id")
			}
		})
	}
}

func TestClassifyQuery(t *testing.T) {
	kind, shardNo, err := classifyQuery("MERCHANT", "M100002", "MERCHANT_SETTLE")
	if err != nil || kind != kindMerchant || shardNo != 0 {
		t.Fatalf("M100002 = %v shard %d err %v", kind, shardNo, err)
	}
	kind, shardNo, err = classifyQuery("MERCHANT", "M100001", "MERCHANT_SETTLE")
	if err != nil || kind != kindMerchant || shardNo != 1 {
		t.Fatalf("M100001 = %v shard %d err %v", kind, shardNo, err)
	}
	owner32 := strings.Repeat("a", 32)
	kind, _, err = classifyQuery("PLATFORM", owner32, "CHANNEL_RECEIVABLE")
	if err != nil || kind != kindPlatform {
		t.Fatalf("channel 32 = %v err %v", kind, err)
	}
	for _, accountType := range []string{"FEE_INCOME", "TRANSIT"} {
		kind, _, err = classifyQuery("PLATFORM", "PLATFORM", accountType)
		if err != nil || kind != kindPlatform {
			t.Fatalf("%s = %v err %v", accountType, kind, err)
		}
	}
	if _, _, err = classifyQuery("PLATFORM", "mockpay", "CHANNEL_RECEIVABLE"); err != nil {
		t.Fatal(err)
	}
}

func TestGetAccountNilAndCanceled(t *testing.T) {
	ctx := context.Background()
	_, err := (*Service)(nil).GetAccount(ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE")
	assertInternal(t, err)

	svc := New(nil, newIDs(t, stubClock{now: openTestTime}), testSubCount)
	_, err = svc.GetAccount(ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE")
	assertInternal(t, err)

	// 故意传入 nil，确认不会 panic。SA1012 禁止正常调用这么写。
	_, err = svc.GetAccount(nil, "MERCHANT", "M100002", "MERCHANT_SETTLE") //nolint:staticcheck
	assertInternal(t, err)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = New(dal.New(nil, nil), nil, testSubCount).GetAccount(canceled, "MERCHANT", "M100002", "MERCHANT_SETTLE")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	_, err = New(dal.New(nil, nil), nil, testSubCount).GetAccount(canceled, "NOT", "X", "NOPE")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled invalid = %v", err)
	}
}

// ACC-02 规则：查询失败不是 40001。库不可用时返回 10003，Error() 不含 SQL。
func TestGetAccountDBErrorIsInternal(t *testing.T) {
	svc := New(dal.New(nil, nil), nil, testSubCount)
	ctx := context.Background()
	cases := []struct {
		name                            string
		ownerType, ownerID, accountType string
	}{
		{name: "merchant", ownerType: "MERCHANT", ownerID: "M100002", accountType: "MERCHANT_SETTLE"},
		{name: "merchant shard 1", ownerType: "MERCHANT", ownerID: "M100001", accountType: "MERCHANT_SETTLE"},
		{name: "channel", ownerType: "PLATFORM", ownerID: "mockpay", accountType: "CHANNEL_RECEIVABLE"},
		{name: "fee", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "FEE_INCOME"},
		{name: "transit", ownerType: "PLATFORM", ownerID: "PLATFORM", accountType: "TRANSIT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.GetAccount(ctx, tc.ownerType, tc.ownerID, tc.accountType)
			assertInternal(t, err)
			if errors.Is(err, errno.ErrAccountNotFound) {
				t.Fatal("db error became 40001")
			}
		})
	}
}

type fakeLister struct {
	rows  []Account
	err   error
	calls []int
	subs  []bool
}

func (f *fakeLister) listAccounts(_ context.Context, shardNo int, _, _, _ string, subNo *int) ([]Account, error) {
	f.calls = append(f.calls, shardNo)
	f.subs = append(f.subs, subNo != nil)
	if f.err != nil {
		return nil, f.err
	}
	var out []Account
	for _, row := range f.rows {
		if row.Shard != shardNo {
			continue
		}
		if subNo != nil && row.SubNo != *subNo {
			continue
		}
		out = append(out, row)
	}
	return out, nil
}

func TestGetAccountUsesLister(t *testing.T) {
	merchant := Account{
		AccountNo:        "AC0000000000000000007",
		OwnerType:        "MERCHANT",
		OwnerID:          "M100001",
		AccountType:      "MERCHANT_SETTLE",
		SubNo:            0,
		Currency:         "CNY",
		BalanceDirection: "CREDIT",
		Balance:          1000,
		FrozenAmount:     300,
		Status:           "ACTIVE",
		Shard:            1,
	}
	fake := &fakeLister{rows: []Account{merchant}}
	svc := New(nil, nil, testSubCount)
	svc.query = fake
	got, err := svc.GetAccount(context.Background(), "MERCHANT", "M100001", "MERCHANT_SETTLE")
	if err != nil {
		t.Fatal(err)
	}
	if got.Balance != 1000 || got.FrozenAmount != 300 || got.AvailableAmount != 700 || got.SubCount != 1 {
		t.Fatalf("account = %+v", got)
	}
	if got.Shard != 1 || got.AccountNo != merchant.AccountNo || got.SubNo != 0 {
		t.Fatalf("account = %+v", got)
	}
	if len(fake.calls) != 1 || fake.calls[0] != 1 || !fake.subs[0] {
		t.Fatalf("calls = %v subs = %v", fake.calls, fake.subs)
	}

	fake.rows = nil
	_, err = svc.GetAccount(context.Background(), "MERCHANT", "M100001", "MERCHANT_SETTLE")
	assertNotFound(t, err)

	boom := errors.New("SELECT account failed password=secret")
	fake.err = boom
	_, err = svc.GetAccount(context.Background(), "MERCHANT", "M100001", "MERCHANT_SETTLE")
	assertInternal(t, err)
	if errors.Is(err, errno.ErrAccountNotFound) || !errors.Is(err, boom) {
		t.Fatalf("error = %v", err)
	}

	fake.err = nil
	fake.calls = nil
	fake.subs = nil
	fake.rows = []Account{
		{OwnerType: "PLATFORM", OwnerID: "mockpay", AccountType: "CHANNEL_RECEIVABLE", Shard: 0, SubNo: 1, Currency: "CNY", BalanceDirection: "DEBIT", Balance: 1000, FrozenAmount: 100, Status: "ACTIVE"},
		{OwnerType: "PLATFORM", OwnerID: "mockpay", AccountType: "CHANNEL_RECEIVABLE", Shard: 1, SubNo: 4, Currency: "CNY", BalanceDirection: "DEBIT", Balance: 2500, FrozenAmount: 50, Status: "ACTIVE"},
	}
	got, err = svc.GetAccount(context.Background(), "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	if err != nil {
		t.Fatal(err)
	}
	if got.Balance != 3500 || got.FrozenAmount != 150 || got.AvailableAmount != 3350 || got.SubCount != 2 {
		t.Fatalf("sum = %+v", got)
	}
	if got.AccountNo != "" || got.Shard != 0 || got.SubNo != 0 || got.BalanceDirection != "DEBIT" || got.Status != "ACTIVE" {
		t.Fatalf("sum = %+v", got)
	}
	if len(fake.calls) != 2 || fake.calls[0] != 0 || fake.calls[1] != 1 || fake.subs[0] || fake.subs[1] {
		t.Fatalf("calls = %v subs = %v", fake.calls, fake.subs)
	}

	fake.rows = fake.rows[:1]
	fake.calls = nil
	got, err = svc.GetAccount(context.Background(), "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	if err != nil {
		t.Fatal(err)
	}
	if got.Balance != 1000 || got.SubCount != 1 || got.AvailableAmount != 900 {
		t.Fatalf("one shard = %+v", got)
	}

	fake.rows = nil
	fake.calls = nil
	_, err = svc.GetAccount(context.Background(), "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	assertNotFound(t, err)
	if len(fake.calls) != 2 {
		t.Fatalf("not found calls = %v", fake.calls)
	}

	fake.err = boom
	_, err = svc.GetAccount(context.Background(), "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	assertInternal(t, err)
	if errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatal("platform db error became 40001")
	}
}

func TestGetAccountSecondShardError(t *testing.T) {
	fake := &seqLister{
		results: []listerResult{
			{rows: []Account{{
				OwnerType: "PLATFORM", OwnerID: "PLATFORM", AccountType: "FEE_INCOME",
				Currency: "CNY", BalanceDirection: "CREDIT", Balance: 10, Status: "ACTIVE",
			}}},
			{err: errors.New("shard 1 down")},
		},
	}
	svc := New(nil, nil, testSubCount)
	svc.query = fake
	_, err := svc.GetAccount(context.Background(), "PLATFORM", "PLATFORM", "FEE_INCOME")
	assertInternal(t, err)
	if errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatal("second shard error became 40001")
	}
	if fake.n != 2 {
		t.Fatalf("calls = %d", fake.n)
	}
}

type listerResult struct {
	rows []Account
	err  error
}

type seqLister struct {
	results []listerResult
	n       int
}

func (s *seqLister) listAccounts(context.Context, int, string, string, string, *int) ([]Account, error) {
	if s.n >= len(s.results) {
		return nil, errors.New("unexpected list")
	}
	item := s.results[s.n]
	s.n++
	return item.rows, item.err
}

func TestOneMerchant(t *testing.T) {
	got, err := oneMerchant([]Account{{
		AccountNo: "AC1", OwnerType: "MERCHANT", OwnerID: "M100002", AccountType: "MERCHANT_SETTLE",
		Currency: "CNY", BalanceDirection: "CREDIT", Balance: 1000, FrozenAmount: 300, Status: "ACTIVE",
	}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.AvailableAmount != 700 || got.SubCount != 1 || got.Shard != 0 || got.Balance != 1000 {
		t.Fatalf("account = %+v", got)
	}
	_, err = oneMerchant(nil, 0)
	assertNotFound(t, err)
	_, err = oneMerchant([]Account{{}, {}}, 0)
	assertInternal(t, err)
	_, err = oneMerchant([]Account{{SubNo: 1, Balance: 1}}, 0)
	assertInternal(t, err)
	_, err = oneMerchant([]Account{{Balance: money.Amount(math.MinInt64), FrozenAmount: 1}}, 0)
	assertInternal(t, err)
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatal("overflow cause lost")
	}
}

func TestSumPlatform(t *testing.T) {
	rows := []Account{
		{OwnerType: "PLATFORM", OwnerID: "mockpay", AccountType: "CHANNEL_RECEIVABLE", Currency: "CNY", BalanceDirection: "DEBIT", Balance: 1000, FrozenAmount: 300, Status: "ACTIVE", Shard: 0, SubNo: 0},
		{OwnerType: "PLATFORM", OwnerID: "mockpay", AccountType: "CHANNEL_RECEIVABLE", Currency: "CNY", BalanceDirection: "DEBIT", Balance: 50, FrozenAmount: 20, Status: "ACTIVE", Shard: 1, SubNo: 7},
	}
	got, err := sumPlatform(rows, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	if err != nil {
		t.Fatal(err)
	}
	// 可用金额用合计后再减：1050 - 320 = 730，不是各行可用金额另做一次浮点运算。
	if got.Balance != 1050 || got.FrozenAmount != 320 || got.AvailableAmount != 730 || got.SubCount != 2 {
		t.Fatalf("sum = %+v", got)
	}
	if got.AccountNo != "" || got.Shard != 0 || got.SubNo != 0 {
		t.Fatalf("omitted fields = %+v", got)
	}

	_, err = sumPlatform(nil, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	assertNotFound(t, err)

	bad := append([]Account{}, rows...)
	bad[1].Status = "FROZEN"
	_, err = sumPlatform(bad, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	assertInternal(t, err)

	bad = append([]Account{}, rows...)
	bad[1].BalanceDirection = "CREDIT"
	_, err = sumPlatform(bad, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	assertInternal(t, err)

	bad = append([]Account{}, rows...)
	bad[0].OwnerID = "other"
	_, err = sumPlatform(bad, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	assertInternal(t, err)

	_, err = sumPlatform([]Account{{
		OwnerType: "PLATFORM", OwnerID: "mockpay", AccountType: "CHANNEL_RECEIVABLE",
		Currency: "CNY", BalanceDirection: "DEBIT", Balance: money.Amount(math.MaxInt64), Status: "ACTIVE",
	}, {
		OwnerType: "PLATFORM", OwnerID: "mockpay", AccountType: "CHANNEL_RECEIVABLE",
		Currency: "CNY", BalanceDirection: "DEBIT", Balance: 1, Status: "ACTIVE",
	}}, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	assertInternal(t, err)
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatal("balance overflow cause lost")
	}

	_, err = sumPlatform([]Account{{
		OwnerType: "PLATFORM", OwnerID: "PLATFORM", AccountType: "TRANSIT",
		Currency: "CNY", BalanceDirection: "CREDIT", FrozenAmount: money.Amount(math.MaxInt64), Status: "ACTIVE",
	}, {
		OwnerType: "PLATFORM", OwnerID: "PLATFORM", AccountType: "TRANSIT",
		Currency: "CNY", BalanceDirection: "CREDIT", FrozenAmount: 1, Status: "ACTIVE",
	}}, "PLATFORM", "PLATFORM", "TRANSIT")
	assertInternal(t, err)
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatal("frozen overflow cause lost")
	}

	_, err = sumPlatform([]Account{{
		OwnerType: "PLATFORM", OwnerID: "PLATFORM", AccountType: "FEE_INCOME",
		Currency: "CNY", BalanceDirection: "CREDIT", Balance: money.Amount(math.MinInt64), FrozenAmount: 1, Status: "ACTIVE",
	}}, "PLATFORM", "PLATFORM", "FEE_INCOME")
	assertInternal(t, err)
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatal("available overflow cause lost")
	}
}

func TestListAccountsNilDB(t *testing.T) {
	sub := 0
	if _, err := listAccounts(context.Background(), nil, "MERCHANT", "M100002", "MERCHANT_SETTLE", &sub); err == nil {
		t.Fatal("nil db")
	}
	if _, err := listAccounts(context.Background(), nil, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", nil); err == nil {
		t.Fatal("nil db platform")
	}
}

type fakeScan struct {
	vals []any
	err  error
}

func (f fakeScan) Scan(dest ...any) error {
	if f.err != nil {
		return f.err
	}
	if len(dest) != len(f.vals) {
		return errors.New("scan argc")
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			s, ok := f.vals[i].(string)
			if !ok {
				return errors.New("want string")
			}
			*p = s
		case *int:
			n, ok := f.vals[i].(int)
			if !ok {
				return errors.New("want int")
			}
			*p = n
		case *int64:
			n, ok := f.vals[i].(int64)
			if !ok {
				return errors.New("want int64")
			}
			*p = n
		default:
			return errors.New("dest type")
		}
	}
	return nil
}

func TestScanAccount(t *testing.T) {
	got, err := scanAccount(fakeScan{vals: []any{
		"AC9", "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, "CNY", "CREDIT", int64(1000), int64(300), "ACTIVE",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got.AccountNo != "AC9" || got.Balance != 1000 || got.FrozenAmount != 300 || got.SubNo != 0 || got.Status != "ACTIVE" {
		t.Fatalf("scanned = %+v", got)
	}
	if got.AvailableAmount != 0 || got.SubCount != 0 || got.Shard != 0 {
		t.Fatalf("scan filled derived fields: %+v", got)
	}
	_, err = scanAccount(fakeScan{err: errors.New("SELECT boom")})
	if err == nil || !strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("error = %v", err)
	}
}

func TestGetAccountRejectsUnknownKind(t *testing.T) {
	_, err := getAccount(context.Background(), &fakeLister{}, 0, 0, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	assertInvalidParam(t, err)
}

func TestGetAccountContextOnQuery(t *testing.T) {
	fake := &fakeLister{err: context.DeadlineExceeded}
	svc := New(nil, nil, testSubCount)
	svc.query = fake
	_, err := svc.GetAccount(context.Background(), "PLATFORM", "PLATFORM", "TRANSIT")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	_, err = svc.GetAccount(context.Background(), "MERCHANT", "M100002", "MERCHANT_SETTLE")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
}
