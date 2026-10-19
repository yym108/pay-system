package biz

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/errno"
)

// 测试时刻固定。biz 包禁止调用当前时间。
var feeZone = time.FixedZone("UTC+8", 8*60*60)

func feeAt(hour, min, sec, nsec int) time.Time {
	return time.Date(2026, 10, 17, hour, min, sec, nsec, feeZone)
}

func assertErrno(t *testing.T, err error, want *errno.Error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %s", err, want.Error())
	}
	if err.Error() != want.Error() {
		t.Fatalf("Error() = %q, want %q", err.Error(), want.Error())
	}
}

type nodbDriver struct{}

func (nodbDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("nodb")
}

func init() {
	sql.Register("payfee-nodb", nodbDriver{})
}

func closedDB(t *testing.T) *sqlx.DB {
	t.Helper()
	raw, err := sql.Open("payfee-nodb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return sqlx.NewDb(raw, "payfee-nodb")
}

// PAY-10：商户号为空、费率越界或生效时间为零值时返回 10001，不写行。
func TestAppendFeeRateRejectsBadInput(t *testing.T) {
	svc := New(closedDB(t))
	ctx := context.Background()
	at := feeAt(10, 0, 0, 0)
	cases := []struct {
		name string
		id   string
		rate int
		at   time.Time
	}{
		{name: "empty merchant", id: "", rate: 0, at: at},
		{name: "rate negative", id: "M100002", rate: -1, at: at},
		{name: "rate above", id: "M100002", rate: 10001, at: at},
		{name: "zero time", id: "M100002", rate: 60, at: time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.AppendFeeRate(ctx, tc.id, tc.rate, tc.at)
			assertErrno(t, err, errno.ErrInvalidParam)
		})
	}
	if err := validateAppend("M100002", 0, at); err != nil {
		t.Fatal(err)
	}
	if err := validateAppend("M100002", 10000, at); err != nil {
		t.Fatal(err)
	}
}

// PAY-10：服务、上下文或库为空是 10003；已取消的上下文原样返回。
func TestAppendFeeRateNilAndCanceled(t *testing.T) {
	at := feeAt(10, 0, 0, 0)
	var none *Service
	_, err := none.AppendFeeRate(context.Background(), "M100002", 60, at)
	assertErrno(t, err, errno.ErrInternal)

	_, err = New(closedDB(t)).AppendFeeRate(nil, "M100002", 60, at) //nolint:staticcheck // 空上下文必须返回 10003
	assertErrno(t, err, errno.ErrInternal)

	_, err = New(nil).AppendFeeRate(context.Background(), "M100002", 60, at)
	assertErrno(t, err, errno.ErrInternal)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = New(nil).AppendFeeRate(ctx, "", -1, time.Time{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}

	_, err = New(nil).CheckOrderFees(ctx, time.Time{}, time.Time{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("check error = %v, want canceled", err)
	}
	_, err = none.CheckOrderFees(context.Background(), time.Time{}, time.Time{})
	assertErrno(t, err, errno.ErrInternal)
}

type fakeRateTx struct {
	merchants map[string]bool
	rates     []FeeRate
	inserts   int
	insertErr error
	lockErr   error
	readErr   error
	missAt    bool
}

func (f *fakeRateTx) lockMerchant(_ context.Context, merchantID string) (bool, error) {
	if f.lockErr != nil {
		return false, f.lockErr
	}
	return f.merchants[merchantID], nil
}

func (f *fakeRateTx) latestRate(_ context.Context, merchantID string) (FeeRate, bool, error) {
	if f.readErr != nil {
		return FeeRate{}, false, f.readErr
	}
	var best FeeRate
	found := false
	for _, row := range f.rates {
		if row.MerchantID != merchantID {
			continue
		}
		if !found || row.EffectiveFrom.After(best.EffectiveFrom) {
			best = row
			found = true
		}
	}
	return best, found, nil
}

func (f *fakeRateTx) rateAt(_ context.Context, merchantID string, effectiveFrom time.Time) (FeeRate, bool, error) {
	if f.missAt {
		return FeeRate{}, false, nil
	}
	if f.readErr != nil {
		return FeeRate{}, false, f.readErr
	}
	for _, row := range f.rates {
		if row.MerchantID == merchantID && row.EffectiveFrom.UnixMilli() == effectiveFrom.UnixMilli() {
			return row, true, nil
		}
	}
	return FeeRate{}, false, nil
}

func (f *fakeRateTx) insertRate(_ context.Context, row FeeRate) error {
	f.inserts++
	if f.insertErr != nil {
		return f.insertErr
	}
	for _, existing := range f.rates {
		if existing.MerchantID == row.MerchantID && existing.EffectiveFrom.UnixMilli() == row.EffectiveFrom.UnixMilli() {
			return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
		}
	}
	f.rates = append(f.rates, row)
	return nil
}

func merchantFake(rates ...FeeRate) *fakeRateTx {
	return &fakeRateTx{merchants: map[string]bool{"M100002": true}, rates: append([]FeeRate(nil), rates...)}
}

// PAY-10：新时间早于已有最大生效时间时返回 10001，不插入。
func TestApplyAppendEarlierThanMax(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	t1 := feeAt(11, 0, 0, 0)
	t2 := feeAt(12, 0, 0, 0)
	fake := merchantFake(
		FeeRate{MerchantID: "M100002", RateBP: 0, EffectiveFrom: t0},
		FeeRate{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t2},
	)
	_, err := applyAppend(context.Background(), fake, "M100002", 30, t1)
	assertErrno(t, err, errno.ErrInvalidParam)
	if fake.inserts != 0 || len(fake.rates) != 2 {
		t.Fatalf("inserts=%d rows=%d", fake.inserts, len(fake.rates))
	}
}

// PAY-10：相同时间相同费率返回已有行，Duplicated 为 true，不新增行。
func TestApplyAppendSameTimeSameRate(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	fake := merchantFake(FeeRate{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t0})
	got, err := applyAppend(context.Background(), fake, "M100002", 60, t0.Add(400*time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Duplicated || got.RateBP != 60 || got.MerchantID != "M100002" {
		t.Fatalf("got = %+v", got)
	}
	if got.EffectiveFrom.UnixMilli() != t0.UnixMilli() {
		t.Fatalf("effective = %s", got.EffectiveFrom)
	}
	if fake.inserts != 1 || len(fake.rates) != 1 || fake.rates[0].RateBP != 60 {
		t.Fatalf("inserts=%d rows=%+v", fake.inserts, fake.rates)
	}
}

// PAY-10：相同时间不同费率返回 10006，旧费率还在。
func TestApplyAppendSameTimeDifferentRate(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	fake := merchantFake(FeeRate{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t0})
	_, err := applyAppend(context.Background(), fake, "M100002", 80, t0)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if fake.inserts != 1 || len(fake.rates) != 1 || fake.rates[0].RateBP != 60 {
		t.Fatalf("inserts=%d rows=%+v", fake.inserts, fake.rates)
	}
}

// PAY-10：商户不存在返回 20004，不插入。
func TestApplyAppendMerchantMissing(t *testing.T) {
	fake := merchantFake()
	_, err := applyAppend(context.Background(), fake, "M100099", 60, feeAt(10, 0, 0, 0))
	assertErrno(t, err, errno.ErrMerchantInvalid)
	if fake.inserts != 0 {
		t.Fatalf("inserts = %d", fake.inserts)
	}
}

func TestApplyAppendFirstAndLater(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	t1 := feeAt(11, 0, 0, 0)
	fake := merchantFake()
	first, err := applyAppend(context.Background(), fake, "M100002", 0, t0)
	if err != nil || first.Duplicated || first.RateBP != 0 {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	second, err := applyAppend(context.Background(), fake, "M100002", 60, t1)
	if err != nil || second.Duplicated || second.RateBP != 60 {
		t.Fatalf("second = %+v err=%v", second, err)
	}
	if len(fake.rates) != 2 {
		t.Fatalf("rows = %+v", fake.rates)
	}
}

func TestApplyAppendDuplicateIsNotText(t *testing.T) {
	fake := merchantFake(FeeRate{MerchantID: "M100002", RateBP: 60, EffectiveFrom: feeAt(10, 0, 0, 0)})
	fake.insertErr = errors.New("Error 1062: Duplicate entry")
	_, err := applyAppend(context.Background(), fake, "M100002", 60, feeAt(11, 0, 0, 0))
	assertErrno(t, err, errno.ErrInternal)

	fake.insertErr = &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	fake.missAt = true
	_, err = applyAppend(context.Background(), fake, "M100002", 60, feeAt(11, 0, 0, 0))
	assertErrno(t, err, errno.ErrInternal)

	fake.missAt = false
	fake.readErr = errors.New("select failed")
	_, err = applyAppend(context.Background(), fake, "M100002", 60, feeAt(11, 0, 0, 0))
	assertErrno(t, err, errno.ErrInternal)
}

func TestApplyAppendCanceled(t *testing.T) {
	fake := merchantFake()
	fake.lockErr = context.Canceled
	_, err := applyAppend(context.Background(), fake, "M100002", 60, feeAt(10, 0, 0, 0))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestApplyAppendTruncatesToZero(t *testing.T) {
	at := time.Date(1, 1, 1, 0, 0, 0, 100, time.UTC)
	if at.IsZero() {
		t.Fatal("precondition")
	}
	fake := merchantFake()
	_, err := applyAppend(context.Background(), fake, "M100002", 0, at)
	assertErrno(t, err, errno.ErrInvalidParam)
	if fake.inserts != 0 {
		t.Fatalf("inserts = %d", fake.inserts)
	}
}

func TestCallErrPassThrough(t *testing.T) {
	if callErr(nil) != nil {
		t.Fatal("nil")
	}
	if err := callErr(errno.ErrInvalidParam); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("error = %v", err)
	}
	wrapped := errno.Wrap(errno.ErrInternal, errors.New("hidden"))
	if err := callErr(wrapped); !errors.Is(err, errno.ErrInternal) || err.Error() != errno.ErrInternal.Error() {
		t.Fatalf("error = %v", err)
	}
}
