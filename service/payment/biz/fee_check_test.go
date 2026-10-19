package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// PAY-10：核对选 effective_from 不晚于创建时间的最近一档。
func TestScanFeesNearestRate(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	t1 := feeAt(11, 0, 0, 0)
	t2 := feeAt(12, 0, 0, 0)
	src := &fakeCheck{
		rates: []FeeRate{
			{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t1},
			{MerchantID: "M100009", RateBP: 100, EffectiveFrom: t0},
			{MerchantID: "M100002", RateBP: 0, EffectiveFrom: t0},
			{MerchantID: "M100002", RateBP: 80, EffectiveFrom: t2},
		},
		orders: []orderRow{
			{ID: 1, PayNo: "PA1", MerchantID: "M100002", Amount: 10000, FeeAmount: 0, CreatedAt: t0},
			{ID: 2, PayNo: "PA2", MerchantID: "M100002", Amount: 10000, FeeAmount: 60, CreatedAt: t1},
			{ID: 3, PayNo: "PA3", MerchantID: "M100002", Amount: 10000, FeeAmount: 0, CreatedAt: t1.Add(-time.Millisecond)},
			{ID: 4, PayNo: "PA4", MerchantID: "M100002", Amount: 10000, FeeAmount: 80, CreatedAt: t2},
		},
	}
	got, err := scanFees(context.Background(), src, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Scanned != 4 || got.Mismatched != 0 || len(got.Details) != 0 {
		t.Fatalf("got = %+v", got)
	}
	if src.calls != 1 || !src.from.IsZero() || !src.to.IsZero() {
		t.Fatalf("calls=%d from=%s to=%s", src.calls, src.from, src.to)
	}
}

// PAY-10：没有落在任何档内的订单计入不符，费率为 -1，应收手续费为 0。
func TestScanFeesNoRate(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	src := &fakeCheck{
		rates: []FeeRate{{MerchantID: "M100002", RateBP: 0, EffectiveFrom: t0}},
		orders: []orderRow{
			{ID: 1, PayNo: "PA1", MerchantID: "M100002", Amount: 10000, FeeAmount: 0, CreatedAt: t0.Add(-time.Millisecond)},
			{ID: 2, PayNo: "PA2", MerchantID: "M100008", Amount: 10000, FeeAmount: 0, CreatedAt: t0},
		},
	}
	got, err := scanFees(context.Background(), src, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Scanned != 2 || got.Mismatched != 2 || len(got.Details) != 2 {
		t.Fatalf("got = %+v", got)
	}
	for _, item := range got.Details {
		if item.RateBP != -1 || item.ExpectedFee != 0 || item.FeeAmount != 0 || item.Amount != 10000 {
			t.Fatalf("detail = %+v", item)
		}
	}
}

// PAY-10：实收手续费与 money.Fee 不一致时计入不符。
func TestScanFeesFeeMismatch(t *testing.T) {
	t1 := feeAt(11, 0, 0, 0)
	want, err := money.Fee(10000, 60)
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeCheck{
		rates: []FeeRate{{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t1}},
		orders: []orderRow{
			{ID: 1, PayNo: "PA1", MerchantID: "M100002", Amount: 10000, FeeAmount: 0, CreatedAt: t1.Add(time.Millisecond)},
			{ID: 2, PayNo: "PA2", MerchantID: "M100002", Amount: 10000, FeeAmount: want, CreatedAt: t1},
		},
	}
	got, err := scanFees(context.Background(), src, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Scanned != 2 || got.Mismatched != 1 || len(got.Details) != 1 {
		t.Fatalf("got = %+v", got)
	}
	item := got.Details[0]
	if item.PayNo != "PA1" || item.RateBP != 60 || item.ExpectedFee != want || item.FeeAmount != 0 {
		t.Fatalf("detail = %+v", item)
	}
}

func TestScanFeesFeeErrorStops(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	src := &fakeCheck{
		rates: []FeeRate{{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t0}},
		orders: []orderRow{
			{ID: 1, PayNo: "PA1", MerchantID: "M100002", Amount: -1, FeeAmount: 0, CreatedAt: t0},
			{ID: 2, PayNo: "PA2", MerchantID: "M100002", Amount: 10000, FeeAmount: 0, CreatedAt: t0},
		},
	}
	got, err := scanFees(context.Background(), src, time.Time{}, time.Time{})
	assertErrno(t, err, errno.ErrInternal)
	if got.Scanned != 0 || got.Mismatched != 0 || len(got.Details) != 0 {
		t.Fatalf("partial = %+v", got)
	}
	if src.calls != 1 {
		t.Fatalf("calls = %d", src.calls)
	}
}

func TestScanFeesDetailCapAndPages(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	orders := make([]orderRow, feeCheckPageSize+1)
	for i := range orders {
		orders[i] = orderRow{
			ID:         uint64(i + 1),
			PayNo:      "PA",
			MerchantID: "M100002",
			Amount:     10000,
			FeeAmount:  1,
			CreatedAt:  t0,
		}
	}
	src := &fakeCheck{
		rates:  []FeeRate{{MerchantID: "M100002", RateBP: 0, EffectiveFrom: t0}},
		orders: orders,
	}
	from := t0
	to := t0.Add(time.Hour)
	got, err := scanFees(context.Background(), src, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if got.Scanned != len(orders) || got.Mismatched != len(orders) || len(got.Details) != feeCheckDetailCap {
		t.Fatalf("scanned=%d mismatched=%d details=%d", got.Scanned, got.Mismatched, len(got.Details))
	}
	if src.calls != 2 {
		t.Fatalf("calls = %d", src.calls)
	}
	if !src.from.Equal(from) || !src.to.Equal(to) {
		t.Fatalf("window from=%s to=%s", src.from, src.to)
	}
}

func TestScanFeesSourceError(t *testing.T) {
	src := &fakeCheck{listErr: context.DeadlineExceeded}
	_, err := scanFees(context.Background(), src, time.Time{}, time.Time{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	src.listErr = errors.New("read rates")
	_, err = scanFees(context.Background(), src, time.Time{}, time.Time{})
	assertErrno(t, err, errno.ErrInternal)
}

func TestScanFeesContextAndOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := scanFees(ctx, &fakeCheck{}, time.Time{}, time.Time{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}

	loopCtx, stop := context.WithCancel(context.Background())
	src := &cancelAfterRates{cancel: stop}
	_, err = scanFees(loopCtx, src, time.Time{}, time.Time{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("loop error = %v", err)
	}
	if src.orders != 0 {
		t.Fatalf("orders read = %d", src.orders)
	}

	bad := &fakeCheck{orders: []orderRow{
		{ID: 5, PayNo: "PA5", MerchantID: "M100002", Amount: 10000, CreatedAt: feeAt(10, 0, 0, 0)},
		{ID: 4, PayNo: "PA4", MerchantID: "M100002", Amount: 10000, CreatedAt: feeAt(10, 0, 0, 0)},
	}}
	got, err := scanFees(context.Background(), bad, time.Time{}, time.Time{})
	assertErrno(t, err, errno.ErrInternal)
	if got.Scanned != 0 || got.Mismatched != 0 || len(got.Details) != 0 {
		t.Fatalf("partial = %+v", got)
	}
}

type cancelAfterRates struct {
	cancel context.CancelFunc
	orders int
}

func (c *cancelAfterRates) listRates(context.Context) ([]FeeRate, error) {
	c.cancel()
	return nil, nil
}

func (c *cancelAfterRates) listOrders(context.Context, uint64, time.Time, time.Time, int) ([]orderRow, error) {
	c.orders++
	return nil, nil
}

func TestOrderScanBounds(t *testing.T) {
	from := feeAt(10, 0, 0, 0)
	to := feeAt(11, 0, 0, 0)
	if q := orderScanQuery(time.Time{}, time.Time{}); q != orderScanAllSQL {
		t.Fatalf("all = %s", q)
	}
	if q := orderScanQuery(from, time.Time{}); q != orderScanFromSQL {
		t.Fatalf("from = %s", q)
	}
	if q := orderScanQuery(time.Time{}, to); q != orderScanToSQL {
		t.Fatalf("to = %s", q)
	}
	if q := orderScanQuery(from, to); q != orderScanFromToSQL {
		t.Fatalf("both = %s", q)
	}
	args := orderScanArgs(9, from, to, feeCheckPageSize)
	if len(args) != 4 || args[0] != uint64(9) || args[3] != feeCheckPageSize {
		t.Fatalf("args = %#v", args)
	}
	open := orderScanArgs(0, time.Time{}, time.Time{}, 2)
	if len(open) != 2 {
		t.Fatalf("open args = %#v", open)
	}
}

type fakeCheck struct {
	rates   []FeeRate
	orders  []orderRow
	listErr error
	calls   int
	from    time.Time
	to      time.Time
}

func (f *fakeCheck) listRates(context.Context) ([]FeeRate, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]FeeRate(nil), f.rates...), nil
}

func (f *fakeCheck) listOrders(_ context.Context, afterID uint64, from, to time.Time, limit int) ([]orderRow, error) {
	f.calls++
	f.from = from
	f.to = to
	if f.listErr != nil && f.calls > 1 {
		return nil, f.listErr
	}
	var page []orderRow
	for _, row := range f.orders {
		if row.ID <= afterID {
			continue
		}
		page = append(page, row)
		if len(page) == limit {
			break
		}
	}
	return page, nil
}
