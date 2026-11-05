package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
)

func refundQuerySeed() (*memOrders, *Payments, RefundOrder) {
	orders := newRefundOrders()
	created := feeAt(10, 0, 0, 0)
	row := RefundOrder{
		RefundNo: "RF0000000000000000001", MerchantID: "M100002", OutRefundNo: "R1",
		PayNo: "PA_REF_1", Amount: 3000, Currency: payCurrencyCNY, Reason: "demo",
		Status: refundStatusProcessing, Channel: channelMockpay, Version: 1,
		CreatedAt: created, UpdatedAt: created,
	}
	orders.refunds = append(orders.refunds, row)
	return orders, newRefundPayments(orders, nil), row
}

func refundReadCount(m *memOrders) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.refundReads
}

// REF-05：两个单号都空、都非空，或请求为 nil，返回 10001，不读库。空串算没提供，不 trim。
func TestQueryRefundRejectsBadKeysWithoutRead(t *testing.T) {
	orders, p, row := refundQuerySeed()
	var nilP *Payments
	if _, err := nilP.QueryRefund(context.Background(), nil); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("nil payments with nil input = %v", err)
	}
	for _, in := range []*RefundQueryInput{
		nil,
		{MerchantID: row.MerchantID},
		{MerchantID: row.MerchantID, RefundNo: "", OutRefundNo: ""},
		{MerchantID: row.MerchantID, RefundNo: row.RefundNo, OutRefundNo: row.OutRefundNo},
		{MerchantID: row.MerchantID, RefundNo: " ", OutRefundNo: " "},
	} {
		got, err := p.QueryRefund(context.Background(), in)
		assertErrno(t, err, errno.ErrInvalidParam)
		if got.RefundNo != "" {
			t.Fatalf("in=%+v got=%+v", in, got)
		}
	}
	if n := refundReadCount(orders); n != 0 {
		t.Fatalf("invalid query read the store %d times", n)
	}
}

// REF-05：本商户按 refund_no 与 out_refund_no 都能查到同一行，原样返回，不改行。四种状态都原样返回。
func TestQueryRefundReadsOwnRefund(t *testing.T) {
	orders, p, row := refundQuerySeed()
	byNo, err := p.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
	if err != nil || byNo != row {
		t.Fatalf("by refund_no = %+v err=%v", byNo, err)
	}
	byOut, err := p.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, OutRefundNo: row.OutRefundNo})
	if err != nil || byOut != row {
		t.Fatalf("by out_refund_no = %+v err=%v", byOut, err)
	}
	succeeded := feeAt(10, 5, 0, 0)
	for _, st := range []struct {
		status, failCode string
		at               time.Time
	}{
		{refundStatusInit, "", time.Time{}},
		{refundStatusProcessing, "", time.Time{}},
		{payStatusSuccess, "", succeeded},
		{refundStatusFailed, failCodeChannelFailed, time.Time{}},
	} {
		orders.mu.Lock()
		orders.refunds[0].Status = st.status
		orders.refunds[0].FailCode = st.failCode
		orders.refunds[0].SucceededAt = st.at
		orders.mu.Unlock()
		got, qerr := p.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
		if qerr != nil || got.Status != st.status || got.FailCode != st.failCode || !got.SucceededAt.Equal(st.at) || got.Version != row.Version {
			t.Fatalf("status %s got=%+v err=%v", st.status, got, qerr)
		}
	}
	rows := copiedRefunds(orders)
	if len(rows) != 1 || rows[0].Version != row.Version || !rows[0].UpdatedAt.Equal(row.UpdatedAt) {
		t.Fatalf("query changed the row: %+v", rows)
	}
	if amt, ver := payRefunded(orders, row.PayNo); amt != 0 || ver != 1 {
		t.Fatalf("query touched payment_order refunded=%d version=%d", amt, ver)
	}
	if len(copiedTxns(orders)) != 0 {
		t.Fatal("query wrote channel_txn")
	}
}

// REF-05 验收 1（单元）：他人的退款单与不存在的退款单是同一个 30001，不带退款单。
// 空商户号、大小写不同、未 trim 的空格也分不出退款单在不在。
func TestQueryRefundOtherMerchantSameAsMissing(t *testing.T) {
	orders, p, row := refundQuerySeed()
	other, errOther := p.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: "M100003", RefundNo: row.RefundNo})
	missing, errMissing := p.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: "RF9999999999999999999"})
	assertErrno(t, errOther, errno.ErrOrderNotFound)
	assertErrno(t, errMissing, errno.ErrOrderNotFound)
	if errOther.Error() != errMissing.Error() || other.RefundNo != "" || missing.RefundNo != "" {
		t.Fatalf("other=%+v %v missing=%+v %v", other, errOther, missing, errMissing)
	}
	for _, in := range []*RefundQueryInput{
		{MerchantID: "", RefundNo: row.RefundNo},
		{MerchantID: "m100002", RefundNo: row.RefundNo},
		{MerchantID: row.MerchantID + " ", RefundNo: row.RefundNo},
		{MerchantID: row.MerchantID, RefundNo: row.RefundNo + " "},
		{MerchantID: "M100003", OutRefundNo: row.OutRefundNo},
		{MerchantID: "", OutRefundNo: row.OutRefundNo},
		{MerchantID: row.MerchantID, OutRefundNo: row.OutRefundNo + " "},
		{MerchantID: row.MerchantID, OutRefundNo: "NO-SUCH"},
	} {
		got, qerr := p.QueryRefund(context.Background(), in)
		assertErrno(t, qerr, errno.ErrOrderNotFound)
		if got.RefundNo != "" || qerr.Error() != errMissing.Error() {
			t.Fatalf("in=%+v got=%+v err=%v", in, got, qerr)
		}
	}
	rows := copiedRefunds(orders)
	if len(rows) != 1 || rows[0] != row {
		t.Fatalf("lookup changed the row: %+v", rows)
	}
}

// 存储层返回字节不同的行（模拟排序规则放宽匹配）时仍是 30001。
func TestQueryRefundRechecksStoredKeys(t *testing.T) {
	orders, p, row := refundQuerySeed()
	store := &looseRefundStore{memOrders: orders}
	p.orders = store
	for _, in := range []*RefundQueryInput{
		{MerchantID: row.MerchantID, RefundNo: "rf0000000000000000001"},
		{MerchantID: "m100002", OutRefundNo: row.OutRefundNo},
		{MerchantID: row.MerchantID, OutRefundNo: "r1"},
	} {
		_, err := p.QueryRefund(context.Background(), in)
		assertErrno(t, err, errno.ErrOrderNotFound)
	}
}

// looseRefundStore 不论单号与商户号，都返回第一张退款单，模拟大小写不敏感的排序规则。
type looseRefundStore struct {
	*memOrders
}

func (l *looseRefundStore) refundByNo(_ context.Context, _ string) (RefundOrder, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.refunds[0], true, nil
}

func (l *looseRefundStore) refundByMerchantOut(_ context.Context, _, _ string) (RefundOrder, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.refunds[0], true, nil
}

// 读库失败返回 10003；ctx 已取消时不读库，原样返回 ctx 错误；依赖未装上返回 10003。
func TestQueryRefundStoreErrorAndCanceledContext(t *testing.T) {
	orders, p, row := refundQuerySeed()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.QueryRefund(ctx, &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	if n := refundReadCount(orders); n != 0 {
		t.Fatalf("canceled query read the store %d times", n)
	}
	orders.mu.Lock()
	orders.readErr = errors.New("db down")
	orders.mu.Unlock()
	_, err := p.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
	assertErrno(t, err, errno.ErrInternal)
	_, err = p.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, OutRefundNo: row.OutRefundNo})
	assertErrno(t, err, errno.ErrInternal)

	noStore := newPayments(nil, nil, nil, "", nil)
	_, err = noStore.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
	assertErrno(t, err, errno.ErrInternal)
	var nilP *Payments
	_, err = nilP.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
	assertErrno(t, err, errno.ErrInternal)
	//nolint:staticcheck // 故意传 nil ctx，确认返回 10003 而不是 panic。
	_, err = p.QueryRefund(nil, &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
	assertErrno(t, err, errno.ErrInternal)
}

// App 转给 Payments；子对象为 nil 时返回 10003。
func TestAppQueryRefund(t *testing.T) {
	_, p, row := refundQuerySeed()
	got, err := NewApp(nil, p).QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
	if err != nil || got != row {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	_, err = NewApp(nil, nil).QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
	assertErrno(t, err, errno.ErrInternal)
	var nilApp *App
	_, err = nilApp.QueryRefund(context.Background(), &RefundQueryInput{MerchantID: row.MerchantID, RefundNo: row.RefundNo})
	assertErrno(t, err, errno.ErrInternal)
}
