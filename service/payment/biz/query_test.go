package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/yym108/pay-system/pkg/errno"
)

func TestQueryPaymentRejectsBadKeysWithoutRead(t *testing.T) {
	p, _, orders, _, _ := newPayTest(t, "merchant-demo")
	created, err := p.CreatePayment(context.Background(), payInput(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	payReads, outReads := orders.queryReads()
	var nilP *Payments
	if _, err = nilP.QueryPayment(context.Background(), nil); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("nil payments with nil input = %v", err)
	}
	cases := []*QueryInput{
		nil,
		{MerchantID: created.MerchantID},
		{MerchantID: created.MerchantID, PayNo: "", OutTradeNo: ""},
		{MerchantID: created.MerchantID, PayNo: created.PayNo, OutTradeNo: created.OutTradeNo},
		// 空格不算空，两侧都算已提供，仍然不读库。不 trim。
		{MerchantID: created.MerchantID, PayNo: " ", OutTradeNo: " "},
	}
	for _, in := range cases {
		_, err = p.QueryPayment(context.Background(), in)
		assertErrno(t, err, errno.ErrInvalidParam)
	}
	if gotPay, gotOut := orders.queryReads(); gotPay != payReads || gotOut != outReads {
		t.Fatalf("query reads pay=%d out=%d, want %d %d", gotPay, gotOut, payReads, outReads)
	}
	if orders.rows[0].Status != payStatusInit || orders.rows[0].Version != created.Version || orders.rows[0].Amount != 10000 {
		t.Fatalf("invalid query changed the row: %+v", orders.rows[0])
	}
}

func TestQueryPaymentReadsOwnOrder(t *testing.T) {
	p, _, orders, _, _ := newPayTest(t, "merchant-demo")
	created, err := p.CreatePayment(context.Background(), payInput(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	byPay, err := p.QueryPayment(context.Background(), &QueryInput{
		MerchantID: created.MerchantID, PayNo: created.PayNo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if byPay.PayNo != created.PayNo || byPay.OutTradeNo != created.OutTradeNo || byPay.MerchantID != created.MerchantID ||
		byPay.Amount != 10000 || byPay.FeeAmount != 60 || byPay.Status != payStatusInit || byPay.Version != created.Version {
		t.Fatalf("by pay_no = %+v", byPay)
	}
	byOut, err := p.QueryPayment(context.Background(), &QueryInput{
		MerchantID: created.MerchantID, OutTradeNo: created.OutTradeNo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if byOut.PayNo != created.PayNo || byOut.Amount != created.Amount || byOut.Status != payStatusInit || byOut.Version != created.Version {
		t.Fatalf("by out_trade_no = %+v", byOut)
	}
	if orders.rows[0].Status != payStatusInit || orders.rows[0].Version != created.Version || orders.updates != 0 {
		t.Fatalf("read changed the row: %+v updates=%d", orders.rows[0], orders.updates)
	}
	for _, status := range []string{payStatusInit, payStatusPaying, payStatusSuccess, payStatusFailed, payStatusClosed} {
		orders.rows[0].Status = status
		got, qerr := p.QueryPayment(context.Background(), &QueryInput{
			MerchantID: created.MerchantID, PayNo: created.PayNo,
		})
		if qerr != nil || got.Status != status || got.Version != created.Version || orders.rows[0].Version != created.Version {
			t.Fatalf("status %s got=%+v err=%v stored=%+v", status, got, qerr, orders.rows[0])
		}
	}
}

// 不存在的 pay_no 与他人订单是同一个 30001。空商户号、大小写不同、未 trim 的空格也分不出订单在不在。
func TestQueryPaymentMissingAndOtherMerchantAreSame(t *testing.T) {
	p, _, orders, _, _ := newPayTest(t, "merchant-demo")
	created, err := p.CreatePayment(context.Background(), payInput(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	other, errOther := p.QueryPayment(context.Background(), &QueryInput{
		MerchantID: "M100001", PayNo: created.PayNo,
	})
	missing, errMissing := p.QueryPayment(context.Background(), &QueryInput{
		MerchantID: created.MerchantID, PayNo: "PA999",
	})
	assertErrno(t, errOther, errno.ErrOrderNotFound)
	assertErrno(t, errMissing, errno.ErrOrderNotFound)
	if errOther.Error() != errMissing.Error() || other.PayNo != "" || missing.PayNo != "" {
		t.Fatalf("errors differ other=%v %q missing=%v %q", errOther, other.PayNo, errMissing, missing.PayNo)
	}
	for _, in := range []*QueryInput{
		{MerchantID: "", PayNo: created.PayNo},
		{MerchantID: "m100002", PayNo: created.PayNo},
		{MerchantID: created.MerchantID + " ", PayNo: created.PayNo},
		{MerchantID: created.MerchantID, PayNo: created.PayNo + " "},
		{MerchantID: "M100001", OutTradeNo: created.OutTradeNo},
		{MerchantID: created.MerchantID, OutTradeNo: created.OutTradeNo + " "},
		{MerchantID: created.MerchantID, OutTradeNo: "NO-SUCH"},
	} {
		got, qerr := p.QueryPayment(context.Background(), in)
		assertErrno(t, qerr, errno.ErrOrderNotFound)
		if got.PayNo != "" || qerr.Error() != errMissing.Error() {
			t.Fatalf("in=%+v got=%+v err=%v", in, got, qerr)
		}
	}
	if orders.rows[0].Status != payStatusInit || orders.rows[0].Version != created.Version || orders.rows[0].Amount != 10000 {
		t.Fatalf("lookup changed the row: %+v", orders.rows[0])
	}
}

func TestQueryPaymentStoreErrorAndCanceledContext(t *testing.T) {
	p, _, orders, _, _ := newPayTest(t, "merchant-demo")
	created, err := p.CreatePayment(context.Background(), payInput(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	payReads, outReads := orders.queryReads()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = p.QueryPayment(ctx, &QueryInput{MerchantID: created.MerchantID, PayNo: created.PayNo}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	if gotPay, gotOut := orders.queryReads(); gotPay != payReads || gotOut != outReads {
		t.Fatalf("canceled query read the store pay=%d out=%d", gotPay, gotOut)
	}
	orders.readErr = errors.New("db down")
	_, err = p.QueryPayment(context.Background(), &QueryInput{MerchantID: created.MerchantID, PayNo: created.PayNo})
	assertErrno(t, err, errno.ErrInternal)
	_, err = p.QueryPayment(context.Background(), &QueryInput{MerchantID: created.MerchantID, OutTradeNo: created.OutTradeNo})
	assertErrno(t, err, errno.ErrInternal)
	if orders.rows[0].Status != payStatusInit || orders.rows[0].Version != created.Version {
		t.Fatalf("read error changed the row: %+v", orders.rows[0])
	}
}
