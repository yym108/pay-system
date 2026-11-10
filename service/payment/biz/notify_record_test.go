package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
)

func TestRecordNotifyRejectsBeforeLookup(t *testing.T) {
	var nilP *Payments
	if _, err := nilP.RecordNotify(context.Background(), NotifyRecordInput{}); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil payments err=%v", err)
	}
	p := &Payments{}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.RecordNotify(canceled, paymentNotifyInput()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err=%v", err)
	}
	if _, err := p.RecordNotify(context.Background(), NotifyRecordInput{}); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("empty err=%v", err)
	}
	clk := notifyTestClock()
	p.clk = clk
	in := paymentNotifyInput()
	in.EventType = "payment.failed"
	if _, err := p.RecordNotify(context.Background(), in); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("bad type err=%v", err)
	}
	p.clk = nil
	if _, err := p.RecordNotify(context.Background(), paymentNotifyInput()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil clock err=%v", err)
	}
}

func TestRecordNotifyOrderURLSkipsMerchant(t *testing.T) {
	orders := &memOrders{}
	orders.rows = append(orders.rows, PaymentOrder{
		PayNo: "PA0000000000000000001", MerchantID: "M100001", NotifyURL: " https://pay.example/n ",
	})
	merchants := &fakeMerchants{cfg: MerchantConfig{MerchantID: "M100001", Status: "DISABLED", DefaultNotifyURL: "https://default.example/n"}}
	p := newPayments(merchants, &stubIDs{prefix: notifyNoPrefix, err: idgen.ErrClockRollback}, notifyTestClock(), "", orders)
	_, err := p.RecordNotify(context.Background(), paymentNotifyInput())
	if !errors.Is(err, errno.ErrUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if merchants.configCalls != 0 {
		t.Fatalf("merchant calls=%d", merchants.configCalls)
	}
}

func TestRecordNotifyMissingAndMismatch(t *testing.T) {
	orders := &memOrders{}
	orders.rows = append(orders.rows, PaymentOrder{PayNo: "PA0000000000000000001", MerchantID: "M100009", NotifyURL: "https://pay.example/n"})
	p := newPayments(&fakeMerchants{}, &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000001"}}, notifyTestClock(), "", orders)
	_, err := p.RecordNotify(context.Background(), paymentNotifyInput())
	if !errors.Is(err, errno.ErrNotFound) {
		t.Fatalf("merchant mismatch err=%v", err)
	}
	orders.rows[0].MerchantID = "M100001"
	orders.readErr = errors.New("db down")
	_, err = p.RecordNotify(context.Background(), paymentNotifyInput())
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("read err=%v", err)
	}
	orders.readErr = nil
	in := paymentNotifyInput()
	in.PayNo = "PA0000000000000000099"
	_, err = p.RecordNotify(context.Background(), in)
	if !errors.Is(err, errno.ErrNotFound) {
		t.Fatalf("missing err=%v", err)
	}
}

func TestRecordNotifyRefundAndTransferLookup(t *testing.T) {
	orders := &memOrders{}
	orders.rows = append(orders.rows, PaymentOrder{PayNo: "PA0000000000000000001", MerchantID: "M100001", NotifyURL: "https://pay.example/n"})
	orders.refunds = append(orders.refunds, RefundOrder{RefundNo: "RF0000000000000000001", PayNo: "PA0000000000000000001", MerchantID: "M100001"})
	merchants := &fakeMerchants{cfg: MerchantConfig{MerchantID: "M100001", Status: "DISABLED", DefaultNotifyURL: "https://default.example/n"}}
	p := newPayments(merchants, &stubIDs{prefix: notifyNoPrefix, err: idgen.ErrClockRollback}, notifyTestClock(), "", orders)
	_, err := p.RecordNotify(context.Background(), refundNotifyInput(false))
	if !errors.Is(err, errno.ErrUnavailable) || merchants.configCalls != 0 {
		t.Fatalf("refund err=%v calls=%d", err, merchants.configCalls)
	}
	orders.refunds[0].PayNo = "PA0000000000000000099"
	_, err = p.RecordNotify(context.Background(), refundNotifyInput(false))
	if !errors.Is(err, errno.ErrNotFound) {
		t.Fatalf("refund mismatch err=%v", err)
	}

	transfers := &memTransfers{}
	transfers.rows = append(transfers.rows, TransferOrder{
		TransferNo: "TF0000000000000000001", MerchantID: "M100001", PayeeMerchantID: "M100002", NotifyURL: "",
	})
	p.transfers = transfers
	orders.rows[0].NotifyURL = ""
	_, err = p.RecordNotify(context.Background(), transferNotifyInput(false))
	if !errors.Is(err, errno.ErrUnavailable) || merchants.configCalls != 1 {
		t.Fatalf("transfer default err=%v calls=%d", err, merchants.configCalls)
	}
	transfers.rows[0].MerchantID = "M100009"
	_, err = p.RecordNotify(context.Background(), transferNotifyInput(false))
	if !errors.Is(err, errno.ErrNotFound) {
		t.Fatalf("transfer mismatch err=%v", err)
	}
}

func TestRecordNotifyEmptyMerchantAndLongURL(t *testing.T) {
	orders := &memOrders{}
	orders.rows = append(orders.rows, PaymentOrder{PayNo: "PA0000000000000000001", MerchantID: "M100001", NotifyURL: ""})
	merchants := &fakeMerchants{cfg: MerchantConfig{MerchantID: "M100099", DefaultNotifyURL: "https://default.example/n"}}
	p := newPayments(merchants, &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000001"}}, notifyTestClock(), "", orders)
	_, err := p.RecordNotify(context.Background(), paymentNotifyInput())
	if !errors.Is(err, errno.ErrMerchantInvalid) {
		t.Fatalf("missing merchant err=%v", err)
	}
	orders.rows[0].NotifyURL = strings.Repeat("a", maxNotifyURLRunes+1)
	_, err = p.RecordNotify(context.Background(), paymentNotifyInput())
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("long url err=%v", err)
	}
	p.orders = nil
	_, err = p.RecordNotify(context.Background(), paymentNotifyInput())
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil orders err=%v", err)
	}
	p.orders = orders
	p.transfers = nil
	_, err = p.RecordNotify(context.Background(), transferNotifyInput(true))
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil transfers err=%v", err)
	}
}

func TestNotifyFromDupBytes(t *testing.T) {
	ok, err := notifyFromDupRow("EV1", "EV1", "NT0000000000000000001")
	if err != nil || ok.NotifyNo != "NT0000000000000000001" {
		t.Fatalf("same=%+v err=%v", ok, err)
	}
	if _, err = notifyFromDupRow("EV1", "ev1", "NT0000000000000000001"); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("case mismatch err=%v", err)
	}
}

func notifyTestClock() *clock.Fake {
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 11, 10, 10, 0, 0, 123000000, notifyShanghai))
	return clk
}

func paymentNotifyInput() NotifyRecordInput {
	return NotifyRecordInput{
		EventID: "EV0000000000000000000000000A", EventType: notifyEventPaymentSucceeded,
		MerchantID: "M100001", PayNo: "PA0000000000000000001", OutTradeNo: "T1",
		Amount: 10000, FeeAmount: 60, PaidAt: "2026-11-10T10:00:00.123+08:00",
	}
}

func refundNotifyInput(failed bool) NotifyRecordInput {
	in := NotifyRecordInput{
		EventID: "EV0000000000000000000000000C", EventType: notifyEventRefundSucceeded,
		MerchantID: "M100001", PayNo: "PA0000000000000000001", RefundNo: "RF0000000000000000001",
		OutRefundNo: "R1", Amount: 1000, SucceededAt: "2026-11-10T10:00:01.000+08:00",
	}
	if failed {
		in.EventType = notifyEventRefundFailed
		in.SucceededAt = ""
		in.FailCode = "CHANNEL_REJECTED"
	}
	return in
}

func transferNotifyInput(failed bool) NotifyRecordInput {
	in := NotifyRecordInput{
		EventID: "EV0000000000000000000000000E", EventType: notifyEventTransferSucceeded,
		MerchantID: "M100001", TransferNo: "TF0000000000000000001", OutTransferNo: "TR1",
		PayeeMerchantID: "M100002", Amount: 500, FinishedAt: "2026-11-10T10:00:02.000+08:00",
	}
	if failed {
		in.EventType = notifyEventTransferFailed
		in.FailCode = "INSUFFICIENT_BALANCE"
	}
	return in
}
