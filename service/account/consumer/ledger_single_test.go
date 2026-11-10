package consumer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/service/account/biz"
)

type fakeLedger struct {
	postCalls int
	postErr   error
	dup       bool
	amount    money.Amount
	fee       money.Amount
	confirm   int
	release   int
	relAmount money.Amount
}

func (f *fakeLedger) PostPay(_ context.Context, _, _, _ string, amount, fee money.Amount) (biz.PostResult, error) {
	f.postCalls++
	f.amount = amount
	f.fee = fee
	if f.postErr != nil {
		return biz.PostResult{}, f.postErr
	}
	return biz.PostResult{TxnNo: "TX1", Duplicated: f.dup}, nil
}

func (f *fakeLedger) ConfirmFreeze(context.Context, string, string, string, string) (biz.ConfirmResult, error) {
	f.confirm++
	if f.postErr != nil {
		return biz.ConfirmResult{}, f.postErr
	}
	return biz.ConfirmResult{FreezeNo: "FZ1"}, nil
}

func (f *fakeLedger) ReleaseFreeze(_ context.Context, _, _, _ string, amount money.Amount) (biz.FreezeResult, error) {
	f.release++
	f.relAmount = amount
	if f.postErr != nil {
		return biz.FreezeResult{}, f.postErr
	}
	return biz.FreezeResult{FreezeNo: "FZ1", Status: "RELEASED"}, nil
}

func payBody(fee string) []byte {
	return []byte(`{"event_id":"EV1","event_type":"payment.succeeded","data":{` +
		`"pay_no":"PA1","merchant_id":"M100002","out_trade_no":"OT-SECRET-NO","channel":"mockpay",` +
		`"amount":10000,"fee_amount":` + fee + `,"paid_at":"2026-11-10T00:00:00.000+08:00"}}`)
}

func payRec(body []byte) mq.Record {
	return mq.Record{
		Topic:     TopicPaymentSucceeded,
		Value:     body,
		EventID:   "EV1",
		EventType: eventPaymentSucceeded,
	}
}

func TestBuildConfigLeavesBeforeCommitNil(t *testing.T) {
	cfg, err := buildConfig(Config{Brokers: []string{"127.0.0.1:1"}, Ledger: &fakeLedger{}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BeforeCommit != nil || cfg.Group != GroupName || len(cfg.Topics) != 3 {
		t.Fatalf("cfg = %+v", cfg)
	}
	for _, topic := range cfg.Topics {
		if topic == "pay.transfer.succeeded" || topic == "pay.transfer.failed" {
			t.Fatalf("subscribed %s", topic)
		}
	}
}

func TestOpenRejectsNilLedger(t *testing.T) {
	if _, err := Open(Config{Brokers: []string{"127.0.0.1:1"}}); err == nil {
		t.Fatal("expected error")
	}
}

func TestHandleParseAndFeePermanent(t *testing.T) {
	ledger := &fakeLedger{}
	cases := []mq.Record{
		{Topic: TopicPaymentSucceeded, Value: []byte("{"), EventType: eventPaymentSucceeded},
		payRec([]byte(`{"event_id":"EV1","event_type":"refund.succeeded","data":{}}`)),
		payRec(payBody(`"60"`)),
		payRec(payBody(`10001`)),
		payRec(payBody(`-1`)),
		{Topic: TopicPaymentSucceeded, Value: payBody(`0`), EventID: "OTHER", EventType: eventPaymentSucceeded},
	}
	for i, rec := range cases {
		err := handle(context.Background(), ledger, rec)
		if !mq.IsPermanent(err) {
			t.Fatalf("case %d err = %v", i, err)
		}
	}
	if ledger.postCalls != 0 {
		t.Fatalf("posted %d", ledger.postCalls)
	}
}

func TestHandleInactiveAndMissingPermanent(t *testing.T) {
	logs := &bytes.Buffer{}
	logx.SetDefault("account", logs)
	t.Cleanup(func() { logx.SetDefault("account", io.Discard) })
	ledger := &fakeLedger{postErr: errno.ErrAccountInactive}
	err := handle(context.Background(), ledger, payRec(payBody(`60`)))
	if !mq.IsPermanent(err) || !errors.Is(err, errno.ErrAccountInactive) {
		t.Fatal(err)
	}
	text := logs.String()
	if !bytes.Contains(logs.Bytes(), []byte("ledger account inactive")) ||
		!bytes.Contains(logs.Bytes(), []byte(`"event_id":"EV1"`)) ||
		!bytes.Contains(logs.Bytes(), []byte(`"event_type":"payment.succeeded"`)) ||
		!bytes.Contains(logs.Bytes(), []byte(`"merchant_id":"M100002"`)) ||
		bytes.Contains(logs.Bytes(), []byte("OT-SECRET-NO")) {
		t.Fatalf("logs = %s", text)
	}
	logs.Reset()
	ledger.postErr = errno.ErrAccountNotFound
	err = handle(context.Background(), ledger, payRec(payBody(`60`)))
	if !mq.IsPermanent(err) || !bytes.Contains(logs.Bytes(), []byte("ledger account missing")) {
		t.Fatalf("err=%v logs=%s", err, logs.String())
	}
}

func TestHandleInternalRetries(t *testing.T) {
	ledger := &fakeLedger{postErr: errno.ErrInternal}
	err := handle(context.Background(), ledger, payRec(payBody(`60`)))
	if err == nil || mq.IsPermanent(err) || !errors.Is(err, errno.ErrInternal) {
		t.Fatal(err)
	}
}

func TestHandleDuplicatePoints(t *testing.T) {
	var hit atomic.Bool
	if err := fault.Init("ledger.after_commit", fault.WithKill(func() { hit.Store(true) })); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Init("") })
	ledger := &fakeLedger{dup: true}
	if err := handle(context.Background(), ledger, payRec(payBody(`0`))); err != nil {
		t.Fatal(err)
	}
	if !hit.Load() || ledger.postCalls != 1 || ledger.fee != 0 || ledger.amount != 10000 {
		t.Fatalf("hit=%v calls=%d fee=%d", hit.Load(), ledger.postCalls, ledger.fee)
	}
}

func TestHandleFreezeConflictCounts(t *testing.T) {
	logs := &bytes.Buffer{}
	logx.SetDefault("account", logs)
	t.Cleanup(func() { logx.SetDefault("account", io.Discard) })
	before := FreezeConflictTotal()
	ledger := &fakeLedger{postErr: errno.ErrFreezeStatusConflict}
	body := []byte(`{"event_id":"EV9","event_type":"refund.succeeded","data":{` +
		`"refund_no":"RF1","pay_no":"PA1","merchant_id":"M100002","out_refund_no":"R1",` +
		`"channel":"mockpay","amount":3000,"succeeded_at":"2026-11-10T00:00:00.000+08:00"}}`)
	err := handle(context.Background(), ledger, mq.Record{
		Topic: TopicRefundSucceeded, Value: body, EventID: "EV9", EventType: eventRefundSucceeded,
	})
	if !mq.IsPermanent(err) || FreezeConflictTotal() != before+1 || ledger.confirm != 1 {
		t.Fatalf("err=%v count=%d calls=%d", err, FreezeConflictTotal(), ledger.confirm)
	}
	if !bytes.Contains(logs.Bytes(), []byte("ledger freeze conflict")) ||
		bytes.Contains(logs.Bytes(), []byte("RF1")) {
		t.Fatalf("logs = %s", logs.String())
	}
	logs.Reset()
	failed := []byte(`{"event_id":"EV8","event_type":"refund.failed","data":{` +
		`"refund_no":"RF2","pay_no":"PA1","merchant_id":"M100002","out_refund_no":"R2",` +
		`"amount":3000,"fail_code":"CHANNEL_FAILED"}}`)
	err = handle(context.Background(), ledger, mq.Record{
		Topic: TopicRefundFailed, Value: failed, EventID: "EV8", EventType: eventRefundFailed,
	})
	if !mq.IsPermanent(err) || FreezeConflictTotal() != before+2 || ledger.release != 1 || ledger.relAmount != 3000 {
		t.Fatalf("err=%v count=%d release=%d amount=%d", err, FreezeConflictTotal(), ledger.release, ledger.relAmount)
	}
}

func TestHandleRefundInternalRetries(t *testing.T) {
	before := FreezeConflictTotal()
	ledger := &fakeLedger{postErr: errno.ErrInternal}
	body := []byte(`{"event_id":"EV7","event_type":"refund.failed","data":{` +
		`"refund_no":"RF3","pay_no":"PA1","merchant_id":"M100002","out_refund_no":"R3",` +
		`"amount":1,"fail_code":"CHANNEL_FAILED"}}`)
	err := handle(context.Background(), ledger, mq.Record{
		Topic: TopicRefundFailed, Value: body, EventID: "EV7", EventType: eventRefundFailed,
	})
	if err == nil || mq.IsPermanent(err) || FreezeConflictTotal() != before {
		t.Fatal(err)
	}
}

func TestHandleRefundInactivePermanent(t *testing.T) {
	// MQ-04：退款确认的 40005 不可重试，不计 pay_ledger_freeze_conflict_total。
	logs := &bytes.Buffer{}
	logx.SetDefault("account", logs)
	t.Cleanup(func() { logx.SetDefault("account", io.Discard) })
	before := FreezeConflictTotal()
	ledger := &fakeLedger{postErr: errno.ErrAccountInactive}
	body := []byte(`{"event_id":"EV-OFFCH","event_type":"refund.succeeded","data":{` +
		`"refund_no":"RF-OFFCH","pay_no":"PA1","merchant_id":"M100002","out_refund_no":"R-OFFCH",` +
		`"channel":"mockpay","amount":800,"succeeded_at":"2026-11-10T00:00:00.000+08:00"}}`)
	err := handle(context.Background(), ledger, mq.Record{
		Topic: TopicRefundSucceeded, Value: body, EventID: "EV-OFFCH", EventType: eventRefundSucceeded,
	})
	if !mq.IsPermanent(err) || FreezeConflictTotal() != before || ledger.confirm != 1 {
		t.Fatalf("err=%v count=%d calls=%d", err, FreezeConflictTotal(), ledger.confirm)
	}
	if !bytes.Contains(logs.Bytes(), []byte("ledger account inactive")) ||
		!bytes.Contains(logs.Bytes(), []byte(`"event_id":"EV-OFFCH"`)) ||
		!bytes.Contains(logs.Bytes(), []byte(`"event_type":"refund.succeeded"`)) ||
		!bytes.Contains(logs.Bytes(), []byte(`"merchant_id":"M100002"`)) ||
		bytes.Contains(logs.Bytes(), []byte("RF-OFFCH")) ||
		bytes.Contains(logs.Bytes(), []byte("pay_mq_dlq_total")) {
		t.Fatalf("logs = %s", logs.String())
	}
}

func TestHandleRefundMissingAccountRetries(t *testing.T) {
	// MQ-04：退款路径上的 40001 继续重试。平台子账户不存在不能进 DLQ。
	before := FreezeConflictTotal()
	ledger := &fakeLedger{postErr: errno.ErrAccountNotFound}
	body := []byte(`{"event_id":"EV-NOSUB","event_type":"refund.succeeded","data":{` +
		`"refund_no":"RF-NOSUB","pay_no":"PA1","merchant_id":"M100002","out_refund_no":"R-NOSUB",` +
		`"channel":"mockpay","amount":800,"succeeded_at":"2026-11-10T00:00:00.000+08:00"}}`)
	err := handle(context.Background(), ledger, mq.Record{
		Topic: TopicRefundSucceeded, Value: body, EventID: "EV-NOSUB", EventType: eventRefundSucceeded,
	})
	if err == nil || mq.IsPermanent(err) || FreezeConflictTotal() != before || ledger.confirm != 1 {
		t.Fatalf("err=%v permanent=%v count=%d", err, mq.IsPermanent(err), FreezeConflictTotal())
	}
}
