package notify

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/service/payment/biz"
)

type memRecorder struct {
	in  biz.NotifyRecordInput
	n   int
	err error
}

func (m *memRecorder) RecordNotify(_ context.Context, in biz.NotifyRecordInput) (biz.NotifyRecordResult, error) {
	m.n++
	m.in = in
	if m.err != nil {
		return biz.NotifyRecordResult{}, m.err
	}
	return biz.NotifyRecordResult{NotifyNo: "NT0000000000000000001"}, nil
}

func TestBuildConfigKeepsProductionHooksOff(t *testing.T) {
	rec := &memRecorder{}
	cfg, err := buildConfig(Config{Brokers: []string{"127.0.0.1:9092"}, Recorder: rec, SessionTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BeforeCommit != nil || cfg.Group != GroupName || cfg.SessionTimeout != time.Second || len(cfg.Topics) != 5 {
		t.Fatalf("cfg=%+v", cfg)
	}
	want := map[string]bool{
		TopicPaymentSucceeded:  true,
		TopicRefundSucceeded:   true,
		TopicRefundFailed:      true,
		TopicTransferSucceeded: true,
		TopicTransferFailed:    true,
	}
	for _, topic := range cfg.Topics {
		if !want[topic] {
			t.Fatalf("unexpected topic %s", topic)
		}
		delete(want, topic)
	}
	if len(want) != 0 {
		t.Fatalf("missing topics %v", want)
	}
	if _, err = buildConfig(Config{}); err == nil || !strings.Contains(err.Error(), "no notify recorder") {
		t.Fatalf("nil recorder err=%v", err)
	}
	if _, err = Open(Config{Recorder: rec}); !errors.Is(err, mq.ErrNoBroker) {
		t.Fatalf("empty brokers err=%v", err)
	}
}

func TestHandleRejectsBadEnvelope(t *testing.T) {
	paid := "2026-11-10T10:00:00.123+08:00"
	base := `{"event_id":"EV0000000000000000000000000A","event_type":"payment.succeeded","data":{"pay_no":"PA0000000000000000001","merchant_id":"M100001","out_trade_no":"T1","channel":"mockpay","amount":10000,"fee_amount":60,"paid_at":"` + paid + `"}}`
	cases := []mq.Record{
		{Topic: TopicPaymentSucceeded, Value: []byte(`{`)},
		{Topic: TopicPaymentSucceeded, Value: []byte(base + `{}`)},
		{Topic: TopicPaymentSucceeded, Value: []byte(`{"event_id":"EV0000000000000000000000000A","event_type":"payment.succeeded","data":{"pay_no":"PA0000000000000000001","merchant_id":"M100001","out_trade_no":"T1","amount":"10000","fee_amount":60,"paid_at":"` + paid + `"}}`)},
		{Topic: TopicPaymentSucceeded, Value: []byte(`{"event_id":"EV0000000000000000000000000A","event_type":"payment.succeeded","data":{"pay_no":"PA0000000000000000001","merchant_id":"M100001","out_trade_no":"T1","amount":10000.5,"fee_amount":60,"paid_at":"` + paid + `"}}`)},
		{Topic: TopicRefundSucceeded, Value: []byte(base)},
		{Topic: TopicPaymentSucceeded, EventID: "EV-other", Value: []byte(base)},
		{Topic: TopicPaymentSucceeded, Value: []byte(`{"event_id":"EV0000000000000000000000000A","event_type":"payment.succeeded","data":{"merchant_id":"M100001","amount":10000}}`)},
	}
	for i, rec := range cases {
		err := handle(context.Background(), &memRecorder{}, rec)
		if !mq.IsPermanent(err) {
			t.Fatalf("case %d err=%v", i, err)
		}
	}
}

func TestHandleClassifiesRecorderErrors(t *testing.T) {
	raw := paymentEnvelope()
	rec := mq.Record{Topic: TopicPaymentSucceeded, EventID: "EV0000000000000000000000000A", EventType: eventPaymentSucceeded, Value: raw}
	permanent := []error{errno.ErrNotFound, errno.ErrMerchantInvalid, errno.ErrInvalidParam}
	for _, want := range permanent {
		err := handle(context.Background(), &memRecorder{err: want}, rec)
		if !mq.IsPermanent(err) || !errors.Is(err, want) {
			t.Fatalf("err=%v want permanent %v", err, want)
		}
	}
	retryable := []error{errno.ErrInternal, errno.ErrUnavailable, context.DeadlineExceeded}
	for _, want := range retryable {
		err := handle(context.Background(), &memRecorder{err: want}, rec)
		if err == nil || mq.IsPermanent(err) || !errors.Is(err, want) {
			t.Fatalf("err=%v want retryable %v", err, want)
		}
	}
}

func TestHandleDecodesFiveEvents(t *testing.T) {
	cases := []struct {
		topic string
		raw   []byte
		check func(biz.NotifyRecordInput) bool
	}{
		{TopicPaymentSucceeded, paymentEnvelope(), func(in biz.NotifyRecordInput) bool {
			return in.EventType == eventPaymentSucceeded && in.PayNo == "PA0000000000000000001" && in.Amount == money.Amount(10000) && in.FeeAmount == 60 && in.PaidAt == "2026-11-10T10:00:00.123+08:00"
		}},
		{TopicRefundSucceeded, refundEnvelope(false), func(in biz.NotifyRecordInput) bool {
			return in.EventType == eventRefundSucceeded && in.RefundNo == "RF0000000000000000001" && in.SucceededAt == "2026-11-10T10:00:01.000+08:00" && in.FailCode == ""
		}},
		{TopicRefundFailed, refundEnvelope(true), func(in biz.NotifyRecordInput) bool {
			return in.EventType == eventRefundFailed && in.FailCode == "CHANNEL_REJECTED" && in.SucceededAt == ""
		}},
		{TopicTransferSucceeded, transferEnvelope(false), func(in biz.NotifyRecordInput) bool {
			return in.EventType == eventTransferSucceeded && in.MerchantID == "M100001" && in.PayeeMerchantID == "M100002" && in.FinishedAt == "2026-11-10T10:00:02.000+08:00" && in.FailCode == ""
		}},
		{TopicTransferFailed, transferEnvelope(true), func(in biz.NotifyRecordInput) bool {
			return in.EventType == eventTransferFailed && in.FailCode == "INSUFFICIENT_BALANCE" && in.FinishedAt == "2026-11-10T10:00:02.000+08:00"
		}},
	}
	for i, tc := range cases {
		mem := &memRecorder{}
		err := handle(context.Background(), mem, mq.Record{Topic: tc.topic, Value: tc.raw})
		if err != nil || mem.n != 1 || !tc.check(mem.in) {
			t.Fatalf("case %d err=%v n=%d in=%+v", i, err, mem.n, mem.in)
		}
	}
}

func paymentEnvelope() []byte {
	return []byte(`{"event_id":"EV0000000000000000000000000A","event_type":"payment.succeeded","version":1,"occurred_at":"2026-11-10T10:00:00.123+08:00","trace_context":{},"data":{"pay_no":"PA0000000000000000001","merchant_id":"M100001","out_trade_no":"T1","channel":"mockpay","amount":10000,"fee_amount":60,"paid_at":"2026-11-10T10:00:00.123+08:00"}}`)
}

func refundEnvelope(failed bool) []byte {
	if failed {
		return []byte(`{"event_id":"EV0000000000000000000000000B","event_type":"refund.failed","data":{"refund_no":"RF0000000000000000001","pay_no":"PA0000000000000000001","merchant_id":"M100001","out_refund_no":"R1","amount":1000,"fail_code":"CHANNEL_REJECTED"}}`)
	}
	return []byte(`{"event_id":"EV0000000000000000000000000C","event_type":"refund.succeeded","data":{"refund_no":"RF0000000000000000001","pay_no":"PA0000000000000000001","merchant_id":"M100001","out_refund_no":"R1","channel":"mockpay","amount":1000,"succeeded_at":"2026-11-10T10:00:01.000+08:00"}}`)
}

func transferEnvelope(failed bool) []byte {
	if failed {
		return []byte(`{"event_id":"EV0000000000000000000000000D","event_type":"transfer.failed","data":{"transfer_no":"TF0000000000000000001","out_transfer_no":"TR1","merchant_id":"M100001","payee_merchant_id":"M100002","amount":500,"mode":"TCC","status":"FAILED","fail_code":"INSUFFICIENT_BALANCE","finished_at":"2026-11-10T10:00:02.000+08:00"}}`)
	}
	return []byte(`{"event_id":"EV0000000000000000000000000E","event_type":"transfer.succeeded","data":{"transfer_no":"TF0000000000000000001","out_transfer_no":"TR1","merchant_id":"M100001","payee_merchant_id":"M100002","amount":500,"mode":"TCC","status":"SUCCESS","finished_at":"2026-11-10T10:00:02.000+08:00"}}`)
}
