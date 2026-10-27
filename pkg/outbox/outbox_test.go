package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

var testZone = time.FixedZone("Asia/Shanghai", 8*60*60)

const (
	wantOccurred = "2026-10-12T10:00:01.300+08:00"
	wantPaidAt   = "2026-10-12T10:00:03.300+08:00"
	wantRefundAt = "2026-10-12T10:00:04.300+08:00"
	wantFinishAt = "2026-10-12T10:00:05.300+08:00"

	traceHex = "0af7651916cd43dd8448eb211c80319c"
	spanHex  = "b7ad6b7169203331"
)

var eventIDRE = regexp.MustCompile(`^EV[0-9]{19}$`)

func clockAt() (time.Time, *clock.Fake) {
	at := time.Date(2026, 10, 12, 10, 0, 1, 300*int(time.Millisecond), testZone)
	clk := &clock.Fake{}
	clk.Set(at)
	return at, clk
}

func mustGen(t *testing.T, clk clock.Clock) *idgen.Generator {
	t.Helper()
	g, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func mustPrepare(t *testing.T, evt Event) prepared {
	t.Helper()
	at, clk := clockAt()
	row, err := prepare(context.Background(), evt, mustGen(t, clk), clk)
	if err != nil {
		t.Fatal(err)
	}
	if !row.at.Equal(at) {
		t.Fatalf("clock = %s", row.at)
	}
	return row
}

func validPayment() Event {
	return Event{
		Type:       "payment.succeeded",
		PayNo:      "PA0000000123456789012",
		MerchantID: "M100001",
		OutTradeNo: "T20261012001",
		Channel:    "mockpay",
		Amount:     10000,
		FeeAmount:  60,
		PaidAt:     time.Date(2026, 10, 12, 10, 0, 3, 300*int(time.Millisecond), testZone),
	}
}

func validRefundSucceeded() Event {
	return Event{
		Type:        "refund.succeeded",
		RefundNo:    "RF0000000123456789012",
		PayNo:       "PA0000000123456789012",
		MerchantID:  "M100001",
		OutRefundNo: "R20261012001",
		Channel:     "mockpay",
		Amount:      3000,
		SucceededAt: time.Date(2026, 10, 12, 10, 0, 4, 300*int(time.Millisecond), testZone),
	}
}

func validRefundFailed() Event {
	return Event{
		Type:        "refund.failed",
		RefundNo:    "RF0000000123456789012",
		PayNo:       "PA0000000123456789012",
		MerchantID:  "M100001",
		OutRefundNo: "R20261012001",
		Amount:      3000,
		FailCode:    "CHANNEL_FAILED",
	}
}

func validTransferSucceeded() Event {
	return Event{
		Type:            "transfer.succeeded",
		TransferNo:      "TF0000000123456789012",
		OutTransferNo:   "TR20261012001",
		MerchantID:      "M100001",
		PayeeMerchantID: "M100002",
		Amount:          800,
		Mode:            "LOCAL",
		Status:          "SUCCESS",
		FinishedAt:      time.Date(2026, 10, 12, 10, 0, 5, 300*int(time.Millisecond), testZone),
	}
}

func validTransferFailed() Event {
	evt := validTransferSucceeded()
	evt.Type = "transfer.failed"
	evt.Status = "FAILED"
	evt.FailCode = "INSUFFICIENT_BALANCE"
	return evt
}

func payloadMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if dec.More() {
		t.Fatal("payload has trailing data")
	}
	return m
}

func dataMap(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("data = %#v", payload["data"])
	}
	return data
}

func assertNumber(t *testing.T, m map[string]any, key, want string) {
	t.Helper()
	n, ok := m[key].(json.Number)
	if !ok || n.String() != want || bytes.Contains([]byte(want), []byte{'.'}) {
		t.Fatalf("%s = %#v, want integer %s", key, m[key], want)
	}
}

func assertString(t *testing.T, m map[string]any, key, want string) {
	t.Helper()
	got, ok := m[key].(string)
	if !ok || got != want {
		t.Fatalf("%s = %#v, want %s", key, m[key], want)
	}
}

func assertData(t *testing.T, payload map[string]any, want map[string]string, numbers map[string]string) {
	t.Helper()
	data := dataMap(t, payload)
	if len(data) != len(want)+len(numbers) {
		t.Fatalf("data keys = %d (%v), want %d", len(data), data, len(want)+len(numbers))
	}
	for k, v := range want {
		assertString(t, data, k, v)
	}
	for k, v := range numbers {
		assertNumber(t, data, k, v)
	}
}

// MQ-01：五种事件的 topic、msg_key、biz_no，以及信封里的整数金额。
func TestEventRouting(t *testing.T) {
	cases := []struct {
		name    string
		evt     Event
		topic   string
		msgKey  string
		bizNo   string
		text    map[string]string
		numbers map[string]string
	}{
		{
			name: "payment.succeeded", evt: validPayment(),
			topic: "pay.payment.succeeded", msgKey: "PA0000000123456789012", bizNo: "PA0000000123456789012",
			text: map[string]string{
				"pay_no": "PA0000000123456789012", "merchant_id": "M100001",
				"out_trade_no": "T20261012001", "channel": "mockpay", "paid_at": wantPaidAt,
			},
			numbers: map[string]string{"amount": "10000", "fee_amount": "60"},
		},
		{
			name: "refund.succeeded", evt: validRefundSucceeded(),
			topic: "pay.refund.succeeded", msgKey: "PA0000000123456789012", bizNo: "RF0000000123456789012",
			text: map[string]string{
				"refund_no": "RF0000000123456789012", "pay_no": "PA0000000123456789012",
				"merchant_id": "M100001", "out_refund_no": "R20261012001",
				"channel": "mockpay", "succeeded_at": wantRefundAt,
			},
			numbers: map[string]string{"amount": "3000"},
		},
		{
			name: "refund.failed", evt: validRefundFailed(),
			topic: "pay.refund.failed", msgKey: "PA0000000123456789012", bizNo: "RF0000000123456789012",
			text: map[string]string{
				"refund_no": "RF0000000123456789012", "pay_no": "PA0000000123456789012",
				"merchant_id": "M100001", "out_refund_no": "R20261012001", "fail_code": "CHANNEL_FAILED",
			},
			numbers: map[string]string{"amount": "3000"},
		},
		{
			name: "transfer.succeeded", evt: validTransferSucceeded(),
			topic: "pay.transfer.succeeded", msgKey: "TF0000000123456789012", bizNo: "TF0000000123456789012",
			text: map[string]string{
				"transfer_no": "TF0000000123456789012", "out_transfer_no": "TR20261012001",
				"merchant_id": "M100001", "payee_merchant_id": "M100002",
				"mode": "LOCAL", "status": "SUCCESS", "finished_at": wantFinishAt,
			},
			numbers: map[string]string{"amount": "800"},
		},
		{
			name: "transfer.failed", evt: validTransferFailed(),
			topic: "pay.transfer.failed", msgKey: "TF0000000123456789012", bizNo: "TF0000000123456789012",
			text: map[string]string{
				"transfer_no": "TF0000000123456789012", "out_transfer_no": "TR20261012001",
				"merchant_id": "M100001", "payee_merchant_id": "M100002",
				"mode": "LOCAL", "status": "FAILED", "fail_code": "INSUFFICIENT_BALANCE",
				"finished_at": wantFinishAt,
			},
			numbers: map[string]string{"amount": "800"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := mustPrepare(t, tc.evt)
			if row.topic != tc.topic || row.msgKey != tc.msgKey || row.bizNo != tc.bizNo || row.eventType != tc.evt.Type {
				t.Fatalf("route = %s %s %s %s", row.topic, row.msgKey, row.bizNo, row.eventType)
			}
			if !eventIDRE.MatchString(row.eventID) {
				t.Fatalf("event_id = %s", row.eventID)
			}
			parsed, err := idgen.Parse(row.eventID)
			if err != nil || parsed.Prefix != "EV" {
				t.Fatalf("parse %s: %v %+v", row.eventID, err, parsed)
			}
			payload := payloadMap(t, row.payload)
			assertString(t, payload, "event_id", row.eventID)
			assertString(t, payload, "event_type", tc.evt.Type)
			assertString(t, payload, "occurred_at", wantOccurred)
			assertNumber(t, payload, "version", "1")
			trace, ok := payload["trace_context"].(map[string]any)
			if !ok || len(trace) != 0 {
				t.Fatalf("trace_context = %#v", payload["trace_context"])
			}
			if !bytes.Contains(row.payload, []byte(`"trace_context":{}`)) {
				t.Fatalf("payload = %s", row.payload)
			}
			assertData(t, payload, tc.text, tc.numbers)
		})
	}
}

// MQ-01：fee_amount 为 0 时仍以整数写入，不省略。
func TestFeeAmountZeroIsInteger(t *testing.T) {
	evt := validPayment()
	evt.FeeAmount = 0
	row := mustPrepare(t, evt)
	assertNumber(t, dataMap(t, payloadMap(t, row.payload)), "fee_amount", "0")
}

// MQ-01：没有有效 span 时 trace_context 为 {}。
func TestAddNoSpanTraceContextEmpty(t *testing.T) {
	tid, err := trace.TraceIDFromHex(traceHex)
	if err != nil {
		t.Fatal(err)
	}
	onlyTrace := trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid,
		Remote:  true,
	}))
	cases := []struct {
		name string
		ctx  context.Context
	}{
		{name: "background", ctx: context.Background()},
		{name: "trace id only", ctx: onlyTrace},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, clk := clockAt()
			row, err := prepare(tc.ctx, validPayment(), mustGen(t, clk), clk)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(row.payload, []byte(`"trace_context":{}`)) {
				t.Fatalf("payload = %s", row.payload)
			}
			traceMap, ok := payloadMap(t, row.payload)["trace_context"].(map[string]any)
			if !ok || len(traceMap) != 0 {
				t.Fatalf("trace_context = %#v", payloadMap(t, row.payload)["trace_context"])
			}
		})
	}
}

// MQ-01：有效 span 写成 W3C traceparent，不带其他上下文字段。
func TestAddSpanWritesTraceparent(t *testing.T) {
	cases := []struct {
		name  string
		flags trace.TraceFlags
		want  string
	}{
		{name: "sampled", flags: trace.FlagsSampled, want: "00-" + traceHex + "-" + spanHex + "-01"},
		{name: "unsampled", flags: 0, want: "00-" + traceHex + "-" + spanHex + "-00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := spanCtx(t, context.Background(), tc.flags)
			_, clk := clockAt()
			row, err := prepare(ctx, validPayment(), mustGen(t, clk), clk)
			if err != nil {
				t.Fatal(err)
			}
			traceMap, ok := payloadMap(t, row.payload)["trace_context"].(map[string]any)
			if !ok || len(traceMap) != 1 {
				t.Fatalf("trace_context = %#v", payloadMap(t, row.payload)["trace_context"])
			}
			assertString(t, traceMap, "traceparent", tc.want)
		})
	}
}

func spanCtx(t *testing.T, parent context.Context, flags trace.TraceFlags) context.Context {
	t.Helper()
	if parent == nil {
		parent = context.Background()
	}
	tid, err := trace.TraceIDFromHex(traceHex)
	if err != nil {
		t.Fatal(err)
	}
	sid, err := trace.SpanIDFromHex(spanHex)
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: flags, Remote: true,
	})
	return trace.ContextWithRemoteSpanContext(parent, sc)
}

// MQ-01：tx 为 nil 时返回错误，不插入。
func TestAddNilTx(t *testing.T) {
	_, clk := clockAt()
	err := Add(context.Background(), nil, validPayment(), mustGen(t, clk), clk)
	if !errors.Is(err, ErrNilTx) {
		t.Fatalf("err = %v", err)
	}
}

// MQ-01：缺字段或未知类型返回错误，不发号、不插行。
func TestAddMissingFieldDoesNotInsert(t *testing.T) {
	at, clk := clockAt()
	ids := mustGen(t, clk)
	if _, err := ids.NewNo(context.Background(), "EV"); err != nil {
		t.Fatal(err)
	}
	clk.Set(at.Add(-time.Hour))

	cases := []struct {
		name string
		base func() Event
		edit func(*Event)
	}{
		{"payment pay_no", validPayment, func(e *Event) { e.PayNo = "" }},
		{"payment merchant_id", validPayment, func(e *Event) { e.MerchantID = "" }},
		{"payment out_trade_no", validPayment, func(e *Event) { e.OutTradeNo = "" }},
		{"payment channel", validPayment, func(e *Event) { e.Channel = "" }},
		{"payment amount zero", validPayment, func(e *Event) { e.Amount = 0 }},
		{"payment amount negative", validPayment, func(e *Event) { e.Amount = -1 }},
		{"payment fee negative", validPayment, func(e *Event) { e.FeeAmount = -1 }},
		{"payment paid_at", validPayment, func(e *Event) { e.PaidAt = time.Time{} }},
		{"refund ok refund_no", validRefundSucceeded, func(e *Event) { e.RefundNo = "" }},
		{"refund ok pay_no", validRefundSucceeded, func(e *Event) { e.PayNo = "" }},
		{"refund ok merchant_id", validRefundSucceeded, func(e *Event) { e.MerchantID = "" }},
		{"refund ok out_refund_no", validRefundSucceeded, func(e *Event) { e.OutRefundNo = "" }},
		{"refund ok channel", validRefundSucceeded, func(e *Event) { e.Channel = "" }},
		{"refund ok amount", validRefundSucceeded, func(e *Event) { e.Amount = 0 }},
		{"refund ok succeeded_at", validRefundSucceeded, func(e *Event) { e.SucceededAt = time.Time{} }},
		{"refund fail refund_no", validRefundFailed, func(e *Event) { e.RefundNo = "" }},
		{"refund fail pay_no", validRefundFailed, func(e *Event) { e.PayNo = "" }},
		{"refund fail merchant_id", validRefundFailed, func(e *Event) { e.MerchantID = "" }},
		{"refund fail out_refund_no", validRefundFailed, func(e *Event) { e.OutRefundNo = "" }},
		{"refund fail amount", validRefundFailed, func(e *Event) { e.Amount = 0 }},
		{"refund fail fail_code", validRefundFailed, func(e *Event) { e.FailCode = "" }},
		{"transfer ok transfer_no", validTransferSucceeded, func(e *Event) { e.TransferNo = "" }},
		{"transfer ok out_transfer_no", validTransferSucceeded, func(e *Event) { e.OutTransferNo = "" }},
		{"transfer ok merchant_id", validTransferSucceeded, func(e *Event) { e.MerchantID = "" }},
		{"transfer ok payee", validTransferSucceeded, func(e *Event) { e.PayeeMerchantID = "" }},
		{"transfer ok amount", validTransferSucceeded, func(e *Event) { e.Amount = 0 }},
		{"transfer ok mode", validTransferSucceeded, func(e *Event) { e.Mode = "" }},
		{"transfer ok status empty", validTransferSucceeded, func(e *Event) { e.Status = "" }},
		{"transfer ok status failed", validTransferSucceeded, func(e *Event) { e.Status = "FAILED" }},
		{"transfer ok finished_at", validTransferSucceeded, func(e *Event) { e.FinishedAt = time.Time{} }},
		{"transfer fail transfer_no", validTransferFailed, func(e *Event) { e.TransferNo = "" }},
		{"transfer fail out_transfer_no", validTransferFailed, func(e *Event) { e.OutTransferNo = "" }},
		{"transfer fail merchant_id", validTransferFailed, func(e *Event) { e.MerchantID = "" }},
		{"transfer fail payee", validTransferFailed, func(e *Event) { e.PayeeMerchantID = "" }},
		{"transfer fail amount", validTransferFailed, func(e *Event) { e.Amount = 0 }},
		{"transfer fail mode", validTransferFailed, func(e *Event) { e.Mode = "" }},
		{"transfer fail status empty", validTransferFailed, func(e *Event) { e.Status = "" }},
		{"transfer fail status success", validTransferFailed, func(e *Event) { e.Status = "SUCCESS" }},
		{"transfer fail fail_code", validTransferFailed, func(e *Event) { e.FailCode = "" }},
		{"transfer fail finished_at", validTransferFailed, func(e *Event) { e.FinishedAt = time.Time{} }},
		{"unknown type", validPayment, func(e *Event) { e.Type = "payment.failed" }},
		{"empty type", validPayment, func(e *Event) { e.Type = "" }},
		{"type case", validPayment, func(e *Event) { e.Type = "Payment.Succeeded" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evt := tc.base()
			tc.edit(&evt)
			err := Add(context.Background(), (*sqlx.Tx)(nil), evt, ids, clk)
			if !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("err = %v", err)
			}
			if errors.Is(err, idgen.ErrClockRollback) || errors.Is(err, ErrNilTx) {
				t.Fatalf("touched idgen or tx: %v", err)
			}
		})
	}
}

// MQ-01：idgen.ErrClockRollback 原样返回，不插行，不映射成新错误码。
func TestAddClockRollbackDoesNotInsert(t *testing.T) {
	at, clk := clockAt()
	ids := mustGen(t, clk)
	if _, err := ids.NewNo(context.Background(), "EV"); err != nil {
		t.Fatal(err)
	}
	clk.Set(at.Add(-10 * time.Millisecond))
	err := Add(context.Background(), nil, validPayment(), ids, clk)
	if !errors.Is(err, idgen.ErrClockRollback) {
		t.Fatalf("err = %v", err)
	}
	if errors.Is(err, ErrNilTx) || errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("err = %v", err)
	}
	if err.Error() == "10004" || bytes.Contains([]byte(err.Error()), []byte("10004")) {
		t.Fatalf("mapped to a new code: %v", err)
	}
}

// MQ-01：依赖为空时返回错误，不把 nil 事务误报成唯一原因，也不发号。
func TestAddNilDependency(t *testing.T) {
	_, clk := clockAt()
	ids := mustGen(t, clk)
	evt := validPayment()
	if err := Add(context.Background(), nil, evt, nil, clk); err == nil || errors.Is(err, ErrNilTx) {
		t.Fatalf("nil ids: %v", err)
	}
	if err := Add(context.Background(), nil, evt, ids, nil); err == nil || errors.Is(err, ErrNilTx) {
		t.Fatalf("nil clock: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Add(canceled, nil, evt, ids, clk); !errors.Is(err, context.Canceled) || errors.Is(err, ErrNilTx) {
		t.Fatalf("canceled ctx: %v", err)
	}
}

// 金额类型是 money.Amount，编译期就不是浮点。
var _ money.Amount = validPayment().Amount
