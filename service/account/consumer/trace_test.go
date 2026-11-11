package consumer

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func withRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prev := otel.GetTracerProvider()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	})
	return rec
}

// OPS-02：入账消费把信封 traceparent 做成 link，并带 pay_no。坏 traceparent 仍然入账。
func TestOPS02ConsumeLinkAndBadTraceparent(t *testing.T) {
	rec := withRecorder(t)
	const traceID = "0af7651916cd43dd8448eb211c80319c"
	good := []byte(`{"event_id":"EV1","event_type":"payment.succeeded","trace_context":{"traceparent":"00-` +
		traceID + `-b7ad6b7169203331-01"},"data":{"pay_no":"PA1","merchant_id":"M100002","out_trade_no":"OT1","channel":"mockpay","amount":10000,"fee_amount":60,"paid_at":"2026-11-10T00:00:00.000+08:00"}}`)
	ledger := &fakeLedger{}
	if err := handle(context.Background(), ledger, payRec(good)); err != nil {
		t.Fatal(err)
	}
	spans := rec.Ended()
	if ledger.postCalls != 1 || len(spans) != 1 || spans[0].Name() != "consume" {
		t.Fatalf("posts=%d spans=%d", ledger.postCalls, len(spans))
	}
	if spans[0].Parent().IsValid() {
		t.Fatal("consume span is a child")
	}
	links := spans[0].Links()
	if len(links) != 1 || links[0].SpanContext.TraceID().String() != traceID {
		t.Fatalf("links = %#v", links)
	}
	found := false
	for _, a := range spans[0].Attributes() {
		if string(a.Key) == "pay_no" && a.Value.AsString() == "PA1" {
			found = true
		}
		if string(a.Key) == "amount" {
			t.Fatal("span has amount")
		}
	}
	if !found {
		t.Fatal("missing pay_no")
	}

	bad := []byte(`{"event_id":"EV1","event_type":"payment.succeeded","trace_context":{"traceparent":"bad"},"data":{"pay_no":"PA1","merchant_id":"M100002","out_trade_no":"OT1","channel":"mockpay","amount":10000,"fee_amount":60,"paid_at":"2026-11-10T00:00:00.000+08:00"}}`)
	ledger2 := &fakeLedger{}
	if err := handle(context.Background(), ledger2, payRec(bad)); err != nil {
		t.Fatal(err)
	}
	if ledger2.postCalls != 1 {
		t.Fatalf("posts = %d", ledger2.postCalls)
	}
	spans = rec.Ended()
	if len(spans) != 2 || len(spans[1].Links()) != 0 {
		t.Fatalf("second links = %d spans=%d", len(spans[1].Links()), len(spans))
	}
}
