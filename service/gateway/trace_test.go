package gateway

import (
	"context"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// OPS-02：入站 traceparent 成为网关 span 的父级，创建成功后 span 带 pay_no。
func TestOPS02PayNoAndParent(t *testing.T) {
	prev := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
		otel.SetTextMapPropagator(prevProp)
	})
	const traceID = "0af7651916cd43dd8448eb211c80319c"
	pay := &gw02Payment{}
	addr := gw02Start(t, pay, nil)
	body := []byte(`{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`)
	hdr := gw02Sign(t, http.MethodPost, "/v1/payments", body)
	hdr.Set("traceparent", "00-"+traceID+"-b7ad6b7169203331-01")
	got := do(t, http.MethodPost, "http://"+addr+"/v1/payments", body, hdr)
	if got.status != http.StatusOK {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	var found bool
	for _, sp := range rec.Ended() {
		if sp.Name() != "/v1/payments" {
			continue
		}
		found = true
		if sp.Parent().TraceID().String() != traceID {
			t.Fatalf("parent = %s", sp.Parent().TraceID())
		}
		ok := false
		for _, a := range sp.Attributes() {
			if string(a.Key) == "pay_no" && a.Value.AsString() == "PA0000000000000000001" {
				ok = true
			}
		}
		if !ok {
			t.Fatal("missing pay_no")
		}
	}
	if !found {
		t.Fatal("create span missing")
	}
}
