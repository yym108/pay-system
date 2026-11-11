package biz

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/yym108/pay-system/pkg/clock"
)

type failTrip struct{}

func (failTrip) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("dial failed")
}

// OPS-02：渠道 HTTP 带 traceparent 和 pay_no。网络失败仍返回 nil，span 标成 Error。
func TestOPS02ChannelSpan(t *testing.T) {
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
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	c := &ChannelClient{
		url:     srv.URL,
		key:     []byte("channel-key"),
		clk:     clk,
		http:    srv.Client(),
		timeout: time.Second,
	}
	if _, err := c.Accept(context.Background(), ChannelAcceptRequest{ReqNo: "PA1", Amount: 100, Subject: "demo"}); err != nil {
		t.Fatal(err)
	}
	if got == "" || len(rec.Ended()) != 1 {
		t.Fatalf("traceparent=%q spans=%d", got, len(rec.Ended()))
	}
	sp := rec.Ended()[0]
	if sp.Name() != "pay" {
		t.Fatalf("name = %s", sp.Name())
	}
	for _, a := range sp.Attributes() {
		if string(a.Key) == "pay_no" && a.Value.AsString() != "PA1" {
			t.Fatalf("pay_no = %s", a.Value.AsString())
		}
		if string(a.Key) == "amount" {
			t.Fatal("span has amount")
		}
	}
	c.http = &http.Client{Transport: failTrip{}}
	if _, err := c.Accept(context.Background(), ChannelAcceptRequest{ReqNo: "PA1", Amount: 100, Subject: "demo"}); err != nil {
		t.Fatalf("network error returned %v", err)
	}
	spans := rec.Ended()
	if len(spans) != 2 || spans[1].Status().Code != codes.Error {
		t.Fatalf("status spans=%d", len(spans))
	}
}
