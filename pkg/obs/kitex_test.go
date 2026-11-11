package obs

import (
	"context"
	"strings"
	"testing"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// OPS-02：客户端中间件把当前 span 的 traceparent 放进持久元信息。
func TestClientMiddlewareInjectsTraceparent(t *testing.T) {
	rec := useRecorder(t)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	var got string
	ep := ClientMiddleware("gateway")(func(ctx context.Context, _, _ interface{}) error {
		got, _ = metainfo.GetPersistentValue(ctx, "traceparent")
		return nil
	})
	if err := ep(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "00-") || len(got) < 55 {
		t.Fatalf("traceparent = %q", got)
	}
	spans := rec.Ended()
	if len(spans) != 1 || spans[0].Name() != "rpc" {
		t.Fatalf("spans = %d name=%s", len(spans), spans[0].Name())
	}
	if !strings.Contains(got, spans[0].SpanContext().TraceID().String()) {
		t.Fatalf("traceparent %s trace %s", got, spans[0].SpanContext().TraceID())
	}
}
