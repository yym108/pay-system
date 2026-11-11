package job

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// OPS-02：每一批补偿只产生一个 span，名字是任务名。
func TestOPS02OneSpanPerBatch(t *testing.T) {
	prev := otel.GetTracerProvider()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	})
	calls := 0
	l := NewLoop("timeout_close", time.Hour, func(context.Context) error {
		calls++
		return nil
	})
	l.runOnce()
	l.runOnce()
	spans := rec.Ended()
	if calls != 2 || len(spans) != 2 {
		t.Fatalf("calls=%d spans=%d", calls, len(spans))
	}
	for _, sp := range spans {
		if sp.Name() != "timeout_close" {
			t.Fatalf("name = %s", sp.Name())
		}
	}
}
