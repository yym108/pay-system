package obs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/errno"
)

func useRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})
	return rec
}

// OPS-02：空 endpoint 的 shutdown 返回 nil，且不替换已有 TracerProvider。
func TestInstallEmptyEndpointKeepsProvider(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
	rec := useRecorder(t)

	shutdown, err := Install(context.Background(), "", "")
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	_, end := HandlerSpan{Service: "gateway"}.Start(context.Background(), "CreatePay")
	end(nil)

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	if spans[0].Name() != "CreatePay" {
		t.Fatalf("name = %s", spans[0].Name())
	}
}

// OPS-02：不认识的 sampler 返回错误，错误文本含 OTEL_TRACES_SAMPLER。
func TestInstallUnknownSampler(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "not_a_sampler")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
	_, err := Install(context.Background(), "svc", "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "OTEL_TRACES_SAMPLER") {
		t.Fatalf("err = %v", err)
	}
}

// OPS-02：traceidratio 只接受 [0,1]。测完环境变量恢复为空。
func TestInstallRatio(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")

	cases := []struct {
		name    string
		sampler string
		arg     string
		ok      bool
	}{
		{name: "ratio 0", sampler: "traceidratio", arg: "0", ok: true},
		{name: "ratio 1", sampler: "traceidratio", arg: "1", ok: true},
		{name: "parent ratio 0", sampler: "parentbased_traceidratio", arg: "0", ok: true},
		{name: "parent ratio 1", sampler: "parentbased_traceidratio", arg: "1", ok: true},
		{name: "ratio 1.5", sampler: "traceidratio", arg: "1.5", ok: false},
		{name: "ratio -1", sampler: "traceidratio", arg: "-1", ok: false},
		{name: "ratio -0.1", sampler: "parentbased_traceidratio", arg: "-0.1", ok: false},
		{name: "empty arg", sampler: "traceidratio", arg: "", ok: false},
		{name: "parent empty arg", sampler: "parentbased_traceidratio", arg: "", ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_SAMPLER", tc.sampler)
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", tc.arg)
			_, err := Install(context.Background(), "svc", "")
			if tc.ok && err != nil {
				t.Fatalf("Install: %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("expected error")
			}
			if !tc.ok && !strings.Contains(err.Error(), "OTEL_TRACES_SAMPLER") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

// OPS-02：允许的非 ratio sampler 在空 endpoint 下安装成功。
func TestInstallNamedSamplers(t *testing.T) {
	for _, name := range []string{"always_on", "always_off", "parentbased_always_on", "parentbased_always_off"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_SAMPLER", name)
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
			shutdown, err := Install(context.Background(), "svc", "")
			if err != nil {
				t.Fatalf("Install: %v", err)
			}
			if err := shutdown(context.Background()); err != nil {
				t.Fatalf("shutdown: %v", err)
			}
		})
	}
}

// OPS-02：SetOrder 只写入传入的非空白名单键。
func TestSetOrderOnlyNonEmpty(t *testing.T) {
	rec := useRecorder(t)
	_, span := otel.Tracer("t").Start(context.Background(), "op")
	SetOrder(span, "P1", "", "T1", "", "E1")
	span.End()

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	got := map[string]string{}
	for _, attr := range spans[0].Attributes() {
		got[string(attr.Key)] = attr.Value.AsString()
	}
	want := map[string]string{"pay_no": "P1", "transfer_no": "T1", "err_code": "E1"}
	if len(got) != len(want) {
		t.Fatalf("attrs = %#v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("attr %s = %q, want %q", k, got[k], v)
		}
	}
}

// OPS-02：非法 traceparent 返回 false；合法的 TraceID 与 SpanID 与输入一致。
func TestLinkFromTraceparent(t *testing.T) {
	bad := []string{
		"",
		"bad",
		"00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-00f067aa0ba902b7-01",
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
	}
	for _, raw := range bad {
		if _, ok := LinkFromTraceparent(raw); ok {
			t.Fatalf("LinkFromTraceparent(%q) = true", raw)
		}
	}

	const raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	link, ok := LinkFromTraceparent(raw)
	if !ok {
		t.Fatal("expected ok")
	}
	sc := link.SpanContext
	if !sc.IsRemote() {
		t.Fatal("SpanContext is not remote")
	}
	if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace = %s", sc.TraceID())
	}
	if sc.SpanID().String() != "00f067aa0ba902b7" {
		t.Fatalf("span = %s", sc.SpanID())
	}
	if sc.TraceFlags() != trace.FlagsSampled {
		t.Fatalf("flags = %02x", byte(sc.TraceFlags()))
	}
}

// OPS-02：HandlerSpan 对 errno 错误只写十进制 err_code，span 名是方法名。
func TestHandlerSpanErrno(t *testing.T) {
	rec := useRecorder(t)
	_, end := HandlerSpan{Service: "payment"}.Start(context.Background(), "Create")
	end(errno.Wrap(&errno.Error{Code: 10003, Msg: "internal error"}, errors.New("db down")))

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	if spans[0].Name() != "Create" {
		t.Fatalf("name = %s", spans[0].Name())
	}
	if spans[0].InstrumentationScope().Name != "payment" {
		t.Fatalf("tracer = %s", spans[0].InstrumentationScope().Name)
	}
	st := spans[0].Status()
	if st.Code != codes.Error || st.Description != "" {
		t.Fatalf("status = %+v", st)
	}
	if len(spans[0].Events()) != 0 {
		t.Fatalf("events = %d", len(spans[0].Events()))
	}
	attrs := spans[0].Attributes()
	if len(attrs) != 1 || string(attrs[0].Key) != "err_code" || attrs[0].Value.AsString() != "10003" {
		t.Fatalf("attrs = %#v", attrs)
	}
}

// OPS-02：消费 span 用 link，不把 traceparent 提成父 span。坏 JSON 仍然开 span。
func TestStartLinked(t *testing.T) {
	rec := useRecorder(t)
	raw := []byte(`{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}`)
	ctx, end := StartLinked(context.Background(), "account", "consume", raw)
	sc := trace.SpanFromContext(ctx).SpanContext()
	end(nil)
	spans := rec.Ended()
	if len(spans) != 1 || spans[0].Name() != "consume" {
		t.Fatalf("spans = %d", len(spans))
	}
	if spans[0].Parent().IsValid() {
		t.Fatal("consume span has a parent")
	}
	if sc.TraceID().String() == "0af7651916cd43dd8448eb211c80319c" {
		t.Fatal("consume reused the linked trace id")
	}
	links := spans[0].Links()
	if len(links) != 1 || links[0].SpanContext.TraceID().String() != "0af7651916cd43dd8448eb211c80319c" {
		t.Fatalf("links = %#v", links)
	}

	_, end = StartLinked(context.Background(), "account", "consume", []byte(`{"traceparent":"bad"}`))
	end(nil)
	if n := len(rec.Ended()); n != 2 {
		t.Fatalf("spans = %d", n)
	}
	if len(rec.Ended()[1].Links()) != 0 {
		t.Fatal("bad traceparent created a link")
	}
}

// OPS-02：采样器不合法时 Use 返回错误，不替换 Provider。
func TestUseRejectsUnknownSampler(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "not_a_sampler")
	rec := useRecorder(t)
	_, err := Use(context.Background(), "gateway", "", nil)
	if err == nil || !strings.Contains(err.Error(), "OTEL_TRACES_SAMPLER") {
		t.Fatalf("err = %v", err)
	}
	_, end := HandlerSpan{Service: "gateway"}.Start(context.Background(), "kept")
	end(nil)
	if len(rec.Ended()) != 1 || rec.Ended()[0].Name() != "kept" {
		t.Fatal("provider was replaced")
	}
}
