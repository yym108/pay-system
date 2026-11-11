// Package obs 提供追踪的安装、span 辅助，以及 OPS-03 的 Prometheus 指标。
package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/errno"
)

// Install 按 OTEL_TRACES_SAMPLER 安装全局 TracerProvider。
// ctx 只传给 exporter 的启动。
// endpoint 去掉空白和一个 http:// 或 https:// 前缀后为空时，不创建 exporter，
// 也不替换已有 Provider；返回的 shutdown 调用结果是 nil。
// sampler 不合法时，即使 endpoint 为空也返回 error。错误文本含 OTEL_TRACES_SAMPLER。
func Install(ctx context.Context, service, endpoint string) (shutdown func(context.Context) error, err error) {
	sampler, err := samplerFromEnv()
	if err != nil {
		return nil, err
	}
	endpoint = stripScheme(strings.TrimSpace(endpoint))
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}

// Use 安装追踪。采样配置不合法时返回 error，调用方应退出进程。
// 其他安装错误写一行到 logOut 并继续，返回的停止函数什么都不做。
// 停止函数最多等 2 秒，并把全局 Provider 和 Propagator 换回调用前的值。
func Use(ctx context.Context, service, endpoint string, logOut io.Writer) (func(), error) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	shutdown, err := Install(ctx, service, endpoint)
	if err != nil {
		if strings.Contains(err.Error(), "OTEL_TRACES_SAMPLER") {
			return nil, err
		}
		if logOut != nil {
			_, _ = fmt.Fprintf(logOut, "otel install: %s\n", err.Error())
		}
		return func() {}, nil
	}
	return func() {
		// 调用方的 ctx 在进程退出时已经取消。脱离它再限 2 秒，flush 才不会被立刻打断。
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = shutdown(stopCtx)
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}, nil
}

func samplerFromEnv() (sdktrace.Sampler, error) {
	name := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER"))
	switch name {
	case "":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), nil
	case "always_on":
		return sdktrace.AlwaysSample(), nil
	case "always_off":
		return sdktrace.NeverSample(), nil
	case "traceidratio":
		ratio, err := parseRatio(os.Getenv("OTEL_TRACES_SAMPLER_ARG"))
		if err != nil {
			return nil, err
		}
		return sdktrace.TraceIDRatioBased(ratio), nil
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), nil
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample()), nil
	case "parentbased_traceidratio":
		ratio, err := parseRatio(os.Getenv("OTEL_TRACES_SAMPLER_ARG"))
		if err != nil {
			return nil, err
		}
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio)), nil
	default:
		return nil, fmt.Errorf("OTEL_TRACES_SAMPLER %q is unknown", name)
	}
}

// parseRatio 只接受 strconv.ParseFloat 能解析、且落在 [0,1] 的参数。
func parseRatio(raw string) (float64, error) {
	arg := strings.TrimSpace(raw)
	if arg == "" {
		return 0, errors.New("OTEL_TRACES_SAMPLER_ARG is empty")
	}
	v, err := strconv.ParseFloat(arg, 64)
	if err != nil || math.IsNaN(v) || v < 0 || v > 1 {
		return 0, fmt.Errorf("OTEL_TRACES_SAMPLER_ARG %q must be in [0,1]", raw)
	}
	return v, nil
}

func stripScheme(endpoint string) string {
	if strings.HasPrefix(endpoint, "https://") {
		return strings.TrimPrefix(endpoint, "https://")
	}
	if strings.HasPrefix(endpoint, "http://") {
		return strings.TrimPrefix(endpoint, "http://")
	}
	return endpoint
}

// HandlerSpan 为服务端 handler 创建 span。Service 是 Tracer 名。
type HandlerSpan struct {
	Service string
}

// Start 以方法名作为 span 名。结束函数在 err 非 nil 时把状态标为 Error。
// 错误链上有 *errno.Error 时只追加 err_code（十进制），不记录错误文本。
func (h HandlerSpan) Start(ctx context.Context, method string) (context.Context, func(error)) {
	ctx, span := otel.Tracer(h.Service).Start(ctx, method)
	return ctx, finish(span)
}

// StartClient 为出站调用创建 span。service 是 Tracer 名，method 是 span 名。
func StartClient(ctx context.Context, service, method string) (context.Context, func(error)) {
	ctx, span := otel.Tracer(service).Start(ctx, method)
	return ctx, finish(span)
}

// StartBatch 为一批补偿任务创建一个 span。Tracer 名固定为 job。
func StartBatch(ctx context.Context, jobName string) (context.Context, func()) {
	ctx, span := otel.Tracer("job").Start(ctx, jobName)
	return ctx, func() { span.End() }
}

// StartLinked 开始一个新的 span，并在 raw 能解析出 traceparent 时加上 link。
// 不把 traceparent 提成父 span，所以这条 span 不是原请求的子 span。
// raw 为空或解析失败时仍然开始 span，且不返回错误。
func StartLinked(ctx context.Context, service, name string, raw []byte) (context.Context, func(error)) {
	opts := make([]trace.SpanStartOption, 0, 1)
	if link, ok := LinkFromTraceContext(raw); ok {
		opts = append(opts, trace.WithLinks(link))
	}
	ctx, span := otel.Tracer(service).Start(ctx, name, opts...)
	return ctx, finish(span)
}

func finish(span trace.Span) func(error) {
	return func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, "")
			var biz *errno.Error
			if errors.As(err, &biz) && biz != nil {
				span.SetAttributes(attribute.String("err_code", strconv.FormatInt(int64(biz.Code), 10)))
			}
		}
		span.End()
	}
}

// SetOrder 只把非空的订单字段写进 span。空字符串不产生属性。
func SetOrder(span trace.Span, payNo, refundNo, transferNo, status, errCode string) {
	attrs := make([]attribute.KeyValue, 0, 5)
	if payNo != "" {
		attrs = append(attrs, attribute.String("pay_no", payNo))
	}
	if refundNo != "" {
		attrs = append(attrs, attribute.String("refund_no", refundNo))
	}
	if transferNo != "" {
		attrs = append(attrs, attribute.String("transfer_no", transferNo))
	}
	if status != "" {
		attrs = append(attrs, attribute.String("status", status))
	}
	if errCode != "" {
		attrs = append(attrs, attribute.String("err_code", errCode))
	}
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}

// InjectMap 用当前 TextMapPropagator 把 trace 上下文写入 map。
func InjectMap(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return map[string]string(carrier)
}

// ExtractMap 从 header 提取 trace 上下文。header 为 nil 时当作空载体。
func ExtractMap(ctx context.Context, header map[string]string) context.Context {
	if header == nil {
		header = map[string]string{}
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(header))
}

// LinkFromTraceContext 从 {"traceparent":"..."} 取出 link。
// JSON 不合法、字段缺失或 traceparent 格式不符时返回 false。
func LinkFromTraceContext(raw []byte) (trace.Link, bool) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return trace.Link{}, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var body struct {
		TraceParent string `json:"traceparent"`
	}
	if err := dec.Decode(&body); err != nil {
		return trace.Link{}, false
	}
	return LinkFromTraceparent(body.TraceParent)
}

// LinkFromTraceparent 解析 00-<32 hex trace>-<16 hex span>-<2 hex flags>。
// 成功时 SpanContext 为 Remote，TraceFlags 来自最后一段。格式不符返回 false。
func LinkFromTraceparent(raw string) (trace.Link, bool) {
	parts := strings.Split(raw, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return trace.Link{}, false
	}
	traceID, err := trace.TraceIDFromHex(parts[1])
	if err != nil {
		return trace.Link{}, false
	}
	spanID, err := trace.SpanIDFromHex(parts[2])
	if err != nil {
		return trace.Link{}, false
	}
	flagBytes, err := decodeHexByte(parts[3])
	if err != nil {
		return trace.Link{}, false
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.TraceFlags(flagBytes),
		Remote:     true,
	})
	if !sc.IsValid() {
		return trace.Link{}, false
	}
	return trace.Link{SpanContext: sc}, true
}

func decodeHexByte(s string) (byte, error) {
	if len(s) != 2 {
		return 0, errors.New("trace flags length")
	}
	var b byte
	for i := 0; i < 2; i++ {
		c := s[i]
		var n byte
		switch {
		case c >= '0' && c <= '9':
			n = c - '0'
		case c >= 'a' && c <= 'f':
			n = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			n = c - 'A' + 10
		default:
			return 0, errors.New("trace flags hex")
		}
		b = b<<4 | n
	}
	return b, nil
}
