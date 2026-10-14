// Package logx 提供 JSON 日志和脱敏。
//
// 每条日志都带 service、trace_id、span_id、request_id。
// 敏感字段按名字脱敏，字符串里的卡号和手机号再做一次兜底扫描。
// 配置启动日志的脱敏在 pkg/config，不走本包。
package logx

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/kitex/pkg/klog"
	"go.opentelemetry.io/otel/trace"
)

// shanghai 与 pkg/clock 的 Real 相同，是固定东八区。
// 不用 LoadLocation：运行镜像没有 tzdata 时按名加载会失败。中国自 1991 年起没有夏令时。
var shanghai = time.FixedZone("Asia/Shanghai", 8*60*60)

type requestIDKey struct{}

// WithRequestID 把 request_id 放进 ctx。
// ctx 为 nil 时从 Background 起。id 为空则日志里该字段为空字符串。
func WithRequestID(ctx context.Context, id string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

// New 返回写入 w 的 JSON logger。service 写进每条日志。
// 不替换进程的默认 logger。w 为 nil 时丢弃输出。
func New(service string, w io.Writer) *slog.Logger {
	logger, _ := newLogger(service, w)
	return logger
}

// SetDefault 把同一个 handler 设为 slog、hlog 和 klog 的输出。
// 三者一起换。测试不要并发调用，也不要和其他测试并行改这个全局 logger。
func SetDefault(service string, w io.Writer) {
	logger, level := newLogger(service, w)
	slog.SetDefault(logger)
	shared := &bridge{logger: logger, level: level}
	hlog.SetLogger(&hertzLogger{bridge: shared})
	klog.SetLogger(&kitexLogger{bridge: shared})
}

// Access 写一条访问日志。
// 只有路径、状态码、耗时、商户号和错误码，签名里没有请求体，避免原文进日志。
// 走 slog 的默认 logger；先 SetDefault，才会带上固定字段和脱敏。
func Access(ctx context.Context, path string, status int, latency time.Duration, merchantID, errCode string) {
	slog.Default().InfoContext(ctx, "request handled",
		"path", path,
		"status", status,
		"elapsed_ms", latency.Milliseconds(),
		"merchant_id", merchantID,
		"err_code", errCode,
	)
}

type op struct {
	group   string
	attr    slog.Attr
	isGroup bool
}

type handler struct {
	inner   slog.Handler
	level   *slog.LevelVar
	service string
	ops     []op
}

func newLogger(service string, w io.Writer) (*slog.Logger, *slog.LevelVar) {
	if w == nil {
		w = io.Discard
	}
	// 零值是 Info。适配器的 SetLevel 会改这一个变量，slog 和两边框架一起生效。
	level := new(slog.LevelVar)
	h := &handler{level: level, service: service}
	h.inner = slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceAttr,
	})
	return slog.New(h), level
}

func (h *handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	rec := make([]slog.Attr, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		rec = append(rec, a)
		return true
	})
	user := withoutFixedKeys(nest(h.ops, rec))
	redacted := make([]slog.Attr, len(user))
	for i, a := range user {
		redacted[i] = redactAttr(a)
	}

	traceID, spanID := traceIDs(ctx)
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	out.AddAttrs(
		slog.String("service", h.service),
		slog.String("trace_id", traceID),
		slog.String("span_id", spanID),
		slog.String("request_id", requestIDFrom(ctx)),
	)
	out.AddAttrs(redacted...)
	return h.inner.Handle(ctx, out)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	h2 := h.clone(len(attrs))
	for _, a := range attrs {
		h2.ops = append(h2.ops, op{attr: a})
	}
	return h2
}

func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := h.clone(1)
	h2.ops = append(h2.ops, op{isGroup: true, group: name})
	return h2
}

func (h *handler) clone(extra int) *handler {
	h2 := *h
	h2.ops = make([]op, len(h.ops), len(h.ops)+extra)
	copy(h2.ops, h.ops)
	return &h2
}

// nest 把 With 的分组和本条日志的属性叠成一棵树。
// 空分组不输出。记录上的属性落在当前最内层分组里。
func nest(ops []op, rec []slog.Attr) []slog.Attr {
	type frame struct {
		name  string
		attrs []slog.Attr
	}
	stack := []frame{{}}
	for _, item := range ops {
		if item.isGroup {
			stack = append(stack, frame{name: item.group})
			continue
		}
		top := &stack[len(stack)-1]
		top.attrs = append(top.attrs, item.attr)
	}
	stack[len(stack)-1].attrs = append(stack[len(stack)-1].attrs, rec...)
	for len(stack) > 1 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if len(top.attrs) == 0 {
			continue
		}
		copied := make([]slog.Attr, len(top.attrs))
		copy(copied, top.attrs)
		parent := &stack[len(stack)-1]
		parent.attrs = append(parent.attrs, slog.Attr{
			Key:   top.name,
			Value: slog.GroupValue(copied...),
		})
	}
	return stack[0].attrs
}

func withoutFixedKeys(attrs []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if isFixedKey(a.Key) {
			continue
		}
		out = append(out, a)
	}
	return out
}

func isFixedKey(key string) bool {
	switch key {
	case "service", "trace_id", "span_id", "request_id":
		return true
	default:
		return false
	}
}

func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 {
		switch a.Key {
		case slog.TimeKey:
			return slog.Time(slog.TimeKey, a.Value.Time().In(shanghai))
		case slog.LevelKey:
			lv, ok := a.Value.Any().(slog.Level)
			if !ok {
				return a
			}
			return slog.String(slog.LevelKey, lv.String())
		case "trace_id", "span_id", "request_id":
			// 关联标识按原值写。span id 里的连续数字不能当成卡号，否则链路对不上。
			return a
		}
	}
	return redactAttr(a)
}

func traceIDs(ctx context.Context) (string, string) {
	if ctx == nil {
		return "", ""
	}
	sc := trace.SpanContextFromContext(ctx)
	var traceID, spanID string
	if sc.HasTraceID() {
		traceID = sc.TraceID().String()
	}
	if sc.HasSpanID() {
		spanID = sc.SpanID().String()
	}
	return traceID, spanID
}

func requestIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
