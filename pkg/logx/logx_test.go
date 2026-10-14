package logx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/kitex/pkg/klog"
	"go.opentelemetry.io/otel/trace"
)

// 这些测试会替换 slog、hlog、klog 的进程级 logger，不要用 t.Parallel。

const (
	// 需求里的公开例子。19 位，按卡号保留前 6 后 4。
	sampleCard = "6222020200112233445"
	maskedCard = "622202*********3445"
)

// BASE-10 验收 1：app_secret 整段替换为 ***，输出不含原值。
func TestAppSecretRedacted(t *testing.T) {
	var buf bytes.Buffer
	SetDefault("payment", &buf)
	slog.Info("x", "app_secret", "abc")
	out := buf.String()
	if strings.Contains(out, "abc") {
		t.Fatalf("plaintext leaked: %s", out)
	}
	if !strings.Contains(out, `"app_secret":"***"`) {
		t.Fatalf("secret not masked: %s", out)
	}
	assertFixed(t, out, "payment")
}

// BASE-10 验收 2：memo 里的 19 位卡号按前 6 后 4 脱敏。
func TestMemoCardRedacted(t *testing.T) {
	var buf bytes.Buffer
	SetDefault("payment", &buf)
	slog.Info("x", "memo", "card "+sampleCard)
	out := buf.String()
	if strings.Contains(out, sampleCard) {
		t.Fatalf("raw card leaked: %s", out)
	}
	if !strings.Contains(out, maskedCard) {
		t.Fatalf("card not masked: %s", out)
	}
	assertFixed(t, out, "payment")
}

// BASE-10 验收 3：上下文里的 otel trace 写入 trace_id。
func TestTraceIDFromContext(t *testing.T) {
	const traceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanHex = "00f067aa0ba902b7"
	ctx := remoteSpan(t, traceHex, spanHex)

	var buf bytes.Buffer
	SetDefault("payment", &buf)
	slog.InfoContext(ctx, "x")
	out := buf.String()
	if !strings.Contains(out, `"trace_id":"`+traceHex+`"`) {
		t.Fatalf("trace_id missing: %s", out)
	}
	if !strings.Contains(out, `"span_id":"`+spanHex+`"`) {
		t.Fatalf("span_id missing: %s", out)
	}
	assertFixed(t, out, "payment")
}

// BASE-10 规则 1：没有 trace 和 request id 时字段仍在，值为空字符串。时间是东八区。
func TestFixedFieldsWhenAbsent(t *testing.T) {
	line := logLine(t, "payment")
	assertFixed(t, line, "payment")
	if !strings.Contains(line, `"trace_id":""`) || !strings.Contains(line, `"span_id":""`) || !strings.Contains(line, `"request_id":""`) {
		t.Fatalf("empty ids missing: %s", line)
	}
	if !strings.Contains(line, `"level":"INFO"`) {
		t.Fatalf("level: %s", line)
	}
}

// BASE-10 规则 1：request_id 来自 WithRequestID。
func TestRequestID(t *testing.T) {
	var buf bytes.Buffer
	logger := New("gateway", &buf)
	ctx := WithRequestID(context.Background(), "req-1")
	logger.InfoContext(ctx, "x")
	out := buf.String()
	if !strings.Contains(out, `"request_id":"req-1"`) {
		t.Fatalf("request_id: %s", out)
	}
	assertFixed(t, out, "gateway")
}

// BASE-10 规则 1：全数字 span_id 原样保留，不按卡号改写。
func TestSpanIDNotScanned(t *testing.T) {
	const traceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanHex = "1234567890123456"
	ctx := remoteSpan(t, traceHex, spanHex)
	var buf bytes.Buffer
	logger := New("payment", &buf)
	logger.InfoContext(ctx, "x")
	out := buf.String()
	if !strings.Contains(out, `"span_id":"`+spanHex+`"`) {
		t.Fatalf("span_id rewritten: %s", out)
	}
}

// BASE-10 规则 2：字段名不区分大小写，包含片段即匹配；太短则整段 ***。
func TestFieldNameRedaction(t *testing.T) {
	cases := []struct {
		name   string
		key    string
		value  any
		want   string
		absent string
	}{
		{name: "password", key: "password", value: "abc", want: `"password":"***"`, absent: "abc"},
		{name: "token case", key: "Access_Token", value: "abc", want: `"Access_Token":"***"`, absent: "abc"},
		{name: "signature", key: "signature", value: "abc", want: `"signature":"***"`, absent: "abc"},
		{name: "master key", key: "SECRET_MASTER_KEY", value: "abc", want: `"SECRET_MASTER_KEY":"***"`, absent: "abc"},
		{name: "token wins over card", key: "token_card_no", value: sampleCard, want: `"token_card_no":"***"`, absent: sampleCard},
		{name: "card", key: "card_no", value: sampleCard, want: `"card_no":"` + maskedCard + `"`, absent: sampleCard},
		{name: "bank card", key: "Bank_Card", value: sampleCard, want: `"Bank_Card":"` + maskedCard + `"`, absent: sampleCard},
		{name: "card too short", key: "card_no", value: "123456789", want: `"card_no":"***"`, absent: "123456789"},
		{name: "card exact 10", key: "card_no", value: "1234567890", want: `"card_no":"1234567890"`},
		{name: "phone letters", key: "phone", value: "abcdefghi", want: `"phone":"abc**fghi"`},
		{name: "mobile too short", key: "mobile", value: "123456", want: `"mobile":"***"`, absent: "123456"},
		{name: "phone exact 7", key: "phone", value: "1234567", want: `"phone":"1234567"`},
		{name: "id card", key: "id_card", value: "110101199001011234", want: `"id_card":"1****************4"`, absent: "110101199001011234"},
		{name: "id card too short", key: "id_card", value: "X", want: `"id_card":"***"`, absent: `"id_card":"X"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line := logLine(t, "payment", tc.key, tc.value)
			if !strings.Contains(line, tc.want) {
				t.Fatalf("got %s, want substring %s", line, tc.want)
			}
			if tc.absent != "" && strings.Contains(line, tc.absent) {
				t.Fatalf("value leaked in %s", line)
			}
		})
	}
}

// BASE-10 规则 3：非敏感字段里的连续数字才兜底；11 位不是卡号，短数字不动。
func TestFallbackScan(t *testing.T) {
	cases := []struct {
		name   string
		value  string
		want   string
		absent string
	}{
		{name: "phone run", value: "call 13800138000 now", want: "call 138****8000 now", absent: "13800138000"},
		{name: "eleven not phone", value: "n 28800138000", want: "n 28800138000"},
		{name: "twelve", value: "n 622202123456", want: "n 622202123456"},
		{name: "thirteen", value: "n 6222021234567", want: "n 622202***4567", absent: "6222021234567"},
		{name: "twenty", value: "n 62220202001122334456", want: "n 62220202001122334456"},
		{name: "amount", value: "amount 100", want: "amount 100"},
		{name: "separated", value: "6222-0202-0011-2233", want: "6222-0202-0011-2233"},
		{name: "both", value: "card " + sampleCard + " phone 13800138000 amount 100", want: "card " + maskedCard + " phone 138****8000 amount 100", absent: sampleCard},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line := logLine(t, "payment", "memo", tc.value)
			if !strings.Contains(line, tc.want) {
				t.Fatalf("got %s, want substring %s", line, tc.want)
			}
			if tc.absent != "" && strings.Contains(line, tc.absent) {
				t.Fatalf("raw run leaked in %s", line)
			}
		})
	}
}

// BASE-10 规则 2：整数不走兜底扫描。金额 100 保持原样。
func TestNumbersAreNotScanned(t *testing.T) {
	line := logLine(t, "payment", "amount", 100)
	if !strings.Contains(line, `"amount":100`) {
		t.Fatalf("amount: %s", line)
	}
}

// BASE-10 规则 2：分组和 error 走同一套脱敏。敏感分组不展开。
func TestGroupAndErrorRedacted(t *testing.T) {
	line := logLine(t, "payment", slog.Group("user", "password", "abc"))
	if strings.Contains(line, "abc") || !strings.Contains(line, `"password":"***"`) {
		t.Fatalf("group password: %s", line)
	}

	line = logLine(t, "payment", slog.Group("password", "raw", "abc"))
	if strings.Contains(line, "abc") || strings.Contains(line, "raw") || !strings.Contains(line, `"password":"***"`) {
		t.Fatalf("sensitive group expanded: %s", line)
	}

	line = logLine(t, "payment", "err", errors.New("card "+sampleCard))
	if strings.Contains(line, sampleCard) || !strings.Contains(line, maskedCard) {
		t.Fatalf("error text: %s", line)
	}

	line = logLine(t, "payment", "db_password", errors.New("abc"))
	if strings.Contains(line, "abc") || !strings.Contains(line, `"db_password":"***"`) {
		t.Fatalf("error field: %s", line)
	}

	line = logLine(t, "payment", "password", secretGroup{"abc"})
	if strings.Contains(line, "abc") || strings.Contains(line, "raw") || !strings.Contains(line, `"password":"***"`) {
		t.Fatalf("log valuer leaked: %s", line)
	}
}

// BASE-10 规则 2：With 进来的分组属性和消息文本也脱敏。调用方写在根上的 trace_id 不能盖住 otel 的值。
func TestGroupsMessageAndFixedIdentity(t *testing.T) {
	var buf bytes.Buffer
	New("payment", &buf).WithGroup("user").With("password", "abc").Info("x", "memo", "hi")
	out := buf.String()
	if !strings.Contains(out, `"user":{"password":"***","memo":"hi"}`) || strings.Contains(out, "abc") {
		t.Fatalf("with group: %s", out)
	}
	assertFixed(t, out, "payment")

	buf.Reset()
	New("payment", &buf).WithGroup("user").Info("x")
	if strings.Contains(buf.String(), `"user"`) {
		t.Fatalf("empty group emitted: %s", buf.String())
	}

	buf.Reset()
	New("payment", &buf).Info("card " + sampleCard)
	if strings.Contains(buf.String(), sampleCard) || !strings.Contains(buf.String(), maskedCard) {
		t.Fatalf("msg not scanned: %s", buf.String())
	}

	buf.Reset()
	ctx := remoteSpan(t, "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")
	New("payment", &buf).InfoContext(ctx, "x", "trace_id", "not-the-span")
	out = buf.String()
	if strings.Contains(out, "not-the-span") || strings.Count(out, `"trace_id"`) != 1 {
		t.Fatalf("trace_id overridden: %s", out)
	}
	if !strings.Contains(out, `"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`) {
		t.Fatalf("otel trace missing: %s", out)
	}
}

// BASE-10 规则 4：hlog 和 klog 写进同一个 slog，不启动服务。
func TestAdaptersShareHandler(t *testing.T) {
	const traceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanHex = "00f067aa0ba902b7"
	ctx := WithRequestID(remoteSpan(t, traceHex, spanHex), "req-h")

	var buf bytes.Buffer
	SetDefault("gateway", &buf)
	hlog.CtxInfof(ctx, "hello hertz")
	klog.Info("card " + sampleCard)
	hlog.Debug("debug-marker")

	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2: %s", len(lines), out)
	}
	if !strings.Contains(lines[0], `"msg":"hello hertz"`) || !strings.Contains(lines[0], `"trace_id":"`+traceHex+`"`) || !strings.Contains(lines[0], `"request_id":"req-h"`) {
		t.Fatalf("hlog line: %s", lines[0])
	}
	assertFixed(t, lines[0], "gateway")
	if strings.Contains(lines[1], sampleCard) || !strings.Contains(lines[1], maskedCard) {
		t.Fatalf("klog line: %s", lines[1])
	}
	assertFixed(t, lines[1], "gateway")
	if strings.Contains(out, "debug-marker") {
		t.Fatalf("debug not filtered: %s", out)
	}

	hlog.SetLevel(hlog.LevelDebug)
	hlog.Debug("debug-on")
	if !strings.Contains(buf.String(), "debug-on") {
		t.Fatalf("debug level not applied: %s", buf.String())
	}
}

// BASE-10 规则 5：访问日志不记录请求体，只留路径、状态码、耗时、商户号、错误码。
func TestAccessOmitsBody(t *testing.T) {
	body := "raw-body " + sampleCard
	var buf bytes.Buffer
	SetDefault("gateway", &buf)
	Access(context.Background(), "/v1/payments", 200, 15*time.Millisecond, "M100001", "0")
	out := buf.String()
	if strings.Contains(out, body) || strings.Contains(out, sampleCard) || strings.Contains(out, "raw-body") {
		t.Fatalf("body leaked: %s", out)
	}
	for _, part := range []string{
		`"msg":"request handled"`,
		`"path":"/v1/payments"`,
		`"status":200`,
		`"elapsed_ms":15`,
		`"merchant_id":"M100001"`,
		`"err_code":"0"`,
	} {
		if !strings.Contains(out, part) {
			t.Fatalf("missing %s in %s", part, out)
		}
	}
	assertFixed(t, out, "gateway")
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &map[string]any{}); err != nil {
		t.Fatalf("json: %v", err)
	}
}

// BASE-10：New 不改全局 logger。并发写同一 logger 不竞争。
func TestNewDoesNotReplaceDefaultAndIsRaceFree(t *testing.T) {
	prev := slog.Default()
	var buf bytes.Buffer
	logger := New("payment", &buf)
	if slog.Default() != prev {
		t.Fatal("New replaced slog default")
	}
	done := make(chan struct{})
	for range 8 {
		go func() {
			logger.Info("x", "memo", "amount 100")
			done <- struct{}{}
		}()
	}
	for range 8 {
		<-done
	}
	if strings.Count(buf.String(), "\n") != 8 {
		t.Fatalf("lines: %s", buf.String())
	}
}

type secretGroup struct{ raw string }

func (s secretGroup) LogValue() slog.Value {
	return slog.GroupValue(slog.String("raw", s.raw))
}

func logLine(t *testing.T, service string, args ...any) string {
	t.Helper()
	var buf bytes.Buffer
	logger := New(service, &buf)
	logger.Info("x", args...)
	line := strings.TrimSpace(buf.String())
	if err := json.Unmarshal([]byte(line), &map[string]any{}); err != nil {
		t.Fatalf("json: %v line %s", err, line)
	}
	return line
}

func assertFixed(t *testing.T, line, service string) {
	t.Helper()
	keys := []string{`"time":`, `"level":`, `"msg":`, `"service":`, `"trace_id":`, `"span_id":`, `"request_id":`}
	last := -1
	for _, key := range keys {
		i := strings.Index(line, key)
		if i <= last {
			t.Fatalf("fixed field %s missing or out of order: %s", key, line)
		}
		last = i
	}
	if !strings.Contains(line, `"service":"`+service+`"`) {
		t.Fatalf("service: %s", line)
	}
	if !strings.Contains(line, "+08:00") {
		t.Fatalf("timezone: %s", line)
	}
}

func remoteSpan(t *testing.T, traceHex, spanHex string) context.Context {
	t.Helper()
	tid, err := trace.TraceIDFromHex(traceHex)
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	sid, err := trace.SpanIDFromHex(spanHex)
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithRemoteSpanContext(context.Background(), sc)
}
