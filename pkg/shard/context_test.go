package shard

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/logx"
)

// ACC-13 验收：JSON 日志带整数 shard；没有 span 时 noop 调用不 panic。
func TestShardLogAndNoopSpan(t *testing.T) {
	if trace.SpanFromContext(context.Background()).IsRecording() {
		t.Fatal("background span is recording")
	}

	var buf bytes.Buffer
	logger := logx.New("account", &buf)
	ctx, logger := WithShard(context.Background(), logger, 1)
	got, ok := ShardFrom(ctx)
	if !ok || got != 1 {
		t.Fatalf("ShardFrom = %d, %v", got, ok)
	}
	logger.InfoContext(ctx, "route")

	dec := json.NewDecoder(strings.NewReader(buf.String()))
	dec.UseNumber()
	var fields map[string]any
	if err := dec.Decode(&fields); err != nil {
		t.Fatalf("log json: %v", err)
	}
	num, ok := fields["shard"].(json.Number)
	if !ok || num.String() != "1" {
		t.Fatalf("shard = %#v", fields["shard"])
	}
	out := buf.String()
	if strings.Contains(out, "dsn") || strings.Contains(strings.ToLower(out), "password") {
		t.Fatal("log leaked dsn or password")
	}
	if _, ok := ShardFrom(context.Background()); ok {
		t.Fatal("background has shard")
	}

	var sink bytes.Buffer
	// 确认 nil ctx 会从 Background 起，这是 WithShard 的约定。
	ctx0, logger0 := WithShard(nil, logx.New("account", &sink), 0) //nolint:staticcheck // SA1012: 故意传入 nil ctx。
	if v, ok := ShardFrom(ctx0); !ok || v != 0 {
		t.Fatalf("ShardFrom nil ctx = %d, %v", v, ok)
	}
	logger0.InfoContext(ctx0, "route-zero")
	if _, loggerNil := WithShard(context.Background(), nil, 0); loggerNil == nil {
		t.Fatal("nil logger")
	}
}
