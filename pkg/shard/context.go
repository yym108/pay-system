package shard

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type shardContextKey struct{}

// WithShard 把分片号放进 ctx，并让返回的 logger 带整数属性 shard。
// 当前 span 上设置同名属性。没有 span 时 SpanFromContext 返回 noop，SetAttributes 不会 panic。
// ctx 为 nil 时从 Background 起。logger 为 nil 时用 slog 的默认 logger。
func WithShard(ctx context.Context, logger *slog.Logger, shard int) (context.Context, *slog.Logger) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, shardContextKey{}, shard)
	if span := trace.SpanFromContext(ctx); span != nil {
		span.SetAttributes(attribute.Int("shard", shard))
	}
	if logger == nil {
		logger = slog.Default()
	}
	return ctx, logger.With(slog.Int("shard", shard))
}

// ShardFrom 读取 WithShard 写入的分片号。没有写入时 ok 为 false。
func ShardFrom(ctx context.Context) (int, bool) {
	if ctx == nil {
		return 0, false
	}
	shard, ok := ctx.Value(shardContextKey{}).(int)
	return shard, ok
}
