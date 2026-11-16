package handler

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/rpcinfo"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/obs"
)

// Tracer 是链路追踪钩子。
type Tracer interface {
	Start(ctx context.Context, method string) (context.Context, func(error))
}

// Metrics 记录一次 RPC 指标。
type Metrics interface {
	Observe(method string, bizCode int32, elapsed time.Duration)
}

// AccessFunc 写一条访问日志。
type AccessFunc func(ctx context.Context, path string, status int, latency time.Duration, merchantID, errCode string)

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string) (context.Context, func(error)) {
	return ctx, func(error) {}
}

type noopMetrics struct{}

func (noopMetrics) Observe(string, int32, time.Duration) {}

func (c Config) tracer() Tracer {
	if c.Tracer == nil {
		return noopTracer{}
	}
	return c.Tracer
}

func (c Config) metrics() Metrics {
	if c.Metrics == nil {
		return noopMetrics{}
	}
	return c.Metrics
}

func (c Config) access() AccessFunc {
	if c.Access == nil {
		return logx.Access
	}
	return c.Access
}

func (c Config) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return ServerTimeout
}

func (c Config) middleware() endpoint.Middleware {
	tr := c.tracer()
	mt := c.metrics()
	access := c.access()
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) (err error) {
			method := methodName(ctx)
			start := time.Now()
			if tp, ok := metainfo.GetPersistentValue(ctx, "traceparent"); ok && tp != "" {
				ctx = obs.ExtractMap(ctx, map[string]string{"traceparent": tp})
			}
			ctx, cancel := context.WithTimeout(ctx, c.timeout())
			defer cancel()
			ctx, endSpan := tr.Start(ctx, method)
			defer func() {
				if rec := recover(); rec != nil {
					_ = rec
					slog.ErrorContext(ctx, "recon handler panicked")
					err = errno.ToBizStatus(errno.ErrInternal)
				}
				err = normalizeErr(ctx, err)
				status, code := errFields(err)
				access(ctx, method, status, time.Since(start), "", code)
				mt.Observe(method, toI32(status), time.Since(start))
				endSpan(err)
			}()
			err = next(ctx, req, resp)
			return err
		}
	}
}

func normalizeErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := kerrors.FromBizStatusError(err); ok {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}
	return toBizErr(err)
}

func toBizErr(err error) error {
	var e *errno.Error
	if errors.As(err, &e) {
		return errno.ToBizStatus(e)
	}
	return errno.ToBizStatus(errno.ErrInternal)
}

func errFields(err error) (int, string) {
	if err == nil {
		return 0, ""
	}
	if bizErr, ok := kerrors.FromBizStatusError(err); ok {
		return int(bizErr.BizStatusCode()), strconv.Itoa(int(bizErr.BizStatusCode()))
	}
	return 0, "INTERNAL"
}

func toI32(v int) int32 {
	if v > 2147483647 || v < -2147483648 {
		return 0
	}
	return int32(v)
}

func methodName(ctx context.Context) string {
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil || ri.Invocation() == nil {
		return "unknown"
	}
	name := ri.Invocation().MethodName()
	if name == "" {
		return "unknown"
	}
	return name
}
