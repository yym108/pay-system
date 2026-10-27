package handler

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/rpcinfo"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
)

// 本文件与 service/account/handler/middleware.go 同一口径。两服务不能互相 import，先各留一份。

// Tracer 是链路追踪钩子。OPS-02 填入 OTel。
// 本功能点的空实现不创建 span，也不连接导出器。实现方不得把请求或响应写进 span 属性。
type Tracer interface {
	Start(ctx context.Context, method string) (context.Context, func(error))
}

// Metrics 记录一次 RPC。bizCode 为 0 表示成功或非业务错误（含超时）。
// OPS-03 填入 Prometheus。本功能点的空实现不计数。
type Metrics interface {
	Observe(method string, bizCode int32, elapsed time.Duration)
}

// AccessFunc 写一条访问日志。签名与 logx.Access 相同，没有请求体和响应体。
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
	if c.Timeout <= 0 {
		return ServerTimeout
	}
	return c.Timeout
}

// middleware 不读取 req 和 resp：GetMerchantCredential 的响应含明文密钥。
// 访问日志的 merchant_id 留空；带 merchant_id 的方法在各自功能点从请求里取。
func (c Config) middleware() endpoint.Middleware {
	tr := c.tracer()
	mt := c.metrics()
	access := c.access()
	limit := c.timeout()
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) (err error) {
			method := methodName(ctx)
			start := time.Now()
			ctx, cancel := context.WithTimeout(ctx, limit)
			defer cancel()
			ctx, endSpan := tr.Start(ctx, method)
			defer func() {
				if rec := recover(); rec != nil {
					_ = rec
					slog.ErrorContext(ctx, "handler panicked")
					err = errno.ToBizStatus(errno.ErrInternal)
				}
				err = normalizeErr(ctx, err)
				observed := loggedErr(ctx, err)
				status, code := errFields(observed)
				access(ctx, method, status, time.Since(start), "", code)
				mt.Observe(method, toI32(status), time.Since(start))
				endSpan(observed)
			}()
			err = next(ctx, req, resp)
			return err
		}
	}
}

// loggedErr 把已经放进 invocation 的业务码取出来写日志。
// Kitex 在设置 BizStatusError 后让 endpoint 返回 nil，不能把 nil 记成成功。
func loggedErr(ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil || ri.Invocation() == nil {
		return nil
	}
	if biz := ri.Invocation().BizStatusErr(); biz != nil {
		return biz
	}
	return nil
}

func normalizeErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := kerrors.FromBizStatusError(err); ok {
		return err
	}
	if errors.Is(err, kerrors.ErrPanic) {
		// panic 值可能含 SQL、地址或密钥。只记固定句子，不记值。与 recoverBiz 同一口径。
		slog.ErrorContext(ctx, "handler panicked")
		return errno.ToBizStatus(errno.ErrInternal)
	}
	return exitErr(ctx, err)
}

func errFields(err error) (int, string) {
	if err == nil {
		return 0, "0"
	}
	if biz, ok := kerrors.FromBizStatusError(err); ok && biz != nil {
		return int(biz.BizStatusCode()), strconv.Itoa(int(biz.BizStatusCode()))
	}
	if kerrors.IsTimeoutError(err) {
		return 0, "timeout"
	}
	e := errno.From(err)
	return int(e.Code), strconv.Itoa(int(e.Code))
}

func methodName(ctx context.Context) string {
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil || ri.Invocation() == nil {
		return ""
	}
	return ri.Invocation().MethodName()
}
