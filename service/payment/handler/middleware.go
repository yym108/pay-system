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

// 本文件与 service/account/handler/middleware.go 同一口径。两服务不能互相 import，先各留一份。

// Tracer 是链路追踪钩子。生产进程装入 obs.HandlerSpan。
// Tracer 为空时不创建 span。实现方不得把请求或响应写进 span 属性。
type Tracer interface {
	Start(ctx context.Context, method string) (context.Context, func(error))
}

// Metrics 记录一次 RPC。bizCode 为 0 表示成功或非业务错误（含超时）。
// 生产进程装入 obs.RPCMetrics。配置为 nil 时不计数。
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

func (c Config) timeoutFor(method string) time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	// CreateRefund 与 Pay 一样要等渠道最多 3 秒，服务端用 5 秒，让渠道超时先到期。
	if method == "Pay" || method == "CreateRefund" {
		return PayServerTimeout
	}
	return ServerTimeout
}

// middleware 不读取 req 和 resp：GetMerchantCredential 的响应含明文密钥。
// 访问日志的 merchant_id 留空；带 merchant_id 的方法在各自功能点从请求里取。
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
			// 槽包住整个处理方法。响应转换留在最后一段里，span 结束前才关。
			slotted := stageMethod(method)
			if slotted {
				ctx = obs.WithSlot(ctx)
			}
			ctx, cancel := context.WithTimeout(ctx, c.timeoutFor(method))
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
				if slotted {
					obs.CloseSlot(ctx)
				}
				endSpan(observed)
				// 关槽之后再回传，最后一段已经记进阶段，不会再加进网关的 gw_auth。
				obs.SendHandlerNS(ctx, time.Since(start))
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

// stageMethod 是会写本进程阶段槽的四个写方法。查询不记这些阶段。
func stageMethod(method string) bool {
	switch method {
	case "CreatePayment", "Pay", "CreateRefund", "CreateTransfer":
		return true
	default:
		return false
	}
}

func methodName(ctx context.Context) string {
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil || ri.Invocation() == nil {
		return ""
	}
	return ri.Invocation().MethodName()
}
