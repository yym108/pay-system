package obs

import (
	"context"
	"strconv"
	"time"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

// HeaderPayHandlerNS 是 payment 回传的处理方法耗时，十进制纳秒。
// 内部 TTHeader 字符串，不是商户头，也不是 JSON 字段。
const HeaderPayHandlerNS = "pay_handler_ns"

// SendHandlerNS 把处理方法耗时写进 TTHeader 回传。没有发送桶、nil 或非正耗时都忽略。
func SendHandlerNS(ctx context.Context, elapsed time.Duration) {
	if ctx == nil || elapsed <= 0 {
		return
	}
	metainfo.SendBackwardValue(ctx, HeaderPayHandlerNS, strconv.FormatInt(elapsed.Nanoseconds(), 10))
}

// ClientMiddleware 为一次 Kitex 调用建立客户端 span，并把 traceparent 放进 TTHeader 持久元信息。
// service 是 Tracer 名。方法名来自 rpcinfo，缺失时用 rpc。
func ClientMiddleware(service string) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) (err error) {
			ctx, end := StartClient(ctx, service, rpcMethod(ctx))
			defer func() { end(err) }()
			if tp := InjectMap(ctx)["traceparent"]; tp != "" {
				ctx = metainfo.WithPersistentValue(ctx, "traceparent", tp)
			}
			err = next(ctx, req, resp)
			return err
		}
	}
}

func rpcMethod(ctx context.Context) string {
	ri := rpcinfo.GetRPCInfo(ctx)
	if ri == nil {
		return "rpc"
	}
	if inv := ri.Invocation(); inv != nil && inv.MethodName() != "" {
		return inv.MethodName()
	}
	if to := ri.To(); to != nil && to.Method() != "" {
		return to.Method()
	}
	return "rpc"
}
