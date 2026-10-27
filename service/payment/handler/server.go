package handler

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/server"

	"github.com/yym108/pay-system/kitex_gen/payment/paymentservice"
)

const (
	// ListenAddr 是 payment 的 Kitex 端口。接口 §10.2 写死 8881，compose 也用这个端口，不另设环境变量。
	ListenAddr = ":8881"
	// ServerTimeout 是除 Pay 以外的服务端处理超时。调用方 ctx 更短时，先到期的生效。
	// 截止时间交给 biz；biz 返回的取消或截止时间会变成 Kitex 超时，不是 10003。
	ServerTimeout = 2 * time.Second
	// PayServerTimeout 是 Pay 的服务端处理超时，与网关调用 Pay 的 5 秒一致。
	// 渠道受理总超时 3 秒，必须先于这个上限到期。不另设环境变量。
	PayServerTimeout = 5 * time.Second
)

// Config 是一次监听。Timeout 小于等于 0 时，Pay 用 PayServerTimeout，其他方法用 ServerTimeout。
// Timeout 大于 0 时全部方法用这个值，测试可以把它设得很短。
// Listener 非空时优先于 Addr，测试用它拿随机端口。
// Exit 非空时替代 Kitex 的默认信号，测试用来停服务。生产留空，由 Kitex 在 SIGINT/SIGTERM 上返回。
// 这不是 OPS-01 的 /readyz 与 10 秒排空。
type Config struct {
	Addr     string
	Listener net.Listener
	Timeout  time.Duration
	Tracer   Tracer
	Metrics  Metrics
	Access   AccessFunc
	Exit     <-chan error
}

// Run 启动 Kitex 并阻塞到退出。h 为 nil 时返回错误，不监听。
func Run(h *Handler, cfg Config) error {
	if h == nil {
		return errors.New("payment handler is nil")
	}
	opts := []server.Option{
		// BizStatusError 走 TTHeader，不进 Thrift 异常。没有这个 meta handler，调用方会把业务错误看成成功。
		server.WithMetaHandler(transmeta.ServerTTHeaderHandler),
		server.WithMiddleware(cfg.middleware()),
		server.WithErrorHandler(func(ctx context.Context, err error) error {
			return normalizeErr(ctx, err)
		}),
	}
	if cfg.Listener != nil {
		opts = append(opts, server.WithListener(cfg.Listener))
	} else {
		addr := cfg.Addr
		if addr == "" {
			addr = ListenAddr
		}
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return err
		}
		opts = append(opts, server.WithServiceAddr(tcpAddr))
	}
	if cfg.Exit != nil {
		exit := cfg.Exit
		opts = append(opts, server.WithExitSignal(func() <-chan error { return exit }))
	}
	svr := paymentservice.NewServer(h, opts...)
	return svr.Run()
}
