package handler

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/server"

	"github.com/yym108/pay-system/kitex_gen/account/accountservice"
)

const (
	// ListenAddr 是 account 的 Kitex 端口。需求写死 8882，compose 也映射这个端口，不另设环境变量。
	ListenAddr = ":8882"
	// ServerTimeout 是服务端处理超时。调用方 ctx 更短时，先到期的生效。
	// 截止时间交给 biz；biz 返回的取消或截止时间会变成 Kitex 超时，不是 10003。
	ServerTimeout = 2 * time.Second
)

// Config 是一次监听。Timeout 小于等于 0 时用 ServerTimeout。
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
		return errors.New("account handler is nil")
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
	svr := accountservice.NewServer(h, opts...)
	return svr.Run()
}
