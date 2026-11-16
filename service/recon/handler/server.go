package handler

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/server"

	"github.com/yym108/pay-system/kitex_gen/recon/reconservice"
)

const (
	// ListenAddr 是 recon 的 Kitex 端口，接口 §10.3 约定为 8883。
	ListenAddr = ":8883"
	// ServerTimeout 是 ReconService.RunBatch 的服务端处理超时。
	ServerTimeout = 60 * time.Second
)

// Config 是 ReconService Kitex 服务端监听配置。
type Config struct {
	Addr     string
	Listener net.Listener
	Timeout  time.Duration
	Tracer   Tracer
	Metrics  Metrics
	Access   AccessFunc
	Exit     <-chan error
	ExitWait time.Duration
}

// Run 启动 Kitex 并阻塞到退出。
func Run(h *Handler, cfg Config) error {
	if h == nil {
		return errors.New("recon handler is nil")
	}
	opts := []server.Option{
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
	if cfg.ExitWait > 0 {
		opts = append(opts, server.WithExitWaitTime(cfg.ExitWait))
	}
	svr := reconservice.NewServer(h, opts...)
	return svr.Run()
}
