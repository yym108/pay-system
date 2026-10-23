package handler

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
)

// exitWait 是优雅退出时等待在途请求的上限。健康检查与 SIGTERM 排空细节属于 OPS-01。
const exitWait = 3 * time.Second

// Serve 在 addr 上启动 HTTP 服务，阻塞到 ctx 取消（优雅退出，返回 nil）或服务出错（返回错误）。
// 信号由调用方转成 ctx 取消，这里不注册信号。
//
// 先自己监听再交给 Hertz：netpoll 传输层监听失败会 panic，标准库传输层在这里更可控。
func Serve(ctx context.Context, addr string, h *Handler) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithExitWaitTime(exitWait),
		server.WithDisablePrintRoute(true),
	)
	h.Register(srv)
	// Spin 遇到 Run 错误只打日志，不返回；在等待函数里把错误留下来。
	var runErr error
	srv.SetCustomSignalWaiter(func(errCh chan error) error {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			runErr = err
			return err
		}
	})
	srv.Spin()
	return runErr
}
