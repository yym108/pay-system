// Package gateway 是商户 API 与管理 API 的 HTTP 入口。
//
// GW-01 搭了骨架：两个 Hertz 实例、统一 JSON、请求体上限、中间件顺序和 Kitex 客户端。
// GW-03 在商户端口的 /v1 上验签。GW-02 注册支付与余额路由。时间窗口、限流、nonce 仍是占位。
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"

	"github.com/yym108/pay-system/pkg/clock"
	payserver "github.com/yym108/pay-system/pkg/server"
)

const (
	// AddrMerchant 是商户 API 与渠道回调的监听地址。不要写成 127.0.0.1。
	AddrMerchant = ":8080"
	// AddrAdmin 是管理接口的监听地址。
	AddrAdmin = ":8090"
)

// Handler 是一个端口上的中间件和响应签名。商户端口和管理端口各一个。
type Handler struct {
	admin      bool
	clk        clock.Clock
	secret     []byte
	lookup     MerchantLookup
	payment    paymentAPI
	account    accountAPI
	notify     notifyAPI
	channelKey []byte
	nextNonce  func() (string, error)
	logger     *slog.Logger
}

// Option 覆盖时钟、响应签名密钥、商户凭证查询或日志。
type Option func(*Handler)

// WithClock 注入响应签名和访问日志用的时钟。nil 时仍用真实时钟。
func WithClock(clk clock.Clock) Option {
	return func(h *Handler) {
		if clk != nil {
			h.clk = clk
		}
	}
}

// WithSignSecret 是商户端口签响应用的 app_secret。管理端口不用它。
// 为空且请求已标记验签时，响应改成 500 / 10003，不签名。
func WithSignSecret(secret []byte) Option {
	return func(h *Handler) {
		h.secret = append([]byte(nil), secret...)
	}
}

// WithLogger 替换本端口的错误日志。nil 时用 slog 默认 logger。
func WithLogger(logger *slog.Logger) Option {
	return func(h *Handler) {
		h.logger = logger
	}
}

// NewMerchant 构造商户端口。验签通过的响应会带平台签名。
func NewMerchant(opts ...Option) *Handler {
	return newHandler(false, opts...)
}

// NewAdmin 构造管理端口。管理端口永远不签名。
func NewAdmin(opts ...Option) *Handler {
	return newHandler(true, opts...)
}

func newHandler(admin bool, opts ...Option) *Handler {
	h := &Handler{admin: admin, clk: clock.Real{}, nextNonce: randomNonce}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	if h.clk == nil {
		h.clk = clock.Real{}
	}
	if h.nextNonce == nil {
		h.nextNonce = randomNonce
	}
	return h
}

// Register 安装中间件。商户端口注册支付、退款、转账与余额路由，管理端口不注册。
//
// 顺序：请求体上限、Recovery、RequestID、Trace、AccessLog、
// 时间窗口占位、验签、限流占位、nonce 占位、响应签名、handler。
func (h *Handler) Register(r *server.Hertz) {
	if h == nil || r == nil {
		return
	}
	r.Use(h.bodyLimit)
	r.Use(h.recovery)
	r.Use(h.requestID)
	r.Use(h.trace)
	r.Use(h.accessLog)
	r.Use(h.timeWindow)
	r.Use(h.verifySign)
	r.Use(h.rateLimit)
	r.Use(h.nonce)
	r.Use(h.platformSign)
	if !h.admin {
		h.registerV1(r)
		h.registerCallback(r)
	}
	r.NoRoute(h.notFound)
}

// Listen 在 addr 上监听。测试传 :0。ctx 已取消时不会绑定端口。
func Listen(ctx context.Context, addr string) (net.Listener, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	return ln, nil
}

// NewEngine 用已经监听的 ln 构造 Hertz。传输层用标准库，避免 netpoll 在监听失败时 panic。
// 流式 body 加上 64KB 上限：超限 body 会进中间件，而不是由 Hertz 写纯文本 413。
func NewEngine(ln net.Listener) *server.Hertz {
	return server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithExitWaitTime(payserver.DefaultDrainTimeout),
		server.WithDisablePrintRoute(true),
		server.WithStreamBody(true),
		server.WithMaxRequestBodySize(maxBodyBytes),
	)
}

// Spin 阻塞到 ctx 取消或服务出错。不注册信号，信号由调用方转成 ctx 取消。
func Spin(ctx context.Context, srv *server.Hertz) error {
	if srv == nil {
		return nil
	}
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

// Serve 监听 addr，注册 h，阻塞到 ctx 取消。extra 在生产路由之后追加路由，生产路径传 nil。
func Serve(ctx context.Context, addr string, h *Handler, extra func(*server.Hertz)) error {
	ln, err := Listen(ctx, addr)
	if err != nil {
		return err
	}
	srv := NewEngine(ln)
	if h != nil {
		h.Register(srv)
	}
	if extra != nil {
		extra(srv)
	}
	return Spin(ctx, srv)
}
