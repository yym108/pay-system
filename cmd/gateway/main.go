package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/server"
	"github.com/yym108/pay-system/service/gateway"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Environ(), os.Stdout, os.Stderr))
}

func run(ctx context.Context, environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadGateway(environ)
	if err != nil {
		return config.WriteResult(stdout, stderr, cfg, err)
	}
	if err = fault.Init(cfg.FaultPoints); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	if _, werr := fmt.Fprintln(stdout, config.Redact(cfg)); werr != nil {
		_, _ = fmt.Fprintf(stderr, "write config: %v\n", werr)
		return 1
	}
	if err = serve(ctx, stderr, cfg.RedisAddr, cfg.ChannelMockpayKey); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	return 0
}

// serve 先建客户端再听 :8080、:8090 和管理端口 :6060。ctx 已取消时直接返回，不绑定端口。
// Redis 连不上也不在这里失败，否则渠道回调会一起起不来。
func serve(ctx context.Context, logOut io.Writer, redisAddr, channelKey string) error {
	if err := ctx.Err(); err != nil {
		return nil
	}
	clients, err := gateway.NewClients()
	if err != nil {
		return err
	}
	logx.SetDefault("gateway", logOut)
	store, ready, closer := gateway.OpenRedisNonce(redisAddr)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	merchant := gateway.NewMerchant(
		gateway.WithClock(clock.Real{}),
		gateway.WithMerchantLookup(gateway.NewPaymentLookup(clients.Payment)),
		gateway.WithPaymentAPI(clients.Payment),
		gateway.WithAccountAPI(clients.Account),
		gateway.WithChannelKey([]byte(channelKey)),
		gateway.WithNotifyAPI(clients.Payment),
		gateway.WithNonceStore(store),
	)
	admin := gateway.NewAdmin(
		gateway.WithClock(clock.Real{}),
		gateway.WithReplayAPI(clients.Payment),
	)
	proc := server.New(server.Config{
		Addr:      server.AddrGateway,
		AllowNoDB: true,
	})
	proc.RegisterCheck(ready)
	proc.RegisterCloser(closer)
	proc.RegisterGroup(listenGroup(ctx, cancel, gateway.AddrMerchant, merchant))
	proc.RegisterGroup(listenGroup(ctx, cancel, gateway.AddrAdmin, admin))
	return proc.Run(ctx)
}

func listenGroup(ctx context.Context, cancel context.CancelFunc, addr string, h *gateway.Handler) server.Group {
	return server.FuncGroup(
		func() error {
			err := gateway.Serve(ctx, addr, h, nil)
			if err != nil && !errors.Is(err, context.Canceled) {
				cancel()
				return err
			}
			return nil
		},
		func(context.Context) error {
			cancel()
			return nil
		},
	)
}
