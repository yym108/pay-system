package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/logx"
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
	if err = serve(ctx, stderr); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	return 0
}

// serve 先建客户端再听 :8080 和 :8090。ctx 已取消时直接返回，不绑定端口。
func serve(ctx context.Context, logOut io.Writer) error {
	if err := ctx.Err(); err != nil {
		return nil
	}
	if _, err := gateway.NewClients(); err != nil {
		return err
	}
	logx.SetDefault("gateway", logOut)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	merchant := gateway.NewMerchant(gateway.WithClock(clock.Real{}))
	admin := gateway.NewAdmin(gateway.WithClock(clock.Real{}))
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	start := func(addr string, h *gateway.Handler) {
		defer wg.Done()
		err := gateway.Serve(ctx, addr, h, nil)
		if err != nil && !errors.Is(err, context.Canceled) {
			cancel()
		}
		errCh <- err
	}
	go start(gateway.AddrMerchant, merchant)
	go start(gateway.AddrAdmin, admin)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}
