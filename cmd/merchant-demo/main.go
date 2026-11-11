package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"

	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/obs"
	payserver "github.com/yym108/pay-system/pkg/server"
)

// listenAddr 是需求 §2.2 的商户演示端口。听所有接口，不写成 127.0.0.1。
const listenAddr = ":9100"

func main() {
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	return runWith(environ, stdout, stderr, func(cfg config.MerchantDemo) error {
		return serve(cfg)
	})
}

func runWith(environ []string, stdout, stderr io.Writer, start func(config.MerchantDemo) error) int {
	cfg, err := config.LoadMerchantDemo(environ)
	if err != nil {
		return config.WriteResult(stdout, stderr, cfg, err)
	}
	if _, werr := fmt.Fprintln(stdout, config.Redact(cfg)); werr != nil {
		_, _ = fmt.Fprintf(stderr, "write config: %v\n", werr)
		return 1
	}
	if err = fault.Init(cfg.FaultPoints); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	if err = start(cfg); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	return 0
}

func serve(cfg config.MerchantDemo) error {
	stop, err := obs.Use(context.Background(), "merchant-demo", cfg.OTELExporterOTLPEndpoint, os.Stderr)
	if err != nil {
		return err
	}
	defer stop()
	logx.SetDefault("merchant-demo", os.Stderr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}
	return serveOn(ctx, ln, newDemo(cfg), payserver.DefaultDrainTimeout)
}

// serveOn 在已经监听的 ln 上阻塞，直到 ctx 取消或 Hertz 出错。
// drain 是连接排空上限。测试传短时间，避免默认 10 秒拖住用例。
func serveOn(ctx context.Context, ln net.Listener, d *demo, drain time.Duration) error {
	if drain <= 0 {
		drain = time.Millisecond
	}
	srv := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithExitWaitTime(drain),
		server.WithDisablePrintRoute(true),
	)
	d.register(srv)
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
