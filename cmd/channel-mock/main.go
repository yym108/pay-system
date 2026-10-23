package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/service/channel_mock/biz"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/handler"
)

const (
	// listenAddr 是 docs/05-api.md §9.1 的业务端口。
	listenAddr = ":9080"
	// openDBTimeout 只限制启动时打开连接池。
	openDBTimeout = 3 * time.Second
	// recoverTimeout 限制启动时扫描 PROCESSING 重建调度。
	recoverTimeout = 30 * time.Second
)

// errOpenDB 不带地址和口令。dbx.Open 的错误可能含地址，不能原样写到 stderr。
var errOpenDB = errors.New("open channel_mock database failed")

func main() {
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	return runWith(environ, stdout, stderr, func(cfg config.ChannelMock) error {
		return serve(cfg, stdout)
	})
}

func runWith(environ []string, stdout, stderr io.Writer, start func(config.ChannelMock) error) int {
	cfg, err := config.LoadChannelMock(environ)
	if err != nil {
		return config.WriteResult(stdout, stderr, cfg, err)
	}
	if _, werr := fmt.Fprintln(stdout, config.Redact(cfg)); werr != nil {
		_, _ = fmt.Fprintf(stderr, "write config: %v\n", werr)
		return 1
	}
	if err = start(cfg); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	return 0
}

func serve(cfg config.ChannelMock, logOut io.Writer) error {
	logx.SetDefault("channel-mock", logOut)
	openCtx, cancel := context.WithTimeout(context.Background(), openDBTimeout)
	defer cancel()
	db, err := dbx.Open(openCtx, cfg.DSN, dbx.PoolConfig{})
	if err != nil {
		return errOpenDB
	}
	defer func() { _ = db.Close() }()
	clk := clock.Real{}
	ids, err := idgen.New(cfg.WorkerID, clk)
	if err != nil {
		return err
	}
	// 先停调度（等正在写库的完成任务结束）再关连接池：defer 按后进先出执行。
	sched := biz.NewTimerScheduler(clk)
	defer sched.Stop()
	svc := biz.New(dal.New(db), ids, clk, sched, biz.Options{
		DelayMinMS: cfg.MockDelayMinMS,
		DelayMaxMS: cfg.MockDelayMaxMS,
	})

	// 开始接收请求前重建调度（CH-01 规则 5）。
	recoverCtx, cancelRecover := context.WithTimeout(context.Background(), recoverTimeout)
	n, err := svc.Recover(recoverCtx)
	cancelRecover()
	if err != nil {
		return fmt.Errorf("recover processing mock txn: %w", err)
	}
	slog.Info("mock txn schedule recovered", slog.Int("count", n))

	// 健康检查和 SIGTERM 排空是 OPS-01。这里只阻塞到收到信号。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return handler.Serve(ctx, listenAddr, handler.New(svc, []byte(cfg.ChannelMockpayKey), clk))
}
