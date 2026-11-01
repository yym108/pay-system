package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/server"
	"github.com/yym108/pay-system/service/channel_mock/biz"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/handler"
)

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

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

func serve(cfg config.ChannelMock, logOut io.Writer) error {
	logx.SetDefault("channel-mock", logOut)
	openCtx, cancel := context.WithTimeout(context.Background(), openDBTimeout)
	defer cancel()
	db, err := dbx.Open(openCtx, cfg.DSN, dbx.PoolConfig{})
	if err != nil {
		return errOpenDB
	}
	var closeOnce sync.Once
	closeDB := func() error {
		var cerr error
		closeOnce.Do(func() { cerr = db.Close() })
		return cerr
	}
	defer func() { _ = closeDB() }()
	clk := clock.Real{}
	ids, err := idgen.New(cfg.WorkerID, clk)
	if err != nil {
		return err
	}
	// Run 没走到排空时，仍先停调度再关库。排空已经停过调度就不再等。
	sched := biz.NewTimerScheduler(clk)
	var schedStopped atomic.Bool
	defer func() {
		if !schedStopped.Load() {
			sched.Stop()
		}
	}()
	store := dal.New(db)
	svc := biz.New(store, ids, clk, sched, biz.Options{
		DelayMinMS: cfg.MockDelayMinMS,
		DelayMaxMS: cfg.MockDelayMaxMS,
	})
	// 钩子只入队。Recover 仍只重建 PROCESSING 的自动完成，不扫描已终态的回调。
	svc.SetOnFinished(biz.NewNotifier(store, ids, clk, sched, []byte(cfg.ChannelMockpayKey)).Handle)

	// 开始接收请求前重建调度（CH-01 规则 5）。
	recoverCtx, cancelRecover := context.WithTimeout(context.Background(), recoverTimeout)
	n, err := svc.Recover(recoverCtx)
	cancelRecover()
	if err != nil {
		return fmt.Errorf("recover processing mock txn: %w", err)
	}
	slog.Info("mock txn schedule recovered", slog.Int("count", n))

	serveCtx, cancelServe := context.WithCancel(context.Background())
	defer cancelServe()
	gate := make(chan struct{})
	var gateOnce sync.Once
	admin := server.New(server.Config{
		Addr:         server.AddrChannelMock,
		DrainTimeout: server.DefaultDrainTimeout,
	})
	admin.RequireDB(server.PingCheck(db), 0)
	admin.RegisterCloser(closeFunc(closeDB))
	admin.RegisterGroup(server.FuncGroup(
		func() error {
			return handler.Serve(serveCtx, listenAddr, handler.New(svc, []byte(cfg.ChannelMockpayKey), clk))
		},
		func(context.Context) error {
			cancelServe()
			return nil
		},
	))
	admin.RegisterGroup(server.FuncGroup(
		func() error {
			<-gate
			return nil
		},
		func(ctx context.Context) error {
			schedStopped.Store(true)
			done := make(chan struct{})
			go func() {
				sched.Stop()
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
			}
			gateOnce.Do(func() { close(gate) })
			return nil
		},
	))
	return admin.Run(context.Background())
}
