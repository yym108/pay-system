package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/server"
	"github.com/yym108/pay-system/service/account/biz"
	"github.com/yym108/pay-system/service/account/consumer"
	"github.com/yym108/pay-system/service/account/dal"
	"github.com/yym108/pay-system/service/account/handler"
)

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

// openDBTimeout 只限制启动时打开两个连接池。不进入业务调用。
const openDBTimeout = 3 * time.Second

// errOpenDB 不带地址和口令。dal.Open 的错误可能含地址，不能原样写到 stderr。
var errOpenDB = errors.New("open account database failed")

func main() {
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	return runWith(environ, stdout, stderr, func(cfg config.Account) error {
		return serve(cfg, stdout)
	})
}

func runWith(environ []string, stdout, stderr io.Writer, start func(config.Account) error) int {
	cfg, err := config.LoadAccount(environ)
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

func serve(cfg config.Account, logOut io.Writer) error {
	stop, err := obs.Use(context.Background(), "account", cfg.OTELExporterOTLPEndpoint, logOut)
	if err != nil {
		return err
	}
	defer stop()
	logx.SetDefault("account", logOut)
	openCtx, cancel := context.WithTimeout(context.Background(), openDBTimeout)
	defer cancel()
	pools, err := dal.Open(openCtx, cfg.AccountDBDSN0, cfg.AccountDBDSN1, dbx.PoolConfig{})
	if err != nil {
		return errOpenDB
	}
	var closeOnce sync.Once
	closePools := func() error {
		var cerr error
		closeOnce.Do(func() { cerr = pools.Close() })
		return cerr
	}
	defer func() { _ = closePools() }()
	ids, err := idgen.New(cfg.WorkerID, clock.Real{})
	if err != nil {
		return err
	}
	svc := biz.New(pools, ids, cfg.PlatformSubAccounts)
	h := handler.New(svc)
	exit := make(chan error, 1)
	admin := server.New(server.Config{
		Addr:         server.AddrAccount,
		DrainTimeout: server.DefaultDrainTimeout,
	})
	// 两个分片各 Ping 1 秒，外层给 2 秒，避免第二次被单项超时提前取消。
	admin.RequireDB(pools.Ready, 2*server.CheckTimeout)
	admin.RegisterCloser(closeFunc(closePools))
	admin.RegisterGroup(server.FuncGroup(
		func() error {
			return handler.Run(h, handler.Config{
				Exit:     exit,
				ExitWait: server.DefaultDrainTimeout,
				Tracer:   obs.HandlerSpan{Service: "account"},
				Metrics:  obs.RPCMetrics{Service: "account"},
			})
		},
		func(context.Context) error {
			select {
			case exit <- nil:
			default:
			}
			return nil
		},
	))
	// 连不上 Kafka 也要启动。只有 broker 列表为空才在这里失败。
	producer, err := mq.RegisterProducer(admin, cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	group, err := consumer.Open(consumer.Config{
		Brokers: mq.ParseBrokers(cfg.KafkaBrokers),
		Ledger:  svc,
	})
	if err != nil {
		_ = producer.Close()
		return err
	}
	admin.RegisterGroup(group)
	return admin.Run(context.Background())
}
