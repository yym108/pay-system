package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/service/account/biz"
	"github.com/yym108/pay-system/service/account/dal"
	"github.com/yym108/pay-system/service/account/handler"
)

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
	if err = start(cfg); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	return 0
}

func serve(cfg config.Account, logOut io.Writer) error {
	logx.SetDefault("account", logOut)
	openCtx, cancel := context.WithTimeout(context.Background(), openDBTimeout)
	defer cancel()
	pools, err := dal.Open(openCtx, cfg.AccountDBDSN0, cfg.AccountDBDSN1, dbx.PoolConfig{})
	if err != nil {
		return errOpenDB
	}
	defer func() { _ = pools.Close() }()
	ids, err := idgen.New(cfg.WorkerID, clock.Real{})
	if err != nil {
		return err
	}
	h := handler.New(biz.New(pools, ids, cfg.PlatformSubAccounts))
	// 健康检查和 SIGTERM 排空是 OPS-01。这里只阻塞到 Kitex 收到信号。
	return handler.Run(h, handler.Config{})
}
