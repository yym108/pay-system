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
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/secretbox"
	"github.com/yym108/pay-system/service/payment/biz"
	"github.com/yym108/pay-system/service/payment/handler"
)

// openDBTimeout 只限制启动时打开连接池。不进入业务调用。
const openDBTimeout = 3 * time.Second

// errOpenDB 不带地址和口令。dbx.Open 的错误可能含地址，不能原样写到 stderr。
var errOpenDB = errors.New("open payment database failed")

func main() {
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	return runWith(environ, stdout, stderr, func(cfg config.Payment) error {
		return serve(cfg, stdout)
	})
}

func runWith(environ []string, stdout, stderr io.Writer, start func(config.Payment) error) int {
	cfg, err := config.LoadPayment(environ)
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

func serve(cfg config.Payment, logOut io.Writer) error {
	logx.SetDefault("payment", logOut)
	// 主密钥先于连库校验。secretbox 的错误是固定文本，不含密钥。
	box, err := secretbox.NewFromBase64(cfg.SecretMasterKey)
	if err != nil {
		return err
	}
	openCtx, cancel := context.WithTimeout(context.Background(), openDBTimeout)
	defer cancel()
	db, err := dbx.Open(openCtx, cfg.MySQLDSN, dbx.PoolConfig{})
	if err != nil {
		return errOpenDB
	}
	defer func() { _ = db.Close() }()
	merchants := biz.NewMerchants(db, box, clock.Real{}, cfg.MerchantConfigCacheTTL)
	ids, err := idgen.New(cfg.WorkerID, clock.Real{})
	if err != nil {
		return err
	}
	payments := biz.NewPayments(merchants, ids, clock.Real{}, cfg.NotifyURLAllowHosts, db)
	channelClient, err := biz.NewChannelClient(biz.ChannelClientConfig{
		BaseURL: cfg.ChannelMockpayURL,
		Key:     []byte(cfg.ChannelMockpayKey),
		Clock:   clock.Real{},
	})
	if err != nil {
		return err
	}
	payments.UsePay(channelClient, biz.UnimplementedPayResult{}, cfg.ChannelNotifyURL)
	// 健康检查和 SIGTERM 排空是 OPS-01。这里只阻塞到 Kitex 收到信号。
	return handler.Run(handler.New(biz.NewApp(merchants, payments)), handler.Config{})
}
