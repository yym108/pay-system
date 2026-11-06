package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/yym108/pay-system/pkg/accountclient"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/secretbox"
	"github.com/yym108/pay-system/pkg/server"
	"github.com/yym108/pay-system/service/payment/biz"
	"github.com/yym108/pay-system/service/payment/handler"
	"github.com/yym108/pay-system/service/payment/job"
)

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

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
	var closeOnce sync.Once
	closeDB := func() error {
		var cerr error
		closeOnce.Do(func() { cerr = db.Close() })
		return cerr
	}
	defer func() { _ = closeDB() }()
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
	payments.UsePay(channelClient, payments, cfg.ChannelNotifyURL)
	payments.UseClose(channelClient)
	accountCli, err := accountclient.Dial("account:8882")
	if err != nil {
		return err
	}
	payments.UseRefund(biz.NewAccountRefundFreezer(accountCli), channelClient, payments, cfg.ChannelNotifyURL)
	payments.UseQuery(channelClient, biz.QueryPolicy{Backoff: cfg.QueryBackoff})
	// 转账执行是 TRF-02、TRF-03，未接入前 CreateTransfer 返回刚插入的 TRYING。
	payments.UseTransfer(cfg.TransferRecoverDelay, nil)
	queryLoop := job.NewQueryCompensate(payments.RunQueryBatch)
	refundLoop := job.NewRefundRecover(payments.RunRefundRecoverBatch)
	closeLoop := job.NewTimeoutClose(payments.RunTimeoutCloseBatch)
	exit := make(chan error, 1)
	admin := server.New(server.Config{
		Addr:         server.AddrPayment,
		DrainTimeout: server.DefaultDrainTimeout,
	})
	admin.RequireDB(server.PingCheck(db), 0)
	admin.RegisterCloser(closeFunc(closeDB))
	admin.RegisterGroup(server.FuncGroup(
		func() error {
			return handler.Run(handler.New(biz.NewApp(merchants, payments)), handler.Config{
				Exit:     exit,
				ExitWait: server.DefaultDrainTimeout,
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
	admin.RegisterGroup(queryLoop)
	admin.RegisterGroup(refundLoop)
	admin.RegisterGroup(closeLoop)
	return admin.Run(context.Background())
}
