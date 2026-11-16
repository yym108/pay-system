package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/kitex_gen/payment/paymentservice"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/server"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/handler"
	"github.com/yym108/pay-system/service/recon/job"
)

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

const (
	openDBTimeout  = 3 * time.Second
	paymentRPCAddr = "payment:8881"
	rpcConnTimeout = 100 * time.Millisecond
	rpcCallTimeout = 3 * time.Second
)

// errOpenDB 不带地址和口令，防止泄漏 DSN。
var (
	errOpenDB        = errors.New("open recon database failed")
	errOpenPaymentDB = errors.New("open payment database failed")
	errOpenAccountDB = errors.New("open account database failed")
)

func main() {
	os.Exit(runArgs(os.Args[1:], os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	return runArgs(nil, environ, stdout, stderr)
}

func runArgs(args, environ []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "run" {
		return runCLI(args[1:], environ, stdout, stderr)
	}
	return runWith(environ, stdout, stderr, func(cfg config.Recon) error {
		return serve(cfg, stdout)
	})
}

func runWith(environ []string, stdout, stderr io.Writer, start func(config.Recon) error) int {
	cfg, err := config.LoadRecon(environ)
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

type cliOptions struct {
	channel  string
	billDate string
	force    bool
}

func parseCLIArgs(args []string, stderr io.Writer) (cliOptions, error) {
	fs := flag.NewFlagSet("recon run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var opt cliOptions
	fs.StringVar(&opt.channel, "channel", "", "channel name (e.g. mockpay)")
	fs.StringVar(&opt.billDate, "date", "", "bill date YYYY-MM-DD")
	fs.BoolVar(&opt.force, "force", false, "force rerun DONE batch")
	if err := fs.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if opt.channel == "" || opt.billDate == "" {
		return cliOptions{}, errors.New("--channel and --date are required")
	}
	return opt, nil
}

func runCLI(args, environ []string, stdout, stderr io.Writer) int {
	opt, err := parseCLIArgs(args, stderr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 2
	}
	return runWith(environ, io.Discard, stderr, func(cfg config.Recon) error {
		return executeCLI(cfg, opt, stdout)
	})
}

func executeCLI(cfg config.Recon, opt cliOptions, stdout io.Writer) error {
	openCtx, cancel := context.WithTimeout(context.Background(), openDBTimeout)
	defer cancel()
	db, err := dbx.Open(openCtx, cfg.DSNPayRecon, dbx.PoolConfig{})
	if err != nil {
		return errOpenDB
	}
	defer func() { _ = db.Close() }()

	paymentDB, err := dbx.Open(openCtx, cfg.DSNPayPayment, dbx.PoolConfig{})
	if err != nil {
		return errOpenPaymentDB
	}
	defer func() { _ = paymentDB.Close() }()

	accountDB0, err := dbx.Open(openCtx, cfg.DSNPayAccount0, dbx.PoolConfig{})
	if err != nil {
		return errOpenAccountDB
	}
	defer func() { _ = accountDB0.Close() }()

	accountDB1, err := dbx.Open(openCtx, cfg.DSNPayAccount1, dbx.PoolConfig{})
	if err != nil {
		return errOpenAccountDB
	}
	defer func() { _ = accountDB1.Close() }()

	svc, err := buildReconService(cfg, db, paymentDB, accountDB0, accountDB1)
	if err != nil {
		return err
	}
	ctx, rcancel := context.WithTimeout(context.Background(), handler.ServerTimeout)
	defer rcancel()
	out, err := svc.RunBatch(ctx, biz.RunBatchInput{
		Channel:  opt.channel,
		BillDate: opt.billDate,
		Force:    opt.force,
	})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func serve(cfg config.Recon, logOut io.Writer) error {
	stop, err := obs.Use(context.Background(), "recon", cfg.OTELExporterOTLPEndpoint, logOut)
	if err != nil {
		return err
	}
	defer stop()
	logx.SetDefault("recon", logOut)

	openCtx, cancel := context.WithTimeout(context.Background(), openDBTimeout)
	defer cancel()
	db, err := dbx.Open(openCtx, cfg.DSNPayRecon, dbx.PoolConfig{})
	if err != nil {
		return errOpenDB
	}
	paymentDB, err := dbx.Open(openCtx, cfg.DSNPayPayment, dbx.PoolConfig{})
	if err != nil {
		_ = db.Close()
		return errOpenPaymentDB
	}
	accountDB0, err := dbx.Open(openCtx, cfg.DSNPayAccount0, dbx.PoolConfig{})
	if err != nil {
		_ = errors.Join(db.Close(), paymentDB.Close())
		return errOpenAccountDB
	}
	accountDB1, err := dbx.Open(openCtx, cfg.DSNPayAccount1, dbx.PoolConfig{})
	if err != nil {
		_ = errors.Join(db.Close(), paymentDB.Close(), accountDB0.Close())
		return errOpenAccountDB
	}
	var closeOnce sync.Once
	closeDB := func() error {
		var cerr error
		closeOnce.Do(func() {
			cerr = errors.Join(db.Close(), paymentDB.Close(), accountDB0.Close(), accountDB1.Close())
		})
		return cerr
	}
	defer func() { _ = closeDB() }()

	svc, err := buildReconService(cfg, db, paymentDB, accountDB0, accountDB1)
	if err != nil {
		return err
	}
	sched := job.NewScheduler(svc, clock.Real{}, cfg.ReconRunAt, nil)
	svc.UseRetryScheduler(sched)

	exit := make(chan error, 1)
	admin := server.New(server.Config{
		Addr:         server.AddrRecon,
		DrainTimeout: server.DefaultDrainTimeout,
	})
	admin.RequireDB(server.PingCheck(db), 0)
	admin.RegisterCloser(closeFunc(closeDB))
	admin.RegisterGroup(server.FuncGroup(
		func() error {
			return handler.Run(handler.New(svc), handler.Config{
				Exit:     exit,
				ExitWait: server.DefaultDrainTimeout,
				Tracer:   obs.HandlerSpan{Service: "recon"},
				Metrics:  obs.RPCMetrics{Service: "recon"},
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
	admin.RegisterGroup(sched)
	return admin.Run(context.Background())
}

func buildReconService(cfg config.Recon, db, paymentDB, accountDB0, accountDB1 *sqlx.DB) (*biz.Recon, error) {
	ids, err := idgen.New(cfg.WorkerID, clock.Real{})
	if err != nil {
		return nil, err
	}
	dl, err := biz.NewBillDownloader(db, biz.BillDownloaderConfig{
		BaseURL: cfg.ChannelMockpayURL,
		Key:     []byte(cfg.ChannelMockpayKey),
		Clock:   clock.Real{},
	})
	if err != nil {
		return nil, err
	}
	payCli, err := paymentservice.NewClient("PaymentService",
		client.WithHostPorts(paymentRPCAddr),
		client.WithConnectTimeout(rpcConnTimeout),
		client.WithRPCTimeout(rpcCallTimeout),
		client.WithTransportProtocol(transport.TTHeader),
		client.WithMetaHandler(transmeta.ClientTTHeaderHandler),
		client.WithMiddleware(obs.ClientMiddleware("recon")),
	)
	if err != nil {
		return nil, err
	}
	extChecker := biz.NewExternalChecker(db, paymentDB, ids, clock.Real{})
	intChecker := biz.NewInternalChecker(db, paymentDB, accountDB0, accountDB1, ids, clock.Real{})
	autoHandler := biz.NewAutoHandler(db, paymentDB, accountDB0, accountDB1, biz.NewPaymentRPCClient(payCli), clock.Real{})
	checker := biz.NewCompositeChecker(extChecker, intChecker, autoHandler)
	return biz.NewRecon(db, ids, clock.Real{}, []string{"mockpay"}, dl, checker), nil
}
