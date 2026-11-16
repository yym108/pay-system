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

const openDBTimeout = 3 * time.Second

// errOpenDB 不带地址和口令，防止泄漏 DSN。
var errOpenDB = errors.New("open recon database failed")

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

	ids, err := idgen.New(cfg.WorkerID, clock.Real{})
	if err != nil {
		return err
	}
	svc := biz.NewRecon(db, ids, clock.Real{}, []string{"mockpay"}, nil, nil)
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
	var closeOnce sync.Once
	closeDB := func() error {
		var cerr error
		closeOnce.Do(func() { cerr = db.Close() })
		return cerr
	}
	defer func() { _ = closeDB() }()

	ids, err := idgen.New(cfg.WorkerID, clock.Real{})
	if err != nil {
		return err
	}
	svc := biz.NewRecon(db, ids, clock.Real{}, []string{"mockpay"}, nil, nil)
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
