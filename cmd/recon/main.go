package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/obs"
)

func main() {
	// TODO(REC-01): 装配并启动对账任务。
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadRecon(environ)
	if err != nil {
		return config.WriteResult(stdout, stderr, cfg, err)
	}
	if err = fault.Init(cfg.FaultPoints); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	stop, err := obs.Use(context.Background(), "recon", cfg.OTELExporterOTLPEndpoint, stderr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	stop()
	return config.WriteResult(stdout, stderr, cfg, nil)
}
