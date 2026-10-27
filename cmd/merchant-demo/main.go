package main

import (
	"fmt"
	"io"
	"os"

	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/fault"
)

func main() {
	// TODO(TL-02): 装配并启动商户演示程序。
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadMerchantDemo(environ)
	if err != nil {
		return config.WriteResult(stdout, stderr, cfg, err)
	}
	if err = fault.Init(cfg.FaultPoints); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	return config.WriteResult(stdout, stderr, cfg, nil)
}
