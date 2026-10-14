package main

import (
	"io"
	"os"

	"github.com/yym108/pay-system/pkg/config"
)

func main() {
	// TODO(TL-02): 装配并启动商户演示程序。
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadMerchantDemo(environ)
	return config.WriteResult(stdout, stderr, cfg, err)
}
