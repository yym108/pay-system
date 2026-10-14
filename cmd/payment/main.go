package main

import (
	"io"
	"os"

	"github.com/yym108/pay-system/pkg/config"
)

func main() {
	// TODO(PAY-01): 装配并启动 payment 服务。
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadPayment(environ)
	return config.WriteResult(stdout, stderr, cfg, err)
}
