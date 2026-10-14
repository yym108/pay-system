package main

import (
	"io"
	"os"

	"github.com/yym108/pay-system/pkg/config"
)

func main() {
	// TODO(ACC-11): 装配并启动 account 服务。
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadAccount(environ)
	return config.WriteResult(stdout, stderr, cfg, err)
}
