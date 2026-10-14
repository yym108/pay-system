package main

import (
	"io"
	"os"

	"github.com/yym108/pay-system/pkg/config"
)

func main() {
	// TODO(CH-01): 装配并启动模拟渠道。
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadChannelMock(environ)
	return config.WriteResult(stdout, stderr, cfg, err)
}
