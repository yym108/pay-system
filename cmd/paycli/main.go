package main

import (
	"io"
	"os"

	"github.com/yym108/pay-system/pkg/config"
)

func main() {
	// TODO(TL-01): 装配并运行命令行工具。
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadPayCLI(environ)
	return config.WriteResult(stdout, stderr, cfg, err)
}
