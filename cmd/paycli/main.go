package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
)

const usageText = `usage: paycli [--gateway URL] [--app-id ID] [--app-secret SECRET] [--admin-token TOKEN] <command> [flags]
commands: create, pay, query, close, refund, query-refund, transfer, query-transfer, balance, admin recon-run, admin replay, admin check
`

func main() {
	os.Exit(run(os.Args[1:], os.Environ(), os.Stdout, os.Stderr, clock.Real{}, randNonce{}.next))
}

func run(args []string, environ []string, stdout, stderr io.Writer, clk clock.Clock, nextNonce func() (string, error)) int {
	return runClient(args, environ, stdout, stderr, clk, nextNonce, nil)
}

func runClient(args []string, environ []string, stdout, stderr io.Writer, clk clock.Clock, nextNonce func() (string, error), newClient func(time.Duration) *http.Client) int {
	if stdout == nil || stderr == nil {
		return 1
	}
	if clk == nil || nextNonce == nil {
		writeLine(stderr, "paycli: clock and nonce are required")
		return 1
	}
	cfg, err := config.LoadPayCLI(environ)
	if err != nil {
		writeLine(stderr, err.Error())
		return 1
	}
	global, cmd, rest, err := splitArgs(args)
	if err != nil {
		if isUsage(err) {
			_, _ = io.WriteString(stderr, usageText)
			return 1
		}
		writeLine(stderr, "paycli: invalid arguments")
		return 1
	}
	if err = applyGlobals(&cfg, global); err != nil {
		writeLine(stderr, "paycli: invalid arguments")
		return 1
	}
	if cmd == "" || cmd == "help" {
		_, _ = io.WriteString(stderr, usageText)
		return 1
	}
	prep, err := prepare(cmd, rest)
	if err != nil {
		if isUsage(err) {
			_, _ = io.WriteString(stderr, usageText)
			return 1
		}
		writeLine(stderr, err.Error())
		return 1
	}
	if err = prep.need(cfg); err != nil {
		writeLine(stderr, err.Error())
		return 1
	}
	return prep.execute(cfg, stdout, stderr, clk, nextNonce, newClient)
}

func writeLine(w io.Writer, msg string) {
	_, _ = fmt.Fprintln(w, msg)
}
