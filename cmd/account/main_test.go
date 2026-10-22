package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/yym108/pay-system/pkg/config"
)

func TestAccountRunRejectsMissingDSN(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"WORKER_ID=3",
		"ACCOUNT_DB_DSN_0=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_0",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	out := stdout.String() + stderr.String()
	if !strings.Contains(out, "ACCOUNT_DB_DSN_1") {
		t.Fatal("output does not contain ACCOUNT_DB_DSN_1")
	}
	if strings.Contains(out, "account-plain-password") {
		t.Fatal("output contains secret material")
	}
}

func TestAccountRunStartsWithoutPrintingSecret(t *testing.T) {
	var stdout, stderr bytes.Buffer
	var got configSeen
	code := runWith([]string{
		"WORKER_ID=3",
		"ACCOUNT_DB_DSN_0=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_0",
		"ACCOUNT_DB_DSN_1=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_1",
	}, &stdout, &stderr, func(cfg config.Account) error {
		got.called = true
		got.worker = cfg.WorkerID
		if cfg.AccountDBDSN0 == "" || cfg.AccountDBDSN1 == "" {
			t.Fatal("start did not receive DSNs")
		}
		return nil
	})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, stderr.String())
	}
	if !got.called || got.worker != 3 {
		t.Fatalf("start not called: %+v", got)
	}
	out := stdout.String() + stderr.String()
	if strings.Contains(out, "account-plain-password") {
		t.Fatal("output contains secret material")
	}
	if !strings.Contains(out, "***") {
		t.Fatal("redacted config was not printed")
	}
}

func TestAccountRunHidesOpenError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runWith([]string{
		"WORKER_ID=3",
		"ACCOUNT_DB_DSN_0=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_0",
		"ACCOUNT_DB_DSN_1=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_1",
	}, &stdout, &stderr, func(config.Account) error {
		return errors.New("open account database failed")
	})
	if code == 0 {
		t.Fatal("exit code = 0")
	}
	if strings.Contains(stdout.String()+stderr.String(), "account-plain-password") {
		t.Fatal("output contains secret material")
	}
}

type configSeen struct {
	called bool
	worker int
}
