package main

import (
	"bytes"
	"strings"
	"testing"
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
