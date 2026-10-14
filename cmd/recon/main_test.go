package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestReconRunRejectsMissingPassword(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"WORKER_ID=4",
		"ACCOUNT_DB_DSN_0=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_0?parseTime=true",
		"ACCOUNT_DB_DSN_1=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_1?parseTime=true",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	out := stdout.String() + stderr.String()
	if !strings.Contains(out, "MYSQL_RECON_PASSWORD") {
		t.Fatal("output does not contain MYSQL_RECON_PASSWORD")
	}
	if strings.Contains(out, "account-plain-password") {
		t.Fatal("output contains secret material")
	}
}
