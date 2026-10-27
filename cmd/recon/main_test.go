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

// BASE-15 验收 5：未登记的 FAULT_POINTS 不以成功配置退出；空配置仍走原来的成功路径。
func TestReconFaultPointsCheckedBeforeStart(t *testing.T) {
	base := []string{
		"WORKER_ID=4",
		"ACCOUNT_DB_DSN_0=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_0?parseTime=true",
		"ACCOUNT_DB_DSN_1=svc_account:account-plain-password@tcp(mysql:3306)/pay_account_1?parseTime=true",
		"MYSQL_RECON_PASSWORD=recon-plain-password",
	}
	var stdout, stderr bytes.Buffer
	code := run(append(append([]string{}, base...), "FAULT_POINTS=pay.after_tx"), &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "pay.after_tx") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	out := stdout.String() + stderr.String()
	if strings.Contains(out, "account-plain-password") || strings.Contains(out, "recon-plain-password") {
		t.Fatal("output contains secret material")
	}

	stdout.Reset()
	stderr.Reset()
	code = run(base, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("empty FAULT_POINTS: code=%d stderr=%s", code, stderr.String())
	}
}
