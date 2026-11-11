package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/yym108/pay-system/pkg/config"
)

func runStopped(environ []string, stdout, stderr io.Writer) int {
	return runWith(environ, stdout, stderr, func(config.MerchantDemo) error { return nil })
}

func TestMerchantDemoRunRejectsMissingWorker(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "WORKER_ID") {
		t.Fatal("output does not contain WORKER_ID")
	}
}

// BASE-15 验收 5：未登记的 FAULT_POINTS 不以成功配置退出；空配置仍走原来的成功路径。
func TestMerchantDemoFaultPointsCheckedBeforeStart(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"WORKER_ID=6", "FAULT_POINTS=pay.after_tx"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "pay.after_tx") {
		t.Fatalf("stderr = %q", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = runStopped([]string{"WORKER_ID=6"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("empty FAULT_POINTS: code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "WORKER_ID=6") {
		t.Fatal("empty FAULT_POINTS did not print the loaded config")
	}
}

func TestMerchantDemoRunDefaults(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runStopped([]string{"WORKER_ID=6"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	out := stdout.String()
	if !strings.Contains(out, "WORKER_ID=6") || !strings.Contains(out, "FAIL_FIRST_N=0") {
		t.Fatal("merchant demo startup output missing plain fields")
	}
	if !strings.Contains(out, "DEMO_APP_SECRET_FORMAT=***") || strings.Contains(out, "demo_secret_") {
		t.Fatalf("secret format was not redacted: %s", out)
	}
}

func TestMerchantDemoRejectsBadSecretFormat(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"WORKER_ID=6", "DEMO_APP_SECRET_FORMAT=super-secret-%s-%s"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "DEMO_APP_SECRET_FORMAT") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if strings.Contains(stdout.String(), "super-secret") || strings.Contains(stderr.String(), "super-secret") {
		t.Fatalf("format leaked stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}
