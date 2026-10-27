package main

import (
	"bytes"
	"strings"
	"testing"
)

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
	code = run([]string{"WORKER_ID=6"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("empty FAULT_POINTS: code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "WORKER_ID=6") {
		t.Fatal("empty FAULT_POINTS did not print the loaded config")
	}
}

func TestMerchantDemoRunDefaults(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"WORKER_ID=6"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	out := stdout.String()
	if !strings.Contains(out, "WORKER_ID=6") || !strings.Contains(out, "FAIL_FIRST_N=0") {
		t.Fatal("merchant demo startup output missing plain fields")
	}
}
