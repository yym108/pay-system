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
