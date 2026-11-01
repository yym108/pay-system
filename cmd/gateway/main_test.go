package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestGatewayRunRejectsMissingAdminToken(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(canceledContext(), []string{
		"WORKER_ID=1",
		"CHANNEL_MOCKPAY_KEY=mockpay-plain-key",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	out := stdout.String() + stderr.String()
	if !strings.Contains(out, "ADMIN_TOKEN") {
		t.Fatal("output does not contain ADMIN_TOKEN")
	}
	if strings.Contains(out, "mockpay-plain-key") {
		t.Fatal("output contains secret material")
	}
}

// BASE-15 验收 5：未登记的 FAULT_POINTS 不以成功配置退出；空配置仍走原来的成功路径。
func TestGatewayFaultPointsCheckedBeforeStart(t *testing.T) {
	base := []string{
		"WORKER_ID=1",
		"ADMIN_TOKEN=0123456789abcdef0123456789abcdef",
		"CHANNEL_MOCKPAY_KEY=mockpay-plain-key",
	}
	var stdout, stderr bytes.Buffer
	code := run(canceledContext(), append(append([]string{}, base...), "FAULT_POINTS=pay.after_tx"), &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "pay.after_tx") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	out := stdout.String() + stderr.String()
	if strings.Contains(out, "mockpay-plain-key") || strings.Contains(out, "0123456789abcdef0123456789abcdef") {
		t.Fatal("output contains secret material")
	}

	stdout.Reset()
	stderr.Reset()
	code = run(canceledContext(), base, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("empty FAULT_POINTS: code=%d stderr=%s", code, stderr.String())
	}
}

func TestGatewayRunRedacts(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(canceledContext(), []string{
		"WORKER_ID=1",
		"ADMIN_TOKEN=0123456789abcdef0123456789abcdef",
		"CHANNEL_MOCKPAY_KEY=mockpay-plain-key",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	out := stdout.String() + stderr.String()
	if !strings.Contains(out, "***") || !strings.Contains(out, "REDIS_ADDR=redis:6379") {
		t.Fatal("gateway startup output missing mask or redis addr")
	}
	if strings.Contains(out, "mockpay-plain-key") || strings.Contains(out, "0123456789abcdef0123456789abcdef") {
		t.Fatal("output contains secret material")
	}
}
