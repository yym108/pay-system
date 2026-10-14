package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestGatewayRunRejectsMissingAdminToken(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
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

func TestGatewayRunRedacts(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
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
