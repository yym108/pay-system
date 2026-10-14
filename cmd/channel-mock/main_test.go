package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestChannelMockRunRejectsMissingPassword(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"WORKER_ID=5",
		"CHANNEL_MOCKPAY_KEY=mockpay-plain-key",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	out := stdout.String() + stderr.String()
	if !strings.Contains(out, "MYSQL_CHANNEL_MOCK_PASSWORD") {
		t.Fatal("output does not contain MYSQL_CHANNEL_MOCK_PASSWORD")
	}
	if strings.Contains(out, "mockpay-plain-key") {
		t.Fatal("output contains secret material")
	}
}
