package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/yym108/pay-system/pkg/config"
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

func TestChannelMockRunStartsWithoutPrintingSecret(t *testing.T) {
	var stdout, stderr bytes.Buffer
	called := false
	code := runWith([]string{
		"WORKER_ID=5",
		"CHANNEL_MOCKPAY_KEY=mockpay-plain-key",
		"MYSQL_CHANNEL_MOCK_PASSWORD=mock-plain-password",
		"MOCK_DELAY_MIN_MS=100",
		"MOCK_DELAY_MAX_MS=300",
	}, &stdout, &stderr, func(cfg config.ChannelMock) error {
		called = true
		if cfg.WorkerID != 5 || cfg.ChannelMockpayKey != "mockpay-plain-key" || cfg.DSN == "" ||
			cfg.MockDelayMinMS != 100 || cfg.MockDelayMaxMS != 300 {
			t.Fatalf("start got unexpected config")
		}
		return nil
	})
	if code != 0 || !called {
		t.Fatalf("exit code = %d, called = %v, stderr = %s", code, called, stderr.String())
	}
	out := stdout.String() + stderr.String()
	if strings.Contains(out, "mockpay-plain-key") || strings.Contains(out, "mock-plain-password") {
		t.Fatal("output contains secret material")
	}
	if !strings.Contains(out, "***") {
		t.Fatal("redacted config was not printed")
	}
}

func TestChannelMockRunReportsStartError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runWith([]string{
		"WORKER_ID=5",
		"CHANNEL_MOCKPAY_KEY=mockpay-plain-key",
		"MYSQL_CHANNEL_MOCK_PASSWORD=mock-plain-password",
	}, &stdout, &stderr, func(config.ChannelMock) error {
		return errors.New("open channel_mock database failed")
	})
	if code == 0 {
		t.Fatal("exit code = 0")
	}
	if !strings.Contains(stderr.String(), "open channel_mock database failed") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), "mock-plain-password") {
		t.Fatal("output contains secret material")
	}
}
