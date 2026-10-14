package main

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func paymentMasterKey() string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x3c}, 32))
}

func paymentFixture(withMaster bool) []string {
	env := []string{
		"WORKER_ID=2",
		"MYSQL_DSN=svc_payment:payment-plain-password@tcp(mysql:3306)/pay_payment?parseTime=true",
		"CHANNEL_MOCKPAY_KEY=mockpay-plain-key",
		"KAFKA_BROKERS=kafka:9092",
	}
	if withMaster {
		env = append(env, "SECRET_MASTER_KEY="+paymentMasterKey())
	}
	return env
}

func assertNoSecret(t *testing.T, out string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(out, secret) {
			t.Fatal("output contains secret material")
		}
	}
}

// BASE-09 验收 1：缺少 SECRET_MASTER_KEY 时 payment 启动失败，退出码非 0，输出包含变量名。
func TestPaymentMissingSecretMasterKey(t *testing.T) {
	t.Setenv("SECRET_MASTER_KEY", paymentMasterKey())
	t.Setenv("WORKER_ID", "9")
	t.Setenv("MYSQL_DSN", "svc_payment:process-dsn-password@tcp(mysql:3306)/pay_payment")
	t.Setenv("CHANNEL_MOCKPAY_KEY", "process-mock-key")

	var stdout, stderr bytes.Buffer
	code := run(paymentFixture(false), &stdout, &stderr)
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	out := stdout.String() + stderr.String()
	if !strings.Contains(out, "SECRET_MASTER_KEY") {
		t.Fatal("output does not contain SECRET_MASTER_KEY")
	}
	assertNoSecret(t, out, paymentMasterKey(), "payment-plain-password", "process-dsn-password", "mockpay-plain-key", "process-mock-key")
	if strings.Contains(out, "WORKER_ID") {
		t.Fatal("missing master key also reported WORKER_ID")
	}
}

// BASE-09 验收 2：启动输出里密钥是 ***，没有明文，非 secret 字段仍可见。
func TestPaymentStartupRedactsSecrets(t *testing.T) {
	t.Setenv("WORKER_ID", "9")
	t.Setenv("CHANNEL_MOCKPAY_KEY", "process-mock-key")
	t.Setenv("SECRET_MASTER_KEY", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, 32)))

	var stdout, stderr bytes.Buffer
	code := run(paymentFixture(true), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	out := stdout.String() + stderr.String()
	if !strings.Contains(out, "***") {
		t.Fatal("startup output has no ***")
	}
	assertNoSecret(t, out, paymentMasterKey(), "payment-plain-password", "mockpay-plain-key", "process-mock-key")
	processMaster := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, 32))
	assertNoSecret(t, out, processMaster)
	if !strings.Contains(out, "WORKER_ID=2") {
		t.Fatal("non-secret WORKER_ID is not visible")
	}
	if strings.Contains(out, "WORKER_ID=9") {
		t.Fatal("process WORKER_ID leaked into startup output")
	}
	if !strings.Contains(out, "KAFKA_BROKERS=kafka:9092") {
		t.Fatal("non-secret KAFKA_BROKERS is not visible")
	}
}
