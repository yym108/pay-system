package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/yym108/pay-system/pkg/config"
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
	started := false
	code := runWith(paymentFixture(true), &stdout, &stderr, func(cfg config.Payment) error {
		started = cfg.WorkerID == 2 && cfg.SecretMasterKey != ""
		return nil
	})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if !started {
		t.Fatal("start did not receive the loaded config")
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

// PAY-01：启动失败时退出码非 0，输出不含 DSN 口令和密钥。
func TestPaymentStartFailureHidesSecrets(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runWith(paymentFixture(true), &stdout, &stderr, func(config.Payment) error {
		return errors.New("open payment database failed")
	})
	if code == 0 {
		t.Fatal("exit code = 0")
	}
	if !strings.Contains(stderr.String(), "open payment database failed") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	assertNoSecret(t, stdout.String()+stderr.String(), paymentMasterKey(), "payment-plain-password", "mockpay-plain-key")
}

// PAY-01：serve 打不开库时返回固定文本，不带地址和口令。端口 1 拒绝连接，不等 3 秒超时。
func TestPaymentServeHidesOpenError(t *testing.T) {
	cfg, err := config.LoadPayment([]string{
		"WORKER_ID=2",
		"MYSQL_DSN=svc_payment:payment-plain-password@tcp(127.0.0.1:1)/pay_payment?parseTime=true",
		"CHANNEL_MOCKPAY_KEY=mockpay-plain-key",
		"SECRET_MASTER_KEY=" + paymentMasterKey(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	err = serve(cfg, &logs)
	if !errors.Is(err, errOpenDB) {
		t.Fatalf("serve error = %v", err)
	}
	assertNoSecret(t, err.Error()+logs.String(), "payment-plain-password", "127.0.0.1:1", paymentMasterKey())
}

// BASE-15 验收 5：未登记的 FAULT_POINTS 在 start 之前失败；空配置仍走原来的成功路径。
func TestPaymentFaultPointsCheckedBeforeStart(t *testing.T) {
	var stdout, stderr bytes.Buffer
	started := false
	code := runWith(append(paymentFixture(true), "FAULT_POINTS=pay.after_tx"), &stdout, &stderr, func(config.Payment) error {
		started = true
		return nil
	})
	if code == 0 {
		t.Fatal("exit code = 0, want non-zero")
	}
	if started {
		t.Fatal("start was called")
	}
	if !strings.Contains(stderr.String(), "pay.after_tx") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	assertNoSecret(t, stdout.String()+stderr.String(), paymentMasterKey(), "payment-plain-password", "mockpay-plain-key")

	stdout.Reset()
	stderr.Reset()
	started = false
	code = runWith(paymentFixture(true), &stdout, &stderr, func(config.Payment) error {
		started = true
		return nil
	})
	if code != 0 || !started {
		t.Fatalf("empty FAULT_POINTS: code=%d started=%v stderr=%s", code, started, stderr.String())
	}
}

// PAY-01：主密钥解不开时不连库，直接失败。
func TestPaymentServeRejectsBadMasterKey(t *testing.T) {
	cfg := config.Payment{SecretMasterKey: "not-base64", MySQLDSN: "svc_payment:payment-plain-password@tcp(127.0.0.1:1)/pay_payment"}
	err := serve(cfg, &bytes.Buffer{})
	if err == nil || errors.Is(err, errOpenDB) {
		t.Fatalf("serve error = %v", err)
	}
	assertNoSecret(t, err.Error(), "payment-plain-password", "not-base64")
}
