//go:build integration

package handler

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/seed"
	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/secretbox"
	"github.com/yym108/pay-system/service/payment/biz"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := testkit.Shutdown(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %s\n", testkit.Redact(err))
	}
	os.Exit(code)
}

// PAY-01 验收 1、2：真实迁移、BASE-03 种子、真实 biz 和真实 Kitex 客户端。
// 主密钥和密钥格式取自 deploy/.env.example（只有演示值）。
func TestRPCMerchantCredentialSeeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	demo := demoEnv(t)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	var schemas [3]*testkit.Schema
	for i, service := range []string{"payment", "account", "account"} {
		s, err := my.OpenSchema(ctx, service)
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
		t.Cleanup(func() { closeSchema(t, s) })
		schemas[i] = s
	}
	pay, acc0, acc1 := schemas[0], schemas[1], schemas[2]
	key, err := base64.StdEncoding.DecodeString(demo["SECRET_MASTER_KEY"])
	if err != nil {
		t.Fatal("SECRET_MASTER_KEY in .env.example is not base64")
	}
	if _, err := seed.Run(ctx, seed.Config{
		AccountDSN0: acc0.DSN(), AccountDSN1: acc1.DSN(), PaymentDSN: pay.DSN(),
		MasterKey: key, PlatformSubAccounts: seed.DefaultPlatformSubAccounts,
		SecretFormat: demo["DEMO_APP_SECRET_FORMAT"],
	}); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	box, err := secretbox.NewFromBase64(demo["SECRET_MASTER_KEY"])
	if err != nil {
		t.Fatal(err)
	}
	merchants := biz.NewMerchants(pay.DB, box, clock.Real{}, time.Minute)
	ids, err := idgen.New(1, clock.Real{})
	if err != nil {
		t.Fatal(err)
	}
	payments := biz.NewPayments(merchants, ids, clock.Real{}, "merchant-demo", pay.DB)
	svc := biz.NewApp(merchants, payments)
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, 2*time.Second)

	for _, id := range []string{"M100001", "M100002", "M100003"} {
		got, err := cli.GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{AppId: "app_" + strings.ToLower(id)})
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		want := strings.Replace(demo["DEMO_APP_SECRET_FORMAT"], "%s", id, 1)
		if got.GetMerchantId() != id || got.GetStatus() != "ACTIVE" || got.GetSecretVersion() != 1 || got.GetDefaultNotifyUrl() != "" {
			t.Fatalf("%s: merchant=%s status=%s version=%d", id, got.GetMerchantId(), got.GetStatus(), got.GetSecretVersion())
		}
		if got.GetAppSecret() != want {
			t.Fatalf("%s: secret does not match the .env demo value", id)
		}
	}
	got, err := cli.GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{AppId: "app_m100001"})
	if err != nil || got.GetAppSecret() != testSecret {
		t.Fatalf("M100001 secret differs from the §3.7 vector, err=%v", err)
	}

	// 验收 2：不存在的 app_id 返回 20004。
	_, err = cli.GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{AppId: "app_m100099"})
	assertBizCode(t, "unknown app_id", err, errno.ErrMerchantInvalid.Code)

	// DISABLED 仍返回记录。
	if _, err := pay.DB.ExecContext(ctx, `UPDATE merchant SET status = 'DISABLED' WHERE merchant_id = ?`, "M100003"); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err = cli.GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{AppId: "app_m100003"})
	if err != nil || got.GetStatus() != "DISABLED" || got.GetMerchantId() != "M100003" {
		t.Fatalf("disabled: status=%s err=%v", got.GetStatus(), err)
	}

	// PAY-02：CreatePayment 已实现。nil 与 expire_minutes=0 是 10001，不再是未实现的 10003。
	_, err = New(svc).CreatePayment(ctx, nil)
	assertBizCode(t, "nil create", err, errno.ErrInvalidParam.Code)
	zero := int32(0)
	_, err = cli.CreatePayment(ctx, &payment.CreatePaymentRequest{
		MerchantId: "M100002", OutTradeNo: "RPC-BAD", Amount: 10000, Subject: "demo", ExpireMinutes: &zero,
	})
	assertBizCode(t, "expire 0", err, errno.ErrInvalidParam.Code)
	created, err := cli.CreatePayment(ctx, &payment.CreatePaymentRequest{
		MerchantId: "M100002", OutTradeNo: "RPC-OK", Amount: 10000, Subject: "demo",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetFeeAmount() != 60 || created.GetStatus() != "INIT" || created.GetAmount() != 10000 ||
		len(created.GetPayNo()) != 21 || created.GetPayNo()[:2] != "PA" {
		t.Fatalf("created = %+v", created)
	}
	_, err = cli.Pay(ctx, &payment.PayRequest{MerchantId: "M100002", PayNo: created.GetPayNo(), Channel: "mockpay"})
	assertBizCode(t, "pay unimplemented", err, errno.ErrInternal.Code)
}

// closeSchema 用新的上下文：测试的 ctx 在清理时可能已经取消。
func closeSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}

func demoEnv(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.DirFS(filepath.Join(repoRoot(t), "deploy")).Open(".env.example")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
