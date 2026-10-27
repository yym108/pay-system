//go:build integration

package biz

import (
	"bufio"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/seed"
	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/secretbox"
)

// demoEnv 读 deploy/.env.example 的演示值。这个文件只有演示值，不是 deploy/.env。
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
		k, v, ok := strings.Cut(line, "=")
		if ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

type seededEnv struct {
	payEnv
	box *secretbox.Box
	env map[string]string
}

// openSeeded 跑真实迁移（testkit）和 BASE-03 种子，主密钥与密钥格式取自 .env.example。
func openSeeded(t *testing.T) seededEnv {
	t.Helper()
	env := openPay(t)
	demo := demoEnv(t)
	my, err := testkit.Ready(env.ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	var shards [2]*testkit.Schema
	for i := range shards {
		s, err := my.OpenSchema(env.ctx, "account")
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := s.Close(ctx); err != nil {
				t.Errorf("close account schema: %s", testkit.Redact(err))
			}
		})
		shards[i] = s
	}
	key, err := base64.StdEncoding.DecodeString(demo["SECRET_MASTER_KEY"])
	if err != nil {
		t.Fatal("SECRET_MASTER_KEY in .env.example is not base64")
	}
	paySchemaDSN := paymentDSN(t, env)
	if _, err := seed.Run(env.ctx, seed.Config{
		AccountDSN0:         shards[0].DSN(),
		AccountDSN1:         shards[1].DSN(),
		PaymentDSN:          paySchemaDSN,
		MasterKey:           key,
		PlatformSubAccounts: seed.DefaultPlatformSubAccounts,
		SecretFormat:        demo["DEMO_APP_SECRET_FORMAT"],
	}); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	box, err := secretbox.NewFromBase64(demo["SECRET_MASTER_KEY"])
	if err != nil {
		t.Fatal(err)
	}
	return seededEnv{payEnv: env, box: box, env: demo}
}

// paymentDSN 取 openPay 已打开的那个库的连接串，种子必须写进同一个库。返回值含口令，不打印。
func paymentDSN(t *testing.T, env payEnv) string {
	t.Helper()
	if env.schema == nil {
		t.Fatal("payment schema not recorded")
	}
	return env.schema.DSN()
}

// PAY-01 验收 1：真实迁移加种子后，种子商户可查到，密钥解密结果与 .env 演示值一致。
func TestPAY01SeedMerchantCredential(t *testing.T) {
	s := openSeeded(t)
	if s.env["DEMO_APP_SECRET_FORMAT"] != seed.DefaultSecretFormat {
		t.Fatalf(".env.example DEMO_APP_SECRET_FORMAT drifted from seed default")
	}
	m := NewMerchants(s.db, s.box, &clock.Fake{}, time.Minute)
	for _, id := range []string{"M100001", "M100002", "M100003"} {
		cred, err := m.GetMerchantCredential(s.ctx, "app_"+strings.ToLower(id))
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		want := strings.Replace(s.env["DEMO_APP_SECRET_FORMAT"], "%s", id, 1)
		if cred.MerchantID != id || cred.Status != "ACTIVE" || cred.SecretVersion != 1 || cred.DefaultNotifyURL != "" {
			t.Fatalf("%s credential = %s", id, cred)
		}
		if cred.AppSecret.Reveal() != want {
			t.Fatalf("%s secret does not match the .env demo value", id)
		}
	}
	// 接口 §3.7 的测试向量用这个密钥。
	cred, err := m.GetMerchantCredential(s.ctx, "app_m100001")
	if err != nil || cred.AppSecret.Reveal() != demoSecret {
		t.Fatalf("M100001 secret differs from the §3.7 vector, err=%v", err)
	}
}

// PAY-01 验收 2：不存在的 app_id 返回 20004。
func TestPAY01UnknownAppID(t *testing.T) {
	s := openSeeded(t)
	m := NewMerchants(s.db, s.box, &clock.Fake{}, time.Minute)
	// 列是 utf8mb4_0900_ai_ci，大小写或重音不同时 SQL 也能命中；只认完全相同的 app_id。
	for _, appID := range []string{"app_m100099", "APP_M100001", "app_M100001", "äpp_m100001", ""} {
		_, err := m.GetMerchantCredential(s.ctx, appID)
		assertErrno(t, err, errno.ErrMerchantInvalid)
	}
	_, err := m.MerchantConfig(s.ctx, "m100001")
	assertErrno(t, err, errno.ErrMerchantInvalid)
}

// PAY-01 规则 2：DISABLED 仍返回记录；密文坏了返回 10003。
func TestPAY01DisabledAndBadCipher(t *testing.T) {
	s := openSeeded(t)
	cipher, err := s.box.Seal([]byte("disabled-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if err := testkit.InsertMerchant(s.ctx, s.db, testkit.Merchant{
		MerchantID: "M100020", SecretCipher: cipher, SecretVersion: 4, NotifyURL: "http://merchant-demo/n", Status: "DISABLED",
	}); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err := testkit.InsertMerchant(s.ctx, s.db, testkit.Merchant{MerchantID: "M100021", SecretCipher: []byte("fixture")}); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	m := NewMerchants(s.db, s.box, &clock.Fake{}, time.Minute)
	cred, err := m.GetMerchantCredential(s.ctx, "app_m100020")
	if err != nil {
		t.Fatal(err)
	}
	if cred.Status != "DISABLED" || cred.MerchantID != "M100020" || cred.SecretVersion != 4 ||
		cred.AppSecret.Reveal() != "disabled-secret" || cred.DefaultNotifyURL != "http://merchant-demo/n" {
		t.Fatalf("disabled credential = %s", cred)
	}
	_, err = m.GetMerchantCredential(s.ctx, "app_m100021")
	assertErrno(t, err, errno.ErrInternal)
}

// PAY-01 规则 3：配置缓存按 TTL 生效；TTL 为 0 时立即看到库里的新值。费率读 merchant_fee_rate。
func TestPAY01MerchantConfigCacheAndFeeRate(t *testing.T) {
	s := openSeeded(t)
	clk := &clock.Fake{}
	clk.Set(feeAt(10, 0, 0, 0))
	cached := NewMerchants(s.db, s.box, clk, time.Minute)
	direct := NewMerchants(s.db, s.box, clk, 0)

	first, err := cached.MerchantConfig(s.ctx, "M100002")
	if err != nil || first.Status != "ACTIVE" || first.DefaultNotifyURL != "" {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	if _, err := s.db.ExecContext(s.ctx,
		`UPDATE merchant SET notify_url = ?, status = 'DISABLED' WHERE merchant_id = ?`,
		"http://merchant-demo/new", "M100002"); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	hit, err := cached.MerchantConfig(s.ctx, "M100002")
	if err != nil || hit != first {
		t.Fatalf("within ttl = %+v err=%v", hit, err)
	}
	now, err := direct.MerchantConfig(s.ctx, "M100002")
	if err != nil || now.Status != "DISABLED" || now.DefaultNotifyURL != "http://merchant-demo/new" {
		t.Fatalf("ttl=0 = %+v err=%v", now, err)
	}
	clk.Advance(time.Minute)
	fresh, err := cached.MerchantConfig(s.ctx, "M100002")
	if err != nil || fresh != now {
		t.Fatalf("after ttl = %+v err=%v", fresh, err)
	}
	_, err = cached.MerchantConfig(s.ctx, "M100099")
	assertErrno(t, err, errno.ErrMerchantInvalid)

	// 种子把 merchant.fee_rate_bp 回填成第一档；之后的生效费率只看 merchant_fee_rate。
	later := time.Date(2030, 1, 1, 0, 0, 0, 0, feeZone)
	rate, ok, err := cached.FeeRateAt(s.ctx, "M100002", later)
	if err != nil || !ok || rate.RateBP != 60 {
		t.Fatalf("seed tier = %+v ok=%v err=%v", rate, ok, err)
	}
	svc := New(s.db)
	if _, err := svc.AppendFeeRate(s.ctx, "M100002", 80, later); err != nil {
		t.Fatal(err)
	}
	rate, ok, err = cached.FeeRateAt(s.ctx, "M100002", later)
	if err != nil || !ok || rate.RateBP != 80 {
		t.Fatalf("appended tier = %+v ok=%v err=%v", rate, ok, err)
	}
	if merchantRate(t, s.payEnv, "M100002") != 60 {
		t.Fatal("merchant.fee_rate_bp changed")
	}
	rate, ok, err = cached.FeeRateAt(s.ctx, "M100002", later.Add(-time.Millisecond))
	if err != nil || !ok || rate.RateBP != 60 {
		t.Fatalf("before new tier = %+v ok=%v err=%v", rate, ok, err)
	}
	_, ok, err = cached.FeeRateAt(s.ctx, "M100002", time.Date(2000, 1, 1, 0, 0, 0, 0, feeZone))
	if err != nil || ok {
		t.Fatalf("before seed tier ok=%v err=%v", ok, err)
	}
}
