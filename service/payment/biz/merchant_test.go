package biz

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/secretbox"
)

// demoSecret 是接口 §3.7 的 M100001 测试向量密钥，也是 BASE-03 种子写入的演示值。
const demoSecret = "demo_secret_M100001_0123456789ab" //nolint:gosec // 演示值，见约定 §8。

func testBox(t *testing.T, fill byte) *secretbox.Box {
	t.Helper()
	box, err := secretbox.NewFromBase64(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, secretbox.KeySize)))
	if err != nil {
		t.Fatal(err)
	}
	return box
}

func seal(t *testing.T, box *secretbox.Box, plain string) []byte {
	t.Helper()
	out, err := box.Seal([]byte(plain))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// fakeMerchantStore 是不连库的商户读接口。计数用来断言缓存是否命中。
type fakeMerchantStore struct {
	mu       sync.Mutex
	byApp    map[string]credentialRow
	byID     map[string]MerchantConfig
	rates    []FeeRate
	err      error
	credRead int
	cfgRead  int
	rateRead int
	lastAt   time.Time
}

func newFakeStore() *fakeMerchantStore {
	return &fakeMerchantStore{byApp: map[string]credentialRow{}, byID: map[string]MerchantConfig{}}
}

func (f *fakeMerchantStore) credentialByAppID(_ context.Context, appID string) (credentialRow, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.credRead++
	if f.err != nil {
		return credentialRow{}, false, f.err
	}
	row, ok := f.byApp[appID]
	return row, ok, nil
}

func (f *fakeMerchantStore) credentialByMerchantID(_ context.Context, merchantID string) (credentialRow, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.credRead++
	if f.err != nil {
		return credentialRow{}, false, f.err
	}
	for _, row := range f.byApp {
		if row.MerchantID == merchantID {
			return row, true, nil
		}
	}
	return credentialRow{}, false, nil
}

func (f *fakeMerchantStore) configByID(_ context.Context, merchantID string) (MerchantConfig, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfgRead++
	if f.err != nil {
		return MerchantConfig{}, false, f.err
	}
	row, ok := f.byID[merchantID]
	return row, ok, nil
}

func (f *fakeMerchantStore) feeRateAt(_ context.Context, merchantID string, at time.Time) (FeeRate, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rateRead++
	f.lastAt = at
	if f.err != nil {
		return FeeRate{}, false, f.err
	}
	var best FeeRate
	found := false
	for _, row := range f.rates {
		if row.MerchantID != merchantID || row.EffectiveFrom.After(at) {
			continue
		}
		if !found || row.EffectiveFrom.After(best.EffectiveFrom) {
			best, found = row, true
		}
	}
	return best, found, nil
}

func (f *fakeMerchantStore) setConfig(cfg MerchantConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byID[cfg.MerchantID] = cfg
}

func (f *fakeMerchantStore) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeMerchantStore) reads() (cred, cfg, rate int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.credRead, f.cfgRead, f.rateRead
}

func fakeClock() *clock.Fake {
	clk := &clock.Fake{}
	clk.Set(feeAt(10, 0, 0, 0))
	return clk
}

// captureLog 把 slog 默认输出接到缓冲区，测试结束后恢复为丢弃。
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	logx.SetDefault("payment", buf)
	t.Cleanup(func() { logx.SetDefault("payment", io.Discard) })
	return buf
}

// PAY-01 验收 1（单元）：密文解开后返回明文与其余字段。
func TestGetMerchantCredentialDecrypts(t *testing.T) {
	box := testBox(t, 0)
	st := newFakeStore()
	st.byApp["app_m100001"] = credentialRow{
		MerchantID: "M100001", Status: "ACTIVE", Cipher: seal(t, box, demoSecret),
		SecretVersion: 1, NotifyURL: "http://merchant-demo/notify",
	}
	m := newMerchants(st, box, fakeClock(), time.Minute)
	got, err := m.GetMerchantCredential(context.Background(), "app_m100001")
	if err != nil {
		t.Fatal(err)
	}
	if got.MerchantID != "M100001" || got.Status != "ACTIVE" || got.AppSecret.Reveal() != demoSecret ||
		got.SecretVersion != 1 || got.DefaultNotifyURL != "http://merchant-demo/notify" {
		t.Fatalf("credential mismatch: merchant=%s status=%s version=%d notify=%s secretOK=%v",
			got.MerchantID, got.Status, got.SecretVersion, got.DefaultNotifyURL, got.AppSecret.Reveal() == demoSecret)
	}
}

// PAY-01 规则 2：DISABLED 商户仍返回记录，由网关决定返回 20004。
func TestGetMerchantCredentialDisabledStillReturned(t *testing.T) {
	box := testBox(t, 0)
	st := newFakeStore()
	st.byApp["app_m100009"] = credentialRow{
		MerchantID: "M100009", Status: "DISABLED", Cipher: seal(t, box, "disabled-secret"), SecretVersion: 3,
	}
	m := newMerchants(st, box, fakeClock(), time.Minute)
	got, err := m.GetMerchantCredential(context.Background(), "app_m100009")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "DISABLED" || got.MerchantID != "M100009" || got.SecretVersion != 3 || got.AppSecret.Reveal() != "disabled-secret" {
		t.Fatalf("disabled merchant: status=%s merchant=%s version=%d", got.Status, got.MerchantID, got.SecretVersion)
	}
}

// PAY-01 验收 2（单元）：不存在或为空的 app_id 返回 20004。空串不查库。
func TestGetMerchantCredentialUnknown(t *testing.T) {
	st := newFakeStore()
	m := newMerchants(st, testBox(t, 0), fakeClock(), time.Minute)
	_, err := m.GetMerchantCredential(context.Background(), "app_missing")
	assertErrno(t, err, errno.ErrMerchantInvalid)
	_, err = m.GetMerchantCredential(context.Background(), "")
	assertErrno(t, err, errno.ErrMerchantInvalid)
	if cred, _, _ := st.reads(); cred != 1 {
		t.Fatalf("store reads = %d, want 1 (empty app_id must not query)", cred)
	}
}

// PAY-01：解密失败返回 10003，ERROR 日志只有 app_id、merchant_id，不含密文和明文。
func TestGetMerchantCredentialDecryptFailure(t *testing.T) {
	sealer := testBox(t, 0)
	good := seal(t, sealer, demoSecret)
	tampered := append([]byte(nil), good...)
	tampered[len(tampered)-1] ^= 0xff
	badVersion := append([]byte(nil), good...)
	badVersion[0] = 9
	cases := []struct {
		name   string
		box    *secretbox.Box
		cipher []byte
	}{
		{"wrong master key", testBox(t, 0x5a), good},
		{"tampered", sealer, tampered},
		{"bad version", sealer, badVersion},
		{"malformed", sealer, []byte("fixture")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLog(t)
			st := newFakeStore()
			st.byApp["app_m100001"] = credentialRow{MerchantID: "M100001", Status: "ACTIVE", Cipher: tc.cipher, SecretVersion: 1}
			m := newMerchants(st, tc.box, fakeClock(), time.Minute)
			got, err := m.GetMerchantCredential(context.Background(), "app_m100001")
			assertErrno(t, err, errno.ErrInternal)
			if e := errno.From(err); e.Unwrap() != nil {
				t.Fatal("decrypt failure must be the bare 10003 sentinel")
			}
			if got.AppSecret.Reveal() != "" || got.MerchantID != "" {
				t.Fatal("decrypt failure returned fields")
			}
			logged := buf.String()
			if !strings.Contains(logged, `"level":"ERROR"`) ||
				!strings.Contains(logged, `"app_id":"app_m100001"`) ||
				!strings.Contains(logged, `"merchant_id":"M100001"`) {
				t.Fatalf("ERROR log missing ids: %s", logged)
			}
			assertNoCipher(t, logged, tc.cipher)
			if strings.Contains(logged, demoSecret) {
				t.Fatal("log contains plaintext secret")
			}
		})
	}
}

func assertNoCipher(t *testing.T, out string, cipher []byte) {
	t.Helper()
	forms := []string{
		string(cipher),
		hex.EncodeToString(cipher),
		strings.ToUpper(hex.EncodeToString(cipher)),
		base64.StdEncoding.EncodeToString(cipher),
		fmt.Sprintf("%v", cipher),
	}
	for _, form := range forms {
		if form != "" && strings.Contains(out, form) {
			t.Fatalf("log contains ciphertext form %q", form)
		}
	}
}

// PAY-01 规则 1：凭证在 fmt 与 slog 中都不打印明文。
func TestMerchantCredentialRedacted(t *testing.T) {
	cred := MerchantCredential{
		MerchantID: "M100001", Status: "ACTIVE", AppSecret: NewSecret(demoSecret), SecretVersion: 1, DefaultNotifyURL: "http://x",
	}
	for _, out := range []string{
		fmt.Sprintf("%v", cred), fmt.Sprintf("%+v", cred), fmt.Sprintf("%#v", cred), fmt.Sprintf("%s", cred), //nolint:gocritic,staticcheck // 逐个覆盖格式动词
		fmt.Sprintf("%v", &cred), fmt.Sprint(cred), fmt.Sprintf("%q", cred), cred.String(),
	} {
		if strings.Contains(out, demoSecret) || !strings.Contains(out, "M100001") {
			t.Fatalf("formatted credential = %q", out)
		}
	}
	buf := &bytes.Buffer{}
	logger := logx.New("payment", buf)
	logger.Info("cred", "c", cred, "p", &cred)
	slog.New(slog.NewJSONHandler(buf, nil)).Info("raw", "c", cred)
	slog.New(slog.NewTextHandler(buf, nil)).Info("raw", "c", cred)
	if strings.Contains(buf.String(), demoSecret) {
		t.Fatalf("slog leaked secret: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "M100001") {
		t.Fatalf("slog dropped merchant id: %s", buf.String())
	}
}

// PAY-01 规则 1：凭证嵌在切片、结构体（含未导出字段）、map 里，经 fmt、json.Marshal、
// slog JSON / Text handler 输出时都不含明文；只有 Reveal 取得到明文。
func TestSecretRedactedWhenNested(t *testing.T) {
	cred := MerchantCredential{MerchantID: "M100001", Status: "ACTIVE", AppSecret: NewSecret(demoSecret), SecretVersion: 1}
	type exported struct{ Cred MerchantCredential }
	type unexported struct{ cred MerchantCredential }
	type rawField struct{ secret Secret }
	values := map[string]any{
		"secret":            cred.AppSecret,
		"secret ptr":        &cred.AppSecret,
		"slice":             []MerchantCredential{cred},
		"ptr slice":         []*MerchantCredential{&cred},
		"nested":            exported{Cred: cred},
		"nested ptr":        &exported{Cred: cred},
		"unexported":        unexported{cred: cred},
		"unexported ptr":    &unexported{cred: cred},
		"unexported secret": rawField{secret: cred.AppSecret},
		"map":               map[string]MerchantCredential{"m": cred},
		"secret slice":      []Secret{cred.AppSecret},
	}
	verbs := []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"}
	for name, v := range values {
		for _, verb := range verbs {
			if out := fmt.Sprintf(verb, v); strings.Contains(out, demoSecret) || strings.Contains(out, hex.EncodeToString([]byte(demoSecret))) {
				t.Fatalf("%s %s leaked: %s", name, verb, out)
			}
		}
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("%s json: %v", name, err)
		}
		if strings.Contains(string(j), demoSecret) {
			t.Fatalf("%s json leaked: %s", name, j)
		}
		buf := &bytes.Buffer{}
		slog.New(slog.NewJSONHandler(buf, nil)).Info("x", "v", v)
		slog.New(slog.NewTextHandler(buf, nil)).Info("x", "v", v)
		logx.New("payment", buf).Info("x", "v", v)
		if strings.Contains(buf.String(), demoSecret) {
			t.Fatalf("%s slog leaked: %s", name, buf.String())
		}
	}

	if j, _ := json.Marshal(cred); !strings.Contains(string(j), `"AppSecret":"***"`) {
		t.Fatalf("json = %s", j)
	}
	if got := fmt.Sprintf("%q", cred.AppSecret); got != `"***"` {
		t.Fatalf("%%q = %s", got)
	}
	if cred.AppSecret.String() != "***" || cred.AppSecret.GoString() != `"***"` || cred.AppSecret.LogValue().String() != "***" {
		t.Fatal("secret methods must redact")
	}
	if cred.AppSecret.Reveal() != demoSecret {
		t.Fatal("Reveal must return plaintext")
	}
	var zero Secret
	if zero.Reveal() != "" || zero.String() != "***" || NewSecret("").Reveal() != "" {
		t.Fatal("zero secret")
	}
}

func TestGetMerchantCredentialErrors(t *testing.T) {
	box := testBox(t, 0)
	ctx := context.Background()

	st := newFakeStore()
	st.setErr(errors.New("dial tcp 10.0.0.1: password=xxx"))
	_, err := newMerchants(st, box, fakeClock(), time.Minute).GetMerchantCredential(ctx, "app_m100001")
	assertErrno(t, err, errno.ErrInternal)
	if errno.From(err).Unwrap() == nil {
		t.Fatal("db error cause must be kept for the handler log")
	}

	st.setErr(context.DeadlineExceeded)
	_, err = newMerchants(st, box, fakeClock(), time.Minute).GetMerchantCredential(ctx, "app_m100001")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline = %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = newMerchants(newFakeStore(), box, fakeClock(), time.Minute).GetMerchantCredential(canceled, "app_m100001")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}

	var nilM *Merchants
	_, err = nilM.GetMerchantCredential(ctx, "app_m100001")
	assertErrno(t, err, errno.ErrInternal)
	//nolint:staticcheck // 故意传 nil ctx
	_, err = newMerchants(newFakeStore(), box, fakeClock(), time.Minute).GetMerchantCredential(nil, "app_m100001")
	assertErrno(t, err, errno.ErrInternal)
	_, err = newMerchants(nil, box, fakeClock(), time.Minute).GetMerchantCredential(ctx, "app_m100001")
	assertErrno(t, err, errno.ErrInternal)
	_, err = newMerchants(newFakeStore(), nil, fakeClock(), time.Minute).GetMerchantCredential(ctx, "app_m100001")
	assertErrno(t, err, errno.ErrInternal)
	_, err = NewMerchants(nil, box, fakeClock(), time.Minute).GetMerchantCredential(ctx, "app_m100001")
	assertErrno(t, err, errno.ErrInternal)
}

// PAY-01 规则 3：TTL 内命中缓存，到 TTL 重新读库。
func TestMerchantConfigCacheHitAndExpire(t *testing.T) {
	st := newFakeStore()
	st.setConfig(MerchantConfig{MerchantID: "M100002", Status: "ACTIVE", DefaultNotifyURL: "http://a"})
	clk := fakeClock()
	m := newMerchants(st, testBox(t, 0), clk, 60*time.Second)
	ctx := context.Background()

	first, err := m.MerchantConfig(ctx, "M100002")
	if err != nil || first.DefaultNotifyURL != "http://a" || first.Status != "ACTIVE" || first.MerchantID != "M100002" {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	st.setConfig(MerchantConfig{MerchantID: "M100002", Status: "DISABLED", DefaultNotifyURL: "http://b"})
	clk.Advance(59*time.Second + 999*time.Millisecond)
	hit, err := m.MerchantConfig(ctx, "M100002")
	if err != nil || hit != first {
		t.Fatalf("within ttl = %+v err=%v", hit, err)
	}
	if _, cfg, _ := st.reads(); cfg != 1 {
		t.Fatalf("reads within ttl = %d", cfg)
	}
	clk.Advance(time.Millisecond)
	fresh, err := m.MerchantConfig(ctx, "M100002")
	if err != nil || fresh.Status != "DISABLED" || fresh.DefaultNotifyURL != "http://b" {
		t.Fatalf("after ttl = %+v err=%v", fresh, err)
	}
	if _, cfg, _ := st.reads(); cfg != 2 {
		t.Fatalf("reads after ttl = %d", cfg)
	}
	// 重新装载后从新的时刻起算。
	clk.Advance(30 * time.Second)
	if _, err := m.MerchantConfig(ctx, "M100002"); err != nil {
		t.Fatal(err)
	}
	if _, cfg, _ := st.reads(); cfg != 2 {
		t.Fatalf("reload did not restart ttl, reads = %d", cfg)
	}
}

// PAY-01 规则 3 / OPT-04：TTL 为 0 时每次读库。
func TestMerchantConfigTTLZeroAlwaysReads(t *testing.T) {
	st := newFakeStore()
	st.setConfig(MerchantConfig{MerchantID: "M100002", Status: "ACTIVE", DefaultNotifyURL: "http://a"})
	m := newMerchants(st, testBox(t, 0), fakeClock(), 0)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := m.MerchantConfig(ctx, "M100002"); err != nil {
			t.Fatal(err)
		}
	}
	st.setConfig(MerchantConfig{MerchantID: "M100002", Status: "DISABLED"})
	got, err := m.MerchantConfig(ctx, "M100002")
	if err != nil || got.Status != "DISABLED" {
		t.Fatalf("ttl=0 = %+v err=%v", got, err)
	}
	if _, cfg, _ := st.reads(); cfg != 4 {
		t.Fatalf("reads = %d, want 4", cfg)
	}
	neg := newMerchants(st, testBox(t, 0), fakeClock(), -time.Second)
	_, _ = neg.MerchantConfig(ctx, "M100002")
	_, _ = neg.MerchantConfig(ctx, "M100002")
	if _, cfg, _ := st.reads(); cfg != 6 {
		t.Fatalf("negative ttl reads = %d, want 6", cfg)
	}
}

// 时钟回拨时缓存视为过期，不让一条旧记录在回拨期间多活。
func TestMerchantConfigClockRollbackRefreshes(t *testing.T) {
	st := newFakeStore()
	st.setConfig(MerchantConfig{MerchantID: "M100002", Status: "ACTIVE"})
	clk := fakeClock()
	m := newMerchants(st, testBox(t, 0), clk, time.Minute)
	ctx := context.Background()
	if _, err := m.MerchantConfig(ctx, "M100002"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(-time.Second)
	if _, err := m.MerchantConfig(ctx, "M100002"); err != nil {
		t.Fatal(err)
	}
	if _, cfg, _ := st.reads(); cfg != 2 {
		t.Fatalf("reads after rollback = %d, want 2", cfg)
	}
}

// 不存在与读库失败都不进缓存，键是 merchant_id。
func TestMerchantConfigMissAndErrorNotCached(t *testing.T) {
	st := newFakeStore()
	clk := fakeClock()
	m := newMerchants(st, testBox(t, 0), clk, time.Minute)
	ctx := context.Background()

	_, err := m.MerchantConfig(ctx, "M100099")
	assertErrno(t, err, errno.ErrMerchantInvalid)
	st.setConfig(MerchantConfig{MerchantID: "M100099", Status: "ACTIVE"})
	got, err := m.MerchantConfig(ctx, "M100099")
	if err != nil || got.MerchantID != "M100099" {
		t.Fatalf("after insert = %+v err=%v", got, err)
	}

	st.setErr(errors.New("boom"))
	_, err = m.MerchantConfig(ctx, "M100002")
	assertErrno(t, err, errno.ErrInternal)
	st.setErr(nil)
	st.setConfig(MerchantConfig{MerchantID: "M100002", Status: "ACTIVE"})
	if got, err := m.MerchantConfig(ctx, "M100002"); err != nil || got.MerchantID != "M100002" {
		t.Fatalf("after error = %+v err=%v", got, err)
	}
	// M100099 仍在缓存，M100002 刚装载；两者各自计时。
	if got, err := m.MerchantConfig(ctx, "M100099"); err != nil || got.MerchantID != "M100099" {
		t.Fatalf("cached = %+v err=%v", got, err)
	}
	if _, cfg, _ := st.reads(); cfg != 4 {
		t.Fatalf("reads = %d, want 4", cfg)
	}

	_, err = m.MerchantConfig(ctx, "")
	assertErrno(t, err, errno.ErrMerchantInvalid)
	if _, cfg, _ := st.reads(); cfg != 4 {
		t.Fatal("empty merchant_id queried the store")
	}
}

func TestMerchantConfigGuards(t *testing.T) {
	ctx := context.Background()
	var nilM *Merchants
	_, err := nilM.MerchantConfig(ctx, "M100002")
	assertErrno(t, err, errno.ErrInternal)
	//nolint:staticcheck // 故意传 nil ctx
	_, err = newMerchants(newFakeStore(), nil, fakeClock(), time.Minute).MerchantConfig(nil, "M100002")
	assertErrno(t, err, errno.ErrInternal)
	_, err = newMerchants(nil, nil, fakeClock(), time.Minute).MerchantConfig(ctx, "M100002")
	assertErrno(t, err, errno.ErrInternal)
	_, err = newMerchants(newFakeStore(), nil, nil, time.Minute).MerchantConfig(ctx, "M100002")
	assertErrno(t, err, errno.ErrInternal)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = newMerchants(newFakeStore(), nil, fakeClock(), time.Minute).MerchantConfig(canceled, "M100002")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	// 读配置不需要主密钥。
	st := newFakeStore()
	st.setConfig(MerchantConfig{MerchantID: "M100002", Status: "ACTIVE"})
	if _, err := newMerchants(st, nil, fakeClock(), time.Minute).MerchantConfig(ctx, "M100002"); err != nil {
		t.Fatalf("config without box = %v", err)
	}
}

// PAY-01 规则 3：并发读同一缓存，带 -race 运行。
func TestMerchantConfigConcurrentReads(t *testing.T) {
	st := newFakeStore()
	for _, id := range []string{"M100001", "M100002", "M100003"} {
		st.setConfig(MerchantConfig{MerchantID: id, Status: "ACTIVE"})
	}
	clk := fakeClock()
	m := newMerchants(st, testBox(t, 0), clk, 10*time.Second)
	ctx := context.Background()
	const workers = 64
	const rounds = 50
	ids := []string{"M100001", "M100002", "M100003"}
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				id := ids[(w+i)%len(ids)]
				got, err := m.MerchantConfig(ctx, id)
				if err != nil || got.MerchantID != id {
					errs <- fmt.Errorf("worker %d: %+v %w", w, got, err)
					return
				}
				if w == 0 && i%10 == 0 {
					clk.Advance(5 * time.Second)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if _, cfg, _ := st.reads(); cfg >= workers*rounds {
		t.Fatalf("cache never hit: reads = %d", cfg)
	}
}

// PAY-01 规则 3 与 PAY-10 同口径：生效费率读 merchant_fee_rate，取 effective_from <= at 的最近一档，不走缓存。
func TestFeeRateAt(t *testing.T) {
	st := newFakeStore()
	t0 := feeAt(10, 0, 0, 0)
	t1 := feeAt(11, 0, 0, 0)
	st.rates = []FeeRate{
		{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t0},
		{MerchantID: "M100002", RateBP: 80, EffectiveFrom: t1},
	}
	m := newMerchants(st, nil, fakeClock(), time.Minute)
	ctx := context.Background()
	cases := []struct {
		at    time.Time
		found bool
		rate  int
	}{
		{t0.Add(-time.Millisecond), false, 0},
		{t0, true, 60},
		{t1.Add(-time.Millisecond), true, 60},
		{t1, true, 80},
		{t1.Add(time.Hour), true, 80},
	}
	for _, tc := range cases {
		got, ok, err := m.FeeRateAt(ctx, "M100002", tc.at)
		if err != nil || ok != tc.found || got.RateBP != tc.rate {
			t.Fatalf("at %s = %+v ok=%v err=%v", tc.at, got, ok, err)
		}
	}
	if _, _, rate := st.reads(); rate != len(cases) {
		t.Fatalf("fee rate reads = %d, want %d (no cache)", rate, len(cases))
	}

	_, _, err := m.FeeRateAt(ctx, "M100002", time.Time{})
	assertErrno(t, err, errno.ErrInvalidParam)
	_, _, err = m.FeeRateAt(ctx, "", t0)
	assertErrno(t, err, errno.ErrInvalidParam)
	st.setErr(errors.New("boom"))
	_, _, err = m.FeeRateAt(ctx, "M100002", t0)
	assertErrno(t, err, errno.ErrInternal)
	var nilM *Merchants
	_, _, err = nilM.FeeRateAt(ctx, "M100002", t0)
	assertErrno(t, err, errno.ErrInternal)
	_, _, err = newMerchants(nil, nil, fakeClock(), time.Minute).FeeRateAt(ctx, "M100002", t0)
	assertErrno(t, err, errno.ErrInternal)
}
