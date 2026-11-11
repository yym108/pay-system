package gateway

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/sign"
)

func gw05Start(t *testing.T, store NonceStore, opts ...Option) (string, *hitBox) {
	t.Helper()
	base := []Option{
		WithClock(gw04Clock()),
		WithMerchantLookup(gw02Lookup()),
		WithNonceStore(store),
	}
	base = append(base, opts...)
	var hit hitBox
	addr := startGW(t, NewMerchant(base...), func(r *server.Hertz) {
		r.POST("/v1/gw05", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	return addr, &hit
}

func gw05Post(t *testing.T, addr, appID, nonce string, body []byte) gwResp {
	t.Helper()
	const path = "/v1/gw05"
	ts := strconv.FormatInt(gw04Now, 10)
	hdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, appID, ts, nonce, body)
	return do(t, http.MethodPost, "http://"+addr+path, body, hdr)
}

func assertRateLimited(t *testing.T, got gwResp) {
	t.Helper()
	assertSignedErrno(t, got, errno.ErrRateLimited)
	ra := got.header.Get("Retry-After")
	n, err := strconv.Atoi(ra)
	if err != nil || strconv.Itoa(n) != ra || n < 1 {
		t.Fatalf("Retry-After %q", ra)
	}
	if strings.Contains(string(got.body), gw03Secret) {
		t.Fatalf("body leaked secret %s", got.body)
	}
}

// GW-05 验收 1：默认 100/s、突发 200，Fake 时钟停在同一时刻连续 300 个已验签请求，恰好 200 个进入 handler，其余 100 个是 429 / 10005 且带 Retry-After。被拒绝的 100 个不调用 nonce 存储。
func TestGW05DefaultBurst(t *testing.T) {
	store := &callNonce{inner: NewMemoryNonce()}
	logs := newLogBuf()
	addr, hit := gw05Start(t, store, WithLogger(logx.New("gateway", logs)))
	body := []byte(`{"n":1}`)
	var rejectedNonce, rejectedSig string
	passed, limited := 0, 0
	for range 300 {
		nonce := gw04Nonce()
		before := store.calls()
		hits, _, _ := hit.snapshot()
		got := gw05Post(t, addr, gw03AppID, nonce, body)
		if got.status == http.StatusOK {
			passed++
			if store.calls() != before+1 {
				t.Fatalf("passed request did not reserve nonce, store %d", store.calls())
			}
			n, id, ok := hit.snapshot()
			if n != hits+1 || !ok || id != gw03MID {
				t.Fatalf("handler n=%d id=%q ok=%v", n, id, ok)
			}
			continue
		}
		limited++
		assertRateLimited(t, got)
		if got.header.Get("Retry-After") != "1" {
			t.Fatalf("Retry-After %q, want 1", got.header.Get("Retry-After"))
		}
		if store.calls() != before {
			t.Fatalf("limited request reserved nonce, store %d", store.calls())
		}
		if n, _, _ := hit.snapshot(); n != hits {
			t.Fatal("handler called on limited request")
		}
		if strings.Contains(string(got.body), nonce) {
			t.Fatalf("body contains nonce %s", got.body)
		}
		if rejectedNonce == "" {
			rejectedNonce = nonce
			rejectedSig = got.header.Get(sign.HeaderSignature)
		}
	}
	if passed != 200 || limited != 100 {
		t.Fatalf("passed %d limited %d", passed, limited)
	}
	if store.calls() != 200 {
		t.Fatalf("nonce reserves %d, want 200", store.calls())
	}
	text := logs.String()
	if strings.Contains(text, gw03Secret) || strings.Contains(text, rejectedNonce) || (rejectedSig != "" && strings.Contains(text, rejectedSig)) {
		t.Fatalf("log leaked secret, nonce, or signature: %s", text)
	}
}

// GW-05 验收 2：一个 app_id 打满不影响另一个 app_id。
func TestGW05OtherAppUnaffected(t *testing.T) {
	store := &callNonce{inner: NewMemoryNonce()}
	addr, hit := gw05Start(t, store)
	body := []byte(`{"n":1}`)
	for range defaultBurst {
		got := gw05Post(t, addr, gw03AppID, gw04Nonce(), body)
		if got.status != http.StatusOK {
			t.Fatalf("fill status %d body %s", got.status, got.body)
		}
	}
	got := gw05Post(t, addr, gw03AppID, gw04Nonce(), body)
	assertRateLimited(t, got)
	const other = "app_m100002"
	before := store.calls()
	hits, _, _ := hit.snapshot()
	got = gw05Post(t, addr, other, gw04Nonce(), body)
	if got.status != http.StatusOK {
		t.Fatalf("other status %d body %s", got.status, got.body)
	}
	n, id, ok := hit.snapshot()
	if n != hits+1 || !ok || id != gw03MID {
		t.Fatalf("other handler n=%d id=%q ok=%v", n, id, ok)
	}
	if store.calls() != before+1 {
		t.Fatalf("other app nonce reserves %d, before %d", store.calls(), before)
	}
}

// GW-05：覆盖把 app_m100001 的突发放大后，300 个已验签请求里通过数等于该突发值。
func TestGW05OverrideBurst(t *testing.T) {
	const burst = 250
	var overs config.RateLimitOverrides
	if err := overs.UnmarshalText([]byte("app_m100001:1000:250")); err != nil {
		t.Fatal(err)
	}
	store := &callNonce{inner: NewMemoryNonce()}
	addr, hit := gw05Start(t, store, WithRateLimit(config.RateLimit{}, overs))
	body := []byte(`{"n":1}`)
	passed := 0
	for range 300 {
		before := store.calls()
		got := gw05Post(t, addr, gw03AppID, gw04Nonce(), body)
		if got.status == http.StatusOK {
			passed++
			if store.calls() != before+1 {
				t.Fatal("passed request did not reserve nonce")
			}
			continue
		}
		assertRateLimited(t, got)
		if store.calls() != before {
			t.Fatal("limited request reserved nonce")
		}
	}
	if passed != burst {
		t.Fatalf("passed %d, want burst %d", passed, burst)
	}
	if n, _, _ := hit.snapshot(); n != burst {
		t.Fatalf("handler %d", n)
	}
	if store.calls() != burst {
		t.Fatalf("nonce reserves %d", store.calls())
	}
}

// GW-05：限流器会拒绝全部已验签 /v1 时，渠道回调仍不调用限流逻辑，也不占用 nonce，并进入渠道回调。
func TestGW05CallbackSkipsRateLimit(t *testing.T) {
	store := &callNonce{err: errNonceUnavailable}
	fake := &gw06Notify{}
	clk := gw04Clock()
	var checks int
	h := NewMerchant(
		WithClock(clk),
		WithChannelKey([]byte("mockpay-plain-key")),
		WithNotifyAPI(fake),
		WithNonceStore(store),
		WithMerchantLookup(gw02Lookup()),
	)
	h.rateAllow = func(string, time.Time) (bool, string) {
		checks++
		return false, "1"
	}
	addr := startGW(t, h, nil)
	ts := strconv.FormatInt(clk.Now().Unix(), 10)
	got := gw06Post(t, addr, []byte("mockpay-plain-key"), ts, []byte(gw06Body))
	if got.status != http.StatusOK || string(got.body) != "SUCCESS" {
		t.Fatalf("callback status %d body %s", got.status, got.body)
	}
	if fake.count() != 1 {
		t.Fatalf("notify calls %d", fake.count())
	}
	if checks != 0 {
		t.Fatalf("rate limit calls %d", checks)
	}
	if store.calls() != 0 {
		t.Fatalf("nonce calls %d", store.calls())
	}
	body := []byte(`{"n":1}`)
	got = gw05Post(t, addr, gw03AppID, gw04Nonce(), body)
	assertRateLimited(t, got)
	if checks != 1 {
		t.Fatalf("v1 rate limit calls %d", checks)
	}
	if store.calls() != 0 {
		t.Fatalf("limited nonce calls %d", store.calls())
	}
	if fake.count() != 1 {
		t.Fatalf("notify calls after v1 %d", fake.count())
	}
}
