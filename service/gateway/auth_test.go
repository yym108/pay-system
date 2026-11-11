package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/sign"
)

const (
	gw03Secret = "demo_secret_M100001_0123456789ab" //nolint:gosec // G101: 接口 §3.7 公开向量，不是生产密钥。
	gw03AppID  = "app_m100001"
	gw03MID    = "M100001"
	gw03Nonce  = "AbCdEf0123456789"
	gw03V1Body = `{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`
	gw03V1Sig  = "hFYKid0+k7q9vCvqh/i6YnvoaAJYzf2niNuvSkMCwoQ="
)

// fakeLookup 记录每次 app_id 的原始字节。不拨号，不缓存。
type fakeLookup struct {
	mu     sync.Mutex
	appIDs []string
	fn     func(appID string) (Credential, error)
}

func (f *fakeLookup) Lookup(_ context.Context, appID string) (Credential, error) {
	f.mu.Lock()
	f.appIDs = append(f.appIDs, appID)
	fn := f.fn
	f.mu.Unlock()
	if fn == nil {
		return Credential{}, errno.ErrMerchantInvalid
	}
	return fn(appID)
}

func (f *fakeLookup) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.appIDs...)
}

type hitBox struct {
	mu sync.Mutex
	n  int
	id string
	ok bool
}

func (h *hitBox) mark(id string, ok bool) {
	h.mu.Lock()
	h.n++
	h.id = id
	h.ok = ok
	h.mu.Unlock()
}

func (h *hitBox) snapshot() (n int, id string, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n, h.id, h.ok
}

func gw03ClockUnix1() *clock.Fake {
	clk := &clock.Fake{}
	clk.Set(time.Unix(1, 0))
	return clk
}

func signedHeader(t *testing.T, secret []byte, method, uri, appID, ts, nonce string, body []byte) http.Header {
	t.Helper()
	sig, err := sign.SignRequest(secret, sign.Request{
		Method:     method,
		RequestURI: uri,
		Timestamp:  ts,
		Nonce:      nonce,
		Body:       body,
	})
	if err != nil {
		t.Fatal(err)
	}
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, appID)
	hdr.Set(sign.HeaderTimestamp, ts)
	hdr.Set(sign.HeaderNonce, nonce)
	hdr.Set(sign.HeaderSignature, sig)
	return hdr
}

func assertErrno(t *testing.T, got gwResp, want *errno.Error) {
	t.Helper()
	env := decodeEnv(t, got.body)
	if got.status != want.HTTPStatus || env.Code != want.Code || env.Msg != want.Msg || string(env.Data) != "{}" {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if signed(got.header) {
		t.Fatal("response was signed")
	}
	id := got.header.Get(headerRequestID)
	if id != env.RequestID || !requestIDPattern.MatchString(id) {
		t.Fatalf("id %q body %q", id, env.RequestID)
	}
}

func waitLog(t *testing.T, logs *logBuf) string {
	t.Helper()
	wait, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case <-logs.ready:
	case <-wait.Done():
		t.Fatalf("log %s", logs.String())
	}
	return logs.String()
}

// GW-03 验收：四个头各自缺失返回 20001，lookup 未调用，handler 未调用。
func TestGW03MissingHeader(t *testing.T) {
	names := []string{sign.HeaderAppID, sign.HeaderTimestamp, sign.HeaderNonce, sign.HeaderSignature}
	body := []byte(`{"n":1}`)
	for _, missing := range names {
		t.Run(missing, func(t *testing.T) {
			look := &fakeLookup{}
			var hit hitBox
			addr := startGW(t, NewMerchant(WithMerchantLookup(look)), func(r *server.Hertz) {
				r.POST("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
					id, ok := MerchantID(c)
					hit.mark(id, ok)
				})
			})
			hdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, "/v1/gw03-probe", gw03AppID, "1", gw03Nonce, body)
			hdr.Del(missing)
			got := do(t, http.MethodPost, "http://"+addr+"/v1/gw03-probe", body, hdr)
			assertErrno(t, got, errno.ErrSignInvalid)
			if n, _, _ := hit.snapshot(); n != 0 {
				t.Fatal("handler called")
			}
			if calls := look.calls(); len(calls) != 0 {
				t.Fatalf("lookup %q", calls)
			}
		})
	}

	t.Run("empty", func(t *testing.T) {
		look := &fakeLookup{}
		var hit hitBox
		addr := startGW(t, NewMerchant(WithMerchantLookup(look)), func(r *server.Hertz) {
			r.POST("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
				id, ok := MerchantID(c)
				hit.mark(id, ok)
			})
		})
		hdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, "/v1/gw03-probe", gw03AppID, "1", gw03Nonce, body)
		hdr.Set(sign.HeaderAppID, "")
		got := do(t, http.MethodPost, "http://"+addr+"/v1/gw03-probe", body, hdr)
		assertErrno(t, got, errno.ErrSignInvalid)
		if n, _, _ := hit.snapshot(); n != 0 {
			t.Fatal("handler called")
		}
		if calls := look.calls(); len(calls) != 0 {
			t.Fatalf("lookup %q", calls)
		}
	})
}

// GW-03 验收：只含空白的 X-App-Id 不 trim，原字节交给 lookup；lookup 返回 20004 时响应 20004。
func TestGW03BlankAppIDNotTrimmed(t *testing.T) {
	const raw = "   "
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{}, errno.ErrMerchantInvalid
	}}
	var hit hitBox
	h := NewMerchant(WithMerchantLookup(look))
	c := app.NewContext(0)
	c.SetHandlers(app.HandlersChain{
		func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
		},
	})
	c.Request.SetRequestURI("/v1/gw03-probe")
	c.Request.Header.SetMethod(http.MethodGet)
	c.Request.Header.Set(sign.HeaderAppID, raw)
	c.Request.Header.Set(sign.HeaderTimestamp, "1")
	c.Request.Header.Set(sign.HeaderNonce, gw03Nonce)
	c.Request.Header.Set(sign.HeaderSignature, "not-a-real-signature")
	if got := string(c.GetHeader(sign.HeaderAppID)); got != raw {
		t.Fatalf("fixture lost spaces: %q", got)
	}
	reqID, err := newRequestID()
	if err != nil {
		t.Fatal(err)
	}
	c.Set(requestIDKey, reqID)
	h.verifySign(context.Background(), c)
	env := decodeEnv(t, c.Response.Body())
	if c.Response.StatusCode() != errno.ErrMerchantInvalid.HTTPStatus || env.Code != errno.ErrMerchantInvalid.Code || env.Msg != errno.ErrMerchantInvalid.Msg || string(env.Data) != "{}" {
		t.Fatalf("status %d body %s", c.Response.StatusCode(), c.Response.Body())
	}
	id := string(c.Response.Header.Peek(headerRequestID))
	if id != env.RequestID || !requestIDPattern.MatchString(id) {
		t.Fatalf("id %q body %q", id, env.RequestID)
	}
	respHdr := http.Header{}
	respHdr.Set(sign.HeaderSignature, string(c.Response.Header.Peek(sign.HeaderSignature)))
	respHdr.Set(sign.HeaderTimestamp, string(c.Response.Header.Peek(sign.HeaderTimestamp)))
	respHdr.Set(sign.HeaderNonce, string(c.Response.Header.Peek(sign.HeaderNonce)))
	if signed(respHdr) {
		t.Fatal("response was signed")
	}
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
	calls := look.calls()
	if len(calls) != 1 || calls[0] != raw {
		t.Fatalf("lookup %q", calls)
	}
}

// GW-03 验收：头含 CR/LF 返回 20001，lookup 未调用，日志不含该原始值。
func TestGW03HeaderCRLF(t *testing.T) {
	const marker = "inject-marker-crlf"
	raw := marker + "\nX-Injected: pwned"
	logs := newLogBuf()
	look := &fakeLookup{}
	h := NewMerchant(WithLogger(logx.New("gateway", logs)), WithMerchantLookup(look))
	c := app.NewContext(0)
	c.Request.SetRequestURI("/v1/gw03-probe")
	c.Request.Header.SetMethod(http.MethodGet)
	c.Request.Header.Set(sign.HeaderAppID, raw)
	c.Request.Header.Set(sign.HeaderTimestamp, "1")
	c.Request.Header.Set(sign.HeaderNonce, gw03Nonce)
	c.Request.Header.Set(sign.HeaderSignature, "not-a-real-signature")
	if !strings.Contains(string(c.GetHeader(sign.HeaderAppID)), "\n") {
		t.Fatal("fixture lost the newline")
	}
	ctx := context.Background()
	h.requestID(ctx, c)
	h.verifySign(ctx, c)
	env := decodeEnv(t, c.Response.Body())
	if c.Response.StatusCode() != errno.ErrSignInvalid.HTTPStatus || env.Code != errno.ErrSignInvalid.Code || env.Msg != errno.ErrSignInvalid.Msg {
		t.Fatalf("status %d body %s", c.Response.StatusCode(), c.Response.Body())
	}
	id := string(c.Response.Header.Peek(headerRequestID))
	if id != env.RequestID || !requestIDPattern.MatchString(id) {
		t.Fatalf("id %q body %q", id, env.RequestID)
	}
	if calls := look.calls(); len(calls) != 0 {
		t.Fatalf("lookup %q", calls)
	}
	if strings.Contains(logs.String(), marker) || strings.Contains(logs.String(), raw) {
		t.Fatalf("log leaked header: %s", logs.String())
	}
}

// GW-03 验收：错误签名返回 20001，handler 未调用，日志有 app_id，不含 secret 与签名值。
func TestGW03BadSignature(t *testing.T) {
	const (
		secret = "secret-value-should-not-log" //nolint:gosec // G101: 测试哨兵，用来确认日志没有密钥。
		sig    = "sig-value-should-not-log"
		appID  = "app_log_target"
	)
	logs := newLogBuf()
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: gw03MID, Status: "ACTIVE", Secret: []byte(secret)}, nil
	}}
	var hit hitBox
	addr := startGW(t, NewMerchant(WithClock(gw03ClockUnix1()), WithMerchantLookup(look), WithLogger(logx.New("gateway", logs))), func(r *server.Hertz) {
		r.POST("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
		})
	})
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, appID)
	hdr.Set(sign.HeaderTimestamp, "1")
	hdr.Set(sign.HeaderNonce, gw03Nonce)
	hdr.Set(sign.HeaderSignature, sig)
	body := []byte(`{"n":1}`)
	got := do(t, http.MethodPost, "http://"+addr+"/v1/gw03-probe", body, hdr)
	assertErrno(t, got, errno.ErrSignInvalid)
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
	text := waitLog(t, logs)
	if !strings.Contains(text, appID) {
		t.Fatalf("log missing app_id: %s", text)
	}
	if strings.Contains(text, secret) || strings.Contains(text, sig) {
		t.Fatalf("log leaked: %s", text)
	}
}

// GW-03 验收：未知 app_id 返回 20004，handler 未调用。
func TestGW03UnknownAppID(t *testing.T) {
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{}, errno.ErrMerchantInvalid
	}}
	var hit hitBox
	addr := startGW(t, NewMerchant(WithClock(gw03ClockUnix1()), WithMerchantLookup(look)), func(r *server.Hertz) {
		r.GET("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
		})
	})
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, "no-such-app")
	hdr.Set(sign.HeaderTimestamp, "1")
	hdr.Set(sign.HeaderNonce, gw03Nonce)
	hdr.Set(sign.HeaderSignature, "not-a-real-signature")
	got := do(t, http.MethodGet, "http://"+addr+"/v1/gw03-probe", nil, hdr)
	assertErrno(t, got, errno.ErrMerchantInvalid)
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
}

// GW-03 验收：DISABLED 在验签之前返回 20004，handler 未调用。
func TestGW03DisabledBeforeVerify(t *testing.T) {
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: gw03MID, Status: "DISABLED", Secret: []byte(gw03Secret)}, nil
	}}
	var hit hitBox
	addr := startGW(t, NewMerchant(WithClock(gw03ClockUnix1()), WithMerchantLookup(look)), func(r *server.Hertz) {
		r.POST("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
		})
	})
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, gw03AppID)
	hdr.Set(sign.HeaderTimestamp, "1")
	hdr.Set(sign.HeaderNonce, gw03Nonce)
	hdr.Set(sign.HeaderSignature, "not-a-real-signature")
	got := do(t, http.MethodPost, "http://"+addr+"/v1/gw03-probe", []byte(`{"n":1}`), hdr)
	assertErrno(t, got, errno.ErrMerchantInvalid)
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
}

// GW-03 验收：非 ACTIVE 的其他状态返回 20004。
func TestGW03StatusNotActive(t *testing.T) {
	for _, status := range []string{"", "active", "FROZEN"} {
		t.Run(status, func(t *testing.T) {
			look := &fakeLookup{fn: func(string) (Credential, error) {
				return Credential{MerchantID: gw03MID, Status: status, Secret: []byte(gw03Secret)}, nil
			}}
			var hit hitBox
			addr := startGW(t, NewMerchant(WithClock(gw03ClockUnix1()), WithMerchantLookup(look)), func(r *server.Hertz) {
				r.GET("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
					id, ok := MerchantID(c)
					hit.mark(id, ok)
				})
			})
			hdr := http.Header{}
			hdr.Set(sign.HeaderAppID, gw03AppID)
			hdr.Set(sign.HeaderTimestamp, "1")
			hdr.Set(sign.HeaderNonce, gw03Nonce)
			hdr.Set(sign.HeaderSignature, "not-a-real-signature")
			got := do(t, http.MethodGet, "http://"+addr+"/v1/gw03-probe", nil, hdr)
			assertErrno(t, got, errno.ErrMerchantInvalid)
			if n, _, _ := hit.snapshot(); n != 0 {
				t.Fatal("handler called")
			}
		})
	}
}

// GW-03 验收：lookup 传输错误返回 10004，不进 handler；再请求一次，lookup 被调用两次。
func TestGW03LookupUnavailableNotCached(t *testing.T) {
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{}, errno.ErrUnavailable
	}}
	var hit hitBox
	addr := startGW(t, NewMerchant(WithClock(gw03ClockUnix1()), WithMerchantLookup(look)), func(r *server.Hertz) {
		r.GET("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
		})
	})
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, gw03AppID)
	hdr.Set(sign.HeaderTimestamp, "1")
	hdr.Set(sign.HeaderNonce, gw03Nonce)
	hdr.Set(sign.HeaderSignature, "not-a-real-signature")
	for i := 0; i < 2; i++ {
		got := do(t, http.MethodGet, "http://"+addr+"/v1/gw03-probe", nil, hdr)
		assertErrno(t, got, errno.ErrUnavailable)
	}
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
	if calls := look.calls(); len(calls) != 2 {
		t.Fatalf("lookup calls %d", len(calls))
	}
}

// GW-03 验收：ACTIVE 但 secret 为空返回 10003，handler 未调用，日志含 app_id，不含签名值。
func TestGW03EmptySecret(t *testing.T) {
	const (
		sig   = "sig-value-should-not-log"
		appID = "app_empty_secret"
	)
	logs := newLogBuf()
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: gw03MID, Status: "ACTIVE", Secret: nil}, nil
	}}
	var hit hitBox
	addr := startGW(t, NewMerchant(WithClock(gw03ClockUnix1()), WithMerchantLookup(look), WithLogger(logx.New("gateway", logs))), func(r *server.Hertz) {
		r.POST("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
		})
	})
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, appID)
	hdr.Set(sign.HeaderTimestamp, "1")
	hdr.Set(sign.HeaderNonce, gw03Nonce)
	hdr.Set(sign.HeaderSignature, sig)
	got := do(t, http.MethodPost, "http://"+addr+"/v1/gw03-probe", []byte(`{"n":1}`), hdr)
	assertErrno(t, got, errno.ErrInternal)
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
	text := waitLog(t, logs)
	if !strings.Contains(text, appID) || strings.Contains(text, sig) {
		t.Fatalf("log %s", text)
	}
}

// GW-03 验收：ACTIVE 但 merchant_id 为空返回 10003，handler 未调用。
func TestGW03EmptyMerchantID(t *testing.T) {
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: "", Status: "ACTIVE", Secret: []byte(gw03Secret)}, nil
	}}
	var hit hitBox
	addr := startGW(t, NewMerchant(WithClock(gw03ClockUnix1()), WithMerchantLookup(look)), func(r *server.Hertz) {
		r.POST("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
		})
	})
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, gw03AppID)
	hdr.Set(sign.HeaderTimestamp, "1")
	hdr.Set(sign.HeaderNonce, gw03Nonce)
	hdr.Set(sign.HeaderSignature, "not-a-real-signature")
	got := do(t, http.MethodPost, "http://"+addr+"/v1/gw03-probe", []byte(`{"n":1}`), hdr)
	assertErrno(t, got, errno.ErrInternal)
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
}

// GW-03 验收：正确签名通过，handler 读到的 MerchantID 等于凭证，响应平台签名用该商户 secret。查询串改变则验签失败。
func TestGW03ValidSignature(t *testing.T) {
	const (
		secret = "merchant-secret-gw03-bytes" //nolint:gosec // G101: 测试哨兵，不是生产密钥。
		other  = "process-secret-not-used"    //nolint:gosec // G101: 用来确认响应没有改用进程内密钥。
		appID  = "app_M100001"
	)
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: gw03MID, Status: "ACTIVE", Secret: []byte(secret)}, nil
	}}
	var hit hitBox
	addr := startGW(t, NewMerchant(WithClock(clk), WithSignSecret([]byte(other)), WithMerchantLookup(look), WithNonceStore(NewMemoryNonce())), func(r *server.Hertz) {
		r.POST("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
			WriteOK(c, map[string]string{"ok": "yes"})
		})
	})
	body := []byte(`{"n":1}`)
	uri := "/v1/gw03-probe?x=1"
	hdr := signedHeader(t, []byte(secret), http.MethodPost, uri, appID, "1791500000", gw03Nonce, body)
	got := do(t, http.MethodPost, "http://"+addr+uri, body, hdr)
	if got.status != http.StatusOK {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	n, id, ok := hit.snapshot()
	if n != 1 || !ok || id != gw03MID {
		t.Fatalf("merchant id n=%d ok=%v id=%q", n, ok, id)
	}
	msg := sign.PlatformMessage{
		Timestamp: got.header.Get(sign.HeaderTimestamp),
		Nonce:     got.header.Get(sign.HeaderNonce),
		Body:      got.body,
	}
	if err := sign.VerifyPlatform([]byte(secret), msg, got.header.Get(sign.HeaderSignature)); err != nil {
		t.Fatal(err)
	}
	if err := sign.VerifyPlatform([]byte(other), msg, got.header.Get(sign.HeaderSignature)); err == nil {
		t.Fatal("response used the process secret")
	}
	calls := look.calls()
	if len(calls) != 1 || calls[0] != appID {
		t.Fatalf("lookup %q", calls)
	}

	bad := do(t, http.MethodPost, "http://"+addr+"/v1/gw03-probe?x=2", body, hdr)
	assertErrno(t, bad, errno.ErrSignInvalid)
	n, _, _ = hit.snapshot()
	if n != 1 {
		t.Fatalf("handler calls %d", n)
	}
}

// GW-03 验收：§3.7 V1 验签通过。该路径现由 GW-02 创建支付单；没有 payment client 时是 10003，平台签名仍用查到的 app_secret。
func TestGW03V1UnknownPathSigned(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: gw03MID, Status: "ACTIVE", Secret: []byte(gw03Secret)}, nil
	}}
	addr := startGW(t, NewMerchant(WithClock(clk), WithMerchantLookup(look), WithNonceStore(NewMemoryNonce())), nil)
	body := []byte(gw03V1Body)
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, gw03AppID)
	hdr.Set(sign.HeaderTimestamp, "1791500000")
	hdr.Set(sign.HeaderNonce, gw03Nonce)
	hdr.Set(sign.HeaderSignature, gw03V1Sig)
	got := do(t, http.MethodPost, "http://"+addr+"/v1/payments", body, hdr)
	if got.status != errno.ErrInternal.HTTPStatus {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	env := decodeEnv(t, got.body)
	if env.Code != errno.ErrInternal.Code || env.Msg != errno.ErrInternal.Msg || string(env.Data) != "{}" {
		t.Fatalf("body %s", got.body)
	}
	if !signed(got.header) {
		t.Fatal("10003 was not signed")
	}
	if err := sign.VerifyPlatform([]byte(gw03Secret), sign.PlatformMessage{
		Timestamp: got.header.Get(sign.HeaderTimestamp),
		Nonce:     got.header.Get(sign.HeaderNonce),
		Body:      got.body,
	}, got.header.Get(sign.HeaderSignature)); err != nil {
		t.Fatal(err)
	}
	calls := look.calls()
	if len(calls) != 1 || calls[0] != gw03AppID {
		t.Fatalf("lookup %q", calls)
	}
}

// GW-03 验收：验签通过但路径仍不存在时是 404 / 10002，data 为 {}，平台签名用查到的 app_secret。
func TestGW03UnknownPathStill404(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: gw03MID, Status: "ACTIVE", Secret: []byte(gw03Secret)}, nil
	}}
	addr := startGW(t, NewMerchant(WithClock(clk), WithMerchantLookup(look), WithNonceStore(NewMemoryNonce())), nil)
	const path = "/v1/gw03-unknown"
	body := []byte(`{"n":1}`)
	hdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, gw03AppID, "1791500000", gw03Nonce, body)
	got := do(t, http.MethodPost, "http://"+addr+path, body, hdr)
	if got.status != http.StatusNotFound {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	env := decodeEnv(t, got.body)
	if env.Code != errno.ErrNotFound.Code || env.Msg != errno.ErrNotFound.Msg || string(env.Data) != "{}" {
		t.Fatalf("body %s", got.body)
	}
	if err := sign.VerifyPlatform([]byte(gw03Secret), sign.PlatformMessage{
		Timestamp: got.header.Get(sign.HeaderTimestamp),
		Nonce:     got.header.Get(sign.HeaderNonce),
		Body:      got.body,
	}, got.header.Get(sign.HeaderSignature)); err != nil {
		t.Fatal(err)
	}
	calls := look.calls()
	if len(calls) != 1 || calls[0] != gw03AppID {
		t.Fatalf("lookup %q", calls)
	}
}

func TestMerchantMetricsNotExposed(t *testing.T) {
	addr := startGW(t, NewMerchant(), nil)
	got := do(t, http.MethodGet, "http://"+addr+"/metrics", nil, nil)
	if got.status != http.StatusNotFound {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if strings.Contains(string(got.body), "go_goroutines") || strings.Contains(string(got.body), "# HELP") {
		t.Fatalf("business port exposed metrics: %s", got.body)
	}
}

// GW-03 验收：未签名 GET /v1/balance 返回 20001，不签名，不进 handler，lookup 未调用。
func TestGW03UnsignedBalance(t *testing.T) {
	look := &fakeLookup{}
	acct := &gw02Account{}
	addr := startGW(t, NewMerchant(WithMerchantLookup(look), WithAccountAPI(acct)), nil)
	got := do(t, http.MethodGet, "http://"+addr+"/v1/balance", nil, nil)
	assertErrno(t, got, errno.ErrSignInvalid)
	if acct.calls() != 0 {
		t.Fatal("account called")
	}
	if calls := look.calls(); len(calls) != 0 {
		t.Fatalf("lookup %q", calls)
	}
}

// GW-03 验收：管理端口未签名 /v1 仍是 404、不签名、lookup 调用次数为 0。
func TestGW03AdminSkipsLookup(t *testing.T) {
	look := &fakeLookup{}
	addr := startGW(t, NewAdmin(WithMerchantLookup(look)), nil)
	got := do(t, http.MethodGet, "http://"+addr+"/v1/gw03-probe", nil, nil)
	if got.status != http.StatusNotFound {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	env := decodeEnv(t, got.body)
	if env.Code != errno.ErrNotFound.Code || env.Msg != errno.ErrNotFound.Msg || string(env.Data) != "{}" {
		t.Fatalf("body %s", got.body)
	}
	if signed(got.header) {
		t.Fatal("admin 404 was signed")
	}
	if calls := look.calls(); len(calls) != 0 {
		t.Fatalf("lookup %q", calls)
	}
}

// GW-03 验收：非 /v1 的 /order 仍是八个名字加 handler，且不调用 lookup。
func TestGW03NonV1OrderSkipsLookup(t *testing.T) {
	look := &fakeLookup{}
	addr := startGW(t, NewMerchant(WithMerchantLookup(look)), func(r *server.Hertz) {
		r.GET("/order", func(_ context.Context, c *app.RequestContext) {
			names := append(append([]string{}, chainNames(c)...), "handler")
			WriteOK(c, map[string]any{"chain": names})
		})
	})
	got := do(t, http.MethodGet, "http://"+addr+"/order", nil, nil)
	env := decodeEnv(t, got.body)
	var data struct {
		Chain []string `json:"chain"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatal(err)
	}
	want := []string{
		nameRecovery, nameRequestID, nameTrace, nameAccessLog,
		nameTimeWindow, nameVerifySign, nameRateLimit, nameNonce, "handler",
	}
	if strings.Join(data.Chain, ",") != strings.Join(want, ",") {
		t.Fatalf("chain %v", data.Chain)
	}
	if calls := look.calls(); len(calls) != 0 {
		t.Fatalf("lookup %q", calls)
	}
}

// GW-04：默认真实时钟下，时间戳 1 超出窗口，返回 20002。不查商户，不进 handler，响应不签名。
func TestGW03NoTimestampOrNonceCheck(t *testing.T) {
	const secret = "merchant-secret-gw03-bytes" //nolint:gosec // G101: 测试哨兵，不是生产密钥。
	look := &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: gw03MID, Status: "ACTIVE", Secret: []byte(secret)}, nil
	}}
	var hit hitBox
	addr := startGW(t, NewMerchant(WithMerchantLookup(look)), func(r *server.Hertz) {
		r.GET("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	uri := "/v1/gw03-probe"
	hdr := signedHeader(t, []byte(secret), http.MethodGet, uri, gw03AppID, "1", gw03Nonce, nil)
	for i := 0; i < 2; i++ {
		got := do(t, http.MethodGet, "http://"+addr+uri, nil, hdr)
		assertErrno(t, got, errno.ErrTimestampExpired)
	}
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
	if calls := look.calls(); len(calls) != 0 {
		t.Fatalf("lookup calls %d", len(calls))
	}
}

// GW-03 验收：/v1foo 与 /v10 不验签；/v1 与 /v1/ 缺头仍是 20001。
func TestGW03PathBoundary(t *testing.T) {
	look := &fakeLookup{}
	addr := startGW(t, NewMerchant(WithMerchantLookup(look)), nil)
	for _, path := range []string{"/v1foo", "/v10"} {
		got := do(t, http.MethodGet, "http://"+addr+path, nil, nil)
		if got.status != http.StatusNotFound {
			t.Fatalf("%s status %d body %s", path, got.status, got.body)
		}
		env := decodeEnv(t, got.body)
		if env.Code != errno.ErrNotFound.Code || signed(got.header) {
			t.Fatalf("%s body %s", path, got.body)
		}
	}
	for _, path := range []string{"/v1", "/v1/"} {
		got := do(t, http.MethodGet, "http://"+addr+path, nil, nil)
		assertErrno(t, got, errno.ErrSignInvalid)
	}
	if calls := look.calls(); len(calls) != 0 {
		t.Fatalf("lookup %q", calls)
	}
}

// GW-03 验收：商户端口未装 lookup 时 /v1 返回 10003，不进 handler。
func TestGW03NilLookup(t *testing.T) {
	var hit hitBox
	addr := startGW(t, NewMerchant(WithClock(gw03ClockUnix1())), func(r *server.Hertz) {
		r.GET("/v1/gw03-probe", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
		})
	})
	hdr := http.Header{}
	hdr.Set(sign.HeaderAppID, gw03AppID)
	hdr.Set(sign.HeaderTimestamp, "1")
	hdr.Set(sign.HeaderNonce, gw03Nonce)
	hdr.Set(sign.HeaderSignature, "not-a-real-signature")
	got := do(t, http.MethodGet, "http://"+addr+"/v1/gw03-probe", nil, hdr)
	assertErrno(t, got, errno.ErrInternal)
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
}
