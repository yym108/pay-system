package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/sign"
)

// 文档 §3.7 公布的向量。密钥是文档里的演示值，不是生产密钥。
const (
	testAppID  = "app_m100001"
	testSecret = "demo_secret_M100001_0123456789ab" //nolint:gosec // G101: 接口文档公开的测试向量，不是生产密钥。
	testToken  = "admin-token-not-in-output-0123456789"

	vectorNonce = "AbCdEf0123456789"
	vectorTS    = "1791500000"
	otherNonce  = "BcDeFg0123456789"

	v1Body  = `{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`
	v1Sig   = "hFYKid0+k7q9vCvqh/i6YnvoaAJYzf2niNuvSkMCwoQ="
	v2Sig   = "D1t81263+AFhLO6+FPSoVQqNFANBAKpi0akuyE9gVu4="
	v3Body  = `{"code":0,"msg":"ok","data":{"pay_no":"PA0000000000000000001"}}`
	v3TS    = "1791500001"
	v3Nonce = "ZyXwVu9876543210"
	v3Sig   = "TuLVqqAxs3SyvTtDXp1cpv+Rpv2D5URAFywIHBNjAh4="

	okBody = `{"code":0,"msg":"ok","data":{}}`
)

func fixedClock() *clock.Fake {
	f := &clock.Fake{}
	f.Set(time.Unix(1791500000, 0))
	return f
}

func oneNonce(v string) func() (string, error) {
	return func() (string, error) { return v, nil }
}

type nonceQueue struct {
	mu sync.Mutex
	v  []string
	i  int
}

func (q *nonceQueue) next() (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.i >= len(q.v) {
		return "", io.EOF
	}
	s := q.v[q.i]
	q.i++
	return s, nil
}

type gotReq struct {
	method string
	uri    string
	body   string
	header http.Header
}

type fakeGW struct {
	t *testing.T

	verifyMerchant bool
	signResponse   bool
	admin          bool
	respBody       []byte
	respTS         string
	respNonce      string
	respSig        string // 非空时不再计算，用来送出公布的 V3 或篡改后的值。
	tamper         bool

	mu   sync.Mutex
	got  []gotReq
	hits int
}

func (f *fakeGW) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			f.t.Errorf("read body: %v", err)
			http.Error(w, "read", http.StatusInternalServerError)
			return
		}
		_ = r.Body.Close()
		f.mu.Lock()
		f.hits++
		f.got = append(f.got, gotReq{
			method: r.Method,
			uri:    r.RequestURI,
			body:   string(body),
			header: r.Header.Clone(),
		})
		f.mu.Unlock()
		if f.verifyMerchant {
			if err := sign.VerifyRequest([]byte(testSecret), sign.Request{
				Method:     r.Method,
				RequestURI: r.RequestURI,
				Timestamp:  r.Header.Get(sign.HeaderTimestamp),
				Nonce:      r.Header.Get(sign.HeaderNonce),
				Body:       body,
			}, r.Header.Get(sign.HeaderSignature)); err != nil {
				f.t.Errorf("VerifyRequest %s %s: %v", r.Method, r.RequestURI, err)
				http.Error(w, "sig", http.StatusUnauthorized)
				return
			}
		}

		raw := f.respBody
		if raw == nil {
			raw = []byte(okBody)
		}
		if f.signResponse {
			ts := f.respTS
			if ts == "" {
				ts = v3TS
			}
			nonce := f.respNonce
			if nonce == "" {
				nonce = v3Nonce
			}
			sig := f.respSig
			if sig == "" {
				var serr error
				sig, serr = sign.SignPlatform([]byte(testSecret), sign.PlatformMessage{
					Timestamp: ts,
					Nonce:     nonce,
					Body:      raw,
				})
				if serr != nil {
					f.t.Errorf("SignPlatform: %v", serr)
					http.Error(w, "sign", http.StatusInternalServerError)
					return
				}
			}
			if f.tamper {
				sig = v1Sig
			}
			w.Header().Set(sign.HeaderTimestamp, ts)
			w.Header().Set(sign.HeaderNonce, nonce)
			w.Header().Set(sign.HeaderSignature, sig)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
	})
}

func (f *fakeGW) requests() []gotReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]gotReq, len(f.got))
	copy(out, f.got)
	return out
}

type cmdCase struct {
	name     string
	args     []string
	method   string
	uri      string
	body     string
	nonce    string
	timeout  time.Duration
	admin    bool
	operator string
	wantSig  string
}

func merchantCases() []cmdCase {
	return []cmdCase{
		{
			name: "create", method: http.MethodPost, uri: "/v1/payments", body: v1Body,
			nonce: vectorNonce, timeout: time.Second, wantSig: v1Sig,
			args: []string{"create", "--out-trade-no", "T20261009001", "--amount", "10000", "--subject", "demo"},
		},
		{
			name: "pay", method: http.MethodPost, uri: "/v1/payments/PA0000000000000000001/pay",
			body: `{"channel":"mockpay"}`, nonce: otherNonce, timeout: 5 * time.Second,
			args: []string{"pay", "--pay-no", "PA0000000000000000001", "--channel", "mockpay"},
		},
		{
			name: "query-pay-no", method: http.MethodGet, uri: "/v1/payments/PA0000000000000000001",
			nonce: otherNonce, timeout: time.Second,
			args: []string{"query", "--pay-no", "PA0000000000000000001"},
		},
		{
			name: "query-out-trade-no", method: http.MethodGet,
			uri: "/v1/payments?out_trade_no=T20261009001", nonce: vectorNonce, timeout: time.Second, wantSig: v2Sig,
			args: []string{"query", "--out-trade-no", "T20261009001"},
		},
		{
			name: "close", method: http.MethodPost, uri: "/v1/payments/PA0000000000000000001/close",
			nonce: otherNonce, timeout: 5 * time.Second,
			args: []string{"close", "--pay-no", "PA0000000000000000001"},
		},
		{
			name: "refund", method: http.MethodPost, uri: "/v1/refunds",
			body:  `{"pay_no":"PA0000000000000000001","out_refund_no":"R20261009001","amount":100}`,
			nonce: otherNonce, timeout: 5 * time.Second,
			args: []string{"refund", "--pay-no", "PA0000000000000000001", "--out-refund-no", "R20261009001", "--amount", "100"},
		},
		{
			name: "query-refund-no", method: http.MethodGet, uri: "/v1/refunds/RF0000000000000000001",
			nonce: otherNonce, timeout: time.Second,
			args: []string{"query-refund", "--refund-no", "RF0000000000000000001"},
		},
		{
			name: "query-refund-out", method: http.MethodGet, uri: "/v1/refunds?out_refund_no=R20261009001",
			nonce: otherNonce, timeout: time.Second,
			args: []string{"query-refund", "--out-refund-no", "R20261009001"},
		},
		{
			name: "transfer", method: http.MethodPost, uri: "/v1/transfers",
			body:  `{"out_transfer_no":"TR20261012001","payee_merchant_id":"M100002","amount":500}`,
			nonce: otherNonce, timeout: 3 * time.Second,
			args: []string{"transfer", "--out-transfer-no", "TR20261012001", "--payee-merchant-id", "M100002", "--amount", "500"},
		},
		{
			name: "query-transfer-no", method: http.MethodGet, uri: "/v1/transfers/TF0000000000000000001",
			nonce: otherNonce, timeout: time.Second,
			args: []string{"query-transfer", "--transfer-no", "TF0000000000000000001"},
		},
		{
			name: "query-transfer-out", method: http.MethodGet, uri: "/v1/transfers?out_transfer_no=TR20261012001",
			nonce: otherNonce, timeout: time.Second,
			args: []string{"query-transfer", "--out-transfer-no", "TR20261012001"},
		},
		{
			name: "balance", method: http.MethodGet, uri: "/v1/balance",
			nonce: otherNonce, timeout: time.Second,
			args: []string{"balance"},
		},
	}
}

func adminCases() []cmdCase {
	return []cmdCase{
		{
			name: "admin-recon-run", method: http.MethodPost, uri: "/admin/recon/run",
			body:    `{"channel":"mockpay","bill_date":"2026-10-09"}`,
			timeout: 120 * time.Second, admin: true, operator: "ops",
			args: []string{"admin", "recon-run", "--channel", "mockpay", "--bill-date", "2026-10-09", "--operator", "ops"},
		},
		{
			name: "admin-replay", method: http.MethodPost, uri: "/admin/outbox/replay",
			body:    `{"event_id":"EV0000000000000000001"}`,
			timeout: time.Second, admin: true, operator: "ops",
			args: []string{"admin", "replay", "--event-id", "EV0000000000000000001", "--operator", "ops"},
		},
		{
			name: "admin-replay-biz", method: http.MethodPost, uri: "/admin/outbox/replay",
			body:    `{"biz_no":"PA0000000000000000001","event_type":"payment.succeeded"}`,
			timeout: time.Second, admin: true, operator: "ops",
			args: []string{"admin", "replay", "--biz-no", "PA0000000000000000001", "--event-type", "payment.succeeded", "--operator", "ops"},
		},
		{
			name: "admin-check", method: http.MethodPost, uri: "/admin/accounts/check",
			body:    `{"merchant_id":"M100001","expect_quiescent":true}`,
			timeout: 120 * time.Second, admin: true,
			args: []string{"admin", "check", "--merchant-id", "M100001", "--expect-quiescent"},
		},
	}
}

func TestPayCLIRunEmpty(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, []string{"WORKER_ID=nope", "PAYCLI_APP_SECRET=" + testSecret}, &stdout, &stderr, clock.Real{}, oneNonce(vectorNonce))
	if code != 1 {
		t.Fatalf("exit code = %d, stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	for _, name := range []string{"create", "pay", "query", "close", "refund", "query-refund", "transfer", "query-transfer", "balance", "admin recon-run", "admin replay", "admin check"} {
		if !strings.Contains(stderr.String(), name) {
			t.Fatalf("usage missing %s: %s", name, stderr.String())
		}
	}
	if strings.Contains(stderr.String(), testSecret) {
		t.Fatalf("usage leaked secret: %s", stderr.String())
	}
}

// TL-01 验收 1：签名与 docs/05-api.md §3.7 的 V1、V2、V3 一致。
func TestTL01SignVectors(t *testing.T) {
	t.Parallel()
	v1, err := sign.SignRequest([]byte(testSecret), sign.Request{
		Method:     http.MethodPost,
		RequestURI: "/v1/payments",
		Timestamp:  vectorTS,
		Nonce:      vectorNonce,
		Body:       []byte(v1Body),
	})
	if err != nil {
		t.Fatal(err)
	}
	if v1 != v1Sig {
		t.Fatalf("V1 = %s", v1)
	}
	v2, err := sign.SignRequest([]byte(testSecret), sign.Request{
		Method:     http.MethodGet,
		RequestURI: "/v1/payments?out_trade_no=T20261009001",
		Timestamp:  vectorTS,
		Nonce:      vectorNonce,
	})
	if err != nil {
		t.Fatal(err)
	}
	if v2 != v2Sig {
		t.Fatalf("V2 = %s", v2)
	}
	v3, err := sign.SignPlatform([]byte(testSecret), sign.PlatformMessage{
		Timestamp: v3TS,
		Nonce:     v3Nonce,
		Body:      []byte(v3Body),
	})
	if err != nil {
		t.Fatal(err)
	}
	if v3 != v3Sig {
		t.Fatalf("V3 = %s", v3)
	}
}

// TL-01 验收 2：每个子命令的方法、路径、body 与签名头正确，假服务端按 §3.1 验签通过。
func TestTL01EachCommand(t *testing.T) {
	t.Parallel()
	cases := append(merchantCases(), adminCases()...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gw := &fakeGW{t: t, verifyMerchant: !tc.admin, signResponse: !tc.admin, admin: tc.admin}
			srv := httptest.NewServer(gw.handler())
			t.Cleanup(srv.Close)
			var stdout, stderr bytes.Buffer
			var gotTimeout time.Duration
			code := runClient(append([]string{"--gateway", srv.URL, "--app-id", testAppID, "--app-secret", testSecret, "--admin-token", testToken}, tc.args...), nil, &stdout, &stderr, fixedClock(), oneNonce(tc.nonce), func(d time.Duration) *http.Client {
				gotTimeout = d
				c := newHTTPClient(d)
				if c.Timeout != d {
					t.Errorf("client timeout = %s, want %s", c.Timeout, d)
				}
				if c.CheckRedirect == nil {
					t.Error("CheckRedirect is nil")
				}
				return c
			})
			if code != 0 {
				t.Fatalf("exit = %d, stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if stdout.String() != okBody {
				t.Fatalf("stdout = %q", stdout.String())
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %s", stderr.String())
			}
			if strings.Contains(stdout.String()+stderr.String(), testSecret) || strings.Contains(stdout.String()+stderr.String(), testToken) {
				t.Fatal("output contains secret or token")
			}
			if gotTimeout != tc.timeout {
				t.Fatalf("timeout = %s, want %s", gotTimeout, tc.timeout)
			}
			reqs := gw.requests()
			if len(reqs) != 1 {
				t.Fatalf("requests = %d", len(reqs))
			}
			got := reqs[0]
			if got.method != tc.method || got.uri != tc.uri || got.body != tc.body {
				t.Fatalf("got %s %s %q", got.method, got.uri, got.body)
			}
			if tc.admin {
				if got.header.Get("X-Admin-Token") != testToken {
					t.Fatalf("admin token header = %q", got.header.Get("X-Admin-Token"))
				}
				if got.header.Get("X-Admin-Operator") != tc.operator {
					t.Fatalf("operator = %q", got.header.Get("X-Admin-Operator"))
				}
				if got.header.Get(sign.HeaderSignature) != "" || got.header.Get(sign.HeaderAppID) != "" || got.header.Get(sign.HeaderNonce) != "" {
					t.Fatal("admin request has merchant signature headers")
				}
			} else {
				if got.header.Get(sign.HeaderAppID) != testAppID {
					t.Fatalf("app id = %q", got.header.Get(sign.HeaderAppID))
				}
				if got.header.Get(sign.HeaderTimestamp) != vectorTS {
					t.Fatalf("timestamp = %q", got.header.Get(sign.HeaderTimestamp))
				}
				if got.header.Get(sign.HeaderNonce) != tc.nonce {
					t.Fatalf("nonce = %q", got.header.Get(sign.HeaderNonce))
				}
				if got.header.Get("X-Admin-Token") != "" {
					t.Fatal("merchant request has admin token")
				}
				var body []byte
				if tc.body != "" {
					body = []byte(tc.body)
				}
				want, err := sign.SignRequest([]byte(testSecret), sign.Request{
					Method: tc.method, RequestURI: tc.uri, Timestamp: vectorTS, Nonce: tc.nonce, Body: body,
				})
				if err != nil {
					t.Fatal(err)
				}
				if got.header.Get(sign.HeaderSignature) != want {
					t.Fatalf("signature = %s, want %s", got.header.Get(sign.HeaderSignature), want)
				}
				if tc.wantSig != "" && got.header.Get(sign.HeaderSignature) != tc.wantSig {
					t.Fatalf("vector signature = %s", got.header.Get(sign.HeaderSignature))
				}
			}
			if tc.body == "" {
				if got.header.Get("Content-Type") != "" {
					t.Fatalf("content-type = %q", got.header.Get("Content-Type"))
				}
			} else if got.header.Get("Content-Type") != "application/json; charset=utf-8" {
				t.Fatalf("content-type = %q", got.header.Get("Content-Type"))
			}
		})
	}
}

// TL-01 验收 2：两次调用的 nonce 不同。
func TestTL01NonceDiffers(t *testing.T) {
	t.Parallel()
	gw := &fakeGW{t: t, verifyMerchant: true, signResponse: true}
	srv := httptest.NewServer(gw.handler())
	t.Cleanup(srv.Close)
	q := &nonceQueue{v: []string{vectorNonce, otherNonce}}
	args := []string{"--gateway", srv.URL, "--app-id", testAppID, "--app-secret", testSecret, "balance"}
	for i := 0; i < 2; i++ {
		var stdout, stderr bytes.Buffer
		code := run(args, nil, &stdout, &stderr, fixedClock(), q.next)
		if code != 0 {
			t.Fatalf("call %d exit = %d stderr=%s", i, code, stderr.String())
		}
	}
	reqs := gw.requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d", len(reqs))
	}
	n1 := reqs[0].header.Get(sign.HeaderNonce)
	n2 := reqs[1].header.Get(sign.HeaderNonce)
	if n1 == n2 || n1 != vectorNonce || n2 != otherNonce {
		t.Fatalf("nonces = %s %s", n1, n2)
	}
}

func TestTL01RandNonce(t *testing.T) {
	t.Parallel()
	a, err := (randNonce{}).next()
	if err != nil {
		t.Fatal(err)
	}
	b, err := (randNonce{}).next()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("two crypto nonces are equal")
	}
	for _, n := range []string{a, b} {
		if !nonceOK(n) {
			t.Fatalf("nonce %q", n)
		}
	}
}

// TL-01 验收 3：正确签名的响应输出 JSON 且退出码 0；签名被篡改退出码 1；code ≠ 0 退出码 1。
func TestTL01Response(t *testing.T) {
	t.Parallel()
	t.Run("signed ok", func(t *testing.T) {
		t.Parallel()
		gw := &fakeGW{t: t, verifyMerchant: true, signResponse: true, respBody: []byte(v3Body), respTS: v3TS, respNonce: v3Nonce, respSig: v3Sig}
		srv := httptest.NewServer(gw.handler())
		t.Cleanup(srv.Close)
		var stdout, stderr bytes.Buffer
		code := run([]string{"--gateway", srv.URL, "--app-id", testAppID, "--app-secret", testSecret, "balance"}, nil, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
		if code != 0 {
			t.Fatalf("exit = %d stderr=%s", code, stderr.String())
		}
		if stdout.String() != v3Body {
			t.Fatalf("stdout = %q", stdout.String())
		}
		if stderr.Len() != 0 {
			t.Fatalf("stderr = %s", stderr.String())
		}
	})
	t.Run("tampered", func(t *testing.T) {
		t.Parallel()
		gw := &fakeGW{t: t, verifyMerchant: true, signResponse: true, tamper: true}
		srv := httptest.NewServer(gw.handler())
		t.Cleanup(srv.Close)
		var stdout, stderr bytes.Buffer
		code := run([]string{"--gateway", srv.URL, "--app-id", testAppID, "--app-secret", testSecret, "balance"}, nil, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
		if code != 1 {
			t.Fatalf("exit = %d", code)
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q", stdout.String())
		}
		if stderr.String() != "paycli: response signature invalid\n" {
			t.Fatalf("stderr = %q", stderr.String())
		}
		if strings.Contains(stderr.String(), testSecret) || strings.Contains(stderr.String(), v1Sig) || strings.Contains(stderr.String(), v3Sig) {
			t.Fatalf("stderr leaked material: %s", stderr.String())
		}
	})
	t.Run("business code", func(t *testing.T) {
		t.Parallel()
		raw := []byte(`{"code":40002,"msg":"insufficient balance","data":{}}`)
		gw := &fakeGW{t: t, verifyMerchant: true, signResponse: true, respBody: raw}
		srv := httptest.NewServer(gw.handler())
		t.Cleanup(srv.Close)
		var stdout, stderr bytes.Buffer
		code := run([]string{"--gateway", srv.URL, "--app-id", testAppID, "--app-secret", testSecret, "balance"}, nil, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
		if code != 1 {
			t.Fatalf("exit = %d stderr=%s", code, stderr.String())
		}
		if stdout.String() != string(raw) {
			t.Fatalf("stdout = %q", stdout.String())
		}
		if stderr.Len() != 0 {
			t.Fatalf("stderr = %s", stderr.String())
		}
	})
	t.Run("admin unsigned code", func(t *testing.T) {
		t.Parallel()
		raw := []byte(`{"code":10001,"msg":"invalid parameter","data":{}}`)
		gw := &fakeGW{t: t, respBody: raw}
		srv := httptest.NewServer(gw.handler())
		t.Cleanup(srv.Close)
		var stdout, stderr bytes.Buffer
		code := run([]string{"--gateway", srv.URL, "--admin-token", testToken, "admin", "check"}, nil, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
		if code != 1 {
			t.Fatalf("exit = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
		}
		if stdout.String() != string(raw) {
			t.Fatalf("stdout = %q", stdout.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), testToken) {
			t.Fatal("admin output contains token")
		}
		if gw.requests()[0].header.Get(sign.HeaderSignature) != "" {
			t.Fatal("admin command signed the request")
		}
	})
	t.Run("admin ok without signature", func(t *testing.T) {
		t.Parallel()
		gw := &fakeGW{t: t}
		srv := httptest.NewServer(gw.handler())
		t.Cleanup(srv.Close)
		var stdout, stderr bytes.Buffer
		code := run([]string{"--gateway", srv.URL, "--admin-token", testToken, "admin", "recon-run", "--channel", "mockpay", "--bill-date", "2026-10-09", "--operator", "ops"}, nil, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
		if code != 0 {
			t.Fatalf("exit = %d stderr=%s", code, stderr.String())
		}
		if stdout.String() != okBody {
			t.Fatalf("stdout = %q", stdout.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), testToken) {
			t.Fatal("admin output contains token")
		}
	})
}

func TestTL01DoesNotSendInvalid(t *testing.T) {
	t.Parallel()
	gw := &fakeGW{t: t, verifyMerchant: true, signResponse: true}
	srv := httptest.NewServer(gw.handler())
	t.Cleanup(srv.Close)
	base := []string{"--gateway", srv.URL, "--app-id", testAppID, "--app-secret", testSecret, "--admin-token", testToken}
	cases := [][]string{
		{"query"},
		{"query", "--pay-no", "P", "--out-trade-no", "T"},
		{"query", "--pay-no", ""},
		{"refund", "--out-refund-no", "R", "--amount", "1"},
		{"refund", "--pay-no", "P", "--out-trade-no", "T", "--out-refund-no", "R", "--amount", "1"},
		{"query-refund"},
		{"query-refund", "--refund-no", "R", "--out-refund-no", "O"},
		{"query-transfer"},
		{"query-transfer", "--transfer-no", "T", "--out-transfer-no", "O"},
		{"admin", "replay", "--operator", "ops"},
		{"admin", "replay", "--event-id", "E", "--biz-no", "B", "--event-type", "payment.succeeded", "--operator", "ops"},
		{"admin", "replay", "--biz-no", "B", "--operator", "ops"},
		{"admin", "recon-run", "--channel", "mockpay", "--bill-date", "2026-10-09"},
		{"create", "--out-trade-no", "T", "--subject", "s"},
		{"create", "--out-trade-no", "T", "--amount", "1.5", "--subject", "s"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(append(append([]string{}, base...), args...), nil, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
			if code != 1 {
				t.Fatalf("exit = %d stderr=%s", code, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q", stdout.String())
			}
			if strings.Contains(stderr.String(), testSecret) || strings.Contains(stderr.String(), testToken) {
				t.Fatalf("leak: %s", stderr.String())
			}
		})
	}
	if gw.requests() != nil && len(gw.requests()) != 0 {
		t.Fatalf("sent %d requests", len(gw.requests()))
	}
}

func TestTL01MissingConfig(t *testing.T) {
	t.Parallel()
	gw := &fakeGW{t: t}
	srv := httptest.NewServer(gw.handler())
	t.Cleanup(srv.Close)
	cases := []struct {
		name string
		args []string
		env  []string
		want string
	}{
		{
			name: "gateway",
			args: []string{"balance"},
			env:  []string{"PAYCLI_APP_ID=" + testAppID, "PAYCLI_APP_SECRET=" + testSecret},
			want: "gateway address is required",
		},
		{
			name: "app id",
			args: []string{"--gateway", srv.URL, "balance"},
			env:  []string{"PAYCLI_APP_SECRET=" + testSecret},
			want: "app_id is required",
		},
		{
			name: "secret",
			args: []string{"--gateway", srv.URL, "--app-id", testAppID, "balance"},
			env:  []string{"PAYCLI_ADMIN_TOKEN=" + testToken},
			want: "app_secret is required",
		},
		{
			name: "token",
			args: []string{"--gateway", srv.URL, "admin", "check"},
			env:  []string{"PAYCLI_APP_SECRET=" + testSecret},
			want: "admin token is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, tc.env, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
			if code != 1 {
				t.Fatalf("exit = %d stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q", stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q", stdout.String())
			}
			if strings.Contains(stderr.String(), testSecret) || strings.Contains(stderr.String(), testToken) {
				t.Fatalf("leak: %s", stderr.String())
			}
		})
	}
	if len(gw.requests()) != 0 {
		t.Fatalf("sent %d requests", len(gw.requests()))
	}
}

func TestTL01FlagOverridesEnv(t *testing.T) {
	t.Parallel()
	wrong := &fakeGW{t: t}
	wrongSrv := httptest.NewServer(wrong.handler())
	t.Cleanup(wrongSrv.Close)
	right := &fakeGW{t: t, verifyMerchant: true, signResponse: true}
	rightSrv := httptest.NewServer(right.handler())
	t.Cleanup(rightSrv.Close)
	env := []string{
		"PAYCLI_GATEWAY_URL=" + wrongSrv.URL,
		"PAYCLI_APP_ID=from-env",
		"PAYCLI_APP_SECRET=" + testSecret,
		"PAYCLI_ADMIN_TOKEN=env-token",
		"WORKER_ID=nope",
		"MYSQL_DSN=user:pass@tcp(127.0.0.1:3306)/db",
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"--gateway", rightSrv.URL, "--app-id", testAppID, "balance"}, env, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
	if code != 0 {
		t.Fatalf("exit = %d stderr=%s", code, stderr.String())
	}
	if len(wrong.requests()) != 0 {
		t.Fatal("flag did not override gateway")
	}
	reqs := right.requests()
	if len(reqs) != 1 || reqs[0].header.Get(sign.HeaderAppID) != testAppID {
		t.Fatalf("requests = %+v", reqs)
	}
	if strings.Contains(stdout.String()+stderr.String(), testSecret) {
		t.Fatal("output contains secret")
	}
}

func TestTL01DoesNotFollowRedirect(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	leaked := false
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		leaked = true
		mu.Unlock()
		_, _ = w.Write([]byte(okBody))
	}))
	t.Cleanup(next.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, next.URL+"/v1/balance", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	var stdout, stderr bytes.Buffer
	code := run([]string{"--gateway", srv.URL, "--app-id", testAppID, "--app-secret", testSecret, "--admin-token", testToken, "balance"}, nil, &stdout, &stderr, fixedClock(), oneNonce(vectorNonce))
	if code != 1 {
		t.Fatalf("exit = %d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	mu.Lock()
	got := leaked
	mu.Unlock()
	if got {
		t.Fatal("client followed redirect")
	}
	if strings.Contains(stdout.String()+stderr.String(), testSecret) || strings.Contains(stdout.String()+stderr.String(), testToken) {
		t.Fatal("output contains secret or token")
	}
}
