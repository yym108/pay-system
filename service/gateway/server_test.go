package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
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

func TestReadCappedStopsAtExtraByte(t *testing.T) {
	body := bytes.Repeat([]byte("a"), maxBodyBytes)
	got, tooBig, err := readCapped(bytes.NewReader(body), maxBodyBytes)
	if err != nil || tooBig || !bytes.Equal(got, body) {
		t.Fatalf("exact: n=%d tooBig=%v err=%v", len(got), tooBig, err)
	}

	over := &stopReader{limit: maxBodyBytes + 1}
	_, tooBig, err = readCapped(over, maxBodyBytes)
	if err != nil || !tooBig {
		t.Fatalf("over: tooBig=%v err=%v read=%d", tooBig, err, over.n)
	}
	if over.n != maxBodyBytes+1 {
		t.Fatalf("read %d bytes, want %d", over.n, maxBodyBytes+1)
	}
}

// stopReader 在读满 limit 之后如果再被读就返回错误。
type stopReader struct {
	n, limit int
}

func (s *stopReader) Read(p []byte) (int, error) {
	if s.n >= s.limit {
		return 0, io.ErrUnexpectedEOF
	}
	want := s.limit - s.n
	if want > len(p) {
		want = len(p)
	}
	for i := 0; i < want; i++ {
		p[i] = 'a'
	}
	s.n += want
	return want, nil
}

func TestNonceUsesUnbiasedBytes(t *testing.T) {
	nonce, err := randomNonce()
	if err != nil {
		t.Fatal(err)
	}
	if len(nonce) != nonceLen || strings.Trim(nonce, nonceAlphabet) != "" {
		t.Fatalf("nonce %q", nonce)
	}
}

// GW-01 验收 1：handler panic 返回 500 / 10003，进程不退出。
func TestGW01PanicReturnsInternalAndStaysUp(t *testing.T) {
	logs := newLogBuf()
	h := NewMerchant(WithLogger(logx.New("gateway", logs)))
	addr := startGW(t, h, func(r *server.Hertz) {
		r.GET("/panic", func(context.Context, *app.RequestContext) {
			panic("boom-secret-panic")
		})
		r.GET("/ok", func(_ context.Context, c *app.RequestContext) {
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	got := do(t, http.MethodGet, "http://"+addr+"/panic", nil, nil)
	if got.status != http.StatusInternalServerError {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	env := decodeEnv(t, got.body)
	if env.Code != errno.ErrInternal.Code || env.Code == errno.ErrInvalidParam.Code {
		t.Fatalf("code %d", env.Code)
	}
	if env.Msg != errno.ErrInternal.Msg || string(env.Data) != "{}" {
		t.Fatalf("msg %q data %s", env.Msg, env.Data)
	}
	if env.RequestID == "" || got.header.Get(headerRequestID) != env.RequestID {
		t.Fatalf("request id header %q body %q", got.header.Get(headerRequestID), env.RequestID)
	}
	if bytes.Contains(got.body, []byte("boom-secret-panic")) || strings.Contains(logs.String(), "boom-secret-panic") {
		t.Fatalf("panic text leaked body %s log %s", got.body, logs.String())
	}
	if signed(got.header) {
		t.Fatal("panic response was signed")
	}
	got = do(t, http.MethodGet, "http://"+addr+"/ok", nil, nil)
	if got.status != http.StatusOK {
		t.Fatalf("after panic: status %d body %s", got.status, got.body)
	}
}

// GW-01 验收 2：超过 64KB 的请求被拒绝。恰好 64KB 通过，handler 还能读到 body。
func TestGW01BodyLimit(t *testing.T) {
	var seen int
	addr := startGW(t, NewMerchant(), func(r *server.Hertz) {
		r.POST("/echo", func(_ context.Context, c *app.RequestContext) {
			body, err := c.Body()
			if err != nil {
				t.Errorf("body: %v", err)
			}
			seen = len(body)
			WriteOK(c, map[string]int{"n": len(body)})
		})
	})

	got := do(t, http.MethodPost, "http://"+addr+"/echo", bytes.Repeat([]byte("a"), maxBodyBytes), nil)
	if got.status != http.StatusOK {
		t.Fatalf("exact status %d body %s", got.status, got.body)
	}
	if seen != maxBodyBytes {
		t.Fatalf("handler saw %d", seen)
	}

	seen = -1
	over := bytes.Repeat([]byte("b"), maxBodyBytes+1)
	got = do(t, http.MethodPost, "http://"+addr+"/echo", over, nil)
	if got.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("over status %d body %s", got.status, got.body)
	}
	env := decodeEnv(t, got.body)
	if env.Code != errno.ErrInvalidParam.Code || env.Msg != errno.ErrInvalidParam.Msg || string(env.Data) != "{}" {
		t.Fatalf("over env %+v", env)
	}
	if signed(got.header) {
		t.Fatal("413 was signed")
	}
	if id := got.header.Get(headerRequestID); id != env.RequestID || !requestIDPattern.MatchString(id) {
		t.Fatalf("id %q body %q", id, env.RequestID)
	}
	if bytes.Contains(got.body, over) {
		t.Fatal("413 body contains the request")
	}
	if seen != -1 {
		t.Fatal("oversize request reached the handler")
	}
}

func TestMiddlewareOrder(t *testing.T) {
	addr := startGW(t, NewMerchant(), func(r *server.Hertz) {
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
}

func TestRequestIDIgnoresClientAndMatchesHeader(t *testing.T) {
	addr := startGW(t, NewMerchant(), func(r *server.Hertz) {
		r.GET("/ok", func(_ context.Context, c *app.RequestContext) {
			WriteOK(c, map[string]bool{"ok": true})
		})
	})
	hdr := http.Header{"X-Request-Id": []string{"from-client"}}
	got := do(t, http.MethodGet, "http://"+addr+"/ok", nil, hdr)
	env := decodeEnv(t, got.body)
	id := got.header.Get(headerRequestID)
	if id != env.RequestID || id == "from-client" || !requestIDPattern.MatchString(id) {
		t.Fatalf("id %q body %q", id, env.RequestID)
	}
}

// GW-03 验收：生产 Register 上未签名的 GET /v1/balance 返回 20001，不签名，lookup 未调用。
func TestUnknownPath(t *testing.T) {
	look := &fakeLookup{}
	addr := startGW(t, NewMerchant(WithMerchantLookup(look)), nil)
	got := do(t, http.MethodGet, "http://"+addr+"/v1/balance", nil, nil)
	assertErrno(t, got, errno.ErrSignInvalid)
	if calls := look.calls(); len(calls) != 0 {
		t.Fatalf("lookup %q", calls)
	}
}

func TestPlatformSignAndAdminNeverSigns(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	secret := []byte("demo-sign-secret")
	merchant := NewMerchant(WithClock(clk), WithSignSecret(secret))
	addr := startGW(t, merchant, func(r *server.Hertz) {
		r.GET("/signed", func(_ context.Context, c *app.RequestContext) {
			MarkVerified(c)
			WriteOK(c, map[string]string{"html": "<tag>"})
		})
		r.GET("/plain", func(_ context.Context, c *app.RequestContext) {
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	got := do(t, http.MethodGet, "http://"+addr+"/signed", nil, nil)
	if got.status != http.StatusOK {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if bytes.Contains(got.body, []byte(`\u003c`)) || !bytes.Contains(got.body, []byte("<tag>")) {
		t.Fatalf("html escaped: %s", got.body)
	}
	if err := sign.VerifyPlatform(secret, sign.PlatformMessage{
		Timestamp: got.header.Get(sign.HeaderTimestamp),
		Nonce:     got.header.Get(sign.HeaderNonce),
		Body:      got.body,
	}, got.header.Get(sign.HeaderSignature)); err != nil {
		t.Fatal(err)
	}
	if got.header.Get(sign.HeaderTimestamp) != sign.FormatTimestamp(clk.Now()) {
		t.Fatalf("timestamp %s", got.header.Get(sign.HeaderTimestamp))
	}
	nonce := got.header.Get(sign.HeaderNonce)
	if len(nonce) != nonceLen || strings.Trim(nonce, nonceAlphabet) != "" {
		t.Fatalf("nonce %q", nonce)
	}
	plain := do(t, http.MethodGet, "http://"+addr+"/plain", nil, nil)
	if signed(plain.header) {
		t.Fatal("unsigned request was signed")
	}

	adminAddr := startGW(t, NewAdmin(WithClock(clk), WithSignSecret(secret)), func(r *server.Hertz) {
		r.GET("/admin", func(_ context.Context, c *app.RequestContext) {
			MarkVerified(c)
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	adminResp := do(t, http.MethodGet, "http://"+adminAddr+"/admin", nil, nil)
	if adminResp.status != http.StatusOK || signed(adminResp.header) {
		t.Fatalf("admin status %d signed %v body %s", adminResp.status, signed(adminResp.header), adminResp.body)
	}
}

func TestSignFailureOmitsSecret(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	const secret = "super-secret-value" //nolint:gosec // G101: 测试哨兵，用来确认日志没有密钥。
	logs := newLogBuf()
	h := NewMerchant(WithClock(clk), WithSignSecret([]byte(secret)), WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
	h.nextNonce = func() (string, error) { return "", io.EOF }
	addr := startGW(t, h, func(r *server.Hertz) {
		r.GET("/signed", func(_ context.Context, c *app.RequestContext) {
			MarkVerified(c)
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	got := do(t, http.MethodGet, "http://"+addr+"/signed", nil, nil)
	env := decodeEnv(t, got.body)
	if got.status != http.StatusInternalServerError || env.Code != errno.ErrInternal.Code || env.Msg != errno.ErrInternal.Msg {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if signed(got.header) || bytes.Contains(got.body, []byte(secret)) || strings.Contains(logs.String(), secret) {
		t.Fatalf("secret leaked body %s log %s", got.body, logs.String())
	}

	empty := NewMerchant(WithClock(clk), WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
	addr = startGW(t, empty, func(r *server.Hertz) {
		r.GET("/signed", func(_ context.Context, c *app.RequestContext) {
			MarkVerified(c)
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	got = do(t, http.MethodGet, "http://"+addr+"/signed", nil, nil)
	env = decodeEnv(t, got.body)
	if got.status != http.StatusInternalServerError || env.Code != errno.ErrInternal.Code || signed(got.header) {
		t.Fatalf("empty key status %d body %s", got.status, got.body)
	}
}

func TestAccessLogSkipsSensitiveFields(t *testing.T) {
	logs := newLogBuf()
	const (
		bodyMarker = "body-marker-do-not-log"
		sigMarker  = "sig-marker-do-not-log"
		tokMarker  = "token-marker-do-not-log"
	)
	addr := startGW(t, NewMerchant(WithLogger(logx.New("gateway", logs))), func(r *server.Hertz) {
		r.POST("/ok", func(_ context.Context, c *app.RequestContext) {
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	hdr := http.Header{}
	hdr.Set(sign.HeaderSignature, sigMarker)
	hdr.Set("X-Admin-Token", tokMarker)
	got := do(t, http.MethodPost, "http://"+addr+"/ok", []byte(bodyMarker), hdr)
	// 响应体先写回客户端，访问日志紧跟在后面。最多等 2 秒。
	wait, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case <-logs.ready:
	case <-wait.Done():
		t.Fatalf("status %d body %s log %s", got.status, got.body, logs.String())
	}
	text := logs.String()
	if !strings.Contains(text, `"merchant_id":""`) {
		t.Fatalf("status %d body %s log %s", got.status, got.body, text)
	}
	if strings.Contains(text, bodyMarker) || strings.Contains(text, sigMarker) || strings.Contains(text, tokMarker) {
		t.Fatalf("status %d body %s log %s", got.status, got.body, text)
	}
}

// logBuf 让服务端写日志和测试读取串行。ready 在第一次写入后关闭。
type logBuf struct {
	mu    sync.Mutex
	b     bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

func newLogBuf() *logBuf {
	return &logBuf{ready: make(chan struct{})}
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	n, err := l.b.Write(p)
	l.mu.Unlock()
	l.once.Do(func() { close(l.ready) })
	return n, err
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

var (
	requestIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	httpClient       = &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
)

func startGW(t *testing.T, h *Handler, extra func(*server.Hertz)) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := Listen(ctx, ":0")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	srv := NewEngine(ln)
	h.Register(srv)
	if extra != nil {
		extra(srv)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- Spin(ctx, srv) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("server did not exit")
		}
	})
	waitListen(t, ln.Addr().String())
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort("127.0.0.1", port)
}

func waitListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now()
	deadline = deadline.Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	dialer := &net.Dialer{Timeout: 50 * time.Millisecond}
	var last error
	for {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		last = err
		now := time.Now()
		if now.After(deadline) {
			t.Fatal(last)
		}
	}
}

// gwResp 只留下状态、头和 body。连接在 do 里关掉，调用方拿不到 *http.Response。
type gwResp struct {
	status int
	header http.Header
	body   []byte
}

func do(t *testing.T, method, url string, body []byte, hdr http.Header) gwResp {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1024))
	if err != nil {
		t.Fatal(err)
	}
	return gwResp{status: resp.StatusCode, header: resp.Header.Clone(), body: raw}
}

func decodeEnv(t *testing.T, raw []byte) envelope {
	t.Helper()
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("json %s: %v", raw, err)
	}
	return env
}

func signed(h http.Header) bool {
	return h.Get(sign.HeaderSignature) != "" || h.Get(sign.HeaderTimestamp) != "" || h.Get(sign.HeaderNonce) != ""
}
