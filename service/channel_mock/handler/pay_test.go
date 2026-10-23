package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

const (
	payPath = "/mockpay/v1/pay"
	// testKey 与 docs/05-api.md §3.7 的渠道密钥一致。
	testKey = "demo_channel_key_mockpay_0123456"
	nowUnix = 1791500002
)

var shanghaiTZ = time.FixedZone("Asia/Shanghai", 8*3600)

type fakePayer struct {
	mu    sync.Mutex
	res   biz.PayResult
	err   error
	panic bool
	// validate 为真时模拟 biz 的参数校验：amount 不是正数返回 ErrInvalidRequest。
	validate bool
	got      []biz.PayRequest
}

func (f *fakePayer) Pay(_ context.Context, req biz.PayRequest) (biz.PayResult, error) {
	f.mu.Lock()
	f.got = append(f.got, req)
	f.mu.Unlock()
	if f.panic {
		panic("payer exploded")
	}
	if f.validate && req.Amount <= 0 {
		return biz.PayResult{}, fmt.Errorf("amount: %w", biz.ErrInvalidRequest)
	}
	return f.res, f.err
}

func (f *fakePayer) calls() []biz.PayRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]biz.PayRequest(nil), f.got...)
}

func testClock() *clock.Fake {
	c := &clock.Fake{}
	c.Set(time.Unix(nowUnix, 0).In(shanghaiTZ))
	return c
}

func newTestServer(t *testing.T, p Payer, key string) *server.Hertz {
	t.Helper()
	return newTestServerWithClock(t, p, key, testClock())
}

func newTestServerWithClock(t *testing.T, p Payer, key string, clk clock.Clock) *server.Hertz {
	t.Helper()
	srv := server.New(server.WithDisablePrintRoute(true))
	New(p, []byte(key), clk).Register(srv)
	return srv
}

func signBody(t *testing.T, key, ts string, body []byte) string {
	t.Helper()
	s, err := sign.SignChannel([]byte(key), sign.ChannelMessage{Timestamp: ts, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func post(srv *server.Hertz, body []byte, headers ...ut.Header) *ut.ResponseRecorder {
	return ut.PerformRequest(srv.Engine, http.MethodPost, payPath,
		&ut.Body{Body: bytes.NewReader(body), Len: len(body)}, headers...)
}

func signedPost(t *testing.T, srv *server.Hertz, key, ts string, body []byte) *ut.ResponseRecorder {
	t.Helper()
	return post(srv, body,
		ut.Header{Key: sign.HeaderMockTimestamp, Value: ts},
		ut.Header{Key: sign.HeaderMockSignature, Value: signBody(t, key, ts, body)},
		ut.Header{Key: "Content-Type", Value: "application/json"})
}

const goodBody = `{"req_no":"PA0001","amount":10000,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`

func nowTS() string { return fmt.Sprint(nowUnix) }

// assertBare 检查 4xx/5xx：body 为空、不带签名头。
func assertBare(t *testing.T, w *ut.ResponseRecorder, status int) {
	t.Helper()
	resp := w.Result()
	if resp.StatusCode() != status {
		t.Fatalf("status = %d, want %d, body = %q", resp.StatusCode(), status, resp.Body())
	}
	if len(resp.Body()) != 0 {
		t.Fatalf("body = %q, want empty", resp.Body())
	}
	if v := resp.Header.Peek(sign.HeaderMockSignature); len(v) != 0 {
		t.Fatalf("error response is signed: %q", v)
	}
	if v := resp.Header.Peek(sign.HeaderMockTimestamp); len(v) != 0 {
		t.Fatalf("error response has timestamp: %q", v)
	}
}

// docs/05-api.md §9.2：200 的响应用渠道密钥签名，时间戳取注入时钟；OK 时带流水号，不带 reason。
func TestPayOKIsSigned(t *testing.T) {
	p := &fakePayer{res: biz.PayResult{Result: biz.ResultOK, ChannelTxnNo: "MC0001", Status: "PROCESSING"}}
	srv := newTestServer(t, p, testKey)
	// 时间窗边界：恰好 ±300 秒仍接受。
	for _, ts := range []string{nowTS(), fmt.Sprint(nowUnix - 300), fmt.Sprint(nowUnix + 300)} {
		w := signedPost(t, srv, testKey, ts, []byte(goodBody))
		resp := w.Result()
		if resp.StatusCode() != http.StatusOK {
			t.Fatalf("ts %s: status = %d", ts, resp.StatusCode())
		}
		body := resp.Body()
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"result": "OK", "channel_txn_no": "MC0001", "status": "PROCESSING"}
		if fmt.Sprint(m) != fmt.Sprint(want) {
			t.Fatalf("body = %s", body)
		}
		if ct := string(resp.Header.ContentType()); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("content-type = %q", ct)
		}
		gotTS := string(resp.Header.Peek(sign.HeaderMockTimestamp))
		if gotTS != nowTS() {
			t.Fatalf("response timestamp = %q", gotTS)
		}
		sig := string(resp.Header.Peek(sign.HeaderMockSignature))
		if err := sign.VerifyChannel([]byte(testKey), sign.ChannelMessage{Timestamp: gotTS, Body: body}, sig); err != nil {
			t.Fatalf("response signature: %v", err)
		}
	}
	calls := p.calls()
	want := biz.PayRequest{ReqNo: "PA0001", Amount: 10000, Subject: "demo", NotifyURL: "http://gateway:8080/callback/mockpay"}
	if len(calls) != 3 || calls[0] != want {
		t.Fatalf("payer calls = %+v", calls)
	}
}

// docs/05-api.md §9.2：REJECTED 带 reason 和当前 status，不带 channel_txn_no。
func TestPayRejectedFields(t *testing.T) {
	p := &fakePayer{res: biz.PayResult{Result: biz.ResultRejected, Reason: biz.ReasonOrderClosed, Status: "CLOSED"}}
	srv := newTestServer(t, p, testKey)
	w := signedPost(t, srv, testKey, nowTS(), []byte(goodBody))
	resp := w.Result()
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode())
	}
	var m map[string]any
	if err := json.Unmarshal(resp.Body(), &m); err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 || m["result"] != "REJECTED" || m["reason"] != "ORDER_CLOSED" || m["status"] != "CLOSED" {
		t.Fatalf("body = %s", resp.Body())
	}
	if _, ok := m["channel_txn_no"]; ok {
		t.Fatal("REJECTED carries channel_txn_no")
	}
}

// CH-01 规则 1：验签失败（含缺头、格式错、时间窗外）返回 401，空 body，不签名，不进入业务。
func TestPayUnauthorized(t *testing.T) {
	body := []byte(goodBody)
	good := signBody(t, testKey, nowTS(), body)
	old := fmt.Sprint(nowUnix - 301)
	future := fmt.Sprint(nowUnix + 301)
	cases := []struct {
		name    string
		body    []byte
		headers []ut.Header
	}{
		{"no headers", body, nil},
		{"no timestamp", body, []ut.Header{{Key: sign.HeaderMockSignature, Value: good}}},
		{"no signature", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: nowTS()}}},
		{"wrong key", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: nowTS()},
			{Key: sign.HeaderMockSignature, Value: signBody(t, "another_key_0123456789abcdef0123", nowTS(), body)}}},
		{"malformed signature", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: nowTS()},
			{Key: sign.HeaderMockSignature, Value: "abc"}}},
		{"tampered body", []byte(strings.Replace(goodBody, "10000", "10001", 1)), []ut.Header{
			{Key: sign.HeaderMockTimestamp, Value: nowTS()}, {Key: sign.HeaderMockSignature, Value: good}}},
		{"timestamp not a number", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: "abc"},
			{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, "abc", body)}}},
		{"timestamp 301s old", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: old},
			{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, old, body)}}},
		{"timestamp 301s ahead", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: future},
			{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, future, body)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePayer{res: biz.PayResult{Result: biz.ResultOK}}
			srv := newTestServer(t, p, testKey)
			assertBare(t, post(srv, c.body, c.headers...), http.StatusUnauthorized)
			if len(p.calls()) != 0 {
				t.Fatal("payer called after failed verification")
			}
		})
	}
}

// 已确认的契约（api 2.1.0 §9.1）：签名正确但参数非法 → 400，空 body，不签名。
func TestPayBadRequest(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		reachPayer bool
	}{
		{"empty body", ``, false},
		{"not json", `{`, false},
		{"json array", `[]`, false},
		{"decimal amount", `{"req_no":"PA1","amount":100.5,"subject":"s","notify_url":"http://g/cb"}`, false},
		{"string amount", `{"req_no":"PA1","amount":"100","subject":"s","notify_url":"http://g/cb"}`, false},
		{"amount overflow", `{"req_no":"PA1","amount":99999999999999999999,"subject":"s","notify_url":"http://g/cb"}`, false},
		{"numeric req_no", `{"req_no":1,"amount":100,"subject":"s","notify_url":"http://g/cb"}`, false},
		{"trailing data", goodBody + `{}`, false},
		{"trailing garbage", goodBody + `x`, false},
		{"null", `null`, false},
		{"json string", `"PA1"`, false},
		{"missing amount", `{"req_no":"PA1","subject":"s","notify_url":"http://g/cb"}`, true},
		// 审查 P3：非法 UTF-8 在解码前拒绝，否则 encoding/json 会把它换成 U+FFFD 绕过业务校验。
		{"invalid utf8 in req_no", "{\"req_no\":\"PA\xff\",\"amount\":100,\"subject\":\"s\",\"notify_url\":\"http://g/cb\"}", false},
		{"invalid utf8 in subject", "{\"req_no\":\"PA1\",\"amount\":100,\"subject\":\"\xc3\",\"notify_url\":\"http://g/cb\"}", false},
		{"invalid utf8 in unknown field", "{\"req_no\":\"PA1\",\"amount\":100,\"subject\":\"s\",\"notify_url\":\"http://g/cb\",\"x\":\"\xff\"}", false},
		// 重复键：encoding/json 默认取最后一个，这里一律拒绝（含未知字段、转义后同名）。
		{"duplicate amount", `{"req_no":"PA1","amount":100,"amount":200,"subject":"s","notify_url":"http://g/cb"}`, false},
		{"duplicate unknown key", `{"req_no":"PA1","amount":100,"subject":"s","notify_url":"http://g/cb","x":1,"x":2}`, false},
		{"duplicate via escape", `{"req_no":"PA1","req\u005fno":"PA2","amount":100,"subject":"s","notify_url":"http://g/cb"}`, false},
		// 字段名大小写：encoding/json 默认不区分，这里只认全小写，大小写变体拒绝。
		{"capitalized amount", `{"req_no":"PA1","Amount":100,"subject":"s","notify_url":"http://g/cb"}`, false},
		{"upper req_no", `{"REQ_NO":"PA1","amount":100,"subject":"s","notify_url":"http://g/cb"}`, false},
		{"case variant beside exact", `{"req_no":"PA1","amount":100,"AMOUNT":200,"subject":"s","notify_url":"http://g/cb"}`, false},
		// U+017F（长 s）按 Unicode 折叠等于 s，encoding/json 会把它当成 subject。
		{"unicode fold of subject", `{"req_no":"PA1","amount":100,"\u017fubject":"s","notify_url":"http://g/cb"}`, false},
		{"null amount", `{"req_no":"PA1","amount":null,"subject":"s","notify_url":"http://g/cb"}`, true},
		{"missing comma", `{"req_no":"PA1" "amount":100,"subject":"s","notify_url":"http://g/cb"}`, false},
		{"bad unknown value", `{"req_no":"PA1","amount":100,"subject":"s","notify_url":"http://g/cb","x":}`, false},
		{"unclosed object", `{"req_no":"PA1","amount":100`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePayer{res: biz.PayResult{Result: biz.ResultOK}, validate: true}
			srv := newTestServer(t, p, testKey)
			assertBare(t, signedPost(t, srv, testKey, nowTS(), []byte(c.body)), http.StatusBadRequest)
			if got := len(p.calls()) > 0; got != c.reachPayer {
				t.Fatalf("payer reached = %v, want %v", got, c.reachPayer)
			}
		})
	}
}

// 未知字段（含嵌套对象、数组）忽略，已知字段照常解析。
func TestPayIgnoresUnknownFields(t *testing.T) {
	p := &fakePayer{res: biz.PayResult{Result: biz.ResultOK, ChannelTxnNo: "MC1", Status: "PROCESSING"}}
	srv := newTestServer(t, p, testKey)
	body := `{"extra":{"a":[1,{"b":null}],"amount":5},"req_no":"PA0001","amount":10000,"subject":"demo",` +
		`"notify_url":"http://gateway:8080/callback/mockpay","tags":["x","y"]}`
	if w := signedPost(t, srv, testKey, nowTS(), []byte(body)); w.Result().StatusCode() != http.StatusOK {
		t.Fatalf("status = %d", w.Result().StatusCode())
	}
	want := biz.PayRequest{ReqNo: "PA0001", Amount: 10000, Subject: "demo", NotifyURL: "http://gateway:8080/callback/mockpay"}
	if calls := p.calls(); len(calls) != 1 || calls[0] != want {
		t.Fatalf("payer calls = %+v", calls)
	}
}

// 孤立代理转义 \udc00 是合法 UTF-8 字节，encoding/json 解码成 U+FFFD 后交给业务；
// 业务对 req_no、notify_url 拒绝 U+FFFD（返回 400，见 biz 测试与集成测试）。
func TestPayLoneSurrogateDecodesToReplacement(t *testing.T) {
	p := &fakePayer{res: biz.PayResult{Result: biz.ResultOK, ChannelTxnNo: "MC1", Status: "PROCESSING"}}
	srv := newTestServer(t, p, testKey)
	body := `{"req_no":"PA\udc00","amount":10000,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`
	signedPost(t, srv, testKey, nowTS(), []byte(body))
	if calls := p.calls(); len(calls) != 1 || calls[0].ReqNo != "PA\uFFFD" {
		t.Fatalf("payer calls = %+v", calls)
	}
}

// 审查 P3：时间窗按整秒比较（本地时间截到秒再减），本地时间带毫秒时 ±300 秒仍接受，±301 秒拒绝。
func TestPayTimestampWindowWholeSeconds(t *testing.T) {
	for _, ms := range []int{1, 999} {
		clk := &clock.Fake{}
		clk.Set(time.Unix(nowUnix, int64(ms)*int64(time.Millisecond)).In(shanghaiTZ))
		cases := []struct {
			offset int
			status int
		}{
			{-300, http.StatusOK},
			{300, http.StatusOK},
			{-301, http.StatusUnauthorized},
			{301, http.StatusUnauthorized},
		}
		for _, c := range cases {
			t.Run(fmt.Sprintf("%dms/%+d", ms, c.offset), func(t *testing.T) {
				p := &fakePayer{res: biz.PayResult{Result: biz.ResultOK, ChannelTxnNo: "MC1", Status: "PROCESSING"}}
				srv := newTestServerWithClock(t, p, testKey, clk)
				w := signedPost(t, srv, testKey, fmt.Sprint(nowUnix+c.offset), []byte(goodBody))
				if c.status != http.StatusOK {
					assertBare(t, w, c.status)
					return
				}
				if got := w.Result().StatusCode(); got != c.status {
					t.Fatalf("status = %d, want %d", got, c.status)
				}
			})
		}
	}
}

// §3.7 V4 向量（回调 body）发到 /pay：签名通过（不是 401），缺 amount 所以 400。
func TestPayV4VectorPassesSignature(t *testing.T) {
	p := &fakePayer{validate: true}
	srv := newTestServer(t, p, testKey)
	body := []byte(`{"txn_type":"PAY","req_no":"PA0000000000000000001","status":"SUCCESS"}`)
	w := post(srv, body,
		ut.Header{Key: sign.HeaderMockTimestamp, Value: "1791500002"},
		ut.Header{Key: sign.HeaderMockSignature, Value: "d1zUa8yWKf9rXU7JL+9BMbK+Mg5ZuQHLao4iLCVdKKY="})
	assertBare(t, w, http.StatusBadRequest)
	if calls := p.calls(); len(calls) != 1 || calls[0].ReqNo != "PA0000000000000000001" {
		t.Fatalf("payer calls = %+v", calls)
	}
}

// 内部错误、空密钥和 panic 都返回 500，空 body，不签名；panic 后服务继续可用。
func TestPayInternalErrors(t *testing.T) {
	t.Run("payer error", func(t *testing.T) {
		srv := newTestServer(t, &fakePayer{err: errors.New("db down")}, testKey)
		assertBare(t, signedPost(t, srv, testKey, nowTS(), []byte(goodBody)), http.StatusInternalServerError)
	})
	t.Run("empty key", func(t *testing.T) {
		p := &fakePayer{res: biz.PayResult{Result: biz.ResultOK}}
		srv := newTestServer(t, p, "")
		assertBare(t, signedPost(t, srv, testKey, nowTS(), []byte(goodBody)), http.StatusInternalServerError)
		if len(p.calls()) != 0 {
			t.Fatal("payer called with empty key")
		}
	})
	t.Run("panic", func(t *testing.T) {
		p := &fakePayer{panic: true}
		srv := newTestServer(t, p, testKey)
		assertBare(t, signedPost(t, srv, testKey, nowTS(), []byte(goodBody)), http.StatusInternalServerError)
		p.mu.Lock()
		p.panic = false
		p.res = biz.PayResult{Result: biz.ResultOK, ChannelTxnNo: "MC1", Status: "PROCESSING"}
		p.mu.Unlock()
		if w := signedPost(t, srv, testKey, nowTS(), []byte(goodBody)); w.Result().StatusCode() != http.StatusOK {
			t.Fatalf("after panic status = %d", w.Result().StatusCode())
		}
	})
}

// 日志写访问记录，不含密钥和签名值。
func TestPayLogsWithoutSecrets(t *testing.T) {
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	var buf bytes.Buffer
	logx.SetDefault("channel-mock", &buf)

	srv := newTestServer(t, &fakePayer{res: biz.PayResult{Result: biz.ResultOK, ChannelTxnNo: "MC1", Status: "PROCESSING"}}, testKey)
	w := signedPost(t, srv, testKey, nowTS(), []byte(goodBody))
	respSig := string(w.Result().Header.Peek(sign.HeaderMockSignature))
	reqSig := signBody(t, testKey, nowTS(), []byte(goodBody))
	post(srv, []byte(goodBody), ut.Header{Key: sign.HeaderMockTimestamp, Value: nowTS()},
		ut.Header{Key: sign.HeaderMockSignature, Value: "bad"})
	srvErr := newTestServer(t, &fakePayer{err: errors.New("db down")}, testKey)
	signedPost(t, srvErr, testKey, nowTS(), []byte(goodBody))

	out := buf.String()
	for _, secret := range []string{testKey, respSig, reqSig} {
		if secret != "" && strings.Contains(out, secret) {
			t.Fatalf("log contains secret material %q:\n%s", secret, out)
		}
	}
	if strings.Count(out, payPath) < 3 {
		t.Fatalf("missing access logs:\n%s", out)
	}
	for _, status := range []string{"200", "401", "500"} {
		if !strings.Contains(out, status) {
			t.Fatalf("log missing status %s:\n%s", status, out)
		}
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// Serve 在真实端口上处理签名请求，ctx 取消后返回 nil。
func TestServeHandlesRequestsAndStops(t *testing.T) {
	addr := freePort(t)
	p := &fakePayer{res: biz.PayResult{Result: biz.ResultOK, ChannelTxnNo: "MC1", Status: "PROCESSING"}}
	clk := &clock.Fake{}
	clk.Set(clock.Real{}.Now())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, addr, New(p, []byte(testKey), clk)) }()

	ts := sign.FormatTimestamp(clk.Now())
	var resp *http.Response
	timeout := time.After(5 * time.Second)
	for resp == nil {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+addr+payPath, strings.NewReader(goodBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(sign.HeaderMockTimestamp, ts)
		req.Header.Set(sign.HeaderMockSignature, signBody(t, testKey, ts, []byte(goodBody)))
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			select {
			case <-timeout:
				t.Fatalf("server not reachable: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	_ = resp.Body.Close()
	http.DefaultClient.CloseIdleConnections()
	if resp.StatusCode != http.StatusOK || resp.Header.Get(sign.HeaderMockSignature) == "" {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

// 端口被占用时 Serve 返回错误，不阻塞。
func TestServeReturnsListenError(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	done := make(chan error, 1)
	go func() {
		done <- Serve(context.Background(), ln.Addr().String(), New(&fakePayer{}, []byte(testKey), testClock()))
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Serve = nil on busy port")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return on busy port")
	}
}
