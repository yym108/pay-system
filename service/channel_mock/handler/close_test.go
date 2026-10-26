package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

func closeAccepted(req biz.CloseRequest) bool {
	if req.ReqNo == "" || !utf8.ValidString(req.ReqNo) || strings.ContainsRune(req.ReqNo, utf8.RuneError) {
		return false
	}
	return utf8.RuneCountInString(req.ReqNo) <= 32
}

func assertCloseKeys(t *testing.T, body []byte) {
	t.Helper()
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(body, &keys); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("keys = %v", keys)
	}
	if _, ok := keys["result"]; !ok {
		t.Fatal("missing result")
	}
	if _, ok := keys["status"]; !ok {
		t.Fatal("missing status")
	}
}

// CH-03：200 只有 result 和 status，渠道签名可用 pkg/sign 验过。
func TestCloseSignedResponses(t *testing.T) {
	cases := []struct {
		name string
		res  biz.CloseResult
		want string
	}{
		{name: "closed", res: biz.CloseResult{Result: biz.ResultOK, Status: "CLOSED"}, want: `{"result":"OK","status":"CLOSED"}`},
		{name: "failed", res: biz.CloseResult{Result: biz.ResultOK, Status: "FAILED"}, want: `{"result":"OK","status":"FAILED"}`},
		{name: "already paid", res: biz.CloseResult{Result: biz.ResultAlreadyPaid, Status: "SUCCESS"}, want: `{"result":"ALREADY_PAID","status":"SUCCESS"}`},
	}
	body := []byte(`{"req_no":"PA0001"}`)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePayer{validate: true, cres: c.res}
			srv := newTestServer(t, p, testKey)
			for _, ts := range []string{nowTS(), fmt.Sprint(nowUnix - 300), fmt.Sprint(nowUnix + 300)} {
				w := postSigned(t, srv, ClosePath, testKey, ts, body)
				assertSignedJSON(t, w, c.want)
				assertCloseKeys(t, w.Result().Body())
			}
			calls := p.closeCalls()
			if len(calls) != 3 || calls[0].ReqNo != "PA0001" {
				t.Fatalf("calls = %+v", calls)
			}
			if _, ok := p.closeCtx.Deadline(); !ok {
				t.Fatal("close context has no deadline")
			}
		})
	}
}

func TestCloseIgnoresUnknownFields(t *testing.T) {
	p := &fakePayer{validate: true, cres: biz.CloseResult{Result: biz.ResultOK, Status: "CLOSED"}}
	srv := newTestServer(t, p, testKey)
	body := `{"amount":10009,"notify_url":"http://evil.example/cb","req_no":"PA0001","extra":{"a":1}}` + " \n"
	w := postSigned(t, srv, ClosePath, testKey, nowTS(), []byte(body))
	assertSignedJSON(t, w, `{"result":"OK","status":"CLOSED"}`)
	assertCloseKeys(t, w.Result().Body())
	calls := p.closeCalls()
	if len(calls) != 1 || calls[0] != (biz.CloseRequest{ReqNo: "PA0001"}) {
		t.Fatalf("calls = %+v", calls)
	}
}

// CH-03：验签失败 401，空 body，不签名，不进入业务。
func TestCloseUnauthorized(t *testing.T) {
	body := []byte(`{"req_no":"PA0001"}`)
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
		{"tampered body", []byte(`{"req_no":"PA0002"}`), []ut.Header{
			{Key: sign.HeaderMockTimestamp, Value: nowTS()}, {Key: sign.HeaderMockSignature, Value: good}}},
		{"timestamp not a number", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: "abc"},
			{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, "abc", body)}}},
		{"timestamp overflow", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: "999999999999999999999"},
			{Key: sign.HeaderMockSignature, Value: "abc"}}},
		{"timestamp 301s old", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: old},
			{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, old, body)}}},
		{"timestamp 301s ahead", body, []ut.Header{{Key: sign.HeaderMockTimestamp, Value: future},
			{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, future, body)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePayer{validate: true, cres: biz.CloseResult{Result: biz.ResultOK, Status: "CLOSED"}}
			srv := newTestServer(t, p, testKey)
			assertBare(t, postTo(srv, ClosePath, c.body, c.headers...), http.StatusUnauthorized)
			if len(p.closeCalls()) != 0 {
				t.Fatal("close called after failed verification")
			}
		})
	}
}

// CH-03：空 req_no、超长、U+FFFD、重复键、大小写、非对象返回 400。
func TestCloseBadRequest(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		reach bool
	}{
		{"empty body", ``, false},
		{"not json", `{`, false},
		{"json array", `[]`, false},
		{"null", `null`, false},
		{"json string", `"PA0001"`, false},
		{"json number", `1`, false},
		{"trailing data", `{"req_no":"PA1"}{}`, false},
		{"invalid utf8", "{\"req_no\":\"PA\xff\"}", false},
		{"duplicate req_no", `{"req_no":"PA1","req_no":"PA2"}`, false},
		{"duplicate via escape", `{"req_no":"PA1","req\u005fno":"PA2"}`, false},
		{"duplicate unknown key", `{"req_no":"PA1","x":1,"x":2}`, false},
		{"field case", `{"REQ_NO":"PA1"}`, false},
		{"req_no number", `{"req_no":1}`, false},
		{"empty req_no", `{"req_no":""}`, true},
		{"missing req_no", `{}`, true},
		{"null req_no", `{"req_no":null}`, true},
		{"req_no 33", `{"req_no":"` + strings.Repeat("a", 33) + `"}`, true},
		{"req_no U+FFFD", "{\"req_no\":\"PA\uFFFD\"}", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePayer{validate: true, cres: biz.CloseResult{Result: biz.ResultOK, Status: "CLOSED"}}
			srv := newTestServer(t, p, testKey)
			assertBare(t, postSigned(t, srv, ClosePath, testKey, nowTS(), []byte(c.body)), http.StatusBadRequest)
			if got := len(p.closeCalls()) > 0; got != c.reach {
				t.Fatalf("close reached = %v, want %v", got, c.reach)
			}
		})
	}
}

// CH-03：空密钥 500，写库错误 500；都是空 body、不签名。
func TestCloseInternalErrors(t *testing.T) {
	body := []byte(`{"req_no":"PA0001"}`)
	t.Run("store error", func(t *testing.T) {
		p := &fakePayer{validate: true, cerr: errors.New("db down")}
		srv := newTestServer(t, p, testKey)
		assertBare(t, postSigned(t, srv, ClosePath, testKey, nowTS(), body), http.StatusInternalServerError)
		if len(p.closeCalls()) != 1 {
			t.Fatal("close was not called")
		}
	})
	t.Run("empty key", func(t *testing.T) {
		p := &fakePayer{validate: true, cres: biz.CloseResult{Result: biz.ResultOK, Status: "CLOSED"}}
		srv := newTestServer(t, p, "")
		assertBare(t, postSigned(t, srv, ClosePath, testKey, nowTS(), body), http.StatusInternalServerError)
		if len(p.closeCalls()) != 0 {
			t.Fatal("close called with empty key")
		}
	})
	t.Run("empty key missing timestamp is 401", func(t *testing.T) {
		p := &fakePayer{validate: true}
		srv := newTestServer(t, p, "")
		assertBare(t, postTo(srv, ClosePath, body), http.StatusUnauthorized)
	})
}

func TestCloseLogsWithoutSecrets(t *testing.T) {
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	var buf bytes.Buffer
	logx.SetDefault("channel-mock", &buf)

	body := []byte(`{"req_no":"PA_LOG_9f3c"}`)
	srv := newTestServer(t, &fakePayer{validate: true, cres: biz.CloseResult{Result: biz.ResultOK, Status: "CLOSED"}}, testKey)
	w := postSigned(t, srv, ClosePath, testKey, nowTS(), body)
	respSig := string(w.Result().Header.Peek(sign.HeaderMockSignature))
	reqSig := signBody(t, testKey, nowTS(), body)
	postTo(srv, ClosePath, body, ut.Header{Key: sign.HeaderMockTimestamp, Value: nowTS()},
		ut.Header{Key: sign.HeaderMockSignature, Value: "bad"})
	srvErr := newTestServer(t, &fakePayer{validate: true, cerr: errors.New("db down")}, testKey)
	postSigned(t, srvErr, ClosePath, testKey, nowTS(), body)
	empty := newTestServer(t, &fakePayer{validate: true}, "")
	postSigned(t, empty, ClosePath, testKey, nowTS(), body)

	out := buf.String()
	for _, secret := range []string{testKey, respSig, reqSig, string(body)} {
		if secret != "" && strings.Contains(out, secret) {
			t.Fatalf("log contains secret material %q:\n%s", secret, out)
		}
	}
	if !strings.Contains(out, "channel key is empty") {
		t.Fatalf("missing empty-key log:\n%s", out)
	}
}
