package handler

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

func postTo(srv *server.Hertz, path string, body []byte, headers ...ut.Header) *ut.ResponseRecorder {
	return ut.PerformRequest(srv.Engine, http.MethodPost, path,
		&ut.Body{Body: bytes.NewReader(body), Len: len(body)}, headers...)
}

func postSigned(t *testing.T, srv *server.Hertz, path, key, ts string, body []byte) *ut.ResponseRecorder {
	t.Helper()
	return postTo(srv, path, body,
		ut.Header{Key: sign.HeaderMockTimestamp, Value: ts},
		ut.Header{Key: sign.HeaderMockSignature, Value: signBody(t, key, ts, body)},
		ut.Header{Key: "Content-Type", Value: "application/json"})
}

func assertSignedJSON(t *testing.T, w *ut.ResponseRecorder, want string) {
	t.Helper()
	resp := w.Result()
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.StatusCode(), resp.Body())
	}
	if string(resp.Body()) != want {
		t.Fatalf("body = %s\nwant %s", resp.Body(), want)
	}
	if ct := string(resp.Header.ContentType()); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	gotTS := string(resp.Header.Peek(sign.HeaderMockTimestamp))
	sig := string(resp.Header.Peek(sign.HeaderMockSignature))
	if gotTS != nowTS() {
		t.Fatalf("response timestamp = %q", gotTS)
	}
	if err := sign.VerifyChannel([]byte(testKey), sign.ChannelMessage{Timestamp: gotTS, Body: resp.Body()}, sig); err != nil {
		t.Fatalf("response signature: %v", err)
	}
}

// queryAccepted 与 biz 的查单参数规则一致，用来让假实现把非法请求映射成 ErrInvalidRequest。
func queryAccepted(req biz.QueryRequest) bool {
	if req.TxnType != "PAY" && req.TxnType != "REFUND" {
		return false
	}
	if req.ReqNo == "" || !utf8.ValidString(req.ReqNo) || strings.ContainsRune(req.ReqNo, utf8.RuneError) {
		return false
	}
	return utf8.RuneCountInString(req.ReqNo) <= 32
}

// CH-02：200 的响应带渠道签名；NOT_FOUND 只有 result；金额 0 仍出现；finished_at 只在非空时出现。
func TestQuerySignedResponses(t *testing.T) {
	finished := time.Date(2026, 10, 12, 10, 0, 0, 123456789, shanghaiTZ)
	utc := time.Date(2026, 10, 12, 2, 0, 0, 0, time.UTC)
	failedAt := time.Date(2026, 10, 12, 10, 0, 1, 0, shanghaiTZ)
	cases := []struct {
		name string
		body string
		res  biz.QueryResult
		want string
	}{
		{
			name: "processing omits finished_at",
			body: `{"txn_type":"PAY","req_no":"PA0001"}`,
			res:  biz.QueryResult{Found: true, Status: "PROCESSING", Amount: 10000, ChannelTxnNo: "MC1"},
			want: `{"result":"OK","status":"PROCESSING","amount":10000,"channel_txn_no":"MC1"}`,
		},
		{
			name: "success with millis",
			body: `{"txn_type":"PAY","req_no":"PA_DROP"}`,
			res:  biz.QueryResult{Found: true, Status: "SUCCESS", Amount: 10002, ChannelTxnNo: "MC_DROP", FinishedAt: finished},
			want: `{"result":"OK","status":"SUCCESS","amount":10002,"channel_txn_no":"MC_DROP","finished_at":"2026-10-12T10:00:00.123+08:00"}`,
		},
		{
			name: "utc instant uses +08:00",
			body: `{"txn_type":"PAY","req_no":"PA_UTC"}`,
			res:  biz.QueryResult{Found: true, Status: "SUCCESS", Amount: 1, ChannelTxnNo: "MC_UTC", FinishedAt: utc},
			want: `{"result":"OK","status":"SUCCESS","amount":1,"channel_txn_no":"MC_UTC","finished_at":"2026-10-12T10:00:00.000+08:00"}`,
		},
		{
			name: "failed keeps finished_at",
			body: `{"txn_type":"PAY","req_no":"PA_FAIL"}`,
			res:  biz.QueryResult{Found: true, Status: "FAILED", Amount: 10001, ChannelTxnNo: "MC_FAIL", FinishedAt: failedAt},
			want: `{"result":"OK","status":"FAILED","amount":10001,"channel_txn_no":"MC_FAIL","finished_at":"2026-10-12T10:00:01.000+08:00"}`,
		},
		{
			name: "amount zero",
			body: `{"txn_type":"PAY","req_no":"PA_ZERO"}`,
			res:  biz.QueryResult{Found: true, Status: "CLOSED", Amount: 0, ChannelTxnNo: "MC_ZERO"},
			want: `{"result":"OK","status":"CLOSED","amount":0,"channel_txn_no":"MC_ZERO"}`,
		},
		{
			name: "processing keeps a non-null finished_at",
			body: `{"txn_type":"PAY","req_no":"PA_ODD"}`,
			res:  biz.QueryResult{Found: true, Status: "PROCESSING", Amount: 10, ChannelTxnNo: "MC_ODD", FinishedAt: failedAt},
			want: `{"result":"OK","status":"PROCESSING","amount":10,"channel_txn_no":"MC_ODD","finished_at":"2026-10-12T10:00:01.000+08:00"}`,
		},
		{
			name: "refund",
			body: `{"txn_type":"REFUND","req_no":"RF1"}`,
			res:  biz.QueryResult{Found: true, Status: "PROCESSING", Amount: 3000, ChannelTxnNo: "MC_RF"},
			want: `{"result":"OK","status":"PROCESSING","amount":3000,"channel_txn_no":"MC_RF"}`,
		},
		{
			name: "not found",
			body: `{"txn_type":"PAY","req_no":"PA_NONE"}`,
			res:  biz.QueryResult{},
			want: `{"result":"NOT_FOUND"}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePayer{validate: true, qres: c.res}
			srv := newTestServer(t, p, testKey)
			for _, ts := range []string{nowTS(), fmt.Sprint(nowUnix - 300), fmt.Sprint(nowUnix + 300)} {
				assertSignedJSON(t, postSigned(t, srv, QueryPath, testKey, ts, []byte(c.body)), c.want)
			}
			calls := p.queryCalls()
			if len(calls) != 3 {
				t.Fatalf("calls = %+v", calls)
			}
			if _, ok := p.queryCtx.Deadline(); !ok {
				t.Fatal("query context has no deadline")
			}
		})
	}
}

func TestQueryIgnoresUnknownFields(t *testing.T) {
	p := &fakePayer{validate: true, qres: biz.QueryResult{Found: true, Status: "PROCESSING", Amount: 10000, ChannelTxnNo: "MC1"}}
	srv := newTestServer(t, p, testKey)
	body := `{"extra":{"amount":5},"txn_type":"PAY","req_no":"PA0001","note":"x"}` + " \n"
	assertSignedJSON(t, postSigned(t, srv, QueryPath, testKey, nowTS(), []byte(body)),
		`{"result":"OK","status":"PROCESSING","amount":10000,"channel_txn_no":"MC1"}`)
	calls := p.queryCalls()
	want := biz.QueryRequest{TxnType: "PAY", ReqNo: "PA0001"}
	if len(calls) != 1 || calls[0] != want {
		t.Fatalf("calls = %+v", calls)
	}
}

// CH-02 验收 7：验签失败 401，空 body，不签名，不进入业务。
func TestQueryUnauthorized(t *testing.T) {
	body := []byte(`{"txn_type":"PAY","req_no":"PA0001"}`)
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
		{"tampered body", []byte(`{"txn_type":"PAY","req_no":"PA0002"}`), []ut.Header{
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
			p := &fakePayer{validate: true, qres: biz.QueryResult{Found: true, Status: "SUCCESS"}}
			srv := newTestServer(t, p, testKey)
			assertBare(t, postTo(srv, QueryPath, c.body, c.headers...), http.StatusUnauthorized)
			if len(p.queryCalls()) != 0 {
				t.Fatal("query called after failed verification")
			}
		})
	}
}

// CH-02 验收 7：签名正确但参数非法返回 400，空 body，不签名。
func TestQueryBadRequest(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		reach bool
	}{
		{"empty body", ``, false},
		{"not json", `{`, false},
		{"json array", `[]`, false},
		{"null", `null`, false},
		{"json string", `"PAY"`, false},
		{"json number", `1`, false},
		{"trailing data", `{"txn_type":"PAY","req_no":"PA1"}{}`, false},
		{"trailing garbage", `{"txn_type":"PAY","req_no":"PA1"}x`, false},
		{"invalid utf8", "{\"txn_type\":\"PAY\",\"req_no\":\"PA\xff\"}", false},
		{"duplicate txn_type", `{"txn_type":"PAY","txn_type":"REFUND","req_no":"PA1"}`, false},
		{"duplicate via escape", `{"txn_type":"PAY","req_no":"PA1","req\u005fno":"PA2"}`, false},
		{"duplicate unknown key", `{"txn_type":"PAY","req_no":"PA1","x":1,"x":2}`, false},
		{"field case txn_type", `{"Txn_Type":"PAY","req_no":"PA1"}`, false},
		{"field case req_no", `{"txn_type":"PAY","REQ_NO":"PA1"}`, false},
		{"txn_type number", `{"txn_type":1,"req_no":"PA1"}`, false},
		{"req_no number", `{"txn_type":"PAY","req_no":1}`, false},
		{"empty txn_type", `{"txn_type":"","req_no":"PA1"}`, true},
		{"missing txn_type", `{"req_no":"PA1"}`, true},
		{"null txn_type", `{"txn_type":null,"req_no":"PA1"}`, true},
		{"illegal txn_type", `{"txn_type":"CLOSE","req_no":"PA1"}`, true},
		{"lower txn_type", `{"txn_type":"pay","req_no":"PA1"}`, true},
		{"empty req_no", `{"txn_type":"PAY","req_no":""}`, true},
		{"missing req_no", `{"txn_type":"PAY"}`, true},
		{"null req_no", `{"txn_type":"PAY","req_no":null}`, true},
		{"req_no 33", `{"txn_type":"PAY","req_no":"` + strings.Repeat("a", 33) + `"}`, true},
		{"req_no U+FFFD", "{\"txn_type\":\"PAY\",\"req_no\":\"PA\uFFFD\"}", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePayer{validate: true, qres: biz.QueryResult{Found: true, Status: "SUCCESS", Amount: 1, ChannelTxnNo: "MC"}}
			srv := newTestServer(t, p, testKey)
			assertBare(t, postSigned(t, srv, QueryPath, testKey, nowTS(), []byte(c.body)), http.StatusBadRequest)
			if got := len(p.queryCalls()) > 0; got != c.reach {
				t.Fatalf("query reached = %v, want %v", got, c.reach)
			}
		})
	}
}

// CH-02 验收 7：空密钥 500，读库错误 500；都是空 body、不签名。panic 后服务仍可用。
func TestQueryInternalErrors(t *testing.T) {
	body := []byte(`{"txn_type":"PAY","req_no":"PA0001"}`)
	t.Run("store error", func(t *testing.T) {
		p := &fakePayer{validate: true, qerr: errors.New("db down")}
		srv := newTestServer(t, p, testKey)
		assertBare(t, postSigned(t, srv, QueryPath, testKey, nowTS(), body), http.StatusInternalServerError)
		if len(p.queryCalls()) != 1 {
			t.Fatal("query was not called")
		}
	})
	t.Run("empty key", func(t *testing.T) {
		p := &fakePayer{validate: true, qres: biz.QueryResult{Found: true, Status: "SUCCESS"}}
		srv := newTestServer(t, p, "")
		assertBare(t, postSigned(t, srv, QueryPath, testKey, nowTS(), body), http.StatusInternalServerError)
		if len(p.queryCalls()) != 0 {
			t.Fatal("query called with empty key")
		}
	})
	t.Run("empty key missing timestamp is 401", func(t *testing.T) {
		p := &fakePayer{validate: true}
		srv := newTestServer(t, p, "")
		assertBare(t, postTo(srv, QueryPath, body), http.StatusUnauthorized)
	})
	t.Run("panic", func(t *testing.T) {
		p := &fakePayer{validate: true, panic: true}
		srv := newTestServer(t, p, testKey)
		assertBare(t, postSigned(t, srv, QueryPath, testKey, nowTS(), body), http.StatusInternalServerError)
		p.mu.Lock()
		p.panic = false
		p.qres = biz.QueryResult{Found: true, Status: "PROCESSING", Amount: 10000, ChannelTxnNo: "MC1"}
		p.mu.Unlock()
		assertSignedJSON(t, postSigned(t, srv, QueryPath, testKey, nowTS(), body),
			`{"result":"OK","status":"PROCESSING","amount":10000,"channel_txn_no":"MC1"}`)
	})
}

func TestQueryLogsWithoutSecrets(t *testing.T) {
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	var buf bytes.Buffer
	logx.SetDefault("channel-mock", &buf)

	body := []byte(`{"txn_type":"PAY","req_no":"PA_LOG_9f3c"}`)
	srv := newTestServer(t, &fakePayer{validate: true, qres: biz.QueryResult{Found: true, Status: "PROCESSING", Amount: 1, ChannelTxnNo: "MC1"}}, testKey)
	w := postSigned(t, srv, QueryPath, testKey, nowTS(), body)
	respSig := string(w.Result().Header.Peek(sign.HeaderMockSignature))
	reqSig := signBody(t, testKey, nowTS(), body)
	postTo(srv, QueryPath, body, ut.Header{Key: sign.HeaderMockTimestamp, Value: nowTS()},
		ut.Header{Key: sign.HeaderMockSignature, Value: "bad"})
	srvErr := newTestServer(t, &fakePayer{validate: true, qerr: errors.New("db down")}, testKey)
	postSigned(t, srvErr, QueryPath, testKey, nowTS(), body)

	out := buf.String()
	for _, secret := range []string{testKey, respSig, reqSig, string(body)} {
		if secret != "" && strings.Contains(out, secret) {
			t.Fatalf("log contains secret material %q:\n%s", secret, out)
		}
	}
	if strings.Count(out, QueryPath) < 3 {
		t.Fatalf("missing access logs:\n%s", out)
	}
	for _, status := range []string{"200", "401", "500"} {
		if !strings.Contains(out, status) {
			t.Fatalf("log missing status %s:\n%s", status, out)
		}
	}
}
