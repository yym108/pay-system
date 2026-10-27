package biz

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/sign"
)

const testChannelKey = "channel-key-0123456789abcdef" //nolint:gosec // 测试密钥，不是生产配置。

func testClock() *clock.Fake {
	clk := &clock.Fake{}
	clk.Set(payAt())
	return clk
}

func signedPayServer(t *testing.T, clk clock.Clock, key string, status int, body []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ts := r.Header.Get(sign.HeaderMockTimestamp)
		if ts != sign.FormatTimestamp(clk.Now()) {
			t.Errorf("timestamp = %s", ts)
		}
		if err := sign.VerifyChannel([]byte(key), sign.ChannelMessage{Timestamp: ts, Body: raw}, r.Header.Get(sign.HeaderMockSignature)); err != nil {
			t.Errorf("request signature: %v", err)
		}
		var got channelPayBody
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("body: %v", err)
		}
		if got.ReqNo != "PA0000000000000000001" || got.Amount != 10000 || got.Subject != "demo" || got.NotifyURL != testChannelNotifyURL {
			t.Errorf("body fields = %+v", got)
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		sig, err := sign.SignChannel([]byte(key), sign.ChannelMessage{Timestamp: ts, Body: body})
		if err != nil {
			t.Errorf("sign response: %v", err)
			return
		}
		w.Header().Set(sign.HeaderMockTimestamp, ts)
		w.Header().Set(sign.HeaderMockSignature, sig)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
}

func newTestClient(t *testing.T, rawURL string, clk clock.Clock, key string) *ChannelClient {
	t.Helper()
	client, err := NewChannelClient(ChannelClientConfig{BaseURL: rawURL, Key: []byte(key), Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func acceptDemo(t *testing.T, client *ChannelClient) (ChannelAcceptResult, error) {
	t.Helper()
	return client.Accept(context.Background(), ChannelAcceptRequest{
		ReqNo: "PA0000000000000000001", Amount: 10000, Subject: "demo", NotifyURL: testChannelNotifyURL,
	})
}

func TestChannelClientOKAndRejected(t *testing.T) {
	clk := testClock()
	okBody := []byte(`{"result":"OK","channel_txn_no":"CH-100","status":"PROCESSING"}`)
	srv := signedPayServer(t, clk, testChannelKey, http.StatusOK, okBody)
	defer srv.Close()
	client := newTestClient(t, srv.URL, clk, testChannelKey)
	if client.timeout != PayChannelTimeout || client.dialTimeout != PayChannelDialTimeout {
		t.Fatalf("timeout=%s dial=%s", client.timeout, client.dialTimeout)
	}
	got, err := acceptDemo(t, client)
	if err != nil || got.Outcome != acceptOK || got.ChannelTxnNo != "CH-100" {
		t.Fatalf("ok = %+v err=%v", got, err)
	}

	rej := []byte(`{"result":"REJECTED","reason":"ORDER_CLOSED","status":"CLOSED"}`)
	srv2 := signedPayServer(t, clk, testChannelKey, http.StatusOK, rej)
	defer srv2.Close()
	got, err = acceptDemo(t, newTestClient(t, srv2.URL, clk, testChannelKey))
	if err != nil || got.Outcome != acceptRejected || got.Reason != "ORDER_CLOSED" || got.ChannelTxnNo != "" {
		t.Fatalf("rejected = %+v err=%v", got, err)
	}
}

func TestChannelClientUnknownResponses(t *testing.T) {
	clk := testClock()
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusInternalServerError} {
		srv := signedPayServer(t, clk, testChannelKey, status, nil)
		got, err := acceptDemo(t, newTestClient(t, srv.URL, clk, testChannelKey))
		srv.Close()
		if err != nil || got.Outcome != acceptUnknown {
			t.Fatalf("status %d = %+v err=%v", status, got, err)
		}
	}
	okEmpty := []byte(`{"result":"OK","status":"PROCESSING"}`)
	srv := signedPayServer(t, clk, testChannelKey, http.StatusOK, okEmpty)
	defer srv.Close()
	got, err := acceptDemo(t, newTestClient(t, srv.URL, clk, testChannelKey))
	if err != nil || got.Outcome != acceptUnknown || got.ChannelTxnNo != "" {
		t.Fatalf("ok without txn no = %+v err=%v", got, err)
	}

	badSig := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := []byte(`{"result":"REJECTED","reason":"ORDER_CLOSED","status":"CLOSED"}`)
		w.Header().Set(sign.HeaderMockTimestamp, sign.FormatTimestamp(clk.Now()))
		w.Header().Set(sign.HeaderMockSignature, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer badSig.Close()
	got, err = acceptDemo(t, newTestClient(t, badSig.URL, clk, testChannelKey))
	if err != nil || got.Outcome != acceptUnknown {
		t.Fatalf("bad signature = %+v err=%v", got, err)
	}
}

func TestChannelClientDoesNotFollowRedirect(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/mockpay/v1/pay" {
			http.Redirect(w, r, "/ok", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"OK","channel_txn_no":"CH-REDIR","status":"PROCESSING"}`))
	}))
	defer srv.Close()
	got, err := acceptDemo(t, newTestClient(t, srv.URL, testClock(), testChannelKey))
	if err != nil || got.Outcome != acceptUnknown || hits.Load() != 1 {
		t.Fatalf("redirect got=%+v err=%v hits=%d", got, err, hits.Load())
	}
}

func TestChannelClientTimeoutAndEmptyKey(t *testing.T) {
	clk := testClock()
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 客户端超时后连接可能仍由 httptest 持有。限时返回，避免 Close 一直等这个处理函数。
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
		case <-timer.C:
		}
	}))
	defer func() {
		hung.CloseClientConnections()
		hung.Close()
	}()
	client := newTestClient(t, hung.URL, clk, testChannelKey)
	client.timeout = 40 * time.Millisecond
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	done := make(chan ChannelAcceptResult, 1)
	go func() {
		got, err := acceptDemo(t, client)
		if err != nil {
			t.Errorf("hang err = %v", err)
		}
		done <- got
	}()
	select {
	case <-timer.C:
		t.Fatal("short timeout was not applied")
	case got := <-done:
		if got.Outcome != acceptUnknown {
			t.Fatalf("hang = %+v", got)
		}
	}

	empty := newTestClient(t, hung.URL, clk, "")
	if _, err := acceptDemo(t, empty); !errorsIsEmptyKey(err) {
		t.Fatalf("empty key err = %v", err)
	}
}

func errorsIsEmptyKey(err error) bool {
	return errors.Is(err, sign.ErrEmptyKey)
}

func TestChannelClientRejectsBadURL(t *testing.T) {
	badURL := "ftp://user:leakmarkerZZ9@example.com/pay" //nolint:gosec // G101: 假地址，用来断言错误文本不含用户信息和主机。
	_, err := NewChannelClient(ChannelClientConfig{BaseURL: badURL, Key: []byte(testChannelKey), Clock: testClock()})
	if err == nil || strings.Contains(err.Error(), "leakmarkerZZ9") || strings.Contains(err.Error(), "example.com") {
		t.Fatalf("bad url err = %v", err)
	}
}
