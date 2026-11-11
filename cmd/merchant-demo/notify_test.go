package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/sign"
)

func testDemo(t *testing.T, failN int, format string) (*demo, *server.Hertz) {
	t.Helper()
	if format == "" {
		cfg, err := config.LoadMerchantDemo([]string{"WORKER_ID=6"})
		if err != nil {
			t.Fatal(err)
		}
		format = cfg.AppSecretFormat
	}
	d := newDemo(config.MerchantDemo{FailFirstN: failN, AppSecretFormat: format})
	srv := server.New(server.WithDisablePrintRoute(true))
	d.register(srv)
	return d, srv
}

func payBody(merchantID, payNo string, n int) []byte {
	return []byte(fmt.Sprintf(
		`{"notify_id":"NT%02d","event_type":"payment.succeeded","merchant_id":%q,"data":{"pay_no":%q,"out_trade_no":"T1","amount":10000,"fee_amount":60,"status":"SUCCESS","paid_at":"2026-10-12T10:00:01.234+08:00"}}`,
		n, merchantID, payNo,
	))
}

func postNotify(t *testing.T, srv *server.Hertz, secret, nonce string, body []byte) *ut.ResponseRecorder {
	t.Helper()
	sig, err := sign.SignPlatform([]byte(secret), sign.PlatformMessage{
		Timestamp: "1791500001",
		Nonce:     nonce,
		Body:      body,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ut.PerformRequest(srv.Engine, http.MethodPost, pathNotify,
		&ut.Body{Body: bytes.NewReader(body), Len: len(body)},
		ut.Header{Key: "Content-Type", Value: "application/json"},
		ut.Header{Key: sign.HeaderTimestamp, Value: "1791500001"},
		ut.Header{Key: sign.HeaderNonce, Value: nonce},
		ut.Header{Key: sign.HeaderSignature, Value: sig},
	)
}

func postRaw(srv *server.Hertz, body []byte, headers ...ut.Header) *ut.ResponseRecorder {
	hs := append([]ut.Header{{Key: "Content-Type", Value: "application/json"}}, headers...)
	return ut.PerformRequest(srv.Engine, http.MethodPost, pathNotify,
		&ut.Body{Body: bytes.NewReader(body), Len: len(body)}, hs...)
}

func assertStatus(t *testing.T, w *ut.ResponseRecorder, status int, body string) {
	t.Helper()
	resp := w.Result()
	if resp.StatusCode() != status || string(resp.Body()) != body {
		t.Fatalf("status=%d body=%q, want %d %q", resp.StatusCode(), resp.Body(), status, body)
	}
}

func getStats(t *testing.T, srv *server.Hertz, bizNo string) (int, string, int) {
	t.Helper()
	w := ut.PerformRequest(srv.Engine, http.MethodGet, pathStats+"?biz_no="+bizNo, nil)
	resp := w.Result()
	if resp.StatusCode() != http.StatusOK {
		return 0, "", resp.StatusCode()
	}
	dec := json.NewDecoder(bytes.NewReader(resp.Body()))
	dec.UseNumber()
	var raw struct {
		Count json.Number `json:"count"`
		Last  string      `json:"last_body"`
	}
	if err := dec.Decode(&raw); err != nil {
		t.Fatalf("stats json: %v body=%q", err, resp.Body())
	}
	n, err := raw.Count.Int64()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return int(n), raw.Last, resp.StatusCode()
}

// TL-02 验收 1：FAIL_FIRST_N=3，同一单号前 3 次正确签名返回 500，第 4 次返回 200、body SUCCESS。
func TestTL02FailFirstN(t *testing.T) {
	_, srv := testDemo(t, 3, "")
	secret := fmt.Sprintf(loadedFormat(t), "M100002")
	const payNo = "PA0000000123456789012"
	for i := 1; i <= 3; i++ {
		body := payBody("M100002", payNo, i)
		w := postNotify(t, srv, secret, fmt.Sprintf("nonce%02d", i), body)
		assertStatus(t, w, http.StatusInternalServerError, bodyFail)
	}
	body := payBody("M100002", payNo, 4)
	w := postNotify(t, srv, secret, "nonce04", body)
	assertStatus(t, w, http.StatusOK, bodySuccess)
}

// TL-02 验收 2：签名错误的通知（另一单号）返回 401，且不计入该单号。
func TestTL02BadSignatureOtherBizNo(t *testing.T) {
	_, srv := testDemo(t, 3, "")
	format := loadedFormat(t)
	secretA := fmt.Sprintf(format, "M100002")
	bodyA := payBody("M100002", "PA0000000123456789012", 1)
	w := postNotify(t, srv, secretA, "nonceA", bodyA)
	assertStatus(t, w, http.StatusInternalServerError, bodyFail)

	bodyB := payBody("M100002", "PA0000000123456789099", 1)
	sigA, err := sign.SignPlatform([]byte(secretA), sign.PlatformMessage{
		Timestamp: "1791500001", Nonce: "nonceB", Body: bodyA,
	})
	if err != nil {
		t.Fatal(err)
	}
	w = postRaw(srv, bodyB,
		ut.Header{Key: sign.HeaderTimestamp, Value: "1791500001"},
		ut.Header{Key: sign.HeaderNonce, Value: "nonceB"},
		ut.Header{Key: sign.HeaderSignature, Value: sigA},
	)
	assertStatus(t, w, http.StatusUnauthorized, "")

	n, last, status := getStats(t, srv, "PA0000000123456789012")
	if status != http.StatusOK || n != 1 || last != string(bodyA) {
		t.Fatalf("first biz stats = %d %q status %d", n, last, status)
	}
	n, last, status = getStats(t, srv, "PA0000000123456789099")
	if status != http.StatusOK || n != 0 || last != "" {
		t.Fatalf("other biz stats = %d %q status %d", n, last, status)
	}
}

// TL-02 验收 3：4 次通知之后，stats 返回次数 4 和第 4 次通知的 body。
func TestTL02StatsAfterFour(t *testing.T) {
	_, srv := testDemo(t, 3, "")
	secret := fmt.Sprintf(loadedFormat(t), "M100002")
	const payNo = "PA0000000123456789012"
	var fourth []byte
	for i := 1; i <= 4; i++ {
		body := payBody("M100002", payNo, i)
		if i == 4 {
			fourth = body
		}
		w := postNotify(t, srv, secret, fmt.Sprintf("nonce%02d", i), body)
		if i < 4 {
			assertStatus(t, w, http.StatusInternalServerError, bodyFail)
		} else {
			assertStatus(t, w, http.StatusOK, bodySuccess)
		}
	}
	n, last, status := getStats(t, srv, payNo)
	if status != http.StatusOK || n != 4 || last != string(fourth) {
		t.Fatalf("stats = %d %q status %d, want 4 and the 4th body", n, last, status)
	}
}

// TL-02：接口 §3.7 V3。默认格式代入 M100001 后与向量密钥相同，验签通过。
func TestTL02PlatformVectorV3(t *testing.T) {
	secret := fmt.Sprintf(loadedFormat(t), "M100001")
	const vectorSecret = "demo_secret_M100001_0123456789ab" //nolint:gosec // G101: 接口 §3.7 公开向量，不是生产密钥。
	if secret != vectorSecret {
		t.Fatalf("secret = %q", secret)
	}
	body := []byte(`{"code":0,"msg":"ok","data":{"pay_no":"PA0000000000000000001"}}`)
	err := sign.VerifyPlatform([]byte(secret), sign.PlatformMessage{
		Timestamp: "1791500001",
		Nonce:     "ZyXwVu9876543210",
		Body:      body,
	}, "TuLVqqAxs3SyvTtDXp1cpv+Rpv2D5URAFywIHBNjAh4=")
	if err != nil {
		t.Fatal(err)
	}
}

func TestTL02EmptySecretIs500AndNotCounted(t *testing.T) {
	var logs bytes.Buffer
	d, srv := testDemo(t, 0, "%s")
	d.log = slogDiscard(&logs)
	body := []byte(`{"notify_id":"NT1","event_type":"payment.succeeded","merchant_id":"","data":{"pay_no":"PAEMPTY"}}`)
	w := postRaw(srv, body,
		ut.Header{Key: sign.HeaderTimestamp, Value: "1791500001"},
		ut.Header{Key: sign.HeaderNonce, Value: "nonceE"},
		ut.Header{Key: sign.HeaderSignature, Value: "not-used"},
	)
	assertStatus(t, w, http.StatusInternalServerError, "")
	if strings.Contains(logs.String(), "not-used") {
		t.Fatalf("log contains signature: %s", logs.String())
	}
	n, last, status := getStats(t, srv, "PAEMPTY")
	if status != http.StatusOK || n != 0 || last != "" {
		t.Fatalf("stats = %d %q status %d", n, last, status)
	}
}

func TestTL02RefundAndTransferBizNo(t *testing.T) {
	_, srv := testDemo(t, 0, "")
	secret := fmt.Sprintf(loadedFormat(t), "M100001")
	refund := []byte(`{"notify_id":"NT9","event_type":"refund.failed","merchant_id":"M100001","data":{"refund_no":"RF0000000123456789012","pay_no":"PAIGNORE","amount":1,"status":"FAILED","fail_code":"CHANNEL_FAILED"}}`)
	w := postNotify(t, srv, secret, "nonceR", refund)
	assertStatus(t, w, http.StatusOK, bodySuccess)
	n, last, status := getStats(t, srv, "RF0000000123456789012")
	if status != http.StatusOK || n != 1 || last != string(refund) {
		t.Fatalf("refund stats = %d %q status %d", n, last, status)
	}
	n, _, status = getStats(t, srv, "PAIGNORE")
	if status != http.StatusOK || n != 0 {
		t.Fatalf("pay_no inside refund was counted: %d status %d", n, status)
	}

	transfer := []byte(`{"notify_id":"NT8","event_type":"transfer.succeeded","merchant_id":"M100001","data":{"transfer_no":"TF0000000123456789020","amount":500,"status":"SUCCESS"}}`)
	w = postNotify(t, srv, secret, "nonceT", transfer)
	assertStatus(t, w, http.StatusOK, bodySuccess)
	n, last, status = getStats(t, srv, "TF0000000123456789020")
	if status != http.StatusOK || n != 1 || last != string(transfer) {
		t.Fatalf("transfer stats = %d %q status %d", n, last, status)
	}
}

func TestTL02SignedWithoutBizNoIs400(t *testing.T) {
	_, srv := testDemo(t, 0, "")
	secret := fmt.Sprintf(loadedFormat(t), "M100002")
	body := []byte(`{"notify_id":"NT1","event_type":"payment.succeeded","merchant_id":"M100002","data":{}}`)
	w := postNotify(t, srv, secret, "nonceZ", body)
	assertStatus(t, w, http.StatusBadRequest, "")
	n, _, status := getStats(t, srv, "PA0000000123456789012")
	if status != http.StatusOK || n != 0 {
		t.Fatalf("counted without biz_no: %d status %d", n, status)
	}
}

func TestTL02BadJSONAndMissingMerchantAre401(t *testing.T) {
	_, srv := testDemo(t, 0, "")
	w := postRaw(srv, []byte(`not-json`))
	assertStatus(t, w, http.StatusUnauthorized, "")
	w = postRaw(srv, []byte(`{"event_type":"payment.succeeded","data":{"pay_no":"PA1"}}`))
	assertStatus(t, w, http.StatusUnauthorized, "")
	w = postRaw(srv, []byte(`{"merchant_id":1,"event_type":"payment.succeeded","data":{"pay_no":"PA1"}}`))
	assertStatus(t, w, http.StatusUnauthorized, "")
}

func TestTL02BodyTooLarge(t *testing.T) {
	_, srv := testDemo(t, 0, "")
	w := postRaw(srv, bytes.Repeat([]byte("a"), maxNotifyBody+1))
	assertStatus(t, w, http.StatusRequestEntityTooLarge, "")
}

func TestTL02StatsMissingBizNo(t *testing.T) {
	_, srv := testDemo(t, 0, "")
	w := ut.PerformRequest(srv.Engine, http.MethodGet, pathStats, nil)
	assertStatus(t, w, http.StatusBadRequest, "")
}

func TestTL02TamperedKeyFails(t *testing.T) {
	_, srv := testDemo(t, 0, "")
	body := payBody("M100002", "PA0000000123456789012", 1)
	w := postNotify(t, srv, "demo_secret_OTHER_0123456789ab", "nonceK", body)
	assertStatus(t, w, http.StatusUnauthorized, "")
}

// TL-02：监听已绑定的端口，取消后返回。不占用 9100。
func TestTL02ServeOnCancel(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d, _ := testDemo(t, 0, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveOn(ctx, ln, d, time.Millisecond)
	}()
	secret := fmt.Sprintf(loadedFormat(t), "M100002")
	body := payBody("M100002", "PASMOKE", 1)
	sig, err := sign.SignPlatform([]byte(secret), sign.PlatformMessage{
		Timestamp: "1791500001", Nonce: "nonceS", Body: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + ln.Addr().String() + pathNotify
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	var resp *http.Response
	for {
		if waitCtx.Err() != nil {
			t.Fatal(err)
		}
		reqCtx, reqCancel := context.WithTimeout(waitCtx, 200*time.Millisecond)
		req, rerr := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
		if rerr != nil {
			reqCancel()
			t.Fatal(rerr)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(sign.HeaderTimestamp, "1791500001")
		req.Header.Set(sign.HeaderNonce, "nonceS")
		req.Header.Set(sign.HeaderSignature, sig)
		resp, err = http.DefaultClient.Do(req)
		reqCancel()
		if err == nil {
			break
		}
		select {
		case <-waitCtx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	got, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(got) != bodySuccess {
		t.Fatalf("live status=%d body=%q", resp.StatusCode, got)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveOn did not return")
	}
}

func loadedFormat(t *testing.T) string {
	t.Helper()
	cfg, err := config.LoadMerchantDemo([]string{"WORKER_ID=6"})
	if err != nil {
		t.Fatal(err)
	}
	return cfg.AppSecretFormat
}

func slogDiscard(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}
