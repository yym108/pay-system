//go:build integration

package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := testkit.Shutdown(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %s\n", testkit.Redact(err))
	}
	os.Exit(code)
}

// 全链路（真实库、真实定时器）：受理、重复受理、非法参数 400、验签失败 401，2 秒内自动完成。
func TestPayEndToEndMySQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	schema, err := my.OpenSchema(ctx, "channel_mock")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer closeCancel()
		if err := schema.Close(closeCtx); err != nil {
			t.Errorf("close schema: %s", testkit.Redact(err))
		}
	})
	store := dal.New(schema.DB)
	ids, err := idgen.New(1, clock.Real{})
	if err != nil {
		t.Fatal(err)
	}
	sched := biz.NewTimerScheduler(clock.Real{})
	t.Cleanup(sched.Stop)
	svc := biz.New(store, ids, clock.Real{}, sched, biz.Options{DelayMinMS: 200, DelayMaxMS: 2000})
	srv := server.New(server.WithDisablePrintRoute(true))
	New(svc, []byte(testKey), clock.Real{}).Register(srv)

	rows := func() int {
		var n int
		if err := schema.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM mock_txn`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	ts := func() string { return sign.FormatTimestamp(clock.Real{}.Now()) }
	decode := func(w *ut.ResponseRecorder) map[string]string {
		t.Helper()
		resp := w.Result()
		if resp.StatusCode() != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode())
		}
		got := string(resp.Header.Peek(sign.HeaderMockTimestamp))
		sig := string(resp.Header.Peek(sign.HeaderMockSignature))
		if err := sign.VerifyChannel([]byte(testKey), sign.ChannelMessage{Timestamp: got, Body: resp.Body()}, sig); err != nil {
			t.Fatalf("response signature: %v", err)
		}
		var m map[string]string
		if err := json.Unmarshal(resp.Body(), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}

	body := []byte(`{"req_no":"PA_E2E","amount":10000,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`)
	first := decode(signedPost(t, srv, testKey, ts(), body))
	if first["result"] != "OK" || first["status"] != "PROCESSING" || first["channel_txn_no"] == "" {
		t.Fatalf("first = %v", first)
	}
	second := decode(signedPost(t, srv, testKey, ts(), body))
	if second["channel_txn_no"] != first["channel_txn_no"] || second["result"] != "OK" {
		t.Fatalf("second = %v", second)
	}
	if n := rows(); n != 1 {
		t.Fatalf("rows = %d", n)
	}

	bad := []byte(`{"req_no":"PA_BAD","amount":0,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`)
	assertBare(t, signedPost(t, srv, testKey, ts(), bad), http.StatusBadRequest)
	v4 := []byte(`{"txn_type":"PAY","req_no":"PA0000000000000000001","status":"SUCCESS"}`)
	assertBare(t, signedPost(t, srv, testKey, ts(), v4), http.StatusBadRequest)
	// 孤立代理 \udc00 解码成 U+FFFD，业务拒绝；原始非法 UTF-8 在解码前拒绝。都不写库。
	surrogate := []byte(`{"req_no":"PA\udc00","amount":10000,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`)
	assertBare(t, signedPost(t, srv, testKey, ts(), surrogate), http.StatusBadRequest)
	rawInvalid := []byte("{\"req_no\":\"PA\xff\",\"amount\":10000,\"subject\":\"demo\",\"notify_url\":\"http://gateway:8080/callback/mockpay\"}")
	assertBare(t, signedPost(t, srv, testKey, ts(), rawInvalid), http.StatusBadRequest)
	dup := []byte(`{"req_no":"PA_DUP","amount":10000,"amount":10001,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`)
	assertBare(t, signedPost(t, srv, testKey, ts(), dup), http.StatusBadRequest)
	other := []byte(`{"req_no":"PA_UNAUTH","amount":10000,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`)
	assertBare(t, signedPost(t, srv, "another_key_0123456789abcdef0123", ts(), other), http.StatusUnauthorized)
	if n := rows(); n != 1 {
		t.Fatalf("rows after rejected requests = %d", n)
	}

	timeout := time.After(2 * time.Second)
	for {
		r, _, err := store.Get(ctx, dal.TxnTypePay, "PA_E2E")
		if err != nil {
			t.Fatal(err)
		}
		if r.Status == dal.StatusSuccess {
			if r.FinishedAt.Sub(r.CreatedAt) >= 2*time.Second || r.BillDate == "" {
				t.Fatalf("row = %+v", r)
			}
			break
		}
		select {
		case <-timeout:
			t.Fatalf("still %s after 2s", r.Status)
		case <-time.After(20 * time.Millisecond):
		}
	}
	third := decode(signedPost(t, srv, testKey, ts(), body))
	if third["status"] != "SUCCESS" || third["channel_txn_no"] != first["channel_txn_no"] {
		t.Fatalf("after finish = %v", third)
	}
}
