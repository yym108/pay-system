package handler

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

func getSigned(t *testing.T, srv *server.Hertz, path, key, ts string) *ut.ResponseRecorder {
	t.Helper()
	return ut.PerformRequest(srv.Engine, http.MethodGet, path, nil,
		ut.Header{Key: sign.HeaderMockTimestamp, Value: ts},
		ut.Header{Key: sign.HeaderMockSignature, Value: signBody(t, key, ts, nil)})
}

// CH-08 验收 1：账单下载 HTTP 200（text/csv）、X-Bill-Count、X-Bill-Sha256、渠道签名；
// 未就绪返回 HTTP 200 BILL_NOT_READY（带渠道签名）。
func TestBillDownloadHTTP(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(time.Unix(nowUnix, 0))
	csvBody := []byte("txn_type,req_no,channel_txn_no,amount,status,finished_at\nPAY,PA1,MC1,10000,SUCCESS,2026-10-12T10:00:01.234+08:00\n")
	hash := fmt.Sprintf("%x", sha256.Sum256(csvBody))

	t.Run("ready csv", func(t *testing.T) {
		fp := &fakePayer{
			bres: biz.BillResult{
				Ready:   true,
				Content: csvBody,
				Count:   1,
				SHA256:  hash,
			},
		}
		srv := server.New(server.WithDisablePrintRoute(true))
		New(fp, []byte(testKey), clk).Register(srv)

		w := getSigned(t, srv, BillPath+"?date=2026-10-12", testKey, nowTS())
		resp := w.Result()
		if resp.StatusCode() != http.StatusOK {
			t.Fatalf("status = %d, body = %s", resp.StatusCode(), resp.Body())
		}
		if ct := string(resp.Header.ContentType()); ct != "text/csv" {
			t.Fatalf("content-type = %q, want text/csv", ct)
		}
		if got := string(resp.Header.Peek("X-Bill-Count")); got != "1" {
			t.Fatalf("X-Bill-Count = %q, want 1", got)
		}
		if got := string(resp.Header.Peek("X-Bill-Sha256")); got != hash {
			t.Fatalf("X-Bill-Sha256 = %q, want %s", got, hash)
		}
		if string(resp.Body()) != string(csvBody) {
			t.Fatalf("body = %q, want %q", resp.Body(), csvBody)
		}
		gotTS := string(resp.Header.Peek(sign.HeaderMockTimestamp))
		sig := string(resp.Header.Peek(sign.HeaderMockSignature))
		if err := sign.VerifyChannel([]byte(testKey), sign.ChannelMessage{Timestamp: gotTS, Body: resp.Body()}, sig); err != nil {
			t.Fatalf("response signature: %v", err)
		}
	})

	t.Run("not ready", func(t *testing.T) {
		fp := &fakePayer{
			bres: biz.BillResult{Ready: false},
		}
		srv := server.New(server.WithDisablePrintRoute(true))
		New(fp, []byte(testKey), clk).Register(srv)

		w := getSigned(t, srv, BillPath+"?date=2026-10-12", testKey, nowTS())
		assertSignedJSON(t, w, `{"result":"BILL_NOT_READY"}`)
	})

	t.Run("auth and validation errors", func(t *testing.T) {
		fp := &fakePayer{validate: true}
		srv := server.New(server.WithDisablePrintRoute(true))
		New(fp, []byte(testKey), clk).Register(srv)

		// 缺少签名头 -> 401 空 body 不签名
		w401 := ut.PerformRequest(srv.Engine, http.MethodGet, BillPath+"?date=2026-10-12", nil)
		if w401.Result().StatusCode() != http.StatusUnauthorized || len(w401.Result().Body()) != 0 {
			t.Fatalf("missing sign: status=%d body=%q", w401.Result().StatusCode(), w401.Result().Body())
		}

		// 缺少 date -> 400 空 body 不签名
		w400 := getSigned(t, srv, BillPath, testKey, nowTS())
		if w400.Result().StatusCode() != http.StatusBadRequest || len(w400.Result().Body()) != 0 {
			t.Fatalf("missing date: status=%d body=%q", w400.Result().StatusCode(), w400.Result().Body())
		}

		// 非法 date -> 400
		wBadDate := getSigned(t, srv, BillPath+"?date=2026-02-30", testKey, nowTS())
		if wBadDate.Result().StatusCode() != http.StatusBadRequest {
			t.Fatalf("invalid date: status=%d", wBadDate.Result().StatusCode())
		}

		// 重复 date 参数 -> 400
		wDupDate := getSigned(t, srv, BillPath+"?date=2026-10-12&date=2026-10-13", testKey, nowTS())
		if wDupDate.Result().StatusCode() != http.StatusBadRequest {
			t.Fatalf("duplicate date: status=%d", wDupDate.Result().StatusCode())
		}
	})

	t.Run("internal error returns 500 bare", func(t *testing.T) {
		fp := &fakePayer{berr: errors.New("db down")}
		srv := server.New(server.WithDisablePrintRoute(true))
		New(fp, []byte(testKey), clk).Register(srv)

		w := getSigned(t, srv, BillPath+"?date=2026-10-12", testKey, nowTS())
		if w.Result().StatusCode() != http.StatusInternalServerError || len(w.Result().Body()) != 0 {
			t.Fatalf("status=%d body=%q", w.Result().StatusCode(), w.Result().Body())
		}
	})
}
