//go:build integration

package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

// CH-08 验收 1 & 2（端到端 MySQL）：
// - 账单行数与 X-Bill-Count 一致，内容哈希与 X-Bill-Sha256 一致，多次下载字节一致；
// - BILL_MISSING 交易不在账单中，BILL_AMOUNT_DIFF 交易金额 +1；
// - 默认不允许下载当天账单（BILL_NOT_READY），推进到次日后可下载。
func TestBillDownloadEndToEndMySQL(t *testing.T) {
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

	startAt := time.Date(2026, 10, 12, 10, 0, 0, 0, shanghaiTZ)
	clk := &clock.Fake{}
	clk.Set(startAt)
	store := dal.New(schema.DB)
	ids, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	sched := biz.NewTimerScheduler(clk)
	t.Cleanup(sched.Stop)
	svc := biz.New(store, ids, clk, sched, biz.Options{DelayMinMS: 200, DelayMaxMS: 200})
	srv := server.New(server.WithDisablePrintRoute(true))
	New(svc, []byte(testKey), clk).Register(srv)

	ts := func() string { return sign.FormatTimestamp(clk.Now()) }
	post := func(path string, body []byte) {
		t.Helper()
		w := ut.PerformRequest(srv.Engine, http.MethodPost, path,
			&ut.Body{Body: bytes.NewReader(body), Len: len(body)},
			ut.Header{Key: sign.HeaderMockTimestamp, Value: ts()},
			ut.Header{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, ts(), body)},
			ut.Header{Key: "Content-Type", Value: "application/json"})
		if w.Result().StatusCode() != http.StatusOK {
			t.Fatalf("POST %s status=%d body=%s", path, w.Result().StatusCode(), w.Result().Body())
		}
	}

	// 1. NORMAL 支付 (10000)、FAIL 支付 (10001)、BILL_MISSING 支付 (10006)、BILL_AMOUNT_DIFF 支付 (10007)、NEVER_FINISH 支付 (10008)
	post(PayPath, []byte(`{"req_no":"PA_NORM","amount":10000,"subject":"s","notify_url":"http://gateway:8080/cb"}`))
	post(PayPath, []byte(`{"req_no":"PA_FAIL","amount":10001,"subject":"s","notify_url":"http://gateway:8080/cb"}`))
	post(PayPath, []byte(`{"req_no":"PA_MISS","amount":10006,"subject":"s","notify_url":"http://gateway:8080/cb"}`))
	post(PayPath, []byte(`{"req_no":"PA_DIFF","amount":10007,"subject":"s","notify_url":"http://gateway:8080/cb"}`))
	post(PayPath, []byte(`{"req_no":"PA_NEV","amount":10008,"subject":"s","notify_url":"http://gateway:8080/cb"}`))

	// 推进时钟 1 秒让自动完成执行
	clk.Advance(1 * time.Second)
	waitUntil := func(desc string, cond func() (bool, error)) {
		t.Helper()
		waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
		defer waitCancel()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			ok, checkErr := cond()
			if checkErr != nil {
				t.Fatal(checkErr)
			}
			if ok {
				return
			}
			select {
			case <-waitCtx.Done():
				t.Fatalf("timed out waiting for %s: %v", desc, waitCtx.Err())
			case <-ticker.C:
			}
		}
	}

	waitUntil("4 txns auto-finished", func() (bool, error) {
		var done int
		if err := schema.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM mock_txn WHERE status IN ('SUCCESS','FAILED')`).Scan(&done); err != nil {
			return false, err
		}
		return done == 4, nil
	})

	// 2. 对 NORMAL 支付发起一笔退款 (3000) 并完成
	post(RefundPath, []byte(`{"req_no":"RF_NORM","orig_req_no":"PA_NORM","amount":3000,"notify_url":"http://gateway:8080/cb"}`))
	clk.Advance(1 * time.Second)
	waitUntil("refund finished", func() (bool, error) {
		var st string
		if err := schema.DB.QueryRowContext(ctx, `SELECT status FROM mock_txn WHERE txn_type='REFUND' AND req_no='RF_NORM'`).Scan(&st); err != nil {
			return false, err
		}
		return st == dal.StatusSuccess, nil
	})

	// 3. 当天下载应返回 BILL_NOT_READY
	getBill := func(date string) *ut.ResponseRecorder {
		return ut.PerformRequest(srv.Engine, http.MethodGet, BillPath+"?date="+date, nil,
			ut.Header{Key: sign.HeaderMockTimestamp, Value: ts()},
			ut.Header{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, ts(), nil)})
	}
	wNotReady := getBill("2026-10-12")
	if wNotReady.Result().StatusCode() != http.StatusOK || !strings.Contains(string(wNotReady.Result().Body()), "BILL_NOT_READY") {
		t.Fatalf("today bill = status=%d body=%s", wNotReady.Result().StatusCode(), wNotReady.Result().Body())
	}

	// 4. 推进到次日，下载 2026-10-12 账单
	clk.Advance(24 * time.Hour)
	w1 := getBill("2026-10-12")
	resp1 := w1.Result()
	if resp1.StatusCode() != http.StatusOK {
		t.Fatalf("bill status=%d", resp1.StatusCode())
	}
	if ct := string(resp1.Header.ContentType()); ct != "text/csv" {
		t.Fatalf("content-type=%q", ct)
	}
	if count := string(resp1.Header.Peek("X-Bill-Count")); count != "3" {
		t.Fatalf("X-Bill-Count=%q, want 3", count)
	}
	wantHash := fmt.Sprintf("%x", sha256.Sum256(resp1.Body()))
	if gotHash := string(resp1.Header.Peek("X-Bill-Sha256")); gotHash != wantHash {
		t.Fatalf("X-Bill-Sha256=%q, want %s", gotHash, wantHash)
	}
	if err := sign.VerifyChannel([]byte(testKey), sign.ChannelMessage{
		Timestamp: string(resp1.Header.Peek(sign.HeaderMockTimestamp)),
		Body:      resp1.Body(),
	}, string(resp1.Header.Peek(sign.HeaderMockSignature))); err != nil {
		t.Fatalf("verify response sig: %v", err)
	}

	records, err := csv.NewReader(strings.NewReader(string(resp1.Body()))).ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(records) != 4 { // 1 header + 3 rows
		t.Fatalf("csv rows=%d, want 4: %v", len(records), records)
	}
	amounts := map[string]string{}
	for _, r := range records[1:] {
		amounts[r[1]] = r[3]
	}
	if amounts["PA_NORM"] != "10000" || amounts["RF_NORM"] != "3000" || amounts["PA_DIFF"] != "10008" {
		t.Fatalf("unexpected bill amounts: %v", amounts)
	}
	if _, ok := amounts["PA_MISS"]; ok {
		t.Fatal("BILL_MISSING appeared in bill")
	}

	// 5. 再次下载字节一致
	w2 := getBill("2026-10-12")
	if string(w2.Result().Body()) != string(resp1.Body()) {
		t.Fatal("second download bytes differed")
	}
}

// CH-09 验收 1（端到端 MySQL）：
// 通过管理接口 POST /mockpay/admin/bill-inject 注入记录后，该记录出现在对应日期账单中。
func TestBillInjectEndToEndMySQL(t *testing.T) {
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

	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 13, 10, 0, 0, 0, shanghaiTZ))
	store := dal.New(schema.DB)
	ids, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	sched := biz.NewTimerScheduler(clk)
	t.Cleanup(sched.Stop)
	svc := biz.New(store, ids, clk, sched, biz.Options{DelayMinMS: 200, DelayMaxMS: 200})

	// 1. 通过管理处理器注入两笔账单记录（一笔默认 req_no，一笔指定 req_no）
	adminH := NewAdminBillInjectHandler(svc)
	inject := func(payload string) (string, string) {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, AdminBillInjectPath, strings.NewReader(payload))
		adminH.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("inject %s status=%d body=%s", payload, rec.Code, rec.Body.String())
		}
		var out struct {
			ReqNo        string `json:"req_no"`
			ChannelTxnNo string `json:"channel_txn_no"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out.ReqNo, out.ChannelTxnNo
	}

	reqNo1, mc1 := inject(`{"date":"2026-10-12","txn_type":"PAY","amount":10500}`)
	reqNo2, mc2 := inject(`{"date":"2026-10-12","txn_type":"REFUND","amount":2500,"req_no":"INJ_RF_MYSQL_01"}`)

	// 2. 从业务端口下载 2026-10-12 账单，验证注入的两条记录均出现
	srv := server.New(server.WithDisablePrintRoute(true))
	New(svc, []byte(testKey), clk).Register(srv)
	ts := sign.FormatTimestamp(clk.Now())
	w := ut.PerformRequest(srv.Engine, http.MethodGet, BillPath+"?date=2026-10-12", nil,
		ut.Header{Key: sign.HeaderMockTimestamp, Value: ts},
		ut.Header{Key: sign.HeaderMockSignature, Value: signBody(t, testKey, ts, nil)})
	resp := w.Result()
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("bill status=%d body=%s", resp.StatusCode(), resp.Body())
	}
	if count := string(resp.Header.Peek("X-Bill-Count")); count != "2" {
		t.Fatalf("X-Bill-Count=%q, want 2", count)
	}
	records, err := csv.NewReader(strings.NewReader(string(resp.Body()))).ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("csv rows=%d, want 3: %v", len(records), records)
	}
	got := map[string][]string{}
	for _, r := range records[1:] {
		got[r[1]] = r
	}
	if r, ok := got[reqNo1]; !ok || r[0] != "PAY" || r[2] != mc1 || r[3] != "10500" || r[4] != "SUCCESS" {
		t.Fatalf("missing or mismatched PAY row: %v", got)
	}
	if r, ok := got[reqNo2]; !ok || r[0] != "REFUND" || r[2] != mc2 || r[3] != "2500" || r[4] != "SUCCESS" {
		t.Fatalf("missing or mismatched REFUND row: %v", got)
	}
}
