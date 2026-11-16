//go:build integration

package biz_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

// REC-02 验收 1：篡改账单内容导致哈希不一致，批次 FAILED。
func TestDownloaderHashMismatchMarksFailed(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	clk.Set(shanghaiTime(2026, 10, 27, 10, 0, 0))
	ids := newTestIDGen(t, clk)

	origBody := "txn_type,req_no,channel_txn_no,amount,status,finished_at\n" +
		"PAY,PA0000000123456789012,MC0000000123456789001,10000,SUCCESS,2026-10-26T10:00:01.234+08:00\n"
	tamperedBody := "txn_type,req_no,channel_txn_no,amount,status,finished_at\n" +
		"PAY,PA0000000123456789012,MC0000000123456789001,99999,SUCCESS,2026-10-26T10:00:01.234+08:00\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		// 响应头使用原哈希，但响应体被篡改
		w.Header().Set("X-Bill-Sha256", sha256Hex([]byte(origBody)))
		w.Header().Set("X-Bill-Count", "1")
		_, _ = w.Write([]byte(tamperedBody))
	}))
	defer srv.Close()

	dl, err := biz.NewBillDownloader(db, biz.BillDownloaderConfig{
		BaseURL: srv.URL,
		Key:     []byte("test-mock-key"),
		Clock:   clk,
	})
	if err != nil {
		t.Fatalf("NewBillDownloader: %v", err)
	}

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, nil)
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{
		Channel:  "mockpay",
		BillDate: "2026-10-26",
	})
	if err != nil {
		t.Fatalf("RunBatch err: %v", err)
	}
	if out.Batch.Status != dal.StatusFailed {
		t.Fatalf("status = %s, want FAILED", out.Batch.Status)
	}
	if out.Batch.ErrorMsg == nil || *out.Batch.ErrorMsg == "" {
		t.Fatal("expected non-empty error_msg on FAILED batch")
	}

	var cnt int
	if err := db.Get(&cnt, `SELECT COUNT(*) FROM recon_channel_bill WHERE batch_no = ?`, out.Batch.BatchNo); err != nil {
		t.Fatalf("count recon_channel_bill: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("expected 0 imported rows on hash mismatch, got %d", cnt)
	}
}

// REC-02 验收 2：1 万行账单导入耗时 < 5 秒。
func TestDownloaderImport10kRowsUnder5s(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	clk.Set(shanghaiTime(2026, 10, 27, 10, 0, 0))
	ids := newTestIDGen(t, clk)

	const totalRows = 10000
	var buf bytes.Buffer
	buf.Grow(totalRows * 96)
	buf.WriteString("txn_type,req_no,channel_txn_no,amount,status,finished_at\n")
	for i := 1; i <= totalRows; i++ {
		txnType := "PAY"
		reqPrefix := "PA"
		if i%5 == 0 {
			txnType = "REFUND"
			reqPrefix = "RF"
		}
		fmt.Fprintf(&buf, "%s,%s%019d,MC%019d,%d,SUCCESS,2026-10-26T10:00:01.234+08:00\n",
			txnType, reqPrefix, i, i, 100+i)
	}
	body := buf.Bytes()
	wantHash := sha256Hex(body)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("X-Bill-Sha256", wantHash)
		w.Header().Set("X-Bill-Count", strconv.Itoa(totalRows))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dl, err := biz.NewBillDownloader(db, biz.BillDownloaderConfig{
		BaseURL: srv.URL,
		Key:     []byte("test-mock-key"),
		Clock:   clk,
	})
	if err != nil {
		t.Fatalf("NewBillDownloader: %v", err)
	}

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, nil)
	realClk := clock.Real{}
	start := realClk.Now()
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{
		Channel:  "mockpay",
		BillDate: "2026-10-26",
	})
	elapsed := realClk.Now().Sub(start)
	if err != nil {
		t.Fatalf("RunBatch err: %v", err)
	}
	if out.Batch.Status != dal.StatusDone {
		t.Fatalf("status = %s, want DONE (error=%v)", out.Batch.Status, out.Batch.ErrorMsg)
	}
	if out.Batch.BillFileHash == nil || *out.Batch.BillFileHash != wantHash {
		t.Fatalf("bill_file_hash = %v, want %s", out.Batch.BillFileHash, wantHash)
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("10k rows import took %v, want < 5s", elapsed)
	}
	t.Logf("10k rows download+import elapsed: %v", elapsed)

	var cnt int
	if err := db.Get(&cnt, `SELECT COUNT(*) FROM recon_channel_bill WHERE batch_no = ?`, out.Batch.BatchNo); err != nil {
		t.Fatalf("count recon_channel_bill: %v", err)
	}
	if cnt != totalRows {
		t.Fatalf("imported rows = %d, want %d", cnt, totalRows)
	}
}
