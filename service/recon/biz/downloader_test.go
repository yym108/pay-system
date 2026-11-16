package biz_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// REC-02 规则 1：下载超时 30 秒，失败重试 3 次（间隔 10 秒）。
func TestDownloadBillRetryOnFailure(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(shanghaiTime(2026, 10, 27, 10, 0, 0))

	csvBody := "txn_type,req_no,channel_txn_no,amount,status,finished_at\n" +
		"PAY,PA0000000123456789012,MC0000000123456789001,10000,SUCCESS,2026-10-26T10:00:01.234+08:00\n"

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		// 验证请求签名头存在且合法
		ts := r.Header.Get(sign.HeaderMockTimestamp)
		sig := r.Header.Get(sign.HeaderMockSignature)
		if ts == "" || sig == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if n <= 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("X-Bill-Sha256", sha256Hex([]byte(csvBody)))
		w.Header().Set("X-Bill-Count", "1")
		_, _ = w.Write([]byte(csvBody))
	}))
	defer srv.Close()

	var sleeps []time.Duration
	dl, err := biz.NewBillDownloader(nil, biz.BillDownloaderConfig{
		BaseURL: srv.URL,
		Key:     []byte("test-mock-key"),
		Clock:   clk,
		SleepFunc: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewBillDownloader: %v", err)
	}

	rows, hash, err := dl.DownloadAndParse(context.Background(), "2026-10-26")
	if err != nil {
		t.Fatalf("expected success on 4th attempt, got err: %v", err)
	}
	if attempts.Load() != 4 {
		t.Fatalf("attempts = %d, want 4", attempts.Load())
	}
	if len(sleeps) != 3 {
		t.Fatalf("sleeps len = %d, want 3", len(sleeps))
	}
	for i, d := range sleeps {
		if d != 10*time.Second {
			t.Fatalf("sleep[%d] = %v, want 10s", i, d)
		}
	}
	if len(rows) != 1 || rows[0].BizNo != "PA0000000123456789012" || rows[0].Amount != 10000 {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if hash != sha256Hex([]byte(csvBody)) {
		t.Fatalf("hash mismatch: %s", hash)
	}

	// 4 次全部失败时返回错误
	attempts.Store(0)
	sleeps = nil
	srvFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srvFail.Close()

	dlFail, err := biz.NewBillDownloader(nil, biz.BillDownloaderConfig{
		BaseURL: srvFail.URL,
		Key:     []byte("test-mock-key"),
		Clock:   clk,
		SleepFunc: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewBillDownloader: %v", err)
	}
	if _, _, err := dlFail.DownloadAndParse(context.Background(), "2026-10-26"); err == nil {
		t.Fatal("expected error when all retries exhausted")
	}
	if attempts.Load() != 4 || len(sleeps) != 3 {
		t.Fatalf("attempts=%d sleeps=%d, want 4 and 3", attempts.Load(), len(sleeps))
	}
}

// REC-02 规则 4：渠道返回 BILL_NOT_READY 时直接返回 ErrBillNotReady，不走 10s 重试。
func TestDownloadBillNotReady(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(shanghaiTime(2026, 10, 27, 10, 0, 0))

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"BILL_NOT_READY"}`))
	}))
	defer srv.Close()

	var slept bool
	dl, err := biz.NewBillDownloader(nil, biz.BillDownloaderConfig{
		BaseURL: srv.URL,
		Key:     []byte("test-mock-key"),
		Clock:   clk,
		SleepFunc: func(_ context.Context, _ time.Duration) error {
			slept = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewBillDownloader: %v", err)
	}

	_, _, err = dl.DownloadAndParse(context.Background(), "2026-10-27")
	if !errors.Is(err, biz.ErrBillNotReady) {
		t.Fatalf("expected ErrBillNotReady, got %v", err)
	}
	if attempts.Load() != 1 || slept {
		t.Fatalf("attempts=%d slept=%v, want 1 and false", attempts.Load(), slept)
	}
}

// REC-02 规则 2：校验 X-Bill-Sha256 与 X-Bill-Count，不一致返回错误。
func TestDownloadBillVerifyHeaders(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(shanghaiTime(2026, 10, 27, 10, 0, 0))

	csvBody := "txn_type,req_no,channel_txn_no,amount,status,finished_at\n" +
		"PAY,PA0000000123456789012,MC0000000123456789001,10000,SUCCESS,2026-10-26T10:00:01.234+08:00\n"

	cases := []struct {
		name   string
		sha256 string
		count  string
	}{
		{name: "missing sha256", sha256: "", count: "1"},
		{name: "wrong sha256", sha256: strings.Repeat("0", 64), count: "1"},
		{name: "missing count", sha256: sha256Hex([]byte(csvBody)), count: ""},
		{name: "wrong count", sha256: sha256Hex([]byte(csvBody)), count: "2"},
		{name: "non-int count", sha256: sha256Hex([]byte(csvBody)), count: "abc"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/csv")
				if tc.sha256 != "" {
					w.Header().Set("X-Bill-Sha256", tc.sha256)
				}
				if tc.count != "" {
					w.Header().Set("X-Bill-Count", tc.count)
				}
				_, _ = w.Write([]byte(csvBody))
			}))
			defer srv.Close()

			dl, err := biz.NewBillDownloader(nil, biz.BillDownloaderConfig{
				BaseURL: srv.URL,
				Key:     []byte("test-mock-key"),
				Clock:   clk,
			})
			if err != nil {
				t.Fatalf("NewBillDownloader: %v", err)
			}
			if _, _, err := dl.DownloadAndParse(context.Background(), "2026-10-26"); err == nil {
				t.Fatalf("expected header verification error for %s", tc.name)
			}
		})
	}
}

// REC-02 规则 3：严格解析 CSV（表头、整数金额、时间格式、行号错误信息）。
func TestParseBillCSVStrict(t *testing.T) {
	header := "txn_type,req_no,channel_txn_no,amount,status,finished_at\n"
	validRow := "PAY,PA0000000123456789012,MC0000000123456789001,10000,SUCCESS,2026-10-26T10:00:01.234+08:00\n"

	cases := []struct {
		name     string
		body     string
		wantLine string
	}{
		{
			name:     "empty file",
			body:     "",
			wantLine: "line 1",
		},
		{
			name:     "utf8 bom in header",
			body:     "\xef\xbb\xbf" + header + validRow,
			wantLine: "line 1",
		},
		{
			name:     "wrong header column",
			body:     "txn_type,biz_no,channel_txn_no,amount,status,finished_at\n" + validRow,
			wantLine: "line 1",
		},
		{
			name:     "wrong column count on line 3",
			body:     header + validRow + "PAY,PA2,MC2,100,SUCCESS\n",
			wantLine: "line 3",
		},
		{
			name:     "invalid txn_type",
			body:     header + "TRANSFER,TR1,MC1,100,SUCCESS,2026-10-26T10:00:01.234+08:00\n",
			wantLine: "line 2",
		},
		{
			name:     "float amount",
			body:     header + "PAY,PA1,MC1,100.50,SUCCESS,2026-10-26T10:00:01.234+08:00\n",
			wantLine: "line 2",
		},
		{
			name:     "negative amount",
			body:     header + "PAY,PA1,MC1,-100,SUCCESS,2026-10-26T10:00:01.234+08:00\n",
			wantLine: "line 2",
		},
		{
			name:     "invalid time format",
			body:     header + "PAY,PA1,MC1,100,SUCCESS,2026-10-26 10:00:01\n",
			wantLine: "line 2",
		},
		{
			name:     "empty req_no",
			body:     header + "PAY,,MC1,100,SUCCESS,2026-10-26T10:00:01.234+08:00\n",
			wantLine: "line 2",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := biz.ParseBillCSV([]byte(tc.body), "RB1", "mockpay", "2026-10-26")
			if err == nil {
				t.Fatalf("expected parse error for %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantLine) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantLine)
			}
		})
	}

	// 合法 0 行账单（只有表头）应正常解析为空切片
	rows, err := biz.ParseBillCSV([]byte(header), "RB1", "mockpay", "2026-10-26")
	if err != nil {
		t.Fatalf("empty data rows should succeed, got: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("len(rows) = %d, want 0", len(rows))
	}
	_ = strconv.Itoa(len(rows))
	_ = dal.StatusInit
}
