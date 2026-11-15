package biz

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

// CH-08：日期参数校验与就绪判定（今天/未来返回 Ready=false；MOCK_ALLOW_PARTIAL_BILL=true 只放行今天，不放行未来）。
func TestDownloadBillValidationAndReadiness(t *testing.T) {
	now := time.Date(2026, 10, 12, 14, 0, 0, 0, shanghai)
	cases := []struct {
		name             string
		allowPartialBill bool
		date             string
		wantErr          error
		wantReady        bool
	}{
		{name: "empty date", date: "", wantErr: ErrInvalidRequest},
		{name: "bad format", date: "2026-10-1", wantErr: ErrInvalidRequest},
		{name: "non-existent date", date: "2026-02-30", wantErr: ErrInvalidRequest},
		{name: "extra suffix", date: "2026-10-12T00:00:00", wantErr: ErrInvalidRequest},
		{name: "today disallowed by default", allowPartialBill: false, date: "2026-10-12", wantReady: false},
		{name: "today allowed when partial enabled", allowPartialBill: true, date: "2026-10-12", wantReady: true},
		{name: "future disallowed even when partial enabled", allowPartialBill: true, date: "2026-10-13", wantReady: false},
		{name: "yesterday always ready", allowPartialBill: false, date: "2026-10-11", wantReady: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			clk := &clock.Fake{}
			clk.Set(now)
			svc := New(store, &seqIDs{}, clk, &manualSched{}, Options{
				DelayMinMS:       200,
				DelayMaxMS:       200,
				AllowPartialBill: tc.allowPartialBill,
			})
			res, err := svc.DownloadBill(t.Context(), tc.date)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if res.Ready != tc.wantReady {
				t.Fatalf("Ready = %v, want %v", res.Ready, tc.wantReady)
			}
		})
	}
}

// CH-08 验收 1：账单行数与 Count 一致，内容哈希与 SHA256 一致；多次下载字节一致；空账单只有表头。
// CH-08 验收 2：BILL_MISSING 不在账单中，BILL_AMOUNT_DIFF 金额 +1。
func TestDownloadBillCSVAndScenarios(t *testing.T) {
	now := time.Date(2026, 10, 13, 10, 0, 0, 0, shanghai)
	store := newMemStore()
	t1 := time.Date(2026, 10, 12, 10, 0, 1, 234999999, shanghai)
	t2 := time.Date(2026, 10, 12, 11, 20, 0, 500000000, shanghai)

	store.rows[key(dal.TxnTypePay, "PA001")] = &dal.Txn{
		ID:           1,
		ChannelTxnNo: "MC0000000123456789001",
		TxnType:      dal.TxnTypePay,
		ReqNo:        "PA0000000123456789012",
		Amount:       10000,
		Status:       dal.StatusSuccess,
		Scenario:     string(scenario.Normal),
		FinishedAt:   t1,
		BillDate:     "2026-10-12",
	}
	store.rows[key(dal.TxnTypePay, "PA_DIFF")] = &dal.Txn{
		ID:           2,
		ChannelTxnNo: "MC0000000123456789002",
		TxnType:      dal.TxnTypePay,
		ReqNo:        "PA0000000123456789099",
		Amount:       10007,
		Status:       dal.StatusSuccess,
		Scenario:     string(scenario.BillAmountDiff),
		FinishedAt:   t1,
		BillDate:     "2026-10-12",
	}
	store.rows[key(dal.TxnTypeRefund, "RF001")] = &dal.Txn{
		ID:           3,
		ChannelTxnNo: "MC0000000123456789003",
		TxnType:      dal.TxnTypeRefund,
		ReqNo:        "RF0000000123456789013",
		Amount:       3000,
		Status:       dal.StatusSuccess,
		Scenario:     string(scenario.Normal),
		FinishedAt:   t2,
		BillDate:     "2026-10-12",
	}
	// BILL_MISSING 即使 bill_date 不为空也不应出现；正常情况 bill_date 为空也不会出现。
	store.rows[key(dal.TxnTypePay, "PA_MISS")] = &dal.Txn{
		ID:           4,
		ChannelTxnNo: "MC0000000123456789004",
		TxnType:      dal.TxnTypePay,
		ReqNo:        "PA_MISS",
		Amount:       10006,
		Status:       dal.StatusSuccess,
		Scenario:     string(scenario.BillMissing),
		FinishedAt:   t2,
		BillDate:     "2026-10-12",
	}
	// 非 SUCCESS、其他日期都不出现。
	store.rows[key(dal.TxnTypePay, "PA_FAIL")] = &dal.Txn{
		ID:           5,
		ChannelTxnNo: "MC0000000123456789005",
		TxnType:      dal.TxnTypePay,
		ReqNo:        "PA_FAIL",
		Amount:       10001,
		Status:       dal.StatusFailed,
		Scenario:     string(scenario.Fail),
		FinishedAt:   t2,
		BillDate:     "2026-10-12",
	}

	clk := &clock.Fake{}
	clk.Set(now)
	svc := New(store, &seqIDs{}, clk, &manualSched{}, Options{
		DelayMinMS: 200,
		DelayMaxMS: 200,
	})
	res1, err := svc.DownloadBill(t.Context(), "2026-10-12")
	if err != nil {
		t.Fatalf("DownloadBill err = %v", err)
	}
	if !res1.Ready {
		t.Fatal("Ready = false, want true")
	}
	wantCSV := strings.Join([]string{
		"txn_type,req_no,channel_txn_no,amount,status,finished_at",
		"PAY,PA0000000123456789012,MC0000000123456789001,10000,SUCCESS,2026-10-12T10:00:01.234+08:00",
		"PAY,PA0000000123456789099,MC0000000123456789002,10008,SUCCESS,2026-10-12T10:00:01.234+08:00",
		"REFUND,RF0000000123456789013,MC0000000123456789003,3000,SUCCESS,2026-10-12T11:20:00.500+08:00",
		"",
	}, "\n")
	if string(res1.Content) != wantCSV {
		t.Fatalf("CSV = %q\nwant %q", string(res1.Content), wantCSV)
	}
	if res1.Count != 3 {
		t.Fatalf("Count = %d, want 3", res1.Count)
	}
	wantHash := fmt.Sprintf("%x", sha256.Sum256([]byte(wantCSV)))
	if res1.SHA256 != wantHash {
		t.Fatalf("SHA256 = %s, want %s", res1.SHA256, wantHash)
	}

	// CH-08 规则 5：多次下载字节一致
	res2, err := svc.DownloadBill(t.Context(), "2026-10-12")
	if err != nil {
		t.Fatalf("second DownloadBill err = %v", err)
	}
	if string(res2.Content) != string(res1.Content) || res2.SHA256 != res1.SHA256 || res2.Count != res1.Count {
		t.Fatal("second download differed from first")
	}

	// 空账单只有表头
	empty, err := svc.DownloadBill(t.Context(), "2026-10-11")
	if err != nil {
		t.Fatalf("empty DownloadBill err = %v", err)
	}
	if empty.Count != 0 || string(empty.Content) != "txn_type,req_no,channel_txn_no,amount,status,finished_at\n" {
		t.Fatalf("empty bill = count=%d content=%q", empty.Count, string(empty.Content))
	}
}

func TestDownloadBillPropagatesStoreError(t *testing.T) {
	now := time.Date(2026, 10, 13, 10, 0, 0, 0, shanghai)
	store := newMemStore()
	store.listErr = errStore
	clk := &clock.Fake{}
	clk.Set(now)
	svc := New(store, &seqIDs{}, clk, &manualSched{}, Options{
		DelayMinMS: 200,
		DelayMaxMS: 200,
	})
	if _, err := svc.DownloadBill(t.Context(), "2026-10-12"); !errors.Is(err, errStore) {
		t.Fatalf("err = %v, want %v", err, errStore)
	}
}
