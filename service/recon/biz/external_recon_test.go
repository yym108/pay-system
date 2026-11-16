package biz_test

import (
	"context"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

type fakeExternalStore struct {
	bills          []dal.ChannelBill
	matchedBillIDs []uint64
	diffBillIDs    []uint64
	savedDiffs     []dal.Diff
	insertedByType map[string]int
}

func (f *fakeExternalStore) ListChannelBillsByBatch(_ context.Context, _ string) ([]dal.ChannelBill, error) {
	return f.bills, nil
}

func (f *fakeExternalStore) SaveExternalCheckResult(_ context.Context, matchedBillIDs, diffBillIDs []uint64, diffs []dal.Diff) (map[string]int, error) {
	f.matchedBillIDs = append([]uint64(nil), matchedBillIDs...)
	f.diffBillIDs = append([]uint64(nil), diffBillIDs...)
	f.savedDiffs = append([]dal.Diff(nil), diffs...)
	if f.insertedByType != nil {
		return f.insertedByType, nil
	}
	counts := make(map[string]int)
	for _, d := range diffs {
		counts[d.DiffType]++
	}
	return counts, nil
}

type fakePaymentTxnReader struct {
	dayTxns   []dal.ChannelTxn
	byBizTxns []dal.ChannelTxn
}

func (f *fakePaymentTxnReader) ListSuccessChannelTxnsByDate(_ context.Context, _ string, _, _ time.Time) ([]dal.ChannelTxn, error) {
	return f.dayTxns, nil
}

func (f *fakePaymentTxnReader) ListChannelTxnsByBizNos(_ context.Context, _ string, bizNos []string) ([]dal.ChannelTxn, error) {
	wanted := make(map[string]struct{}, len(bizNos))
	for _, no := range bizNos {
		wanted[no] = struct{}{}
	}
	var out []dal.ChannelTxn
	for _, tx := range f.byBizTxns {
		if _, ok := wanted[tx.BizNo]; ok {
			out = append(out, tx)
		}
	}
	return out, nil
}

// REC-03 验收 1：四类外部差错各识别 1 条，其余全部匹配。
func TestExternalReconFourDiffTypes(t *testing.T) {
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	finTime := time.Date(2026, 10, 26, 12, 0, 0, 0, shanghai)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids, err := idgen.New(4, clk)
	if err != nil {
		t.Fatalf("idgen.New: %v", err)
	}

	mcMatched := "MC0001"
	mcLocalMissing := "MC0002"
	mcAmountMismatch := "MC0003"
	mcStatusMismatch := "MC0004"
	mcChannelMissing := "MC0005"

	store := &fakeExternalStore{
		bills: []dal.ChannelBill{
			{ID: 1, BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26", TxnType: "PAY", ChannelTxnNo: mcMatched, BizNo: "PA001", Amount: 1000, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
			{ID: 2, BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26", TxnType: "PAY", ChannelTxnNo: mcLocalMissing, BizNo: "PA002", Amount: 2000, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
			{ID: 3, BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26", TxnType: "PAY", ChannelTxnNo: mcAmountMismatch, BizNo: "PA003", Amount: 3001, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
			{ID: 4, BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26", TxnType: "PAY", ChannelTxnNo: mcStatusMismatch, BizNo: "PA004", Amount: 4000, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
		},
	}
	txns := &fakePaymentTxnReader{
		dayTxns: []dal.ChannelTxn{
			{Channel: "mockpay", TxnType: "PAY", BizNo: "PA001", ChannelTxnNo: &mcMatched, Amount: 1000, Status: "SUCCESS"},
			{Channel: "mockpay", TxnType: "PAY", BizNo: "PA003", ChannelTxnNo: &mcAmountMismatch, Amount: 3000, Status: "SUCCESS"},
			{Channel: "mockpay", TxnType: "PAY", BizNo: "PA005", ChannelTxnNo: &mcChannelMissing, Amount: 5000, Status: "SUCCESS"},
		},
		byBizTxns: []dal.ChannelTxn{
			{Channel: "mockpay", TxnType: "PAY", BizNo: "PA001", ChannelTxnNo: &mcMatched, Amount: 1000, Status: "SUCCESS"},
			{Channel: "mockpay", TxnType: "PAY", BizNo: "PA003", ChannelTxnNo: &mcAmountMismatch, Amount: 3000, Status: "SUCCESS"},
			{Channel: "mockpay", TxnType: "PAY", BizNo: "PA004", ChannelTxnNo: &mcStatusMismatch, Amount: 4000, Status: "PROCESSING"},
		},
	}

	checker := biz.NewExternalCheckerWithDeps(store, txns, ids, clk)
	res, err := checker.Check(context.Background(), &dal.Batch{
		BatchNo:  "RB001",
		Channel:  "mockpay",
		BillDate: "2026-10-26",
	})
	if err != nil {
		t.Fatalf("Check err: %v", err)
	}

	if res.ChannelCount != 4 || res.ChannelAmount != 10001 {
		t.Fatalf("channel stats = (%d, %d), want (4, 10001)", res.ChannelCount, res.ChannelAmount)
	}
	if res.LocalCount != 3 || res.LocalAmount != 9000 {
		t.Fatalf("local stats = (%d, %d), want (3, 9000)", res.LocalCount, res.LocalAmount)
	}
	if res.MatchedCount != 1 {
		t.Fatalf("matched = %d, want 1", res.MatchedCount)
	}
	if len(store.matchedBillIDs) != 1 || store.matchedBillIDs[0] != 1 {
		t.Fatalf("matchedBillIDs = %v, want [1]", store.matchedBillIDs)
	}
	if len(store.diffBillIDs) != 3 {
		t.Fatalf("diffBillIDs = %v, want 3 items", store.diffBillIDs)
	}

	byType := make(map[string]dal.Diff)
	for _, d := range store.savedDiffs {
		byType[d.DiffType] = d
		if d.Status != dal.DiffStatusPending {
			t.Fatalf("diff %s status = %s, want PENDING", d.DiffType, d.Status)
		}
		if len(d.DiffNo) != 21 || d.DiffNo[:2] != "RD" {
			t.Fatalf("diff_no = %q, want RD prefix 21 chars", d.DiffNo)
		}
	}
	if len(byType) != 4 {
		t.Fatalf("diff types = %d, want 4: %+v", len(byType), store.savedDiffs)
	}
	if d := byType[dal.DiffLocalMissing]; d.BizNo != "PA002" || d.LocalAmount != nil || *d.ChannelAmount != 2000 {
		t.Fatalf("LOCAL_MISSING = %+v", d)
	}
	if d := byType[dal.DiffAmountMismatch]; d.BizNo != "PA003" || *d.LocalAmount != 3000 || *d.ChannelAmount != 3001 {
		t.Fatalf("AMOUNT_MISMATCH = %+v", d)
	}
	if d := byType[dal.DiffStatusMismatch]; d.BizNo != "PA004" || *d.LocalStatus != "PROCESSING" || *d.ChannelStatus != "SUCCESS" {
		t.Fatalf("STATUS_MISMATCH = %+v", d)
	}
	if d := byType[dal.DiffChannelMissing]; d.BizNo != "PA005" || *d.LocalAmount != 5000 || d.ChannelAmount != nil || d.ChannelStatus != nil {
		t.Fatalf("CHANNEL_MISSING = %+v", d)
	}
}

// REC-03 验收 2：全部正常的账单差错数为 0。
func TestExternalReconAllMatched(t *testing.T) {
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	finTime := time.Date(2026, 10, 26, 12, 0, 0, 0, shanghai)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids, err := idgen.New(4, clk)
	if err != nil {
		t.Fatalf("idgen.New: %v", err)
	}

	mc1 := "MC1001"
	mc2 := "MC1002"
	store := &fakeExternalStore{
		bills: []dal.ChannelBill{
			{ID: 1, BatchNo: "RB002", Channel: "mockpay", BillDate: "2026-10-26", TxnType: "PAY", ChannelTxnNo: mc1, BizNo: "PA101", Amount: 1200, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
			{ID: 2, BatchNo: "RB002", Channel: "mockpay", BillDate: "2026-10-26", TxnType: "REFUND", ChannelTxnNo: mc2, BizNo: "RF102", Amount: 300, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
		},
	}
	txns := &fakePaymentTxnReader{
		dayTxns: []dal.ChannelTxn{
			{Channel: "mockpay", TxnType: "PAY", BizNo: "PA101", ChannelTxnNo: &mc1, Amount: 1200, Status: "SUCCESS"},
			{Channel: "mockpay", TxnType: "REFUND", BizNo: "RF102", ChannelTxnNo: &mc2, Amount: 300, Status: "SUCCESS"},
		},
		byBizTxns: []dal.ChannelTxn{
			{Channel: "mockpay", TxnType: "PAY", BizNo: "PA101", ChannelTxnNo: &mc1, Amount: 1200, Status: "SUCCESS"},
			{Channel: "mockpay", TxnType: "REFUND", BizNo: "RF102", ChannelTxnNo: &mc2, Amount: 300, Status: "SUCCESS"},
		},
	}

	checker := biz.NewExternalCheckerWithDeps(store, txns, ids, clk)
	res, err := checker.Check(context.Background(), &dal.Batch{
		BatchNo:  "RB002",
		Channel:  "mockpay",
		BillDate: "2026-10-26",
	})
	if err != nil {
		t.Fatalf("Check err: %v", err)
	}
	if res.ChannelCount != 2 || res.LocalCount != 2 || res.MatchedCount != 2 {
		t.Fatalf("stats = %+v, want 2/2/2", res)
	}
	if len(store.savedDiffs) != 0 {
		t.Fatalf("savedDiffs = %d, want 0", len(store.savedDiffs))
	}
	if len(store.matchedBillIDs) != 2 || len(store.diffBillIDs) != 0 {
		t.Fatalf("bill statuses = (%v, %v), want 2 matched, 0 diff", store.matchedBillIDs, store.diffBillIDs)
	}
}
