package biz_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

type fakeInternalStore struct {
	savedDiffs     []dal.Diff
	insertedByType map[string]int
}

func (f *fakeInternalStore) InsertInternalDiffs(_ context.Context, diffs []dal.Diff) (map[string]int, error) {
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

type fakeInternalPaymentReader struct {
	payments  []dal.InternalPaymentOrder
	refunds   []dal.InternalRefundOrder
	transfers []dal.InternalTransferOrder
}

func (f *fakeInternalPaymentReader) ListSuccessPaymentOrdersByDate(_ context.Context, _ string, _, _ time.Time) ([]dal.InternalPaymentOrder, error) {
	return f.payments, nil
}

func (f *fakeInternalPaymentReader) ListSuccessRefundOrdersByDate(_ context.Context, _ string, _, _ time.Time) ([]dal.InternalRefundOrder, error) {
	return f.refunds, nil
}

func (f *fakeInternalPaymentReader) ListFinishedTransferOrdersByDate(_ context.Context, _, _ time.Time) ([]dal.InternalTransferOrder, error) {
	return f.transfers, nil
}

type fakeAccountShardReader struct {
	ledgers  []dal.AccountLedgerTxn
	freezes  []dal.AccountFreezeRecord
	branches []dal.AccountTransferBranch
}

func (f *fakeAccountShardReader) ListLedgerTxnsByBizNos(_ context.Context, bizTypes, bizNos []string) ([]dal.AccountLedgerTxn, error) {
	typeSet := make(map[string]struct{}, len(bizTypes))
	for _, bt := range bizTypes {
		typeSet[bt] = struct{}{}
	}
	noSet := make(map[string]struct{}, len(bizNos))
	for _, bn := range bizNos {
		noSet[bn] = struct{}{}
	}
	var out []dal.AccountLedgerTxn
	for _, l := range f.ledgers {
		if _, ok := typeSet[l.BizType]; !ok {
			continue
		}
		if _, ok := noSet[l.BizNo]; !ok {
			continue
		}
		out = append(out, l)
	}
	return out, nil
}

func (f *fakeAccountShardReader) ListAccountFreezesByBizNos(_ context.Context, bizType string, bizNos []string) ([]dal.AccountFreezeRecord, error) {
	noSet := make(map[string]struct{}, len(bizNos))
	for _, bn := range bizNos {
		noSet[bn] = struct{}{}
	}
	var out []dal.AccountFreezeRecord
	for _, fr := range f.freezes {
		if fr.BizType == bizType {
			if _, ok := noSet[fr.BizNo]; ok {
				out = append(out, fr)
			}
		}
	}
	return out, nil
}

func (f *fakeAccountShardReader) ListTransferBranchesByTransferNos(_ context.Context, transferNos []string) ([]dal.AccountTransferBranch, error) {
	noSet := make(map[string]struct{}, len(transferNos))
	for _, tn := range transferNos {
		noSet[tn] = struct{}{}
	}
	var out []dal.AccountTransferBranch
	for _, br := range f.branches {
		if _, ok := noSet[br.TransferNo]; ok {
			out = append(out, br)
		}
	}
	return out, nil
}

// REC-05 验收 1：支付单/退款单缺失凭证或金额不等识别为 LEDGER_MISSING，金额不等时 handle_note 注明双方金额。
func TestInternalReconPaymentAndRefundLedgerMissing(t *testing.T) {
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids, err := idgen.New(4, clk)
	if err != nil {
		t.Fatalf("idgen.New: %v", err)
	}

	// M100000 -> shard 0, M100001 -> shard 1
	payReader := &fakeInternalPaymentReader{
		payments: []dal.InternalPaymentOrder{
			{PayNo: "PA_OK_0", MerchantID: "M100000", Amount: 1000, Status: "SUCCESS"},
			{PayNo: "PA_MISS_1", MerchantID: "M100001", Amount: 2000, Status: "SUCCESS"},
			{PayNo: "PA_AMT_0", MerchantID: "M100000", Amount: 3000, Status: "SUCCESS"},
		},
		refunds: []dal.InternalRefundOrder{
			{RefundNo: "RF_OK_1", MerchantID: "M100001", Amount: 500, Status: "SUCCESS"},
			{RefundNo: "RF_MISS_0", MerchantID: "M100000", Amount: 600, Status: "SUCCESS"},
			{RefundNo: "RF_AMT_1", MerchantID: "M100001", Amount: 700, Status: "SUCCESS"},
		},
	}

	shard0 := &fakeAccountShardReader{
		ledgers: []dal.AccountLedgerTxn{
			{TxnNo: "TX_P_OK", BizType: "PAY", BizNo: "PA_OK_0", Amount: 1000},
			{TxnNo: "TX_P_AMT", BizType: "PAY", BizNo: "PA_AMT_0", Amount: 2999},
		},
	}
	shard1 := &fakeAccountShardReader{
		ledgers: []dal.AccountLedgerTxn{
			{TxnNo: "TX_R_OK", BizType: "REFUND", BizNo: "RF_OK_1", Amount: 500},
			{TxnNo: "TX_R_AMT", BizType: "REFUND", BizNo: "RF_AMT_1", Amount: 701},
		},
	}

	store := &fakeInternalStore{}
	checker := biz.NewInternalCheckerWithDeps(store, payReader, [2]biz.AccountShardReader{shard0, shard1}, ids, clk)
	batch := &dal.Batch{BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26"}

	if _, err := checker.Check(context.Background(), batch); err != nil {
		t.Fatalf("Check: %v", err)
	}

	if len(store.savedDiffs) != 4 {
		t.Fatalf("savedDiffs count = %d, want 4: %+v", len(store.savedDiffs), store.savedDiffs)
	}

	byKey := make(map[string]dal.Diff, len(store.savedDiffs))
	for _, d := range store.savedDiffs {
		byKey[d.TxnType+":"+d.BizNo] = d
		if d.DiffType != dal.DiffLedgerMissing {
			t.Errorf("diff %s type = %s, want LEDGER_MISSING", d.BizNo, d.DiffType)
		}
		if d.ChannelTxnNo != nil || d.ChannelAmount != nil || d.ChannelStatus != nil {
			t.Errorf("diff %s channel fields should be nil", d.BizNo)
		}
	}

	if d, ok := byKey["PAY:PA_MISS_1"]; !ok || *d.LocalAmount != 2000 || *d.LocalStatus != "SUCCESS" {
		t.Errorf("unexpected PAY:PA_MISS_1 diff: %+v", d)
	}
	if d, ok := byKey["PAY:PA_AMT_0"]; !ok || d.HandleNote == nil || !strings.Contains(*d.HandleNote, "3000") || !strings.Contains(*d.HandleNote, "2999") {
		t.Errorf("PAY:PA_AMT_0 handle_note should contain both amounts, got: %+v", d.HandleNote)
	}
	if _, ok := byKey["REFUND:RF_MISS_0"]; !ok {
		t.Errorf("missing REFUND:RF_MISS_0 diff")
	}
	if d, ok := byKey["REFUND:RF_AMT_1"]; !ok || d.HandleNote == nil || !strings.Contains(*d.HandleNote, "700") || !strings.Contains(*d.HandleNote, "701") {
		t.Errorf("REFUND:RF_AMT_1 handle_note should contain both amounts, got: %+v", d.HandleNote)
	}
}

// REC-05 验收 2：转账单按不变量 12 检查，缺失 TRANSFER_IN 等异常识别为 TRANSFER_MISMATCH，正常转账差错数为 0。
func TestInternalReconTransferMismatch(t *testing.T) {
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids, err := idgen.New(4, clk)
	if err != nil {
		t.Fatalf("idgen.New: %v", err)
	}

	payReader := &fakeInternalPaymentReader{
		transfers: []dal.InternalTransferOrder{
			// 正常同分片 SUCCESS
			{TransferNo: "TF_OK_LOCAL", Amount: 1000, Mode: "LOCAL", PayerShard: 0, PayeeShard: 0, Status: "SUCCESS"},
			// 正常跨分片 SUCCESS
			{TransferNo: "TF_OK_TCC", Amount: 2000, Mode: "TCC", PayerShard: 0, PayeeShard: 1, Status: "SUCCESS"},
			// 正常 FAILED（无凭证、冻结 RELEASED、分支 CANCELLED）
			{TransferNo: "TF_OK_FAIL", Amount: 3000, Mode: "TCC", PayerShard: 1, PayeeShard: 0, Status: "FAILED"},
			// 异常 TCC：缺少 TRANSFER_IN 凭证
			{TransferNo: "TF_MISS_IN", Amount: 4000, Mode: "TCC", PayerShard: 0, PayeeShard: 1, Status: "SUCCESS"},
			// 异常 LOCAL：缺少 TRANSFER 凭证
			{TransferNo: "TF_MISS_LOCAL", Amount: 5000, Mode: "LOCAL", PayerShard: 1, PayeeShard: 1, Status: "SUCCESS"},
			// 异常 FAILED：残留 TRANSFER_OUT 凭证
			{TransferNo: "TF_FAIL_LEFTOVER", Amount: 6000, Mode: "TCC", PayerShard: 0, PayeeShard: 1, Status: "FAILED"},
		},
	}

	shard0 := &fakeAccountShardReader{
		ledgers: []dal.AccountLedgerTxn{
			{TxnNo: "TX_L_OK", BizType: "TRANSFER", BizNo: "TF_OK_LOCAL", Amount: 1000},
			{TxnNo: "TX_TCC_OUT", BizType: "TRANSFER_OUT", BizNo: "TF_OK_TCC", Amount: 2000},
			{TxnNo: "TX_MISS_IN_OUT", BizType: "TRANSFER_OUT", BizNo: "TF_MISS_IN", Amount: 4000},
			{TxnNo: "TX_FAIL_OUT", BizType: "TRANSFER_OUT", BizNo: "TF_FAIL_LEFTOVER", Amount: 6000},
		},
		freezes: []dal.AccountFreezeRecord{
			{BizType: "TRANSFER", BizNo: "TF_OK_TCC", Amount: 2000, Status: "CONFIRMED"},
			{BizType: "TRANSFER", BizNo: "TF_MISS_IN", Amount: 4000, Status: "CONFIRMED"},
			{BizType: "TRANSFER", BizNo: "TF_FAIL_LEFTOVER", Amount: 6000, Status: "RELEASED"},
		},
		branches: []dal.AccountTransferBranch{
			{TransferNo: "TF_OK_FAIL", Role: "PAYEE", Amount: 3000, Status: "CANCELLED"},
		},
	}
	shard1 := &fakeAccountShardReader{
		ledgers: []dal.AccountLedgerTxn{
			{TxnNo: "TX_TCC_IN", BizType: "TRANSFER_IN", BizNo: "TF_OK_TCC", Amount: 2000},
		},
		freezes: []dal.AccountFreezeRecord{
			{BizType: "TRANSFER", BizNo: "TF_OK_FAIL", Amount: 3000, Status: "RELEASED"},
		},
		branches: []dal.AccountTransferBranch{
			{TransferNo: "TF_OK_TCC", Role: "PAYEE", Amount: 2000, Status: "CONFIRMED"},
			{TransferNo: "TF_MISS_IN", Role: "PAYEE", Amount: 4000, Status: "CONFIRMED"},
		},
	}

	store := &fakeInternalStore{}
	checker := biz.NewInternalCheckerWithDeps(store, payReader, [2]biz.AccountShardReader{shard0, shard1}, ids, clk)
	batch := &dal.Batch{BatchNo: "RB002", Channel: "mockpay", BillDate: "2026-10-26"}

	if _, err := checker.Check(context.Background(), batch); err != nil {
		t.Fatalf("Check: %v", err)
	}

	if len(store.savedDiffs) != 3 {
		t.Fatalf("savedDiffs count = %d, want 3: %+v", len(store.savedDiffs), store.savedDiffs)
	}
	byNo := make(map[string]dal.Diff, len(store.savedDiffs))
	for _, d := range store.savedDiffs {
		byNo[d.BizNo] = d
		if d.DiffType != dal.DiffTransferMismatch {
			t.Errorf("%s diff_type = %s, want TRANSFER_MISMATCH", d.BizNo, d.DiffType)
		}
		if d.TxnType != "TRANSFER" {
			t.Errorf("%s txn_type = %s, want TRANSFER", d.BizNo, d.TxnType)
		}
		if d.ChannelTxnNo != nil || d.ChannelAmount != nil || d.ChannelStatus != nil {
			t.Errorf("%s channel fields must be nil", d.BizNo)
		}
		if d.HandleNote == nil || *d.HandleNote == "" {
			t.Errorf("%s handle_note must record shard details", d.BizNo)
		}
	}

	for _, want := range []string{"TF_MISS_IN", "TF_MISS_LOCAL", "TF_FAIL_LEFTOVER"} {
		if _, ok := byNo[want]; !ok {
			t.Errorf("expected TRANSFER_MISMATCH for %s", want)
		}
	}
}

func TestInternalReconNonMockpaySkipsTransfer(t *testing.T) {
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids, err := idgen.New(4, clk)
	if err != nil {
		t.Fatalf("idgen.New: %v", err)
	}

	payReader := &fakeInternalPaymentReader{
		transfers: []dal.InternalTransferOrder{
			{TransferNo: "TF_MISS", Amount: 1000, Mode: "LOCAL", PayerShard: 0, PayeeShard: 0, Status: "SUCCESS"},
		},
	}
	store := &fakeInternalStore{}
	checker := biz.NewInternalCheckerWithDeps(store, payReader, [2]biz.AccountShardReader{&fakeAccountShardReader{}, &fakeAccountShardReader{}}, ids, clk)
	batch := &dal.Batch{BatchNo: "RB003", Channel: "alipay", BillDate: "2026-10-26"}

	if _, err := checker.Check(context.Background(), batch); err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(store.savedDiffs) != 0 {
		t.Fatalf("expected 0 diffs for non-mockpay transfer check, got %d", len(store.savedDiffs))
	}
}
