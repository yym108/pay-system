//go:build integration

package biz_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

func openReconAndPaymentDBs(t *testing.T) (*sqlx.DB, *sqlx.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mysql, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatalf("testkit.Ready: %v", err)
	}
	reconSchema, err := mysql.OpenSchema(ctx, "recon")
	if err != nil {
		t.Fatalf("OpenSchema(recon): %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer ccancel()
		_ = reconSchema.Close(cctx)
	})
	paySchema, err := mysql.OpenSchema(ctx, "payment")
	if err != nil {
		t.Fatalf("OpenSchema(payment): %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer ccancel()
		_ = paySchema.Close(cctx)
	})
	return reconSchema.DB, paySchema.DB
}

func insertChannelTxnFixture(t *testing.T, db *sqlx.DB, txnType, bizNo string, channelTxnNo *string, amount int64, status string, finishedAt *time.Time) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `INSERT INTO channel_txn
		(channel, txn_type, biz_no, channel_txn_no, amount, status, channel_finished_at)
		VALUES ('mockpay', ?, ?, ?, ?, ?, ?)`,
		txnType, bizNo, channelTxnNo, amount, status, finishedAt)
	if err != nil {
		t.Fatalf("insert channel_txn %s: %v", bizNo, err)
	}
}

func reconDiffCounter(t *testing.T, diffType string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "pay_recon_diffs_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "diff_type" && lp.GetValue() == diffType {
					return uint64(m.GetCounter().GetValue())
				}
			}
		}
	}
	return 0
}

// REC-03 验收 1：四类外部差错各识别 1 条（LOCAL_MISSING、CHANNEL_MISSING、AMOUNT_MISMATCH、STATUS_MISMATCH），其余全部匹配。
func TestExternalReconIntegrationFourDiffTypes(t *testing.T) {
	reconDB, paymentDB := openReconAndPaymentDBs(t)
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	billDate := "2026-10-26"
	finTime := time.Date(2026, 10, 26, 15, 30, 0, 0, shanghai)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids := newTestIDGen(t, clk)

	mcMatch := "MC_MATCH_01"
	mcLocalMissing := "MC_LM_02"
	mcAmtMismatch := "MC_AM_03"
	mcStatusMismatch := "MC_SM_04"
	mcChannelMissing := "MC_CM_05"

	// 本地流水：1 条正常匹配、1 条金额不符、1 条状态不符（PROCESSING）、1 条短款（无账单行）。
	insertChannelTxnFixture(t, paymentDB, "PAY", "PA_MATCH_01", &mcMatch, 1000, "SUCCESS", &finTime)
	insertChannelTxnFixture(t, paymentDB, "PAY", "PA_AM_03", &mcAmtMismatch, 3000, "SUCCESS", &finTime)
	insertChannelTxnFixture(t, paymentDB, "PAY", "PA_SM_04", &mcStatusMismatch, 4000, "PROCESSING", nil)
	insertChannelTxnFixture(t, paymentDB, "PAY", "PA_CM_05", &mcChannelMissing, 5000, "SUCCESS", &finTime)

	dl := &fakeDownloader{
		fn: func(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (biz.DownloadResult, error) {
			bills := []dal.ChannelBill{
				{BatchNo: batch.BatchNo, Channel: batch.Channel, BillDate: batch.BillDate, TxnType: "PAY", ChannelTxnNo: mcMatch, BizNo: "PA_MATCH_01", Amount: 1000, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
				{BatchNo: batch.BatchNo, Channel: batch.Channel, BillDate: batch.BillDate, TxnType: "PAY", ChannelTxnNo: mcLocalMissing, BizNo: "PA_LM_02", Amount: 2000, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
				{BatchNo: batch.BatchNo, Channel: batch.Channel, BillDate: batch.BillDate, TxnType: "PAY", ChannelTxnNo: mcAmtMismatch, BizNo: "PA_AM_03", Amount: 3001, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
				{BatchNo: batch.BatchNo, Channel: batch.Channel, BillDate: batch.BillDate, TxnType: "PAY", ChannelTxnNo: mcStatusMismatch, BizNo: "PA_SM_04", Amount: 4000, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
			}
			if err := dal.InsertChannelBillsBatch(ctx, exec, bills); err != nil {
				return biz.DownloadResult{}, err
			}
			return biz.DownloadResult{FileHash: "hash1"}, nil
		},
	}

	checker := biz.NewExternalChecker(reconDB, paymentDB, ids, clk)
	svc := biz.NewRecon(reconDB, ids, clk, []string{"mockpay"}, dl, checker)

	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	if out.Batch.Status != dal.StatusDone {
		t.Fatalf("status = %s, errMsg = %v", out.Batch.Status, out.Batch.ErrorMsg)
	}
	if out.Batch.ChannelCount != 4 || out.Batch.ChannelAmount != 10001 {
		t.Fatalf("channel = (%d, %d), want (4, 10001)", out.Batch.ChannelCount, out.Batch.ChannelAmount)
	}
	if out.Batch.LocalCount != 3 || out.Batch.LocalAmount != 9000 {
		t.Fatalf("local = (%d, %d), want (3, 9000)", out.Batch.LocalCount, out.Batch.LocalAmount)
	}
	if out.Batch.MatchedCount != 1 || out.Batch.DiffCount != 4 {
		t.Fatalf("matched/diff = (%d, %d), want (1, 4)", out.Batch.MatchedCount, out.Batch.DiffCount)
	}
	if len(out.Diffs) != 4 {
		t.Fatalf("len(diffs) = %d, want 4", len(out.Diffs))
	}

	// 验证 recon_channel_bill.match_status。
	var matchCount, diffBillCount int
	if err := reconDB.Get(&matchCount, `SELECT COUNT(*) FROM recon_channel_bill WHERE batch_no = ? AND match_status = 'MATCHED'`, out.Batch.BatchNo); err != nil {
		t.Fatal(err)
	}
	if err := reconDB.Get(&diffBillCount, `SELECT COUNT(*) FROM recon_channel_bill WHERE batch_no = ? AND match_status = 'DIFF'`, out.Batch.BatchNo); err != nil {
		t.Fatal(err)
	}
	if matchCount != 1 || diffBillCount != 3 {
		t.Fatalf("bill match_status counts = (%d MATCHED, %d DIFF), want (1, 3)", matchCount, diffBillCount)
	}
}

// REC-03 验收 2：全部正常的账单差错数为 0。
func TestExternalReconIntegrationAllMatched(t *testing.T) {
	reconDB, paymentDB := openReconAndPaymentDBs(t)
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	billDate := "2026-10-26"
	finTime := time.Date(2026, 10, 26, 11, 0, 0, 0, shanghai)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids := newTestIDGen(t, clk)

	mc1, mc2 := "MC_OK_1", "MC_OK_2"
	insertChannelTxnFixture(t, paymentDB, "PAY", "PA_OK_1", &mc1, 1500, "SUCCESS", &finTime)
	insertChannelTxnFixture(t, paymentDB, "REFUND", "RF_OK_2", &mc2, 500, "SUCCESS", &finTime)

	dl := &fakeDownloader{
		fn: func(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (biz.DownloadResult, error) {
			bills := []dal.ChannelBill{
				{BatchNo: batch.BatchNo, Channel: batch.Channel, BillDate: batch.BillDate, TxnType: "PAY", ChannelTxnNo: mc1, BizNo: "PA_OK_1", Amount: 1500, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
				{BatchNo: batch.BatchNo, Channel: batch.Channel, BillDate: batch.BillDate, TxnType: "REFUND", ChannelTxnNo: mc2, BizNo: "RF_OK_2", Amount: 500, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
			}
			if err := dal.InsertChannelBillsBatch(ctx, exec, bills); err != nil {
				return biz.DownloadResult{}, err
			}
			return biz.DownloadResult{FileHash: "hash_ok"}, nil
		},
	}

	checker := biz.NewExternalChecker(reconDB, paymentDB, ids, clk)
	svc := biz.NewRecon(reconDB, ids, clk, []string{"mockpay"}, dl, checker)

	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	if out.Batch.Status != dal.StatusDone || out.Batch.DiffCount != 0 || out.Batch.MatchedCount != 2 {
		t.Fatalf("unexpected batch: %+v", out.Batch)
	}
	if len(out.Diffs) != 0 {
		t.Fatalf("diffs = %d, want 0", len(out.Diffs))
	}
}

// REC-03 验收 3：外部差错与内部差错并存（同一笔支付同时产生 AMOUNT_MISMATCH 与 LEDGER_MISSING，无 1062，diff_count = 2）。
func TestExternalReconIntegrationCoexistExternalAndInternalDiff(t *testing.T) {
	reconDB, paymentDB := openReconAndPaymentDBs(t)
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	billDate := "2026-10-26"
	finTime := time.Date(2026, 10, 26, 14, 0, 0, 0, shanghai)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids := newTestIDGen(t, clk)

	mc := "MC_COEXIST_01"
	insertChannelTxnFixture(t, paymentDB, "PAY", "PA_COEXIST_01", &mc, 2000, "SUCCESS", &finTime)

	dl := &fakeDownloader{
		fn: func(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (biz.DownloadResult, error) {
			bills := []dal.ChannelBill{
				{BatchNo: batch.BatchNo, Channel: batch.Channel, BillDate: batch.BillDate, TxnType: "PAY", ChannelTxnNo: mc, BizNo: "PA_COEXIST_01", Amount: 2001, ChannelStatus: "SUCCESS", ChannelFinishedAt: finTime},
			}
			if err := dal.InsertChannelBillsBatch(ctx, exec, bills); err != nil {
				return biz.DownloadResult{}, err
			}
			return biz.DownloadResult{FileHash: "hash_coexist"}, nil
		},
	}

	extChecker := biz.NewExternalChecker(reconDB, paymentDB, ids, clk)
	// 组合 Checker：先执行外部比对，再模拟 REC-05 插入同笔交易的 LEDGER_MISSING。
	combined := &fakeChecker{
		fn: func(ctx context.Context, batch *dal.Batch) (biz.CheckResult, error) {
			res, err := extChecker.Check(ctx, batch)
			if err != nil {
				return biz.CheckResult{}, err
			}
			diffNo, err := ids.NewNo(ctx, "RD")
			if err != nil {
				return biz.CheckResult{}, err
			}
			localAmt := int64(2000)
			localSt := "SUCCESS"
			_, err = dal.InsertDiffsSkipDuplicate(ctx, reconDB, []dal.Diff{{
				DiffNo:      diffNo,
				BatchNo:     batch.BatchNo,
				Channel:     batch.Channel,
				BillDate:    batch.BillDate,
				DiffType:    dal.DiffLedgerMissing,
				TxnType:     "PAY",
				BizNo:       "PA_COEXIST_01",
				LocalAmount: &localAmt,
				LocalStatus: &localSt,
				Status:      dal.DiffStatusPending,
				CreatedAt:   clk.Now(),
				UpdatedAt:   clk.Now(),
			}})
			if err != nil {
				return biz.CheckResult{}, err
			}
			return res, nil
		},
	}

	svc := biz.NewRecon(reconDB, ids, clk, []string{"mockpay"}, dl, combined)
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	if out.Batch.Status != dal.StatusDone || out.Batch.DiffCount != 2 {
		t.Fatalf("batch = %+v, want DONE with diff_count=2", out.Batch)
	}
	if len(out.Diffs) != 2 {
		t.Fatalf("len(diffs) = %d, want 2", len(out.Diffs))
	}
}

// REC-03 验收 4：同键跳过（已有 IGNORED 的 CHANNEL_MISSING，重新比对再次得到同键差错时不报 1062，
// 已有行的 status、handle_action、handle_note、updated_at 不变，pay_recon_diffs_total 不增加，diff_count 等于实际行数）。
func TestExternalReconIntegrationSkipDuplicateDiff(t *testing.T) {
	reconDB, paymentDB := openReconAndPaymentDBs(t)
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	billDate := "2026-10-26"
	finTime := time.Date(2026, 10, 26, 16, 0, 0, 0, shanghai)
	t0 := time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai)
	clk := &clock.Fake{}
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	mc := "MC_SKIP_01"
	insertChannelTxnFixture(t, paymentDB, "PAY", "PA_SKIP_01", &mc, 5000, "SUCCESS", &finTime)

	dl := &fakeDownloader{
		fn: func(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (biz.DownloadResult, error) {
			// 空账单，PA_SKIP_01 将产生 CHANNEL_MISSING。
			return biz.DownloadResult{FileHash: "hash_empty"}, nil
		},
	}

	checker := biz.NewExternalChecker(reconDB, paymentDB, ids, clk)
	svc := biz.NewRecon(reconDB, ids, clk, []string{"mockpay"}, dl, checker)

	// 首次运行生成 CHANNEL_MISSING。
	out1, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate})
	if err != nil {
		t.Fatalf("first RunBatch: %v", err)
	}
	if out1.Batch.DiffCount != 1 || len(out1.Diffs) != 1 {
		t.Fatalf("first diffs = %+v", out1)
	}

	// 将该差错置为 IGNORED，并写入固定的 handle_action / handle_note / updated_at。
	fixedUpdatedAt := time.Date(2026, 10, 27, 10, 5, 0, 0, shanghai)
	_, err = reconDB.ExecContext(context.Background(), `UPDATE recon_diff
		SET status = 'IGNORED', handle_action = 'REQUERY', handler = 'system',
		    handle_note = 'channel confirmed success', updated_at = ?
		WHERE id = ?`, fixedUpdatedAt, out1.Diffs[0].ID)
	if err != nil {
		t.Fatalf("update existing diff: %v", err)
	}

	// 推进时钟后 force 重跑，同键 CHANNEL_MISSING 应被跳过且指标不增加。
	clk.Advance(10 * time.Minute)
	beforeMetric := reconDiffCounter(t, dal.DiffChannelMissing)

	out2, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate, Force: true})
	if err != nil {
		t.Fatalf("rerun RunBatch: %v", err)
	}
	if out2.Batch.Status != dal.StatusDone || out2.Batch.DiffCount != 1 {
		t.Fatalf("rerun batch = %+v, want DONE diff_count=1", out2.Batch)
	}

	afterMetric := reconDiffCounter(t, dal.DiffChannelMissing)
	if afterMetric != beforeMetric {
		t.Fatalf("pay_recon_diffs_total{CHANNEL_MISSING} increased from %v to %v, want unchanged", beforeMetric, afterMetric)
	}

	var got struct {
		ID           uint64         `db:"id"`
		Status       string         `db:"status"`
		HandleAction sql.NullString `db:"handle_action"`
		HandleNote   sql.NullString `db:"handle_note"`
		UpdatedAt    time.Time      `db:"updated_at"`
	}
	if err := reconDB.Get(&got, `SELECT id, status, handle_action, handle_note, updated_at FROM recon_diff WHERE batch_no = ?`, out2.Batch.BatchNo); err != nil {
		t.Fatalf("query recon_diff: %v", err)
	}
	if got.ID != out1.Diffs[0].ID || got.Status != "IGNORED" ||
		got.HandleAction.String != "REQUERY" || got.HandleNote.String != "channel confirmed success" ||
		!got.UpdatedAt.Equal(fixedUpdatedAt) {
		t.Fatalf("existing diff was modified: %+v (want id=%d updated_at=%v)", got, out1.Diffs[0].ID, fixedUpdatedAt)
	}
}
