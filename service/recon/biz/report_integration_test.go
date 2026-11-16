//go:build integration

package biz_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

func insertReconDiffRow(t *testing.T, db *sqlx.DB, d dal.Diff) uint64 {
	t.Helper()
	res, err := db.ExecContext(context.Background(), `INSERT INTO recon_diff
		(diff_no, batch_no, channel, bill_date, diff_type, txn_type, biz_no,
		 channel_txn_no, local_amount, channel_amount, local_status, channel_status,
		 status, handle_action, handler, handle_note, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		d.DiffNo, d.BatchNo, d.Channel, d.BillDate, d.DiffType, d.TxnType, d.BizNo,
		d.ChannelTxnNo, d.LocalAmount, d.ChannelAmount, d.LocalStatus, d.ChannelStatus,
		d.Status, d.HandleAction, d.Handler, d.HandleNote, d.CreatedAt, d.UpdatedAt,
	)
	if err != nil {
		t.Fatalf("insert recon_diff (%s): %v", d.DiffNo, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId: %v", err)
	}
	if id < 0 {
		t.Fatalf("unexpected negative id %d", id)
	}
	return uint64(id) // #nosec G115 -- 已校验 id >= 0
}

func seedDoneBatch(t *testing.T, db *sqlx.DB, batchNo, billDate string, now time.Time) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `INSERT INTO recon_batch
		(batch_no, channel, bill_date, status, retry_count, started_at, finished_at, created_at, updated_at)
		VALUES (?, 'mockpay', ?, 'DONE', 0, ?, ?, ?, ?)`,
		batchNo, billDate, now, now, now, now,
	)
	if err != nil {
		t.Fatalf("insert recon_batch (%s): %v", batchNo, err)
	}
}

// REC-08 验收 1：报告中的笔数和金额与账单、本地数据的手工统计一致，
// 且按 recon_diff 实时聚合各类差错数与自动处理数，并刷新 pay_recon_pending_diffs。
func TestReconReportCountsAndAmounts(t *testing.T) {
	reconDB, paymentDB := openReconAndPaymentDBs(t)
	clk := &clock.Fake{}
	clk.Set(shanghaiTime(2026, 10, 27, 10, 0, 0))
	ids := newTestIDGen(t, clk)

	billDate := "2026-10-26"
	finishedAt := shanghaiTime(2026, 10, 26, 12, 0, 0)

	// 本地 channel_txn：3 笔 SUCCESS（1000 + 2000 + 3000 = 6000）
	// - PAY1: 1000（与账单一致）
	// - PAY2: 2000（账单金额 2500 -> AMOUNT_MISMATCH）
	// - PAY3: 3000（账单无 -> CHANNEL_MISSING）
	ctn1, ctn2, ctn3 := "CTN001", "CTN002", "CTN003"
	insertChannelTxnFixture(t, paymentDB, "PAY", "P001", &ctn1, 1000, "SUCCESS", &finishedAt)
	insertChannelTxnFixture(t, paymentDB, "PAY", "P002", &ctn2, 2000, "SUCCESS", &finishedAt)
	insertChannelTxnFixture(t, paymentDB, "PAY", "P003", &ctn3, 3000, "SUCCESS", &finishedAt)

	// 账单行（通过 Downloader 写入 recon_channel_bill）：
	// - P001: 1000（匹配）
	// - P002: 2500（AMOUNT_MISMATCH）
	// - P004: 4000（LOCAL_MISSING）
	// 账单总笔数 3，总金额 1000 + 2500 + 4000 = 7500
	dl := &fakeDownloader{fn: func(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (biz.DownloadResult, error) {
		bills := []dal.ChannelBill{
			{BatchNo: batch.BatchNo, Channel: "mockpay", BillDate: billDate, TxnType: "PAY", ChannelTxnNo: "CTN001", BizNo: "P001", Amount: 1000, ChannelStatus: "SUCCESS", ChannelFinishedAt: finishedAt, MatchStatus: dal.BillMatchUnchecked, CreatedAt: clk.Now()},
			{BatchNo: batch.BatchNo, Channel: "mockpay", BillDate: billDate, TxnType: "PAY", ChannelTxnNo: "CTN002", BizNo: "P002", Amount: 2500, ChannelStatus: "SUCCESS", ChannelFinishedAt: finishedAt, MatchStatus: dal.BillMatchUnchecked, CreatedAt: clk.Now()},
			{BatchNo: batch.BatchNo, Channel: "mockpay", BillDate: billDate, TxnType: "PAY", ChannelTxnNo: "CTN004", BizNo: "P004", Amount: 4000, ChannelStatus: "SUCCESS", ChannelFinishedAt: finishedAt, MatchStatus: dal.BillMatchUnchecked, CreatedAt: clk.Now()},
		}
		if err := dal.InsertChannelBillsBatch(ctx, exec, bills); err != nil {
			return biz.DownloadResult{}, err
		}
		return biz.DownloadResult{FileHash: "abc123"}, nil
	}}

	ext := biz.NewExternalChecker(reconDB, paymentDB, ids, clk)
	// 用一个包装 checker 模拟有一条差错被自动处理
	checker := &fakeChecker{fn: func(ctx context.Context, batch *dal.Batch) (biz.CheckResult, error) {
		res, err := ext.Check(ctx, batch)
		if err != nil {
			return res, err
		}
		// 把 P003 的 CHANNEL_MISSING 标为已自动处理（REQUERY，保持 PENDING）
		if _, err := reconDB.ExecContext(ctx,
			`UPDATE recon_diff SET handle_action = 'REQUERY', handler = 'system', handle_note = 'processing' WHERE batch_no = ? AND biz_no = 'P003'`,
			batch.BatchNo); err != nil {
			return res, err
		}
		return res, nil
	}}

	svc := biz.NewRecon(reconDB, ids, clk, []string{"mockpay"}, dl, checker)
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{
		Channel:  "mockpay",
		BillDate: billDate,
	})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}

	// 手工统计校验：
	// ChannelCount=3, ChannelAmount=7500, LocalCount=3, LocalAmount=6000, MatchedCount=1, DiffCount=3
	if out.Batch.ChannelCount != 3 || out.Batch.ChannelAmount != 7500 {
		t.Fatalf("channel stats = (%d, %d), want (3, 7500)", out.Batch.ChannelCount, out.Batch.ChannelAmount)
	}
	if out.Batch.LocalCount != 3 || out.Batch.LocalAmount != 6000 {
		t.Fatalf("local stats = (%d, %d), want (3, 6000)", out.Batch.LocalCount, out.Batch.LocalAmount)
	}
	if out.Batch.MatchedCount != 1 || out.Batch.DiffCount != 3 {
		t.Fatalf("matched/diff = (%d, %d), want (1, 3)", out.Batch.MatchedCount, out.Batch.DiffCount)
	}
	if len(out.Diffs) != 3 {
		t.Fatalf("len(Diffs) = %d, want 3", len(out.Diffs))
	}
	if out.DiffCountsByType[dal.DiffAmountMismatch] != 1 ||
		out.DiffCountsByType[dal.DiffLocalMissing] != 1 ||
		out.DiffCountsByType[dal.DiffChannelMissing] != 1 {
		t.Fatalf("DiffCountsByType = %+v, want 1 each for AMOUNT_MISMATCH/LOCAL_MISSING/CHANNEL_MISSING", out.DiffCountsByType)
	}
	if out.AutoHandledCount != 1 {
		t.Fatalf("AutoHandledCount = %d, want 1", out.AutoHandledCount)
	}
	if got := gaugeValue(t, "pay_recon_pending_diffs", map[string]string{"diff_type": dal.DiffAmountMismatch}); got != 1 {
		t.Fatalf("pay_recon_pending_diffs{AMOUNT_MISMATCH} = %v, want 1", got)
	}
	if got := gaugeValue(t, "pay_recon_pending_diffs", map[string]string{"diff_type": dal.DiffChannelMissing}); got != 1 {
		t.Fatalf("pay_recon_pending_diffs{CHANNEL_MISSING} = %v, want 1", got)
	}
	if got := gaugeValue(t, "pay_recon_pending_diffs", map[string]string{"diff_type": dal.DiffLocalMissing}); got != 1 {
		t.Fatalf("pay_recon_pending_diffs{LOCAL_MISSING} = %v, want 1", got)
	}
}

// REC-08 验收 2：“等待人工处理”筛选与顺序：
// D-9 handle_action=REQUERY (PENDING)、D-3 channel_status=FAILED 的 CHANNEL_MISSING (PENDING)、
// D-3 窗口内 handle_action=REQUERY 且 channel_status 为空、D-9 handle_action 为空的 LOCAL_MISSING。
// 报告“等待人工处理”恰好列出前两条，manual_total = 2，manual_diffs 依次为 D-9、D-3。
func TestReconReportManualDiffsFilterAndOrder(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	// D = 2026-10-27 -> D-8 = 2026-10-19, D-9 = 2026-10-18, D-3 = 2026-10-24
	now := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(now)
	ids := newTestIDGen(t, clk)

	seedDoneBatch(t, db, "RB_D9", "2026-10-18", now)
	seedDoneBatch(t, db, "RB_D3", "2026-10-24", now)

	requery := "REQUERY"
	failed := "FAILED"
	sys := "system"

	// 1) D-9, handle_action=REQUERY, PENDING -> 应入选（第 1 条）
	insertReconDiffRow(t, db, dal.Diff{
		DiffNo: "RD_M1", BatchNo: "RB_D9", Channel: "mockpay", BillDate: "2026-10-18",
		DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "P_M1",
		Status: dal.DiffStatusPending, HandleAction: &requery, Handler: &sys,
		CreatedAt: now, UpdatedAt: now,
	})
	// 2) D-3, CHANNEL_MISSING, channel_status=FAILED, PENDING -> 应入选（第 2 条）
	insertReconDiffRow(t, db, dal.Diff{
		DiffNo: "RD_M2", BatchNo: "RB_D3", Channel: "mockpay", BillDate: "2026-10-24",
		DiffType: dal.DiffChannelMissing, TxnType: "PAY", BizNo: "P_M2",
		ChannelStatus: &failed, Status: dal.DiffStatusPending, HandleAction: &requery, Handler: &sys,
		CreatedAt: now, UpdatedAt: now,
	})
	// 3) D-3 窗口内, handle_action=REQUERY, channel_status=NULL, PENDING -> 不入选
	insertReconDiffRow(t, db, dal.Diff{
		DiffNo: "RD_SKIP1", BatchNo: "RB_D3", Channel: "mockpay", BillDate: "2026-10-24",
		DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "P_SKIP1",
		Status: dal.DiffStatusPending, HandleAction: &requery, Handler: &sys,
		CreatedAt: now, UpdatedAt: now,
	})
	// 4) D-9, LOCAL_MISSING, handle_action=NULL, PENDING -> 不入选
	insertReconDiffRow(t, db, dal.Diff{
		DiffNo: "RD_SKIP2", BatchNo: "RB_D9", Channel: "mockpay", BillDate: "2026-10-18",
		DiffType: dal.DiffLocalMissing, TxnType: "PAY", BizNo: "P_SKIP2",
		Status:    dal.DiffStatusPending,
		CreatedAt: now, UpdatedAt: now,
	})

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, &fakeDownloader{}, &fakeChecker{})
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{
		Channel:  "mockpay",
		BillDate: "2026-10-26",
	})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}

	if out.ManualTotal != 2 {
		t.Fatalf("ManualTotal = %d, want 2", out.ManualTotal)
	}
	if len(out.ManualDiffs) != 2 {
		t.Fatalf("len(ManualDiffs) = %d, want 2", len(out.ManualDiffs))
	}
	if out.ManualDiffs[0].DiffNo != "RD_M1" || out.ManualDiffs[1].DiffNo != "RD_M2" {
		t.Fatalf("ManualDiffs order = [%s, %s], want [RD_M1, RD_M2]", out.ManualDiffs[0].DiffNo, out.ManualDiffs[1].DiffNo)
	}
}

// REC-08 验收 3：“等待人工处理”的排序与截断：
// 构造 101 条符合条件的差错，其中 3 条 bill_date 相同。
// manual_total = 101，manual_diffs 有 100 条，按 bill_date 升序；同一 bill_date 内按 id 升序；被截掉的是排序最后的那条。
func TestReconReportManualDiffsSortAndTruncate(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	// D = 2026-10-27 -> D-8 = 2026-10-19
	now := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(now)
	ids := newTestIDGen(t, clk)

	baseDate := time.Date(2026, 6, 1, 0, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*3600))
	requery := "REQUERY"
	sys := "system"

	type inserted struct {
		id       uint64
		diffNo   string
		billDate string
	}
	var rows []inserted

	// 先插入 98 条不同 bill_date 的（2026-06-01 .. 2026-09-06，均早于 2026-10-19），故意倒序插入以验证按 bill_date 升序
	for i := 97; i >= 0; i-- {
		bd := baseDate.AddDate(0, 0, i).Format("2006-01-02")
		bno := fmt.Sprintf("RB_S_%03d", i)
		seedDoneBatch(t, db, bno, bd, now)
		dno := fmt.Sprintf("RD_S_%03d", i)
		id := insertReconDiffRow(t, db, dal.Diff{
			DiffNo: dno, BatchNo: bno, Channel: "mockpay", BillDate: bd,
			DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: fmt.Sprintf("P_S_%03d", i),
			Status: dal.DiffStatusPending, HandleAction: &requery, Handler: &sys,
			CreatedAt: now, UpdatedAt: now,
		})
		rows = append(rows, inserted{id: id, diffNo: dno, billDate: bd})
	}

	// 再插入 3 条相同 bill_date（2026-09-10，排序在最后），id 依次递增
	sameDate := "2026-09-10"
	seedDoneBatch(t, db, "RB_SAME", sameDate, now)
	var sameIDs [3]uint64
	for j := 0; j < 3; j++ {
		dno := fmt.Sprintf("RD_SAME_%d", j)
		id := insertReconDiffRow(t, db, dal.Diff{
			DiffNo: dno, BatchNo: "RB_SAME", Channel: "mockpay", BillDate: sameDate,
			DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: fmt.Sprintf("P_SAME_%d", j),
			Status: dal.DiffStatusPending, HandleAction: &requery, Handler: &sys,
			CreatedAt: now, UpdatedAt: now,
		})
		sameIDs[j] = id
	}
	_ = rows

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, &fakeDownloader{}, &fakeChecker{})
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{
		Channel:  "mockpay",
		BillDate: "2026-10-26",
	})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}

	if out.ManualTotal != 101 {
		t.Fatalf("ManualTotal = %d, want 101", out.ManualTotal)
	}
	if len(out.ManualDiffs) != 100 {
		t.Fatalf("len(ManualDiffs) = %d, want 100", len(out.ManualDiffs))
	}

	// 校验严格按 (bill_date ASC, id ASC) 排序
	for i := 1; i < len(out.ManualDiffs); i++ {
		prev := out.ManualDiffs[i-1]
		curr := out.ManualDiffs[i]
		if prev.BillDate > curr.BillDate || (prev.BillDate == curr.BillDate && prev.ID >= curr.ID) {
			t.Fatalf("unsorted at %d: prev=(%s,%d) curr=(%s,%d)", i, prev.BillDate, prev.ID, curr.BillDate, curr.ID)
		}
	}

	// 最后两条应为 sameDate 的前两条，第 3 条（sameIDs[2]）被截掉
	if out.ManualDiffs[98].ID != sameIDs[0] || out.ManualDiffs[99].ID != sameIDs[1] {
		t.Fatalf("last two IDs = (%d, %d), want (%d, %d)", out.ManualDiffs[98].ID, out.ManualDiffs[99].ID, sameIDs[0], sameIDs[1])
	}
	for _, d := range out.ManualDiffs {
		if d.ID == sameIDs[2] {
			t.Fatalf("truncated last item id=%d unexpectedly present", sameIDs[2])
		}
	}
}
