//go:build integration

package biz_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

func insertDiffFixture(t *testing.T, db *sqlx.DB, d dal.Diff) uint64 {
	t.Helper()
	res, err := db.ExecContext(context.Background(), `INSERT INTO recon_diff
		(diff_no, batch_no, channel, bill_date, diff_type, txn_type, biz_no,
		 local_amount, local_status, channel_status, status, handle_action, handler, handle_note, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		d.DiffNo, d.BatchNo, d.Channel, d.BillDate, d.DiffType, d.TxnType, d.BizNo,
		d.LocalAmount, d.LocalStatus, d.ChannelStatus, d.Status, d.HandleAction, d.Handler, d.HandleNote, d.CreatedAt, d.UpdatedAt)
	if err != nil {
		t.Fatalf("insert recon_diff %s: %v", d.DiffNo, err)
	}
	id, err := res.LastInsertId()
	if err != nil || id < 0 {
		t.Fatalf("LastInsertId: id=%d err=%v", id, err)
	}
	return uint64(id) //nolint:gosec // checked id >= 0 above
}

func getDiffByIDIntegration(t *testing.T, db *sqlx.DB, id uint64) dal.Diff {
	t.Helper()
	d, err := dal.GetDiffByID(context.Background(), db, id)
	if err != nil {
		t.Fatalf("GetDiffByID(%d): %v", id, err)
	}
	return d
}

// REC-06 验收 1 & 3：自动处理失败写 handle_action=REQUERY；force 重跑保留该行（id 不变）；后续批次运行恢复后置 RESOLVED。
func TestAutoHandleIntegration_StatusMismatchAndForceRerun(t *testing.T) {
	reconDB, paymentDB, accDB0, accDB1 := openReconPaymentAndAccountDBs(t)
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))
	ids, _ := idgen.New(4, clk)

	now := clk.Now()
	billDate := "2026-10-25"
	finTime := time.Date(2026, 10, 25, 12, 0, 0, 0, sh)

	// 创建已 DOWNLOADED 的批次，模拟 bill 有一条 SUCCESS，local channel_txn 是 PROCESSING
	batchID, err := dal.InsertBatch(context.Background(), reconDB, "RB_AH_01", "mockpay", billDate, now)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	_, _ = dal.MarkDownloadedCAS(context.Background(), reconDB, batchID, "hash1", now, now)
	_, err = reconDB.ExecContext(context.Background(), `INSERT INTO recon_channel_bill
		(batch_no, channel, bill_date, txn_type, channel_txn_no, biz_no, amount, channel_status, channel_finished_at, match_status)
		VALUES ('RB_AH_01', 'mockpay', ?, 'PAY', 'MC_AH_01', 'PA_AH_01', 1000, 'SUCCESS', ?, 'UNCHECKED')`,
		billDate, finTime)
	if err != nil {
		t.Fatalf("insert bill: %v", err)
	}
	_, err = paymentDB.ExecContext(context.Background(), `INSERT INTO channel_txn
		(channel, txn_type, biz_no, channel_txn_no, amount, status, channel_finished_at)
		VALUES ('mockpay', 'PAY', 'PA_AH_01', 'MC_AH_01', 1000, 'PROCESSING', ?)`, finTime)
	if err != nil {
		t.Fatalf("insert channel_txn: %v", err)
	}

	failSync := true
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			if failSync {
				return biz.SyncChannelTxnResult{}, errors.New("mock channel error")
			}
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: true}, nil
		},
	}

	ext := biz.NewExternalChecker(reconDB, paymentDB, ids, clk)
	ah := biz.NewAutoHandler(reconDB, paymentDB, accDB0, accDB1, payCli, clk)
	checker := biz.NewCompositeChecker(ext, ah)

	// 推进 CHECKING 并执行
	_, _ = dal.MarkCheckingCAS(context.Background(), reconDB, batchID, now, now)
	b, _ := dal.GetBatchByID(context.Background(), reconDB, batchID)
	if _, err := checker.Check(context.Background(), &b); err != nil {
		t.Fatalf("Check 1 err: %v", err)
	}
	diffs, _ := dal.ListBatchDiffs(context.Background(), reconDB, "RB_AH_01", 10)
	if len(diffs) != 1 {
		t.Fatalf("diffs len = %d, want 1", len(diffs))
	}
	origID := diffs[0].ID
	if diffs[0].Status != dal.DiffStatusPending || diffs[0].HandleAction == nil || *diffs[0].HandleAction != "REQUERY" || diffs[0].HandleNote == nil || !strings.Contains(*diffs[0].HandleNote, "mock channel error") {
		t.Fatalf("unexpected diff after failure: %+v", diffs[0])
	}

	// force 重跑（ResetForRerunTx），保留 handle_action 非空的差错且 id 不变
	_, _ = dal.MarkDoneCAS(context.Background(), reconDB, batchID, now, dal.CheckStats{DiffCount: 1}, now)
	won, err := dal.ResetForRerunTx(context.Background(), reconDB, batchID, "RB_AH_01", dal.StatusDone, now, now)
	if err != nil || !won {
		t.Fatalf("ResetForRerunTx: won=%v err=%v", won, err)
	}
	kept := getDiffByIDIntegration(t, reconDB, origID)
	if kept.ID != origID || kept.Status != dal.DiffStatusPending {
		t.Fatalf("kept diff changed: %+v", kept)
	}

	// 恢复后，在另一账期批次运行，窗口内 [D-8, D-2] 自动重试并置为 RESOLVED
	failSync = false
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_OTHER", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check 2 err: %v", err)
	}
	resolved := getDiffByIDIntegration(t, reconDB, origID)
	if resolved.Status != dal.DiffStatusResolved || resolved.ResolvedAt == nil {
		t.Fatalf("expected RESOLVED, got: %+v", resolved)
	}
}

// REC-06 验收 2 & 4：LEDGER_MISSING 重放失败保持 PENDING，下批次复核再次重放，入账后置 RESOLVED。
func TestAutoHandleIntegration_LedgerMissingReplayAndRecheck(t *testing.T) {
	reconDB, paymentDB, accDB0, accDB1 := openReconPaymentAndAccountDBs(t)
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))

	now := clk.Now()
	billDate := "2026-10-25"
	paidAt := time.Date(2026, 10, 25, 11, 0, 0, 0, sh)
	expireAt := paidAt.Add(time.Hour)
	merchantID := "M002001"
	sIdx, serr := shard.Of(merchantID)
	if serr != nil {
		t.Fatalf("shard.Of: %v", serr)
	}
	accDB := accDB0
	if sIdx == 1 {
		accDB = accDB1
	}

	insertPaymentOrderFixture(t, paymentDB, "PA_LM_01", merchantID, "OUT_LM_01", 3000, "SUCCESS", &paidAt, expireAt)
	id := insertDiffFixture(t, reconDB, dal.Diff{
		DiffNo: "RD_LM_01", BatchNo: "RB_LM_01", Channel: "mockpay", BillDate: billDate,
		DiffType: dal.DiffLedgerMissing, TxnType: "PAY", BizNo: "PA_LM_01",
		LocalAmount: i64Ptr(3000), LocalStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending,
		CreatedAt: now, UpdatedAt: now,
	})

	failReplay := true
	payCli := &fakePaymentRPCClient{
		replayFn: func(bizNo, eventType string) (biz.ReplayEventResult, error) {
			if failReplay {
				return biz.ReplayEventResult{}, errors.New("kafka error")
			}
			return biz.ReplayEventResult{EventID: "EV_LM_01"}, nil
		},
	}
	ah := biz.NewAutoHandler(reconDB, paymentDB, accDB0, accDB1, payCli, clk)

	// Run 1: ReplayEvent 失败仍写 handle_action=REPLAY
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_LM_01", Channel: "mockpay", BillDate: billDate}); err != nil {
		t.Fatalf("Check 1: %v", err)
	}
	d := getDiffByIDIntegration(t, reconDB, id)
	if d.Status != dal.DiffStatusPending || d.HandleAction == nil || *d.HandleAction != "REPLAY" {
		t.Fatalf("unexpected diff 1: %+v", d)
	}

	// Run 2: 复核仍无凭证，再次重放（成功）
	failReplay = false
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_LM_02", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check 2: %v", err)
	}
	if len(payCli.replayCalls) != 2 {
		t.Fatalf("replayCalls = %d, want 2", len(payCli.replayCalls))
	}

	// 写入入账凭证
	insertLedgerTxnFixture(t, accDB, "TX_LM_01", "PAY", "PA_LM_01", 3000)

	// Run 3: 复核通过 → RESOLVED
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_LM_03", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check 3: %v", err)
	}
	d = getDiffByIDIntegration(t, reconDB, id)
	if d.Status != dal.DiffStatusResolved || d.ResolvedAt == nil {
		t.Fatalf("expected RESOLVED, got: %+v", d)
	}
}

// REC-06 验收 5：自动重试窗口 [D-8, D-2] 在 MySQL idx_status_bill_date 查询下的行为。
func TestAutoHandleIntegration_RetryWindow(t *testing.T) {
	reconDB, paymentDB, accDB0, accDB1 := openReconPaymentAndAccountDBs(t)
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))
	now := clk.Now()

	idD1 := insertDiffFixture(t, reconDB, dal.Diff{DiffNo: "RD_W1", BatchNo: "RB_W1", Channel: "mockpay", BillDate: "2026-10-26", DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_W1", LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending, HandleAction: strPtr("REQUERY"), CreatedAt: now, UpdatedAt: now})
	idD2 := insertDiffFixture(t, reconDB, dal.Diff{DiffNo: "RD_W2", BatchNo: "RB_W2", Channel: "mockpay", BillDate: "2026-10-25", DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_W2", LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending, HandleAction: strPtr("REQUERY"), CreatedAt: now, UpdatedAt: now})
	idD8 := insertDiffFixture(t, reconDB, dal.Diff{DiffNo: "RD_W8", BatchNo: "RB_W8", Channel: "mockpay", BillDate: "2026-10-19", DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_W8", LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending, HandleAction: strPtr("REQUERY"), CreatedAt: now, UpdatedAt: now})
	idD9 := insertDiffFixture(t, reconDB, dal.Diff{DiffNo: "RD_W9", BatchNo: "RB_W9", Channel: "mockpay", BillDate: "2026-10-18", DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_W9", LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending, HandleAction: strPtr("REQUERY"), CreatedAt: now, UpdatedAt: now})

	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: true}, nil
		},
	}
	ah := biz.NewAutoHandler(reconDB, paymentDB, accDB0, accDB1, payCli, clk)
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_CUR", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check err: %v", err)
	}
	if getDiffByIDIntegration(t, reconDB, idD1).Status != dal.DiffStatusPending ||
		getDiffByIDIntegration(t, reconDB, idD9).Status != dal.DiffStatusPending {
		t.Fatal("out-of-window diffs should remain PENDING")
	}
	if getDiffByIDIntegration(t, reconDB, idD2).Status != dal.DiffStatusResolved ||
		getDiffByIDIntegration(t, reconDB, idD8).Status != dal.DiffStatusResolved {
		t.Fatal("in-window diffs should be RESOLVED")
	}
}

// REC-06 验收 6：严重不一致在 MySQL 写入 channel_status 后，后续运行不再查询。
func TestAutoHandleIntegration_ChannelMissingSevereInconsistent(t *testing.T) {
	reconDB, paymentDB, accDB0, accDB1 := openReconPaymentAndAccountDBs(t)
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))
	now := clk.Now()

	id := insertDiffFixture(t, reconDB, dal.Diff{
		DiffNo: "RD_SEV_1", BatchNo: "RB_SEV_1", Channel: "mockpay", BillDate: "2026-10-25",
		DiffType: dal.DiffChannelMissing, TxnType: "PAY", BizNo: "PA_SEV_1",
		LocalStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending, CreatedAt: now, UpdatedAt: now,
	})
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "FAILED", Changed: false}, nil
		},
	}
	ah := biz.NewAutoHandler(reconDB, paymentDB, accDB0, accDB1, payCli, clk)

	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_SEV_1", Channel: "mockpay", BillDate: "2026-10-25"}); err != nil {
		t.Fatalf("Check 1: %v", err)
	}
	d := getDiffByIDIntegration(t, reconDB, id)
	if d.Status != dal.DiffStatusPending || d.ChannelStatus == nil || *d.ChannelStatus != "FAILED" {
		t.Fatalf("expected PENDING + channel_status=FAILED, got: %+v", d)
	}

	payCli.syncCalls = nil
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_SEV_2", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check 2: %v", err)
	}
	if len(payCli.syncCalls) != 0 {
		t.Fatalf("severe inconsistent diff should not be retried, got calls: %v", payCli.syncCalls)
	}
}

// REC-06 验收 7 & 8：MySQL 下的并发 CAS 与删除行处理。
func TestAutoHandleIntegration_ConcurrentCASAndDeletedRow(t *testing.T) {
	reconDB, paymentDB, accDB0, accDB1 := openReconPaymentAndAccountDBs(t)
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))
	now := clk.Now()

	id := insertDiffFixture(t, reconDB, dal.Diff{
		DiffNo: "RD_CC_1", BatchNo: "RB_CC_1", Channel: "mockpay", BillDate: "2026-10-26",
		DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_CC_1",
		LocalStatus: strPtr("PROCESSING"), ChannelStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending,
		CreatedAt: now, UpdatedAt: now,
	})
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: true}, nil
		},
	}

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ah := biz.NewAutoHandler(reconDB, paymentDB, accDB0, accDB1, payCli, clk)
			_, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_CC_1", Channel: "mockpay", BillDate: "2026-10-26"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Check err: %v", err)
		}
	}
	d := getDiffByIDIntegration(t, reconDB, id)
	if d.Status != dal.DiffStatusResolved {
		t.Fatalf("expected RESOLVED, got: %+v", d)
	}

	// 验证对已删除行调用 ResolveDiffCAS 不会报错，GetDiffByID 返回 ErrNotFound
	won, err := dal.ResolveDiffCAS(context.Background(), reconDB, 99999999, dal.DiffStatusResolved, "REQUERY", "system", "note", now, now)
	if err != nil || won {
		t.Fatalf("expected (false, nil) for missing row, got (%v, %v)", won, err)
	}
}
