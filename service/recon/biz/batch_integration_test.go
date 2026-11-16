//go:build integration

package biz_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
	"github.com/yym108/pay-system/service/recon/job"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_ = testkit.Shutdown(ctx)
	cancel()
	os.Exit(code)
}

func openReconTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mysql, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatalf("testkit.Ready: %v", err)
	}
	schema, err := mysql.OpenSchema(ctx, "recon")
	if err != nil {
		t.Fatalf("OpenSchema(recon): %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer ccancel()
		_ = schema.Close(cctx)
	})
	return schema.DB
}

func newTestIDGen(t *testing.T, clk clock.Clock) *idgen.Generator {
	t.Helper()
	g, err := idgen.New(4, clk)
	if err != nil {
		t.Fatalf("idgen.New: %v", err)
	}
	return g
}

type fakeDownloader struct {
	fn func(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (biz.DownloadResult, error)
}

func (f *fakeDownloader) DownloadAndImport(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (biz.DownloadResult, error) {
	if f.fn == nil {
		return biz.DownloadResult{FileHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, nil
	}
	return f.fn(ctx, exec, batch)
}

type fakeChecker struct {
	fn func(ctx context.Context, batch *dal.Batch) (biz.CheckResult, error)
}

func (f *fakeChecker) Check(ctx context.Context, batch *dal.Batch) (biz.CheckResult, error) {
	if f.fn == nil {
		return biz.CheckResult{}, nil
	}
	return f.fn(ctx, batch)
}

// REC-01 验收 1：同一账期并发触发两次，只有一次执行。
func TestRunBatchConcurrentSameDateOnlyOnce(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	clk.Set(shanghaiTime(2026, 10, 27, 10, 0, 0))
	ids := newTestIDGen(t, clk)

	var downloadCalls atomic.Int32
	var checkCalls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	dl := &fakeDownloader{fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
		if downloadCalls.Add(1) == 1 {
			close(started)
			<-release
		}
		return biz.DownloadResult{FileHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, nil
	}}
	ck := &fakeChecker{fn: func(_ context.Context, _ *dal.Batch) (biz.CheckResult, error) {
		checkCalls.Add(1)
		return biz.CheckResult{ChannelCount: 2, ChannelAmount: 200, LocalCount: 2, LocalAmount: 200, MatchedCount: 2}, nil
	}}

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, ck)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	outs := make([]*biz.RunBatchOutput, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		outs[0], errs[0] = svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	}()

	<-started
	wg.Add(1)
	go func() {
		defer wg.Done()
		outs[1], errs[1] = svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	}()

	// 给第二个调用一点时间读取 INIT 状态并返回，再放行第一个调用
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d err: %v", i, err)
		}
	}
	if downloadCalls.Load() != 1 || checkCalls.Load() != 1 {
		t.Fatalf("calls download=%d check=%d, want 1/1", downloadCalls.Load(), checkCalls.Load())
	}
	if outs[0].Batch.Status != dal.StatusDone {
		t.Fatalf("first status=%s, want DONE", outs[0].Batch.Status)
	}
	if outs[1].Batch.Status != dal.StatusInit {
		t.Fatalf("second status=%s, want INIT", outs[1].Batch.Status)
	}
}

// REC-01 验收 2：force 不打断执行中的批次；满 30 分钟时按第 5 条接管，而不是按第 3 条重跑。
func TestRunBatchForceDoesNotInterruptInProgress(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	for _, status := range []string{dal.StatusInit, dal.StatusDownloaded, dal.StatusChecking} {
		t.Run("under_30m_"+status, func(t *testing.T) {
			billDate := map[string]string{
				dal.StatusInit:       "2026-10-21",
				dal.StatusDownloaded: "2026-10-22",
				dal.StatusChecking:   "2026-10-23",
			}[status]
			clk.Set(t0)
			batchNo, err := ids.NewNo(context.Background(), "RB")
			if err != nil {
				t.Fatal(err)
			}
			var startedAt sql.NullTime
			if status == dal.StatusChecking {
				startedAt = sql.NullTime{Time: t0, Valid: true}
			}
			_, err = db.ExecContext(context.Background(), `INSERT INTO recon_batch (batch_no, channel, bill_date, status, retry_count, started_at, created_at, updated_at)
				VALUES (?, 'mockpay', ?, ?, 2, ?, ?, ?)`, batchNo, billDate, status, startedAt, t0, t0)
			if err != nil {
				t.Fatal(err)
			}
			// 插入一条账单和差错，验证未被删除
			_, err = db.ExecContext(context.Background(), `INSERT INTO recon_channel_bill (batch_no, channel, bill_date, txn_type, channel_txn_no, biz_no, amount, channel_status, channel_finished_at)
				VALUES (?, 'mockpay', ?, 'PAY', 'CH1', 'P1', 100, 'SUCCESS', ?)`, batchNo, billDate, t0)
			if err != nil {
				t.Fatal(err)
			}

			var dlCalls, ckCalls atomic.Int32
			svc := biz.NewRecon(db, ids, clk, []string{"mockpay"},
				&fakeDownloader{fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
					dlCalls.Add(1)
					return biz.DownloadResult{}, nil
				}},
				&fakeChecker{fn: func(_ context.Context, _ *dal.Batch) (biz.CheckResult, error) {
					ckCalls.Add(1)
					return biz.CheckResult{}, nil
				}},
			)

			// 推进 29 分钟，force = true 不打断
			clk.Advance(29 * time.Minute)
			out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate, Force: true})
			if err != nil {
				t.Fatal(err)
			}
			if out.Batch.Status != status || dlCalls.Load() != 0 || ckCalls.Load() != 0 {
				t.Fatalf("under 30m: status=%s dl=%d ck=%d", out.Batch.Status, dlCalls.Load(), ckCalls.Load())
			}

			// 推进到满 30 分钟，force = true 按第 5 条接管（DOWNLOADED 不重新下载，retry_count 不归 0）
			clk.Advance(1 * time.Minute)
			out, err = svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate, Force: true})
			if err != nil {
				t.Fatal(err)
			}
			if out.Batch.Status != dal.StatusDone {
				t.Fatalf("at 30m: status=%s, want DONE", out.Batch.Status)
			}
			if status == dal.StatusDownloaded && dlCalls.Load() != 0 {
				t.Fatalf("DOWNLOADED takeover called downloader %d times, want 0", dlCalls.Load())
			}
			var retryCount int
			if err := db.QueryRowContext(context.Background(), `SELECT retry_count FROM recon_batch WHERE batch_no = ?`, batchNo).Scan(&retryCount); err != nil {
				t.Fatal(err)
			}
			if retryCount != 2 {
				t.Fatalf("retry_count=%d, want 2 (not reset to 0)", retryCount)
			}
		})
	}
}

// REC-01 验收 3 & 4：重跑后差错结果与首次一致，已处理的差错不丢失；重跑不触发 1062、已自动处理的差错保留。
func TestRunBatchRerunNo1062AndKeepsAutoHandled(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	dl := &fakeDownloader{fn: func(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (biz.DownloadResult, error) {
		_, err := exec.ExecContext(ctx, `INSERT INTO recon_channel_bill
			(batch_no, channel, bill_date, txn_type, channel_txn_no, biz_no, amount, channel_status, channel_finished_at)
			VALUES (?, ?, ?, 'PAY', 'CH_LOCAL_MISS', 'P_LOCAL_MISS', 100, 'SUCCESS', ?)`,
			batch.BatchNo, batch.Channel, batch.BillDate, t0)
		if err != nil {
			return biz.DownloadResult{}, err
		}
		return biz.DownloadResult{FileHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, nil
	}}

	insertDiff := func(batch *dal.Batch, diffType, bizNo string) error {
		dNo, err := ids.NewNo(context.Background(), "RD")
		if err != nil {
			return err
		}
		_, err = db.ExecContext(context.Background(), `INSERT INTO recon_diff
			(diff_no, batch_no, channel, bill_date, diff_type, txn_type, biz_no, status, handler, handle_note, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'PAY', ?, 'PENDING', '', '', ?, ?)
			ON DUPLICATE KEY UPDATE id = id`,
			dNo, batch.BatchNo, batch.Channel, batch.BillDate, diffType, bizNo, t0, t0)
		return err
	}

	ck := &fakeChecker{fn: func(_ context.Context, batch *dal.Batch) (biz.CheckResult, error) {
		for _, item := range []struct{ dt, bizNo string }{
			{"CHANNEL_MISSING", "P_CH_MISS"},
			{"STATUS_MISMATCH", "P_STATUS"},
			{"LEDGER_MISSING", "P_LEDGER"},
			{"LOCAL_MISSING", "P_LOCAL_MISS"},
		} {
			if err := insertDiff(batch, item.dt, item.bizNo); err != nil {
				return biz.CheckResult{}, err
			}
		}
		return biz.CheckResult{ChannelCount: 1, ChannelAmount: 100, LocalCount: 3, LocalAmount: 300, MatchedCount: 0}, nil
	}}

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, ck)
	out1, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	if err != nil {
		t.Fatal(err)
	}
	if out1.Batch.DiffCount != 4 {
		t.Fatalf("first diff_count=%d, want 4", out1.Batch.DiffCount)
	}

	// 模拟前三条分别变成 IGNORED、RESOLVED、PENDING+REPLAY
	_, err = db.ExecContext(context.Background(), `UPDATE recon_diff SET status = 'IGNORED', handler = 'system', handle_note = 'ignored note' WHERE batch_no = ? AND biz_no = 'P_CH_MISS'`, out1.Batch.BatchNo)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(context.Background(), `UPDATE recon_diff SET status = 'RESOLVED', handle_action = 'REQUERY', handler = 'system', handle_note = 'resolved note' WHERE batch_no = ? AND biz_no = 'P_STATUS'`, out1.Batch.BatchNo)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(context.Background(), `UPDATE recon_diff SET handle_action = 'REPLAY', handler = 'system', handle_note = 'replayed note' WHERE batch_no = ? AND biz_no = 'P_LEDGER'`, out1.Batch.BatchNo)
	if err != nil {
		t.Fatal(err)
	}

	type diffSnap struct {
		ID           uint64         `db:"id"`
		BizNo        string         `db:"biz_no"`
		Status       string         `db:"status"`
		HandleAction sql.NullString `db:"handle_action"`
		HandleNote   sql.NullString `db:"handle_note"`
	}
	var before []diffSnap
	if err := db.SelectContext(context.Background(), &before, `SELECT id, biz_no, status, handle_action, handle_note FROM recon_diff WHERE batch_no = ? ORDER BY biz_no`, out1.Batch.BatchNo); err != nil {
		t.Fatal(err)
	}

	clk.Advance(time.Minute)
	out2, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26", Force: true})
	if err != nil {
		t.Fatalf("force rerun failed: %v", err)
	}
	if out2.Batch.Status != dal.StatusDone || out2.Batch.DiffCount != 4 {
		t.Fatalf("rerun status=%s diff_count=%d", out2.Batch.Status, out2.Batch.DiffCount)
	}

	var after []diffSnap
	if err := db.SelectContext(context.Background(), &after, `SELECT id, biz_no, status, handle_action, handle_note FROM recon_diff WHERE batch_no = ? ORDER BY biz_no`, out1.Batch.BatchNo); err != nil {
		t.Fatal(err)
	}
	if len(before) != 4 || len(after) != 4 {
		t.Fatalf("len before=%d after=%d", len(before), len(after))
	}
	for i := range before {
		if before[i].BizNo == "P_LOCAL_MISS" {
			if after[i].ID == before[i].ID {
				t.Fatalf("P_LOCAL_MISS should have been deleted and recreated with new id")
			}
		} else if after[i] != before[i] {
			t.Fatalf("kept diff %s changed: before=%+v after=%+v", before[i].BizNo, before[i], after[i])
		}
	}
}

// REC-01 验收 5：中断后接管与一次性运行一致。
func TestRunBatchTakeoverMatchesFreshRun(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	dl := &fakeDownloader{}
	ck := &fakeChecker{fn: func(ctx context.Context, batch *dal.Batch) (biz.CheckResult, error) {
		for _, item := range []struct{ dt, bizNo, status, action string }{
			{"LEDGER_MISSING", "P1", "PENDING", "REPLAY"},
			{"CHANNEL_MISSING", "P2", "IGNORED", ""},
			{"LOCAL_MISSING", "P3", "PENDING", ""},
		} {
			dNo, err := ids.NewNo(ctx, "RD")
			if err != nil {
				return biz.CheckResult{}, err
			}
			var act sql.NullString
			if item.action != "" {
				act = sql.NullString{String: item.action, Valid: true}
			}
			_, err = db.ExecContext(ctx, `INSERT INTO recon_diff
				(diff_no, batch_no, channel, bill_date, diff_type, txn_type, biz_no, status, handle_action, handler, handle_note, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, 'PAY', ?, ?, ?, '', '', ?, ?)
				ON DUPLICATE KEY UPDATE id = id`,
				dNo, batch.BatchNo, batch.Channel, batch.BillDate, item.dt, item.bizNo, item.status, act, t0, t0)
			if err != nil {
				return biz.CheckResult{}, err
			}
		}
		return biz.CheckResult{ChannelCount: 10, ChannelAmount: 1000, LocalCount: 9, LocalAmount: 900, MatchedCount: 7}, nil
	}}

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, ck)
	fresh, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-25"})
	if err != nil {
		t.Fatal(err)
	}

	// 构造第二份中断的 CHECKING 批次
	b2No, _ := ids.NewNo(context.Background(), "RB")
	staleAt := t0.Add(-31 * time.Minute)
	_, err = db.ExecContext(context.Background(), `INSERT INTO recon_batch (batch_no, channel, bill_date, status, started_at, created_at, updated_at)
		VALUES (?, 'mockpay', '2026-10-26', 'CHECKING', ?, ?, ?)`, b2No, staleAt, staleAt, staleAt)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ dt, bizNo, status, action string }{
		{"LEDGER_MISSING", "P1", "PENDING", "REPLAY"},
		{"CHANNEL_MISSING", "P2", "IGNORED", ""},
		{"LOCAL_MISSING", "P3", "PENDING", ""},
	} {
		dNo, _ := ids.NewNo(context.Background(), "RD")
		var act sql.NullString
		if item.action != "" {
			act = sql.NullString{String: item.action, Valid: true}
		}
		_, err = db.ExecContext(context.Background(), `INSERT INTO recon_diff
			(diff_no, batch_no, channel, bill_date, diff_type, txn_type, biz_no, status, handle_action, handler, handle_note, created_at, updated_at)
			VALUES (?, ?, 'mockpay', '2026-10-26', ?, 'PAY', ?, ?, ?, '', '', ?, ?)`,
			dNo, b2No, item.dt, item.bizNo, item.status, act, staleAt, staleAt)
		if err != nil {
			t.Fatal(err)
		}
	}

	resumed, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Batch.Status != dal.StatusDone ||
		resumed.Batch.DiffCount != fresh.Batch.DiffCount ||
		resumed.Batch.MatchedCount != fresh.Batch.MatchedCount ||
		resumed.Batch.ChannelCount != fresh.Batch.ChannelCount ||
		resumed.Batch.ChannelAmount != fresh.Batch.ChannelAmount ||
		resumed.Batch.LocalCount != fresh.Batch.LocalCount ||
		resumed.Batch.LocalAmount != fresh.Batch.LocalAmount {
		t.Fatalf("mismatch: fresh=%+v resumed=%+v", fresh.Batch, resumed.Batch)
	}
}

// REC-01 验收 6：接管后旧运行不覆盖结果。
func TestRunBatchTakeoverOldRunDoesNotOverwrite(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	oldChecking := make(chan struct{})
	oldRelease := make(chan struct{})
	var calls atomic.Int32

	ck := &fakeChecker{fn: func(_ context.Context, _ *dal.Batch) (biz.CheckResult, error) {
		n := calls.Add(1)
		if n == 1 {
			close(oldChecking)
			<-oldRelease
			return biz.CheckResult{ChannelCount: 1, MatchedCount: 1}, nil
		}
		return biz.CheckResult{ChannelCount: 99, MatchedCount: 99}, nil
	}}
	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, &fakeDownloader{}, ck)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	}()

	<-oldChecking
	clk.Advance(31 * time.Minute)
	outNew, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	if err != nil {
		t.Fatal(err)
	}
	if outNew.Batch.Status != dal.StatusDone || outNew.Batch.ChannelCount != 99 {
		t.Fatalf("new run batch=%+v", outNew.Batch)
	}

	close(oldRelease)
	wg.Wait()

	var chCount int
	var status string
	if err := db.QueryRowContext(context.Background(), `SELECT status, channel_count FROM recon_batch WHERE channel='mockpay' AND bill_date='2026-10-26'`).Scan(&status, &chCount); err != nil {
		t.Fatal(err)
	}
	if status != dal.StatusDone || chCount != 99 {
		t.Fatalf("old run overwrote result: status=%s channel_count=%d", status, chCount)
	}
}

// REC-01 验收 7：INIT / DOWNLOADED / CHECKING 29 分钟不接管、31 分钟接管、两接管者并发只有一个执行。
func TestRunBatchTimeoutTakeover(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	bNo, _ := ids.NewNo(context.Background(), "RB")
	_, err := db.ExecContext(context.Background(), `INSERT INTO recon_batch (batch_no, channel, bill_date, status, started_at, created_at, updated_at)
		VALUES (?, 'mockpay', '2026-10-26', 'CHECKING', ?, ?, ?)`, bNo, t0, t0, t0)
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	gate := make(chan struct{})
	release := make(chan struct{})
	ck := &fakeChecker{fn: func(_ context.Context, _ *dal.Batch) (biz.CheckResult, error) {
		if calls.Add(1) == 1 {
			close(gate)
			<-release
		}
		return biz.CheckResult{}, nil
	}}
	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, &fakeDownloader{}, ck)

	clk.Advance(29 * time.Minute)
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	if err != nil || out.Batch.Status != dal.StatusChecking || calls.Load() != 0 {
		t.Fatalf("at 29m: err=%v status=%s calls=%d", err, out.Batch.Status, calls.Load())
	}

	clk.Advance(2 * time.Minute) // total 31m
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	}()
	<-gate
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("concurrent takeovers executed %d times, want 1", calls.Load())
	}
}

// REC-01 验收 8：没有后台扫描：停在 DOWNLOADED 的批次在 Fake 时钟推进 2 小时、期间没有 RunBatch 调用时，状态与 updated_at 都不变。
func TestSchedulerNoBackgroundScan(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 5, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	bNo, _ := ids.NewNo(context.Background(), "RB")
	_, err := db.ExecContext(context.Background(), `INSERT INTO recon_batch (batch_no, channel, bill_date, status, created_at, updated_at)
		VALUES (?, 'mockpay', '2026-10-26', 'DOWNLOADED', ?, ?)`, bNo, t0, t0)
	if err != nil {
		t.Fatal(err)
	}

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, &fakeDownloader{}, &fakeChecker{})
	sched := job.NewScheduler(svc, clk, "10:00", nil)
	svc.UseRetryScheduler(sched)

	clk.Advance(2 * time.Hour)
	if err := sched.Step(context.Background()); err != nil {
		t.Fatal(err)
	}

	var status string
	var updatedAt time.Time
	if err := db.QueryRowContext(context.Background(), `SELECT status, updated_at FROM recon_batch WHERE batch_no = ?`, bNo).Scan(&status, &updatedAt); err != nil {
		t.Fatal(err)
	}
	if status != dal.StatusDownloaded || !updatedAt.Equal(t0) {
		t.Fatalf("unexpected background scan change: status=%s updated_at=%v", status, updatedAt)
	}
}

// REC-01 验收 9：假渠道一直返回 BILL_NOT_READY：首次下载之后每 30 分钟重试一次，retry_count 依次为 1..6，第 6 次重试后 FAILED。
func TestBillNotReadyRetryUntilFailed(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	var dlCalls atomic.Int32
	dl := &fakeDownloader{fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
		dlCalls.Add(1)
		return biz.DownloadResult{}, biz.ErrBillNotReady
	}}
	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, &fakeChecker{})
	sched := job.NewScheduler(svc, clk, "10:00", nil)
	svc.UseRetryScheduler(sched)

	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Batch.Status != dal.StatusInit {
		t.Fatalf("initial status=%s, want INIT", out.Batch.Status)
	}

	for wantRetry := 2; wantRetry <= 6; wantRetry++ {
		clk.Advance(30 * time.Minute)
		if err := sched.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		var status string
		var rc int
		if err := db.QueryRowContext(context.Background(), `SELECT status, retry_count FROM recon_batch WHERE batch_no = ?`, out.Batch.BatchNo).Scan(&status, &rc); err != nil {
			t.Fatal(err)
		}
		if status != dal.StatusInit || rc != wantRetry {
			t.Fatalf("step %d: status=%s retry_count=%d, want INIT/%d", wantRetry, status, rc, wantRetry)
		}
	}

	// 第 6 次重试（第 7 次请求）
	clk.Advance(30 * time.Minute)
	if err := sched.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	var status string
	var errMsg sql.NullString
	var rc int
	if err := db.QueryRowContext(context.Background(), `SELECT status, error_msg, retry_count FROM recon_batch WHERE batch_no = ?`, out.Batch.BatchNo).Scan(&status, &errMsg, &rc); err != nil {
		t.Fatal(err)
	}
	if status != dal.StatusFailed || errMsg.String != "BILL_NOT_READY" || rc != 6 {
		t.Fatalf("final: status=%s err=%q rc=%d", status, errMsg.String, rc)
	}
	if dlCalls.Load() != 7 {
		t.Fatalf("total download calls=%d, want 7", dlCalls.Load())
	}

	// 之后不再自动请求
	clk.Advance(2 * time.Hour)
	if err := sched.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dlCalls.Load() != 7 {
		t.Fatalf("download calls after FAILED=%d, want 7", dlCalls.Load())
	}
}

// REC-01 验收 10：recon 重启后计数不丢。
func TestBillNotReadyRestartPreservesRetryCount(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	bNo, _ := ids.NewNo(context.Background(), "RB")
	staleAt := t0.Add(-24 * time.Hour)
	_, err := db.ExecContext(context.Background(), `INSERT INTO recon_batch (batch_no, channel, bill_date, status, retry_count, created_at, updated_at)
		VALUES (?, 'mockpay', '2026-10-25', 'INIT', 3, ?, ?)`, bNo, staleAt, staleAt)
	if err != nil {
		t.Fatal(err)
	}

	// 模拟重启：新建 Recon 和 Scheduler 实例，原内存调度任务丢失
	dl := &fakeDownloader{fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
		return biz.DownloadResult{}, biz.ErrBillNotReady
	}}
	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, &fakeChecker{})
	sched := job.NewScheduler(svc, clk, "10:00", nil)
	svc.UseRetryScheduler(sched)

	if err := sched.Step(context.Background()); err != nil {
		t.Fatal(err)
	}

	var rc int
	if err := db.QueryRowContext(context.Background(), `SELECT retry_count FROM recon_batch WHERE batch_no = ?`, bNo).Scan(&rc); err != nil {
		t.Fatal(err)
	}
	if rc != 4 {
		t.Fatalf("retry_count=%d, want 4", rc)
	}
}

// REC-01 验收 11：旧账期由定时触发接管（D-3 DOWNLOADED、D-5 CHECKING、D-9 INIT、D-4 FAILED）。
func TestDailyRunTakesOverOldPendingBatches(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0) // D = 2026-10-27
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	stale := t0.Add(-31 * time.Minute)
	seed := func(billDate, status string) {
		bNo, _ := ids.NewNo(context.Background(), "RB")
		var started sql.NullTime
		if status == dal.StatusChecking {
			started = sql.NullTime{Time: stale, Valid: true}
		}
		_, err := db.ExecContext(context.Background(), `INSERT INTO recon_batch (batch_no, channel, bill_date, status, started_at, created_at, updated_at)
			VALUES (?, 'mockpay', ?, ?, ?, ?, ?)`, bNo, billDate, status, started, stale, stale)
		if err != nil {
			t.Fatal(err)
		}
	}
	seed("2026-10-24", dal.StatusDownloaded) // D-3
	seed("2026-10-22", dal.StatusChecking)   // D-5
	seed("2026-10-18", dal.StatusInit)       // D-9 (窗口外)
	seed("2026-10-23", dal.StatusFailed)     // D-4 FAILED

	var dlDates []string
	var mu sync.Mutex
	dl := &fakeDownloader{fn: func(_ context.Context, _ sqlx.ExtContext, b *dal.Batch) (biz.DownloadResult, error) {
		mu.Lock()
		dlDates = append(dlDates, b.BillDate)
		mu.Unlock()
		return biz.DownloadResult{FileHash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}, nil
	}}

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, &fakeChecker{})
	if err := svc.RunDaily(context.Background()); err != nil {
		t.Fatal(err)
	}

	checkStatus := func(billDate, want string) {
		var s string
		if err := db.QueryRowContext(context.Background(), `SELECT status FROM recon_batch WHERE channel='mockpay' AND bill_date=?`, billDate).Scan(&s); err != nil {
			t.Fatalf("query %s: %v", billDate, err)
		}
		if s != want {
			t.Fatalf("%s status=%s, want %s", billDate, s, want)
		}
	}
	checkStatus("2026-10-26", dal.StatusDone)   // D-1
	checkStatus("2026-10-24", dal.StatusDone)   // D-3
	checkStatus("2026-10-22", dal.StatusDone)   // D-5
	checkStatus("2026-10-18", dal.StatusInit)   // D-9
	checkStatus("2026-10-23", dal.StatusFailed) // D-4

	for _, d := range dlDates {
		if d == "2026-10-24" {
			t.Fatalf("DOWNLOADED D-3 batch should not have called downloader")
		}
	}
}

// REC-01 验收 12：重跑把 retry_count 重置为 0。
func TestRerunResetsRetryCountToZero(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	bNo, _ := ids.NewNo(context.Background(), "RB")
	_, err := db.ExecContext(context.Background(), `INSERT INTO recon_batch (batch_no, channel, bill_date, status, error_msg, retry_count, created_at, updated_at)
		VALUES (?, 'mockpay', '2026-10-26', 'FAILED', 'BILL_NOT_READY', 6, ?, ?)`, bNo, t0, t0)
	if err != nil {
		t.Fatal(err)
	}

	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, &fakeDownloader{}, &fakeChecker{})
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26", Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if out.Batch.Status != dal.StatusDone {
		t.Fatalf("status=%s, want DONE", out.Batch.Status)
	}
	var rc int
	if err := db.QueryRowContext(context.Background(), `SELECT retry_count FROM recon_batch WHERE batch_no = ?`, bNo).Scan(&rc); err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Fatalf("retry_count=%d, want 0", rc)
	}
}

// REC-01 验收 13：假渠道前 3 次返回 BILL_NOT_READY、第 4 次返回账单：批次在第 3 次重试时执行到 DONE，之后不再请求账单。
func TestBillNotReadySucceedsOnFourthAttempt(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	var calls atomic.Int32
	dl := &fakeDownloader{fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
		if calls.Add(1) <= 3 {
			return biz.DownloadResult{}, biz.ErrBillNotReady
		}
		return biz.DownloadResult{FileHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}, nil
	}}
	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, &fakeChecker{})
	sched := job.NewScheduler(svc, clk, "10:00", nil)
	svc.UseRetryScheduler(sched)

	if _, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		clk.Advance(30 * time.Minute)
		if err := sched.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	var status string
	if err := db.QueryRowContext(context.Background(), `SELECT status FROM recon_batch WHERE channel='mockpay' AND bill_date='2026-10-26'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != dal.StatusDone || calls.Load() != 4 {
		t.Fatalf("status=%s calls=%d, want DONE/4", status, calls.Load())
	}

	clk.Advance(2 * time.Hour)
	if err := sched.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 {
		t.Fatalf("unexpected extra call: %d", calls.Load())
	}
}

func TestRunBatchDoneWithoutForceAndFailureTransitions(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	// 1. 下载失败置 FAILED
	dlErr := errors.New("download timeout")
	dl := &fakeDownloader{fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
		return biz.DownloadResult{}, dlErr
	}}
	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl, &fakeChecker{})
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-25"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Batch.Status != dal.StatusFailed || out.Batch.ErrorMsg == nil || *out.Batch.ErrorMsg != dlErr.Error() {
		t.Fatalf("download failed batch=%+v", out.Batch)
	}

	// 2. 比对失败置 FAILED，然后重跑成功到 DONE，不带 force 再次调用不重跑
	var dlCount, ckCount atomic.Int32
	dl2 := &fakeDownloader{fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
		dlCount.Add(1)
		return biz.DownloadResult{FileHash: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}, nil
	}}
	ck2 := &fakeChecker{fn: func(_ context.Context, _ *dal.Batch) (biz.CheckResult, error) {
		if ckCount.Add(1) == 1 {
			return biz.CheckResult{}, errors.New("check panic err")
		}
		return biz.CheckResult{ChannelCount: 1, MatchedCount: 1}, nil
	}}
	svc2 := biz.NewRecon(db, ids, clk, []string{"mockpay"}, dl2, ck2)

	// FAILED 状态重跑（即使 force=false），在 checker 第 1 次失败
	out, err = svc2.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-25"})
	if err != nil || out.Batch.Status != dal.StatusFailed {
		t.Fatalf("checker failed: err=%v batch=%+v", err, out.Batch)
	}

	// 再次调用重跑成功到 DONE
	clk.Advance(time.Minute)
	out, err = svc2.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-25"})
	if err != nil || out.Batch.Status != dal.StatusDone {
		t.Fatalf("second run: err=%v batch=%+v", err, out.Batch)
	}

	// DONE 且 force=false 不重跑
	outAgain, err := svc2.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-25", Force: false})
	if err != nil || outAgain.Batch.Status != dal.StatusDone {
		t.Fatalf("done without force: err=%v batch=%+v", err, outAgain.Batch)
	}
	if dlCount.Load() != 2 || ckCount.Load() != 2 {
		t.Fatalf("counts dl=%d ck=%d, want 2/2", dlCount.Load(), ckCount.Load())
	}
}

type errIDGen struct{ err error }

func (e errIDGen) NewNo(context.Context, string) (string, error) { return "", e.err }

func TestTakeoverInitAndClockRollback(t *testing.T) {
	db := openReconTestDB(t)
	clk := &clock.Fake{}
	t0 := shanghaiTime(2026, 10, 27, 10, 0, 0)
	clk.Set(t0)
	ids := newTestIDGen(t, clk)

	// 1. idgen 时钟回拨映射 10004
	svcRollback := biz.NewRecon(db, errIDGen{err: idgen.ErrClockRollback}, clk, []string{"mockpay"}, nil, nil)
	if _, err := svcRollback.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"}); err == nil {
		t.Fatal("expected rollback error")
	}

	// 2. INIT 满 30 分钟接管成功执行到 DONE
	bNo, _ := ids.NewNo(context.Background(), "RB")
	stale := t0.Add(-31 * time.Minute)
	_, err := db.ExecContext(context.Background(), `INSERT INTO recon_batch (batch_no, channel, bill_date, status, created_at, updated_at)
		VALUES (?, 'mockpay', '2026-10-26', 'INIT', ?, ?)`, bNo, stale, stale)
	if err != nil {
		t.Fatal(err)
	}
	svc := biz.NewRecon(db, ids, clk, []string{"mockpay"}, &fakeDownloader{}, &fakeChecker{})
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-26"})
	if err != nil || out.Batch.Status != dal.StatusDone {
		t.Fatalf("INIT takeover: err=%v batch=%+v", err, out.Batch)
	}

	// 3. 被接管后旧 CHECKING 运行返回 error 也不覆盖已 DONE 的新结果
	gate := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	ckFailOld := &fakeChecker{fn: func(_ context.Context, _ *dal.Batch) (biz.CheckResult, error) {
		if calls.Add(1) == 1 {
			close(gate)
			<-release
			return biz.CheckResult{}, errors.New("old run error")
		}
		return biz.CheckResult{ChannelCount: 7, MatchedCount: 7}, nil
	}}
	svcRace := biz.NewRecon(db, ids, clk, []string{"mockpay"}, &fakeDownloader{}, ckFailOld)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = svcRace.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-25"})
	}()
	<-gate
	clk.Advance(31 * time.Minute)
	outNew, err := svcRace.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: "2026-10-25"})
	if err != nil || outNew.Batch.Status != dal.StatusDone || outNew.Batch.ChannelCount != 7 {
		t.Fatalf("new run: err=%v batch=%+v", err, outNew.Batch)
	}
	close(release)
	wg.Wait()

	var status string
	if err := db.QueryRowContext(context.Background(), `SELECT status FROM recon_batch WHERE channel='mockpay' AND bill_date='2026-10-25'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != dal.StatusDone {
		t.Fatalf("status=%s after old error, want DONE", status)
	}
}
