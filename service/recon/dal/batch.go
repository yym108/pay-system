// Package dal 提供对账库（pay_recon）的数据访问。
package dal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
)

// 批次状态常量，与 recon_batch.ck_batch_status 一致。
const (
	StatusInit       = "INIT"
	StatusDownloaded = "DOWNLOADED"
	StatusChecking   = "CHECKING"
	StatusDone       = "DONE"
	StatusFailed     = "FAILED"
)

// ErrBatchExists 表示 uk_channel_bill_date 唯一约束冲突。
var ErrBatchExists = errors.New("recon: batch already exists")

// ErrNotFound 表示记录不存在。
var ErrNotFound = errors.New("recon: record not found")

// Batch 对应 recon_batch 表。
type Batch struct {
	ID            uint64     `db:"id"`
	BatchNo       string     `db:"batch_no"`
	Channel       string     `db:"channel"`
	BillDate      string     `db:"bill_date"`
	Status        string     `db:"status"`
	BillFileHash  *string    `db:"bill_file_hash"`
	ChannelCount  int32      `db:"channel_count"`
	ChannelAmount int64      `db:"channel_amount"`
	LocalCount    int32      `db:"local_count"`
	LocalAmount   int64      `db:"local_amount"`
	MatchedCount  int32      `db:"matched_count"`
	DiffCount     int32      `db:"diff_count"`
	ErrorMsg      *string    `db:"error_msg"`
	RetryCount    int        `db:"retry_count"`
	StartedAt     *time.Time `db:"started_at"`
	FinishedAt    *time.Time `db:"finished_at"`
	CreatedAt     time.Time  `db:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at"`
}

// Diff 对应 recon_diff 表。
type Diff struct {
	ID            uint64     `db:"id"`
	DiffNo        string     `db:"diff_no"`
	BatchNo       string     `db:"batch_no"`
	Channel       string     `db:"channel"`
	BillDate      string     `db:"bill_date"`
	DiffType      string     `db:"diff_type"`
	TxnType       string     `db:"txn_type"`
	BizNo         string     `db:"biz_no"`
	ChannelTxnNo  *string    `db:"channel_txn_no"`
	LocalAmount   *int64     `db:"local_amount"`
	ChannelAmount *int64     `db:"channel_amount"`
	LocalStatus   *string    `db:"local_status"`
	ChannelStatus *string    `db:"channel_status"`
	Status        string     `db:"status"`
	HandleAction  *string    `db:"handle_action"`
	Handler       *string    `db:"handler"`
	HandleNote    *string    `db:"handle_note"`
	ResolvedAt    *time.Time `db:"resolved_at"`
	CreatedAt     time.Time  `db:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at"`
}

// CheckStats 是比对完成后写入批次的汇总统计。
type CheckStats struct {
	ChannelCount  int32
	ChannelAmount int64
	LocalCount    int32
	LocalAmount   int64
	MatchedCount  int32
	DiffCount     int32
}

const batchColumns = `id, batch_no, channel, DATE_FORMAT(bill_date, '%Y-%m-%d') AS bill_date, status,
	bill_file_hash, channel_count, channel_amount, local_count, local_amount, matched_count, diff_count,
	error_msg, retry_count, started_at, finished_at, created_at, updated_at`

const diffColumns = `id, diff_no, batch_no, channel, DATE_FORMAT(bill_date, '%Y-%m-%d') AS bill_date,
	diff_type, txn_type, biz_no, channel_txn_no, local_amount, channel_amount, local_status, channel_status,
	status, handle_action, handler, handle_note, resolved_at, created_at, updated_at`

// InsertBatch 创建新批次（INIT）。uk_channel_bill_date 冲突时返回 ErrBatchExists。
func InsertBatch(ctx context.Context, exec sqlx.ExtContext, batchNo, channel, billDate string, now time.Time) (uint64, error) {
	res, err := exec.ExecContext(ctx, `INSERT INTO recon_batch
		(batch_no, channel, bill_date, status, retry_count, created_at, updated_at)
		VALUES (?, ?, ?, ?, 0, ?, ?)`,
		batchNo, channel, billDate, StatusInit, now, now)
	if err != nil {
		if dbx.IsDuplicate(err) {
			return 0, ErrBatchExists
		}
		return 0, fmt.Errorf("insert recon_batch: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	if id < 0 {
		return 0, fmt.Errorf("unexpected negative insert id: %d", id)
	}
	return uint64(id), nil
}

// GetBatchByChannelDate 按 (channel, bill_date) 查询批次。
func GetBatchByChannelDate(ctx context.Context, exec sqlx.ExtContext, channel, billDate string) (Batch, error) {
	var b Batch
	err := sqlx.GetContext(ctx, exec, &b,
		`SELECT `+batchColumns+` FROM recon_batch WHERE channel = ? AND bill_date = ?`,
		channel, billDate)
	if errors.Is(err, sql.ErrNoRows) {
		return Batch{}, ErrNotFound
	}
	if err != nil {
		return Batch{}, fmt.Errorf("get recon_batch by channel/date: %w", err)
	}
	return b, nil
}

// GetBatchByID 按主键查询批次。
func GetBatchByID(ctx context.Context, exec sqlx.ExtContext, id uint64) (Batch, error) {
	var b Batch
	err := sqlx.GetContext(ctx, exec, &b,
		`SELECT `+batchColumns+` FROM recon_batch WHERE id = ?`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Batch{}, ErrNotFound
	}
	if err != nil {
		return Batch{}, fmt.Errorf("get recon_batch by id: %w", err)
	}
	return b, nil
}

// ListPendingBatches 查询指定渠道在 [fromDate, toDate] 内未终态（INIT/DOWNLOADED/CHECKING）的批次。
func ListPendingBatches(ctx context.Context, exec sqlx.ExtContext, channel, fromDate, toDate string) ([]Batch, error) {
	var list []Batch
	err := sqlx.SelectContext(ctx, exec, &list,
		`SELECT `+batchColumns+` FROM recon_batch
		WHERE channel = ? AND bill_date >= ? AND bill_date <= ? AND status IN (?, ?, ?)
		ORDER BY bill_date ASC, id ASC`,
		channel, fromDate, toDate, StatusInit, StatusDownloaded, StatusChecking)
	if err != nil {
		return nil, fmt.Errorf("list pending batches: %w", err)
	}
	return list, nil
}

// ResetForRerunTx 在同一事务内清理账单行与尚未自动处理的差错，并用 CAS 把批次重置为 INIT、retry_count = 0。
// 只在原状态为 FAILED 或 DONE 时调用。返回是否成功抢占重置。
func ResetForRerunTx(ctx context.Context, db *sqlx.DB, id uint64, batchNo, expectedStatus string, expectedUpdatedAt, now time.Time) (bool, error) {
	var won bool
	err := dbx.WithTxTimeout(ctx, db, 90*time.Second, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE recon_batch
			SET status = ?, bill_file_hash = NULL,
			    channel_count = 0, channel_amount = 0,
			    local_count = 0, local_amount = 0,
			    matched_count = 0, diff_count = 0,
			    error_msg = NULL, retry_count = 0,
			    started_at = NULL, finished_at = NULL,
			    updated_at = ?
			WHERE id = ? AND status = ? AND updated_at = ?`,
			StatusInit, now, id, expectedStatus, expectedUpdatedAt)
		if err != nil {
			return fmt.Errorf("reset batch CAS: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			won = false
			return nil
		}
		if err := deleteChannelBills(ctx, tx, batchNo); err != nil {
			return err
		}
		if err := deleteUnprocessedDiffs(ctx, tx, batchNo); err != nil {
			return err
		}
		won = true
		return nil
	})
	return won, err
}

// TakeoverInitCAS 接管超时的 INIT 批次：将 updated_at 改写为 now。
func TakeoverInitCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, oldUpdatedAt, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch SET updated_at = ? WHERE id = ? AND status = ? AND updated_at = ?`,
		now, id, StatusInit, oldUpdatedAt)
	if err != nil {
		return false, fmt.Errorf("takeover init CAS: %w", err)
	}
	return affectedOne(res)
}

// TakeoverDownloadedToCheckingCAS 接管超时的 DOWNLOADED 批次：直接推进为 CHECKING，写 started_at = now。
func TakeoverDownloadedToCheckingCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, oldUpdatedAt, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch SET status = ?, started_at = ?, updated_at = ? WHERE id = ? AND status = ? AND updated_at = ?`,
		StatusChecking, now, now, id, StatusDownloaded, oldUpdatedAt)
	if err != nil {
		return false, fmt.Errorf("takeover downloaded CAS: %w", err)
	}
	return affectedOne(res)
}

// TakeoverCheckingCAS 接管超时的 CHECKING 批次：改写 started_at = now 并删除尚未自动处理的 PENDING 差错。
func TakeoverCheckingCAS(ctx context.Context, db *sqlx.DB, id uint64, batchNo string, oldStartedAt, now time.Time) (bool, error) {
	var won bool
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE recon_batch SET started_at = ?, updated_at = ? WHERE id = ? AND status = ? AND started_at = ?`,
			now, now, id, StatusChecking, oldStartedAt)
		if err != nil {
			return fmt.Errorf("takeover checking CAS: %w", err)
		}
		ok, err := affectedOne(res)
		if err != nil || !ok {
			won = false
			return err
		}
		if err := deleteUnprocessedDiffs(ctx, tx, batchNo); err != nil {
			return err
		}
		won = true
		return nil
	})
	return won, err
}

// MarkDownloadedCAS 将 INIT 批次推进为 DOWNLOADED 并记录账单文件 hash。
func MarkDownloadedCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, fileHash string, oldUpdatedAt, now time.Time) (bool, error) {
	var hashPtr *string
	if fileHash != "" {
		hashPtr = &fileHash
	}
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch SET status = ?, bill_file_hash = ?, updated_at = ? WHERE id = ? AND status = ? AND updated_at = ?`,
		StatusDownloaded, hashPtr, now, id, StatusInit, oldUpdatedAt)
	if err != nil {
		return false, fmt.Errorf("mark downloaded CAS: %w", err)
	}
	return affectedOne(res)
}

// MarkCheckingCAS 将 DOWNLOADED 批次推进为 CHECKING 并写入 started_at = now。
func MarkCheckingCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, oldUpdatedAt, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch SET status = ?, started_at = ?, updated_at = ? WHERE id = ? AND status = ? AND updated_at = ?`,
		StatusChecking, now, now, id, StatusDownloaded, oldUpdatedAt)
	if err != nil {
		return false, fmt.Errorf("mark checking CAS: %w", err)
	}
	return affectedOne(res)
}

// IncrementRetryCountCAS 在 BILL_NOT_READY 且 retry_count < 6 时递增 retry_count，保持 INIT。
func IncrementRetryCountCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, oldRetryCount int, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch SET retry_count = retry_count + 1, updated_at = ? WHERE id = ? AND status = ? AND retry_count = ?`,
		now, id, StatusInit, oldRetryCount)
	if err != nil {
		return false, fmt.Errorf("increment retry_count CAS: %w", err)
	}
	return affectedOne(res)
}

// MarkBillNotReadyFailedCAS 在 retry_count 已达 6 时将 INIT 批次置为 FAILED（error_msg = BILL_NOT_READY）。
func MarkBillNotReadyFailedCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, oldRetryCount int, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch SET status = ?, error_msg = ?, finished_at = ?, updated_at = ? WHERE id = ? AND status = ? AND retry_count = ?`,
		StatusFailed, "BILL_NOT_READY", now, now, id, StatusInit, oldRetryCount)
	if err != nil {
		return false, fmt.Errorf("mark bill_not_ready failed CAS: %w", err)
	}
	return affectedOne(res)
}

// MarkDoneCAS 使用本次运行的 started_at 将 CHECKING 批次推进为 DONE。
func MarkDoneCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, startedAt time.Time, stats CheckStats, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch
		SET status = ?, channel_count = ?, channel_amount = ?, local_count = ?, local_amount = ?,
		    matched_count = ?, diff_count = ?, finished_at = ?, updated_at = ?
		WHERE id = ? AND status = ? AND started_at = ?`,
		StatusDone, stats.ChannelCount, stats.ChannelAmount, stats.LocalCount, stats.LocalAmount,
		stats.MatchedCount, stats.DiffCount, now, now, id, StatusChecking, startedAt)
	if err != nil {
		return false, fmt.Errorf("mark done CAS: %w", err)
	}
	return affectedOne(res)
}

// MarkFailedFromCheckingCAS 使用本次运行的 started_at 将 CHECKING 批次置为 FAILED。
func MarkFailedFromCheckingCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, startedAt time.Time, errMsg string, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch SET status = ?, error_msg = ?, finished_at = ?, updated_at = ? WHERE id = ? AND status = ? AND started_at = ?`,
		StatusFailed, truncateErrMsg(errMsg), now, now, id, StatusChecking, startedAt)
	if err != nil {
		return false, fmt.Errorf("mark failed from checking CAS: %w", err)
	}
	return affectedOne(res)
}

// MarkFailedWithUpdatedAtCAS 将 INIT 或 DOWNLOADED 批次按原状态和 updated_at 置为 FAILED。
func MarkFailedWithUpdatedAtCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, expectedStatus string, oldUpdatedAt time.Time, errMsg string, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_batch SET status = ?, error_msg = ?, finished_at = ?, updated_at = ? WHERE id = ? AND status = ? AND updated_at = ?`,
		StatusFailed, truncateErrMsg(errMsg), now, now, id, expectedStatus, oldUpdatedAt)
	if err != nil {
		return false, fmt.Errorf("mark failed CAS: %w", err)
	}
	return affectedOne(res)
}

// CountBatchDiffs 统计本批次 recon_diff 实际行数。
func CountBatchDiffs(ctx context.Context, exec sqlx.ExtContext, batchNo string) (int32, error) {
	var n int32
	if err := sqlx.GetContext(ctx, exec, &n, `SELECT COUNT(*) FROM recon_diff WHERE batch_no = ?`, batchNo); err != nil {
		return 0, fmt.Errorf("count batch diffs: %w", err)
	}
	return n, nil
}

// ListBatchDiffs 查询本批次最多 limit 条差错。
func ListBatchDiffs(ctx context.Context, exec sqlx.ExtContext, batchNo string, limit int) ([]Diff, error) {
	var list []Diff
	err := sqlx.SelectContext(ctx, exec, &list,
		`SELECT `+diffColumns+` FROM recon_diff WHERE batch_no = ? ORDER BY id ASC LIMIT ?`,
		batchNo, limit)
	if err != nil {
		return nil, fmt.Errorf("list batch diffs: %w", err)
	}
	return list, nil
}

func deleteChannelBills(ctx context.Context, exec sqlx.ExtContext, batchNo string) error {
	const deleteBatchSize = 5000
	for {
		res, err := exec.ExecContext(ctx, `DELETE FROM recon_channel_bill WHERE batch_no = ? LIMIT ?`, batchNo, deleteBatchSize)
		if err != nil {
			return fmt.Errorf("delete recon_channel_bill: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("delete recon_channel_bill rows affected: %w", err)
		}
		if n < deleteBatchSize {
			return nil
		}
	}
}

func deleteUnprocessedDiffs(ctx context.Context, exec sqlx.ExtContext, batchNo string) error {
	if _, err := exec.ExecContext(ctx,
		`DELETE FROM recon_diff WHERE batch_no = ? AND status = 'PENDING' AND handle_action IS NULL`,
		batchNo,
	); err != nil {
		return fmt.Errorf("delete unprocessed recon_diff: %w", err)
	}
	return nil
}

func affectedOne(res sql.Result) (bool, error) {
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func truncateErrMsg(s string) string {
	const maxLen = 512
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
