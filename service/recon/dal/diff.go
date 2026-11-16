package dal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
)

// 差错类型与状态常量，与 recon_diff CHECK 约束一致。
const (
	DiffLocalMissing     = "LOCAL_MISSING"
	DiffChannelMissing   = "CHANNEL_MISSING"
	DiffAmountMismatch   = "AMOUNT_MISMATCH"
	DiffStatusMismatch   = "STATUS_MISMATCH"
	DiffLedgerMissing    = "LEDGER_MISSING"
	DiffTransferMismatch = "TRANSFER_MISMATCH"

	DiffStatusPending  = "PENDING"
	DiffStatusResolved = "RESOLVED"
	DiffStatusIgnored  = "IGNORED"

	queryChunkSize = 1000
	diffChunkSize  = 500
)

// ChannelTxn 是 pay_payment.channel_txn 的只读视图（由 recon 经只读 DSN 查询，不 import payment 包）。
type ChannelTxn struct {
	Channel           string     `db:"channel"`
	TxnType           string     `db:"txn_type"`
	BizNo             string     `db:"biz_no"`
	ChannelTxnNo      *string    `db:"channel_txn_no"`
	Amount            int64      `db:"amount"`
	Status            string     `db:"status"`
	ChannelFinishedAt *time.Time `db:"channel_finished_at"`
}

const billColumns = `id, batch_no, channel, DATE_FORMAT(bill_date, '%Y-%m-%d') AS bill_date,
	txn_type, channel_txn_no, biz_no, amount, channel_status, channel_finished_at, match_status, created_at`

const channelTxnColumns = `channel, txn_type, biz_no, channel_txn_no, amount, status, channel_finished_at`

// ListChannelBillsByBatch 查询指定批次的全部账单明细。
func ListChannelBillsByBatch(ctx context.Context, exec sqlx.ExtContext, batchNo string) ([]ChannelBill, error) {
	var list []ChannelBill
	err := sqlx.SelectContext(ctx, exec, &list,
		`SELECT `+billColumns+` FROM recon_channel_bill WHERE batch_no = ? ORDER BY id ASC`,
		batchNo)
	if err != nil {
		return nil, fmt.Errorf("list recon_channel_bill: %w", err)
	}
	return list, nil
}

// UpdateChannelBillMatchStatus 批量更新账单明细的 match_status。
func UpdateChannelBillMatchStatus(ctx context.Context, exec sqlx.ExtContext, ids []uint64, status string) error {
	if len(ids) == 0 {
		return nil
	}
	for start := 0; start < len(ids); start += queryChunkSize {
		end := start + queryChunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]
		query, args, err := sqlx.In(`UPDATE recon_channel_bill SET match_status = ? WHERE id IN (?)`, status, chunk)
		if err != nil {
			return fmt.Errorf("build update match_status IN: %w", err)
		}
		if _, err := exec.ExecContext(ctx, exec.Rebind(query), args...); err != nil {
			return fmt.Errorf("update recon_channel_bill match_status: %w", err)
		}
	}
	return nil
}

// InsertDiffsSkipDuplicate 按 uk_batch_type_biz_no_diff_type 插入差错。
// 同键已存在时通过 ON DUPLICATE KEY UPDATE id = id 跳过，不修改已有行；
// 返回每种 diff_type 实际新插入的行数（影响行数为 1 的部分）。
func InsertDiffsSkipDuplicate(ctx context.Context, exec sqlx.ExtContext, diffs []Diff) (map[string]int, error) {
	inserted := make(map[string]int)
	if len(diffs) == 0 {
		return inserted, nil
	}
	// 按 diff_type 分组批量插入，使 RowsAffected 精确对应每种 diff_type 的新增行数。
	byType := make(map[string][]Diff)
	var typeOrder []string
	for _, d := range diffs {
		if _, ok := byType[d.DiffType]; !ok {
			typeOrder = append(typeOrder, d.DiffType)
		}
		byType[d.DiffType] = append(byType[d.DiffType], d)
	}
	for _, dt := range typeOrder {
		group := byType[dt]
		for start := 0; start < len(group); start += diffChunkSize {
			end := start + diffChunkSize
			if end > len(group) {
				end = len(group)
			}
			n, err := insertDiffChunkSkipDuplicate(ctx, exec, group[start:end])
			if err != nil {
				return nil, err
			}
			inserted[dt] += n
		}
	}
	return inserted, nil
}

func insertDiffChunkSkipDuplicate(ctx context.Context, exec sqlx.ExtContext, chunk []Diff) (int, error) {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO recon_diff
		(diff_no, batch_no, channel, bill_date, diff_type, txn_type, biz_no,
		 channel_txn_no, local_amount, channel_amount, local_status, channel_status,
		 status, handle_note, created_at, updated_at)
		VALUES `)
	args := make([]any, 0, len(chunk)*16)
	for i, d := range chunk {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		status := d.Status
		if status == "" {
			status = DiffStatusPending
		}
		args = append(args,
			d.DiffNo,
			d.BatchNo,
			d.Channel,
			d.BillDate,
			d.DiffType,
			d.TxnType,
			d.BizNo,
			d.ChannelTxnNo,
			d.LocalAmount,
			d.ChannelAmount,
			d.LocalStatus,
			d.ChannelStatus,
			status,
			d.HandleNote,
			d.CreatedAt,
			d.UpdatedAt,
		)
	}
	sb.WriteString(` ON DUPLICATE KEY UPDATE id = id`)
	res, err := exec.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("insert recon_diff (%d rows): %w", len(chunk), err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("recon_diff rows affected: %w", err)
	}
	return int(n), nil
}

// SaveExternalCheckResultTx 在同一事务内更新账单行 match_status 并插入外部差错。
func SaveExternalCheckResultTx(ctx context.Context, db *sqlx.DB, matchedBillIDs, diffBillIDs []uint64, diffs []Diff) (map[string]int, error) {
	var inserted map[string]int
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		if err := UpdateChannelBillMatchStatus(ctx, tx, matchedBillIDs, BillMatchMatched); err != nil {
			return err
		}
		if err := UpdateChannelBillMatchStatus(ctx, tx, diffBillIDs, BillMatchDiff); err != nil {
			return err
		}
		var err error
		inserted, err = InsertDiffsSkipDuplicate(ctx, tx, diffs)
		return err
	})
	return inserted, err
}

// ListSuccessChannelTxnsByDate 只读查询 pay_payment.channel_txn 中指定渠道在 [dayStart, dayEnd) 的 SUCCESS 记录。
func ListSuccessChannelTxnsByDate(ctx context.Context, paymentExec sqlx.ExtContext, channel string, dayStart, dayEnd time.Time) ([]ChannelTxn, error) {
	var list []ChannelTxn
	err := sqlx.SelectContext(ctx, paymentExec, &list,
		`SELECT `+channelTxnColumns+` FROM channel_txn
		WHERE channel = ? AND status = 'SUCCESS' AND channel_finished_at >= ? AND channel_finished_at < ?`,
		channel, dayStart, dayEnd)
	if err != nil {
		return nil, fmt.Errorf("list success channel_txn by date: %w", err)
	}
	return list, nil
}

// ListChannelTxnsByBizNos 只读查询 pay_payment.channel_txn 中指定渠道和单号集合的记录（不限日期和状态）。
func ListChannelTxnsByBizNos(ctx context.Context, paymentExec sqlx.ExtContext, channel string, bizNos []string) ([]ChannelTxn, error) {
	if len(bizNos) == 0 {
		return nil, nil
	}
	var out []ChannelTxn
	for start := 0; start < len(bizNos); start += queryChunkSize {
		end := start + queryChunkSize
		if end > len(bizNos) {
			end = len(bizNos)
		}
		chunk := bizNos[start:end]
		query, args, err := sqlx.In(
			`SELECT `+channelTxnColumns+` FROM channel_txn WHERE channel = ? AND biz_no IN (?)`,
			channel, chunk)
		if err != nil {
			return nil, fmt.Errorf("build channel_txn IN: %w", err)
		}
		var part []ChannelTxn
		if err := sqlx.SelectContext(ctx, paymentExec, &part, paymentExec.Rebind(query), args...); err != nil {
			return nil, fmt.Errorf("list channel_txn by biz_nos: %w", err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// GetDiffByID 按主键查询单条差错。不存在时返回 ErrNotFound。
func GetDiffByID(ctx context.Context, exec sqlx.ExtContext, id uint64) (Diff, error) {
	var d Diff
	err := sqlx.GetContext(ctx, exec, &d,
		`SELECT `+diffColumns+` FROM recon_diff WHERE id = ?`, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Diff{}, ErrNotFound
		}
		return Diff{}, fmt.Errorf("get recon_diff by id %d: %w", id, err)
	}
	return d, nil
}

// ListPendingUnhandledDiffsByBatch 查询本批次新插入且尚未自动处理的 PENDING 差错。
func ListPendingUnhandledDiffsByBatch(ctx context.Context, exec sqlx.ExtContext, batchNo string) ([]Diff, error) {
	var list []Diff
	err := sqlx.SelectContext(ctx, exec, &list,
		`SELECT `+diffColumns+` FROM recon_diff
		WHERE batch_no = ? AND status = 'PENDING' AND handle_action IS NULL
		ORDER BY id ASC`,
		batchNo)
	if err != nil {
		return nil, fmt.Errorf("list pending unhandled recon_diff: %w", err)
	}
	return list, nil
}

// ListRetryableRequeryDiffs 查询自动重试窗口 [fromDate, toDate] 内待 REQUERY 重试的 PENDING 差错
// （排除已判定严重不一致的 CHANNEL_MISSING）。
func ListRetryableRequeryDiffs(ctx context.Context, exec sqlx.ExtContext, fromDate, toDate string) ([]Diff, error) {
	var list []Diff
	err := sqlx.SelectContext(ctx, exec, &list,
		`SELECT `+diffColumns+` FROM recon_diff
		WHERE status = 'PENDING' AND bill_date >= ? AND bill_date <= ?
		  AND handle_action = 'REQUERY'
		  AND (diff_type <> 'CHANNEL_MISSING' OR channel_status IS NULL)
		ORDER BY id ASC`,
		fromDate, toDate)
	if err != nil {
		return nil, fmt.Errorf("list retryable requery recon_diff: %w", err)
	}
	return list, nil
}

// ListRetryableReplayLedgerDiffs 查询自动重试窗口 [fromDate, toDate] 内待复核或重放的 LEDGER_MISSING 差错。
func ListRetryableReplayLedgerDiffs(ctx context.Context, exec sqlx.ExtContext, fromDate, toDate string) ([]Diff, error) {
	var list []Diff
	err := sqlx.SelectContext(ctx, exec, &list,
		`SELECT `+diffColumns+` FROM recon_diff
		WHERE status = 'PENDING' AND bill_date >= ? AND bill_date <= ?
		  AND diff_type = 'LEDGER_MISSING' AND handle_action = 'REPLAY'
		ORDER BY id ASC`,
		fromDate, toDate)
	if err != nil {
		return nil, fmt.Errorf("list retryable replay recon_diff: %w", err)
	}
	return list, nil
}

// ResolveDiffCAS 用 CAS 将 PENDING 差错置为终态（RESOLVED 或 IGNORED）。
func ResolveDiffCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, status, handleAction, handler, handleNote string, resolvedAt, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_diff
		SET status = ?, handle_action = ?, handler = ?, handle_note = ?, resolved_at = ?, updated_at = ?
		WHERE id = ? AND status = 'PENDING'`,
		status, handleAction, handler, handleNote, resolvedAt, now, id)
	if err != nil {
		return false, fmt.Errorf("resolve recon_diff %d CAS: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("resolve recon_diff %d rows affected: %w", id, err)
	}
	return n > 0, nil
}

// UpdateDiffPendingCAS 用 CAS 更新 PENDING 差错的处理动作、说明与（可选的）渠道状态，差错保持 PENDING。
func UpdateDiffPendingCAS(ctx context.Context, exec sqlx.ExtContext, id uint64, handleAction, handler, handleNote string, channelStatus *string, now time.Time) (bool, error) {
	res, err := exec.ExecContext(ctx,
		`UPDATE recon_diff
		SET handle_action = ?, handler = ?, handle_note = ?,
		    channel_status = COALESCE(?, channel_status),
		    updated_at = ?
		WHERE id = ? AND status = 'PENDING'`,
		handleAction, handler, handleNote, channelStatus, now, id)
	if err != nil {
		return false, fmt.Errorf("update pending recon_diff %d CAS: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("update pending recon_diff %d rows affected: %w", id, err)
	}
	return n > 0, nil
}
