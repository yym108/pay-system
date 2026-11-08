package biz

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
)

// claimTransferRecoverSQL 走 idx_status_next_retry_at。now 由调用方绑定，不用 SQL NOW()。
// 列与读路径相同，不含自增 id。
const claimTransferRecoverSQL = `
SELECT transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
       mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
       version, created_at, updated_at
FROM transfer_order
WHERE status IN ('TRYING', 'CONFIRMING', 'CANCELLING') AND next_retry_at <= ?
ORDER BY next_retry_at
LIMIT ?
FOR UPDATE SKIP LOCKED`

// 只装了 LOCAL 时不抢 TCC，也不抢 CONFIRMING / CANCELLING（它们只能是 TCC）。
const claimTransferRecoverLocalSQL = `
SELECT transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
       mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
       version, created_at, updated_at
FROM transfer_order
WHERE status = 'TRYING' AND mode = 'LOCAL' AND next_retry_at <= ?
ORDER BY next_retry_at
LIMIT ?
FOR UPDATE SKIP LOCKED`

// 只装了 TCC 时不抢 TRYING + LOCAL。
const claimTransferRecoverTCCSQL = `
SELECT transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
       mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
       version, created_at, updated_at
FROM transfer_order
WHERE mode = 'TCC' AND status IN ('TRYING', 'CONFIRMING', 'CANCELLING') AND next_retry_at <= ?
ORDER BY next_retry_at
LIMIT ?
FOR UPDATE SKIP LOCKED`

// bumpTransferRecoverSQL 写入 Go 算好的 retry_count 和 next_retry_at。
// updated_at 显式写成注入时钟：这一列 ON UPDATE CURRENT_TIMESTAMP，不写就会变成 SQL NOW()。
// 不改 version、status、fail_code、finished_at、txn_no。
const bumpTransferRecoverSQL = `
UPDATE transfer_order
SET retry_count = ?, next_retry_at = ?, updated_at = ?
WHERE transfer_no = ? AND status = ? AND retry_count = ?`

// claimTransferRecover 在一个短事务里抢占至多 transferRecoverBatchSize 行。
// 返回行的 RetryCount 是加 1 之后的值。事务内不调用外部系统。影响行数不是 1 时整笔回滚。
func (s sqlTransferStore) claimTransferRecover(ctx context.Context, now time.Time, localOK, tccOK bool) ([]TransferOrder, error) {
	if s.db == nil || (!localOK && !tccOK) {
		return nil, errno.ErrInternal
	}
	query := claimTransferRecoverSQL
	switch {
	case localOK && !tccOK:
		query = claimTransferRecoverLocalSQL
	case tccOK && !localOK:
		query = claimTransferRecoverTCCSQL
	}
	var claimed []TransferOrder
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		claimed = nil
		rows, txErr := selectTransferRecover(ctx, tx, query, now)
		if txErr != nil {
			return txErr
		}
		for i := range rows {
			nextCount, nextAt := nextTransferRetry(now, rows[i].RetryCount)
			res, execErr := tx.ExecContext(ctx, bumpTransferRecoverSQL,
				nextCount, nextAt, now, rows[i].TransferNo, rows[i].Status, rows[i].RetryCount)
			if execErr != nil {
				return execErr
			}
			n, nerr := res.RowsAffected()
			if nerr != nil {
				return nerr
			}
			if n != 1 {
				return errno.ErrInternal
			}
			rows[i].RetryCount = nextCount
			rows[i].NextRetryAt = nextAt
			rows[i].UpdatedAt = now
		}
		claimed = rows
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

func selectTransferRecover(ctx context.Context, tx *sqlx.Tx, query string, now time.Time) ([]TransferOrder, error) {
	rows, err := tx.QueryContext(ctx, query, now, transferRecoverBatchSize)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []TransferOrder
	for rows.Next() {
		row, scanErr := scanTransfer(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
