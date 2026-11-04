package biz

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
)

const casRefundProcessingSQL = `
UPDATE refund_order
   SET status = 'PROCESSING', version = version + 1, updated_at = ?
 WHERE refund_no = ? AND merchant_id = ? AND status = 'INIT'`

const insertRefundChannelTxnSQL = `
INSERT INTO channel_txn (
  channel, txn_type, biz_no, channel_txn_no, amount, status,
  notify_count, query_count, next_query_at, version, created_at, updated_at
) VALUES (?, 'REFUND', ?, NULL, ?, 'PROCESSING', 0, 0, ?, 0, ?, ?)`

const refundByNoSQL = `
SELECT refund_no, merchant_id, out_refund_no, pay_no, amount, currency, reason,
       status, channel, fail_code, fail_msg, succeeded_at, version, created_at, updated_at
FROM refund_order
WHERE refund_no = ?`

// casRefundProcessing 把 INIT 退款改成 PROCESSING，并插入一条 channel_txn_no 为空的 REFUND 流水。
// 更新 0 行或插入 1062 时回滚，返回 false。渠道调用不在这个事务里。
func (s sqlOrderStore) casRefundProcessing(ctx context.Context, row RefundOrder, now time.Time) (bool, error) {
	var won bool
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		won = false
		res, err := tx.ExecContext(ctx, casRefundProcessingSQL, now, row.RefundNo, row.MerchantID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return errCASMiss
		}
		_, err = tx.ExecContext(ctx, insertRefundChannelTxnSQL,
			row.Channel, row.RefundNo, int64(row.Amount), now.Add(payNextQueryDelay), now, now,
		)
		if err != nil {
			return err
		}
		won = true
		return nil
	})
	if errors.Is(err, errCASMiss) || dbx.IsDuplicate(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return won, nil
}

func (s sqlOrderStore) refundByNo(ctx context.Context, refundNo string) (RefundOrder, bool, error) {
	row, err := scanRefund(s.db.QueryRowContext(ctx, refundByNoSQL, refundNo))
	if errors.Is(err, sql.ErrNoRows) {
		return RefundOrder{}, false, nil
	}
	if err != nil {
		return RefundOrder{}, false, err
	}
	if row.RefundNo != refundNo {
		return RefundOrder{}, false, nil
	}
	return row, true, nil
}
