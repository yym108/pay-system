package biz

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/money"
)

// updated_at 显式写入，避免 ON UPDATE CURRENT_TIMESTAMP 改成数据库时钟。WHERE 不含 version。
const occupyRefundSQL = `
UPDATE payment_order
   SET refunded_amount = refunded_amount + ?, version = version + 1, updated_at = ?
 WHERE pay_no = ? AND merchant_id = ? AND status = 'SUCCESS' AND refunded_amount + ? <= amount`

const insertRefundSQL = `
INSERT INTO refund_order (
  refund_no, merchant_id, out_refund_no, pay_no, amount, currency, reason,
  status, channel, version, created_at, updated_at
) VALUES (
  ?, ?, ?, ?, ?, ?, ?,
  'INIT', ?, 0, ?, ?
)`

const refundByMerchantOutSQL = `
SELECT refund_no, merchant_id, out_refund_no, pay_no, amount, currency, reason,
       status, channel, fail_code, fail_msg, succeeded_at, version, created_at, updated_at
FROM refund_order
WHERE merchant_id = ? AND out_refund_no = ?`

func (s sqlOrderStore) refundByMerchantOut(ctx context.Context, merchantID, outRefundNo string) (RefundOrder, bool, error) {
	row, err := scanRefund(s.db.QueryRowContext(ctx, refundByMerchantOutSQL, merchantID, outRefundNo))
	if errors.Is(err, sql.ErrNoRows) {
		return RefundOrder{}, false, nil
	}
	if err != nil {
		return RefundOrder{}, false, err
	}
	if row.MerchantID != merchantID || row.OutRefundNo != outRefundNo {
		return RefundOrder{}, false, nil
	}
	return row, true, nil
}

// occupyRefund 在一个事务里条件增加 refunded_amount 并插入 INIT 退款单。
// 影响 0 行时回调返回 nil，空事务提交，不插入。
// 插入 1062 时返回错误让事务回滚（额度不能留下），调用方看到 duplicate=true 且 err=nil。
func (s sqlOrderStore) occupyRefund(ctx context.Context, pay PaymentOrder, refund RefundOrder) (bool, bool, error) {
	var affected, duplicate bool
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		affected = false
		duplicate = false
		res, err := tx.ExecContext(ctx, occupyRefundSQL,
			int64(refund.Amount), refund.UpdatedAt, pay.PayNo, pay.MerchantID, int64(refund.Amount))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		_, err = tx.ExecContext(ctx, insertRefundSQL,
			refund.RefundNo, refund.MerchantID, refund.OutRefundNo, refund.PayNo,
			int64(refund.Amount), refund.Currency, refund.Reason, refund.Channel,
			refund.CreatedAt, refund.UpdatedAt)
		if dbx.IsDuplicate(err) {
			duplicate = true
			return err
		}
		if err != nil {
			return err
		}
		affected = true
		return nil
	})
	if duplicate {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	return affected, false, nil
}

func scanRefund(s rowScanner) (RefundOrder, error) {
	var (
		row                  RefundOrder
		amount               int64
		failCode, failMsg    sql.NullString
		succeededAt          sql.NullTime
		createdAt, updatedAt time.Time
	)
	err := s.Scan(
		&row.RefundNo, &row.MerchantID, &row.OutRefundNo, &row.PayNo, &amount, &row.Currency, &row.Reason,
		&row.Status, &row.Channel, &failCode, &failMsg, &succeededAt, &row.Version, &createdAt, &updatedAt,
	)
	if err != nil {
		return RefundOrder{}, err
	}
	row.Amount = money.Amount(amount)
	if failCode.Valid {
		row.FailCode = failCode.String
	}
	if failMsg.Valid {
		row.FailMsg = failMsg.String
	}
	if succeededAt.Valid {
		row.SucceededAt = succeededAt.Time
	}
	row.CreatedAt = createdAt
	row.UpdatedAt = updatedAt
	return row, nil
}
