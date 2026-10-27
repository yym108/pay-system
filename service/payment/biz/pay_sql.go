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

const paymentByPayNoSQL = `
SELECT pay_no, merchant_id, out_trade_no, amount, currency, subject, status,
       channel, fee_amount, refunded_amount, notify_url, expire_at,
       paid_at, closed_at, fail_code, fail_msg, version, created_at, updated_at
FROM payment_order
WHERE pay_no = ?`

const casInitToPayingSQL = `
UPDATE payment_order
   SET status = 'PAYING', channel = 'mockpay', version = version + 1, updated_at = ?
 WHERE pay_no = ? AND merchant_id = ? AND status = 'INIT'`

const insertChannelTxnSQL = `
INSERT INTO channel_txn (
  channel, txn_type, biz_no, channel_txn_no, amount, status,
  notify_count, query_count, next_query_at, version, created_at, updated_at
) VALUES ('mockpay', 'PAY', ?, NULL, ?, 'PROCESSING', 0, 0, ?, 0, ?, ?)`

// updated_at = updated_at 让这一列出现在 SET 里，避免 ON UPDATE CURRENT_TIMESTAMP 改成数据库时钟。
const fillChannelTxnNoSQL = `
UPDATE channel_txn
   SET channel_txn_no = ?, updated_at = updated_at
 WHERE biz_no = ? AND channel_txn_no IS NULL`

const channelTxnByBizNoSQL = `
SELECT channel, txn_type, biz_no, channel_txn_no, amount, status,
       notify_count, query_count, next_query_at, version, created_at, updated_at
FROM channel_txn
WHERE biz_no = ?`

// errCASMiss 只用于让 WithTx 回滚影响 0 行的更新。不是 MySQL 错误号，不会触发事务重试。
var errCASMiss = errors.New("pay: cas miss")

func (s sqlOrderStore) paymentByPayNo(ctx context.Context, payNo string) (PaymentOrder, bool, error) {
	row, err := scanPayment(s.db.QueryRowContext(ctx, paymentByPayNoSQL, payNo))
	if errors.Is(err, sql.ErrNoRows) {
		return PaymentOrder{}, false, nil
	}
	if err != nil {
		return PaymentOrder{}, false, err
	}
	if row.PayNo != payNo {
		return PaymentOrder{}, false, nil
	}
	return row, true, nil
}

func (s sqlOrderStore) casInitToPaying(ctx context.Context, order PaymentOrder, now time.Time) (bool, error) {
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, casInitToPayingSQL, now, order.PayNo, order.MerchantID)
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
		_, err = tx.ExecContext(ctx, insertChannelTxnSQL,
			order.PayNo, int64(order.Amount), now.Add(payNextQueryDelay), now, now,
		)
		return err
	})
	if errors.Is(err, errCASMiss) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s sqlOrderStore) fillChannelTxnNo(ctx context.Context, bizNo, txnNo string) (int64, error) {
	res, err := s.db.ExecContext(ctx, fillChannelTxnNoSQL, txnNo, bizNo)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s sqlOrderStore) channelTxnByBizNo(ctx context.Context, bizNo string) (channelTxn, bool, error) {
	var (
		row    channelTxn
		txnNo  sql.NullString
		amount int64
	)
	err := s.db.QueryRowContext(ctx, channelTxnByBizNoSQL, bizNo).Scan(
		&row.Channel, &row.TxnType, &row.BizNo, &txnNo, &amount, &row.Status,
		&row.NotifyCount, &row.QueryCount, &row.NextQueryAt, &row.Version, &row.CreatedAt, &row.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return channelTxn{}, false, nil
	}
	if err != nil {
		return channelTxn{}, false, err
	}
	if row.BizNo != bizNo {
		return channelTxn{}, false, nil
	}
	row.Amount = money.Amount(amount)
	if txnNo.Valid {
		row.ChannelTxnNo = txnNo.String
	}
	return row, true, nil
}
