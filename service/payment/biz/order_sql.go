package biz

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/money"
)

const insertPaymentSQL = `
INSERT INTO payment_order (
  pay_no, merchant_id, out_trade_no, amount, currency, subject, status,
  channel, fee_amount, refunded_amount, notify_url, expire_at,
  paid_at, closed_at, fail_code, fail_msg, version, created_at, updated_at
) VALUES (
  ?, ?, ?, ?, ?, ?, ?,
  NULL, ?, ?, ?, ?,
  NULL, NULL, NULL, NULL, ?, ?, ?
)`

const paymentByOutTradeNoSQL = `
SELECT pay_no, merchant_id, out_trade_no, amount, currency, subject, status,
       channel, fee_amount, refunded_amount, notify_url, expire_at,
       paid_at, closed_at, fail_code, fail_msg, version, created_at, updated_at
FROM payment_order
WHERE merchant_id = ? AND out_trade_no = ?`

type sqlOrderStore struct {
	db *sqlx.DB
}

func (s sqlOrderStore) insertPayment(ctx context.Context, row PaymentOrder) error {
	_, err := s.db.ExecContext(ctx, insertPaymentSQL,
		row.PayNo, row.MerchantID, row.OutTradeNo, int64(row.Amount), row.Currency, row.Subject, row.Status,
		int64(row.FeeAmount), int64(row.RefundedAmount), row.NotifyURL, row.ExpireAt,
		row.Version, row.CreatedAt, row.UpdatedAt,
	)
	return err
}

func (s sqlOrderStore) paymentByOutTradeNo(ctx context.Context, merchantID, outTradeNo string) (PaymentOrder, bool, error) {
	row, err := scanPayment(s.db.QueryRowContext(ctx, paymentByOutTradeNoSQL, merchantID, outTradeNo))
	if errors.Is(err, sql.ErrNoRows) {
		return PaymentOrder{}, false, nil
	}
	if err != nil {
		return PaymentOrder{}, false, err
	}
	// 列排序规则可能命中字节不同的键。只认完全相同的商户号和商户订单号。
	if row.MerchantID != merchantID || row.OutTradeNo != outTradeNo {
		return PaymentOrder{}, false, nil
	}
	return row, true, nil
}

func scanPayment(s rowScanner) (PaymentOrder, error) {
	var (
		row                        PaymentOrder
		amount, fee, refunded      int64
		channel, failCode, failMsg sql.NullString
		paidAt, closedAt           sql.NullTime
	)
	err := s.Scan(
		&row.PayNo, &row.MerchantID, &row.OutTradeNo, &amount, &row.Currency, &row.Subject, &row.Status,
		&channel, &fee, &refunded, &row.NotifyURL, &row.ExpireAt,
		&paidAt, &closedAt, &failCode, &failMsg, &row.Version, &row.CreatedAt, &row.UpdatedAt,
	)
	if err != nil {
		return PaymentOrder{}, err
	}
	row.Amount = money.Amount(amount)
	row.FeeAmount = money.Amount(fee)
	row.RefundedAmount = money.Amount(refunded)
	if channel.Valid {
		row.Channel = channel.String
	}
	if failCode.Valid {
		row.FailCode = failCode.String
	}
	if failMsg.Valid {
		row.FailMsg = failMsg.String
	}
	if paidAt.Valid {
		row.PaidAt = paidAt.Time
	}
	if closedAt.Valid {
		row.ClosedAt = closedAt.Time
	}
	return row, nil
}
