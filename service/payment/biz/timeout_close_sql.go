package biz

import (
	"context"
	"time"
)

// expiredPaymentsSQL 按单个状态读过期支付单，走 idx_status_expire_at：status 等值、expire_at 范围，
// 按 expire_at 排序即索引顺序，读够 LIMIT 条就停，不做 filesort。
// 不加锁：多实例由关单路径上的状态 CAS 和渠道关单幂等互斥（PAY-07 规则 2）。
const expiredPaymentsSQL = `
SELECT pay_no, merchant_id, out_trade_no, amount, currency, subject, status,
       channel, fee_amount, refunded_amount, notify_url, expire_at,
       paid_at, closed_at, fail_code, fail_msg, version, created_at, updated_at
FROM payment_order
WHERE status = ? AND expire_at <= ?
ORDER BY expire_at
LIMIT ?`

// expiredPayments 读至多 limit 条状态为 status、expire_at <= now 的支付单，最早过期的在前。
func (s sqlOrderStore) expiredPayments(ctx context.Context, status string, now time.Time, limit int) ([]PaymentOrder, error) {
	rows, err := s.db.QueryContext(ctx, expiredPaymentsSQL, status, now, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []PaymentOrder
	for rows.Next() {
		row, err := scanPayment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
