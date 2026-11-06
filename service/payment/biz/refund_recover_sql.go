package biz

import (
	"context"
	"time"
)

// 两条 SQL 都走 idx_status_updated_at (status, updated_at)，二级索引隐含主键 id，按 (updated_at, id) 排序不用 filesort。
// 只读，不加锁：互斥靠后续的冻结幂等和状态 CAS。
const (
	stuckRefundsSQL = `
SELECT id, refund_no, merchant_id, out_refund_no, pay_no, amount, currency, reason,
       status, channel, fail_code, fail_msg, succeeded_at, version, created_at, updated_at
FROM refund_order
WHERE status = 'INIT' AND updated_at <= ?
ORDER BY updated_at, id
LIMIT ?`

	// stuckRefundsAfterSQL 取游标 (updated_at, id) 之后的下一页。
	stuckRefundsAfterSQL = `
SELECT id, refund_no, merchant_id, out_refund_no, pay_no, amount, currency, reason,
       status, channel, fail_code, fail_msg, succeeded_at, version, created_at, updated_at
FROM refund_order
WHERE status = 'INIT' AND updated_at <= ?
  AND (updated_at > ? OR (updated_at = ? AND id > ?))
ORDER BY updated_at, id
LIMIT ?`
)

// refundCursor 是一页最后一行的 (updated_at, id)，下一页从它之后开始。
type refundCursor struct {
	updatedAt time.Time
	id        uint64
}

// idFirstScanner 先把第一列 id 读进 id，其余列交给 scanRefund。
type idFirstScanner struct {
	s  rowScanner
	id *uint64
}

func (w idFirstScanner) Scan(dest ...any) error {
	return w.s.Scan(append([]any{w.id}, dest...)...)
}

// stuckRefunds 按 (updated_at, id) 升序读出至多 limit 行 updated_at <= cutoff 的 INIT 退款。
// after 为 nil 时从头读，否则读游标之后的行。返回值里的游标是最后一行的位置，没有行时为零值。
func (s sqlOrderStore) stuckRefunds(ctx context.Context, cutoff time.Time, after *refundCursor, limit int) ([]RefundOrder, refundCursor, error) {
	query, args := stuckRefundsSQL, []any{cutoff, limit}
	if after != nil {
		query, args = stuckRefundsAfterSQL, []any{cutoff, after.updatedAt, after.updatedAt, after.id, limit}
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, refundCursor{}, err
	}
	defer func() { _ = rows.Close() }()
	var (
		out  []RefundOrder
		last refundCursor
	)
	for rows.Next() {
		var id uint64
		row, scanErr := scanRefund(idFirstScanner{s: rows, id: &id})
		if scanErr != nil {
			return nil, refundCursor{}, scanErr
		}
		out = append(out, row)
		last = refundCursor{updatedAt: row.UpdatedAt, id: id}
	}
	if err = rows.Err(); err != nil {
		return nil, refundCursor{}, err
	}
	return out, last, nil
}
