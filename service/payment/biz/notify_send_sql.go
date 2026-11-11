package biz

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
)

// errNotifyClaimCount 表示抢占后的更新没有覆盖全部锁住的行。事务回滚，这批不发送。
var errNotifyClaimCount = errors.New("notify claim update count")

// claimNotifySQL 走 idx_status_next_attempt_at。SKIP LOCKED 让多个实例各取各的行。
const claimNotifySQL = `
SELECT id, notify_no, merchant_id, biz_type, biz_no, notify_url, payload, attempt_count
FROM merchant_notify
WHERE status = 'PENDING' AND next_attempt_at <= ?
ORDER BY next_attempt_at
LIMIT ?
FOR UPDATE SKIP LOCKED`

// bumpNotifyAttemptSQL 在同一事务里把次数加 1 并推后下次时间。不改 status。
// updated_at 显式写入，避免 ON UPDATE CURRENT_TIMESTAMP 用数据库时间。
// 名字避开 result.go 里更新 channel_txn.notify_count 的 bumpNotifySQL。
const bumpNotifyAttemptSQL = `
UPDATE merchant_notify
   SET attempt_count = attempt_count + 1, next_attempt_at = ?, updated_at = ?
 WHERE id IN (?) AND status = 'PENDING'`

const notifySuccessSQL = `
UPDATE merchant_notify
   SET status = 'SUCCESS', last_http_status = ?, last_error = NULL, succeeded_at = ?, updated_at = ?
 WHERE id = ? AND status = 'PENDING'`

const notifyDeadSQL = `
UPDATE merchant_notify
   SET status = 'DEAD', last_http_status = ?, last_error = ?, updated_at = ?
 WHERE id = ? AND status = 'PENDING'`

const notifyKeepSQL = `
UPDATE merchant_notify
   SET last_http_status = ?, last_error = ?, updated_at = ?
 WHERE id = ? AND status = 'PENDING'`

// claimNotifyTasks 在一个短事务里抢占至多 notifyBatchSize 行。
// 返回的 Attempt 是加 1 之后的值。事务里不发送 HTTP。
func (s sqlOrderStore) claimNotifyTasks(ctx context.Context, now time.Time, interval time.Duration) ([]notifyTask, error) {
	var tasks []notifyTask
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		tasks = nil
		claimed, txErr := selectNotifyTasks(ctx, tx, now)
		if txErr != nil {
			return txErr
		}
		if len(claimed) == 0 {
			return nil
		}
		next := now.Add(interval)
		ids := make([]int64, len(claimed))
		for i := range claimed {
			claimed[i].Attempt++
			claimed[i].NextAttemptAt = next
			ids[i] = claimed[i].ID
		}
		n, txErr := execInCount(ctx, tx, bumpNotifyAttemptSQL, next, now, ids)
		if txErr != nil {
			return txErr
		}
		if int(n) != len(ids) {
			return errNotifyClaimCount
		}
		tasks = claimed
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func selectNotifyTasks(ctx context.Context, tx *sqlx.Tx, now time.Time) ([]notifyTask, error) {
	rows, err := tx.QueryContext(ctx, claimNotifySQL, now, notifyBatchSize)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []notifyTask
	for rows.Next() {
		var task notifyTask
		var payload []byte
		if err = rows.Scan(&task.ID, &task.NotifyNo, &task.MerchantID, &task.BizType, &task.BizNo, &task.NotifyURL, &payload, &task.Attempt); err != nil {
			return nil, err
		}
		task.Payload = append([]byte(nil), payload...)
		out = append(out, task)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func execInCount(ctx context.Context, tx *sqlx.Tx, query string, args ...any) (int64, error) {
	expanded, flat, err := sqlx.In(query, args...)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, tx.Rebind(expanded), flat...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// finishNotify 按结果 CAS。源状态只有 PENDING。影响 0 行表示别的实例已经写成终态。
func (s sqlOrderStore) finishNotify(ctx context.Context, id int64, out notifyOutcome, succeeded *time.Time, now time.Time) (bool, error) {
	var (
		res sql.Result
		err error
	)
	switch out.status {
	case notifyStatusSuccess:
		res, err = s.db.ExecContext(ctx, notifySuccessSQL, out.httpStatus, succeeded, now, id)
	case notifyDeadStatus:
		res, err = s.db.ExecContext(ctx, notifyDeadSQL, out.httpStatus, out.lastError, now, id)
	default:
		res, err = s.db.ExecContext(ctx, notifyKeepSQL, out.httpStatus, out.lastError, now, id)
	}
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
