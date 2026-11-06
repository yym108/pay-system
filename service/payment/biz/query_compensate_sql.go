package biz

import (
	"context"
	"slices"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
)

// claimQuerySQL 走 idx_status_next_query_at。SKIP LOCKED 让多个实例各取各的行。
const claimQuerySQL = `
SELECT id, biz_no, txn_type, query_count, created_at
FROM channel_txn
WHERE status = 'PROCESSING' AND next_query_at <= ?
ORDER BY next_query_at
LIMIT ?
FOR UPDATE SKIP LOCKED`

// bumpQuerySQL 推后同一档的行。不改 version：抢占不是状态变化，推进终态的 CAS 也不比较 version。
const bumpQuerySQL = `
UPDATE channel_txn
   SET query_count = query_count + 1, last_query_at = ?, next_query_at = ?, updated_at = ?
 WHERE id IN (?) AND status = 'PROCESSING'`

// exhaustQuerySQL 让超过 24 小时的行不再被抢占，query_count 与 last_query_at 不变。
const exhaustQuerySQL = `
UPDATE channel_txn
   SET next_query_at = NULL, updated_at = ?
 WHERE id IN (?) AND status = 'PROCESSING'`

// claimQueryTasks 在一个短事务里抢占至多 queryBatchSize 行到期的 PROCESSING 流水。
// 返回要查单的任务（QueryCount 是加一之后的值）和超过查单期限、已置 next_query_at = NULL 的行。
// 事务内不调用外部系统。
func (s sqlOrderStore) claimQueryTasks(ctx context.Context, now time.Time, policy QueryPolicy) (tasks, stale []queryTask, err error) {
	err = dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		tasks, stale = nil, nil
		claimed, txErr := selectQueryTasks(ctx, tx, now)
		if txErr != nil {
			return txErr
		}
		byCount := map[int][]int64{}
		var staleIDs []int64
		for _, task := range claimed {
			if now.Sub(task.CreatedAt) > queryGiveUpAfter {
				staleIDs = append(staleIDs, task.ID)
				stale = append(stale, task)
				continue
			}
			task.QueryCount++
			task.NextQueryAt = now.Add(queryDelay(policy, task.QueryCount))
			byCount[task.QueryCount] = append(byCount[task.QueryCount], task.ID)
			tasks = append(tasks, task)
		}
		if len(staleIDs) > 0 {
			if txErr = execIn(ctx, tx, exhaustQuerySQL, now, staleIDs); txErr != nil {
				return txErr
			}
		}
		counts := make([]int, 0, len(byCount))
		for n := range byCount {
			counts = append(counts, n)
		}
		slices.Sort(counts)
		for _, n := range counts {
			// 与上面写进 task.NextQueryAt 的是同一个值，任务启动前据此判断这一行是否已可被别人接手。
			if txErr = execIn(ctx, tx, bumpQuerySQL, now, now.Add(queryDelay(policy, n)), now, byCount[n]); txErr != nil {
				return txErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return tasks, stale, nil
}

func selectQueryTasks(ctx context.Context, tx *sqlx.Tx, now time.Time) ([]queryTask, error) {
	rows, err := tx.QueryContext(ctx, claimQuerySQL, now, queryBatchSize)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []queryTask
	for rows.Next() {
		var task queryTask
		if err = rows.Scan(&task.ID, &task.BizNo, &task.TxnType, &task.QueryCount, &task.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// execIn 展开 IN (?)。最后一个参数是 id 切片。
func execIn(ctx context.Context, tx *sqlx.Tx, query string, args ...any) error {
	expanded, flat, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, tx.Rebind(expanded), flat...)
	return err
}
