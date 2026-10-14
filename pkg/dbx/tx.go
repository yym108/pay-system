package dbx

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

// WithTx 在一个事务里执行 fn。
// fn 返回错误或 panic 时回滚。1213 和 1205 会把整个 fn 重试，含第一次最多 4 次。
// 总时长默认 3 秒，从父 ctx 派生；先到期的那个生效。超时后取消并回滚。
func WithTx(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	if db == nil {
		return errors.New("dbx: nil db")
	}
	if fn == nil {
		return errors.New("dbx: nil fn")
	}
	ctx, cancel := context.WithTimeout(ctx, txTimeout)
	defer cancel()

	var err error
	for attempt := 1; attempt <= maxTxAttempts; attempt++ {
		if attempt > 1 {
			if waitErr := waitBackoff(ctx, retryBackoff[attempt-2]); waitErr != nil {
				return waitErr
			}
		}
		err = runOnce(ctx, db, fn)
		if err == nil || !isRetryable(err) {
			return err
		}
	}
	return err
}

func runOnce(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) (err error) {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			// 回滚失败也不能换掉原来的 panic 值。
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			// 提交失败时连接可能已经不可用。回滚错误不盖住 fn 或 Commit 的错误。
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func isRetryable(err error) bool {
	n, ok := mysqlErrno(err)
	if !ok {
		return false
	}
	return n == mysqlDeadlock || n == mysqlLockWaitTimeout
}

func waitBackoff(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
