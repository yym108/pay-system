// Package dbx 打开 MySQL 连接并执行本地事务。
//
// SQL 只能用 ? 占位符。本包不拼接 SQL，也不把驱动错误改写成业务码。
// 事务里不要调用 RPC、HTTP 或 MQ。
package dbx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const (
	mysqlDupEntry        uint16 = 1062
	mysqlLockWaitTimeout uint16 = 1205
	mysqlDeadlock        uint16 = 1213
	mysqlCheckConstraint uint16 = 3819

	defaultMaxOpen = 50
	defaultMaxIdle = 10

	// defaultTxTimeout 是 WithTx 的默认总时长。
	defaultTxTimeout = 3 * time.Second
	maxTxAttempts    = 4
)

const defaultMaxLifetime = 30 * time.Minute

// txTimeout 是单次 WithTx 的总时长，含重试等待。
// 测试可以改短，结束时必须恢复成 defaultTxTimeout。不要在并行测试里改。
var txTimeout = defaultTxTimeout

// retryBackoff 是三次重试前的等待。3 是重试次数，加上第一次共 4 次。
var retryBackoff = [3]time.Duration{
	10 * time.Millisecond,
	20 * time.Millisecond,
	40 * time.Millisecond,
}

// PoolConfig 是连接池上限。字段为 0 时用默认值，负数非法。
type PoolConfig struct {
	// MaxOpen 是最大打开连接数。0 表示 50。
	MaxOpen int
	// MaxIdle 是最大空闲连接数。0 表示 10。
	MaxIdle int
	// MaxLifetime 是连接最长存活时间。0 表示 30 分钟。
	MaxLifetime time.Duration
}

// NormalizePool 把 0 换成默认值。任一字段为负时返回错误，不连接数据库。
func NormalizePool(pool PoolConfig) (PoolConfig, error) {
	if pool.MaxOpen < 0 || pool.MaxIdle < 0 || pool.MaxLifetime < 0 {
		return PoolConfig{}, errors.New("dbx: negative pool config")
	}
	if pool.MaxOpen == 0 {
		pool.MaxOpen = defaultMaxOpen
	}
	if pool.MaxIdle == 0 {
		pool.MaxIdle = defaultMaxIdle
	}
	if pool.MaxLifetime == 0 {
		pool.MaxLifetime = defaultMaxLifetime
	}
	return pool, nil
}

// NormalizeDSN 返回覆盖强制参数后的 DSN，不连接数据库。
// 调用方写了别的值也会被覆盖。tx_isolation 会删掉，避免和 transaction_isolation 抢会话变量。
func NormalizeDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("dbx: parse dsn: %w", err)
	}
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return "", fmt.Errorf("dbx: load location: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = loc
	cfg.Timeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	if cfg.Params == nil {
		cfg.Params = make(map[string]string, 1)
	}
	delete(cfg.Params, "tx_isolation")
	cfg.Params["transaction_isolation"] = "'READ-COMMITTED'"
	return cfg.FormatDSN(), nil
}

// Open 打开连接池并 Ping。连接前强制 DSN 参数，不执行业务 SQL。
func Open(ctx context.Context, dsn string, pool PoolConfig) (*sqlx.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	norm, err := NormalizePool(pool)
	if err != nil {
		return nil, err
	}
	normalized, err := NormalizeDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sqlx.Open("mysql", normalized)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(norm.MaxOpen)
	db.SetMaxIdleConns(norm.MaxIdle)
	db.SetConnMaxLifetime(norm.MaxLifetime)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// IsDuplicate 报告 err 链上是否有 MySQL 1062。
// 这不是业务码。调用方自己决定算不算幂等成功。
func IsDuplicate(err error) bool {
	n, ok := mysqlErrno(err)
	return ok && n == mysqlDupEntry
}

// IsCheckViolation 报告 err 链上是否有 MySQL 3819。这不是业务码。
func IsCheckViolation(err error) bool {
	n, ok := mysqlErrno(err)
	return ok && n == mysqlCheckConstraint
}

// MustAffectOne 要求影响行数正好是 1。
// 行数不对时返回普通错误，不映射业务码，文本里没有 SQL。
// RowsAffected 自己失败时返回那个错误。
func MustAffectOne(result sql.Result) error {
	if result == nil {
		return errors.New("dbx: nil result")
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("dbx: expected 1 row affected, got %d", n)
	}
	return nil
}

func mysqlErrno(err error) (uint16, bool) {
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me == nil {
		return 0, false
	}
	return me.Number, true
}
