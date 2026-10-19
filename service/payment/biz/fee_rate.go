// Package biz 实现商户费率历史和订单手续费核对。
//
// AppendFeeRate 只追加费率，不改 merchant。CheckOrderFees 只读，不改订单和账。
package biz

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
)

const maxFeeRateBP = 10000

// FeeRate 是一档商户费率。Duplicated 为 true 时表示这次没有插入新行。
type FeeRate struct {
	MerchantID    string
	RateBP        int
	EffectiveFrom time.Time
	Duplicated    bool
}

// Service 追加费率并核对订单手续费。库由调用方传入，本类型不打开连接。
type Service struct {
	db *sqlx.DB
}

// New 使用调用方传入的库。
func New(db *sqlx.DB) *Service {
	return &Service{db: db}
}

// AppendFeeRate 为商户追加一档费率。
// 生效时间截断到毫秒，并保持调用方传入的时区。
// 相同时间相同费率返回已有行；相同时间不同费率返回 10006，不改旧行。
func (s *Service) AppendFeeRate(ctx context.Context, merchantID string, rateBP int, effectiveFrom time.Time) (FeeRate, error) {
	if err := guardCall(s, ctx); err != nil {
		return FeeRate{}, err
	}
	if err := validateAppend(merchantID, rateBP, effectiveFrom); err != nil {
		return FeeRate{}, err
	}
	var out FeeRate
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		loaded, err := applyAppend(ctx, sqlRateTx{tx: tx}, merchantID, rateBP, effectiveFrom)
		if err != nil {
			return err
		}
		out = loaded
		return nil
	})
	if err != nil {
		return FeeRate{}, callErr(err)
	}
	return out, nil
}

func guardCall(s *Service, ctx context.Context) error {
	if s == nil || ctx == nil {
		return errno.ErrInternal
	}
	// 已取消的上下文原样返回，不写成 10003。库为空仍是 10003。
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.db == nil {
		return errno.ErrInternal
	}
	return nil
}

func validateAppend(merchantID string, rateBP int, effectiveFrom time.Time) error {
	if merchantID == "" || rateBP < 0 || rateBP > maxFeeRateBP || effectiveFrom.IsZero() {
		return errno.ErrInvalidParam
	}
	return nil
}

// rateTx 是同一事务里的商户锁、读档和插入。
// 实现不得另开事务。假实现只给不连数据库的单元测试。
type rateTx interface {
	lockMerchant(ctx context.Context, merchantID string) (bool, error)
	latestRate(ctx context.Context, merchantID string) (FeeRate, bool, error)
	insertRate(ctx context.Context, row FeeRate) error
	rateAt(ctx context.Context, merchantID string, effectiveFrom time.Time) (FeeRate, bool, error)
}

func applyAppend(ctx context.Context, db rateTx, merchantID string, rateBP int, effectiveFrom time.Time) (FeeRate, error) {
	// 截断用绝对时间，Location 保持不变。
	effectiveFrom = effectiveFrom.Truncate(time.Millisecond)
	if effectiveFrom.IsZero() {
		return FeeRate{}, errno.ErrInvalidParam
	}
	found, err := db.lockMerchant(ctx, merchantID)
	if err != nil {
		return FeeRate{}, callErr(err)
	}
	if !found {
		return FeeRate{}, errno.ErrMerchantInvalid
	}
	latest, has, err := db.latestRate(ctx, merchantID)
	if err != nil {
		return FeeRate{}, callErr(err)
	}
	// 商户行锁还在。更早的时间在这里拒绝；相同时间留给唯一键。
	if has && effectiveFrom.Before(latest.EffectiveFrom) {
		return FeeRate{}, errno.ErrInvalidParam
	}
	row := FeeRate{MerchantID: merchantID, RateBP: rateBP, EffectiveFrom: effectiveFrom}
	if err := db.insertRate(ctx, row); err != nil {
		// 1062 不结束 InnoDB 事务。先读已有档，再决定幂等还是 10006。
		if !dbx.IsDuplicate(err) {
			return FeeRate{}, callErr(err)
		}
		existing, ok, readErr := db.rateAt(ctx, merchantID, effectiveFrom)
		if readErr != nil {
			return FeeRate{}, callErr(readErr)
		}
		if !ok {
			return FeeRate{}, errno.ErrInternal
		}
		if existing.RateBP != rateBP {
			return FeeRate{}, errno.ErrIdempotentConflict
		}
		existing.Duplicated = true
		return existing, nil
	}
	return row, nil
}

func callErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var known *errno.Error
	if errors.As(err, &known) {
		return err
	}
	return errno.Wrap(errno.ErrInternal, err)
}

const lockMerchantSQL = `
SELECT merchant_id FROM merchant WHERE merchant_id = ? FOR UPDATE`

const latestRateSQL = `
SELECT merchant_id, fee_rate_bp, effective_from
FROM merchant_fee_rate
WHERE merchant_id = ?
ORDER BY effective_from DESC
LIMIT 1`

const insertRateSQL = `
INSERT INTO merchant_fee_rate (merchant_id, fee_rate_bp, effective_from)
VALUES (?, ?, ?)`

const rateAtSQL = `
SELECT merchant_id, fee_rate_bp, effective_from
FROM merchant_fee_rate
WHERE merchant_id = ? AND effective_from = ?`

type sqlRateTx struct {
	tx *sqlx.Tx
}

func (s sqlRateTx) lockMerchant(ctx context.Context, merchantID string) (bool, error) {
	var id string
	err := s.tx.QueryRowContext(ctx, lockMerchantSQL, merchantID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s sqlRateTx) latestRate(ctx context.Context, merchantID string) (FeeRate, bool, error) {
	row, err := scanRate(s.tx.QueryRowContext(ctx, latestRateSQL, merchantID))
	if errors.Is(err, sql.ErrNoRows) {
		return FeeRate{}, false, nil
	}
	if err != nil {
		return FeeRate{}, false, err
	}
	return row, true, nil
}

func (s sqlRateTx) insertRate(ctx context.Context, row FeeRate) error {
	_, err := s.tx.ExecContext(ctx, insertRateSQL, row.MerchantID, row.RateBP, row.EffectiveFrom)
	return err
}

func (s sqlRateTx) rateAt(ctx context.Context, merchantID string, effectiveFrom time.Time) (FeeRate, bool, error) {
	row, err := scanRate(s.tx.QueryRowContext(ctx, rateAtSQL, merchantID, effectiveFrom))
	if errors.Is(err, sql.ErrNoRows) {
		return FeeRate{}, false, nil
	}
	if err != nil {
		return FeeRate{}, false, err
	}
	return row, true, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRate(row rowScanner) (FeeRate, error) {
	var out FeeRate
	err := row.Scan(&out.MerchantID, &out.RateBP, &out.EffectiveFrom)
	return out, err
}
