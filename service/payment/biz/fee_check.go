package biz

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

const (
	feeCheckPageSize  = 200
	feeCheckDetailCap = 100
	noEffectiveRate   = -1
)

// FeeMismatch 是一张手续费与生效费率不一致的订单。没有生效费率时 RateBP 为 -1，ExpectedFee 为 0。
type FeeMismatch struct {
	PayNo       string
	MerchantID  string
	CreatedAt   time.Time
	Amount      money.Amount
	FeeAmount   money.Amount
	ExpectedFee money.Amount
	RateBP      int
}

// FeeCheck 是一轮只读核对的结果。Mismatched 是全量不符张数，Details 最多 100 条。
type FeeCheck struct {
	Scanned    int
	Mismatched int
	Details    []FeeMismatch
}

// CheckOrderFees 按订单创建时间核对实收手续费。
// from 含、to 不含；任一为零值表示该侧没有界。
// 使用 REPEATABLE READ 只读快照，读完回滚。扫描开始后新提交的费率要等下一轮。
// 不修改订单，也不修改账。
func (s *Service) CheckOrderFees(ctx context.Context, from, to time.Time) (FeeCheck, error) {
	if err := guardCall(s, ctx); err != nil {
		return FeeCheck{}, err
	}
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return FeeCheck{}, callErr(err)
	}
	defer func() { _ = tx.Rollback() }()
	return scanFees(ctx, sqlCheckTx{tx: tx}, from, to)
}

type orderRow struct {
	ID         uint64
	PayNo      string
	MerchantID string
	Amount     money.Amount
	FeeAmount  money.Amount
	CreatedAt  time.Time
}

// checkSource 在调用方已经打开的只读事务里读费率和订单。
// 实现不得再 BEGIN。假实现只给不连数据库的单元测试。
type checkSource interface {
	listRates(ctx context.Context) ([]FeeRate, error)
	listOrders(ctx context.Context, afterID uint64, from, to time.Time, limit int) ([]orderRow, error)
}

func scanFees(ctx context.Context, src checkSource, from, to time.Time) (FeeCheck, error) {
	if err := ctx.Err(); err != nil {
		return FeeCheck{}, err
	}
	rates, err := src.listRates(ctx)
	if err != nil {
		return FeeCheck{}, callErr(err)
	}
	book := indexRates(rates)
	var out FeeCheck
	var after uint64
	for {
		if err := ctx.Err(); err != nil {
			return FeeCheck{}, err
		}
		page, err := src.listOrders(ctx, after, from, to, feeCheckPageSize)
		if err != nil {
			return FeeCheck{}, callErr(err)
		}
		if len(page) == 0 {
			return out, nil
		}
		for _, order := range page {
			if order.ID <= after {
				return FeeCheck{}, errno.ErrInternal
			}
			after = order.ID
			item, bad, judgeErr := judgeOrder(book, order)
			if judgeErr != nil {
				return FeeCheck{}, judgeErr
			}
			out.Scanned++
			if !bad {
				continue
			}
			out.Mismatched++
			if len(out.Details) < feeCheckDetailCap {
				out.Details = append(out.Details, item)
			}
		}
		if len(page) < feeCheckPageSize {
			return out, nil
		}
	}
}

func judgeOrder(book map[string][]FeeRate, order orderRow) (FeeMismatch, bool, error) {
	tier, ok := latestTier(book[order.MerchantID], order.CreatedAt)
	if !ok {
		return FeeMismatch{
			PayNo:       order.PayNo,
			MerchantID:  order.MerchantID,
			CreatedAt:   order.CreatedAt,
			Amount:      order.Amount,
			FeeAmount:   order.FeeAmount,
			ExpectedFee: 0,
			RateBP:      noEffectiveRate,
		}, true, nil
	}
	expected, err := money.Fee(order.Amount, tier.RateBP)
	if err != nil {
		return FeeMismatch{}, false, callErr(err)
	}
	if expected == order.FeeAmount {
		return FeeMismatch{}, false, nil
	}
	return FeeMismatch{
		PayNo:       order.PayNo,
		MerchantID:  order.MerchantID,
		CreatedAt:   order.CreatedAt,
		Amount:      order.Amount,
		FeeAmount:   order.FeeAmount,
		ExpectedFee: expected,
		RateBP:      tier.RateBP,
	}, true, nil
}

func indexRates(rows []FeeRate) map[string][]FeeRate {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].MerchantID != rows[j].MerchantID {
			return rows[i].MerchantID < rows[j].MerchantID
		}
		return rows[i].EffectiveFrom.Before(rows[j].EffectiveFrom)
	})
	out := make(map[string][]FeeRate)
	for _, row := range rows {
		out[row.MerchantID] = append(out[row.MerchantID], row)
	}
	return out
}

func latestTier(tiers []FeeRate, createdAt time.Time) (FeeRate, bool) {
	i := sort.Search(len(tiers), func(i int) bool {
		return tiers[i].EffectiveFrom.After(createdAt)
	})
	if i == 0 {
		return FeeRate{}, false
	}
	return tiers[i-1], true
}

const orderScanSelect = `
SELECT id, pay_no, merchant_id, amount, fee_amount, created_at
FROM payment_order
WHERE id > ?`

const orderScanTail = `
ORDER BY id
LIMIT ?`

const (
	orderScanAllSQL    = orderScanSelect + orderScanTail
	orderScanFromSQL   = orderScanSelect + " AND created_at >= ?" + orderScanTail
	orderScanToSQL     = orderScanSelect + " AND created_at < ?" + orderScanTail
	orderScanFromToSQL = orderScanSelect + " AND created_at >= ? AND created_at < ?" + orderScanTail
)

func orderScanQuery(from, to time.Time) string {
	switch {
	case !from.IsZero() && !to.IsZero():
		return orderScanFromToSQL
	case !from.IsZero():
		return orderScanFromSQL
	case !to.IsZero():
		return orderScanToSQL
	default:
		return orderScanAllSQL
	}
}

func orderScanArgs(after uint64, from, to time.Time, limit int) []any {
	args := []any{after}
	if !from.IsZero() {
		args = append(args, from)
	}
	if !to.IsZero() {
		args = append(args, to)
	}
	args = append(args, limit)
	return args
}

const listRatesSQL = `
SELECT merchant_id, fee_rate_bp, effective_from
FROM merchant_fee_rate
ORDER BY merchant_id, effective_from`

type sqlCheckTx struct {
	tx *sqlx.Tx
}

func (s sqlCheckTx) listRates(ctx context.Context) ([]FeeRate, error) {
	rows, err := s.tx.QueryContext(ctx, listRatesSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []FeeRate
	for rows.Next() {
		var row FeeRate
		if err := rows.Scan(&row.MerchantID, &row.RateBP, &row.EffectiveFrom); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s sqlCheckTx) listOrders(ctx context.Context, afterID uint64, from, to time.Time, limit int) ([]orderRow, error) {
	rows, err := s.tx.QueryContext(ctx, orderScanQuery(from, to), orderScanArgs(afterID, from, to, limit)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []orderRow
	for rows.Next() {
		var row orderRow
		var amount, fee int64
		if err := rows.Scan(&row.ID, &row.PayNo, &row.MerchantID, &amount, &fee, &row.CreatedAt); err != nil {
			return nil, err
		}
		row.Amount = money.Amount(amount)
		row.FeeAmount = money.Amount(fee)
		out = append(out, row)
	}
	return out, rows.Err()
}
