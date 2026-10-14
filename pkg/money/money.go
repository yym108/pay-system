// Package money 提供以分为单位的整数金额运算。
// 全链路禁止用浮点数表示金额。String 只用于日志和展示，不参与计算。
package money

import (
	"errors"
	"fmt"
	"math"
)

const (
	feeBase      Amount = 10_000
	feeRoundHalf Amount = feeBase / 2
	maxRateBP           = 10_000
	centsPerYuan Amount = 100

	// MinPayAmount 是单笔支付、退款、转账的最小金额，单位分。
	MinPayAmount Amount = 1
	// MaxPayAmount 是单笔支付、退款、转账的最大金额，单位分（100,000.00 元）。
	MaxPayAmount Amount = 10_000_000
)

var (
	// ErrOverflow 表示 Add 或 Sub 的结果超出 int64。
	ErrOverflow = errors.New("money: amount overflow")
	// ErrRateOutOfRange 表示费率不在 0–10000 基点。
	ErrRateOutOfRange = errors.New("money: rate_bp out of range")
	// ErrNegativeAmount 表示手续费金额为负。四舍五入只对非负金额成立。
	ErrNegativeAmount = errors.New("money: negative amount")
	// ErrPayAmount 表示金额不在单笔支付、退款、转账允许的范围内。
	ErrPayAmount = errors.New("money: pay amount out of range")
)

// Amount 是以分为单位的金额。不要用浮点数表示或运算金额。
type Amount int64

// Add 返回 a+b。和超出 int64 时返回 ErrOverflow 和 0，不回绕。
// 先比边界再相加：b 为负时 MinInt64-b 仍在 int64 内，比较本身不会溢出。
func Add(a, b Amount) (Amount, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, ErrOverflow
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, ErrOverflow
	}
	return a + b, nil
}

// Sub 返回 a-b。差超出 int64 时返回 ErrOverflow 和 0，不回绕。
// b 为 MinInt64 时上界 MaxInt64+b 等于 -1，比较本身不会溢出。
func Sub(a, b Amount) (Amount, error) {
	if b > 0 && a < math.MinInt64+b {
		return 0, ErrOverflow
	}
	if b < 0 && a > math.MaxInt64+b {
		return 0, ErrOverflow
	}
	return a - b, nil
}

// Fee 计算手续费，单位分：(amount×rateBP+5000)/10000，对非负金额四舍五入到分。
// rateBP 必须在 0–10000，越界返回 ErrRateOutOfRange。
// rateBP 为 0 时结果为 0。amount 为负且 rateBP 不为 0 时返回 ErrNegativeAmount。
func Fee(amount Amount, rateBP int) (Amount, error) {
	if rateBP < 0 || rateBP > maxRateBP {
		return 0, fmt.Errorf("money: rate_bp %d: %w", rateBP, ErrRateOutOfRange)
	}
	if rateBP == 0 {
		return 0, nil
	}
	if amount < 0 {
		return 0, fmt.Errorf("money: fee amount %d: %w", amount, ErrNegativeAmount)
	}
	// 拆成整万和余数再乘，避免 amount×rateBP 超出 int64。
	// rateBP≤10000 时两部分以及它们的和都落在 int64 内。
	rate := Amount(rateBP)
	q := amount / feeBase
	r := amount % feeBase
	return q*rate + (r*rate+feeRoundHalf)/feeBase, nil
}

// ValidatePayAmount 校验单笔支付、退款、转账金额。
// 合法范围是 MinPayAmount 到 MaxPayAmount，含两端。越界返回 ErrPayAmount。
func ValidatePayAmount(amount Amount) error {
	if amount < MinPayAmount || amount > MaxPayAmount {
		return fmt.Errorf("money: pay amount %d: %w", amount, ErrPayAmount)
	}
	return nil
}

// String 返回金额的“元”表示，固定两位小数。100 分是 "1.00"，-150 分是 "-1.50"。
// 只用于日志和展示，不参与计算。
func (a Amount) String() string {
	if a < 0 {
		if a == math.MinInt64 {
			// 不能对 MinInt64 取相反数。其绝对值是 1<<63。
			const abs uint64 = 1 << 63
			return fmt.Sprintf("-%d.%02d", abs/uint64(centsPerYuan), abs%uint64(centsPerYuan))
		}
		v := -a
		return fmt.Sprintf("-%d.%02d", v/centsPerYuan, v%centsPerYuan)
	}
	return fmt.Sprintf("%d.%02d", a/centsPerYuan, a%centsPerYuan)
}
