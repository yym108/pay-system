package money

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"testing"
)

// BASE-04 验收 1：手续费向量
func TestFeeVectors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		amount Amount
		rateBP int
		want   Amount
	}{
		{name: "below one fen", amount: 1, rateBP: 60, want: 0},
		{name: "100 yuan at 60bp", amount: 10000, rateBP: 60, want: 60},
		{name: "round to 50", amount: 8333, rateBP: 60, want: 50},
		{name: "half up", amount: 1, rateBP: 5000, want: 1},
		{name: "max pay full rate", amount: 10000000, rateBP: 10000, want: 10000000},
		{name: "zero rate", amount: 123456, rateBP: 0, want: 0},
		{name: "zero rate at zero", amount: 0, rateBP: 0, want: 0},
		{name: "zero rate at max int", amount: math.MaxInt64, rateBP: 0, want: 0},
		{name: "zero rate at min int", amount: math.MinInt64, rateBP: 0, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Fee(tc.amount, tc.rateBP)
			if err != nil {
				t.Fatalf("Fee(%d, %d) error: %v", tc.amount, tc.rateBP, err)
			}
			if got != tc.want {
				t.Errorf("Fee(%d, %d) = %d, want %d", tc.amount, tc.rateBP, got, tc.want)
			}
		})
	}
}

// BASE-04 规则 3：费率越界、负金额返回错误；大额乘法与公式一致且不回绕。
func TestFeeRejectsAndLargeAmount(t *testing.T) {
	t.Parallel()
	rejects := []struct {
		name   string
		amount Amount
		rateBP int
		want   error
	}{
		{name: "rate negative", amount: 10000, rateBP: -1, want: ErrRateOutOfRange},
		{name: "rate above 10000", amount: 10000, rateBP: 10001, want: ErrRateOutOfRange},
		{name: "negative amount", amount: -1, rateBP: 60, want: ErrNegativeAmount},
		{name: "min int amount", amount: math.MinInt64, rateBP: 1, want: ErrNegativeAmount},
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Fee(tc.amount, tc.rateBP)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Fee(%d, %d) = %d, err %v, want %v", tc.amount, tc.rateBP, got, err, tc.want)
			}
		})
	}

	edges := []struct {
		amount Amount
		rateBP int
	}{
		{amount: 0, rateBP: 60},
		{amount: 4999, rateBP: 1},
		{amount: 5000, rateBP: 1},
		{amount: math.MaxInt64, rateBP: 1},
		{amount: math.MaxInt64, rateBP: 9999},
		{amount: math.MaxInt64, rateBP: 10000},
	}
	for _, tc := range edges {
		t.Run(fmt.Sprintf("%d_%d", tc.amount, tc.rateBP), func(t *testing.T) {
			t.Parallel()
			got, err := Fee(tc.amount, tc.rateBP)
			if err != nil {
				t.Fatalf("Fee(%d, %d) error: %v", tc.amount, tc.rateBP, err)
			}
			want := feeByBig(tc.amount, tc.rateBP)
			if got != want {
				t.Errorf("Fee(%d, %d) = %d, want %d", tc.amount, tc.rateBP, got, want)
			}
		})
	}
}

// BASE-04 验收 2：Add(MaxInt64, 1) 返回溢出错误。
func TestAddMaxInt64Overflow(t *testing.T) {
	t.Parallel()
	got, err := Add(math.MaxInt64, 1)
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("Add(MaxInt64, 1) = %d, err %v, want overflow", got, err)
	}
	if got != 0 {
		t.Fatalf("Add(MaxInt64, 1) = %d, want 0 on overflow", got)
	}
}

// BASE-04 规则 2：Add、Sub 在 int64 边界（含负数与 MinInt64）不回绕。
func TestAddSubBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		op   string
		a, b Amount
		want Amount
		err  error
	}{
		{name: "add zero", op: "add", a: math.MaxInt64, b: 0, want: math.MaxInt64},
		{name: "add down from max", op: "add", a: math.MaxInt64, b: -1, want: math.MaxInt64 - 1},
		{name: "add max and min", op: "add", a: math.MaxInt64, b: math.MinInt64, want: -1},
		{name: "add min and one", op: "add", a: math.MinInt64, b: 1, want: math.MinInt64 + 1},
		{name: "add min and zero", op: "add", a: math.MinInt64, b: 0, want: math.MinInt64},
		{name: "add min and max", op: "add", a: math.MinInt64, b: math.MaxInt64, want: -1},
		{name: "add negatives", op: "add", a: -5, b: -3, want: -8},
		{name: "add positives", op: "add", a: 5, b: 3, want: 8},
		{name: "add to zero", op: "add", a: -1, b: 1, want: 0},
		{name: "add max overflow", op: "add", a: math.MaxInt64, b: 1, err: ErrOverflow},
		{name: "add two max", op: "add", a: math.MaxInt64, b: math.MaxInt64, err: ErrOverflow},
		{name: "add min overflow", op: "add", a: math.MinInt64, b: -1, err: ErrOverflow},
		{name: "add two min", op: "add", a: math.MinInt64, b: math.MinInt64, err: ErrOverflow},

		{name: "sub one from max", op: "sub", a: math.MaxInt64, b: 1, want: math.MaxInt64 - 1},
		{name: "sub self", op: "sub", a: math.MaxInt64, b: math.MaxInt64, want: 0},
		{name: "sub zero", op: "sub", a: math.MinInt64, b: 0, want: math.MinInt64},
		{name: "sub negative from min", op: "sub", a: math.MinInt64, b: -1, want: math.MinInt64 + 1},
		{name: "sub min from min", op: "sub", a: math.MinInt64, b: math.MinInt64, want: 0},
		{name: "sub min from negative one", op: "sub", a: -1, b: math.MinInt64, want: math.MaxInt64},
		{name: "sub negatives", op: "sub", a: -1, b: 1, want: -2},
		{name: "sub to two", op: "sub", a: 1, b: -1, want: 2},
		{name: "sub max overflow", op: "sub", a: math.MaxInt64, b: -1, err: ErrOverflow},
		{name: "sub min from max", op: "sub", a: math.MaxInt64, b: math.MinInt64, err: ErrOverflow},
		{name: "sub one from min", op: "sub", a: math.MinInt64, b: 1, err: ErrOverflow},
		{name: "sub max from min", op: "sub", a: math.MinInt64, b: math.MaxInt64, err: ErrOverflow},
		{name: "sub min from zero", op: "sub", a: 0, b: math.MinInt64, err: ErrOverflow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var (
				got Amount
				err error
			)
			if tc.op == "add" {
				got, err = Add(tc.a, tc.b)
			} else {
				got, err = Sub(tc.a, tc.b)
			}
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("%s(%d, %d) = %d, err %v, want %v", tc.op, tc.a, tc.b, got, err, tc.err)
				}
				if got != 0 {
					t.Fatalf("%s(%d, %d) = %d, want 0 on overflow", tc.op, tc.a, tc.b, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s(%d, %d) error: %v", tc.op, tc.a, tc.b, err)
			}
			if got != tc.want {
				t.Errorf("%s(%d, %d) = %d, want %d", tc.op, tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// BASE-04 验收 3：元格式只用于展示。
func TestAmountStringYuan(t *testing.T) {
	t.Parallel()
	cases := []struct {
		amount Amount
		want   string
	}{
		{amount: 1, want: "0.01"},
		{amount: -150, want: "-1.50"},
		{amount: 0, want: "0.00"},
		{amount: 10, want: "0.10"},
		{amount: 100, want: "1.00"},
		{amount: 10000, want: "100.00"},
		{amount: -1, want: "-0.01"},
		{amount: -10, want: "-0.10"},
		{amount: -100, want: "-1.00"},
		{amount: math.MaxInt64, want: "92233720368547758.07"},
		{amount: math.MinInt64, want: "-92233720368547758.08"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			if got := tc.amount.String(); got != tc.want {
				t.Errorf("Amount(%d).String() = %q, want %q", tc.amount, got, tc.want)
			}
		})
	}
}

// BASE-04 规则 4：单笔支付、退款、转账金额 1–10,000,000 分。
func TestValidatePayAmount(t *testing.T) {
	t.Parallel()
	if MinPayAmount != 1 || MaxPayAmount != 10_000_000 {
		t.Fatalf("pay bounds = [%d, %d], want [1, 10000000]", MinPayAmount, MaxPayAmount)
	}
	cases := []struct {
		name   string
		amount Amount
		ok     bool
	}{
		{name: "min", amount: MinPayAmount, ok: true},
		{name: "max", amount: MaxPayAmount, ok: true},
		{name: "zero", amount: 0, ok: false},
		{name: "negative", amount: -1, ok: false},
		{name: "above max", amount: MaxPayAmount + 1, ok: false},
		{name: "max int", amount: math.MaxInt64, ok: false},
		{name: "min int", amount: math.MinInt64, ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidatePayAmount(tc.amount)
			if tc.ok {
				if err != nil {
					t.Fatalf("ValidatePayAmount(%d) error: %v", tc.amount, err)
				}
				return
			}
			if !errors.Is(err, ErrPayAmount) {
				t.Fatalf("ValidatePayAmount(%d) err %v, want %v", tc.amount, err, ErrPayAmount)
			}
		})
	}
}

// feeByBig 用大整数复算手续费公式，避免测试和实现共用会回绕的 int64 乘法。
func feeByBig(amount Amount, rateBP int) Amount {
	var prod, half, base, q big.Int
	prod.Mul(big.NewInt(int64(amount)), big.NewInt(int64(rateBP)))
	half.SetInt64(5000)
	prod.Add(&prod, &half)
	base.SetInt64(10000)
	q.Quo(&prod, &base)
	if !q.IsInt64() {
		panic(fmt.Sprintf("fee(%d, %d) = %s does not fit in int64", amount, rateBP, q.String()))
	}
	return Amount(q.Int64())
}
