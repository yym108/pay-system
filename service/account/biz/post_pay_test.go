package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/dal"
)

func TestPostPayRejectsIllegalFee(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		payNo, merchant, channel string
		amount, fee              int64
	}{
		{"", "M100002", "mockpay", 10, 1},
		{"P1", "BAD", "mockpay", 10, 1},
		{"P1", "M100002", "", 10, 1},
		{"P1", "M100002", "mockpay", 0, 0},
		{"P1", "M100002", "mockpay", 10, 11},
		{"P1", "M100002", "mockpay", 10, -1},
	}
	for i, c := range cases {
		_, err := (*Service)(nil).PostPay(ctx, c.payNo, c.merchant, c.channel, money.Amount(c.amount), money.Amount(c.fee))
		if !errors.Is(err, errno.ErrInvalidParam) {
			t.Fatalf("case %d err = %v", i, err)
		}
	}
}

func TestPostPayNilAndCanceled(t *testing.T) {
	_, err := (*Service)(nil).PostPay(context.Background(), "P1", "M100002", "mockpay", 10, 1)
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := New(dal.New(nil, nil), newIDs(t, stubClock{now: openTestTime}), testSubCount)
	_, err = svc.PostPay(ctx, "P1", "M100002", "mockpay", 10, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_, err = New(nil, newIDs(t, stubClock{now: openTestTime}), testSubCount).PostPay(context.Background(), "P1", "M100002", "mockpay", 10, 0)
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatal(err)
	}
}
