package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/yym108/pay-system/pkg/errno"
)

// PAY-04：未知 result、未知 source、空 biz_no，以及 SUCCESS 未传完成时间，都是 10001，不写库。
func TestApplyRejectsParams(t *testing.T) {
	ctx := context.Background()
	valid := PayResult{BizNo: "PA0000000000000000001", Result: payResultFailed, Source: paySourceSync}
	var bare *Payments
	if _, err := bare.ApplyPayResult(ctx, PayResult{}); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("nil receiver empty = %v", err)
	}
	if _, err := bare.ApplyPayResult(ctx, PayResult{
		BizNo: "PA1", Result: payResultSuccess, Source: paySourceNotify,
	}); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("nil receiver success without time = %v", err)
	}
	if _, err := bare.ApplyPayResult(ctx, valid); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil receiver valid = %v", err)
	}

	p, orders, order := seedInit(t)
	finished := feeAt(8, 0, 0, 0)
	cases := []PayResult{
		{},
		{Result: payResultFailed, Source: paySourceSync},
		{BizNo: order.PayNo, Result: "PAYING", Source: paySourceNotify, FinishedAt: finished},
		{BizNo: order.PayNo, Result: payResultSuccess, Source: "WEB", FinishedAt: finished},
		{BizNo: order.PayNo, Result: payResultSuccess, Source: paySourceNotify},
		{BizNo: order.PayNo, Result: payResultClosed, Source: ""},
	}
	for _, in := range cases {
		_, err := p.ApplyPayResult(ctx, in)
		assertErrno(t, err, errno.ErrInvalidParam)
	}
	if orders.rows[0].Status != payStatusInit || len(orders.txns) != 0 {
		t.Fatalf("invalid params wrote status=%s txns=%d", orders.rows[0].Status, len(orders.txns))
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := p.ApplyPayResult(done, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx = %v", err)
	}
	if orders.rows[0].Status != payStatusInit || len(orders.txns) != 0 {
		t.Fatalf("canceled ctx wrote status=%s txns=%d", orders.rows[0].Status, len(orders.txns))
	}
}
