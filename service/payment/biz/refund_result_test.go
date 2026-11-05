package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/yym108/pay-system/pkg/errno"
)

// REF-03 规则 4（同 PAY-04 第 2 条）：空单号、未知结果、未知来源、SUCCESS 未带完成时间、失败原因与结果不符，都是 10001，不写库。
func TestApplyRefundResultRejectsParams(t *testing.T) {
	ctx := context.Background()
	finished := feeAt(8, 0, 0, 0)
	cases := map[string]RefundResult{
		"empty":                {},
		"no refund_no":         {Result: payResultFailed, Source: paySourceSync, FailCode: failCodeInsufficientBalance},
		"closed result":        {RefundNo: "RF1", Result: payResultClosed, Source: paySourceNotify, FinishedAt: finished},
		"unknown source":       {RefundNo: "RF1", Result: payResultSuccess, Source: "WEB", FinishedAt: finished},
		"success without time": {RefundNo: "RF1", Result: payResultSuccess, Source: paySourceNotify},
		"success with code":    {RefundNo: "RF1", Result: payResultSuccess, Source: paySourceNotify, FinishedAt: finished, FailCode: failCodeChannelFailed},
		"unknown fail_code":    {RefundNo: "RF1", Result: payResultFailed, Source: paySourceSync, FailCode: "OTHER"},
		"pay fail_code":        {RefundNo: "RF1", Result: payResultFailed, Source: paySourceSync, FailCode: "ACCOUNT_INVALID"},
	}
	orders := newRefundOrders()
	p, _ := refundSubmitPayments(orders)
	var bare *Payments
	for name, in := range cases {
		_, err := p.ApplyRefundResult(ctx, in)
		if !errors.Is(err, errno.ErrInvalidParam) {
			t.Fatalf("%s: err = %v", name, err)
		}
		if _, err = bare.ApplyRefundResult(ctx, in); !errors.Is(err, errno.ErrInvalidParam) {
			t.Fatalf("%s nil receiver: err = %v", name, err)
		}
	}
	if len(copiedRefunds(orders)) != 0 || len(copiedTxns(orders)) != 0 {
		t.Fatal("invalid params wrote rows")
	}
}

// 合法入参但依赖缺失：nil 接收者、nil ctx、内存存储（没有事务）都是 10003；已取消的 ctx 原样返回。
func TestApplyRefundResultDependencies(t *testing.T) {
	valid := RefundResult{RefundNo: "RF0000000000000000001", Result: payResultFailed, FailCode: failCodeInsufficientBalance, Source: paySourceSync}
	var bare *Payments
	if _, err := bare.ApplyRefundResult(context.Background(), valid); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil receiver = %v", err)
	}
	p, _ := refundSubmitPayments(newRefundOrders())
	if _, err := p.ApplyRefundResult(nil, valid); !errors.Is(err, errno.ErrInternal) { //nolint:staticcheck // 覆盖 nil ctx。
		t.Fatalf("nil ctx = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.ApplyRefundResult(canceled, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	if _, err := p.ApplyRefundResult(context.Background(), valid); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("mem store = %v", err)
	}
	if _, err := (&Payments{}).ApplyRefundResult(context.Background(), valid); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("empty payments = %v", err)
	}
}

// 回调入口只接受 source = NOTIFY；其余入参校验同 ApplyRefundResult。
func TestApplyRefundNotifyRejects(t *testing.T) {
	ctx := context.Background()
	finished := feeAt(8, 0, 0, 0)
	p, _ := refundSubmitPayments(newRefundOrders())
	if _, err := p.ApplyRefundNotify(ctx, RefundResult{}); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("empty = %v", err)
	}
	query := RefundResult{RefundNo: "RF1", Result: payResultSuccess, Source: paySourceQuery, FinishedAt: finished}
	if _, err := p.ApplyRefundNotify(ctx, query); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("query source = %v", err)
	}
	notify := RefundResult{RefundNo: "RF1", Result: payResultSuccess, Source: paySourceNotify, FinishedAt: finished}
	if _, err := (&Payments{}).ApplyRefundNotify(ctx, notify); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil store = %v", err)
	}
	var bare *Payments
	if _, err := bare.ApplyRefundNotify(ctx, notify); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil receiver = %v", err)
	}
	var app *App
	if _, err := app.ApplyRefundNotify(ctx, notify); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil app = %v", err)
	}
}
