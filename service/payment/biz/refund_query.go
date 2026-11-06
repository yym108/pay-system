package biz

import (
	"context"

	"github.com/yym108/pay-system/pkg/errno"
)

// RefundQueryInput 是退款查询的入参。
// RefundNo 与 OutRefundNo 二选一。空串算没提供，不 trim。
type RefundQueryInput struct {
	MerchantID  string
	RefundNo    string
	OutRefundNo string
}

// QueryRefund 转给退款查询。payments 为 nil 时返回 10003。
func (a *App) QueryRefund(ctx context.Context, in *RefundQueryInput) (RefundOrder, error) {
	if a == nil || a.payments == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	return a.payments.QueryRefund(ctx, in)
}

// QueryRefund 直接读取 refund_order，规则同 QueryPayment。不缓存，不写行，不推进状态，不改 version。
// 请求为 nil，或两个单号都空、都非空，返回 10001，不读库。
// 没有行，或 refund_no 路径上商户号与库中逐字节不同，返回 30001。
// 只提供 out_refund_no 时，没有行（含他人的商户退款单号）也是 30001。两种情况是同一个错误。
func (p *Payments) QueryRefund(ctx context.Context, in *RefundQueryInput) (RefundOrder, error) {
	if !exactlyOneRefundKey(in) {
		return RefundOrder{}, errno.ErrInvalidParam
	}
	if p == nil || p.orders == nil || ctx == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return RefundOrder{}, err
	}
	var (
		row RefundOrder
		ok  bool
		err error
	)
	if in.RefundNo != "" {
		row, ok, err = p.orders.refundByNo(ctx, in.RefundNo)
	} else {
		row, ok, err = p.orders.refundByMerchantOut(ctx, in.MerchantID, in.OutRefundNo)
	}
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	// 库列排序规则可能放宽匹配，读回后逐字节比较。没有行与商户号不同都是 30001，调用方看不出退款单在不在。
	if !ok || row.MerchantID != in.MerchantID ||
		(in.RefundNo != "" && row.RefundNo != in.RefundNo) ||
		(in.OutRefundNo != "" && row.OutRefundNo != in.OutRefundNo) {
		return RefundOrder{}, errno.ErrOrderNotFound
	}
	return row, nil
}

func exactlyOneRefundKey(in *RefundQueryInput) bool {
	if in == nil {
		return false
	}
	return (in.RefundNo != "") != (in.OutRefundNo != "")
}
