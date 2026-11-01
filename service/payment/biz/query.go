package biz

import (
	"context"

	"github.com/yym108/pay-system/pkg/errno"
)

// QueryInput 是支付单查询的入参。
// PayNo 与 OutTradeNo 二选一。空串算没提供，不 trim。
type QueryInput struct {
	MerchantID string
	PayNo      string
	OutTradeNo string
}

// QueryPayment 直接读取 payment_order。不缓存，不写行，不推进状态，不改 version。
// 请求为 nil，或两个单号都空、都非空，返回 10001，不读库。
// 没有行，或 pay_no 路径上商户号与库中逐字节不同，返回 30001。
// 只提供 out_trade_no 时，没有行（含他人的商户订单号）也是 30001。两种情况是同一个错误。
func (p *Payments) QueryPayment(ctx context.Context, in *QueryInput) (PaymentOrder, error) {
	if !exactlyOneQueryKey(in) {
		return PaymentOrder{}, errno.ErrInvalidParam
	}
	if p == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	if err := p.ready(ctx); err != nil {
		return PaymentOrder{}, err
	}
	if in.PayNo != "" {
		return p.queryByPayNo(ctx, in)
	}
	return p.queryByOutTradeNo(ctx, in)
}

func exactlyOneQueryKey(in *QueryInput) bool {
	if in == nil {
		return false
	}
	return (in.PayNo != "") != (in.OutTradeNo != "")
}

func (p *Payments) queryByPayNo(ctx context.Context, in *QueryInput) (PaymentOrder, error) {
	order, ok, err := p.orders.paymentByPayNo(ctx, in.PayNo)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	// 没有行和商户号字节不同都是 30001，调用方看不出订单在不在。空商户号同样比较。
	if !ok || order.PayNo != in.PayNo || order.MerchantID != in.MerchantID {
		return PaymentOrder{}, errno.ErrOrderNotFound
	}
	return order, nil
}

func (p *Payments) queryByOutTradeNo(ctx context.Context, in *QueryInput) (PaymentOrder, error) {
	order, ok, err := p.orders.paymentByOutTradeNo(ctx, in.MerchantID, in.OutTradeNo)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !ok || order.MerchantID != in.MerchantID || order.OutTradeNo != in.OutTradeNo {
		return PaymentOrder{}, errno.ErrOrderNotFound
	}
	return order, nil
}
