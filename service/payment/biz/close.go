package biz

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
)

// CloseInput 是商户关单的入参。PayNo 为空是 10001，不读库。
type CloseInput struct {
	MerchantID string
	PayNo      string
}

// UseClose 装上渠道关单。不改 UsePay 已经装上的受理客户端。
func (p *Payments) UseClose(closer channelCloser) {
	if p == nil {
		return
	}
	p.closer = closer
}

// ClosePayment 按当前状态关闭支付单。
// 请求为 nil 或 pay_no 为空返回 10001，不写库。
// 订单不存在，或商户号与库中字节不同，返回 30001。
// INIT 只做本地 CAS。CLOSED 幂等返回。SUCCESS 与 FAILED 返回 30003 和当前订单，不调用渠道。
// PAYING 在事务外调用渠道。本地还没有渠道流水号时先查单，查无此单返回 10004，不调用关单。
// 已有流水号，或查单确认渠道已有行，再关单并按结果调用 ApplyPayResult。调用失败返回 10004，订单保持 PAYING。
func (p *Payments) ClosePayment(ctx context.Context, in *CloseInput) (PaymentOrder, error) {
	if in == nil || in.PayNo == "" {
		return PaymentOrder{}, errno.ErrInvalidParam
	}
	if p == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	if err := p.ready(ctx); err != nil {
		return PaymentOrder{}, err
	}
	order, ok, err := p.orders.paymentByPayNo(ctx, in.PayNo)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !ok || order.PayNo != in.PayNo || order.MerchantID != in.MerchantID {
		return PaymentOrder{}, errno.ErrOrderNotFound
	}
	return p.closeStatus(ctx, in, order)
}

func (p *Payments) closeStatus(ctx context.Context, in *CloseInput, order PaymentOrder) (PaymentOrder, error) {
	switch order.Status {
	case payStatusInit:
		return p.closeInit(ctx, in, order)
	case payStatusClosed:
		return order, nil
	case payStatusSuccess, payStatusFailed:
		return order, errno.ErrOrderStatusConflict
	case payStatusPaying:
		return p.closePaying(ctx, order)
	default:
		return PaymentOrder{}, errno.ErrInternal
	}
}

func (p *Payments) closeInit(ctx context.Context, in *CloseInput, order PaymentOrder) (PaymentOrder, error) {
	now := p.clk.Now().Truncate(time.Millisecond)
	won, err := p.orders.casInitToClosed(ctx, order, now)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !won {
		return p.afterInitMiss(ctx, in)
	}
	order.Status = payStatusClosed
	order.ClosedAt = now
	order.Version++
	order.UpdatedAt = now
	return order, nil
}

func (p *Payments) afterInitMiss(ctx context.Context, in *CloseInput) (PaymentOrder, error) {
	order, ok, err := p.orders.paymentByPayNo(ctx, in.PayNo)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !ok || order.PayNo != in.PayNo || order.MerchantID != in.MerchantID {
		return PaymentOrder{}, errno.ErrOrderNotFound
	}
	switch order.Status {
	case payStatusClosed:
		return order, nil
	case payStatusPaying:
		return p.closePaying(ctx, order)
	case payStatusSuccess, payStatusFailed:
		return order, errno.ErrOrderStatusConflict
	default:
		return PaymentOrder{}, errno.ErrInternal
	}
}

func (p *Payments) closePaying(ctx context.Context, order PaymentOrder) (PaymentOrder, error) {
	if p.closer == nil || p.applier == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	// 受理事务已提交但渠道还没写入 mock_txn 时，直接关单会插入 amount 0 的占位。
	// 没有渠道流水号就先查单；NOT_FOUND 不关单。
	visible, err := p.channelTxnVisible(ctx, order.PayNo)
	if err != nil {
		return PaymentOrder{}, err
	}
	if !visible {
		q, qerr := p.closer.QueryPay(ctx, order.PayNo)
		if qerr != nil {
			return PaymentOrder{}, p.channelCloseErr(ctx, order.PayNo, qerr)
		}
		if !q.hasRow() {
			return PaymentOrder{}, errno.ErrUnavailable
		}
	}
	return p.closeOnChannel(ctx, order.PayNo, paySourceClose)
}

// closeOnChannel 调用渠道关单，并按结果调用 ApplyPayResult，source 写入这次结果。
// 商户关单（PAY-06）传 CLOSE，查单补偿遇到 NOT_FOUND（PAY-05）传 QUERY。
// CLOSED 成功返回订单且 error 为 nil；ALREADY_PAID 再查单写 SUCCESS，渠道 FAILED 写 FAILED，这两种返回 30003 和当前订单。
// 调用失败返回 10004，订单保持 PAYING。
func (p *Payments) closeOnChannel(ctx context.Context, payNo, source string) (PaymentOrder, error) {
	if p.closer == nil || p.applier == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	res, err := p.closer.ClosePay(ctx, payNo)
	if err != nil {
		return PaymentOrder{}, p.channelCloseErr(ctx, payNo, err)
	}
	switch res.Outcome {
	case closeClosed:
		return p.applyClosed(ctx, PayResult{
			BizNo:  payNo,
			Result: payResultClosed,
			Source: source,
		})
	case closeAlreadyPaid:
		return p.applyAlreadyPaid(ctx, payNo, source)
	case closeTxnFailed:
		return p.applyTerminal(ctx, PayResult{
			BizNo:    payNo,
			Result:   payResultFailed,
			FailCode: failCodeChannelFailed,
			Source:   source,
		})
	default:
		return PaymentOrder{}, errno.ErrUnavailable
	}
}

// channelTxnVisible 为真表示本地已有非空 channel_txn_no，可以直接关单。
// 没有这行，或流水号仍为空，返回 false。读库失败按 callErr 返回。
func (p *Payments) channelTxnVisible(ctx context.Context, payNo string) (bool, error) {
	txn, ok, err := p.orders.channelTxnByBizNo(ctx, payNo)
	if err != nil {
		return false, callErr(err)
	}
	return ok && txn.ChannelTxnNo != "", nil
}

func (p *Payments) channelCloseErr(ctx context.Context, payNo string, err error) error {
	if isLocalChannelErr(err) {
		slog.ErrorContext(ctx, "channel close config",
			"pay_no", payNo,
			"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
		)
		return errno.ErrInternal
	}
	return errno.ErrUnavailable
}

func (p *Payments) applyClosed(ctx context.Context, in PayResult) (PaymentOrder, error) {
	got, err := p.applier.ApplyPayResult(ctx, in)
	if err != nil {
		return got, err
	}
	if got.Status != payStatusClosed {
		return conflictIfTerminal(got)
	}
	return got, nil
}

func (p *Payments) applyAlreadyPaid(ctx context.Context, payNo, source string) (PaymentOrder, error) {
	q, err := p.closer.QueryPay(ctx, payNo)
	if err != nil {
		return PaymentOrder{}, p.channelCloseErr(ctx, payNo, err)
	}
	if !q.OK {
		return PaymentOrder{}, errno.ErrUnavailable
	}
	amount := q.Amount
	return p.applyTerminal(ctx, PayResult{
		BizNo:        payNo,
		Result:       payResultSuccess,
		Source:       source,
		Amount:       &amount,
		ChannelTxnNo: q.ChannelTxnNo,
		FinishedAt:   q.FinishedAt,
	})
}

func (p *Payments) applyTerminal(ctx context.Context, in PayResult) (PaymentOrder, error) {
	got, err := p.applier.ApplyPayResult(ctx, in)
	if err != nil {
		return got, err
	}
	return conflictIfTerminal(got)
}

func conflictIfTerminal(got PaymentOrder) (PaymentOrder, error) {
	switch got.Status {
	case payStatusSuccess, payStatusFailed, payStatusClosed:
		return got, errno.ErrOrderStatusConflict
	default:
		return PaymentOrder{}, errno.ErrInternal
	}
}
