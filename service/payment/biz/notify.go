package biz

import (
	"context"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
)

// ApplyChannelNotify 把 PAY 回调交给已有的 ApplyPayResult。
// 提交成功之后才调用故障点 callback.after_commit。
func (p *Payments) ApplyChannelNotify(ctx context.Context, in PayResult) (string, error) {
	if err := validatePayResult(in); err != nil {
		return "", err
	}
	if in.Source != paySourceNotify {
		return "", errno.ErrInvalidParam
	}
	if p == nil || p.orders == nil {
		return "", errno.ErrInternal
	}
	before, ok, err := p.orders.channelTxnByBizNo(ctx, in.BizNo)
	if err != nil {
		return "", callErr(err)
	}
	order, err := p.ApplyPayResult(ctx, in)
	if err != nil {
		return "", err
	}
	fault.Point(ctx, "callback.after_commit")
	if order.PayNo == "" {
		return "", errno.ErrInternal
	}
	if ok && isPayTerminal(before.Status) && before.Status != in.Result {
		return "CONFLICT", nil
	}
	if ok && before.Status == in.Result {
		return "DUPLICATE", nil
	}
	return "APPLIED", nil
}
