package biz

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/money"
)

const (
	refundStatusProcessing      = "PROCESSING"
	refundStatusFailed          = "FAILED"
	failCodeInsufficientBalance = "INSUFFICIENT_BALANCE"
	failCodeFreezeConflict      = "FREEZE_CONFLICT"
	txnTypeRefund               = "REFUND"
	freezeStatusFrozen          = "FROZEN"
	freezeStatusConfirmed       = "CONFIRMED"
	faultRefundAfterFreeze      = "refund.after_freeze"
	channelRefundPath           = "/mockpay/v1/refund"
)

// RefundFreeze 是冻结调用的结果。Status 为 FROZEN 才继续提交渠道。
type RefundFreeze struct {
	Status string
}

// RefundFreezer 按商户号冻结结算账户。payment 不查账户号。
type RefundFreezer interface {
	FreezeRefund(ctx context.Context, merchantID, refundNo string, amount money.Amount) (RefundFreeze, error)
}

// ChannelRefundRequest 是发往渠道的退款参数。NotifyURL 是平台回调地址。
type ChannelRefundRequest struct {
	ReqNo     string
	OrigReqNo string
	Amount    money.Amount
	NotifyURL string
}

// ChannelRefunder 提交一笔渠道退款。实现不得把密钥写入日志或错误。
type ChannelRefunder interface {
	Refund(ctx context.Context, req ChannelRefundRequest) (ChannelAcceptResult, error)
}

// RefundResult 是交给 ApplyRefundResult 的失败结果。本切片不落库。
type RefundResult struct {
	RefundNo string
	Result   string
	FailCode string
	FailMsg  string
	Source   string
}

// RefundResultApplier 把退款推进到终态。生产实现要等 REF-03；本切片的方法只返回 10003。
type RefundResultApplier interface {
	ApplyRefundResult(ctx context.Context, in RefundResult) (RefundOrder, error)
}

// *Payments.ApplyRefundResult 是占位实现。REF-03 才会改退款单、释放额度和写 outbox。
var _ RefundResultApplier = (*Payments)(nil)

// UseRefund 装上冻结、渠道退款和结果推进。
// applier 为 nil 时装上 p 自己的 ApplyRefundResult。
// notifyURL 非空时覆盖 UsePay 已经写入的平台回调地址。
// freezer 为 nil 时，CreateRefund 仍只返回刚插入的 INIT。
func (p *Payments) UseRefund(freezer RefundFreezer, refunder ChannelRefunder, applier RefundResultApplier, notifyURL string) {
	if p == nil {
		return
	}
	if applier == nil {
		applier = p
	}
	p.refundFreezer = freezer
	p.refundChannel = refunder
	p.refundApplier = applier
	if notifyURL != "" {
		p.channelNotifyURL = notifyURL
	}
}

// ApplyRefundResult 在 REF-03 之前不写库。
// 调用方据此知道失败分支还没有把退款改成 FAILED，也没有减少 refunded_amount。
func (p *Payments) ApplyRefundResult(ctx context.Context, _ RefundResult) (RefundOrder, error) {
	if p == nil || ctx == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return RefundOrder{}, err
	}
	return RefundOrder{}, errno.ErrInternal
}

// submitRefund 在额度事务提交之后冻结并提交渠道。
// 未装上冻结客户端时原样返回刚插入的 INIT。
func (p *Payments) submitRefund(ctx context.Context, row RefundOrder) (RefundOrder, error) {
	if p.refundFreezer == nil {
		return row, nil
	}
	fr, err := p.refundFreezer.FreezeRefund(ctx, row.MerchantID, row.RefundNo, row.Amount)
	if err != nil {
		// 冻结超时或不可用时结果未知。即使调用方 ctx 已经到期，也返回刚插入的 INIT，不调用渠道。
		if errors.Is(err, errno.ErrUnavailable) {
			return row, nil
		}
		if ctx.Err() != nil {
			return RefundOrder{}, ctx.Err()
		}
		switch {
		case errors.Is(err, errno.ErrInsufficientBalance):
			return p.finishRefund(ctx, row, failCodeInsufficientBalance, "", errno.ErrInsufficientBalance)
		case errors.Is(err, errno.ErrFreezeStatusConflict):
			return p.finishRefund(ctx, row, failCodeFreezeConflict, "", nil)
		case errors.Is(err, errno.ErrAccountNotFound), errors.Is(err, errno.ErrAccountInactive):
			slog.ErrorContext(ctx, "refund freeze account",
				"refund_no", row.RefundNo,
				"merchant_id", row.MerchantID,
				"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
			)
			return RefundOrder{}, errno.ErrInternal
		default:
			return RefundOrder{}, callErr(err)
		}
	}
	if fr.Status != freezeStatusFrozen {
		return RefundOrder{}, errno.ErrInternal
	}
	// 冻结已经成功。故障点放在 PROCESSING 事务之前，进程被杀掉时退款单仍是 INIT。
	fault.Point(ctx, faultRefundAfterFreeze)
	now := p.clk.Now().Truncate(time.Millisecond)
	won, err := p.orders.casRefundProcessing(ctx, row, now)
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	if !won {
		return p.refundAfterMiss(ctx, row.RefundNo)
	}
	row.Status = refundStatusProcessing
	row.Version++
	row.UpdatedAt = now
	return p.callRefundChannel(ctx, row)
}

func (p *Payments) finishRefund(ctx context.Context, row RefundOrder, failCode, failMsg string, bizErr error) (RefundOrder, error) {
	if p.refundApplier == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	got, err := p.refundApplier.ApplyRefundResult(ctx, RefundResult{
		RefundNo: row.RefundNo,
		Result:   payResultFailed,
		FailCode: failCode,
		FailMsg:  failMsg,
		Source:   paySourceSync,
	})
	if err != nil {
		return got, err
	}
	if bizErr != nil && got.Status == refundStatusFailed {
		return got, bizErr
	}
	return got, nil
}

func (p *Payments) refundAfterMiss(ctx context.Context, refundNo string) (RefundOrder, error) {
	row, ok, err := p.orders.refundByNo(ctx, refundNo)
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	if !ok || row.RefundNo != refundNo {
		return RefundOrder{}, errno.ErrInternal
	}
	return row, nil
}

func (p *Payments) callRefundChannel(ctx context.Context, row RefundOrder) (RefundOrder, error) {
	if p.refundChannel == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	callCtx, cancel := context.WithTimeout(ctx, p.payChannelTimeout())
	defer cancel()
	res, err := p.refundChannel.Refund(callCtx, ChannelRefundRequest{
		ReqNo:     row.RefundNo,
		OrigReqNo: row.PayNo,
		Amount:    row.Amount,
		NotifyURL: p.channelNotifyURL,
	})
	if err != nil {
		if isLocalChannelErr(err) {
			slog.ErrorContext(ctx, "channel refund config",
				"refund_no", row.RefundNo,
				"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
			)
			return RefundOrder{}, errno.ErrInternal
		}
		return row, nil
	}
	switch res.Outcome {
	case acceptOK:
		if res.ChannelTxnNo == "" {
			return row, nil
		}
		n, fillErr := p.orders.fillChannelTxnNo(ctx, row.RefundNo, res.ChannelTxnNo)
		if fillErr != nil {
			return RefundOrder{}, callErr(fillErr)
		}
		if n == 0 {
			if _, _, readErr := p.orders.channelTxnByBizNo(ctx, row.RefundNo); readErr != nil {
				return RefundOrder{}, callErr(readErr)
			}
		}
		return row, nil
	case acceptRejected:
		return p.finishRefund(ctx, row, failCodeChannelRejected, res.Reason, errno.ErrChannelRejected)
	default:
		return row, nil
	}
}
