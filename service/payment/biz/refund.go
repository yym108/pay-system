package biz

import (
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

const (
	refundNoPrefix   = "RF"
	refundStatusInit = "INIT"
	maxReasonRunes   = 255
)

// CreateRefundInput 是创建退款的入参。PayNo 与 OutTradeNo 二选一，空串算没提供，不 trim。
// Reason 未传时为空串。
type CreateRefundInput struct {
	MerchantID  string
	PayNo       string
	OutTradeNo  string
	OutRefundNo string
	Amount      money.Amount
	Reason      string
}

// RefundOrder 是 refund_order 的一行。金额单位为分。时间已截断到毫秒。
type RefundOrder struct {
	RefundNo    string
	MerchantID  string
	OutRefundNo string
	PayNo       string
	Amount      money.Amount
	Currency    string
	Reason      string
	Status      string
	Channel     string
	FailCode    string
	FailMsg     string
	Version     uint64
	SucceededAt time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CreateRefund 转给退款创建。payments 为 nil 时返回 10003。
func (a *App) CreateRefund(ctx context.Context, in *CreateRefundInput) (RefundOrder, error) {
	if a == nil || a.payments == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	return a.payments.CreateRefund(ctx, in)
}

// CreateRefund 插入一条 INIT 退款单，并占用原支付单的 refunded_amount。
// 新插入成功且已装上冻结客户端时，同一请求内冻结并提交渠道。未装上时返回刚插入的 INIT。
// 幂等键是 (merchant_id, out_refund_no)。命中且 pay_no、amount 一致时只读，不增加 refunded_amount，也不再次冻结。
func (p *Payments) CreateRefund(ctx context.Context, in *CreateRefundInput) (RefundOrder, error) {
	if p == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	if err := validateRefundInput(in); err != nil {
		return RefundOrder{}, err
	}
	if err := p.refundReady(ctx); err != nil {
		return RefundOrder{}, err
	}
	order, err := p.loadRefundPayment(ctx, in)
	if err != nil {
		return RefundOrder{}, err
	}
	existing, ok, err := p.orders.refundByMerchantOut(ctx, in.MerchantID, in.OutRefundNo)
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	if ok {
		return sameRefund(existing, order.PayNo, in.Amount)
	}
	refundNo, err := p.ids.NewNo(ctx, refundNoPrefix)
	if err != nil {
		if errors.Is(err, idgen.ErrClockRollback) {
			return RefundOrder{}, errno.ErrUnavailable
		}
		return RefundOrder{}, callErr(err)
	}
	if order.Channel == "" {
		return RefundOrder{}, errno.ErrInternal
	}
	now := p.clk.Now().Truncate(time.Millisecond)
	row := RefundOrder{
		RefundNo:    refundNo,
		MerchantID:  order.MerchantID,
		OutRefundNo: in.OutRefundNo,
		PayNo:       order.PayNo,
		Amount:      in.Amount,
		Currency:    order.Currency,
		Reason:      in.Reason,
		Status:      refundStatusInit,
		Channel:     order.Channel,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	affected, duplicate, err := p.orders.occupyRefund(ctx, order, row)
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	if affected {
		return p.submitRefund(ctx, row)
	}
	return p.afterRefundMiss(ctx, in, order, duplicate)
}

func validateRefundInput(in *CreateRefundInput) error {
	if in == nil {
		return errno.ErrInvalidParam
	}
	if (in.PayNo != "") == (in.OutTradeNo != "") {
		return errno.ErrInvalidParam
	}
	if in.OutRefundNo == "" || len(in.OutRefundNo) > maxOutTradeNoLen || !outTradeNoPattern.MatchString(in.OutRefundNo) {
		return errno.ErrInvalidParam
	}
	if err := money.ValidatePayAmount(in.Amount); err != nil {
		return errno.ErrInvalidParam
	}
	if !utf8.ValidString(in.Reason) || utf8.RuneCountInString(in.Reason) > maxReasonRunes {
		return errno.ErrInvalidParam
	}
	return nil
}

func (p *Payments) refundReady(ctx context.Context) error {
	if p.ids == nil || p.clk == nil || p.orders == nil {
		return errno.ErrInternal
	}
	if ctx == nil {
		return errno.ErrInternal
	}
	return ctx.Err()
}

func (p *Payments) loadRefundPayment(ctx context.Context, in *CreateRefundInput) (PaymentOrder, error) {
	var (
		order PaymentOrder
		ok    bool
		err   error
	)
	if in.PayNo != "" {
		order, ok, err = p.orders.paymentByPayNo(ctx, in.PayNo)
		if err != nil {
			return PaymentOrder{}, callErr(err)
		}
		if !ok || order.PayNo != in.PayNo || order.MerchantID != in.MerchantID {
			return PaymentOrder{}, errno.ErrOrderNotFound
		}
	} else {
		order, ok, err = p.orders.paymentByOutTradeNo(ctx, in.MerchantID, in.OutTradeNo)
		if err != nil {
			return PaymentOrder{}, callErr(err)
		}
		if !ok || order.MerchantID != in.MerchantID || order.OutTradeNo != in.OutTradeNo {
			return PaymentOrder{}, errno.ErrOrderNotFound
		}
	}
	if order.Status != payStatusSuccess {
		return PaymentOrder{}, errno.ErrOrderStatusConflict
	}
	return order, nil
}

func sameRefund(existing RefundOrder, payNo string, amount money.Amount) (RefundOrder, error) {
	if existing.PayNo == payNo && existing.Amount == amount {
		return existing, nil
	}
	return RefundOrder{}, errno.ErrIdempotentConflict
}

// afterRefundMiss 在额度事务结束后处理 0 行和 1062 两种结局。1062 要先回滚才能撤销额度，
// 两种结局因此共用这条事务外的普通读。READ COMMITTED 下事务内再读也能看到已提交的退款，这里不靠事务外读保证可见性。
func (p *Payments) afterRefundMiss(ctx context.Context, in *CreateRefundInput, order PaymentOrder, _ bool) (RefundOrder, error) {
	existing, ok, err := p.orders.refundByMerchantOut(ctx, in.MerchantID, in.OutRefundNo)
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	if ok {
		return sameRefund(existing, order.PayNo, in.Amount)
	}
	again, err := p.loadRefundPayment(ctx, in)
	if err != nil {
		return RefundOrder{}, err
	}
	if again.Status != payStatusSuccess {
		return RefundOrder{}, errno.ErrOrderStatusConflict
	}
	return RefundOrder{}, errno.ErrRefundExceeded
}
