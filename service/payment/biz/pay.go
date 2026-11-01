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
	"github.com/yym108/pay-system/pkg/sign"
)

const (
	channelMockpay          = "mockpay"
	payStatusPaying         = "PAYING"
	payStatusSuccess        = "SUCCESS"
	payStatusFailed         = "FAILED"
	payStatusClosed         = "CLOSED"
	txnTypePay              = "PAY"
	txnStatusProcessing     = "PROCESSING"
	failCodeChannelRejected = "CHANNEL_REJECTED"
	payResultFailed         = "FAILED"
	paySourceSync           = "SYNC"
	faultPayAfterTx1        = "pay.after_tx1"
	channelPayPath          = "/mockpay/v1/pay"
	payExpireMargin         = 30 * time.Second
	payNextQueryDelay       = 10 * time.Second
	maxChannelBody          = 64 << 10
)

const (
	// PayChannelTimeout 是渠道受理的总超时。父 ctx 更短时，先到期的生效。
	PayChannelTimeout = 3 * time.Second
	// PayChannelDialTimeout 是渠道受理的 TCP 连接超时。
	PayChannelDialTimeout = 500 * time.Millisecond
)

// PayInput 是发起支付的入参。Channel 必须恰好是 mockpay。
type PayInput struct {
	MerchantID string
	PayNo      string
	Channel    string
}

// ChannelAcceptRequest 是发往渠道的受理参数。NotifyURL 是平台回调地址，不是商户通知地址。
type ChannelAcceptRequest struct {
	ReqNo     string
	Amount    money.Amount
	Subject   string
	NotifyURL string
}

type acceptOutcome int

const (
	acceptUnknown acceptOutcome = iota
	acceptOK
	acceptRejected
)

// ChannelAcceptResult 是渠道受理的分类结果。零值表示结果未知。
type ChannelAcceptResult struct {
	Outcome      acceptOutcome
	ChannelTxnNo string
	Reason       string
}

// ChannelAcceptor 受理一笔渠道支付。实现不得把密钥写入日志或错误。
type ChannelAcceptor interface {
	Accept(ctx context.Context, req ChannelAcceptRequest) (ChannelAcceptResult, error)
}

// PayResultApplier 把渠道终态推进到订单。生产实现是 *Payments。
type PayResultApplier interface {
	ApplyPayResult(ctx context.Context, in PayResult) (PaymentOrder, error)
}

// *Payments.ApplyPayResult 是生产实现。空实现只留在测试文件里。
var _ PayResultApplier = (*Payments)(nil)

// channelTxn 是 channel_txn 中 Pay 会读到的列。
type channelTxn struct {
	Channel      string
	TxnType      string
	BizNo        string
	ChannelTxnNo string
	Amount       money.Amount
	Status       string
	NotifyCount  int
	QueryCount   int
	NextQueryAt  time.Time
	Version      uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// UsePay 装上渠道受理、结果推进和平台回调地址。
// applier 为 nil 时装上 p 自己的 ApplyPayResult。不改 NewPayments 的参数。
func (p *Payments) UsePay(channel ChannelAcceptor, applier PayResultApplier, notifyURL string) {
	if p == nil {
		return
	}
	if applier == nil {
		applier = p
	}
	p.channel = channel
	p.applier = applier
	p.channelNotifyURL = notifyURL
}

func (p *Payments) payChannelTimeout() time.Duration {
	if p != nil && p.channelTimeout > 0 {
		return p.channelTimeout
	}
	return PayChannelTimeout
}

// Pay 把 INIT 支付单推进到 PAYING 并请求渠道受理。
// 请求为 nil 返回 10001。channel 不是 mockpay 返回 30007，不读库。
// 渠道同步拒绝在 ApplyPayResult 写成 FAILED 后，由本方法返回 30006 和该订单。
// 适配器返回的错误原样带出。其余业务错误不带订单。
func (p *Payments) Pay(ctx context.Context, in *PayInput) (PaymentOrder, error) {
	if in == nil {
		return PaymentOrder{}, errno.ErrInvalidParam
	}
	if in.Channel != channelMockpay {
		return PaymentOrder{}, errno.ErrChannelUnsupported
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
	switch order.Status {
	case payStatusPaying, payStatusSuccess:
		return order, nil
	case payStatusFailed, payStatusClosed:
		return PaymentOrder{}, errno.ErrOrderStatusConflict
	case payStatusInit:
	default:
		return PaymentOrder{}, errno.ErrInternal
	}
	now := p.clk.Now().Truncate(time.Millisecond)
	if !order.ExpireAt.After(now.Add(payExpireMargin)) {
		return PaymentOrder{}, errno.ErrOrderExpired
	}
	// 未装配渠道时不把订单改成 PAYING，避免一次配置错误留下无法在本次调用里完成的受理。
	if p.channel == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	won, err := p.orders.casInitToPaying(ctx, order, now)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !won {
		return p.afterCASMiss(ctx, in)
	}
	order.Status = payStatusPaying
	order.Channel = channelMockpay
	order.Version++
	order.UpdatedAt = now
	// 事务已经提交。故障点放在这里，进程被杀掉时订单停在 PAYING。
	fault.Point(ctx, faultPayAfterTx1)
	return p.callChannel(ctx, order)
}

func (p *Payments) afterCASMiss(ctx context.Context, in *PayInput) (PaymentOrder, error) {
	order, ok, err := p.orders.paymentByPayNo(ctx, in.PayNo)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !ok || order.PayNo != in.PayNo || order.MerchantID != in.MerchantID {
		return PaymentOrder{}, errno.ErrOrderNotFound
	}
	switch order.Status {
	case payStatusPaying, payStatusSuccess, payStatusInit:
		return order, nil
	case payStatusFailed, payStatusClosed:
		return PaymentOrder{}, errno.ErrOrderStatusConflict
	default:
		return PaymentOrder{}, errno.ErrInternal
	}
}

func (p *Payments) callChannel(ctx context.Context, order PaymentOrder) (PaymentOrder, error) {
	callCtx, cancel := context.WithTimeout(ctx, p.payChannelTimeout())
	defer cancel()
	res, err := p.channel.Accept(callCtx, ChannelAcceptRequest{
		ReqNo:     order.PayNo,
		Amount:    order.Amount,
		Subject:   order.Subject,
		NotifyURL: p.channelNotifyURL,
	})
	if err != nil {
		if isLocalChannelErr(err) {
			slog.ErrorContext(ctx, "channel accept config",
				"pay_no", order.PayNo,
				"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
			)
			return PaymentOrder{}, errno.ErrInternal
		}
		return order, nil
	}
	switch res.Outcome {
	case acceptOK:
		if res.ChannelTxnNo == "" {
			return order, nil
		}
		n, fillErr := p.orders.fillChannelTxnNo(ctx, order.PayNo, res.ChannelTxnNo)
		if fillErr != nil {
			return PaymentOrder{}, callErr(fillErr)
		}
		if n == 0 {
			if _, _, readErr := p.orders.channelTxnByBizNo(ctx, order.PayNo); readErr != nil {
				return PaymentOrder{}, callErr(readErr)
			}
		}
		return order, nil
	case acceptRejected:
		if p.applier == nil {
			return PaymentOrder{}, errno.ErrInternal
		}
		got, err := p.applier.ApplyPayResult(ctx, PayResult{
			BizNo:    order.PayNo,
			Result:   payResultFailed,
			FailCode: failCodeChannelRejected,
			FailMsg:  res.Reason,
			Source:   paySourceSync,
		})
		if err != nil {
			return got, err
		}
		if got.Status == payStatusFailed {
			return got, errno.ErrChannelRejected
		}
		return got, nil
	default:
		return order, nil
	}
}

func isLocalChannelErr(err error) bool {
	return errors.Is(err, sign.ErrEmptyKey) || errors.Is(err, sign.ErrInvalidField) ||
		errors.Is(err, errChannelConfig) || errors.Is(err, errChannelMarshal)
}
