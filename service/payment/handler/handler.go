// Package handler 是 payment 的 Kitex 适配。
// 只做协议转换和错误码转换，不改 biz 的幂等与 CAS。
// 已实现 GetMerchantCredential（PAY-01）、CreatePayment（PAY-02）、Pay（PAY-03）、ClosePayment（PAY-06）、QueryPayment（PAY-08）和 CreateRefund（REF-01）。
// 其余方法属于各自的功能点，实现前调用一律返回 10003，不进 biz。
package handler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/payment/biz"
)

// Handler 实现 kitex 的 PaymentService。
type Handler struct {
	svc Service
}

var _ payment.PaymentService = (*Handler)(nil)

// New 用进程内服务构造 handler。svc 为 nil 时已实现的方法返回 10003。
func New(svc Service) *Handler {
	return &Handler{svc: svc}
}

func recoverBiz(ctx context.Context, errp *error) {
	if rec := recover(); rec != nil {
		// panic 值可能带上 SQL、地址或密钥。只记固定句子，不把值写进日志或返回给调用方。
		_ = rec
		slog.ErrorContext(ctx, "handler panicked")
		*errp = errno.ToBizStatus(errno.ErrInternal)
	}
}

func (h *Handler) ready() error {
	if h == nil || h.svc == nil {
		return errno.ToBizStatus(errno.ErrInternal)
	}
	return nil
}

// notImplemented 是尚未实现方法的统一出口。不是这些方法将来的错误码语义。
func notImplemented() error {
	return errno.ToBizStatus(errno.ErrInternal)
}

// GetMerchantCredential 返回商户凭证，含明文密钥。响应对象的 String() 会打印全部字段，任何地方都不能记录它。
func (h *Handler) GetMerchantCredential(ctx context.Context, req *payment.GetMerchantCredentialRequest) (r *payment.GetMerchantCredentialResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.GetMerchantCredential(ctx, req.AppId)
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return toCredential(got), nil
}

// CreatePayment 创建支付单。请求为 nil 返回 10001。可选字段把指针原样交给 biz，不用 Get 的零值。
func (h *Handler) CreatePayment(ctx context.Context, req *payment.CreatePaymentRequest) (r *payment.PaymentOrder, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.CreatePayment(ctx, &biz.CreatePaymentInput{
		MerchantID:    req.MerchantId,
		OutTradeNo:    req.OutTradeNo,
		Amount:        money.Amount(req.Amount),
		Currency:      req.Currency,
		Subject:       req.Subject,
		NotifyURL:     req.NotifyUrl,
		ExpireMinutes: req.ExpireMinutes,
	})
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return toPaymentOrder(got), nil
}

// Pay 发起支付。请求为 nil 返回 10001。
// 30006 放在 PayResponse.biz_code，error 为 nil，并携带订单。其他业务错误走 BizStatusError，不带订单。
func (h *Handler) Pay(ctx context.Context, req *payment.PayRequest) (r *payment.PayResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.Pay(ctx, &biz.PayInput{
		MerchantID: req.MerchantId,
		PayNo:      req.PayNo,
		Channel:    req.Channel,
	})
	if errors.Is(callErr, errno.ErrChannelRejected) {
		code, rpcErr := errno.RPCExit(callErr, true)
		if rpcErr != nil {
			return nil, rpcErr
		}
		return &payment.PayResponse{BizCode: code, PaymentOrder: toPaymentOrder(got)}, nil
	}
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &payment.PayResponse{BizCode: 0, PaymentOrder: toPaymentOrder(got)}, nil
}

// ClosePayment 关闭支付单。请求为 nil 或 pay_no 为空返回 10001。
// 30003 放在 ClosePaymentResponse.biz_code，error 为 nil，并携带订单。
// 30001、10004、50002 和其他业务错误走 BizStatusError，不带订单。
func (h *Handler) ClosePayment(ctx context.Context, req *payment.ClosePaymentRequest) (r *payment.ClosePaymentResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil || req.PayNo == "" {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.ClosePayment(ctx, &biz.CloseInput{
		MerchantID: req.MerchantId,
		PayNo:      req.PayNo,
	})
	if errors.Is(callErr, errno.ErrOrderStatusConflict) {
		code, rpcErr := errno.RPCExit(callErr, true)
		if rpcErr != nil {
			return nil, rpcErr
		}
		return &payment.ClosePaymentResponse{BizCode: code, PaymentOrder: toPaymentOrder(got)}, nil
	}
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &payment.ClosePaymentResponse{BizCode: 0, PaymentOrder: toPaymentOrder(got)}, nil
}

// QueryPayment 读取当前商户的支付单。响应是 PaymentOrder，不带 biz_code。
// 请求为 nil，或 pay_no 与 out_trade_no 都空、都非空，返回 10001，不进 biz。空串算没提供，不 trim。
// 30001 走 BizStatusError，不带订单。成功时 error 为 nil 并返回订单。
func (h *Handler) QueryPayment(ctx context.Context, req *payment.QueryPaymentRequest) (r *payment.PaymentOrder, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil || !queryKeysOK(req.GetPayNo(), req.GetOutTradeNo()) {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.QueryPayment(ctx, &biz.QueryInput{
		MerchantID: req.MerchantId,
		PayNo:      req.GetPayNo(),
		OutTradeNo: req.GetOutTradeNo(),
	})
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return toPaymentOrder(got), nil
}

func queryKeysOK(payNo, outTradeNo string) bool {
	return (payNo != "") != (outTradeNo != "")
}

// CreateRefund 创建退款（REF-01）。请求为 nil 返回 10001，不进 biz。
// 成功返回刚插入的 INIT 退款单，biz_code 为 0。
// 30005、30003、30001、10006 走 BizStatusError，不带订单。同一请求内不冻结、不调用渠道。
func (h *Handler) CreateRefund(ctx context.Context, req *payment.CreateRefundRequest) (r *payment.CreateRefundResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	reason := ""
	if req.Reason != nil {
		reason = *req.Reason
	}
	got, callErr := h.svc.CreateRefund(ctx, &biz.CreateRefundInput{
		MerchantID:  req.MerchantId,
		PayNo:       req.GetPayNo(),
		OutTradeNo:  req.GetOutTradeNo(),
		OutRefundNo: req.OutRefundNo,
		Amount:      money.Amount(req.Amount),
		Reason:      reason,
	})
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &payment.CreateRefundResponse{BizCode: 0, RefundOrder: toRefundOrder(got)}, nil
}

// QueryRefund 属于 REF-05，未实现，返回 10003。
func (h *Handler) QueryRefund(_ context.Context, _ *payment.QueryRefundRequest) (*payment.RefundOrder, error) {
	return nil, notImplemented()
}

// CreateTransfer 属于 TRF-01~03，未实现，返回 10003。
func (h *Handler) CreateTransfer(_ context.Context, _ *payment.CreateTransferRequest) (*payment.CreateTransferResponse, error) {
	return nil, notImplemented()
}

// QueryTransfer 属于 TRF-05，未实现，返回 10003。
func (h *Handler) QueryTransfer(_ context.Context, _ *payment.QueryTransferRequest) (*payment.Transfer, error) {
	return nil, notImplemented()
}

// HandleChannelNotify 接收渠道回调。PAY 交给 ApplyChannelNotify。REFUND 仍是 10003。
func (h *Handler) HandleChannelNotify(ctx context.Context, req *payment.HandleChannelNotifyRequest) (r *payment.HandleChannelNotifyResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil || req.TxnType != "PAY" {
		if req == nil {
			return nil, errno.ToBizStatus(errno.ErrInvalidParam)
		}
		return nil, errno.ToBizStatus(errno.ErrInternal)
	}
	finished, perr := time.Parse("2006-01-02T15:04:05.000Z07:00", req.FinishedAt)
	if perr != nil || req.ReqNo == "" || (req.Status != "SUCCESS" && req.Status != "FAILED") {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	amt := money.Amount(req.Amount)
	kind, callErr := h.svc.ApplyChannelNotify(ctx, biz.PayResult{
		BizNo:        req.ReqNo,
		Result:       req.Status,
		Source:       "NOTIFY",
		ChannelTxnNo: req.ChannelTxnNo,
		Amount:       &amt,
		FinishedAt:   finished,
	})
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &payment.HandleChannelNotifyResponse{Result_: kind}, nil
}

// SyncChannelTxn 属于 PAY-09，未实现，返回 10003。
func (h *Handler) SyncChannelTxn(_ context.Context, _ *payment.SyncChannelTxnRequest) (*payment.SyncChannelTxnResponse, error) {
	return nil, notImplemented()
}

// ReplayEvent 属于 MQ-05，未实现，返回 10003。
func (h *Handler) ReplayEvent(_ context.Context, _ *payment.ReplayEventRequest) (*payment.ReplayEventResponse, error) {
	return nil, notImplemented()
}
