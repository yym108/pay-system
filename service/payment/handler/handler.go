// Package handler 是 payment 的 Kitex 适配。
// 只做协议转换和错误码转换，不改 biz 的幂等与 CAS。
// 目前只实现 GetMerchantCredential（PAY-01）；其余方法属于各自的功能点，实现前调用一律返回 10003，不进 biz。
package handler

import (
	"context"
	"log/slog"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
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

// CreatePayment 属于 PAY-02，未实现，返回 10003。
func (h *Handler) CreatePayment(_ context.Context, _ *payment.CreatePaymentRequest) (*payment.PaymentOrder, error) {
	return nil, notImplemented()
}

// Pay 属于 PAY-03，未实现，返回 10003。
func (h *Handler) Pay(_ context.Context, _ *payment.PayRequest) (*payment.PaymentOrder, error) {
	return nil, notImplemented()
}

// ClosePayment 属于 PAY-06，未实现，返回 10003。
func (h *Handler) ClosePayment(_ context.Context, _ *payment.ClosePaymentRequest) (*payment.PaymentOrder, error) {
	return nil, notImplemented()
}

// QueryPayment 属于 PAY-08，未实现，返回 10003。
func (h *Handler) QueryPayment(_ context.Context, _ *payment.QueryPaymentRequest) (*payment.PaymentOrder, error) {
	return nil, notImplemented()
}

// CreateRefund 属于 REF-01、REF-02，未实现，返回 10003。
func (h *Handler) CreateRefund(_ context.Context, _ *payment.CreateRefundRequest) (*payment.CreateRefundResponse, error) {
	return nil, notImplemented()
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

// HandleChannelNotify 属于 GW-06、PAY-04、REF-03，未实现，返回 10003。
func (h *Handler) HandleChannelNotify(_ context.Context, _ *payment.HandleChannelNotifyRequest) (*payment.HandleChannelNotifyResponse, error) {
	return nil, notImplemented()
}

// SyncChannelTxn 属于 PAY-09，未实现，返回 10003。
func (h *Handler) SyncChannelTxn(_ context.Context, _ *payment.SyncChannelTxnRequest) (*payment.SyncChannelTxnResponse, error) {
	return nil, notImplemented()
}

// ReplayEvent 属于 MQ-05，未实现，返回 10003。
func (h *Handler) ReplayEvent(_ context.Context, _ *payment.ReplayEventRequest) (*payment.ReplayEventResponse, error) {
	return nil, notImplemented()
}
