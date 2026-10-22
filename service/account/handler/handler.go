// Package handler 是 account 的 Kitex 适配。
// 只做协议转换和错误码转换，不改 biz 的幂等与 CAS，也不在这里记账。
// Reverse 的冲正规则属于 ACC-08。本包只暴露方法，调用一律返回 10003，不写行。
package handler

import (
	"context"
	"log/slog"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// Handler 实现 kitex 的 AccountService。
type Handler struct {
	svc Service
}

var _ account.AccountService = (*Handler)(nil)

// New 用进程内服务构造 handler。svc 为 nil 时每个方法返回 10003。
func New(svc Service) *Handler {
	return &Handler{svc: svc}
}

func recoverBiz(ctx context.Context, errp *error) {
	if rec := recover(); rec != nil {
		// panic 值可能带上 SQL 或地址。只记固定句子，不把值写进日志或返回给调用方。
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

// OpenAccount 转发开户。不在客户端重试清单里，即使 biz 幂等也不由本包重试。
func (h *Handler) OpenAccount(ctx context.Context, req *account.OpenAccountRequest) (r *account.Account, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.OpenAccount(ctx, req.OwnerType, req.OwnerId, req.AccountType, int(req.Shard), int(req.SubNo))
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return toAccount(got), nil
}

// GetAccount 转发查询。
func (h *Handler) GetAccount(ctx context.Context, req *account.GetAccountRequest) (r *account.Account, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.GetAccount(ctx, req.OwnerType, req.OwnerId, req.AccountType)
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return toAccount(got), nil
}

// Freeze 转发冻结。duplicated 不进 RPC。
func (h *Handler) Freeze(ctx context.Context, req *account.FreezeRequest) (r *account.FreezeResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.Freeze(ctx, req.BizType, req.BizNo, req.MerchantId, money.Amount(req.Amount))
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &account.FreezeResponse{FreezeNo: got.FreezeNo, Status: got.Status}, nil
}

// ReleaseFreeze 转发解冻。
func (h *Handler) ReleaseFreeze(ctx context.Context, req *account.ReleaseFreezeRequest) (r *account.ReleaseFreezeResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.ReleaseFreeze(ctx, req.BizType, req.BizNo, req.MerchantId, money.Amount(req.Amount))
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &account.ReleaseFreezeResponse{FreezeNo: got.FreezeNo, Status: got.Status}, nil
}

// ConfirmFreeze 转发确认。请求没有金额，金额仍由 biz 读冻结单。
func (h *Handler) ConfirmFreeze(ctx context.Context, req *account.ConfirmFreezeRequest) (r *account.ConfirmFreezeResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.ConfirmFreeze(ctx, req.BizType, req.BizNo, req.MerchantId, req.Channel)
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &account.ConfirmFreezeResponse{FreezeNo: got.FreezeNo, TxnNo: got.TxnNo}, nil
}

// TransferLocal 转发同分片转账。
// 余额不足时 biz 的 error 为 nil，fail_code 在响应里，不能改成 BizStatusError 40002。
func (h *Handler) TransferLocal(ctx context.Context, req *account.TransferLocalRequest) (r *account.TransferLocalResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.TransferLocal(ctx, req.TransferNo, req.PayerMerchantId, req.PayeeMerchantId, money.Amount(req.Amount))
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &account.TransferLocalResponse{
		Status:   got.Status,
		TxnNo:    optionalString(got.TxnNo),
		FailCode: optionalString(got.FailCode),
	}, nil
}

// TransferInTry 转发收款方 Try。
func (h *Handler) TransferInTry(ctx context.Context, req *account.TransferInTryRequest) (r *account.TransferInTryResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.TransferInTry(ctx, req.TransferNo, req.PayeeMerchantId, money.Amount(req.Amount))
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &account.TransferInTryResponse{Status: got.Status}, nil
}

// TransferInConfirm 转发收款方 Confirm。
func (h *Handler) TransferInConfirm(ctx context.Context, req *account.TransferInConfirmRequest) (r *account.TransferInConfirmResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.TransferInConfirm(ctx, req.TransferNo, req.PayeeMerchantId)
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &account.TransferInConfirmResponse{Status: got.Status, TxnNo: got.TxnNo}, nil
}

// TransferInCancel 转发收款方 Cancel。
func (h *Handler) TransferInCancel(ctx context.Context, req *account.TransferInCancelRequest) (r *account.TransferInCancelResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, callErr := h.svc.TransferInCancel(ctx, req.TransferNo, req.PayeeMerchantId, money.Amount(req.Amount))
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &account.TransferInCancelResponse{Status: got.Status}, nil
}

// CheckBalance 转发核对。passed 为 false 不是错误码。
func (h *Handler) CheckBalance(ctx context.Context, req *account.CheckBalanceRequest) (r *account.CheckBalanceResponse, err error) {
	defer recoverBiz(ctx, &err)
	if err = h.ready(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	merchantID := ""
	if req.IsSetMerchantId() {
		merchantID = req.GetMerchantId()
	}
	quiet := false
	if req.IsSetExpectQuiescent() {
		quiet = req.GetExpectQuiescent()
	}
	got, callErr := h.svc.CheckBalance(ctx, merchantID, quiet)
	if callErr != nil {
		return nil, statusErr(ctx, callErr)
	}
	return &account.CheckBalanceResponse{Passed: got.Passed, Failures: toFailures(got.Failures)}, nil
}

// Reverse 在 ACC-08 完成前不调用 biz，不写行。任何调用返回 10003。
// 这不是 ACC-08 将来的 10001 / 40001 / 40002 语义。
func (h *Handler) Reverse(ctx context.Context, _ *account.ReverseRequest) (r *account.ReverseResponse, err error) {
	defer recoverBiz(ctx, &err)
	return nil, errno.ToBizStatus(errno.ErrInternal)
}
