package handler

import (
	"context"

	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/biz"
)

// Service 是 handler 调用的进程内账户服务。
// *biz.Service 已实现它。测试用假实现替换，不改 biz 的幂等和 CAS。
type Service interface {
	OpenAccount(ctx context.Context, ownerType, ownerID, accountType string, shardNo, subNo int) (biz.Account, error)
	GetAccount(ctx context.Context, ownerType, ownerID, accountType string) (biz.Account, error)
	Freeze(ctx context.Context, bizType, bizNo, merchantID string, amount money.Amount) (biz.FreezeResult, error)
	ReleaseFreeze(ctx context.Context, bizType, bizNo, merchantID string, amount money.Amount) (biz.FreezeResult, error)
	ConfirmFreeze(ctx context.Context, bizType, bizNo, merchantID, channel string) (biz.ConfirmResult, error)
	TransferLocal(ctx context.Context, transferNo, payerMerchantID, payeeMerchantID string, amount money.Amount) (biz.TransferLocalResult, error)
	TransferInTry(ctx context.Context, transferNo, payeeMerchantID string, amount money.Amount) (biz.TransferInResult, error)
	TransferInConfirm(ctx context.Context, transferNo, payeeMerchantID string) (biz.TransferInResult, error)
	TransferInCancel(ctx context.Context, transferNo, payeeMerchantID string, amount money.Amount) (biz.TransferInResult, error)
	CheckBalance(ctx context.Context, merchantID string, expectQuiescent bool) (biz.CheckBalanceResult, error)
}

var _ Service = (*biz.Service)(nil)
