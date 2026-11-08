package biz

import (
	"context"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/pkg/accountclient"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// accountTCCTransfer 通过已有的 account 客户端做跨分片分支。不新增错误码。
type accountTCCTransfer struct {
	cli *accountclient.Client
}

// NewAccountTCCTransfer 用 payment 进程里的 account 客户端执行 TCC 分支。
func NewAccountTCCTransfer(cli *accountclient.Client) TCCBrancher {
	return accountTCCTransfer{cli: cli}
}

func (a accountTCCTransfer) Freeze(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	if err := a.ready(ctx); err != nil {
		return err
	}
	_, err := a.cli.Freeze(ctx, &account.FreezeRequest{
		BizType: transferBizType, BizNo: transferNo, MerchantId: payer, Amount: int64(amount),
	})
	return tccErrFromRPC(ctx, err)
}

func (a accountTCCTransfer) TryPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	if err := a.ready(ctx); err != nil {
		return err
	}
	_, err := a.cli.TransferInTry(ctx, &account.TransferInTryRequest{
		TransferNo: transferNo, PayeeMerchantId: payee, Amount: int64(amount),
	})
	return tccErrFromRPC(ctx, err)
}

func (a accountTCCTransfer) ConfirmPayer(ctx context.Context, transferNo, payer string) error {
	if err := a.ready(ctx); err != nil {
		return err
	}
	// TRANSFER 的 channel 必须是空串，贷方是在途而不是渠道应收。
	_, err := a.cli.ConfirmFreeze(ctx, &account.ConfirmFreezeRequest{
		BizType: transferBizType, BizNo: transferNo, MerchantId: payer, Channel: "",
	})
	return tccErrFromRPC(ctx, err)
}

func (a accountTCCTransfer) ConfirmPayee(ctx context.Context, transferNo, payee string) error {
	if err := a.ready(ctx); err != nil {
		return err
	}
	_, err := a.cli.TransferInConfirm(ctx, &account.TransferInConfirmRequest{
		TransferNo: transferNo, PayeeMerchantId: payee,
	})
	return tccErrFromRPC(ctx, err)
}

func (a accountTCCTransfer) CancelPayer(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	if err := a.ready(ctx); err != nil {
		return err
	}
	_, err := a.cli.ReleaseFreeze(ctx, &account.ReleaseFreezeRequest{
		BizType: transferBizType, BizNo: transferNo, MerchantId: payer, Amount: int64(amount),
	})
	return tccErrFromRPC(ctx, err)
}

func (a accountTCCTransfer) CancelPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	if err := a.ready(ctx); err != nil {
		return err
	}
	_, err := a.cli.TransferInCancel(ctx, &account.TransferInCancelRequest{
		TransferNo: transferNo, PayeeMerchantId: payee, Amount: int64(amount),
	})
	return tccErrFromRPC(ctx, err)
}

func (a accountTCCTransfer) ready(ctx context.Context) error {
	if a.cli == nil || ctx == nil {
		return errno.ErrInternal
	}
	return ctx.Err()
}

// tccErrFromRPC 把 RPC 错误收成业务错误。超时先于父 ctx，与冻结、同分片转账同一口径。
func tccErrFromRPC(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if freezeTimedOut(err) {
		return errno.ErrUnavailable
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	mapped := errno.FromBizStatus(err)
	if mapped == nil || mapped.Code == errno.OK.Code {
		return errno.ErrInternal
	}
	return mapped
}
