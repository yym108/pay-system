package biz

import (
	"context"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/pkg/accountclient"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// accountLocalTransfer 通过已有的 account 客户端做同分片转账。不新增错误码。
type accountLocalTransfer struct {
	cli *accountclient.Client
}

// NewAccountLocalTransfer 用 payment 进程里的 account 客户端执行 LOCAL 转账。
func NewAccountLocalTransfer(cli *accountclient.Client) LocalTransferrer {
	return accountLocalTransfer{cli: cli}
}

func (a accountLocalTransfer) TransferLocal(ctx context.Context, transferNo, payerMerchantID, payeeMerchantID string, amount money.Amount) (LocalTransferResult, error) {
	if a.cli == nil {
		return LocalTransferResult{}, errno.ErrInternal
	}
	if ctx == nil {
		return LocalTransferResult{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return LocalTransferResult{}, err
	}
	resp, err := a.cli.TransferLocal(ctx, &account.TransferLocalRequest{
		TransferNo:      transferNo,
		PayerMerchantId: payerMerchantID,
		PayeeMerchantId: payeeMerchantID,
		Amount:          int64(amount),
	})
	return localResultFromRPC(ctx, resp, err)
}

// localResultFromRPC 把 RPC 结果收成业务结果。超时先于父 ctx，与冻结同一口径。
func localResultFromRPC(ctx context.Context, resp *account.TransferLocalResponse, err error) (LocalTransferResult, error) {
	if err != nil {
		if freezeTimedOut(err) {
			return LocalTransferResult{}, errno.ErrUnavailable
		}
		if ctx != nil && ctx.Err() != nil {
			return LocalTransferResult{}, ctx.Err()
		}
		mapped := errno.FromBizStatus(err)
		if mapped == nil || mapped.Code == errno.OK.Code {
			return LocalTransferResult{}, errno.ErrInternal
		}
		return LocalTransferResult{}, mapped
	}
	if resp == nil || (resp.Status != transferStatusSuccess && resp.Status != transferStatusFailed) {
		return LocalTransferResult{}, errno.ErrInternal
	}
	return LocalTransferResult{Status: resp.Status, TxnNo: resp.GetTxnNo(), FailCode: resp.GetFailCode()}, nil
}
