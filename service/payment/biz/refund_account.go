package biz

import (
	"context"
	"errors"
	"net"
	"os"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/pkg/accountclient"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// accountRefundFreezer 通过已有的 account 客户端冻结。不新增错误码，不查账户号。
type accountRefundFreezer struct {
	cli *accountclient.Client
}

// NewAccountRefundFreezer 用 payment 进程里的 account 客户端冻结退款。
func NewAccountRefundFreezer(cli *accountclient.Client) RefundFreezer {
	return accountRefundFreezer{cli: cli}
}

func (a accountRefundFreezer) FreezeRefund(ctx context.Context, merchantID, refundNo string, amount money.Amount) (RefundFreeze, error) {
	if a.cli == nil {
		return RefundFreeze{}, errno.ErrInternal
	}
	if ctx == nil {
		return RefundFreeze{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return RefundFreeze{}, err
	}
	resp, err := a.cli.Freeze(ctx, &account.FreezeRequest{
		BizType:    txnTypeRefund,
		BizNo:      refundNo,
		MerchantId: merchantID,
		Amount:     int64(amount),
	})
	if err != nil {
		// 先认超时。父 ctx 同时到期时，这次冻结结果仍是未知，交给上层返回 INIT。
		if freezeTimedOut(err) {
			return RefundFreeze{}, errno.ErrUnavailable
		}
		if ctx.Err() != nil {
			return RefundFreeze{}, ctx.Err()
		}
		mapped := errno.FromBizStatus(err)
		if mapped == nil || mapped.Code == errno.OK.Code {
			return RefundFreeze{}, errno.ErrInternal
		}
		return RefundFreeze{}, mapped
	}
	if resp == nil || (resp.Status != freezeStatusFrozen && resp.Status != freezeStatusConfirmed) {
		return RefundFreeze{}, errno.ErrInternal
	}
	return RefundFreeze{Status: resp.Status}, nil
}

// freezeTimedOut 与 accountclient 的超时判断同一口径。
// 服务端把处理超时写成 TransError 时，消息仍是 rpc timeout，不是业务码。
func freezeTimedOut(err error) bool {
	if err == nil {
		return false
	}
	if kerrors.IsTimeoutError(err) || errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var timed interface{ Timeout() bool }
	if errors.As(err, &timed) && timed.Timeout() {
		return true
	}
	var trans *remote.TransError
	return errors.As(err, &trans) && trans != nil && trans.Error() == kerrors.ErrRPCTimeout.Error()
}
