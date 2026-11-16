package biz

import (
	"context"
	"errors"
	"log/slog"
	"strconv"

	"github.com/yym108/pay-system/pkg/errno"
)

// SyncChannelTxnResult 是立即同步渠道状态的结果（PAY-09）。
type SyncChannelTxnResult struct {
	LocalStatus   string
	ChannelStatus string
	Changed       bool
}

// SyncChannelTxn 立即同步渠道状态（PAY-09，仅对账自动处理 REC-06 使用）：
// 先读本地 channel_txn（不存在返回 30001），再立即查单一次；
// 本地仍为 PROCESSING 时按 PAY-05 的分发规则推进，已是终态时只返回双方状态且不做修改。
func (p *Payments) SyncChannelTxn(ctx context.Context, bizNo string) (SyncChannelTxnResult, error) {
	if p == nil || p.clk == nil || p.querier == nil || p.orders == nil || ctx == nil {
		return SyncChannelTxnResult{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return SyncChannelTxnResult{}, err
	}
	if bizNo == "" {
		return SyncChannelTxnResult{}, errno.ErrInvalidParam
	}
	txn, ok, err := p.orders.channelTxnByBizNo(ctx, bizNo)
	if err != nil {
		return SyncChannelTxnResult{}, callErr(err)
	}
	if !ok || txn.BizNo != bizNo {
		return SyncChannelTxnResult{}, errno.ErrOrderNotFound
	}
	if txn.TxnType != txnTypePay && txn.TxnType != txnTypeRefund {
		return SyncChannelTxnResult{}, errno.ErrInternal
	}

	res, err := p.querier.QueryTxn(ctx, txn.TxnType, txn.BizNo)
	if err != nil {
		if isLocalChannelErr(err) {
			slog.ErrorContext(ctx, "sync channel query config",
				"biz_type", txn.TxnType,
				"biz_no", txn.BizNo,
				"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
			)
			return SyncChannelTxnResult{}, errno.ErrInternal
		}
		return SyncChannelTxnResult{}, errno.ErrUnavailable
	}

	var channelStatus string
	switch res.Kind {
	case queryNotFound:
		channelStatus = queryResultNotFound
	case queryFound:
		channelStatus = res.Status
	default:
		return SyncChannelTxnResult{}, errno.ErrUnavailable
	}

	if txn.Status != txnStatusProcessing {
		return SyncChannelTxnResult{
			LocalStatus:   txn.Status,
			ChannelStatus: channelStatus,
			Changed:       false,
		}, nil
	}

	dispatched, err := p.dispatchSyncQuery(ctx, txn, res)
	if err != nil {
		return SyncChannelTxnResult{}, err
	}
	if !dispatched {
		return SyncChannelTxnResult{
			LocalStatus:   txn.Status,
			ChannelStatus: channelStatus,
			Changed:       false,
		}, nil
	}

	after, ok, err := p.orders.channelTxnByBizNo(ctx, txn.BizNo)
	if err != nil {
		return SyncChannelTxnResult{}, callErr(err)
	}
	if !ok || after.BizNo != txn.BizNo {
		return SyncChannelTxnResult{}, errno.ErrInternal
	}
	return SyncChannelTxnResult{
		LocalStatus:   after.Status,
		ChannelStatus: channelStatus,
		Changed:       after.Status != txn.Status,
	}, nil
}

func (p *Payments) dispatchSyncQuery(ctx context.Context, txn channelTxn, res channelQueryResult) (bool, error) {
	switch res.Kind {
	case queryNotFound:
		if p.clk.Now().Sub(txn.CreatedAt) <= queryNotFoundGrace {
			return false, nil
		}
		if txn.TxnType == txnTypePay {
			_, err := p.closeOnChannel(ctx, txn.BizNo, paySourceQuery)
			if err != nil && !errors.Is(err, errno.ErrOrderStatusConflict) {
				return false, err
			}
			return true, nil
		}
		err := p.resubmitRefundErr(ctx, txn.BizNo)
		if err != nil && !errors.Is(err, errno.ErrChannelRejected) {
			return false, err
		}
		return true, nil
	case queryFound:
		if txn.TxnType == txnTypePay {
			in, ok := buildPayQueryResult(txn.BizNo, res)
			if !ok {
				return false, nil
			}
			if p.applier == nil {
				return false, errno.ErrInternal
			}
			if _, err := p.applier.ApplyPayResult(ctx, in); err != nil && !errors.Is(err, errno.ErrOrderStatusConflict) {
				return false, err
			}
			return true, nil
		}
		in, ok := buildRefundQueryResult(ctx, txn.BizNo, res)
		if !ok {
			return false, nil
		}
		if p.refundApplier == nil {
			return false, errno.ErrInternal
		}
		if _, err := p.refundApplier.ApplyRefundResult(ctx, in); err != nil && !errors.Is(err, errno.ErrOrderStatusConflict) {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}
