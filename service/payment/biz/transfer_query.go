package biz

import (
	"context"

	"github.com/yym108/pay-system/pkg/errno"
)

// TransferQueryInput 是转账查询的入参。MerchantID 只取自验签结果。
// TransferNo 与 OutTransferNo 二选一。空串算没提供，不 trim。
type TransferQueryInput struct {
	MerchantID    string
	TransferNo    string
	OutTransferNo string
}

// QueryTransfer 转给转账查询。payments 为 nil 时返回 10003。
func (a *App) QueryTransfer(ctx context.Context, in *TransferQueryInput) (TransferOrder, error) {
	if a == nil || a.payments == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	return a.payments.QueryTransfer(ctx, in)
}

// QueryTransfer 直接读取 transfer_order（TRF-05），规则同 QueryRefund。不缓存，不写行，不推进状态，不改 version。
// 只有付款商户（transfer_order.merchant_id）可以查询。
// 请求为 nil，或两个单号都空、都非空，返回 10001，不读库。
// 没有行，或 transfer_no 路径上商户号与库中逐字节不同（收款商户、其他商户、空商户号），返回 30001。
// 只提供 out_transfer_no 时按 (merchant_id, out_transfer_no) 读，没有行也是 30001。几种情况是同一个错误。
func (p *Payments) QueryTransfer(ctx context.Context, in *TransferQueryInput) (TransferOrder, error) {
	if !exactlyOneTransferKey(in) {
		return TransferOrder{}, errno.ErrInvalidParam
	}
	if p == nil || p.transfers == nil || ctx == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return TransferOrder{}, err
	}
	var (
		row TransferOrder
		ok  bool
		err error
	)
	if in.TransferNo != "" {
		row, ok, err = p.transfers.transferByNo(ctx, in.TransferNo)
	} else {
		row, ok, err = p.transfers.transferByMerchantOut(ctx, in.MerchantID, in.OutTransferNo)
	}
	if err != nil {
		return TransferOrder{}, callErr(err)
	}
	// 库列排序规则可能放宽匹配，读回后逐字节比较。收款商户与付款商户不同，同样落到这里返回 30001，
	// 调用方看不出转账单在不在。
	if !ok || row.MerchantID != in.MerchantID ||
		(in.TransferNo != "" && row.TransferNo != in.TransferNo) ||
		(in.OutTransferNo != "" && row.OutTransferNo != in.OutTransferNo) {
		return TransferOrder{}, errno.ErrOrderNotFound
	}
	return row, nil
}

func exactlyOneTransferKey(in *TransferQueryInput) bool {
	if in == nil {
		return false
	}
	return (in.TransferNo != "") != (in.OutTransferNo != "")
}
