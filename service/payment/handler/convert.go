package handler

import (
	"context"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/base"
	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/service/payment/biz"
)

const timestampLayout = "2006-01-02T15:04:05.000Z07:00"

var shanghai = time.FixedZone("Asia/Shanghai", 8*60*60)

// statusErr 把 biz 错误交给 Kitex。GetMerchantCredential 没有携带对象的错误，carryObject=false。
// 退款和转账要连同对象返回的 40002、30006 由各自功能点用 errno.RPCExit(err, true) 处理。
// 映射成 10003 且带有原因时，原因只进日志，不进 BizMessage。
func statusErr(ctx context.Context, err error) error {
	return exitErr(ctx, err)
}

func exitErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	_, rpcErr := errno.RPCExit(err, false)
	logInternalCause(ctx, err, rpcErr)
	return rpcErr
}

// logInternalCause 落实约定 §5.3 第 3 条：未识别错误转成 10003 时，原因进日志，不进对外 msg。
// 登记过的业务错误和不带原因的 10003 哨兵（如解密失败，biz 已打过 ERROR）不再打 ERROR。
// 字段名用约定 §7.5：path 是方法名，err_code 是码。
func logInternalCause(ctx context.Context, raw, rpcErr error) {
	biz, ok := kerrors.FromBizStatusError(rpcErr)
	if !ok || biz == nil || biz.BizStatusCode() != errno.ErrInternal.Code {
		return
	}
	e := errno.From(raw)
	if e == nil || e.Unwrap() == nil {
		return
	}
	slog.ErrorContext(ctx, "map internal error",
		"path", methodName(ctx),
		"err_code", strconv.FormatInt(int64(errno.ErrInternal.Code), 10),
		"error", e.Unwrap(),
	)
}

func toCredential(in biz.MerchantCredential) *payment.GetMerchantCredentialResponse {
	return &payment.GetMerchantCredentialResponse{
		MerchantId:       in.MerchantID,
		Status:           in.Status,
		AppSecret:        in.AppSecret.Reveal(),
		SecretVersion:    toI32(in.SecretVersion),
		DefaultNotifyUrl: in.DefaultNotifyURL,
	}
}

func toPaymentOrder(in biz.PaymentOrder) *payment.PaymentOrder {
	out := &payment.PaymentOrder{
		PayNo:          in.PayNo,
		OutTradeNo:     in.OutTradeNo,
		Amount:         int64(in.Amount),
		FeeAmount:      int64(in.FeeAmount),
		RefundedAmount: int64(in.RefundedAmount),
		Currency:       in.Currency,
		Subject:        in.Subject,
		Status:         in.Status,
		ExpireAt:       formatTimestamp(in.ExpireAt),
		CreatedAt:      formatTimestamp(in.CreatedAt),
	}
	if in.Channel != "" {
		ch := in.Channel
		out.Channel = &ch
	}
	if !in.PaidAt.IsZero() {
		ts := formatTimestamp(in.PaidAt)
		out.PaidAt = &ts
	}
	if !in.ClosedAt.IsZero() {
		ts := formatTimestamp(in.ClosedAt)
		out.ClosedAt = &ts
	}
	if in.FailCode != "" {
		code := in.FailCode
		out.FailCode = &code
	}
	if in.FailMsg != "" {
		msg := in.FailMsg
		out.FailMsg = &msg
	}
	return out
}

func toRefundOrder(in biz.RefundOrder) *payment.RefundOrder {
	out := &payment.RefundOrder{
		RefundNo:    in.RefundNo,
		OutRefundNo: in.OutRefundNo,
		PayNo:       in.PayNo,
		Amount:      int64(in.Amount),
		Currency:    in.Currency,
		Reason:      in.Reason,
		Status:      in.Status,
		CreatedAt:   formatTimestamp(in.CreatedAt),
	}
	if !in.SucceededAt.IsZero() {
		ts := formatTimestamp(in.SucceededAt)
		out.SucceededAt = &ts
	}
	if in.FailCode != "" {
		code := in.FailCode
		out.FailCode = &code
	}
	if in.FailMsg != "" {
		msg := in.FailMsg
		out.FailMsg = &msg
	}
	return out
}

// toTransfer 按接口 §5.3 输出。fail_code、finished_at 只在有值时设置。
func toTransfer(in biz.TransferOrder) *payment.Transfer {
	out := &payment.Transfer{
		TransferNo:      in.TransferNo,
		OutTransferNo:   in.OutTransferNo,
		MerchantId:      in.MerchantID,
		PayeeMerchantId: in.PayeeMerchantID,
		Amount:          int64(in.Amount),
		Currency:        in.Currency,
		Mode:            in.Mode,
		Status:          in.Status,
		CreatedAt:       formatTimestamp(in.CreatedAt),
	}
	if in.FailCode != "" {
		code := in.FailCode
		out.FailCode = &code
	}
	if !in.FinishedAt.IsZero() {
		ts := formatTimestamp(in.FinishedAt)
		out.FinishedAt = &ts
	}
	return out
}

func formatTimestamp(t time.Time) base.Timestamp {
	return t.Truncate(time.Millisecond).In(shanghai).Format(timestampLayout)
}

// toI32 把库里的 INT 收成 i32。secret_version 是 MySQL INT，不会超出 int32。
func toI32(v int) int32 {
	if v > math.MaxInt32 || v < math.MinInt32 {
		return 0
	}
	return int32(v) //nolint:gosec // 上一行已经挡住溢出
}
