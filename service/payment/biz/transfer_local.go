package biz

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/obs"
)

// LocalTransferResult 是一次同分片转账的业务结果。
// 余额不足时 error 为 nil，FailCode 为 INSUFFICIENT_BALANCE，不能改成 40002。
type LocalTransferResult struct {
	Status   string
	TxnNo    string
	FailCode string
}

// LocalTransferrer 在事务外调用 AccountService.TransferLocal。
// 超时、10003、10004 由调用方保持 TRYING，不在这里改转账单。
type LocalTransferrer interface {
	TransferLocal(ctx context.Context, transferNo, payerMerchantID, payeeMerchantID string, amount money.Amount) (LocalTransferResult, error)
}

// executeLocal 执行 mode=LOCAL 的转账。账户调用在事务外；CAS 与 outbox 在 finishTransfer 的事务里。
func (p *Payments) executeLocal(ctx context.Context, row TransferOrder) (TransferOrder, error) {
	if p == nil || p.localTransfer == nil || p.clk == nil || p.transfers == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	// 发号器类型不对就不要调用账户。账户成功后再发现写不了 outbox，钱已经过去、单仍是 TRYING。
	gen, ok := p.ids.(*idgen.Generator)
	if !ok || gen == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	if ctx == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return TransferOrder{}, err
	}
	endRPC := obs.Begin(ctx, "", obs.StageRPCTransferLocal)
	res, err := p.localTransfer.TransferLocal(ctx, row.TransferNo, row.MerchantID, row.PayeeMerchantID, row.Amount)
	endRPC()
	if err != nil {
		return p.classifyLocalErr(ctx, row, err, gen)
	}
	return p.applyLocalResult(ctx, row, res, gen)
}

func (p *Payments) classifyLocalErr(ctx context.Context, row TransferOrder, err error, gen *idgen.Generator) (TransferOrder, error) {
	// 超时优先于父 ctx。截止时间先到时结果仍未知，保持 TRYING，接口返回 code 0。
	if freezeTimedOut(err) || errors.Is(err, errno.ErrUnavailable) || errors.Is(err, errno.ErrInternal) {
		return row, nil
	}
	var known *errno.Error
	if errors.As(err, &known) && known != nil {
		switch known.Code {
		case errno.ErrInvalidParam.Code, errno.ErrIdempotentConflict.Code:
			logTransferLocal(ctx, "transfer local inconsistent", row.TransferNo, known.Code)
			return row, nil
		case errno.ErrAccountNotFound.Code, errno.ErrAccountInactive.Code:
			logTransferLocal(ctx, "transfer account invalid", row.TransferNo, known.Code)
			return p.finishLocal(ctx, row, transferStatusFailed, transferFailAccountInvalid, "", gen)
		}
	}
	if ctx.Err() != nil {
		return TransferOrder{}, ctx.Err()
	}
	logTransferLocal(ctx, "transfer local inconsistent", row.TransferNo, errno.ErrInternal.Code)
	return row, nil
}

func (p *Payments) applyLocalResult(ctx context.Context, row TransferOrder, res LocalTransferResult, gen *idgen.Generator) (TransferOrder, error) {
	switch res.Status {
	case transferStatusSuccess:
		if res.TxnNo == "" {
			logTransferLocal(ctx, "transfer local inconsistent", row.TransferNo, errno.ErrInternal.Code)
			return row, nil
		}
		return p.finishLocal(ctx, row, transferStatusSuccess, "", res.TxnNo, gen)
	case transferStatusFailed:
		if res.FailCode == "" {
			logTransferLocal(ctx, "transfer local inconsistent", row.TransferNo, errno.ErrInternal.Code)
			return row, nil
		}
		return p.finishLocal(ctx, row, transferStatusFailed, res.FailCode, "", gen)
	default:
		logTransferLocal(ctx, "transfer local inconsistent", row.TransferNo, errno.ErrInternal.Code)
		return row, nil
	}
}

func (p *Payments) finishLocal(ctx context.Context, row TransferOrder, status, failCode, txnNo string, gen *idgen.Generator) (TransferOrder, error) {
	at := p.clk.Now().Truncate(time.Millisecond)
	endFinish := obs.Begin(ctx, "", obs.StageDBFinishTx)
	got, _, err := p.transfers.finishTransfer(ctx, row, status, failCode, txnNo, at, gen, p.clk)
	if err != nil {
		endFinish()
		if errors.Is(err, idgen.ErrClockRollback) {
			return TransferOrder{}, errno.ErrUnavailable
		}
		return TransferOrder{}, err
	}
	obs.KeepOrEnd(ctx, endFinish)
	return got, nil
}

func logTransferLocal(ctx context.Context, msg, transferNo string, code int32) {
	slog.ErrorContext(ctx, msg,
		"transfer_no", transferNo,
		"err_code", strconv.Itoa(int(code)),
	)
}
