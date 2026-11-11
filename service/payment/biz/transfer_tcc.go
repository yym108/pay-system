package biz

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/obs"
)

const (
	faultTransferAfterTry          = "transfer.after_try"
	faultTransferAfterDecision     = "transfer.after_decision"
	faultTransferAfterPayerConfirm = "transfer.after_payer_confirm"
	// tccPhaseAttempts 含第一次。账户 WithTx 只有 3 秒，锁等待打满后分支返回超时，整组再跑。
	tccPhaseAttempts = 6
)

// transferInconsistent 是 pay_transfer_inconsistent_total。
// 进程内无标签计数，一次确认或取消里只要出现 40004 就加 1。/metrics 仍由 OPS-03 暴露。
var transferInconsistent atomic.Uint64

// TCCBrancher 在 payment 事务外调用账户的跨分片分支。nil error 表示这一步成功。
type TCCBrancher interface {
	Freeze(ctx context.Context, transferNo, payer string, amount money.Amount) error
	TryPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error
	ConfirmPayer(ctx context.Context, transferNo, payer string) error
	ConfirmPayee(ctx context.Context, transferNo, payee string) error
	CancelPayer(ctx context.Context, transferNo, payer string, amount money.Amount) error
	CancelPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error
}

// executeTCC 协调 mode=TCC。账户调用在事务外；决策和终态各自用本库事务。
func (p *Payments) executeTCC(ctx context.Context, row TransferOrder) (TransferOrder, error) {
	if p == nil || p.tccTransfer == nil || p.clk == nil || p.transfers == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	// 发号器类型不对就不要冻结或占用分支。终态事务还要用它写 outbox。
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
	endTry := obs.Begin(ctx, "", obs.StageTry)
	payerErr, payeeErr := p.tryBranches(ctx, row)
	// 有请求槽时留给下一阶段的 Begin 结束，中间的故障点和错误分类也算在 try 里，避免空隙。
	obs.KeepOrEnd(ctx, endTry)
	fault.Point(ctx, faultTransferAfterTry)
	if errors.Is(payerErr, context.Canceled) || errors.Is(payeeErr, context.Canceled) {
		return TransferOrder{}, context.Canceled
	}
	if payerErr == nil && payeeErr == nil {
		return p.afterDecide(ctx, row, transferStatusConfirming, "", gen)
	}
	picked := payerErr
	if picked == nil {
		picked = payeeErr
	}
	failCode, logIt, logCode := tccFailCode(picked)
	if logIt {
		logTransferTCC(ctx, "transfer tcc try", row.TransferNo, logCode)
	}
	return p.afterDecide(ctx, row, transferStatusCancelling, failCode, gen)
}

// tryBranches 等待两个 Try 都结束。不用会取消兄弟调用的 errgroup 上下文。
// 超时或 1213/1205 时整组再跑；WithTx 超时会回滚，再调 Freeze / TryPayee 是幂等的。
func (p *Payments) tryBranches(ctx context.Context, row TransferOrder) (error, error) {
	return repeatPhase(ctx, func() (error, error) {
		return runBranches(p.tccParallel, func() error {
			return p.tccTransfer.Freeze(ctx, row.TransferNo, row.MerchantID, row.Amount)
		}, func() error {
			return p.tccTransfer.TryPayee(ctx, row.TransferNo, row.PayeeMerchantID, row.Amount)
		})
	})
}

func (p *Payments) afterDecide(ctx context.Context, row TransferOrder, status, failCode string, gen *idgen.Generator) (TransferOrder, error) {
	at := p.clk.Now().Truncate(time.Millisecond)
	endDecision := obs.Begin(ctx, "", obs.StageDBDecision)
	got, applied, err := p.transfers.decideTransfer(ctx, row, status, failCode, at)
	obs.KeepOrEnd(ctx, endDecision)
	if err != nil {
		return TransferOrder{}, tccStoreErr(err)
	}
	if !applied {
		return p.continueDecided(ctx, got, gen)
	}
	fault.Point(ctx, faultTransferAfterDecision)
	return p.continueDecided(ctx, got, gen)
}

// continueDecided 按已经落盘的状态走确认或取消。终态直接返回，不再调用分支。
func (p *Payments) continueDecided(ctx context.Context, row TransferOrder, gen *idgen.Generator) (TransferOrder, error) {
	switch row.Status {
	case transferStatusConfirming:
		return p.confirmTCC(ctx, row, gen)
	case transferStatusCancelling:
		return p.cancelTCC(ctx, row, gen)
	case transferStatusSuccess, transferStatusFailed:
		return row, nil
	default:
		return row, errno.ErrInternal
	}
}

func (p *Payments) confirmTCC(ctx context.Context, row TransferOrder, gen *idgen.Generator) (TransferOrder, error) {
	endConfirm := obs.Begin(ctx, "", obs.StageConfirm)
	payerErr, payeeErr := repeatPhase(ctx, func() (error, error) {
		return p.confirmBranches(ctx, row)
	})
	obs.KeepOrEnd(ctx, endConfirm)
	return p.finishPhase(ctx, row, transferStatusConfirming, transferStatusSuccess, "", payerErr, payeeErr, gen)
}

func (p *Payments) confirmBranches(ctx context.Context, row TransferOrder) (error, error) {
	if !p.tccParallel {
		payerErr := p.tccTransfer.ConfirmPayer(ctx, row.TransferNo, row.MerchantID)
		// 串行时付款方返回后才数这个点，不论付款方是否成功，收款方仍要调用。
		fault.Point(ctx, faultTransferAfterPayerConfirm)
		payeeErr := p.tccTransfer.ConfirmPayee(ctx, row.TransferNo, row.PayeeMerchantID)
		return payerErr, payeeErr
	}
	return runBranches(true, func() error {
		return p.tccTransfer.ConfirmPayer(ctx, row.TransferNo, row.MerchantID)
	}, func() error {
		return p.tccTransfer.ConfirmPayee(ctx, row.TransferNo, row.PayeeMerchantID)
	})
}

func (p *Payments) cancelTCC(ctx context.Context, row TransferOrder, gen *idgen.Generator) (TransferOrder, error) {
	endCancel := obs.Begin(ctx, "", obs.StageCancel)
	payerErr, payeeErr := repeatPhase(ctx, func() (error, error) {
		return runBranches(p.tccParallel, func() error {
			return p.tccTransfer.CancelPayer(ctx, row.TransferNo, row.MerchantID, row.Amount)
		}, func() error {
			return p.tccTransfer.CancelPayee(ctx, row.TransferNo, row.PayeeMerchantID, row.Amount)
		})
	})
	obs.KeepOrEnd(ctx, endCancel)
	return p.finishPhase(ctx, row, transferStatusCancelling, transferStatusFailed, row.FailCode, payerErr, payeeErr, gen)
}

// repeatPhase 在超时或锁冲突时把两个分支再跑一整轮。含第一次最多 tccPhaseAttempts 轮。
// 父 ctx 已经结束，或任一边不是可重试错误，就停。Try、Confirm、Cancel 都走这里。
func repeatPhase(ctx context.Context, round func() (error, error)) (error, error) {
	var payerErr, payeeErr error
	for attempt := 0; attempt < tccPhaseAttempts; attempt++ {
		payerErr, payeeErr = round()
		if !tccRetryRound(ctx, payerErr, payeeErr) {
			return payerErr, payeeErr
		}
	}
	return payerErr, payeeErr
}

func tccRetryRound(ctx context.Context, payerErr, payeeErr error) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	if payerErr == nil && payeeErr == nil {
		return false
	}
	if payerErr != nil && !tccRetryableBranch(payerErr) {
		return false
	}
	if payeeErr != nil && !tccRetryableBranch(payeeErr) {
		return false
	}
	return true
}

// tccRetryableBranch 只认截止超时、事务已结束、结果集已关闭和 MySQL 1213/1205。
// 10003 的 Error() 不含 cause，这些原因仍挂在链上，不能因为外层是 *errno.Error 就放弃。
// 纯的 errno.ErrInternal（没有这些 cause）不重试。
func tccRetryableBranch(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, sql.ErrTxDone) {
		return true
	}
	// database/sql 的 errRowsClosed 未导出，只能按每一环的原文认出。
	for link := err; link != nil; link = errors.Unwrap(link) {
		if link.Error() == "sql: Rows are closed" {
			return true
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) && me != nil {
		return me.Number == 1213 || me.Number == 1205
	}
	return false
}

// finishPhase 把两个分支结果收成终态，或停在当前中间态。
func (p *Payments) finishPhase(ctx context.Context, row TransferOrder, from, to, failCode string, payerErr, payeeErr error, gen *idgen.Generator) (TransferOrder, error) {
	if errors.Is(payerErr, context.Canceled) || errors.Is(payeeErr, context.Canceled) {
		return TransferOrder{}, context.Canceled
	}
	if tccConflict(payerErr) || tccConflict(payeeErr) {
		transferInconsistent.Add(1)
		obs.AddTransferInconsistent("TCC")
		code := errno.ErrFreezeStatusConflict.Code
		if !tccConflict(payerErr) {
			code = errnoCode(payeeErr)
		} else if payerErr != nil {
			code = errnoCode(payerErr)
		}
		logTransferTCC(ctx, "transfer tcc inconsistent", row.TransferNo, code)
		return row, nil
	}
	if payerErr != nil || payeeErr != nil {
		return row, nil
	}
	at := p.clk.Now().Truncate(time.Millisecond)
	endFinish := obs.Begin(ctx, "", obs.StageDBFinishTx)
	got, _, err := p.transfers.finishTCCTransfer(ctx, row, from, to, failCode, at, gen, p.clk)
	if err != nil {
		endFinish()
		return TransferOrder{}, tccStoreErr(err)
	}
	obs.KeepOrEnd(ctx, endFinish)
	return got, nil
}

// runBranches 在 parallel 时同时跑两边，否则按参数顺序。任一边的错误都不取消另一边。
func runBranches(parallel bool, first, second func() error) (error, error) {
	if !parallel {
		return first(), second()
	}
	var payerErr, payeeErr error
	var g errgroup.Group
	g.Go(func() error {
		payerErr = first()
		return nil
	})
	g.Go(func() error {
		payeeErr = second()
		return nil
	})
	_ = g.Wait()
	return payerErr, payeeErr
}

// tccFailCode 把一个失败的 Try 收成 CANCELLING 的 fail_code。
// 第二个返回值表示要打 ERROR 日志。10001 和 10006 不能写成别的终态原因。
func tccFailCode(err error) (string, bool, int32) {
	if freezeTimedOut(err) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errno.ErrUnavailable) {
		return transferFailTryTimeout, false, 0
	}
	var known *errno.Error
	if errors.As(err, &known) && known != nil {
		switch known.Code {
		case errno.ErrInsufficientBalance.Code:
			return transferFailInsufficient, false, 0
		case errno.ErrAccountNotFound.Code, errno.ErrAccountInactive.Code:
			return transferFailAccountInvalid, false, 0
		case errno.ErrInvalidParam.Code, errno.ErrIdempotentConflict.Code:
			return transferFailTryTimeout, true, known.Code
		default:
			return transferFailTryTimeout, false, 0
		}
	}
	return transferFailTryTimeout, false, 0
}

func tccConflict(err error) bool {
	return errnoCode(err) == errno.ErrFreezeStatusConflict.Code
}

func errnoCode(err error) int32 {
	var known *errno.Error
	if errors.As(err, &known) && known != nil {
		return known.Code
	}
	return 0
}

func tccStoreErr(err error) error {
	if errors.Is(err, idgen.ErrClockRollback) {
		return errno.ErrUnavailable
	}
	return err
}

func logTransferTCC(ctx context.Context, msg, transferNo string, code int32) {
	slog.ErrorContext(ctx, msg,
		"transfer_no", transferNo,
		"err_code", strconv.Itoa(int(code)),
	)
}
