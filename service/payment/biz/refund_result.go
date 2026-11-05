package biz

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/outbox"
)

const (
	failCodeRefundTimeout = "REFUND_TIMEOUT"
	faultCallbackCommit   = "callback.after_commit"
)

// refundQuotaInconsistent 是 pay_refund_quota_inconsistent_total（REF-03 第 5 条）。
// 进程内计数，由 OPS-03 暴露。
var refundQuotaInconsistent atomic.Uint64

var (
	// errRefundChannelMiss 让 WithTx 回滚影响 0 行的渠道更新。不是 MySQL 错误号，不会触发事务重试。
	errRefundChannelMiss = errors.New("refund result: channel not processing")
	// errRefundOrderMiss 让 WithTx 回滚退款单 CAS 未命中的事务，渠道流水回到 PROCESSING。
	errRefundOrderMiss = errors.New("refund result: refund not processing")
	// errRefundInitMiss 表示 INIT → FAILED 的 CAS 未命中。
	errRefundInitMiss = errors.New("refund result: refund not init")
	// errRefundQuota 表示释放 refunded_amount 影响 0 行，整个事务回滚。
	errRefundQuota = errors.New("refund result: quota inconsistent")
)

// ApplyRefundResult 把一次退款终态写进 refund_order，并在同一事务里推进 channel_txn、释放额度、写 outbox。
//
// 分支按这次结果的来源意图选，不按库里读到的状态选：
// FAILED 且失败原因是 INSUFFICIENT_BALANCE、FREEZE_CONFLICT、REFUND_TIMEOUT 时走 INIT 分支（REF-03 第 2 条），
// 只 CAS refund_order INIT → FAILED，不碰 channel_txn；其余走渠道分支（第 1 条），先 CAS channel_txn。
// 这样卡单恢复判 INIT 超时与退款被推进为 PROCESSING 并发时，INIT CAS 未命中，不会把已提交渠道的退款判失败。
// 本函数不调用外部系统，也不调用故障点。
func (p *Payments) ApplyRefundResult(ctx context.Context, in RefundResult) (RefundOrder, error) {
	if err := validateRefundResult(in); err != nil {
		return RefundOrder{}, err
	}
	if p == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	if err := p.refundReady(ctx); err != nil {
		return RefundOrder{}, err
	}
	db, ok := p.paymentDB()
	if !ok {
		return RefundOrder{}, errno.ErrInternal
	}
	gen, ok := p.ids.(*idgen.Generator)
	if !ok || gen == nil {
		return RefundOrder{}, errno.ErrInternal
	}
	now := p.clk.Now().Truncate(time.Millisecond)
	if isInitFailure(in) {
		return p.applyRefundInit(ctx, db, in, now, gen)
	}
	return p.applyRefundChannel(ctx, db, in, now, gen)
}

func validateRefundResult(in RefundResult) error {
	if in.RefundNo == "" || !knownRefundSource(in.Source) {
		return errno.ErrInvalidParam
	}
	switch in.Result {
	case payResultSuccess:
		if in.FinishedAt.IsZero() || in.FailCode != "" {
			return errno.ErrInvalidParam
		}
	case payResultFailed:
		if !knownRefundFailCode(in.FailCode) {
			return errno.ErrInvalidParam
		}
	default:
		return errno.ErrInvalidParam
	}
	return nil
}

// knownRefundSource 与 PAY-04 的来源相同，但不含 CLOSE：退款没有关单路径。
func knownRefundSource(source string) bool {
	switch source {
	case paySourceNotify, paySourceQuery, paySourceJob, paySourceAdmin, paySourceSync:
		return true
	default:
		return false
	}
}

// knownRefundFailCode 列出约定 §5.4 的退款 fail_code。空串表示渠道 FAILED 未带原因，落库为 CHANNEL_FAILED。
func knownRefundFailCode(code string) bool {
	switch code {
	case "", failCodeChannelFailed, failCodeChannelRejected,
		failCodeInsufficientBalance, failCodeFreezeConflict, failCodeRefundTimeout:
		return true
	default:
		return false
	}
}

// isInitFailure 判断这次结果是否属于 INIT 退款的失败（冻结失败或超时失败），这类失败渠道一定没有收到请求。
func isInitFailure(in RefundResult) bool {
	if in.Result != payResultFailed {
		return false
	}
	switch in.FailCode {
	case failCodeInsufficientBalance, failCodeFreezeConflict, failCodeRefundTimeout:
		return true
	default:
		return false
	}
}

// quotaMiss 记下释放额度失败的那一行。日志和计数在 WithTx 回滚之后输出，记录时这次调用已经没有留下改动。
// errRefundQuota 不是 MySQL 错误号，不会触发事务重试。
type quotaMiss struct {
	refundNo string
	payNo    string
	amount   int64
}

func (p *Payments) applyRefundChannel(ctx context.Context, db *sqlx.DB, in RefundResult, now time.Time, gen *idgen.Generator) (RefundOrder, error) {
	var finished time.Time
	if !in.FinishedAt.IsZero() {
		finished = in.FinishedAt.Truncate(time.Millisecond)
	}
	var (
		stored RefundOrder
		miss   quotaMiss
	)
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		row, txErr := applyRefundChannelTx(ctx, tx, in, now, finished, gen, p.clk)
		if txErr != nil {
			if errors.Is(txErr, errRefundQuota) {
				miss = quotaMiss{refundNo: row.RefundNo, payNo: row.PayNo, amount: int64(row.Amount)}
			}
			return txErr
		}
		stored = row
		return nil
	})
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, errRefundChannelMiss):
		return p.afterRefundChannelMiss(ctx, in, now)
	case errors.Is(err, errRefundOrderMiss):
		slog.ErrorContext(ctx, "refund result order cas failed",
			"refund_no", in.RefundNo,
			"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
		)
		return RefundOrder{}, errno.ErrInternal
	default:
		return RefundOrder{}, refundTxErr(ctx, err, miss)
	}
}

func applyRefundChannelTx(ctx context.Context, tx *sqlx.Tx, in RefundResult, now, finished time.Time, gen *idgen.Generator, clk clock.Clock) (RefundOrder, error) {
	channelSQL := applyRefundChannelSQL
	switch {
	case in.Source == paySourceNotify && in.Amount != nil:
		channelSQL = applyRefundChannelNotifyAmountSQL
	case in.Source == paySourceNotify:
		channelSQL = applyRefundChannelNotifySQL
	case in.Amount != nil:
		channelSQL = applyRefundChannelAmountSQL
	}
	args := channelArgs(PayResult{
		BizNo:            in.RefundNo,
		Result:           in.Result,
		Amount:           in.Amount,
		ChannelTxnNo:     in.ChannelTxnNo,
		ChannelStatusRaw: in.ChannelStatusRaw,
		FinishedAt:       in.FinishedAt,
	}, now, finished)
	if err := execOne(ctx, tx, errRefundChannelMiss, channelSQL, args...); err != nil {
		return RefundOrder{}, err
	}
	var (
		query  string
		qargs  []any
		failed = in.Result == payResultFailed
	)
	if failed {
		code := in.FailCode
		if code == "" {
			code = failCodeChannelFailed
		}
		query = applyRefundFailedSQL
		qargs = []any{code, in.FailMsg, now, in.RefundNo}
	} else {
		query = applyRefundSuccessSQL
		qargs = []any{finished, now, in.RefundNo}
	}
	if err := execOne(ctx, tx, errRefundOrderMiss, query, qargs...); err != nil {
		return RefundOrder{}, err
	}
	row, err := scanRefund(tx.QueryRowContext(ctx, refundByNoSQL, in.RefundNo))
	if err != nil {
		return RefundOrder{}, err
	}
	if row.RefundNo != in.RefundNo {
		return RefundOrder{}, errno.ErrInternal
	}
	if failed {
		return releaseAndFail(ctx, tx, row, now, gen, clk)
	}
	// succeeded_at 用渠道完成时间。对账按这个时间归属，不能改成本事务的注入时钟。
	err = outbox.Add(ctx, tx, outbox.Event{
		Type:        outbox.EventRefundSucceeded,
		RefundNo:    row.RefundNo,
		PayNo:       row.PayNo,
		MerchantID:  row.MerchantID,
		OutRefundNo: row.OutRefundNo,
		Channel:     row.Channel,
		Amount:      row.Amount,
		SucceededAt: finished,
	}, gen, clk)
	if err != nil {
		return RefundOrder{}, err
	}
	return row, nil
}

func (p *Payments) applyRefundInit(ctx context.Context, db *sqlx.DB, in RefundResult, now time.Time, gen *idgen.Generator) (RefundOrder, error) {
	var (
		stored RefundOrder
		miss   quotaMiss
	)
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		if txErr := execOne(ctx, tx, errRefundInitMiss, applyRefundInitFailedSQL, in.FailCode, in.FailMsg, now, in.RefundNo); txErr != nil {
			return txErr
		}
		row, txErr := scanRefund(tx.QueryRowContext(ctx, refundByNoSQL, in.RefundNo))
		if txErr != nil {
			return txErr
		}
		if row.RefundNo != in.RefundNo {
			return errno.ErrInternal
		}
		row, txErr = releaseAndFail(ctx, tx, row, now, gen, p.clk)
		if txErr != nil {
			if errors.Is(txErr, errRefundQuota) {
				miss = quotaMiss{refundNo: row.RefundNo, payNo: row.PayNo, amount: int64(row.Amount)}
			}
			return txErr
		}
		stored = row
		return nil
	})
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, errRefundInitMiss):
		return p.afterRefundInitMiss(ctx, in)
	default:
		return RefundOrder{}, refundTxErr(ctx, err, miss)
	}
}

// releaseAndFail 释放 refunded_amount 并写 refund.failed。释放影响 0 行时返回 errRefundQuota 和这一行，调用方据此回滚并记日志。
func releaseAndFail(ctx context.Context, tx *sqlx.Tx, row RefundOrder, now time.Time, gen *idgen.Generator, clk clock.Clock) (RefundOrder, error) {
	r := int64(row.Amount)
	res, err := tx.ExecContext(ctx, releaseRefundQuotaSQL, r, now, row.PayNo, r)
	if err != nil {
		return RefundOrder{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return RefundOrder{}, err
	}
	if n == 0 {
		return row, errRefundQuota
	}
	if n != 1 {
		return RefundOrder{}, errno.ErrInternal
	}
	err = outbox.Add(ctx, tx, outbox.Event{
		Type:        outbox.EventRefundFailed,
		RefundNo:    row.RefundNo,
		PayNo:       row.PayNo,
		MerchantID:  row.MerchantID,
		OutRefundNo: row.OutRefundNo,
		Amount:      row.Amount,
		FailCode:    row.FailCode,
	}, gen, clk)
	if err != nil {
		return RefundOrder{}, err
	}
	return row, nil
}

// execOne 执行一条条件更新。影响 0 行返回 miss，影响多于 1 行返回 10003。
func execOne(ctx context.Context, tx *sqlx.Tx, miss error, query string, args ...any) error {
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return miss
	}
	if n != 1 {
		return errno.ErrInternal
	}
	return nil
}

// refundTxErr 映射事务错误。额度不一致在事务回滚之后才记 ERROR 日志和计数。
func refundTxErr(ctx context.Context, err error, miss quotaMiss) error {
	switch {
	case errors.Is(err, errRefundQuota):
		refundQuotaInconsistent.Add(1)
		slog.ErrorContext(ctx, "refund quota inconsistent",
			"refund_no", miss.refundNo,
			"pay_no", miss.payNo,
			"amount", miss.amount,
			"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
		)
		return errno.ErrInternal
	case errors.Is(err, idgen.ErrClockRollback):
		return errno.ErrUnavailable
	default:
		return callErr(err)
	}
}

// afterRefundChannelMiss 处理渠道 CAS 未命中，规则同 PAY-04 第 3 条：先比金额，再看终态。
func (p *Payments) afterRefundChannelMiss(ctx context.Context, in RefundResult, now time.Time) (RefundOrder, error) {
	txn, ok, err := p.orders.channelTxnByBizNo(ctx, in.RefundNo)
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	// PAY 流水不是退款流水。按退款单号查到支付流水时视为不存在。
	if !ok || txn.BizNo != in.RefundNo || txn.TxnType != txnTypeRefund {
		return RefundOrder{}, errno.ErrOrderNotFound
	}
	if in.Amount != nil && *in.Amount != txn.Amount {
		slog.ErrorContext(ctx, "refund result amount mismatch",
			"refund_no", in.RefundNo,
			"err_code", strconv.Itoa(int(errno.ErrChannelAmountMismatch.Code)),
		)
		return RefundOrder{}, errno.ErrChannelAmountMismatch
	}
	if txn.Status == in.Result {
		if in.Source == paySourceNotify {
			moved, bumpErr := p.bumpNotify(ctx, in.RefundNo, txn.Status, now)
			if bumpErr != nil {
				return RefundOrder{}, bumpErr
			}
			if moved != "" && moved != txn.Status {
				return p.noteRefundConflict(ctx, in.RefundNo, txn.Channel, moved, in.Result)
			}
		}
		return p.currentRefund(ctx, in.RefundNo)
	}
	if isPayTerminal(txn.Status) {
		return p.noteRefundConflict(ctx, in.RefundNo, txn.Channel, txn.Status, in.Result)
	}
	slog.ErrorContext(ctx, "refund result channel miss",
		"refund_no", in.RefundNo,
		"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
	)
	return RefundOrder{}, errno.ErrInternal
}

// afterRefundInitMiss 处理 INIT → FAILED 未命中。已 FAILED 视为重复，返回该单。
// 已 SUCCESS 是并发竞争（INIT 失败与另一路冻结、提交并收到 SUCCESS），不是渠道给出的矛盾结果：
// WARN 日志，不计 pay_callback_conflicts_total，返回当前退款单且 error 为 nil，调用方按返回的状态判断。
// 仍在 INIT 或 PROCESSING 返回 10003。
func (p *Payments) afterRefundInitMiss(ctx context.Context, in RefundResult) (RefundOrder, error) {
	row, ok, err := p.orders.refundByNo(ctx, in.RefundNo)
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	if !ok || row.RefundNo != in.RefundNo {
		return RefundOrder{}, errno.ErrOrderNotFound
	}
	switch row.Status {
	case refundStatusFailed:
		return row, nil
	case payResultSuccess:
		slog.WarnContext(ctx, "refund result not init",
			"refund_no", row.RefundNo,
			"status", row.Status,
			"fail_code", in.FailCode,
		)
		return row, nil
	default:
		slog.WarnContext(ctx, "refund result not init",
			"refund_no", row.RefundNo,
			"status", row.Status,
			"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
		)
		return RefundOrder{}, errno.ErrInternal
	}
}

func (p *Payments) noteRefundConflict(ctx context.Context, refundNo, channel, from, to string) (RefundOrder, error) {
	addCallbackConflict(channel)
	slog.ErrorContext(ctx, "refund result conflict",
		"refund_no", refundNo,
		"channel", channel,
		"from_status", from,
		"to_status", to,
	)
	return p.currentRefund(ctx, refundNo)
}

func (p *Payments) currentRefund(ctx context.Context, refundNo string) (RefundOrder, error) {
	row, ok, err := p.orders.refundByNo(ctx, refundNo)
	if err != nil {
		return RefundOrder{}, callErr(err)
	}
	if !ok || row.RefundNo != refundNo {
		return RefundOrder{}, errno.ErrInternal
	}
	return row, nil
}

// ApplyRefundNotify 把 REFUND 回调交给 ApplyRefundResult。只接受 source = NOTIFY 和渠道侧的失败原因。
// 提交成功之后才调用故障点 callback.after_commit。返回 APPLIED、DUPLICATE 或 CONFLICT，含义同 ApplyChannelNotify。
func (p *Payments) ApplyRefundNotify(ctx context.Context, in RefundResult) (string, error) {
	if err := validateRefundResult(in); err != nil {
		return "", err
	}
	if in.Source != paySourceNotify || isInitFailure(in) {
		return "", errno.ErrInvalidParam
	}
	if p == nil || p.orders == nil || ctx == nil {
		return "", errno.ErrInternal
	}
	before, ok, err := p.orders.channelTxnByBizNo(ctx, in.RefundNo)
	if err != nil {
		return "", callErr(err)
	}
	row, err := p.ApplyRefundResult(ctx, in)
	if err != nil {
		return "", err
	}
	fault.Point(ctx, faultCallbackCommit)
	if row.RefundNo == "" {
		return "", errno.ErrInternal
	}
	if ok && isPayTerminal(before.Status) && before.Status != in.Result {
		return "CONFLICT", nil
	}
	if ok && before.Status == in.Result {
		return "DUPLICATE", nil
	}
	return "APPLIED", nil
}
