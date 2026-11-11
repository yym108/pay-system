package biz

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/outbox"
)

const (
	payResultSuccess      = "SUCCESS"
	payResultClosed       = "CLOSED"
	paySourceNotify       = "NOTIFY"
	paySourceQuery        = "QUERY"
	paySourceClose        = "CLOSE"
	paySourceJob          = "JOB"
	paySourceAdmin        = "ADMIN"
	failCodeChannelFailed = "CHANNEL_FAILED"
)

// payCallbackConflicts 是 pay_callback_conflicts_total{channel}。
// 按渠道分桶，进程内计数。不起 HTTP，也不引入 Prometheus 客户端。
var payCallbackConflicts sync.Map

// errChannelMiss 让 WithTx 回滚影响 0 行的渠道更新。不是 MySQL 错误号，不会触发事务重试。
var errChannelMiss = errors.New("pay result: channel not processing")

// errOrderMiss 让 WithTx 回滚订单 CAS 未命中的事务，渠道流水回到 PROCESSING。
var errOrderMiss = errors.New("pay result: order not paying")

// PayResult 是一次渠道终态。Amount 为 nil 表示这次结果没有带金额，不参与比较。
// ChannelTxnNo 为空时写成 SQL NULL，避免 COALESCE 把已有流水号盖成空。
// FinishedAt 为零表示调用方没有传渠道完成时间。
type PayResult struct {
	BizNo            string
	Result           string
	Source           string
	FailCode         string
	FailMsg          string
	Amount           *money.Amount
	ChannelTxnNo     string
	ChannelStatusRaw string
	FinishedAt       time.Time
}

const applyChannelSQL = `
UPDATE channel_txn
   SET status = ?,
       channel_txn_no = COALESCE(channel_txn_no, ?),
       channel_status_raw = ?,
       channel_finished_at = ?,
       next_query_at = NULL,
       version = version + 1,
       updated_at = ?
 WHERE biz_no = ? AND status = 'PROCESSING'`

const applyChannelAmountSQL = `
UPDATE channel_txn
   SET status = ?,
       channel_txn_no = COALESCE(channel_txn_no, ?),
       channel_status_raw = ?,
       channel_finished_at = ?,
       next_query_at = NULL,
       version = version + 1,
       updated_at = ?
 WHERE biz_no = ? AND status = 'PROCESSING' AND amount = ?`

const applyChannelNotifySQL = `
UPDATE channel_txn
   SET status = ?,
       channel_txn_no = COALESCE(channel_txn_no, ?),
       channel_status_raw = ?,
       channel_finished_at = ?,
       next_query_at = NULL,
       notify_count = notify_count + 1,
       version = version + 1,
       updated_at = ?
 WHERE biz_no = ? AND status = 'PROCESSING'`

const applyChannelNotifyAmountSQL = `
UPDATE channel_txn
   SET status = ?,
       channel_txn_no = COALESCE(channel_txn_no, ?),
       channel_status_raw = ?,
       channel_finished_at = ?,
       next_query_at = NULL,
       notify_count = notify_count + 1,
       version = version + 1,
       updated_at = ?
 WHERE biz_no = ? AND status = 'PROCESSING' AND amount = ?`

const applyOrderSuccessSQL = `
UPDATE payment_order
   SET status = 'SUCCESS', paid_at = ?, version = version + 1, updated_at = ?
 WHERE pay_no = ? AND status = 'PAYING'`

const applyOrderFailedSQL = `
UPDATE payment_order
   SET status = 'FAILED', fail_code = ?, fail_msg = ?, version = version + 1, updated_at = ?
 WHERE pay_no = ? AND status = 'PAYING'`

const applyOrderClosedSQL = `
UPDATE payment_order
   SET status = 'CLOSED', closed_at = ?, version = version + 1, updated_at = ?
 WHERE pay_no = ? AND status = 'PAYING'`

const bumpNotifySQL = `
UPDATE channel_txn
   SET notify_count = notify_count + 1, updated_at = ?
 WHERE biz_no = ? AND status = ?`

// ApplyPayResult 把渠道终态写进 channel_txn 和 payment_order。
// 成功时返回订单且 error 为 nil。FAILED 与 CLOSED 不写 outbox。
// 只有 SUCCESS 在同一事务里调用 outbox.Add。本函数不调用外部系统，也不调用故障点。
func (p *Payments) ApplyPayResult(ctx context.Context, in PayResult) (PaymentOrder, error) {
	if err := validatePayResult(in); err != nil {
		return PaymentOrder{}, err
	}
	if p == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	if err := p.ready(ctx); err != nil {
		return PaymentOrder{}, err
	}
	db, ok := p.paymentDB()
	if !ok {
		return PaymentOrder{}, errno.ErrInternal
	}
	var gen *idgen.Generator
	if in.Result == payResultSuccess {
		gen, ok = p.ids.(*idgen.Generator)
		if !ok || gen == nil {
			return PaymentOrder{}, errno.ErrInternal
		}
	}
	now := p.clk.Now().Truncate(time.Millisecond)
	var finished time.Time
	if !in.FinishedAt.IsZero() {
		finished = in.FinishedAt.Truncate(time.Millisecond)
	}
	ctx = outbox.WithCollected(ctx)
	endCallback := obs.Stage(ctx, obs.ChainPaySettle, obs.StageCallbackTx)
	var stored PaymentOrder
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		order, txErr := applyPayResultTx(ctx, tx, in, now, finished, gen, p.clk)
		if txErr != nil {
			return txErr
		}
		stored = order
		return nil
	})
	endCallback()
	if err == nil && in.Result == payResultSuccess {
		for _, id := range outbox.CollectedIDs(ctx) {
			obs.HoldWait(obs.OutboxWaitKey(id), obs.ChainPaySettle, obs.StageOutboxWait)
		}
	}
	if errors.Is(err, errChannelMiss) {
		return p.afterChannelMiss(ctx, in, now)
	}
	if errors.Is(err, errOrderMiss) {
		slog.ErrorContext(ctx, "pay result order cas failed",
			"pay_no", in.BizNo,
			"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
		)
		return PaymentOrder{}, errno.ErrInternal
	}
	if errors.Is(err, idgen.ErrClockRollback) {
		return PaymentOrder{}, errno.ErrUnavailable
	}
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	return stored, nil
}

func validatePayResult(in PayResult) error {
	if in.BizNo == "" || !knownPayResult(in.Result) || !knownPaySource(in.Source) {
		return errno.ErrInvalidParam
	}
	if in.Result == payResultSuccess && in.FinishedAt.IsZero() {
		return errno.ErrInvalidParam
	}
	return nil
}

func knownPayResult(result string) bool {
	switch result {
	case payResultSuccess, payResultFailed, payResultClosed:
		return true
	default:
		return false
	}
}

func knownPaySource(source string) bool {
	switch source {
	case paySourceNotify, paySourceQuery, paySourceClose, paySourceJob, paySourceAdmin, paySourceSync:
		return true
	default:
		return false
	}
}

func (p *Payments) paymentDB() (*sqlx.DB, bool) {
	if p == nil || p.orders == nil {
		return nil, false
	}
	store, ok := p.orders.(sqlOrderStore)
	if !ok || store.db == nil {
		return nil, false
	}
	return store.db, true
}

func applyPayResultTx(ctx context.Context, tx *sqlx.Tx, in PayResult, now, finished time.Time, gen *idgen.Generator, clk clock.Clock) (PaymentOrder, error) {
	channelSQL := applyChannelSQL
	switch {
	case in.Source == paySourceNotify && in.Amount != nil:
		channelSQL = applyChannelNotifyAmountSQL
	case in.Source == paySourceNotify:
		channelSQL = applyChannelNotifySQL
	case in.Amount != nil:
		channelSQL = applyChannelAmountSQL
	}
	args := channelArgs(in, now, finished)
	res, err := tx.ExecContext(ctx, channelSQL, args...)
	if err != nil {
		return PaymentOrder{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return PaymentOrder{}, err
	}
	if n == 0 {
		return PaymentOrder{}, errChannelMiss
	}
	if n != 1 {
		return PaymentOrder{}, errno.ErrInternal
	}
	if err = execOrder(ctx, tx, in, now, finished); err != nil {
		return PaymentOrder{}, err
	}
	order, err := scanPayment(tx.QueryRowContext(ctx, paymentByPayNoSQL, in.BizNo))
	if err != nil {
		return PaymentOrder{}, err
	}
	if order.PayNo != in.BizNo {
		return PaymentOrder{}, errno.ErrInternal
	}
	if in.Result != payResultSuccess {
		return order, nil
	}
	// paid_at 用渠道完成时间。对账按这个时间归属，不能改成本事务的注入时钟。
	err = outbox.Add(ctx, tx, outbox.Event{
		Type:       outbox.EventPaymentSucceeded,
		PayNo:      order.PayNo,
		MerchantID: order.MerchantID,
		OutTradeNo: order.OutTradeNo,
		Channel:    order.Channel,
		Amount:     order.Amount,
		FeeAmount:  order.FeeAmount,
		PaidAt:     finished,
	}, gen, clk)
	if err != nil {
		return PaymentOrder{}, err
	}
	return order, nil
}

func channelArgs(in PayResult, now, finished time.Time) []any {
	var finishedArg any
	if !in.FinishedAt.IsZero() {
		finishedArg = finished
	}
	args := []any{
		in.Result,
		nullString(in.ChannelTxnNo),
		nullString(in.ChannelStatusRaw),
		finishedArg,
		now,
		in.BizNo,
	}
	if in.Amount != nil {
		args = append(args, int64(*in.Amount))
	}
	return args
}

func execOrder(ctx context.Context, tx *sqlx.Tx, in PayResult, now, finished time.Time) error {
	var (
		query string
		args  []any
	)
	switch in.Result {
	case payResultSuccess:
		query = applyOrderSuccessSQL
		args = []any{finished, now, in.BizNo}
	case payResultFailed:
		code := in.FailCode
		if code == "" {
			code = failCodeChannelFailed
		}
		query = applyOrderFailedSQL
		args = []any{code, in.FailMsg, now, in.BizNo}
	case payResultClosed:
		closedAt := now
		if !in.FinishedAt.IsZero() {
			closedAt = finished
		}
		query = applyOrderClosedSQL
		args = []any{closedAt, now, in.BizNo}
	default:
		return errno.ErrInvalidParam
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errOrderMiss
	}
	obs.OrderTransition(txnTypePay, payStatusPaying, in.Result)
	return nil
}

func (p *Payments) afterChannelMiss(ctx context.Context, in PayResult, now time.Time) (PaymentOrder, error) {
	txn, ok, err := p.orders.channelTxnByBizNo(ctx, in.BizNo)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !ok || txn.BizNo != in.BizNo {
		return PaymentOrder{}, errno.ErrOrderNotFound
	}
	// 金额先于终态比较。已终态但金额不对仍是 50002，不能当成重复成功或冲突。
	if in.Amount != nil && *in.Amount != txn.Amount {
		slog.ErrorContext(ctx, "pay result amount mismatch",
			"pay_no", in.BizNo,
			"err_code", strconv.Itoa(int(errno.ErrChannelAmountMismatch.Code)),
		)
		return PaymentOrder{}, errno.ErrChannelAmountMismatch
	}
	if txn.Status == in.Result && isPayTerminal(txn.Status) {
		if in.Source == paySourceNotify {
			moved, bumpErr := p.bumpNotify(ctx, in.BizNo, txn.Status, now)
			if bumpErr != nil {
				return PaymentOrder{}, bumpErr
			}
			if moved != "" && moved != txn.Status {
				return p.noteConflict(ctx, in.BizNo, txn.Channel, moved, in.Result)
			}
		}
		return p.currentOrder(ctx, in.BizNo)
	}
	if isPayTerminal(txn.Status) && txn.Status != in.Result {
		return p.noteConflict(ctx, in.BizNo, txn.Channel, txn.Status, in.Result)
	}
	slog.ErrorContext(ctx, "pay result channel miss",
		"pay_no", in.BizNo,
		"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
	)
	return PaymentOrder{}, errno.ErrInternal
}

func (p *Payments) bumpNotify(ctx context.Context, bizNo, status string, now time.Time) (string, error) {
	db, ok := p.paymentDB()
	if !ok {
		return "", errno.ErrInternal
	}
	res, err := db.ExecContext(ctx, bumpNotifySQL, now, bizNo, status)
	if err != nil {
		return "", callErr(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", callErr(err)
	}
	if n == 1 {
		return "", nil
	}
	txn, found, err := p.orders.channelTxnByBizNo(ctx, bizNo)
	if err != nil {
		return "", callErr(err)
	}
	if !found || txn.BizNo != bizNo {
		return "", errno.ErrOrderNotFound
	}
	if txn.Status == status {
		return "", nil
	}
	return txn.Status, nil
}

func (p *Payments) noteConflict(ctx context.Context, payNo, channel, from, to string) (PaymentOrder, error) {
	addCallbackConflict(channel)
	slog.ErrorContext(ctx, "pay result conflict",
		"pay_no", payNo,
		"channel", channel,
		"from_status", from,
		"to_status", to,
	)
	return p.currentOrder(ctx, payNo)
}

func (p *Payments) currentOrder(ctx context.Context, payNo string) (PaymentOrder, error) {
	order, ok, err := p.orders.paymentByPayNo(ctx, payNo)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !ok || order.PayNo != payNo {
		return PaymentOrder{}, errno.ErrInternal
	}
	return order, nil
}

func nullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

func isPayTerminal(status string) bool {
	switch status {
	case payResultSuccess, payResultFailed, payResultClosed:
		return true
	default:
		return false
	}
}

func addCallbackConflict(channel string) {
	actual, _ := payCallbackConflicts.LoadOrStore(channel, new(atomic.Uint64))
	counter, ok := actual.(*atomic.Uint64)
	if !ok {
		return
	}
	counter.Add(1)
	obs.AddCallbackConflict(channel)
}

func callbackConflicts(channel string) uint64 {
	actual, ok := payCallbackConflicts.Load(channel)
	if !ok {
		return 0
	}
	counter, ok := actual.(*atomic.Uint64)
	if !ok {
		return 0
	}
	return counter.Load()
}
