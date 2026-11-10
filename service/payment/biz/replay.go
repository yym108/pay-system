package biz

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/outbox"
)

// 运行镜像没有 tzdata。固定 +8 与 outbox.Add 的时间一致。
var replayShanghai = time.FixedZone("Asia/Shanghai", 8*60*60)

// testReplayAfterMiss 只给同包测试。生产保持 nil。
// 按业务单号第一次没找到 outbox 之后、打开重建事务之前调用，用来插入一行，证明等锁期间已有行就不再插。
var testReplayAfterMiss func()

// ReplayInput 是重放入参。EventID 与 BizNo+EventType 二选一。空串算没提供，不 trim。
type ReplayInput struct {
	EventID   string
	BizNo     string
	EventType string
}

// ReplayResult 是重放结果。Rebuilt 为真表示按单据当前数据新建了一条 outbox。
type ReplayResult struct {
	EventID string
	Rebuilt bool
}

// ReplayEvent 把 SENT 或 DEAD 的 outbox 改回 PENDING，或在行已被删掉时按单据重建。
// 不调用 Kafka、HTTP 或 RPC，也不直接记账。投递仍由 outbox.Dispatcher。
func (a *App) ReplayEvent(ctx context.Context, in *ReplayInput) (ReplayResult, error) {
	if !replayKeysOK(in) {
		return ReplayResult{}, errno.ErrInvalidParam
	}
	if a == nil || a.payments == nil {
		return ReplayResult{}, errno.ErrInternal
	}
	return a.payments.ReplayEvent(ctx, in)
}

// ReplayEvent 见 App.ReplayEvent。payments 为 nil、没有库或没有时钟时返回 10003。
func (p *Payments) ReplayEvent(ctx context.Context, in *ReplayInput) (ReplayResult, error) {
	if !replayKeysOK(in) {
		return ReplayResult{}, errno.ErrInvalidParam
	}
	if p == nil || p.clk == nil {
		return ReplayResult{}, errno.ErrInternal
	}
	db, ok := p.replayDB()
	if !ok {
		return ReplayResult{}, errno.ErrInternal
	}
	now := replayNow(p.clk)
	if in.EventID != "" {
		return resetReplay(ctx, db, in.EventID, now, true)
	}
	return p.replayByBiz(ctx, db, in.BizNo, in.EventType, now)
}

func replayKeysOK(in *ReplayInput) bool {
	if in == nil {
		return false
	}
	hasEvent := in.EventID != ""
	hasBiz := in.BizNo != ""
	hasType := in.EventType != ""
	if hasEvent {
		return !hasBiz && !hasType
	}
	return hasBiz && hasType
}

func replayNow(clk clock.Clock) time.Time {
	return clk.Now().Truncate(time.Millisecond).In(replayShanghai)
}

func (p *Payments) replayDB() (*sqlx.DB, bool) {
	if p == nil {
		return nil, false
	}
	if db, ok := p.paymentDB(); ok {
		return db, true
	}
	store, ok := p.transfers.(sqlTransferStore)
	if ok && store.db != nil {
		return store.db, true
	}
	return nil, false
}

type replayQuery interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type replayHit struct {
	eventID   string
	bizNo     string
	eventType string
	status    string
}

const replayResetSQL = `
UPDATE outbox
SET status = 'PENDING', retry_count = 0, next_retry_at = ?, updated_at = ?
WHERE event_id = ? AND status IN ('SENT', 'DEAD')`

const replayByEventSQL = `
SELECT event_id, biz_no, event_type, status
FROM outbox
WHERE event_id = ?`

const replayByBizSQL = `
SELECT event_id, biz_no, event_type, status
FROM outbox USE INDEX (idx_biz_no_event_type)
WHERE biz_no = ? AND event_type = ?
ORDER BY id DESC
LIMIT 1`

// updated_at 必须写进 SET。该列 ON UPDATE CURRENT_TIMESTAMP，漏写会变成数据库时钟。
// 不改 sent_at、last_error。影响 0 行再读：没有行是 10002，已是 PENDING 则不改行。
func resetReplay(ctx context.Context, q replayQuery, eventID string, now time.Time, again bool) (ReplayResult, error) {
	res, err := q.ExecContext(ctx, replayResetSQL, now, now, eventID)
	if err != nil {
		return ReplayResult{}, callErr(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return ReplayResult{}, callErr(err)
	}
	if n == 1 {
		return ReplayResult{EventID: eventID, Rebuilt: false}, nil
	}
	if n != 0 {
		return ReplayResult{}, errno.ErrInternal
	}
	row, ok, err := loadReplayByEvent(ctx, q, eventID)
	if err != nil {
		return ReplayResult{}, callErr(err)
	}
	if !ok {
		return ReplayResult{}, errno.ErrNotFound
	}
	if row.status == "PENDING" {
		return ReplayResult{EventID: row.eventID, Rebuilt: false}, nil
	}
	if again && (row.status == "SENT" || row.status == "DEAD") {
		return resetReplay(ctx, q, eventID, now, false)
	}
	return ReplayResult{}, errno.ErrInternal
}

func loadReplayByEvent(ctx context.Context, q replayQuery, eventID string) (replayHit, bool, error) {
	row, err := scanReplayHit(q.QueryRowContext(ctx, replayByEventSQL, eventID))
	if errors.Is(err, sql.ErrNoRows) {
		return replayHit{}, false, nil
	}
	if err != nil {
		return replayHit{}, false, err
	}
	if row.eventID != eventID {
		return replayHit{}, false, nil
	}
	return row, true, nil
}

func loadReplayByBiz(ctx context.Context, q replayQuery, bizNo, eventType string) (replayHit, bool, error) {
	row, err := scanReplayHit(q.QueryRowContext(ctx, replayByBizSQL, bizNo, eventType))
	if errors.Is(err, sql.ErrNoRows) {
		return replayHit{}, false, nil
	}
	if err != nil {
		return replayHit{}, false, err
	}
	if row.bizNo != bizNo || row.eventType != eventType {
		return replayHit{}, false, nil
	}
	return row, true, nil
}

func scanReplayHit(s rowScanner) (replayHit, error) {
	var row replayHit
	err := s.Scan(&row.eventID, &row.bizNo, &row.eventType, &row.status)
	if err != nil {
		return replayHit{}, err
	}
	return row, nil
}

func (p *Payments) replayByBiz(ctx context.Context, db *sqlx.DB, bizNo, eventType string, now time.Time) (ReplayResult, error) {
	row, ok, err := loadReplayByBiz(ctx, db, bizNo, eventType)
	if err != nil {
		return ReplayResult{}, callErr(err)
	}
	if ok {
		return finishReplayHit(ctx, db, row, now)
	}
	if testReplayAfterMiss != nil {
		testReplayAfterMiss()
	}
	if !knownReplayType(eventType) {
		row, ok, err = loadReplayByBiz(ctx, db, bizNo, eventType)
		if err != nil {
			return ReplayResult{}, callErr(err)
		}
		if ok {
			return finishReplayHit(ctx, db, row, now)
		}
		return ReplayResult{}, errno.ErrInvalidParam
	}
	return p.rebuildReplay(ctx, db, bizNo, eventType, now)
}

func finishReplayHit(ctx context.Context, q replayQuery, row replayHit, now time.Time) (ReplayResult, error) {
	if row.status == "PENDING" {
		return ReplayResult{EventID: row.eventID, Rebuilt: false}, nil
	}
	if row.status != "SENT" && row.status != "DEAD" {
		return ReplayResult{}, errno.ErrInternal
	}
	return resetReplay(ctx, q, row.eventID, now, true)
}

func knownReplayType(eventType string) bool {
	switch eventType {
	case outbox.EventPaymentSucceeded, outbox.EventRefundSucceeded, outbox.EventRefundFailed,
		outbox.EventTransferSucceeded, outbox.EventTransferFailed:
		return true
	default:
		return false
	}
}

// rebuildReplay 只在按业务单号且 outbox 没有行时调用。
// 同一事务里先锁单据，再查一次 outbox。等锁期间已经插入的行直接返回，不再插。
func (p *Payments) rebuildReplay(ctx context.Context, db *sqlx.DB, bizNo, eventType string, now time.Time) (ReplayResult, error) {
	var result ReplayResult
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		evt, lockErr := p.lockReplayOrder(ctx, tx, bizNo, eventType)
		row, ok, err := loadReplayByBiz(ctx, tx, bizNo, eventType)
		if err != nil {
			return err
		}
		if ok {
			got, ferr := finishReplayHit(ctx, tx, row, now)
			if ferr != nil {
				return ferr
			}
			result = got
			return nil
		}
		if lockErr != nil {
			return lockErr
		}
		gen, isGen := p.ids.(*idgen.Generator)
		if !isGen || gen == nil {
			return errno.ErrInternal
		}
		if err = outbox.Add(ctx, tx, evt, gen, p.clk); err != nil {
			if errors.Is(err, idgen.ErrClockRollback) {
				return errno.ErrUnavailable
			}
			if errors.Is(err, outbox.ErrInvalidEvent) {
				return errno.ErrInternal
			}
			return err
		}
		row, ok, err = loadReplayByBiz(ctx, tx, bizNo, eventType)
		if err != nil {
			return err
		}
		if !ok || !strings.HasPrefix(row.eventID, "EV") || row.status != "PENDING" {
			return errno.ErrInternal
		}
		result = ReplayResult{EventID: row.eventID, Rebuilt: true}
		return nil
	})
	if err != nil {
		return ReplayResult{}, callErr(err)
	}
	return result, nil
}

const replayLockPaymentSQL = `
SELECT pay_no, merchant_id, out_trade_no, amount, currency, subject, status,
       channel, fee_amount, refunded_amount, notify_url, expire_at,
       paid_at, closed_at, fail_code, fail_msg, version, created_at, updated_at
FROM payment_order
WHERE pay_no = ?
FOR UPDATE`

const replayLockRefundSQL = `
SELECT refund_no, merchant_id, out_refund_no, pay_no, amount, currency, reason,
       status, channel, fail_code, fail_msg, succeeded_at, version, created_at, updated_at
FROM refund_order
WHERE refund_no = ?
FOR UPDATE`

const replayLockTransferSQL = `
SELECT transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
       mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
       version, created_at, updated_at
FROM transfer_order
WHERE transfer_no = ?
FOR UPDATE`

func (p *Payments) lockReplayOrder(ctx context.Context, tx *sqlx.Tx, bizNo, eventType string) (outbox.Event, error) {
	switch eventType {
	case outbox.EventPaymentSucceeded:
		order, err := scanPayment(tx.QueryRowContext(ctx, replayLockPaymentSQL, bizNo))
		if errors.Is(err, sql.ErrNoRows) {
			return outbox.Event{}, errno.ErrNotFound
		}
		if err != nil {
			return outbox.Event{}, err
		}
		if order.PayNo != bizNo {
			return outbox.Event{}, errno.ErrNotFound
		}
		return replayPaymentEvent(order)
	case outbox.EventRefundSucceeded, outbox.EventRefundFailed:
		order, err := scanRefund(tx.QueryRowContext(ctx, replayLockRefundSQL, bizNo))
		if errors.Is(err, sql.ErrNoRows) {
			return outbox.Event{}, errno.ErrNotFound
		}
		if err != nil {
			return outbox.Event{}, err
		}
		if order.RefundNo != bizNo {
			return outbox.Event{}, errno.ErrNotFound
		}
		return replayRefundEvent(order, eventType)
	case outbox.EventTransferSucceeded, outbox.EventTransferFailed:
		order, err := scanTransfer(tx.QueryRowContext(ctx, replayLockTransferSQL, bizNo))
		if errors.Is(err, sql.ErrNoRows) {
			return outbox.Event{}, errno.ErrNotFound
		}
		if err != nil {
			return outbox.Event{}, err
		}
		if order.TransferNo != bizNo {
			return outbox.Event{}, errno.ErrNotFound
		}
		return replayTransferEvent(order, eventType)
	default:
		return outbox.Event{}, errno.ErrInvalidParam
	}
}

func replayPaymentEvent(order PaymentOrder) (outbox.Event, error) {
	if order.Status != payStatusSuccess {
		return outbox.Event{}, errno.ErrOrderStatusConflict
	}
	return outbox.Event{
		Type:       outbox.EventPaymentSucceeded,
		PayNo:      order.PayNo,
		MerchantID: order.MerchantID,
		OutTradeNo: order.OutTradeNo,
		Channel:    order.Channel,
		Amount:     order.Amount,
		FeeAmount:  order.FeeAmount,
		PaidAt:     order.PaidAt,
	}, nil
}

func replayRefundEvent(order RefundOrder, eventType string) (outbox.Event, error) {
	switch eventType {
	case outbox.EventRefundSucceeded:
		if order.Status != payStatusSuccess {
			return outbox.Event{}, errno.ErrOrderStatusConflict
		}
		return outbox.Event{
			Type:        outbox.EventRefundSucceeded,
			RefundNo:    order.RefundNo,
			PayNo:       order.PayNo,
			MerchantID:  order.MerchantID,
			OutRefundNo: order.OutRefundNo,
			Channel:     order.Channel,
			Amount:      order.Amount,
			SucceededAt: order.SucceededAt,
		}, nil
	case outbox.EventRefundFailed:
		if order.Status != refundStatusFailed {
			return outbox.Event{}, errno.ErrOrderStatusConflict
		}
		return outbox.Event{
			Type:        outbox.EventRefundFailed,
			RefundNo:    order.RefundNo,
			PayNo:       order.PayNo,
			MerchantID:  order.MerchantID,
			OutRefundNo: order.OutRefundNo,
			Amount:      order.Amount,
			FailCode:    order.FailCode,
		}, nil
	default:
		return outbox.Event{}, errno.ErrInvalidParam
	}
}

func replayTransferEvent(order TransferOrder, eventType string) (outbox.Event, error) {
	evt := outbox.Event{
		TransferNo:      order.TransferNo,
		OutTransferNo:   order.OutTransferNo,
		MerchantID:      order.MerchantID,
		PayeeMerchantID: order.PayeeMerchantID,
		Amount:          order.Amount,
		Mode:            order.Mode,
		FinishedAt:      order.FinishedAt,
	}
	switch eventType {
	case outbox.EventTransferSucceeded:
		if order.Status != transferStatusSuccess {
			return outbox.Event{}, errno.ErrOrderStatusConflict
		}
		evt.Type = outbox.EventTransferSucceeded
		evt.Status = transferStatusSuccess
	case outbox.EventTransferFailed:
		if order.Status != transferStatusFailed {
			return outbox.Event{}, errno.ErrOrderStatusConflict
		}
		evt.Type = outbox.EventTransferFailed
		evt.Status = transferStatusFailed
		evt.FailCode = order.FailCode
	default:
		return outbox.Event{}, errno.ErrInvalidParam
	}
	return evt, nil
}
