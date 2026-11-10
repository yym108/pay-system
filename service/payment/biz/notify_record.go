package biz

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

const (
	notifyNoPrefix = "NT"

	notifyEventPaymentSucceeded  = "payment.succeeded"
	notifyEventRefundSucceeded   = "refund.succeeded"
	notifyEventRefundFailed      = "refund.failed"
	notifyEventTransferSucceeded = "transfer.succeeded"
	notifyEventTransferFailed    = "transfer.failed"

	notifyBizPay      = "PAY"
	notifyBizRefund   = "REFUND"
	notifyBizTransfer = "TRANSFER"

	notifyStatusPending = "PENDING"
	notifyStatusSuccess = "SUCCESS"
	notifyStatusFailed  = "FAILED"

	notifyMaxIDRunes  = 32
	notifyMaxOutRunes = 64
	notifyMaxFail     = 32
	notifyMaxTime     = 64
)

// notifyShanghai 是固定东八区。不加载 zoneinfo，运行镜像没有 tzdata 时也能用。
var notifyShanghai = time.FixedZone("Asia/Shanghai", 8*60*60)

// NotifyRecordInput 是一条已经拆开的通知事件。
// 金额单位为分。时间字符串原样写入 payload，不再格式化。
type NotifyRecordInput struct {
	EventID         string
	EventType       string
	MerchantID      string
	PayNo           string
	OutTradeNo      string
	RefundNo        string
	OutRefundNo     string
	TransferNo      string
	OutTransferNo   string
	PayeeMerchantID string
	Amount          money.Amount
	FeeAmount       money.Amount
	FailCode        string
	PaidAt          string
	SucceededAt     string
	FinishedAt      string
}

// NotifyRecordResult 是本次写入或已经存在的通知单。
type NotifyRecordResult struct {
	NotifyNo string
}

type notifyBody struct {
	NotifyID   string `json:"notify_id"`
	EventType  string `json:"event_type"`
	MerchantID string `json:"merchant_id"`
	Data       any    `json:"data"`
}

type payNotifyData struct {
	PayNo      string       `json:"pay_no"`
	OutTradeNo string       `json:"out_trade_no"`
	Amount     money.Amount `json:"amount"`
	FeeAmount  money.Amount `json:"fee_amount"`
	Status     string       `json:"status"`
	PaidAt     string       `json:"paid_at"`
}

type refundNotifyData struct {
	RefundNo    string       `json:"refund_no"`
	OutRefundNo string       `json:"out_refund_no"`
	PayNo       string       `json:"pay_no"`
	Amount      money.Amount `json:"amount"`
	Status      string       `json:"status"`
	SucceededAt string       `json:"succeeded_at,omitempty"`
	FailCode    string       `json:"fail_code,omitempty"`
}

type transferNotifyData struct {
	TransferNo      string       `json:"transfer_no"`
	OutTransferNo   string       `json:"out_transfer_no"`
	PayeeMerchantID string       `json:"payee_merchant_id"`
	Amount          money.Amount `json:"amount"`
	Status          string       `json:"status"`
	FailCode        string       `json:"fail_code,omitempty"`
	FinishedAt      string       `json:"finished_at"`
}

type notifyRow struct {
	merchantID string
	bizType    string
	bizNo      string
	notifyURL  string
	data       any
}

// RecordNotify 为一件支付、退款或转账事件插入一条 PENDING 通知。
// 同一 event_id 再来一次视为已处理。不发送 HTTP。
func (p *Payments) RecordNotify(ctx context.Context, in NotifyRecordInput) (NotifyRecordResult, error) {
	if p == nil || ctx == nil {
		return NotifyRecordResult{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return NotifyRecordResult{}, err
	}
	if err := validateNotifyInput(in); err != nil {
		return NotifyRecordResult{}, err
	}
	if p.clk == nil {
		return NotifyRecordResult{}, errno.ErrInternal
	}
	row, err := p.resolveNotify(ctx, in)
	if err != nil {
		return NotifyRecordResult{}, err
	}
	if err = notifyURLLen(row.notifyURL); err != nil {
		return NotifyRecordResult{}, err
	}
	if p.ids == nil {
		return NotifyRecordResult{}, errno.ErrInternal
	}
	notifyNo, err := p.ids.NewNo(ctx, notifyNoPrefix)
	if err != nil {
		if errors.Is(err, idgen.ErrClockRollback) {
			return NotifyRecordResult{}, errno.ErrUnavailable
		}
		return NotifyRecordResult{}, callErr(err)
	}
	db, ok := p.paymentDB()
	if !ok {
		return NotifyRecordResult{}, errno.ErrInternal
	}
	body, err := json.Marshal(notifyBody{
		NotifyID:   notifyNo,
		EventType:  in.EventType,
		MerchantID: row.merchantID,
		Data:       row.data,
	})
	if err != nil {
		return NotifyRecordResult{}, callErr(err)
	}
	now := notifyNow(p.clk)
	_, err = db.ExecContext(ctx, `
INSERT INTO merchant_notify (
  notify_no, event_id, merchant_id, biz_type, biz_no, notify_url, payload,
  status, attempt_count, next_attempt_at, last_http_status, last_error, succeeded_at,
  created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, NULL, NULL, ?, ?)`,
		notifyNo, in.EventID, row.merchantID, row.bizType, row.bizNo, row.notifyURL, body,
		notifyStatusPending, now, now, now,
	)
	if err == nil {
		return NotifyRecordResult{NotifyNo: notifyNo}, nil
	}
	if !mysqlDup(err) {
		return NotifyRecordResult{}, callErr(err)
	}
	// 1062 可能是 event_id，也可能是 notify_no。只有再读到同一 event_id 才算已处理。
	return notifyOnDup(ctx, db, in.EventID)
}

func (p *Payments) resolveNotify(ctx context.Context, in NotifyRecordInput) (notifyRow, error) {
	switch in.EventType {
	case notifyEventPaymentSucceeded:
		return p.resolvePaymentNotify(ctx, in)
	case notifyEventRefundSucceeded, notifyEventRefundFailed:
		return p.resolveRefundNotify(ctx, in)
	case notifyEventTransferSucceeded, notifyEventTransferFailed:
		return p.resolveTransferNotify(ctx, in)
	default:
		return notifyRow{}, errno.ErrInvalidParam
	}
}

func (p *Payments) resolvePaymentNotify(ctx context.Context, in NotifyRecordInput) (notifyRow, error) {
	if p.orders == nil {
		return notifyRow{}, errno.ErrInternal
	}
	order, ok, err := p.orders.paymentByPayNo(ctx, in.PayNo)
	if err != nil {
		return notifyRow{}, callErr(err)
	}
	if !ok || !sameBytes(order.PayNo, in.PayNo) || !sameBytes(order.MerchantID, in.MerchantID) {
		return notifyRow{}, errno.ErrNotFound
	}
	url, err := p.lookupNotifyURL(ctx, order.NotifyURL, order.MerchantID)
	if err != nil {
		return notifyRow{}, err
	}
	return notifyRow{
		merchantID: order.MerchantID,
		bizType:    notifyBizPay,
		bizNo:      order.PayNo,
		notifyURL:  url,
		data: payNotifyData{
			PayNo:      in.PayNo,
			OutTradeNo: in.OutTradeNo,
			Amount:     in.Amount,
			FeeAmount:  in.FeeAmount,
			Status:     notifyStatusSuccess,
			PaidAt:     in.PaidAt,
		},
	}, nil
}

func (p *Payments) resolveRefundNotify(ctx context.Context, in NotifyRecordInput) (notifyRow, error) {
	if p.orders == nil {
		return notifyRow{}, errno.ErrInternal
	}
	pay, ok, err := p.orders.paymentByPayNo(ctx, in.PayNo)
	if err != nil {
		return notifyRow{}, callErr(err)
	}
	if !ok || !sameBytes(pay.PayNo, in.PayNo) || !sameBytes(pay.MerchantID, in.MerchantID) {
		return notifyRow{}, errno.ErrNotFound
	}
	refund, ok, err := p.orders.refundByNo(ctx, in.RefundNo)
	if err != nil {
		return notifyRow{}, callErr(err)
	}
	if !ok || !sameBytes(refund.RefundNo, in.RefundNo) || !sameBytes(refund.PayNo, in.PayNo) || !sameBytes(refund.MerchantID, in.MerchantID) {
		return notifyRow{}, errno.ErrNotFound
	}
	url, err := p.lookupNotifyURL(ctx, pay.NotifyURL, pay.MerchantID)
	if err != nil {
		return notifyRow{}, err
	}
	data := refundNotifyData{
		RefundNo:    in.RefundNo,
		OutRefundNo: in.OutRefundNo,
		PayNo:       in.PayNo,
		Amount:      in.Amount,
		Status:      notifyStatusSuccess,
		SucceededAt: in.SucceededAt,
	}
	if in.EventType == notifyEventRefundFailed {
		data.Status = notifyStatusFailed
		data.FailCode = in.FailCode
		data.SucceededAt = ""
	}
	return notifyRow{
		merchantID: pay.MerchantID,
		bizType:    notifyBizRefund,
		bizNo:      refund.RefundNo,
		notifyURL:  url,
		data:       data,
	}, nil
}

func (p *Payments) resolveTransferNotify(ctx context.Context, in NotifyRecordInput) (notifyRow, error) {
	if p.transfers == nil {
		return notifyRow{}, errno.ErrInternal
	}
	order, ok, err := p.transfers.transferByNo(ctx, in.TransferNo)
	if err != nil {
		return notifyRow{}, callErr(err)
	}
	if !ok || !sameBytes(order.TransferNo, in.TransferNo) || !sameBytes(order.MerchantID, in.MerchantID) || !sameBytes(order.PayeeMerchantID, in.PayeeMerchantID) {
		return notifyRow{}, errno.ErrNotFound
	}
	url, err := p.lookupNotifyURL(ctx, order.NotifyURL, order.MerchantID)
	if err != nil {
		return notifyRow{}, err
	}
	data := transferNotifyData{
		TransferNo:      in.TransferNo,
		OutTransferNo:   in.OutTransferNo,
		PayeeMerchantID: in.PayeeMerchantID,
		Amount:          in.Amount,
		Status:          notifyStatusSuccess,
		FinishedAt:      in.FinishedAt,
	}
	if in.EventType == notifyEventTransferFailed {
		data.Status = notifyStatusFailed
		data.FailCode = in.FailCode
	}
	return notifyRow{
		merchantID: order.MerchantID,
		bizType:    notifyBizTransfer,
		bizNo:      order.TransferNo,
		notifyURL:  url,
		data:       data,
	}, nil
}

// lookupNotifyURL 只在订单地址是空串时读商户默认地址。不 TrimSpace。
// 商户不存在时原样返回 20004。默认地址也是空串时仍用空串。
func (p *Payments) lookupNotifyURL(ctx context.Context, orderURL, merchantID string) (string, error) {
	if orderURL != "" {
		return orderURL, nil
	}
	if p.merchants == nil {
		return "", errno.ErrInternal
	}
	cfg, err := p.merchants.MerchantConfig(ctx, merchantID)
	if err != nil {
		return "", err
	}
	return cfg.DefaultNotifyURL, nil
}

func notifyOnDup(ctx context.Context, db *sqlx.DB, eventID string) (NotifyRecordResult, error) {
	var gotEvent, gotNo string
	err := db.QueryRowContext(ctx, `
SELECT event_id, notify_no FROM merchant_notify WHERE event_id = ?`, eventID).Scan(&gotEvent, &gotNo)
	if errors.Is(err, sql.ErrNoRows) {
		return NotifyRecordResult{}, errno.ErrInternal
	}
	if err != nil {
		return NotifyRecordResult{}, callErr(err)
	}
	return notifyFromDupRow(eventID, gotEvent, gotNo)
}

// notifyFromDupRow 只在再读到的 event_id 逐字节相同才当作已处理。
// 排序规则放宽时，同一键可能读出大小写不同的值，那不是这一次事件。
func notifyFromDupRow(want, gotEvent, gotNo string) (NotifyRecordResult, error) {
	if !sameBytes(want, gotEvent) {
		return NotifyRecordResult{}, errno.ErrInternal
	}
	return NotifyRecordResult{NotifyNo: gotNo}, nil
}

func notifyNow(clk clock.Clock) time.Time {
	return clk.Now().Truncate(time.Millisecond).In(notifyShanghai)
}

func notifyURLLen(raw string) error {
	if raw == "" {
		return nil
	}
	if !utf8.ValidString(raw) || utf8.RuneCountInString(raw) > maxNotifyURLRunes {
		return errno.ErrInvalidParam
	}
	return nil
}

func validateNotifyInput(in NotifyRecordInput) error {
	if !fitNotify(in.EventID, notifyMaxIDRunes) || !fitNotify(in.MerchantID, notifyMaxIDRunes) {
		return errno.ErrInvalidParam
	}
	switch in.EventType {
	case notifyEventPaymentSucceeded:
		if !fitNotify(in.PayNo, notifyMaxIDRunes) || !fitNotify(in.OutTradeNo, notifyMaxOutRunes) || !fitNotify(in.PaidAt, notifyMaxTime) {
			return errno.ErrInvalidParam
		}
	case notifyEventRefundSucceeded:
		if !fitNotify(in.RefundNo, notifyMaxIDRunes) || !fitNotify(in.PayNo, notifyMaxIDRunes) || !fitNotify(in.OutRefundNo, notifyMaxOutRunes) || !fitNotify(in.SucceededAt, notifyMaxTime) {
			return errno.ErrInvalidParam
		}
	case notifyEventRefundFailed:
		if !fitNotify(in.RefundNo, notifyMaxIDRunes) || !fitNotify(in.PayNo, notifyMaxIDRunes) || !fitNotify(in.OutRefundNo, notifyMaxOutRunes) || !fitNotify(in.FailCode, notifyMaxFail) {
			return errno.ErrInvalidParam
		}
	case notifyEventTransferSucceeded:
		if !fitNotify(in.TransferNo, notifyMaxIDRunes) || !fitNotify(in.OutTransferNo, notifyMaxOutRunes) || !fitNotify(in.PayeeMerchantID, notifyMaxIDRunes) || !fitNotify(in.FinishedAt, notifyMaxTime) {
			return errno.ErrInvalidParam
		}
	case notifyEventTransferFailed:
		if !fitNotify(in.TransferNo, notifyMaxIDRunes) || !fitNotify(in.OutTransferNo, notifyMaxOutRunes) || !fitNotify(in.PayeeMerchantID, notifyMaxIDRunes) || !fitNotify(in.FailCode, notifyMaxFail) || !fitNotify(in.FinishedAt, notifyMaxTime) {
			return errno.ErrInvalidParam
		}
	default:
		return errno.ErrInvalidParam
	}
	return nil
}

func fitNotify(value string, maxRunes int) bool {
	n := utf8.RuneCountInString(value)
	return n > 0 && n <= maxRunes && utf8.ValidString(value)
}

func sameBytes(a, b string) bool {
	return bytes.Equal([]byte(a), []byte(b))
}

func mysqlDup(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}
