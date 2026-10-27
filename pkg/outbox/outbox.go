// Package outbox 在调用方事务里写入本地消息表。
//
// Add 只负责插入一行。投递、领取和 Kafka 发送不属于本包这一层。
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

const (
	// EventPaymentSucceeded 是支付成功事件。
	EventPaymentSucceeded = "payment.succeeded"
	// EventRefundSucceeded 是退款成功事件。
	EventRefundSucceeded = "refund.succeeded"
	// EventRefundFailed 是退款失败事件。
	EventRefundFailed = "refund.failed"
	// EventTransferSucceeded 是转账成功事件。
	EventTransferSucceeded = "transfer.succeeded"
	// EventTransferFailed 是转账失败事件。
	EventTransferFailed = "transfer.failed"

	eventIDPrefix = "EV"

	topicPaymentSucceeded  = "pay.payment.succeeded"
	topicRefundSucceeded   = "pay.refund.succeeded"
	topicRefundFailed      = "pay.refund.failed"
	topicTransferSucceeded = "pay.transfer.succeeded"
	topicTransferFailed    = "pay.transfer.failed"

	statusPending = "PENDING"
	statusSuccess = "SUCCESS"
	statusFailed  = "FAILED"

	envelopeVersion = 1
	rfc3339Milli    = "2006-01-02T15:04:05.000Z07:00"
)

// 运行镜像没有 tzdata。中国自 1991 年起没有夏令时，固定 +8 与当前民用时间一致。
var shanghai = time.FixedZone("Asia/Shanghai", 8*60*60)

var (
	// ErrNilTx 表示没有传入事务。Add 不自己 BEGIN。
	ErrNilTx = errors.New("outbox: nil transaction")
	// ErrInvalidEvent 表示事件类型未知，或缺少该类型必填字段。不插入行。
	ErrInvalidEvent = errors.New("outbox: invalid event")

	errNilDependency = errors.New("outbox: nil dependency")
)

// Event 是一次 Add 要写入的业务事件。
// 金额用整数分。PaidAt、SucceededAt、FinishedAt 由调用方传入，Add 不改成自己的时钟。
// 转账 Status 必须与类型一致：成功为 SUCCESS，失败为 FAILED。
type Event struct {
	Type            string
	PayNo           string
	RefundNo        string
	TransferNo      string
	MerchantID      string
	PayeeMerchantID string
	OutTradeNo      string
	OutRefundNo     string
	OutTransferNo   string
	Channel         string
	Amount          money.Amount
	FeeAmount       money.Amount
	Mode            string
	Status          string
	FailCode        string
	PaidAt          time.Time
	SucceededAt     time.Time
	FinishedAt      time.Time
}

type prepared struct {
	eventID   string
	eventType string
	topic     string
	msgKey    string
	bizNo     string
	payload   []byte
	at        time.Time
}

type routed struct {
	topic  string
	msgKey string
	bizNo  string
	data   any
}

type envelope struct {
	EventID      string          `json:"event_id"`
	EventType    string          `json:"event_type"`
	Version      int             `json:"version"`
	OccurredAt   string          `json:"occurred_at"`
	TraceContext json.RawMessage `json:"trace_context"`
	Data         any             `json:"data"`
}

type paymentSucceededData struct {
	PayNo      string `json:"pay_no"`
	MerchantID string `json:"merchant_id"`
	OutTradeNo string `json:"out_trade_no"`
	Channel    string `json:"channel"`
	Amount     int64  `json:"amount"`
	FeeAmount  int64  `json:"fee_amount"`
	PaidAt     string `json:"paid_at"`
}

type refundSucceededData struct {
	RefundNo    string `json:"refund_no"`
	PayNo       string `json:"pay_no"`
	MerchantID  string `json:"merchant_id"`
	OutRefundNo string `json:"out_refund_no"`
	Channel     string `json:"channel"`
	Amount      int64  `json:"amount"`
	SucceededAt string `json:"succeeded_at"`
}

type refundFailedData struct {
	RefundNo    string `json:"refund_no"`
	PayNo       string `json:"pay_no"`
	MerchantID  string `json:"merchant_id"`
	OutRefundNo string `json:"out_refund_no"`
	Amount      int64  `json:"amount"`
	FailCode    string `json:"fail_code"`
}

type transferSucceededData struct {
	TransferNo      string `json:"transfer_no"`
	OutTransferNo   string `json:"out_transfer_no"`
	MerchantID      string `json:"merchant_id"`
	PayeeMerchantID string `json:"payee_merchant_id"`
	Amount          int64  `json:"amount"`
	Mode            string `json:"mode"`
	Status          string `json:"status"`
	FinishedAt      string `json:"finished_at"`
}

type transferFailedData struct {
	TransferNo      string `json:"transfer_no"`
	OutTransferNo   string `json:"out_transfer_no"`
	MerchantID      string `json:"merchant_id"`
	PayeeMerchantID string `json:"payee_merchant_id"`
	Amount          int64  `json:"amount"`
	Mode            string `json:"mode"`
	Status          string `json:"status"`
	FailCode        string `json:"fail_code"`
	FinishedAt      string `json:"finished_at"`
}

// Add 在 tx 中插入一行 outbox。调用方必须已经 BEGIN；本函数不提交。
// 事件不合法时先返回，不发号。发号失败（含 idgen.ErrClockRollback）原样返回，不插入，也不换成新的错误码。
// tx 为 nil 时在发号之后返回 ErrNilTx，不执行 INSERT。
func Add(ctx context.Context, tx *sqlx.Tx, evt Event, ids *idgen.Generator, clk clock.Clock) error {
	row, err := prepare(ctx, evt, ids, clk)
	if err != nil {
		return err
	}
	if tx == nil {
		return ErrNilTx
	}
	return insert(ctx, tx, row)
}

func prepare(ctx context.Context, evt Event, ids *idgen.Generator, clk clock.Clock) (prepared, error) {
	routed, err := route(evt)
	if err != nil {
		return prepared{}, err
	}
	if ctx == nil || ids == nil || clk == nil {
		return prepared{}, errNilDependency
	}
	eventID, err := ids.NewNo(ctx, eventIDPrefix)
	if err != nil {
		return prepared{}, err
	}
	// 提交前只读这一次。不能改成提交成功后再读，否则回滚与提交不再是同一次时钟。
	at := clk.Now().Truncate(time.Millisecond).In(shanghai)
	payload, err := marshalEnvelope(eventID, evt.Type, at, ctx, routed.data)
	if err != nil {
		return prepared{}, err
	}
	return prepared{
		eventID:   eventID,
		eventType: evt.Type,
		topic:     routed.topic,
		msgKey:    routed.msgKey,
		bizNo:     routed.bizNo,
		payload:   payload,
		at:        at,
	}, nil
}

func route(evt Event) (routed, error) {
	switch evt.Type {
	case EventPaymentSucceeded:
		return routePayment(evt)
	case EventRefundSucceeded:
		return routeRefundSucceeded(evt)
	case EventRefundFailed:
		return routeRefundFailed(evt)
	case EventTransferSucceeded:
		return routeTransferSucceeded(evt)
	case EventTransferFailed:
		return routeTransferFailed(evt)
	default:
		return routed{}, invalid("event_type")
	}
}

func routePayment(evt Event) (routed, error) {
	if err := nonEmpty(evt.PayNo, "pay_no"); err != nil {
		return routed{}, err
	}
	if err := nonEmpty(evt.MerchantID, "merchant_id"); err != nil {
		return routed{}, err
	}
	if err := nonEmpty(evt.OutTradeNo, "out_trade_no"); err != nil {
		return routed{}, err
	}
	if err := nonEmpty(evt.Channel, "channel"); err != nil {
		return routed{}, err
	}
	if err := positiveAmount(evt.Amount); err != nil {
		return routed{}, err
	}
	if evt.FeeAmount < 0 {
		return routed{}, invalid("fee_amount")
	}
	if err := nonZero(evt.PaidAt, "paid_at"); err != nil {
		return routed{}, err
	}
	return routed{
		topic:  topicPaymentSucceeded,
		msgKey: evt.PayNo,
		bizNo:  evt.PayNo,
		data: paymentSucceededData{
			PayNo: evt.PayNo, MerchantID: evt.MerchantID, OutTradeNo: evt.OutTradeNo,
			Channel: evt.Channel, Amount: int64(evt.Amount), FeeAmount: int64(evt.FeeAmount),
			PaidAt: formatTime(evt.PaidAt),
		},
	}, nil
}

func routeRefundSucceeded(evt Event) (routed, error) {
	if err := refundIdentity(evt); err != nil {
		return routed{}, err
	}
	if err := nonEmpty(evt.Channel, "channel"); err != nil {
		return routed{}, err
	}
	if err := nonZero(evt.SucceededAt, "succeeded_at"); err != nil {
		return routed{}, err
	}
	// 分区键是原支付单，退款单号只放 biz_no，供按退款单重放。
	return routed{
		topic:  topicRefundSucceeded,
		msgKey: evt.PayNo,
		bizNo:  evt.RefundNo,
		data: refundSucceededData{
			RefundNo: evt.RefundNo, PayNo: evt.PayNo, MerchantID: evt.MerchantID,
			OutRefundNo: evt.OutRefundNo, Channel: evt.Channel, Amount: int64(evt.Amount),
			SucceededAt: formatTime(evt.SucceededAt),
		},
	}, nil
}

func routeRefundFailed(evt Event) (routed, error) {
	if err := refundIdentity(evt); err != nil {
		return routed{}, err
	}
	if err := nonEmpty(evt.FailCode, "fail_code"); err != nil {
		return routed{}, err
	}
	return routed{
		topic:  topicRefundFailed,
		msgKey: evt.PayNo,
		bizNo:  evt.RefundNo,
		data: refundFailedData{
			RefundNo: evt.RefundNo, PayNo: evt.PayNo, MerchantID: evt.MerchantID,
			OutRefundNo: evt.OutRefundNo, Amount: int64(evt.Amount), FailCode: evt.FailCode,
		},
	}, nil
}

func refundIdentity(evt Event) error {
	if err := nonEmpty(evt.RefundNo, "refund_no"); err != nil {
		return err
	}
	if err := nonEmpty(evt.PayNo, "pay_no"); err != nil {
		return err
	}
	if err := nonEmpty(evt.MerchantID, "merchant_id"); err != nil {
		return err
	}
	if err := nonEmpty(evt.OutRefundNo, "out_refund_no"); err != nil {
		return err
	}
	return positiveAmount(evt.Amount)
}

func routeTransferSucceeded(evt Event) (routed, error) {
	if err := transferIdentity(evt); err != nil {
		return routed{}, err
	}
	if evt.Status != statusSuccess {
		return routed{}, invalid("status")
	}
	return routed{
		topic:  topicTransferSucceeded,
		msgKey: evt.TransferNo,
		bizNo:  evt.TransferNo,
		data: transferSucceededData{
			TransferNo: evt.TransferNo, OutTransferNo: evt.OutTransferNo,
			MerchantID: evt.MerchantID, PayeeMerchantID: evt.PayeeMerchantID,
			Amount: int64(evt.Amount), Mode: evt.Mode, Status: evt.Status,
			FinishedAt: formatTime(evt.FinishedAt),
		},
	}, nil
}

func routeTransferFailed(evt Event) (routed, error) {
	if err := transferIdentity(evt); err != nil {
		return routed{}, err
	}
	if evt.Status != statusFailed {
		return routed{}, invalid("status")
	}
	if err := nonEmpty(evt.FailCode, "fail_code"); err != nil {
		return routed{}, err
	}
	return routed{
		topic:  topicTransferFailed,
		msgKey: evt.TransferNo,
		bizNo:  evt.TransferNo,
		data: transferFailedData{
			TransferNo: evt.TransferNo, OutTransferNo: evt.OutTransferNo,
			MerchantID: evt.MerchantID, PayeeMerchantID: evt.PayeeMerchantID,
			Amount: int64(evt.Amount), Mode: evt.Mode, Status: evt.Status,
			FailCode: evt.FailCode, FinishedAt: formatTime(evt.FinishedAt),
		},
	}, nil
}

func transferIdentity(evt Event) error {
	if err := nonEmpty(evt.TransferNo, "transfer_no"); err != nil {
		return err
	}
	if err := nonEmpty(evt.OutTransferNo, "out_transfer_no"); err != nil {
		return err
	}
	if err := nonEmpty(evt.MerchantID, "merchant_id"); err != nil {
		return err
	}
	if err := nonEmpty(evt.PayeeMerchantID, "payee_merchant_id"); err != nil {
		return err
	}
	if err := positiveAmount(evt.Amount); err != nil {
		return err
	}
	if err := nonEmpty(evt.Mode, "mode"); err != nil {
		return err
	}
	return nonZero(evt.FinishedAt, "finished_at")
}

func nonEmpty(v, field string) error {
	if v == "" {
		return invalid(field)
	}
	return nil
}

func positiveAmount(v money.Amount) error {
	if v <= 0 {
		return invalid("amount")
	}
	return nil
}

func nonZero(t time.Time, field string) error {
	if t.IsZero() {
		return invalid(field)
	}
	return nil
}

func invalid(field string) error {
	return fmt.Errorf("%w: %s", ErrInvalidEvent, field)
}

func marshalEnvelope(eventID, eventType string, at time.Time, ctx context.Context, data any) ([]byte, error) {
	traceRaw, err := traceContext(ctx)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(envelope{
		EventID:      eventID,
		EventType:    eventType,
		Version:      envelopeVersion,
		OccurredAt:   formatTime(at),
		TraceContext: traceRaw,
		Data:         data,
	})
	if err != nil {
		return nil, fmt.Errorf("outbox: payload: %w", err)
	}
	return body, nil
}

func traceContext(ctx context.Context) (json.RawMessage, error) {
	if ctx == nil {
		return json.RawMessage(`{}`), nil
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() || !sc.HasSpanID() {
		return json.RawMessage(`{}`), nil
	}
	raw, err := json.Marshal(struct {
		TraceParent string `json:"traceparent"`
	}{
		TraceParent: fmt.Sprintf("00-%s-%s-%s", sc.TraceID().String(), sc.SpanID().String(), sc.TraceFlags().String()),
	})
	if err != nil {
		return nil, fmt.Errorf("outbox: trace_context: %w", err)
	}
	return raw, nil
}

func formatTime(t time.Time) string {
	return t.Truncate(time.Millisecond).In(shanghai).Format(rfc3339Milli)
}

const insertSQL = `
INSERT INTO outbox (
    event_id, event_type, topic, msg_key, biz_no, payload,
    status, retry_count, next_retry_at, last_error, sent_at,
    created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func insert(ctx context.Context, tx *sqlx.Tx, row prepared) error {
	// event_id 刚发出来。1062 说明号撞了，换一个新号会变成另一条事件，不能当成成功。
	res, err := tx.ExecContext(ctx, insertSQL,
		row.eventID, row.eventType, row.topic, row.msgKey, row.bizNo, string(row.payload),
		statusPending, 0, row.at, nil, nil, row.at, row.at,
	)
	if err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	if err := dbx.MustAffectOne(res); err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}
