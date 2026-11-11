// Package notify 在 payment 进程里生成商户通知记录。
// 只插入 merchant_notify，不发送 HTTP，不签名，不重试。
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/service/payment/biz"
)

const (
	// GroupName 是 payment 进程里的通知记录消费组。
	GroupName = "payment-notify"

	// TopicPaymentSucceeded、TopicRefundSucceeded、TopicRefundFailed、
	// TopicTransferSucceeded、TopicTransferFailed 是本消费组仅订阅的五个 topic。
	TopicPaymentSucceeded  = "pay.payment.succeeded"
	TopicRefundSucceeded   = "pay.refund.succeeded"
	TopicRefundFailed      = "pay.refund.failed"
	TopicTransferSucceeded = "pay.transfer.succeeded"
	TopicTransferFailed    = "pay.transfer.failed"

	eventPaymentSucceeded  = "payment.succeeded"
	eventRefundSucceeded   = "refund.succeeded"
	eventRefundFailed      = "refund.failed"
	eventTransferSucceeded = "transfer.succeeded"
	eventTransferFailed    = "transfer.failed"

	maxIDRunes   = 32
	maxOutRunes  = 64
	maxFailRunes = 32
	maxTimeRunes = 64
)

// Recorder 把一条事件写成通知记录。*biz.Payments 已实现。
type Recorder interface {
	RecordNotify(ctx context.Context, in biz.NotifyRecordInput) (biz.NotifyRecordResult, error)
}

// Config 装配消费组。SessionTimeout 和 Sleeper 只给测试。生产路径保持零值。
// 不提供 BeforeCommit。那个钩子只给框架测试。
type Config struct {
	Brokers        []string
	Recorder       Recorder
	SessionTimeout time.Duration
	Sleeper        mq.Sleeper
}

var errEnvelope = errors.New("notify envelope invalid")

type envelope struct {
	EventID      string          `json:"event_id"`
	EventType    string          `json:"event_type"`
	TraceContext json.RawMessage `json:"trace_context"`
	Data         json.RawMessage `json:"data"`
}

// Open 创建 payment-notify 消费组。不在这里拨号，也不开始拉取。
// 生产路径不设置 BeforeCommit。
func Open(cfg Config) (*mq.Consumer, error) {
	built, err := buildConfig(cfg)
	if err != nil {
		return nil, err
	}
	return mq.OpenConsumer(built)
}

func buildConfig(cfg Config) (mq.ConsumerConfig, error) {
	if cfg.Recorder == nil {
		return mq.ConsumerConfig{}, errors.New("no notify recorder")
	}
	return mq.ConsumerConfig{
		Brokers: cfg.Brokers,
		Group:   GroupName,
		Topics: []string{
			TopicPaymentSucceeded,
			TopicRefundSucceeded,
			TopicRefundFailed,
			TopicTransferSucceeded,
			TopicTransferFailed,
		},
		Handler: func(ctx context.Context, rec mq.Record) error {
			return handle(ctx, cfg.Recorder, rec)
		},
		Sleeper:        cfg.Sleeper,
		SessionTimeout: cfg.SessionTimeout,
	}, nil
}

func handle(ctx context.Context, recorder Recorder, rec mq.Record) (err error) {
	in, traceRaw, err := decodeRecord(rec)
	if err != nil {
		logNotify(ctx, "notify envelope invalid", rec.EventID, rec.EventType, "")
		return mq.Permanent(err)
	}
	ctx, end := obs.StartLinked(ctx, "payment", "consume", traceRaw)
	defer func() { end(err) }()
	obs.SetOrder(trace.SpanFromContext(ctx), in.PayNo, in.RefundNo, in.TransferNo, "", "")
	_, err = recorder.RecordNotify(ctx, in)
	if err == nil {
		return nil
	}
	logNotify(ctx, "notify record failed", in.EventID, in.EventType, in.MerchantID)
	if errors.Is(err, errno.ErrNotFound) || errors.Is(err, errno.ErrMerchantInvalid) || errors.Is(err, errno.ErrInvalidParam) {
		return mq.Permanent(err)
	}
	return err
}

func logNotify(ctx context.Context, msg, eventID, eventType, merchantID string) {
	slog.ErrorContext(ctx, msg,
		slog.String("event_id", eventID),
		slog.String("event_type", eventType),
		slog.String("merchant_id", merchantID),
	)
}

func decodeRecord(rec mq.Record) (biz.NotifyRecordInput, []byte, error) {
	env, err := decodeEnvelope(rec.Value)
	if err != nil {
		return biz.NotifyRecordInput{}, nil, errEnvelope
	}
	if env.EventID == "" || env.EventType == "" {
		return biz.NotifyRecordInput{}, nil, errEnvelope
	}
	if rec.EventID != "" && rec.EventID != env.EventID {
		return biz.NotifyRecordInput{}, nil, errEnvelope
	}
	if rec.EventType != "" && rec.EventType != env.EventType {
		return biz.NotifyRecordInput{}, nil, errEnvelope
	}
	want, ok := eventByTopic(rec.Topic)
	if !ok || want != env.EventType {
		return biz.NotifyRecordInput{}, nil, errEnvelope
	}
	fields, err := objectFields(env.Data)
	if err != nil {
		return biz.NotifyRecordInput{}, nil, err
	}
	in, err := fillInput(env, fields)
	if err != nil {
		return biz.NotifyRecordInput{}, nil, err
	}
	return in, env.TraceContext, nil
}

func eventByTopic(topic string) (string, bool) {
	switch topic {
	case TopicPaymentSucceeded:
		return eventPaymentSucceeded, true
	case TopicRefundSucceeded:
		return eventRefundSucceeded, true
	case TopicRefundFailed:
		return eventRefundFailed, true
	case TopicTransferSucceeded:
		return eventTransferSucceeded, true
	case TopicTransferFailed:
		return eventTransferFailed, true
	default:
		return "", false
	}
}

func fillInput(env envelope, fields map[string]json.RawMessage) (biz.NotifyRecordInput, error) {
	merchantID, err := requiredString(fields, "merchant_id", maxIDRunes)
	if err != nil {
		return biz.NotifyRecordInput{}, err
	}
	in := biz.NotifyRecordInput{
		EventID:    env.EventID,
		EventType:  env.EventType,
		MerchantID: merchantID,
	}
	if utf8.RuneCountInString(env.EventID) > maxIDRunes {
		return biz.NotifyRecordInput{}, errEnvelope
	}
	switch env.EventType {
	case eventPaymentSucceeded:
		return fillPayment(in, fields)
	case eventRefundSucceeded:
		return fillRefund(in, fields, false)
	case eventRefundFailed:
		return fillRefund(in, fields, true)
	case eventTransferSucceeded:
		return fillTransfer(in, fields, false)
	case eventTransferFailed:
		return fillTransfer(in, fields, true)
	default:
		return biz.NotifyRecordInput{}, errEnvelope
	}
}

func fillPayment(in biz.NotifyRecordInput, fields map[string]json.RawMessage) (biz.NotifyRecordInput, error) {
	var err error
	if in.PayNo, err = requiredString(fields, "pay_no", maxIDRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.OutTradeNo, err = requiredString(fields, "out_trade_no", maxOutRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.Amount, err = requiredAmount(fields, "amount"); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.FeeAmount, err = requiredAmount(fields, "fee_amount"); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.PaidAt, err = requiredString(fields, "paid_at", maxTimeRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	return in, nil
}

func fillRefund(in biz.NotifyRecordInput, fields map[string]json.RawMessage, failed bool) (biz.NotifyRecordInput, error) {
	var err error
	if in.RefundNo, err = requiredString(fields, "refund_no", maxIDRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.PayNo, err = requiredString(fields, "pay_no", maxIDRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.OutRefundNo, err = requiredString(fields, "out_refund_no", maxOutRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.Amount, err = requiredAmount(fields, "amount"); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if failed {
		if in.FailCode, err = requiredString(fields, "fail_code", maxFailRunes); err != nil {
			return biz.NotifyRecordInput{}, err
		}
		return in, nil
	}
	if in.SucceededAt, err = requiredString(fields, "succeeded_at", maxTimeRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	return in, nil
}

func fillTransfer(in biz.NotifyRecordInput, fields map[string]json.RawMessage, failed bool) (biz.NotifyRecordInput, error) {
	var err error
	if in.TransferNo, err = requiredString(fields, "transfer_no", maxIDRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.OutTransferNo, err = requiredString(fields, "out_transfer_no", maxOutRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.PayeeMerchantID, err = requiredString(fields, "payee_merchant_id", maxIDRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.Amount, err = requiredAmount(fields, "amount"); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if in.FinishedAt, err = requiredString(fields, "finished_at", maxTimeRunes); err != nil {
		return biz.NotifyRecordInput{}, err
	}
	if failed {
		if in.FailCode, err = requiredString(fields, "fail_code", maxFailRunes); err != nil {
			return biz.NotifyRecordInput{}, err
		}
	}
	return in, nil
}

func decodeEnvelope(raw []byte) (envelope, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var env envelope
	if err := dec.Decode(&env); err != nil {
		return envelope{}, err
	}
	if err := rejectTrailing(dec); err != nil {
		return envelope{}, err
	}
	return env, nil
}

func objectFields(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, errEnvelope
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var fields map[string]json.RawMessage
	if err := dec.Decode(&fields); err != nil || fields == nil {
		return nil, errEnvelope
	}
	if err := rejectTrailing(dec); err != nil {
		return nil, err
	}
	return fields, nil
}

func requiredString(fields map[string]json.RawMessage, key string, maxRunes int) (string, error) {
	raw, ok := fields[key]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return "", errEnvelope
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", errEnvelope
	}
	n := utf8.RuneCountInString(value)
	if n == 0 || n > maxRunes {
		return "", errEnvelope
	}
	return value, nil
}

func requiredAmount(fields map[string]json.RawMessage, key string) (money.Amount, error) {
	raw, ok := fields[key]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return 0, errEnvelope
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return 0, errEnvelope
	}
	number, ok := tok.(json.Number)
	if !ok {
		// 带引号的数字也不是整数分。
		return 0, errEnvelope
	}
	if err = rejectTrailing(dec); err != nil {
		return 0, err
	}
	value, err := number.Int64()
	if err != nil {
		return 0, errEnvelope
	}
	return money.Amount(value), nil
}

func rejectTrailing(dec *json.Decoder) error {
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return errEnvelope
	}
	return nil
}
