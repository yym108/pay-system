// Package consumer 在 account 进程里逐条消费支付和退款事件。
// 一条消息一个分片事务。不批量记账，不订阅转账 topic。
package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/account/biz"
)

const (
	// GroupName 是 account 进程里的入账消费组。
	GroupName = "account-ledger"

	// TopicPaymentSucceeded、TopicRefundSucceeded、TopicRefundFailed 是本消费组仅订阅的三个 topic。
	TopicPaymentSucceeded = "pay.payment.succeeded"
	TopicRefundSucceeded  = "pay.refund.succeeded"
	TopicRefundFailed     = "pay.refund.failed"

	eventPaymentSucceeded = "payment.succeeded"
	eventRefundSucceeded  = "refund.succeeded"
	eventRefundFailed     = "refund.failed"

	bizRefund = "REFUND"

	faultAfterCommit = "ledger.after_commit"

	maxNoRunes      = 64
	maxChannelRunes = 32
	maxFailRunes    = 32
	maxTimeRunes    = 64
)

// ledgerFreezeConflict 是 pay_ledger_freeze_conflict_total。
// 退款事件返回 40004 时加 1。只在本进程内存里，不进 Prometheus，也不进 /metrics。
var ledgerFreezeConflict atomic.Uint64

// FreezeConflictTotal 返回本进程累计的冻结冲突次数。
func FreezeConflictTotal() uint64 {
	return ledgerFreezeConflict.Load()
}

// Ledger 是逐条入账用到的账户方法。*biz.Service 已实现。
type Ledger interface {
	PostPay(ctx context.Context, payNo, merchantID, channel string, amount, fee money.Amount) (biz.PostResult, error)
	ConfirmFreeze(ctx context.Context, bizType, bizNo, merchantID, channel string) (biz.ConfirmResult, error)
	ReleaseFreeze(ctx context.Context, bizType, bizNo, merchantID string, amount money.Amount) (biz.FreezeResult, error)
}

// Config 装配消费组。SessionTimeout 和 Sleeper 只给测试。生产路径保持零值。
// 不提供 BeforeCommit。那个钩子只给框架测试。
type Config struct {
	Brokers        []string
	Ledger         Ledger
	SessionTimeout time.Duration
	Sleeper        mq.Sleeper
}

var (
	errEnvelope = errors.New("ledger envelope invalid")
	errFee      = errors.New("ledger fee invalid")
)

type envelope struct {
	EventID      string          `json:"event_id"`
	EventType    string          `json:"event_type"`
	TraceContext json.RawMessage `json:"trace_context"`
	Data         json.RawMessage `json:"data"`
}

// Open 创建 account-ledger 消费组。不在这里拨号，也不开始拉取。
// 生产路径不设置 BeforeCommit。
func Open(cfg Config) (*mq.Consumer, error) {
	built, err := buildConfig(cfg)
	if err != nil {
		return nil, err
	}
	return mq.OpenConsumer(built)
}

func buildConfig(cfg Config) (mq.ConsumerConfig, error) {
	if cfg.Ledger == nil {
		return mq.ConsumerConfig{}, errors.New("no ledger")
	}
	return mq.ConsumerConfig{
		Brokers: cfg.Brokers,
		Group:   GroupName,
		Topics:  []string{TopicPaymentSucceeded, TopicRefundSucceeded, TopicRefundFailed},
		Handler: func(ctx context.Context, rec mq.Record) error {
			return HandleRecord(ctx, cfg.Ledger, rec)
		},
		Sleeper:        cfg.Sleeper,
		SessionTimeout: cfg.SessionTimeout,
	}, nil
}

// HandleRecord 处理一条入账消息。消费组回调走这里。
// trace_context 解析失败仍然处理消息，不把它写成永久错误。
func HandleRecord(ctx context.Context, ledger Ledger, rec mq.Record) error {
	return handle(ctx, ledger, rec)
}

func handle(ctx context.Context, ledger Ledger, rec mq.Record) (err error) {
	env, fields, err := decodeRecord(rec)
	if err != nil {
		return mq.Permanent(err)
	}
	ctx, end := obs.StartLinked(ctx, "account", "consume", env.TraceContext)
	defer func() { end(err) }()
	// 生产时刻在 Kafka 头里。头缺失或无法解析不记样本，也不进 DLQ。
	if env.EventType == eventPaymentSucceeded {
		obs.ObserveMQWait(ctx, rec.ProducedAt)
	}
	switch env.EventType {
	case eventPaymentSucceeded:
		return handlePayment(ctx, ledger, env, fields)
	case eventRefundSucceeded:
		return handleRefundSucceeded(ctx, ledger, env, fields)
	case eventRefundFailed:
		return handleRefundFailed(ctx, ledger, env, fields)
	default:
		return mq.Permanent(errEnvelope)
	}
}

func handlePayment(ctx context.Context, ledger Ledger, env envelope, fields map[string]json.RawMessage) error {
	payNo, err := requiredString(fields, "pay_no", maxNoRunes)
	if err != nil {
		return mq.Permanent(err)
	}
	obs.SetOrder(trace.SpanFromContext(ctx), payNo, "", "", "", "")
	merchantID, err := requiredMerchant(fields)
	if err != nil {
		return mq.Permanent(err)
	}
	if _, err = requiredString(fields, "out_trade_no", maxNoRunes); err != nil {
		return mq.Permanent(err)
	}
	channel, err := requiredString(fields, "channel", maxChannelRunes)
	if err != nil || utf8.RuneCountInString(channel) == 0 {
		return mq.Permanent(errEnvelope)
	}
	amount, err := requiredAmount(fields, "amount")
	if err != nil || amount <= 0 {
		return mq.Permanent(errEnvelope)
	}
	fee, err := requiredAmount(fields, "fee_amount")
	if err != nil {
		return mq.Permanent(errEnvelope)
	}
	if _, err = requiredString(fields, "paid_at", maxTimeRunes); err != nil {
		return mq.Permanent(err)
	}
	if fee < 0 || fee > amount {
		return mq.Permanent(errFee)
	}
	started := time.Now()
	_, err = ledger.PostPay(ctx, payNo, merchantID, channel, amount, fee)
	if err != nil {
		return classifyPay(ctx, env.EventID, env.EventType, merchantID, err)
	}
	obs.ObserveLedgerPost("PAY", ledgerShard(merchantID), time.Since(started))
	// 事务已经提交。在返回 nil 之前打点，进程被杀掉时 offset 还没提交。
	fault.Point(ctx, faultAfterCommit)
	return nil
}

func handleRefundSucceeded(ctx context.Context, ledger Ledger, env envelope, fields map[string]json.RawMessage) error {
	refundNo, merchantID, channel, err := refundIdentity(fields, true)
	if err != nil {
		return mq.Permanent(err)
	}
	markRefund(ctx, fields, refundNo)
	if _, err = requiredString(fields, "succeeded_at", maxTimeRunes); err != nil {
		return mq.Permanent(err)
	}
	started := time.Now()
	_, err = ledger.ConfirmFreeze(ctx, bizRefund, refundNo, merchantID, channel)
	if err != nil {
		return classifyRefund(ctx, env.EventID, env.EventType, merchantID, err)
	}
	obs.ObserveLedgerPost(bizRefund, ledgerShard(merchantID), time.Since(started))
	fault.Point(ctx, faultAfterCommit)
	return nil
}

func handleRefundFailed(ctx context.Context, ledger Ledger, env envelope, fields map[string]json.RawMessage) error {
	refundNo, merchantID, _, err := refundIdentity(fields, false)
	if err != nil {
		return mq.Permanent(err)
	}
	markRefund(ctx, fields, refundNo)
	amount, err := requiredAmount(fields, "amount")
	if err != nil || amount <= 0 {
		return mq.Permanent(errEnvelope)
	}
	if _, err = requiredString(fields, "fail_code", maxFailRunes); err != nil {
		return mq.Permanent(err)
	}
	_, err = ledger.ReleaseFreeze(ctx, bizRefund, refundNo, merchantID, amount)
	if err != nil {
		return classifyRefund(ctx, env.EventID, env.EventType, merchantID, err)
	}
	fault.Point(ctx, faultAfterCommit)
	return nil
}

func ledgerShard(merchantID string) string {
	n, err := shard.Of(merchantID)
	if err != nil {
		return "unknown"
	}
	return strconv.Itoa(n)
}

func refundIdentity(fields map[string]json.RawMessage, withChannel bool) (refundNo, merchantID, channel string, err error) {
	refundNo, err = requiredString(fields, "refund_no", maxNoRunes)
	if err != nil {
		return "", "", "", err
	}
	if _, err = requiredString(fields, "pay_no", maxNoRunes); err != nil {
		return "", "", "", err
	}
	merchantID, err = requiredMerchant(fields)
	if err != nil {
		return "", "", "", err
	}
	if _, err = requiredString(fields, "out_refund_no", maxNoRunes); err != nil {
		return "", "", "", err
	}
	amount, amountErr := requiredAmount(fields, "amount")
	if amountErr != nil || amount <= 0 {
		return "", "", "", errEnvelope
	}
	if withChannel {
		channel, err = requiredString(fields, "channel", maxChannelRunes)
		if err != nil {
			return "", "", "", err
		}
	}
	return refundNo, merchantID, channel, nil
}

func classifyPay(ctx context.Context, eventID, eventType, merchantID string, err error) error {
	switch {
	case errors.Is(err, errno.ErrAccountNotFound):
		logLedger(ctx, "ledger account missing", eventID, eventType, merchantID)
		return mq.Permanent(err)
	case errors.Is(err, errno.ErrAccountInactive):
		logLedger(ctx, "ledger account inactive", eventID, eventType, merchantID)
		return mq.Permanent(err)
	default:
		return err
	}
}

func classifyRefund(ctx context.Context, eventID, eventType, merchantID string, err error) error {
	switch {
	case errors.Is(err, errno.ErrFreezeStatusConflict):
		ledgerFreezeConflict.Add(1)
		logLedger(ctx, "ledger freeze conflict", eventID, eventType, merchantID)
		return mq.Permanent(err)
	case errors.Is(err, errno.ErrAccountInactive):
		// 平台渠道应收不是 ACTIVE。永久错误写入 DLQ 后提交 offset，分区继续；不算冻结冲突。
		logLedger(ctx, "ledger account inactive", eventID, eventType, merchantID)
		return mq.Permanent(err)
	default:
		// 40001 是平台子账户还不存在，继续重试。
		return err
	}
}

func markRefund(ctx context.Context, fields map[string]json.RawMessage, refundNo string) {
	payNo, err := requiredString(fields, "pay_no", maxNoRunes)
	if err != nil {
		payNo = ""
	}
	obs.SetOrder(trace.SpanFromContext(ctx), payNo, refundNo, "", "", "")
}

func logLedger(ctx context.Context, msg, eventID, eventType, merchantID string) {
	slog.ErrorContext(ctx, msg,
		slog.String("event_id", eventID),
		slog.String("event_type", eventType),
		slog.String("merchant_id", merchantID),
	)
}

func decodeRecord(rec mq.Record) (envelope, map[string]json.RawMessage, error) {
	env, err := decodeEnvelope(rec.Value)
	if err != nil {
		return envelope{}, nil, errEnvelope
	}
	if env.EventID == "" || env.EventType == "" {
		return envelope{}, nil, errEnvelope
	}
	if rec.EventID != "" && rec.EventID != env.EventID {
		return envelope{}, nil, errEnvelope
	}
	if rec.EventType != "" && rec.EventType != env.EventType {
		return envelope{}, nil, errEnvelope
	}
	want, ok := eventByTopic(rec.Topic)
	if !ok || want != env.EventType {
		return envelope{}, nil, errEnvelope
	}
	fields, err := objectFields(env.Data)
	if err != nil {
		return envelope{}, nil, err
	}
	return env, fields, nil
}

func eventByTopic(topic string) (string, bool) {
	switch topic {
	case TopicPaymentSucceeded:
		return eventPaymentSucceeded, true
	case TopicRefundSucceeded:
		return eventRefundSucceeded, true
	case TopicRefundFailed:
		return eventRefundFailed, true
	default:
		return "", false
	}
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

func requiredMerchant(fields map[string]json.RawMessage) (string, error) {
	merchantID, err := requiredString(fields, "merchant_id", maxNoRunes)
	if err != nil {
		return "", err
	}
	if _, err = shard.Of(merchantID); err != nil {
		return "", errEnvelope
	}
	return merchantID, nil
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
		// 带引号的数字也不是整数分。Go 1.27 的 json.Number 会收下这种字符串。
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
