package obs

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// LatencyBuckets 是全进程共用的延迟直方图桶，单位秒。
// 经典直方图还会带上 +Inf。对账、转账卡单、退款额度、分阶段耗时和入账批大小只注册，不在这里递增。
var LatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

const (
	routeUnmatched = "unmatched"
	labelUnknown   = "unknown"
)

var (
	httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_http_requests_total",
		Help: "HTTP requests handled by the gateway access log.",
	}, []string{"route", "code"})
	httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pay_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: LatencyBuckets,
	}, []string{"route", "code"})
	rpcDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pay_rpc_duration_seconds",
		Help:    "RPC handler duration in seconds.",
		Buckets: LatencyBuckets,
	}, []string{"service", "method", "biz_code"})
	orderTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_order_transitions_total",
		Help: "Successful order status CAS transitions.",
	}, []string{"type", "from", "to"})
	outboxPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pay_outbox_pending",
		Help: "Outbox rows currently pending, by topic.",
	}, []string{"topic"})
	outboxDead = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pay_outbox_dead",
		Help: "Outbox rows currently dead, by topic.",
	}, []string{"topic"})
	mqLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pay_mq_consume_lag",
		Help: "Sum of partition consume lag for a topic and group.",
	}, []string{"topic", "group"})
	mqErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_mq_consume_errors_total",
		Help: "Consume errors by topic and group.",
	}, []string{"topic", "group"})
	mqDLQ = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_mq_dlq_total",
		Help: "Messages sent to the DLQ by topic and group.",
	}, []string{"topic", "group"})
	channelRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_channel_requests_total",
		Help: "Channel HTTP calls that were actually sent.",
	}, []string{"api", "result"})
	channelDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pay_channel_request_duration_seconds",
		Help:    "Channel HTTP call duration in seconds.",
		Buckets: LatencyBuckets,
	}, []string{"api", "result"})
	channelQuery = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_channel_query_total",
		Help: "Channel query outcomes from compensate.",
	}, []string{"result"})
	channelQueryExhausted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_channel_query_exhausted_total",
		Help: "Query tasks that will not be queried again.",
	}, []string{"result"})
	callbackConflicts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_callback_conflicts_total",
		Help: "Callback results that conflict with the stored terminal status.",
	}, []string{"channel"})
	timeoutCloseFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_timeout_close_failures_total",
		Help: "Timeout close attempts that returned an error.",
	}, []string{"err_code"})
	callbackRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_callback_rejected_total",
		Help: "Channel callbacks rejected with a business code.",
	}, []string{"channel", "code"})
	notifyAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_notify_attempts_total",
		Help: "Merchant notify writebacks that won the CAS.",
	}, []string{"result"})
	ledgerCheckFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pay_ledger_check_failures_total",
		Help: "Completed balance checks that found at least one failure.",
	})
	ledgerPostDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pay_ledger_post_duration_seconds",
		Help:    "Successful ledger post duration in seconds.",
		Buckets: LatencyBuckets,
	}, []string{"biz_type", "shard"})
	ledgerBatchSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pay_ledger_batch_size",
		Help:    "Ledger batch size. No batch path observes this yet.",
		Buckets: LatencyBuckets,
	}, []string{"shard"})
	ledgerBatchFallback = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_ledger_batch_fallback_total",
		Help: "Ledger batch fallbacks. No batch path increments this yet.",
	}, []string{"shard"})
	stageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pay_stage_duration_seconds",
		Help:    "Stage duration in seconds.",
		Buckets: LatencyBuckets,
	}, []string{"chain", "stage"})
	transferStuck = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pay_transfer_stuck",
		Help: "Stuck transfers. The in-memory set is not exported.",
	}, []string{"mode"})
	transferInconsistent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_transfer_inconsistent_total",
		Help: "Transfer results that disagree across branches.",
	}, []string{"mode"})
	refundQuotaInconsistent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pay_refund_quota_inconsistent_total",
		Help: "Refund quota mismatches. Registered only; the in-process counter is not mirrored.",
	})
	reconDiffs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_recon_diffs_total",
		Help: "Reconciliation diffs. No recon instrumentation yet.",
	}, []string{"diff_type"})
	reconPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pay_recon_pending_diffs",
		Help: "Pending reconciliation diffs. No recon instrumentation yet.",
	}, []string{"diff_type"})
	reconChannelInconsistent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pay_recon_channel_inconsistent_total",
		Help: "Severe channel inconsistencies. No recon instrumentation yet.",
	}, []string{"channel"})
	ratelimitRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pay_ratelimit_rejected_total",
		Help: "Merchant requests rejected by the rate limiter.",
	})
	nonceUnavailable = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pay_nonce_unavailable_total",
		Help: "Nonce checks that failed because the store was unavailable.",
	})
)

func init() {
	prometheus.MustRegister(
		httpRequests,
		httpDuration,
		rpcDuration,
		orderTransitions,
		outboxPending,
		outboxDead,
		mqLag,
		mqErrors,
		mqDLQ,
		channelRequests,
		channelDuration,
		channelQuery,
		channelQueryExhausted,
		callbackConflicts,
		timeoutCloseFailures,
		callbackRejected,
		notifyAttempts,
		ledgerCheckFailures,
		ledgerPostDuration,
		ledgerBatchSize,
		ledgerBatchFallback,
		stageDuration,
		transferStuck,
		transferInconsistent,
		refundQuotaInconsistent,
		reconDiffs,
		reconPending,
		reconChannelInconsistent,
		ratelimitRejected,
		nonceUnavailable,
	)
}

// Handler 是管理端口的 GET /metrics。默认注册表已经带有 Go runtime 与进程指标。
func Handler() http.Handler {
	return promhttp.Handler()
}

// RPCMetrics 实现 payment 与 account 的 Metrics。Service 写入 service 标签。
// 零值仍会计数，service 标签为 unknown。调用方把 nil 接口留在 handler 配置里才是空实现。
type RPCMetrics struct {
	Service string
}

// Observe 记录一次 RPC。bizCode 按十进制写入，不使用单号。
func (m RPCMetrics) Observe(method string, bizCode int32, elapsed time.Duration) {
	rpcDuration.WithLabelValues(label(m.Service), label(method), strconv.FormatInt(int64(bizCode), 10)).Observe(seconds(elapsed))
}

// ObserveHTTP 记录网关访问日志上的一次请求。route 为空时记为 unmatched，不用原始路径。
func ObserveHTTP(route, code string, elapsed time.Duration) {
	if route == "" {
		route = routeUnmatched
	}
	httpRequests.WithLabelValues(route, label(code)).Inc()
	httpDuration.WithLabelValues(route, label(code)).Observe(seconds(elapsed))
}

// OrderTransition 记录一次成功的状态 CAS。幂等重放不要调用。
func OrderTransition(kind, from, to string) {
	orderTransitions.WithLabelValues(label(kind), label(from), label(to)).Inc()
}

// SetOutboxPending 用当前积压覆盖该 topic 的 gauge。
func SetOutboxPending(topic string, n int64) {
	outboxPending.WithLabelValues(label(topic)).Set(float64(n))
}

// SetOutboxDead 用当前死信数覆盖该 topic 的 gauge。
func SetOutboxDead(topic string, n int64) {
	outboxDead.WithLabelValues(label(topic)).Set(float64(n))
}

// SetConsumeLag 覆盖 topic 与 group 的分区延迟合计。
func SetConsumeLag(topic, group string, lag int64) {
	mqLag.WithLabelValues(label(topic), label(group)).Set(float64(lag))
}

// AddConsumeError 把消费错误加 1。
func AddConsumeError(topic, group string) {
	mqErrors.WithLabelValues(label(topic), label(group)).Inc()
}

// AddDLQ 把进入死信的消息加 1。
func AddDLQ(topic, group string) {
	mqDLQ.WithLabelValues(label(topic), label(group)).Inc()
}

// ObserveChannel 记录一次已经发出的渠道 HTTP。耗时由调用方的注入时钟计算。
func ObserveChannel(api, result string, elapsed time.Duration) {
	channelRequests.WithLabelValues(label(api), label(result)).Inc()
	channelDuration.WithLabelValues(label(api), label(result)).Observe(seconds(elapsed))
}

// AddChannelQuery 记录一次查单结果。
func AddChannelQuery(result string) {
	channelQuery.WithLabelValues(label(result)).Inc()
}

// AddChannelQueryExhausted 记录一次不再查单的任务。
func AddChannelQueryExhausted(result string) {
	channelQueryExhausted.WithLabelValues(label(result)).Inc()
}

// AddCallbackConflict 记录一次回调冲突。
func AddCallbackConflict(channel string) {
	callbackConflicts.WithLabelValues(label(channel)).Inc()
}

// AddTimeoutCloseFailure 按错误码记录一次超时关单失败。
func AddTimeoutCloseFailure(errCode string) {
	timeoutCloseFailures.WithLabelValues(label(errCode)).Inc()
}

// AddCallbackRejected 记录一次被业务码拒绝的回调。
func AddCallbackRejected(channel, code string) {
	callbackRejected.WithLabelValues(label(channel), label(code)).Inc()
}

// AddNotifyAttempt 记录一次写回成功的通知。result 为 success、failed 或 dead。
func AddNotifyAttempt(result string) {
	notifyAttempts.WithLabelValues(label(result)).Inc()
}

// AddLedgerCheckFailure 把余额核对失败加 1。
func AddLedgerCheckFailure() {
	ledgerCheckFailures.Inc()
}

// ObserveLedgerPost 记录一次成功入账的耗时。
func ObserveLedgerPost(bizType, shard string, elapsed time.Duration) {
	ledgerPostDuration.WithLabelValues(label(bizType), label(shard)).Observe(seconds(elapsed))
}

// AddRateLimitRejected 把限流拒绝加 1。没有商户号标签。
func AddRateLimitRejected() {
	ratelimitRejected.Inc()
}

// AddNonceUnavailable 把 nonce 存储不可用加 1。
func AddNonceUnavailable() {
	nonceUnavailable.Inc()
}

// AddTransferInconsistent 记录一次转账不一致。
func AddTransferInconsistent(mode string) {
	transferInconsistent.WithLabelValues(label(mode)).Inc()
}

func label(v string) string {
	if v == "" {
		return labelUnknown
	}
	return v
}

func seconds(d time.Duration) float64 {
	if d < 0 {
		d = 0
	}
	return d.Seconds()
}
