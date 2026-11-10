package job

import (
	"context"
	"time"
)

// OutboxDeliverInterval 是 outbox 投递的执行间隔（MQ-02 第 1 条）。
const OutboxDeliverInterval = 200 * time.Millisecond

// OutboxGaugeInterval 是 outbox 积压指标的刷新间隔（MQ-02 第 5 条）。
const OutboxGaugeInterval = 10 * time.Second

// NewOutboxDeliver 每 200ms 调用一次 batch。生产传 (*outbox.Dispatcher).RunBatch。
func NewOutboxDeliver(batch func(context.Context) error) *Loop {
	return NewLoop("outbox_deliver", OutboxDeliverInterval, batch)
}

// NewOutboxGauge 每 10 秒调用一次 batch。生产传 (*outbox.Dispatcher).RefreshGauges。
func NewOutboxGauge(batch func(context.Context) error) *Loop {
	return NewLoop("outbox_gauge", OutboxGaugeInterval, batch)
}
