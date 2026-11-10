package mq

import "sync"

// 计数和延迟都在进程内，按 topic + group 区分。延迟再按分区。
// 不提供 HTTP，也不暴露 /metrics。

type metricKey struct {
	topic string
	group string
}

type lagKey struct {
	topic     string
	group     string
	partition int32
}

var (
	metricMu      sync.Mutex
	consumeErrors = map[metricKey]int64{}
	dlqTotals     = map[metricKey]int64{}
	consumeLags   = map[lagKey]int64{}
)

func addConsumeError(group, topic string) {
	metricMu.Lock()
	consumeErrors[metricKey{topic: topic, group: group}]++
	metricMu.Unlock()
}

func addDLQ(group, topic string) {
	metricMu.Lock()
	dlqTotals[metricKey{topic: topic, group: group}]++
	metricMu.Unlock()
}

func setLag(group, topic string, partition int32, lag int64) {
	metricMu.Lock()
	consumeLags[lagKey{topic: topic, group: group, partition: partition}] = lag
	metricMu.Unlock()
}

// ConsumeErrors 返回该消费组在 topic 上的 pay_mq_consume_errors_total。
func (c *Consumer) ConsumeErrors(topic string) int64 {
	metricMu.Lock()
	defer metricMu.Unlock()
	return consumeErrors[metricKey{topic: topic, group: c.group}]
}

// DLQTotal 返回该消费组在 topic 上的 pay_mq_dlq_total。
func (c *Consumer) DLQTotal(topic string) int64 {
	metricMu.Lock()
	defer metricMu.Unlock()
	return dlqTotals[metricKey{topic: topic, group: c.group}]
}

// ConsumeLag 返回该分区的 pay_mq_consume_lag。
func (c *Consumer) ConsumeLag(topic string, partition int32) int64 {
	metricMu.Lock()
	defer metricMu.Unlock()
	return consumeLags[lagKey{topic: topic, group: c.group, partition: partition}]
}
