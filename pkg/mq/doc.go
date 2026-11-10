// Package mq 提供 Kafka 生产与消费框架。
// 生产者使用 acks=all 并保持幂等生产。消费者按分区顺序处理，处理函数返回 nil 后才提交 offset。
package mq
