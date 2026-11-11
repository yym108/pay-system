package mq

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// HeaderEventID、HeaderEventType、HeaderTraceparent、HeaderProducedAt 是每条生产消息都写的头。
	// traceparent 为空也写这个键。produced_at_ns 是生产时刻的十进制纳秒。
	HeaderEventID       = "event_id"
	HeaderEventType     = "event_type"
	HeaderTraceparent   = "traceparent"
	HeaderProducedAt    = "produced_at_ns"
	HeaderOrigTopic     = "x-orig-topic"
	HeaderOrigPartition = "x-orig-partition"
	HeaderOrigOffset    = "x-orig-offset"
	HeaderError         = "x-error"

	// DLQTopic 是不可重试消息的目标 topic。
	DLQTopic = "pay.dlq"

	errorHeaderMax = 512
)

// Message 是调用方交给生产者的一条记录。本包不解析 outbox 信封。
type Message struct {
	Topic       string
	Key         []byte
	Value       []byte
	EventID     string
	EventType   string
	Traceparent string
}

// Record 是交给处理函数的一条已拉取记录。
type Record struct {
	Topic       string
	Partition   int32
	Offset      int64
	Key         []byte
	Value       []byte
	EventID     string
	EventType   string
	Traceparent string
	ProducedAt  string
}

// Handler 处理一条消息。返回 nil 才提交 offset。
type Handler func(ctx context.Context, rec Record) error

// CommitHook 在处理函数返回 nil 之后、提交 offset 之前运行。
// 生产路径不设置。测试用它在提交前结束进程。
type CommitHook func(ctx context.Context, rec Record)

func messageRecord(msg Message) *kgo.Record {
	producedAt := time.Now()
	return &kgo.Record{
		Topic: msg.Topic,
		Key:   append([]byte(nil), msg.Key...),
		Value: append([]byte(nil), msg.Value...),
		Headers: []kgo.RecordHeader{
			{Key: HeaderEventID, Value: []byte(msg.EventID)},
			{Key: HeaderEventType, Value: []byte(msg.EventType)},
			{Key: HeaderTraceparent, Value: []byte(msg.Traceparent)},
			{Key: HeaderProducedAt, Value: []byte(strconv.FormatInt(producedAt.UnixNano(), 10))},
		},
	}
}

func toRecord(rec *kgo.Record) Record {
	return Record{
		Topic:       rec.Topic,
		Partition:   rec.Partition,
		Offset:      rec.Offset,
		Key:         rec.Key,
		Value:       rec.Value,
		EventID:     headerString(rec, HeaderEventID),
		EventType:   headerString(rec, HeaderEventType),
		Traceparent: headerString(rec, HeaderTraceparent),
		ProducedAt:  headerString(rec, HeaderProducedAt),
	}
}

func headerString(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
