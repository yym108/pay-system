package mq

import (
	"context"
	"errors"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer 向 Kafka 写消息。创建客户端不会拨号，broker 当时不可达也不拒绝启动。
type Producer struct {
	client *kgo.Client
}

// OpenProducer 按给定 broker 创建生产者。空列表返回 ErrNoBroker，错误文本不含地址。
// 不调用 DisableIdempotentWrite，幂等生产保持开启。
func OpenProducer(brokers []string) (*Producer, error) {
	seeds := compactBrokers(brokers)
	if len(seeds) == 0 {
		return nil, ErrNoBroker
	}
	cl, err := kgo.NewClient(baseOpts(seeds)...)
	if err != nil {
		return nil, errOpenClient
	}
	return &Producer{client: cl}, nil
}

func baseOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(Linger),
		kgo.RecordDeliveryTimeout(ProduceTimeout),
		kgo.ProduceRequestTimeout(ProduceTimeout),
	}
}

func compactBrokers(brokers []string) []string {
	out := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			out = append(out, broker)
		}
	}
	return out
}

// ParseBrokers 按逗号拆开 broker，去掉空白和空段。
func ParseBrokers(raw string) []string {
	return compactBrokers(strings.Split(raw, ","))
}

// Produce 等待这条消息被确认。单次超时 5 秒。三个消息头总会写入。
func (p *Producer) Produce(ctx context.Context, msg Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pctx, cancel := context.WithTimeout(ctx, ProduceTimeout)
	defer cancel()
	return mapProduceErr(ctx, p.client.ProduceSync(pctx, messageRecord(msg)).FirstErr())
}

// mapProduceErr 去掉 broker 地址。调用方 ctx 已取消时返回该错误。
func mapProduceErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, kgo.ErrRecordTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return errProduceTimeout
	}
	return ErrUnavailable
}

// Flush 把已经交给客户端的消息发出去。客户端为空时返回 nil。失败时的错误文本不含地址。
func (p *Producer) Flush(ctx context.Context) error {
	if p == nil || p.client == nil {
		return nil
	}
	return mapProduceErr(ctx, p.client.Flush(ctx))
}

// Close 关闭底层客户端。
func (p *Producer) Close() error {
	if p == nil || p.client == nil {
		return nil
	}
	p.client.Close()
	return nil
}

// Ready 在 ctx 内向 broker 要元数据。失败返回 ErrUnavailable，文本不含地址。
func (p *Producer) Ready(ctx context.Context) error {
	if p == nil || p.client == nil {
		return ErrUnavailable
	}
	if err := p.client.Ping(ctx); err != nil {
		return ErrUnavailable
	}
	return nil
}
