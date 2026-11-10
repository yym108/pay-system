package mq

import (
	"errors"

	"github.com/yym108/pay-system/pkg/server"
)

// RegisterProducer 用 brokers 创建生产者，并登记就绪检查、刷新和关闭。
// 返回的 *Producer 就是已登记的那一个。payment 用它投递，退出时仍由 Flush 和 Close 处理。
// brokers 是逗号分隔的地址。只有一个可用地址都没有时返回 ErrNoBroker。
// 创建客户端不拨号，broker 不可达时进程仍可启动。本函数不启动消费组。
func RegisterProducer(admin *server.Server, brokers string) (*Producer, error) {
	if admin == nil {
		return nil, errors.New("no admin server")
	}
	producer, err := OpenProducer(ParseBrokers(brokers))
	if err != nil {
		return nil, err
	}
	admin.RegisterCheck(producer.Ready)
	admin.RegisterFlusher(producer)
	admin.RegisterCloser(producer)
	return producer, nil
}
