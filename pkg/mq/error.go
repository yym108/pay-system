package mq

import "errors"

// ErrNoBroker 表示配置里没有可用的 broker。错误文本不含地址。
var ErrNoBroker = errors.New("no kafka broker")

// ErrUnavailable 表示就绪检查没有连上 Kafka。错误文本不含地址。
var ErrUnavailable = errors.New("unavailable")

var errOpenClient = errors.New("open kafka client failed")

// errProduceTimeout 是单次生产超时的固定文本，不含 broker 地址。
var errProduceTimeout = errors.New("produce timeout")

// errHandlerFailed 是处理协程 panic 后的可重试错误。文本固定，不含 panic 值。
var errHandlerFailed = errors.New("kafka handler failed")

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }

func (e *permanentError) Unwrap() error { return e.err }

// Permanent 把 err 标成不可重试。nil 仍返回 nil。
// 反序列化失败、数据缺失、业务冲突由调用方包成 Permanent。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent 报告 err 链上是否有 Permanent 标记。其余非 nil 错误都按可重试处理。
func IsPermanent(err error) bool {
	var target *permanentError
	return errors.As(err, &target)
}
