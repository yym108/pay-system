package mq

import (
	"context"
	"time"
)

const (
	// RetryBase 是可重试错误的第一次等待。之后每次翻倍。
	RetryBase = 100 * time.Millisecond
	// RetryMax 是原地退避的上限。
	RetryMax = 5 * time.Second
	// ProduceTimeout 是单次生产等待确认的上限。
	ProduceTimeout = 5 * time.Second
	// Linger 是生产者凑批的时间。
	Linger = 5 * time.Millisecond
)

// Sleeper 等待一次退避。返回值非 nil 时消费循环停止，不提交 offset。
type Sleeper func(ctx context.Context, delay time.Duration) error

func nextDelay(current time.Duration) time.Duration {
	if current >= RetryMax/2 {
		return RetryMax
	}
	next := current * 2
	if next > RetryMax {
		return RetryMax
	}
	return next
}

func defaultSleeper(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
