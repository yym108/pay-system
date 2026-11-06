package job

import (
	"context"
	"time"
)

// TimeoutCloseInterval 是超时关单的执行间隔（PAY-07 规则 1）。
const TimeoutCloseInterval = 5 * time.Second

// NewTimeoutClose 每 5 秒调用一次 batch。生产传 (*biz.Payments).RunTimeoutCloseBatch。
func NewTimeoutClose(batch func(context.Context) error) *Loop {
	return NewLoop("timeout_close", TimeoutCloseInterval, batch)
}
