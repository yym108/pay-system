package job

import (
	"context"
	"time"
)

// RefundRecoverInterval 是退款卡单恢复的执行间隔（REF-04 第 1 条）。
const RefundRecoverInterval = 10 * time.Second

// NewRefundRecover 每 10 秒调用一次 batch。生产传 (*biz.Payments).RunRefundRecoverBatch。
func NewRefundRecover(batch func(context.Context) error) *Loop {
	return NewLoop("refund_recover", RefundRecoverInterval, batch)
}
