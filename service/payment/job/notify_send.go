package job

import (
	"context"
	"time"
)

// NotifySendInterval 是商户通知发送的扫描间隔（NTF-02 第 1 条）。
// 这不是重试间隔。重试间隔是 NOTIFY_RETRY_INTERVAL，写在抢占时的 next_attempt_at 上。
const NotifySendInterval = time.Second

// NewNotifySend 每秒调用一次 batch。生产传 (*biz.Payments).RunNotifyBatch。
func NewNotifySend(batch func(context.Context) error) *Loop {
	return NewLoop("notify_send", NotifySendInterval, batch)
}
