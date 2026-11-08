package job

import (
	"context"
	"time"
)

// NewTransferRecover 按 interval 调用一次 batch。生产传 cfg.TransferRecoverInterval 和
// (*biz.Payments).RunTransferRecoverBatch。interval 小于等于 0 时 NewLoop 改用 1 秒。
func NewTransferRecover(interval time.Duration, batch func(context.Context) error) *Loop {
	return NewLoop("transfer_recover", interval, batch)
}
