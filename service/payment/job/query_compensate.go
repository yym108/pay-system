package job

import (
	"context"
	"time"
)

// QueryCompensateInterval 是查单补偿的执行间隔（PAY-05 第 1 条）。
const QueryCompensateInterval = time.Second

// NewQueryCompensate 每秒调用一次 batch。生产传 (*biz.Payments).RunQueryBatch。
func NewQueryCompensate(batch func(context.Context) error) *Loop {
	return NewLoop("query_compensate", QueryCompensateInterval, batch)
}
