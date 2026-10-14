package testkit

import (
	"time"

	"github.com/yym108/pay-system/pkg/clock"
)

// NewClock 返回测试时钟，当前时间是 now。
// 实现是已有的 clock.Fake，不读取真实时间，也不修改时钟包。
func NewClock(now time.Time) *clock.Fake {
	f := &clock.Fake{}
	f.Set(now)
	return f
}
