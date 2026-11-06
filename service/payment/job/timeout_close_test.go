package job

import (
	"context"
	"testing"
	"time"
)

// PAY-07 规则 1：超时关单每 5 秒执行一次，日志字段 job 为 timeout_close。
func TestNewTimeoutClose(t *testing.T) {
	l := NewTimeoutClose(func(context.Context) error { return nil })
	if l.interval != TimeoutCloseInterval || TimeoutCloseInterval != 5*time.Second || l.name != "timeout_close" {
		t.Fatalf("loop = %s %s", l.name, l.interval)
	}
}
