package testkit

import (
	"testing"
	"time"
)

// BASE-11：构造工具返回已有的 clock.Fake，不读取真实时间。
func TestNewClockKeepsInjectedInstant(t *testing.T) {
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	now := time.Date(2026, 10, 15, 8, 30, 0, 0, loc)
	c := NewClock(now)
	if !c.Now().Equal(now) {
		t.Fatalf("Now = %s, want %s", c.Now(), now)
	}
	if c.Now().Location().String() != "Asia/Shanghai" {
		t.Fatalf("location = %s", c.Now().Location())
	}
	c.Advance(10 * time.Second)
	if !c.Now().Equal(now.Add(10 * time.Second)) {
		t.Fatalf("Advance = %s", c.Now())
	}
}
