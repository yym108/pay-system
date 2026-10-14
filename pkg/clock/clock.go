// Package clock 提供可注入的当前时间。
//
// 业务代码通过 Clock 取时间。Real 返回固定东八区时间，不读取 zoneinfo。
// Fake 供测试设定和拨动时间。
package clock

import (
	"sync"
	"time"
)

// shanghaiOffset 是东八区相对 UTC 的秒数。
// 中国自 1991 年起没有夏令时，固定偏移与当前民用时间一致。
const shanghaiOffset = 8 * 60 * 60

// shanghai 的地点名是 Asia/Shanghai。
// 用固定时区，是为了不调用 time.LoadLocation：运行镜像没有 tzdata 时，按名加载会失败。
var shanghai = time.FixedZone("Asia/Shanghai", shanghaiOffset)

// Clock 提供当前时间。生产注入 Real，测试注入 Fake。
type Clock interface {
	Now() time.Time
}

// Real 是生产时钟。
type Real struct{}

// Now 返回当前瞬间的东八区时间。地点名是 Asia/Shanghai，偏移固定为 +8 小时。
// 先取 time.Now() 再换地点：整段 time.Now().In 会让 forbidigo 的排除规则对不上。
func (Real) Now() time.Time {
	now := time.Now()
	return now.In(shanghai)
}

// Fake 是测试时钟。零值的当前时间是 Go 零时间。
// 并发安全。不要复制 Fake，复制会复制锁。
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// Now 返回最近一次 Set 或 Advance 之后的时间。
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Set 把当前时间设为 t，瞬间和地点都按 t 保留。
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}

// Advance 把当前时间前进 d。d 为负时回拨。前进量就是 d，不读取真实时钟。
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// 编译期确认两个实现都满足 Clock。Fake 含锁，只能以指针注入。
var (
	_ Clock = Real{}
	_ Clock = (*Fake)(nil)
)
