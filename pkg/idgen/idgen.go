// Package idgen 生成带前缀的雪花业务单号。
//
// 布局见 docs/03-conventions.md §2.1。回拨超过 5ms 时返回 ErrClockRollback，
// 不返回数字错误码；调用方以后把该错误映射为已有的 10004。时钟由本包的 Clock 注入，不是 pkg/clock。
package idgen

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	workerBits   = 10
	sequenceBits = 12
	timeShift    = workerBits + sequenceBits // 22
	maxWorkerID  = (1 << workerBits) - 1     // 1023
	maxSequence  = (1 << sequenceBits) - 1   // 4095
	maxTimestamp = (int64(1) << 41) - 1
	rollbackMax  = 5 * time.Millisecond
	workerEnv    = "WORKER_ID"
	numberDigits = 19
)

// epoch 是 2026-01-01 00:00:00 +08:00。用固定时区，不依赖 tzdata。
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))

var epochUnixMilli = epoch.UnixMilli()

// 前缀与约定 §2.2 一致。未登记的拒绝，本包不新增前缀。
var prefixes = map[string]struct{}{
	"PA": {},
	"RF": {},
	"TF": {},
	"TX": {},
	"FZ": {},
	"AC": {},
	"EV": {},
	"NT": {},
	"RB": {},
	"RD": {},
	"MC": {},
}

var (
	// ErrClockRollback 表示墙上时钟比上次发号回退超过 5ms。调用方应映射为 10004。
	ErrClockRollback = errors.New("idgen: clock rollback exceeds 5ms")
	// ErrTimeRange 表示时间早于纪元，或毫秒时间戳放不进 41 位。不会回绕。
	ErrTimeRange = errors.New("idgen: timestamp out of range")
	// ErrPrefix 表示前缀未在约定 §2.2 登记。
	ErrPrefix = errors.New("idgen: prefix not registered")
	// ErrWorkerID 表示 worker ID 缺失、不是整数或不在 0–1023。
	ErrWorkerID = errors.New("idgen: worker id invalid")
)

// Clock 提供当前时间。生产用 Real，测试注入实现。这不是 BASE-07 的 pkg/clock。
type Clock interface {
	Now() time.Time
}

// Real 使用本机当前时间。
type Real struct{}

// Now 返回本机当前时间。
func (Real) Now() time.Time { return time.Now() }

// Parsed 是业务单号拆开后的结果。Time 精确到发号毫秒。
type Parsed struct {
	Prefix   string
	Time     time.Time
	WorkerID int
	Sequence int
}

// Generator 按固定 worker 发号。同一个 Generator 并发安全。
type Generator struct {
	clock    Clock
	workerID int

	mu       sync.Mutex
	issued   bool
	lastWall time.Time
	lastMS   int64
	seq      int
}

// New 创建生成器。workerID 必须在 0–1023，clock 不能为 nil。
func New(workerID int, clk Clock) (*Generator, error) {
	if clk == nil {
		return nil, errors.New("idgen: nil clock")
	}
	if workerID < 0 || workerID > maxWorkerID {
		return nil, fmt.Errorf("%w: %d", ErrWorkerID, workerID)
	}
	return &Generator{clock: clk, workerID: workerID}, nil
}

// WorkerIDFromEnv 读取环境变量 WORKER_ID。
// 缺失、不是整数或不在 0–1023 时返回 ErrWorkerID。不退出进程。
func WorkerIDFromEnv() (int, error) {
	v, ok := os.LookupEnv(workerEnv)
	if !ok {
		return 0, fmt.Errorf("%w: %s is unset", ErrWorkerID, workerEnv)
	}
	return parseWorkerID(v)
}

func parseWorkerID(s string) (int, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || n > maxWorkerID {
		return 0, fmt.Errorf("%w: %q", ErrWorkerID, s)
	}
	return int(n), nil
}

// NewNo 返回 prefix 加 19 位左补零十进制，总长 21。
// prefix 必须是约定 §2.2 已登记的值。
// ctx 用来取消“等下一毫秒”和“追平不超过 5ms 的回拨”；取消时返回 ctx 的错误。
// 回拨超过 5ms 返回 ErrClockRollback，不返回数字错误码。
func (g *Generator) NewNo(ctx context.Context, prefix string) (string, error) {
	if ctx == nil {
		return "", errors.New("idgen: nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if _, ok := prefixes[prefix]; !ok {
		return "", fmt.Errorf("%w: %q", ErrPrefix, prefix)
	}
	id, err := g.next(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%0*d", prefix, numberDigits, id), nil
}

// Parse 解析业务单号中的前缀、毫秒时间、worker ID 和序列号。不阻塞。
func Parse(no string) (Parsed, error) {
	if len(no) != 2+numberDigits {
		return Parsed{}, fmt.Errorf("idgen: number length %d, want 21", len(no))
	}
	prefix := no[:2]
	if _, ok := prefixes[prefix]; !ok {
		return Parsed{}, fmt.Errorf("%w: %q", ErrPrefix, prefix)
	}
	digits := no[2:]
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return Parsed{}, fmt.Errorf("idgen: number %q has a non-digit", no)
		}
	}
	v, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return Parsed{}, fmt.Errorf("idgen: number %q: %w", no, err)
	}
	if v > uint64(math.MaxInt64) {
		return Parsed{}, fmt.Errorf("idgen: number %q has sign bit set", no)
	}
	id := int64(v)
	ms := id >> timeShift
	worker := int((id >> sequenceBits) & maxWorkerID)
	seq := int(id & maxSequence)
	return Parsed{
		Prefix:   prefix,
		Time:     epoch.Add(time.Duration(ms) * time.Millisecond),
		WorkerID: worker,
		Sequence: seq,
	}, nil
}

func (g *Generator) next(ctx context.Context) (int64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		g.mu.Lock()
		nowWall := wall(g.clock.Now())
		if g.issued && nowWall.Before(g.lastWall) {
			gap := g.lastWall.Sub(nowWall)
			g.mu.Unlock()
			if gap > rollbackMax {
				return 0, ErrClockRollback
			}
			if err := sleepCtx(ctx, gap); err != nil {
				return 0, err
			}
			continue
		}
		ms := nowWall.UnixMilli() - epochUnixMilli
		if ms < 0 || ms > maxTimestamp {
			g.mu.Unlock()
			return 0, ErrTimeRange
		}
		if g.issued && ms == g.lastMS && g.seq >= maxSequence {
			lastWall := g.lastWall
			g.mu.Unlock()
			if err := waitUntilMillis(ctx, g.clock, lastWall, ms+1); err != nil {
				return 0, err
			}
			continue
		}
		if !g.issued || ms != g.lastMS {
			g.seq = 0
			g.lastMS = ms
		} else {
			g.seq++
		}
		g.lastWall = nowWall
		g.issued = true
		id := compose(g.lastMS, g.workerID, g.seq)
		g.mu.Unlock()
		return id, nil
	}
}

// waitUntilMillis 等到雪花毫秒达到 target。回拨超过 5ms 时直接返回，避免在这里睡满整个缺口。
func waitUntilMillis(ctx context.Context, clk Clock, lastWall time.Time, target int64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		nowWall := wall(clk.Now())
		if nowWall.Before(lastWall) {
			gap := lastWall.Sub(nowWall)
			if gap > rollbackMax {
				return ErrClockRollback
			}
		}
		ms := nowWall.UnixMilli() - epochUnixMilli
		if ms >= target {
			return nil
		}
		gap := epoch.Add(time.Duration(target) * time.Millisecond).Sub(nowWall)
		if gap <= 0 || gap > time.Millisecond {
			gap = time.Millisecond
		}
		if err := sleepCtx(ctx, gap); err != nil {
			return err
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	// 过短的间隔会在假时钟停住时空转。3ms 以上的回拨等待保持原值，下限不改变验收耗时。
	if d < 200*time.Microsecond {
		d = 200 * time.Microsecond
	}
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func compose(ms int64, workerID, seq int) int64 {
	return (ms << timeShift) | (int64(workerID) << sequenceBits) | int64(seq)
}

// wall 去掉单调钟。回拨是墙上时钟往回跳；两边都带单调读数时，Before 看不到这次跳跃。
func wall(t time.Time) time.Time {
	return t.Round(0)
}
