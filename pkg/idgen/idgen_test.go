package idgen

import (
	"context"
	"errors"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// 与约定 §2.1 的纪元同一瞬间。时区名不参与比较。
var testEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))

func mustGenerator(t *testing.T, workerID int, clk Clock) *Generator {
	t.Helper()
	g, err := New(workerID, clk)
	if err != nil {
		t.Fatalf("New(%d): %v", workerID, err)
	}
	return g
}

// BASE-05 验收 1：64 个 goroutine 共生成 100 万个 ID，无重复，单个 goroutine 内严格递增。
func TestMillionIDsUniqueAndMonotonic(t *testing.T) {
	const (
		goroutines = 64
		total      = 1_000_000
		perG       = total / goroutines
		workerID   = 7
	)
	if goroutines*perG != total {
		t.Fatalf("total %d is not divisible by %d", total, goroutines)
	}
	g := mustGenerator(t, workerID, Real{})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	parts := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got := make([]uint64, perG)
			var prev uint64
			for j := 0; j < perG; j++ {
				no, err := g.NewNo(ctx, "PA")
				if err != nil {
					errCh <- err
					return
				}
				if len(no) != 21 || no[:2] != "PA" {
					errCh <- errors.New("bad number " + no)
					return
				}
				n, err := strconv.ParseUint(no[2:], 10, 64)
				if err != nil {
					errCh <- err
					return
				}
				if n > uint64(math.MaxInt64) {
					errCh <- errors.New("sign bit set")
					return
				}
				if ((n >> 12) & 0x3FF) != workerID {
					errCh <- errors.New("worker id mismatch")
					return
				}
				if j > 0 && n <= prev {
					errCh <- errors.New("id not strictly increasing")
					return
				}
				got[j] = n
				prev = n
			}
			parts[i] = got
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	seen := make(map[uint64]struct{}, total)
	for _, part := range parts {
		if len(part) != perG {
			t.Fatalf("goroutine produced %d ids", len(part))
		}
		for _, n := range part {
			if _, ok := seen[n]; ok {
				t.Fatalf("duplicate id %d", n)
			}
			seen[n] = struct{}{}
		}
	}
	if len(seen) != total {
		t.Fatalf("unique ids = %d, want %d", len(seen), total)
	}
}

// BASE-05 验收 2：注入回拨 3ms 的时钟，生成成功且耗时 ≥ 3ms；回拨 10ms 返回错误。
func TestClockRollback(t *testing.T) {
	cases := []struct {
		name       string
		rewind     time.Duration
		wantErr    bool
		minElapsed time.Duration
	}{
		{name: "3ms", rewind: 3 * time.Millisecond, minElapsed: 3 * time.Millisecond},
		{name: "10ms", rewind: 10 * time.Millisecond, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &offsetClock{}
			g := mustGenerator(t, 4, clk)
			ctx := context.Background()
			if _, err := g.NewNo(ctx, "PA"); err != nil {
				t.Fatal(err)
			}
			clk.Rewind(tc.rewind)
			start := time.Now()
			_, err := g.NewNo(ctx, "PA")
			elapsed := time.Since(start)
			if tc.wantErr {
				if !errors.Is(err, ErrClockRollback) {
					t.Fatalf("err = %v, want ErrClockRollback", err)
				}
				if strings.Contains(err.Error(), "10004") {
					t.Fatalf("idgen must not return numeric code 10004: %v", err)
				}
				if elapsed >= 5*time.Millisecond {
					t.Fatalf("rollback %s returned in %s, want fail fast", tc.rewind, elapsed)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if elapsed < tc.minElapsed {
				t.Fatalf("elapsed %s, want >= %s", elapsed, tc.minElapsed)
			}
		})
	}
}

// BASE-05 验收 3：NewNo("PA") 长度 21，Parse 结果与生成时间误差 < 1ms。
func TestNewNoPALengthAndParseTime(t *testing.T) {
	// 250µs 用来确认比较的是毫秒精度，而不是把断言放宽到整秒。
	genAt := testEpoch.Add(1700*time.Millisecond + 250*time.Microsecond)
	const workerID = 42
	g := mustGenerator(t, workerID, stubClock{t: genAt})
	no, err := g.NewNo(context.Background(), "PA")
	if err != nil {
		t.Fatal(err)
	}
	if len(no) != 21 {
		t.Fatalf("len(%s) = %d, want 21", no, len(no))
	}
	if no[:2] != "PA" {
		t.Fatalf("prefix = %s", no[:2])
	}
	for _, c := range no[2:] {
		if c < '0' || c > '9' {
			t.Fatalf("non-digit in %s", no)
		}
	}
	parsed, err := Parse(no)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Prefix != "PA" || parsed.WorkerID != workerID {
		t.Fatalf("parsed = %+v", parsed)
	}
	delta := genAt.Sub(parsed.Time)
	if delta < 0 || delta >= time.Millisecond {
		t.Fatalf("parse time delta %s, want >= 0 and < 1ms", delta)
	}
}

func TestClockRollbackBoundary(t *testing.T) {
	cases := []struct {
		name    string
		rewind  time.Duration
		wantErr bool
	}{
		{name: "5ms", rewind: 5 * time.Millisecond},
		{name: "6ms", rewind: 6 * time.Millisecond, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := &offsetClock{}
			g := mustGenerator(t, 1, clk)
			ctx := context.Background()
			if _, err := g.NewNo(ctx, "RF"); err != nil {
				t.Fatal(err)
			}
			clk.Rewind(tc.rewind)
			start := time.Now()
			_, err := g.NewNo(ctx, "RF")
			elapsed := time.Since(start)
			if tc.wantErr {
				if !errors.Is(err, ErrClockRollback) {
					t.Fatalf("err = %v, want ErrClockRollback", err)
				}
				if elapsed >= 5*time.Millisecond {
					t.Fatalf("elapsed %s, want fail fast", elapsed)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if elapsed < tc.rewind {
				t.Fatalf("elapsed %s, want >= %s", elapsed, tc.rewind)
			}
		})
	}
}

func TestRollbackWaitCanBeCancelled(t *testing.T) {
	clk := &frozenRollbackClock{}
	g := mustGenerator(t, 2, clk)
	ctx := context.Background()
	if _, err := g.NewNo(ctx, "TF"); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := g.NewNo(waitCtx, "TF")
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rollback wait ignored cancellation")
	}
}

func TestPrefixTable(t *testing.T) {
	registered := []string{"PA", "RF", "TF", "TX", "FZ", "AC", "EV", "NT", "RB", "RD", "MC"}
	g := mustGenerator(t, 9, stubClock{t: testEpoch.Add(8 * time.Millisecond)})
	ctx := context.Background()
	for _, prefix := range registered {
		no, err := g.NewNo(ctx, prefix)
		if err != nil {
			t.Errorf("NewNo(%s): %v", prefix, err)
			continue
		}
		if len(no) != 21 || !strings.HasPrefix(no, prefix) {
			t.Errorf("NewNo(%s) = %s", prefix, no)
		}
		parsed, err := Parse(no)
		if err != nil {
			t.Errorf("Parse(%s): %v", no, err)
			continue
		}
		if parsed.Prefix != prefix || parsed.WorkerID != 9 {
			t.Errorf("Parse(%s) = %+v", no, parsed)
		}
	}
	rejected := []string{"", "P", "PAX", "pa", "XX", "P1", "12", "TFX"}
	for _, prefix := range rejected {
		if _, err := g.NewNo(ctx, prefix); !errors.Is(err, ErrPrefix) {
			t.Errorf("NewNo(%q) err = %v, want ErrPrefix", prefix, err)
		}
	}
}

func TestParseRejects(t *testing.T) {
	valid := "PA" + "0000000000000000001"
	cases := []struct {
		name string
		no   string
	}{
		{name: "short", no: "PA1"},
		{name: "long", no: valid + "0"},
		{name: "bad prefix", no: "ZZ0000000000000000001"},
		{name: "non digit", no: "PA000000000000000000X"},
		{name: "sign bit", no: "PA9223372036854775808"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse(tc.no); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := Parse("ZZ0000000000000000001"); !errors.Is(err, ErrPrefix) {
		t.Fatalf("bad prefix err = %v", err)
	}
}

func TestBitLayout(t *testing.T) {
	cases := []struct {
		name     string
		at       time.Time
		workerID int
		n        int
		wantMS   int64
		wantSeq  int
	}{
		{name: "epoch worker 0", at: testEpoch, workerID: 0, n: 1, wantMS: 0, wantSeq: 0},
		{name: "epoch plus 1ms worker 1", at: testEpoch.Add(time.Millisecond), workerID: 1, n: 2, wantMS: 1, wantSeq: 0},
		{name: "worker 1023", at: testEpoch.Add(50 * time.Millisecond), workerID: 1023, n: 1, wantMS: 50, wantSeq: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := mustGenerator(t, tc.workerID, stubClock{t: tc.at})
			var prev int64
			for i := 0; i < tc.n; i++ {
				no, err := g.NewNo(context.Background(), "TX")
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := Parse(no)
				if err != nil {
					t.Fatal(err)
				}
				if parsed.WorkerID != tc.workerID || parsed.Sequence != tc.wantSeq+i {
					t.Fatalf("parsed = %+v, want worker %d seq %d", parsed, tc.workerID, tc.wantSeq+i)
				}
				wantTime := testEpoch.Add(time.Duration(tc.wantMS) * time.Millisecond)
				if !parsed.Time.Equal(wantTime) {
					t.Fatalf("time = %s, want %s", parsed.Time, wantTime)
				}
				raw, err := strconv.ParseInt(no[2:], 10, 64)
				if err != nil || raw < 0 {
					t.Fatalf("id %s sign bit or parse: %v", no, err)
				}
				if i > 0 && raw <= prev {
					t.Fatal("sequence did not increase")
				}
				prev = raw
			}
		})
	}
}

func TestMaxTimestampKeepsSignBitClear(t *testing.T) {
	const maxMS = int64(1<<41) - 1
	at := testEpoch.Add(time.Duration(maxMS) * time.Millisecond)
	g := mustGenerator(t, 1023, stubClock{t: at})
	// 填满这一毫秒（序列 0–4095），确认最大值的符号位仍是 0。
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first, err := g.NewNo(ctx, "FZ")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 4095; i++ {
		if _, err := g.NewNo(ctx, "FZ"); err != nil {
			t.Fatalf("seq %d: %v", i, err)
		}
	}
	last, err := g.NewNo(ctx, "FZ")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(last)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.WorkerID != 1023 || parsed.Sequence != 4095 {
		t.Fatalf("parsed = %+v", parsed)
	}
	if !parsed.Time.Equal(at) {
		t.Fatalf("time = %s", parsed.Time)
	}
	raw, err := strconv.ParseInt(last[2:], 10, 64)
	if err != nil || raw < 0 {
		t.Fatalf("last id %s: %v", last, err)
	}
	firstRaw, err := strconv.ParseInt(first[2:], 10, 64)
	if err != nil || firstRaw < 0 || firstRaw >= raw {
		t.Fatalf("first %s last %s", first, last)
	}
}

func TestTimestampRejectsOutOfRange(t *testing.T) {
	const maxMS = int64(1<<41) - 1
	cases := []struct {
		name string
		at   time.Time
	}{
		{name: "before epoch", at: testEpoch.Add(-time.Millisecond)},
		{name: "one nanosecond before epoch", at: testEpoch.Add(-time.Nanosecond)},
		{name: "overflow 41 bits", at: testEpoch.Add(time.Duration(maxMS)*time.Millisecond + time.Millisecond)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := mustGenerator(t, 1, stubClock{t: tc.at})
			_, err := g.NewNo(context.Background(), "AC")
			if !errors.Is(err, ErrTimeRange) {
				t.Fatalf("err = %v, want ErrTimeRange", err)
			}
		})
	}
}

func TestSequenceWaitsForNextMillisecond(t *testing.T) {
	base := testEpoch.Add(80 * time.Millisecond)
	clk := &manualClock{t: base}
	g := mustGenerator(t, 6, clk)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var prev uint64
	for i := 0; i <= 4095; i++ {
		no, err := g.NewNo(ctx, "EV")
		if err != nil {
			t.Fatalf("id %d: %v", i, err)
		}
		parsed, err := Parse(no)
		if err != nil {
			t.Fatal(err)
		}
		if !parsed.Time.Equal(base) || parsed.Sequence != i || parsed.WorkerID != 6 {
			t.Fatalf("id %d parsed = %+v", i, parsed)
		}
		raw, err := strconv.ParseUint(no[2:], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && raw <= prev {
			t.Fatal("not increasing inside the millisecond")
		}
		prev = raw
	}
	errCh := make(chan error, 1)
	var no string
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	go func() {
		var err error
		no, err = g.NewNo(waitCtx, "EV")
		errCh <- err
	}()
	clk.Set(base.Add(time.Millisecond))
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sequence wait did not observe the injected clock")
	}
	parsed, err := Parse(no)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Time.Equal(base.Add(time.Millisecond)) || parsed.Sequence != 0 || parsed.WorkerID != 6 {
		t.Fatalf("after wait parsed = %+v", parsed)
	}
}

func TestSequenceWaitCanBeCancelled(t *testing.T) {
	clk := stubClock{t: testEpoch.Add(20 * time.Millisecond)}
	g := mustGenerator(t, 3, clk)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i <= 4095; i++ {
		if _, err := g.NewNo(ctx, "NT"); err != nil {
			t.Fatalf("id %d: %v", i, err)
		}
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := g.NewNo(waitCtx, "NT")
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sequence wait ignored cancellation")
	}
}

func TestWorkerIDFromEnv(t *testing.T) {
	cases := []struct {
		name    string
		set     bool
		value   string
		want    int
		wantErr bool
	}{
		{name: "unset", wantErr: true},
		{name: "empty", set: true, value: "", wantErr: true},
		{name: "zero", set: true, value: "0", want: 0},
		{name: "max", set: true, value: "1023", want: 1023},
		{name: "padded", set: true, value: "007", want: 7},
		{name: "plus", set: true, value: "+8", want: 8},
		{name: "over", set: true, value: "1024", wantErr: true},
		{name: "negative", set: true, value: "-1", wantErr: true},
		{name: "text", set: true, value: "abc", wantErr: true},
		{name: "fraction", set: true, value: "1.5", wantErr: true},
		{name: "space", set: true, value: " 1", wantErr: true},
		{name: "huge", set: true, value: "999999999999999999999", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WORKER_ID", "sentinel")
			if tc.set {
				t.Setenv("WORKER_ID", tc.value)
			} else if err := os.Unsetenv("WORKER_ID"); err != nil {
				t.Fatal(err)
			}
			got, err := WorkerIDFromEnv()
			if tc.wantErr {
				if !errors.Is(err, ErrWorkerID) {
					t.Fatalf("err = %v, want ErrWorkerID", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("worker = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestNewRejectsWorkerAndNilClock(t *testing.T) {
	if _, err := New(-1, stubClock{t: testEpoch}); !errors.Is(err, ErrWorkerID) {
		t.Fatalf("worker -1 err = %v", err)
	}
	if _, err := New(1024, stubClock{t: testEpoch}); !errors.Is(err, ErrWorkerID) {
		t.Fatalf("worker 1024 err = %v", err)
	}
	if _, err := New(0, nil); err == nil {
		t.Fatal("nil clock should be rejected")
	}
	g := mustGenerator(t, 0, stubClock{t: testEpoch})
	// 故意传入 nil，确认不会 panic。SA1012 禁止正常调用这么写。
	if _, err := g.NewNo(nil, "MC"); err == nil { //nolint:staticcheck
		t.Fatal("nil context should be rejected")
	}
}

// offsetClock 在 Rewind 之前返回真实时间。
// Rewind 后的第一次 Now 返回上一次读数减去回拨量，之后跟着真实时间走。
// 回拨锚定在上一次读数上，避免两次调用之间流逝的时间把 3ms 等待吃短。
type offsetClock struct {
	mu         sync.Mutex
	last       time.Time
	hasLast    bool
	rewind     time.Duration
	anchorWall time.Time
	anchorRead time.Time
	tracking   bool
}

func (c *offsetClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rewind > 0 {
		base := time.Now()
		if c.hasLast {
			base = c.last
		}
		c.anchorRead = base.Add(-c.rewind)
		c.anchorWall = time.Now()
		c.rewind = 0
		c.tracking = true
		c.last = c.anchorRead
		c.hasLast = true
		return c.anchorRead
	}
	if c.tracking {
		t := c.anchorRead.Add(time.Since(c.anchorWall))
		c.last = t
		return t
	}
	t := time.Now()
	c.last = t
	c.hasLast = true
	return t
}

func (c *offsetClock) Rewind(d time.Duration) {
	c.mu.Lock()
	c.rewind = d
	c.mu.Unlock()
}

// frozenRollbackClock 第一次返回真实时间，之后始终停在那次读数减去 3ms。
type frozenRollbackClock struct {
	mu    sync.Mutex
	first time.Time
	calls int
}

func (c *frozenRollbackClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls == 1 {
		c.first = time.Now()
		return c.first
	}
	return c.first.Add(-3 * time.Millisecond)
}

type stubClock struct {
	t time.Time
}

func (c stubClock) Now() time.Time { return c.t }

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) Set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}
