package clock

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// BASE-07 验收 1：Fake.Advance(10s) 后 Now() 精确前进 10 秒。
func TestFakeAdvanceTenSeconds(t *testing.T) {
	t.Parallel()
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	start := time.Date(2026, 10, 15, 1, 2, 3, 4, loc)
	f := &Fake{}
	f.Set(start)
	before := f.Now()
	f.Advance(10 * time.Second)
	got := f.Now()
	want := before.Add(10 * time.Second)
	if !got.Equal(want) || !want.Equal(start.Add(10*time.Second)) {
		t.Fatalf("Now() = %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
	if got.Sub(before) != 10*time.Second || got.UnixNano()-before.UnixNano() != int64(10*time.Second) {
		t.Fatalf("advanced %s (unix nano delta %d), want 10s", got.Sub(before), got.UnixNano()-before.UnixNano())
	}
	if got.Location().String() != before.Location().String() {
		t.Fatalf("location = %s, want %s", got.Location(), before.Location())
	}
}

// BASE-07 规则：Fake 的 Set、Advance、Now 并发安全。
func TestFakeConcurrentAdvance(t *testing.T) {
	t.Parallel()
	const n = 100
	start := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	f := &Fake{}
	f.Set(start)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			f.Advance(time.Second)
			_ = f.Now()
		}()
	}
	wg.Wait()
	got := f.Now()
	if got.Sub(start) != time.Duration(n)*time.Second {
		t.Fatalf("after %d concurrent Advance(1s), delta = %s", n, got.Sub(start))
	}
}

// BASE-07 验收 2：Real.Now().Location() 为 Asia/Shanghai，且不调用 LoadLocation，偏移为 +8 小时。
func TestRealLocationAsiaShanghai(t *testing.T) {
	t.Parallel()
	assertNoLoadLocation(t)

	before := time.Now()
	got := Real{}.Now()
	after := time.Now()
	if got.Location() == nil || got.Location().String() != "Asia/Shanghai" {
		t.Fatalf("Location() = %v, want Asia/Shanghai", got.Location())
	}
	if got.Before(before.Add(-5*time.Second)) || got.After(after.Add(5*time.Second)) {
		t.Fatalf("Real.Now() = %s, outside %s .. %s", got.Format(time.RFC3339Nano), before.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano))
	}

	utc := got.UTC()
	if !got.Equal(utc) {
		t.Fatal("Real.Now() and its UTC form are not the same instant")
	}
	if wallDelta(got, utc) != 8*time.Hour {
		t.Fatalf("wall clock ahead of UTC by %s, want 8h", wallDelta(got, utc))
	}
	_, offset := got.Zone()
	if offset != 8*60*60 {
		t.Fatalf("Zone offset = %d, want %d", offset, 8*60*60)
	}

	// 1991-07-01 处于中国夏令时。LoadLocation("Asia/Shanghai") 的偏移是 +9 小时；
	// 固定 +8 的地点必须仍是 +8，才能证明这里用的不是 zoneinfo。
	iana, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("LoadLocation(Asia/Shanghai) = %v", err)
	}
	summer := time.Date(1991, time.July, 1, 12, 0, 0, 0, time.UTC)
	_, ianaOffset := summer.In(iana).Zone()
	_, realOffset := summer.In(got.Location()).Zone()
	if ianaOffset == 8*60*60 {
		t.Fatal("IANA Asia/Shanghai no longer observes DST on 1991-07-01; the comparison no longer proves a fixed zone")
	}
	if realOffset != 8*60*60 || realOffset == ianaOffset {
		t.Fatalf("1991-07-01 offset = %d, IANA = %d, want fixed +8 and different from LoadLocation", realOffset, ianaOffset)
	}
}

// wallDelta 是同一瞬间在 loc 与 UTC 上的墙钟差。跨日时仍把日期算进去。
func wallDelta(loc, utc time.Time) time.Duration {
	civil := func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	}
	return civil(loc).Sub(civil(utc))
}

// assertNoLoadLocation 扫描本包非测试源码，确认没有 LoadLocation 调用，也没有 import time/tzdata。
func assertNoLoadLocation(t *testing.T) {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	names, err := filepath.Glob(filepath.Join(filepath.Dir(filename), "*.go"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	var sources []string
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		sources = append(sources, name)
	}
	if len(sources) == 0 {
		t.Fatal("no production Go files in pkg/clock")
	}
	fset := token.NewFileSet()
	for _, name := range sources {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range file.Imports {
			if imp.Path != nil && imp.Path.Value == `"time/tzdata"` {
				t.Fatalf("%s imports time/tzdata", name)
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if node.Sel != nil && node.Sel.Name == "LoadLocation" {
					t.Fatalf("%s calls LoadLocation", name)
				}
			case *ast.Ident:
				if node.Name == "LoadLocation" {
					t.Fatalf("%s references LoadLocation", name)
				}
			}
			return true
		})
	}
}
