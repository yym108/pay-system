package fault

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	switch os.Getenv("PAY_FAULT_CHILD") {
	case "startup":
		childStartup()
	case "kill":
		childKill()
	}
	os.Exit(m.Run())
}

func childStartup() {
	entered := os.Getenv("PAY_FAULT_ENTERED")
	err := Init(os.Getenv("FAULT_POINTS"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(entered, []byte("entered"), 0o600); err != nil { //nolint:gosec // 路径由父进程传入，只标记是否进入业务。
		fmt.Fprintf(os.Stderr, "enter business: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func childKill() {
	name := os.Getenv("FAULT_POINTS")
	if err := Init(name); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	Point(context.Background(), name)
	after := os.Getenv("PAY_FAULT_AFTER")
	if err := os.WriteFile(after, []byte("after"), 0o600); err != nil { //nolint:gosec // 路径由父进程传入，只标记 Point 之后有没有执行。
		fmt.Fprintf(os.Stderr, "after point: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func childEnv(overrides ...string) []string {
	drop := make(map[string]struct{}, len(overrides))
	for _, ov := range overrides {
		key, _, ok := strings.Cut(ov, "=")
		if ok {
			drop[key] = struct{}{}
		}
	}
	env := make([]string, 0, len(os.Environ())+len(overrides))
	for _, item := range os.Environ() {
		key, _, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		if _, skip := drop[key]; skip {
			continue
		}
		env = append(env, item)
	}
	return append(env, overrides...)
}

func runChild(t *testing.T, env []string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), os.Args[0]) //nolint:gosec // 重新执行当前测试二进制，不带额外参数。
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return stderr.String(), err
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("child did not exit")
		return "", nil
	}
}

func resetFault(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if err := Init(""); err != nil {
			t.Errorf("reset fault: %v", err)
		}
	})
}

// BASE-15 验收 1：替换发信号函数后，FAULT_POINTS=pay.after_tx1 时 Point(ctx, "pay.after_tx1") 发信号 1 次，Point(ctx, "callback.after_commit") 发信号 0 次。
func TestPointSignalsOnlyEnabledName(t *testing.T) {
	if defaultMarkerDir != "/var/run/fault" {
		t.Fatalf("default marker dir = %q", defaultMarkerDir)
	}
	dir := filepath.Join(t.TempDir(), "absent")
	var n int
	err := Init("pay.after_tx1", WithKill(func() { n++ }), WithMarkerDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	resetFault(t)

	ctx := context.Background()
	Point(ctx, "pay.after_tx1")
	if n != 1 {
		t.Fatalf("pay.after_tx1 signals = %d, want 1", n)
	}
	Point(ctx, "callback.after_commit")
	if n != 1 {
		t.Fatalf("callback.after_commit signaled, signals = %d", n)
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatalf("non-once Point created marker dir: %v", statErr)
	}

	Point(nil, "pay.after_tx1") //nolint:staticcheck // nil ctx 是约定行为，不能 panic，也不能跳过信号。
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	Point(canceled, "pay.after_tx1")
	if n != 3 {
		t.Fatalf("nil or canceled ctx skipped the signal, signals = %d", n)
	}
}

// BASE-15 验收 2：FAULT_POINTS=pay.after_tx1:once、标记目录为临时目录。第一次调用先写标记文件再发信号；同一目录再次 Init 后不再发信号。
func TestPointOnceMarkerSurvivesReinit(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "pay.after_tx1")
	var n int
	kill := func() {
		if _, err := os.Stat(marker); err != nil {
			t.Errorf("marker missing when signal fires: %v", err)
		}
		n++
	}
	if err := Init("pay.after_tx1:once", WithKill(kill), WithMarkerDir(dir)); err != nil {
		t.Fatal(err)
	}
	resetFault(t)

	Point(context.Background(), "pay.after_tx1")
	if n != 1 {
		t.Fatalf("signals = %d, want 1", n)
	}
	info, err := os.Stat(marker)
	if err != nil {
		t.Fatal(err)
	}
	if info.IsDir() {
		t.Fatal("marker is a directory")
	}

	if err := Init("pay.after_tx1:once", WithKill(kill), WithMarkerDir(dir)); err != nil {
		t.Fatal(err)
	}
	Point(context.Background(), "pay.after_tx1")
	if n != 1 {
		t.Fatalf("signals after re-init = %d, want 1", n)
	}
}

// BASE-15 验收 3：FAULT_POINTS 为空时任何名称都不发信号，临时标记目录中没有新建文件。
func TestPointEmptyConfigLeavesMarkerDirUntouched(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	var n int
	if err := Init("", WithKill(func() { n++ }), WithMarkerDir(dir)); err != nil {
		t.Fatal(err)
	}
	resetFault(t)

	for _, name := range []string{
		"pay.after_tx1",
		"callback.after_commit",
		"outbox.after_send",
		"ledger.after_commit",
		"ledger.after_batch_commit",
		"refund.after_freeze",
		"transfer.after_try",
		"transfer.after_decision",
		"transfer.after_payer_confirm",
		"pay.after_tx",
	} {
		Point(context.Background(), name)
	}
	if n != 0 {
		t.Fatalf("signals = %d, want 0", n)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("empty config touched marker dir: %v", err)
	}
}

// BASE-15 验收 4：三个非法配置各自返回错误且含该项；带空格的 :once 加另一个合法名称则成功。
func TestInitValidatesPoints(t *testing.T) {
	resetFault(t)
	cases := []struct {
		raw  string
		ok   bool
		item string
	}{
		{raw: "pay.after_tx", item: "pay.after_tx"},
		{raw: "pay.after_tx1,,callback.after_commit", item: `""`},
		{raw: "pay.after_tx1:twice", item: "pay.after_tx1:twice"},
		{raw: "pay.after_tx1:once, callback.after_commit", ok: true},
	}
	for _, tc := range cases {
		dir := t.TempDir()
		var n int
		err := Init(tc.raw, WithKill(func() { n++ }), WithMarkerDir(dir))
		if tc.ok {
			if err != nil {
				t.Fatalf("Init(%q) err = %v", tc.raw, err)
			}
			Point(context.Background(), "callback.after_commit")
			Point(context.Background(), "pay.after_tx1")
			if n != 2 {
				t.Fatalf("Init(%q) signals = %d, want 2", tc.raw, n)
			}
			if _, statErr := os.Stat(filepath.Join(dir, "pay.after_tx1")); statErr != nil {
				t.Fatalf("once marker: %v", statErr)
			}
			continue
		}
		if err == nil {
			t.Fatalf("Init(%q) err = nil", tc.raw)
		}
		if !strings.Contains(err.Error(), tc.item) {
			t.Fatalf("Init(%q) err = %q, want item %s", tc.raw, err.Error(), tc.item)
		}
	}
}

// BASE-15 验收 5：FAULT_POINTS=pay.after_tx 的子进程在启动阶段以非 0 退出，stderr 含该项，没有进入业务。
func TestSubprocessUnknownPointExitsBeforeBusiness(t *testing.T) {
	entered := filepath.Join(t.TempDir(), "entered")
	stderr, err := runChild(t, childEnv(
		"PAY_FAULT_CHILD=startup",
		"FAULT_POINTS=pay.after_tx",
		"PAY_FAULT_ENTERED="+entered,
	))
	if err == nil {
		t.Fatal("exit code = 0, want non-zero")
	}
	if !strings.Contains(stderr, "pay.after_tx") {
		t.Fatalf("stderr = %q, err = %v", stderr, err)
	}
	if _, statErr := os.Stat(entered); !os.IsNotExist(statErr) {
		t.Fatalf("entered-business marker exists: %v", statErr)
	}
}

// BASE-15 验收 6：子进程执行到 Point 时被 SIGKILL 结束，父进程看到 signal: killed，Point 之后的标记文件不存在。
func TestSubprocessPointSIGKILLSkipsFollowingWrite(t *testing.T) {
	after := filepath.Join(t.TempDir(), "after")
	stderr, err := runChild(t, childEnv(
		"PAY_FAULT_CHILD=kill",
		"FAULT_POINTS=pay.after_tx1",
		"PAY_FAULT_AFTER="+after,
	))
	if err == nil {
		t.Fatal("child survived Point")
	}
	if !strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("err = %v, stderr = %q", err, stderr)
	}
	if _, statErr := os.Stat(after); !os.IsNotExist(statErr) {
		t.Fatalf("statement after Point wrote %s: %v", after, statErr)
	}
}

func TestInitRejectsDuplicateName(t *testing.T) {
	resetFault(t)
	for _, raw := range []string{
		"pay.after_tx1,pay.after_tx1",
		"pay.after_tx1:once,pay.after_tx1",
		"pay.after_tx1,pay.after_tx1:once",
	} {
		err := Init(raw)
		if err == nil || !strings.Contains(err.Error(), "pay.after_tx1") {
			t.Fatalf("Init(%q) err = %v", raw, err)
		}
	}
}

func TestInitRejectsCaseAndUnknownSuffix(t *testing.T) {
	resetFault(t)
	for _, raw := range []string{
		"Pay.after_tx1",
		"pay.after_tx1:ONCE",
		"pay.after_tx1:once:once",
		" pay.after_tx ",
	} {
		err := Init(raw)
		if err == nil {
			t.Fatalf("Init(%q) err = nil", raw)
		}
		if !strings.Contains(err.Error(), strings.TrimSpace(raw)) {
			t.Fatalf("Init(%q) err = %q", raw, err.Error())
		}
	}
}

func TestInitAcceptsAllRegisteredNames(t *testing.T) {
	names := []string{
		"pay.after_tx1",
		"callback.after_commit",
		"outbox.after_send",
		"ledger.after_commit",
		"ledger.after_batch_commit",
		"refund.after_freeze",
		"transfer.after_try",
		"transfer.after_decision",
		"transfer.after_payer_confirm",
	}
	var n int
	if err := Init(strings.Join(names, ","), WithKill(func() { n++ })); err != nil {
		t.Fatal(err)
	}
	resetFault(t)
	for _, name := range names {
		Point(context.Background(), name)
	}
	if n != len(names) {
		t.Fatalf("signals = %d, want %d", n, len(names))
	}
}

func TestPointOnceCreatesMissingDirAndSkipsSignalOnWriteError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nested")
	var n int
	if err := Init("ledger.after_commit:once", WithKill(func() { n++ }), WithMarkerDir(missing)); err != nil {
		t.Fatal(err)
	}
	resetFault(t)
	Point(context.Background(), "ledger.after_commit")
	if n != 1 {
		t.Fatalf("signals = %d, want 1", n)
	}
	if _, err := os.Stat(filepath.Join(missing, "ledger.after_commit")); err != nil {
		t.Fatal(err)
	}

	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Init("ledger.after_commit:once", WithKill(func() { n++ }), WithMarkerDir(blocked)); err != nil {
		t.Fatal(err)
	}
	Point(context.Background(), "ledger.after_commit")
	if n != 1 {
		t.Fatalf("write failure signaled, signals = %d", n)
	}
}

func TestInitErrorDoesNotReplaceEnabledSet(t *testing.T) {
	var n int
	if err := Init("outbox.after_send", WithKill(func() { n++ })); err != nil {
		t.Fatal(err)
	}
	resetFault(t)
	if err := Init("pay.after_tx"); err == nil {
		t.Fatal("invalid Init err = nil")
	}
	Point(context.Background(), "outbox.after_send")
	if n != 1 {
		t.Fatalf("failed Init replaced the enabled set, signals = %d", n)
	}
}

func TestPointNotEnabledDoesNotWrite(t *testing.T) {
	dir := t.TempDir()
	var n int
	if err := Init("refund.after_freeze", WithKill(func() { n++ }), WithMarkerDir(dir)); err != nil {
		t.Fatal(err)
	}
	resetFault(t)
	Point(context.Background(), "transfer.after_try")
	if n != 0 {
		t.Fatalf("signals = %d", n)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unexpected marker entries: %v", entries)
	}
}

func TestSubprocessErrorIsExit(t *testing.T) {
	stderr, err := runChild(t, childEnv(
		"PAY_FAULT_CHILD=startup",
		"FAULT_POINTS=pay.after_tx1:twice",
		"PAY_FAULT_ENTERED="+filepath.Join(t.TempDir(), "entered"),
	))
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("err = %v, stderr = %q", err, stderr)
	}
	if strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("invalid config was killed: %v", err)
	}
	if !strings.Contains(stderr, "pay.after_tx1:twice") {
		t.Fatalf("stderr = %q", stderr)
	}
}
