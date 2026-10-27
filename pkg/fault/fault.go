// Package fault 按 FAULT_POINTS 向本进程发 SIGKILL，模拟 kill -9。
//
// 空配置时 Point 什么都不做，也不碰标记目录。带 :once 的故障点先写标记文件，
// 同一目录上的新进程看到文件后不再发信号。发信号函数和标记目录只能通过选项替换，
// 给测试用，不是配置项。
package fault

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// defaultMarkerDir 是 :once 的标记目录。不是配置项。
// chaos 覆盖文件把同一个命名卷挂到这个路径，重启后标记还在。
const defaultMarkerDir = "/var/run/fault"

// registered 是需求 BASE-15 第 6 条的名称，大小写敏感。
var registered = map[string]struct{}{
	"pay.after_tx1":                {},
	"callback.after_commit":        {},
	"outbox.after_send":            {},
	"ledger.after_commit":          {},
	"ledger.after_batch_commit":    {},
	"refund.after_freeze":          {},
	"transfer.after_try":           {},
	"transfer.after_decision":      {},
	"transfer.after_payer_confirm": {},
}

type pointMode uint8

const (
	modeSignal pointMode = iota + 1
	modeOnce
)

// Option 替换测试用的发信号函数或标记目录。不读取环境变量。
type Option func(*settings)

type settings struct {
	kill func()
	dir  string
}

// WithKill 替换向本进程发 SIGKILL 的函数。fn 为 nil 时保留生产实现。
func WithKill(fn func()) Option {
	return func(s *settings) {
		if fn != nil {
			s.kill = fn
		}
	}
}

// WithMarkerDir 替换 :once 的标记目录。dir 为空时保留默认目录。
func WithMarkerDir(dir string) Option {
	return func(s *settings) {
		if dir != "" {
			s.dir = dir
		}
	}
}

type runtimeState struct {
	enabled map[string]pointMode
	kill    func()
	dir     string
}

var (
	mu      sync.Mutex
	current runtimeState
)

// Init 用 raw 替换本进程启用的故障点。raw 为空则清空。
// 未登记的名称、空项、:once 以外的后缀、重复名称都返回错误；失败时不改已有状态。
// opts 只给测试替换发信号函数和标记目录。
func Init(raw string, opts ...Option) error {
	cfg := settings{kill: killProcess, dir: defaultMarkerDir}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	enabled, err := parsePoints(raw)
	if err != nil {
		return err
	}
	mu.Lock()
	current = runtimeState{enabled: enabled, kill: cfg.kill, dir: cfg.dir}
	mu.Unlock()
	return nil
}

// Point 在 name 已启用时向本进程发 SIGKILL。
// 未启用、空配置或尚未 Init 时立即返回，不创建、不读取标记目录。
// ctx 取消不跳过已经启用的信号；ctx 为 nil 也不读取，避免 panic。
func Point(ctx context.Context, name string) {
	mu.Lock()
	mode := current.enabled[name]
	kill := current.kill
	dir := current.dir
	mu.Unlock()
	switch mode {
	case modeSignal:
		if kill != nil {
			kill()
		}
	case modeOnce:
		signalOnce(dir, name, kill)
	default:
		return
	}
	// 参数保留给调用方传递调用点的上下文。取消和 nil 都不改变上面的分支。
	_ = ctx
}

func parsePoints(raw string) (map[string]pointMode, error) {
	if raw == "" {
		return map[string]pointMode{}, nil
	}
	parts := strings.Split(raw, ",")
	enabled := make(map[string]pointMode, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			// %q 让空项的错误里出现 ""，调用方能看出是空项。
			return nil, fmt.Errorf("fault: invalid point %q", item)
		}
		name, mode, err := classify(item)
		if err != nil {
			return nil, err
		}
		if _, exists := enabled[name]; exists {
			return nil, fmt.Errorf("fault: duplicate point %q", name)
		}
		enabled[name] = mode
	}
	return enabled, nil
}

func classify(item string) (string, pointMode, error) {
	name := item
	mode := modeSignal
	if strings.HasSuffix(name, onceSuffix) {
		name = strings.TrimSuffix(name, onceSuffix)
		mode = modeOnce
	}
	if _, ok := registered[name]; !ok {
		return "", 0, fmt.Errorf("fault: invalid point %q", item)
	}
	return name, mode, nil
}

const onceSuffix = ":once"

func signalOnce(dir, name string, kill func()) {
	if kill == nil {
		return
	}
	// 没有挂载时本地演练也能写。空配置不会走到这里。
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // G301: 需求规定标记目录权限 0755。
		return
	}
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644) //nolint:gosec // 路径是标记目录加已登记的故障点名称。
	if err != nil {
		return
	}
	// 写失败则不发信号，避免没留下标记却已经杀掉，重启后又杀一次。
	if err := f.Sync(); err != nil {
		_ = f.Close()
		removeMarker(path)
		return
	}
	if err := f.Close(); err != nil {
		removeMarker(path)
		return
	}
	kill()
}

func removeMarker(path string) {
	_ = os.Remove(path) //nolint:gosec // G703: 路径是标记目录加已登记的故障点名称。
}

func killProcess() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
}
