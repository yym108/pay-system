package testkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// sideRE 限制屏障文件名，避免把调用方输入拼进路径。
var sideRE = regexp.MustCompile(`^[a-z][a-z0-9]{0,15}$`)

// Pair 让同一次 go test 里的两个包在指定阶段汇合。
// 目录用父进程 pid 区分，上一次运行留下的文件不会被当成这一次的对端。
type Pair struct {
	id  string
	dir string
}

// NewPair 登记本包的名字，例如 left。对端要使用同一个父进程启动。
func NewPair(id string) (*Pair, error) {
	if !sideRE.MatchString(id) {
		return nil, fmt.Errorf("testkit: side %q is invalid", id)
	}
	dir := filepath.Join(os.TempDir(), "pay-system-base11", strconv.Itoa(os.Getppid()))
	return &Pair{id: id, dir: dir}, nil
}

// Barrier 先写下本侧的阶段文件，再等待对端写下同一阶段。
// 两侧都要调用。对端一直不到达时返回 ctx 的错误。
func (p *Pair) Barrier(ctx context.Context, stage, peer string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || !sideRE.MatchString(stage) || !sideRE.MatchString(peer) {
		return errors.New("testkit: barrier name is invalid")
	}
	if err := os.MkdirAll(p.dir, 0o750); err != nil {
		return err
	}
	own := filepath.Join(p.dir, stage+"-"+p.id)
	if err := os.WriteFile(own, []byte("ok\n"), 0o600); err != nil {
		return err
	}
	target := filepath.Join(p.dir, stage+"-"+peer)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := os.Stat(target)
		if err == nil {
			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("testkit: wait %s-%s: %w", stage, peer, ctx.Err())
		case <-ticker.C:
		}
	}
}
