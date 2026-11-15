package base

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取当前工作目录：%v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("从 %s 向上未找到 go.mod", dir)
		}
		dir = parent
	}
}

func repoPath(t *testing.T, elems ...string) string {
	t.Helper()
	parts := append([]string{repoRoot(t)}, elems...)
	return filepath.Join(parts...)
}

func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(repoPath(t, path)) //nolint:gosec // path is a test constant.
	if err != nil {
		t.Fatalf("读取 %s：%v", path, err)
	}
	return string(content)
}

func repoCommand(t *testing.T, ctx context.Context, name string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // command and args are controlled by tests.
	cmd.Dir = repoRoot(t)
	return cmd
}
