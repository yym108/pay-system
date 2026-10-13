package base

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// BASE-01 验收 1：7 个可执行程序均有位于 cmd/<name>/main.go 的入口。
func TestBASE01CommandEntrypoints(t *testing.T) {
	commands := []string{"gateway", "payment", "account", "recon", "channel-mock", "merchant-demo", "paycli"}
	for _, command := range commands {
		path := filepath.Join("cmd", command, "main.go")
		content, err := os.ReadFile(path) //nolint:gosec // path is assembled from the fixed command list above.
		if err != nil {
			t.Errorf("读取 %s：%v", path, err)
			continue
		}
		text := string(content)
		if !strings.Contains(text, "package main") || !regexp.MustCompile(`func\s+main\s*\(`).MatchString(text) {
			t.Errorf("%s 必须包含 package main 和 func main", path)
		}
	}
}

// BASE-01 规则 1：模块路径和 Go 版本与项目基线一致。
func TestBASE01ModuleMetadata(t *testing.T) {
	content, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("读取 go.mod：%v", err)
	}
	text := string(content)
	for _, required := range []string{"module github.com/yym108/pay-system", "go 1.27.0", "github.com/bytedance/sonic v1.15.4"} {
		if !strings.Contains(text, required) {
			t.Errorf("go.mod 缺少 %q", required)
		}
	}
}

// BASE-01 验收 1：Makefile 提供需求规定的构建、检查和后续流程入口。
func TestBASE01MakeTargets(t *testing.T) {
	content, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("读取 Makefile：%v", err)
	}
	text := string(content)
	for _, target := range []string{"gen", "build", "lint", "test", "test-integration", "cover", "up", "down", "migrate", "seed", "e2e", "chaos", "load"} {
		if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(target) + `:`).MatchString(text) {
			t.Errorf("Makefile 缺少目标 %s", target)
		}
	}
}

// BASE-01 验收 1：make build 产出全部 7 个可执行程序。
func TestBASE01BuildOutputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "make", "build").CombinedOutput(); err != nil {
		t.Fatalf("make build 失败：%v\n%s", err, output)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll("bin")
	})

	for _, command := range []string{"gateway", "payment", "account", "recon", "channel-mock", "merchant-demo", "paycli"} {
		path := filepath.Join("bin", command)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("构建产物 %s 不存在：%v", path, err)
			continue
		}
		if info.Mode().Perm()&0o111 == 0 {
			t.Errorf("构建产物 %s 不可执行", path)
		}
	}
}

// BASE-01 验收 2：静态检查启用规定的安全、错误和分层约束规则。
func TestBASE01LintPolicy(t *testing.T) {
	content, err := os.ReadFile(".golangci.yml")
	if err != nil {
		t.Fatalf("读取 .golangci.yml：%v", err)
	}
	text := string(content)
	for _, linter := range []string{"errcheck", "govet", "staticcheck", "gosec", "errorlint", "bodyclose", "sqlclosecheck", "rowserrcheck", "noctx", "contextcheck", "depguard", "forbidigo"} {
		if !strings.Contains(text, linter) {
			t.Errorf(".golangci.yml 未启用 %s", linter)
		}
	}
	if !strings.Contains(text, "time") || !strings.Contains(text, "Now") {
		t.Error(".golangci.yml 未配置对 time.Now() 的拦截")
	}
	for _, required := range []string{"files:", "service_account:", "service_payment:", "service_channel_mock:", "gateway:", "job:", "pkg:", "cmd_account:", "cmd_payment:", "cmd_channel_mock:", "cmd_recon:"} {
		if !strings.Contains(text, required) {
			t.Errorf(".golangci.yml 未配置按文件范围的 depguard 规则 %s", required)
		}
	}
}

// BASE-01 验收 3：Dockerfile 支持按 APP 构建，并以非 root 用户运行最小运行镜像。
func TestBASE01Dockerfile(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("deploy", "Dockerfile"))
	if err != nil {
		t.Fatalf("读取 deploy/Dockerfile：%v", err)
	}
	text := string(content)
	for _, required := range []string{"FROM", "golang:1.27.2-alpine@sha256:", "alpine:3.22.1@sha256:", "ARG APP", "USER nonroot"} {
		if !strings.Contains(text, required) {
			t.Errorf("deploy/Dockerfile 缺少 %q", required)
		}
	}
	if strings.Count(text, "FROM ") < 2 {
		t.Error("deploy/Dockerfile 必须是多阶段构建")
	}
}

// BASE-01 规则 6：构建产物、密钥、覆盖率和 IDE 文件必须被忽略。
func TestBASE01Gitignore(t *testing.T) {
	content, err := os.ReadFile(".gitignore")
	if err != nil {
		t.Fatalf("读取 .gitignore：%v", err)
	}
	text := string(content)
	for _, required := range []string{"/bin/", ".env", "coverage.", ".idea/"} {
		if !strings.Contains(text, required) {
			t.Errorf(".gitignore 缺少 %q", required)
		}
	}
}
