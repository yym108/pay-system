package base

import (
	"encoding/base64"
	"os"
	"regexp"
	"strings"
	"testing"
)

// base02Topics 与 docs/05-api.md §11.1 一致。集成测试复用这份列表。
var base02Topics = []string{
	"pay.payment.succeeded",
	"pay.refund.succeeded",
	"pay.refund.failed",
	"pay.transfer.succeeded",
	"pay.transfer.failed",
	"pay.dlq",
}

// BASE-02 规则 1、2、5、6：compose 声明锁定镜像、obs/apps profile 和本机端口。
func TestBASE02ComposeDeclaresInfra(t *testing.T) {
	text := readRepoFile(t, "deploy/docker-compose.yml")
	for _, required := range []string{
		"image: mysql:8.4.11",
		"image: redis:8.10.2",
		"image: apache/kafka:4.3.1",
		"image: migrate/migrate:v4.20.1",
		"image: prom/prometheus:v3.15.0",
		"image: jaegertracing/jaeger:2.22.0",
		"127.0.0.1:3306:3306",
		"127.0.0.1:6379:6379",
		"127.0.0.1:9094:9094",
		"127.0.0.1:9090:9090",
		"127.0.0.1:16686:16686",
		"127.0.0.1:4317:4317",
		"127.0.0.1:8090:8090",
		"127.0.0.1:6060:6060",
		"8080:8080",
		"9080:9080",
		"9100:9100",
		"profiles:",
		"- obs",
		"- apps",
		"service_completed_successfully",
		"KAFKA_AUTO_CREATE_TOPICS_ENABLE",
		"KAFKA_HEAP_OPTS",
		"--innodb-buffer-pool-size=256M",
		"--max-connections=500",
		"--default-time-zone=+08:00",
		"exit 1",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("deploy/docker-compose.yml 缺少 %q", required)
		}
	}
}

// BASE-02 验收 5：kafka-init 脚本创建 §11.1 的 topic，3 分区、副本数 1。
func TestBASE02TopicScript(t *testing.T) {
	text := readRepoFile(t, "deploy/kafka/init-topics.sh")
	if !strings.Contains(text, "--partitions 3") || !strings.Contains(text, "--replication-factor 1") {
		t.Fatal("init-topics.sh 必须指定 3 分区和副本数 1")
	}
	if !strings.Contains(text, "--if-not-exists") {
		t.Error("init-topics.sh 应可重复执行")
	}
	for _, topic := range base02Topics {
		if !strings.Contains(text, topic) {
			t.Errorf("init-topics.sh 缺少 topic %s", topic)
		}
	}
}

// BASE-02 规则 7：example 只放演示值，覆盖 compose 与约定 §8 的敏感项。
func TestBASE02EnvExample(t *testing.T) {
	text := readRepoFile(t, "deploy/.env.example")
	values := parseEnvExample(text)
	for _, key := range []string{
		"MYSQL_ROOT_PASSWORD",
		"SECRET_MASTER_KEY",
		"ADMIN_TOKEN",
		"CHANNEL_MOCKPAY_KEY",
		"WORKER_ID",
		"MYSQL_DSN",
		"KAFKA_BROKERS",
	} {
		if values[key] == "" {
			t.Errorf("deploy/.env.example 缺少或空值 %s", key)
		}
	}
	raw, err := base64.StdEncoding.DecodeString(values["SECRET_MASTER_KEY"])
	if err != nil || len(raw) != 32 {
		t.Errorf("SECRET_MASTER_KEY 必须是 32 字节的 base64，实际长度 %d", len(raw))
	}
	if len(values["ADMIN_TOKEN"]) < 32 {
		t.Errorf("ADMIN_TOKEN 演示值长度 %d，约定要求 ≥ 32", len(values["ADMIN_TOKEN"]))
	}
}

// BASE-02 验收 1、4：up / down 不再是占位失败，并提供 up-lite。
func TestBASE02MakefileTargets(t *testing.T) {
	text := readRepoFile(t, "Makefile")
	if strings.Contains(text, "up 由 BASE-02 实现") || strings.Contains(text, "down 由 BASE-02 实现") {
		t.Fatal("up/down 仍是 BASE-02 占位")
	}
	if !regexp.MustCompile(`(?m)^up-lite:`).MatchString(text) {
		t.Error("Makefile 缺少 up-lite")
	}
	if !strings.Contains(text, "--profile obs") {
		t.Error("make up 应默认包含 profile obs")
	}
	if !strings.Contains(text, "filter 1,$(V)") {
		t.Error("make down 应按 V=1 删除数据卷")
	}
}

// BASE-02 规则 8：chaos 覆盖文件存在，且不实现 QA-01 的故障注入。
func TestBASE02ChaosPlaceholder(t *testing.T) {
	text := readRepoFile(t, "deploy/docker-compose.chaos.yml")
	if !strings.Contains(text, "QA-01") {
		t.Error("chaos 覆盖文件应标明由 QA-01 实现")
	}
	if strings.Contains(text, "FAULT_POINTS") {
		t.Error("BASE-02 不应在 chaos 覆盖里配置故障注入")
	}
}

// BASE-02 规则 7：compose 专用的 root 密码登记到约定 §8。
func TestBASE02ConventionsRegisterRootPassword(t *testing.T) {
	text := readRepoFile(t, "docs/03-conventions.md")
	if !strings.Contains(text, "MYSQL_ROOT_PASSWORD") {
		t.Fatal("约定 §8 未登记 MYSQL_ROOT_PASSWORD")
	}
}

func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path) //nolint:gosec // path is a test constant.
	if err != nil {
		t.Fatalf("读取 %s：%v", path, err)
	}
	return string(content)
}

func parseEnvExample(text string) map[string]string {
	values := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		values[key] = value
	}
	return values
}
