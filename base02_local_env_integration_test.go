//go:build integration

package base

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// BASE-02 验收 3：make up 的渲染结果只绑定 127.0.0.1；apps profile 仅 8080、9080、9100 可以不绑定。
func TestBASE02ComposeConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	envFile := copyEnvExample(t)

	obs := composeConfig(t, ctx, envFile, "--profile", "obs")
	assertImages(t, obs)
	assertLocalhostOnly(t, obs)
	assertPublished(t, obs, map[string][]int{
		"mysql":      {3306},
		"redis":      {6379},
		"kafka":      {9094},
		"prometheus": {9090},
		"jaeger":     {16686, 4317},
	})
	if _, ok := obs.Services["gateway"]; ok {
		t.Error("profile obs 的渲染结果不应包含应用容器")
	}
	for _, name := range []string{"mysql", "redis", "kafka", "prometheus", "jaeger"} {
		if len(obs.Services[name].Healthcheck.Test) == 0 {
			t.Errorf("%s 缺少健康检查", name)
		}
	}

	apps := composeConfig(t, ctx, envFile, "--profile", "apps", "--profile", "obs")
	assertLocalhostExceptPublic(t, apps)
	assertPublished(t, apps, map[string][]int{
		"gateway":       {8080, 8090, 6060},
		"payment":       {8881, 6061},
		"account":       {8882, 6062},
		"recon":         {8883, 6063},
		"channel-mock":  {9080, 6064},
		"merchant-demo": {9100},
	})
	for _, name := range []string{"gateway", "payment", "account", "recon", "channel-mock", "merchant-demo", "migrate", "seed"} {
		svc, ok := apps.Services[name]
		if !ok {
			t.Errorf("profile apps 缺少 %s", name)
			continue
		}
		if !contains(svc.Profiles, "apps") {
			t.Errorf("%s 应属于 profile apps，实际 %v", name, svc.Profiles)
		}
	}
	gateway := apps.Services["gateway"]
	for _, dep := range []string{"mysql", "redis", "kafka"} {
		if gateway.DependsOn[dep].Condition != "service_healthy" {
			t.Errorf("gateway 对 %s 的依赖应为 service_healthy，实际 %q", dep, gateway.DependsOn[dep].Condition)
		}
	}
	for _, dep := range []string{"kafka-init", "migrate", "seed"} {
		if gateway.DependsOn[dep].Condition != "service_completed_successfully" {
			t.Errorf("gateway 对 %s 的依赖应为 service_completed_successfully，实际 %q", dep, gateway.DependsOn[dep].Condition)
		}
	}

	lite := composeConfig(t, ctx, envFile)
	if _, ok := lite.Services["prometheus"]; ok {
		t.Error("默认渲染（up-lite）不应包含 prometheus")
	}
	if _, ok := lite.Services["jaeger"]; ok {
		t.Error("默认渲染（up-lite）不应包含 jaeger")
	}

	merged := composeConfigFiles(t, ctx, envFile, "deploy/docker-compose.chaos.yml")
	if _, ok := merged.Services["mysql"]; !ok {
		t.Fatal("chaos 覆盖文件无法与 docker-compose.yml 合并")
	}
}

// BASE-02 验收 1：复制 env 后 make up，3 分钟内基础设施 healthy，kafka-init 退出 0，不启动 apps。
// BASE-02 验收 2：make down 停容器并保留卷；make down V=1 删除卷。
// BASE-02 验收 4：make up-lite 不包含 prometheus、jaeger。
// BASE-02 验收 5：6 个 topic 各 3 分区、副本数 1，且关闭自动建 topic。
// 启动耗时是墙钟 SLA，不用 clock.Fake。子步骤共用一次环境，因为验收本身是有顺序的启停序列。
func TestBASE02Stack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	envFile := copyEnvExample(t)
	existed := fileExists("deploy/.env")
	t.Cleanup(func() {
		downCtx, downCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer downCancel()
		if err := runMake(downCtx, envFile, "down", "V=1"); err != nil {
			t.Errorf("清理 make down V=1：%v", err)
		}
		if !existed && fileExists("deploy/.env") {
			t.Error("测试留下了 deploy/.env")
		}
	})

	if err := runMake(ctx, envFile, "down", "V=1"); err != nil {
		t.Fatalf("预清理失败：%v", err)
	}
	ensureImages(ctx, t)

	// 计时从镜像已在本地开始，对应验收“镜像已拉取的前提下”。
	start := time.Now()
	if err := runMake(ctx, envFile, "up"); err != nil {
		t.Fatalf("make up：%v", err)
	}
	elapsed := time.Since(start)
	t.Logf("make up 到全部 healthy 用时 %s", elapsed.Round(time.Second))
	if elapsed > 3*time.Minute {
		t.Fatalf("make up 用时 %s，超过 3 分钟", elapsed.Round(time.Second))
	}

	assertServicesHealthy(t, ctx, envFile, []string{"mysql", "redis", "kafka", "prometheus", "jaeger"})
	assertKafkaInitExited(t, ctx, envFile)
	assertAppsNotRunning(t, ctx, envFile)
	assertTopics(t, ctx, envFile)
	assertMySQLSettings(t, ctx, envFile)
	assertKafkaRuntime(t, ctx, envFile)

	volumes := projectVolumes(t, ctx)
	if len(volumes) == 0 {
		t.Fatal("make up 后应存在本项目数据卷")
	}

	if err := runMake(ctx, envFile, "down"); err != nil {
		t.Fatalf("make down：%v", err)
	}
	if running := projectContainers(t, ctx); len(running) != 0 {
		t.Fatalf("make down 后仍有容器：%v", running)
	}
	if remaining := projectVolumes(t, ctx); len(remaining) == 0 {
		t.Fatal("make down 不应删除数据卷")
	}

	if err := runMake(ctx, envFile, "down", "V=1"); err != nil {
		t.Fatalf("make down V=1：%v", err)
	}
	if remaining := projectVolumes(t, ctx); len(remaining) != 0 {
		t.Fatalf("make down V=1 后仍有数据卷：%v", remaining)
	}

	if err := runMake(ctx, envFile, "up-lite"); err != nil {
		t.Fatalf("make up-lite：%v", err)
	}
	assertServicesHealthy(t, ctx, envFile, []string{"mysql", "redis", "kafka"})
	assertKafkaInitExited(t, ctx, envFile)
	for _, row := range composePsAll(t, ctx, envFile) {
		if row.Service == "prometheus" || row.Service == "jaeger" {
			t.Errorf("make up-lite 不应启动 %s", row.Service)
		}
	}
}

func copyEnvExample(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("deploy/.env.example")
	if err != nil {
		t.Fatalf("读取 deploy/.env.example：%v", err)
	}
	path := filepath.Join(t.TempDir(), "env")
	if err := os.WriteFile(path, src, 0o600); err != nil { //nolint:gosec // path is t.TempDir plus a fixed filename, not external input.
		t.Fatalf("写入临时 env：%v", err)
	}
	return path
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func ensureImages(ctx context.Context, t *testing.T) {
	t.Helper()
	images := []string{
		"mysql:8.4.11",
		"redis:8.10.2",
		"apache/kafka:4.3.1",
		"prom/prometheus:v3.15.0",
		"jaegertracing/jaeger:2.22.0",
	}
	for _, image := range images {
		if exec.CommandContext(ctx, "docker", "image", "inspect", image).Run() == nil { //nolint:gosec // image tags are the locked list above.
			continue
		}
		t.Logf("拉取 %s", image)
		out, err := exec.CommandContext(ctx, "docker", "pull", image).CombinedOutput() //nolint:gosec // image tags are the locked list above.
		if err != nil {
			t.Fatalf("拉取 %s：%v\n%s", image, err, out)
		}
	}
}

func runMake(ctx context.Context, envFile string, args ...string) error {
	// 命令行变量覆盖 Makefile 的 ?=，不依赖 make 是否导入环境变量。
	cmdArgs := append([]string{"ENV_FILE=" + envFile}, args...)
	cmd := exec.CommandContext(ctx, "make", cmdArgs...) //nolint:gosec // targets are fixed by the caller; env file is a temp copy of the example.
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w\n%s", err, out)
	}
	return nil
}

func compose(ctx context.Context, envFile string, args ...string) ([]byte, error) {
	cmdArgs := append([]string{"deploy/compose.sh"}, args...)
	cmd := exec.CommandContext(ctx, "sh", cmdArgs...) //nolint:gosec // compose.sh plus fixed compose subcommands.
	cmd.Env = append(os.Environ(), "ENV_FILE="+envFile)
	return cmd.CombinedOutput()
}

type composeFile struct {
	Services map[string]composeService `json:"services"`
}

type composeService struct {
	Image       string                      `json:"image"`
	Ports       []composePort               `json:"ports"`
	Profiles    []string                    `json:"profiles"`
	DependsOn   map[string]composeDependsOn `json:"depends_on"`
	Healthcheck composeHealthcheck          `json:"healthcheck"`
}

type composeDependsOn struct {
	Condition string `json:"condition"`
}

type composeHealthcheck struct {
	Test []string `json:"test"`
}

type composePort struct {
	HostIP    string          `json:"host_ip"`
	Published json.RawMessage `json:"published"`
	Target    int             `json:"target"`
}

func (p composePort) published() int {
	raw := strings.Trim(string(p.Published), `"`)
	n, err := strconv.Atoi(raw)
	if err != nil {
		return p.Target
	}
	return n
}

func composeConfig(t *testing.T, ctx context.Context, envFile string, extra ...string) composeFile {
	t.Helper()
	args := append(extra, "config", "--format", "json")
	return decodeCompose(t, ctx, envFile, args...)
}

func composeConfigFiles(t *testing.T, ctx context.Context, envFile, overlay string) composeFile {
	t.Helper()
	return decodeCompose(t, ctx, envFile, "-f", overlay, "config", "--format", "json")
}

func decodeCompose(t *testing.T, ctx context.Context, envFile string, args ...string) composeFile {
	t.Helper()
	out, err := compose(ctx, envFile, args...)
	if err != nil {
		t.Fatalf("docker compose config：%v\n%s", err, out)
	}
	var parsed composeFile
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("解析 compose config：%v\n%s", err, out)
	}
	return parsed
}

func assertImages(t *testing.T, file composeFile) {
	t.Helper()
	want := map[string]string{
		"mysql":      "mysql:8.4.11",
		"redis":      "redis:8.10.2",
		"kafka":      "apache/kafka:4.3.1",
		"kafka-init": "apache/kafka:4.3.1",
		"prometheus": "prom/prometheus:v3.15.0",
		"jaeger":     "jaegertracing/jaeger:2.22.0",
	}
	for name, image := range want {
		got := file.Services[name].Image
		if !strings.Contains(got, image) {
			t.Errorf("%s 镜像 %q，期望包含 %s", name, got, image)
		}
	}
}

func assertLocalhostOnly(t *testing.T, file composeFile) {
	t.Helper()
	for name, svc := range file.Services {
		for _, port := range svc.Ports {
			if hostIP(port) != "127.0.0.1" {
				t.Errorf("%s 端口 %d 绑定 %s，必须是 127.0.0.1", name, port.published(), hostIP(port))
			}
		}
	}
}

func assertLocalhostExceptPublic(t *testing.T, file composeFile) {
	t.Helper()
	public := map[int]bool{8080: true, 9080: true, 9100: true}
	for name, svc := range file.Services {
		for _, port := range svc.Ports {
			published := port.published()
			if public[published] {
				continue
			}
			if hostIP(port) != "127.0.0.1" {
				t.Errorf("%s 端口 %d 绑定 %s，必须是 127.0.0.1", name, published, hostIP(port))
			}
		}
	}
}

func assertPublished(t *testing.T, file composeFile, want map[string][]int) {
	t.Helper()
	for name, ports := range want {
		got := map[int]bool{}
		for _, port := range file.Services[name].Ports {
			got[port.published()] = true
		}
		for _, port := range ports {
			if !got[port] {
				t.Errorf("%s 未发布端口 %d，实际 %v", name, port, got)
			}
		}
		if len(got) != len(ports) {
			t.Errorf("%s 发布端口 %v，期望 %v", name, got, ports)
		}
	}
}

func hostIP(port composePort) string {
	if port.HostIP == "" {
		return "0.0.0.0"
	}
	return port.HostIP
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

type composePs struct {
	Service  string `json:"Service"`
	State    string `json:"State"`
	Health   string `json:"Health"`
	ExitCode int    `json:"ExitCode"`
	Name     string `json:"Name"`
}

func composePsAll(t *testing.T, ctx context.Context, envFile string) []composePs {
	t.Helper()
	out, err := compose(ctx, envFile, "ps", "-a", "--format", "json")
	if err != nil {
		t.Fatalf("compose ps：%v\n%s", err, out)
	}
	var rows []composePs
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		var row composePs
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("解析 compose ps：%v\n%s", err, line)
		}
		rows = append(rows, row)
	}
	return rows
}

func assertServicesHealthy(t *testing.T, ctx context.Context, envFile string, services []string) {
	t.Helper()
	rows := composePsAll(t, ctx, envFile)
	byService := map[string]composePs{}
	for _, row := range rows {
		byService[row.Service] = row
	}
	for _, service := range services {
		row, ok := byService[service]
		if !ok {
			t.Errorf("缺少容器 %s", service)
			continue
		}
		if row.Health != "healthy" {
			t.Errorf("%s 状态 %s health %s，期望 healthy", service, row.State, row.Health)
		}
	}
}

func assertKafkaInitExited(t *testing.T, ctx context.Context, envFile string) {
	t.Helper()
	for _, row := range composePsAll(t, ctx, envFile) {
		if row.Service != "kafka-init" {
			continue
		}
		if row.State != "exited" || row.ExitCode != 0 {
			t.Fatalf("kafka-init 状态 %s 退出码 %d，期望 exited 0", row.State, row.ExitCode)
		}
		return
	}
	t.Fatal("未找到 kafka-init")
}

func assertAppsNotRunning(t *testing.T, ctx context.Context, envFile string) {
	t.Helper()
	apps := map[string]bool{
		"gateway": true, "payment": true, "account": true, "recon": true,
		"channel-mock": true, "merchant-demo": true, "migrate": true, "seed": true,
	}
	for _, row := range composePsAll(t, ctx, envFile) {
		if apps[row.Service] {
			t.Errorf("make up 不应启动 %s（状态 %s）", row.Service, row.State)
		}
	}
}

func assertTopics(t *testing.T, ctx context.Context, envFile string) {
	t.Helper()
	out, err := compose(ctx, envFile, "exec", "-T", "kafka",
		"/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--describe")
	if err != nil {
		t.Fatalf("描述 topic：%v\n%s", err, out)
	}
	text := string(out)
	for _, topic := range base02Topics {
		line := topicLine(text, topic)
		if line == "" {
			t.Errorf("缺少 topic %s\n%s", topic, text)
			continue
		}
		if !strings.Contains(line, "PartitionCount: 3") || !strings.Contains(line, "ReplicationFactor: 1") {
			t.Errorf("topic %s 分区或副本不符合：%s", topic, line)
		}
	}
}

func topicLine(text, topic string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "Topic: "+topic) && strings.Contains(line, "PartitionCount:") {
			return line
		}
	}
	return ""
}

func assertMySQLSettings(t *testing.T, ctx context.Context, envFile string) {
	t.Helper()
	password := envValue(t, envFile, "MYSQL_ROOT_PASSWORD")
	out, err := compose(ctx, envFile, "exec", "-T", "-e", "MYSQL_PWD="+password, "mysql",
		"mysql", "-uroot", "-N", "-e",
		"SELECT @@innodb_buffer_pool_size, @@max_connections, @@global.time_zone")
	if err != nil {
		t.Fatalf("读取 MySQL 变量：%v\n%s", err, redact(string(out), password))
	}
	fields := strings.Fields(string(out))
	if len(fields) != 3 {
		t.Fatalf("MySQL 变量输出无法解析：%q", redact(string(out), password))
	}
	if fields[0] != "268435456" {
		t.Errorf("innodb_buffer_pool_size=%s，期望 268435456（256M）", fields[0])
	}
	if fields[1] != "500" {
		t.Errorf("max_connections=%s，期望 500", fields[1])
	}
	if fields[2] != "+08:00" {
		t.Errorf("time_zone=%s，期望 +08:00", fields[2])
	}
}

func assertKafkaRuntime(t *testing.T, ctx context.Context, envFile string) {
	t.Helper()
	cmdline, err := compose(ctx, envFile, "exec", "-T", "kafka",
		"/bin/bash", "-c", `tr '\0' ' ' < /proc/1/cmdline`)
	if err != nil {
		t.Fatalf("读取 Kafka 命令行：%v\n%s", err, cmdline)
	}
	if !strings.Contains(strings.ToLower(string(cmdline)), "-xmx512") {
		t.Errorf("Kafka 堆不是 512MB：%s", cmdline)
	}
	props, err := compose(ctx, envFile, "exec", "-T", "kafka",
		"/bin/bash", "-c", "grep -E '^auto.create.topics.enable=' /opt/kafka/config/server.properties")
	if err != nil {
		t.Fatalf("读取 auto.create.topics.enable：%v\n%s", err, props)
	}
	if strings.TrimSpace(string(props)) != "auto.create.topics.enable=false" {
		t.Errorf("自动建 topic 未关闭：%s", props)
	}
}

func projectVolumes(t *testing.T, ctx context.Context) []string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", "volume", "ls", "-q",
		"--filter", "label=com.docker.compose.project=pay-system").CombinedOutput()
	if err != nil {
		t.Fatalf("列出数据卷：%v\n%s", err, out)
	}
	return nonEmptyLines(string(out))
}

func projectContainers(t *testing.T, ctx context.Context) []string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", "ps", "-aq",
		"--filter", "label=com.docker.compose.project=pay-system").CombinedOutput()
	if err != nil {
		t.Fatalf("列出容器：%v\n%s", err, out)
	}
	return nonEmptyLines(string(out))
}

func nonEmptyLines(text string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func envValue(t *testing.T, path, key string) string {
	t.Helper()
	content, err := os.ReadFile(path) //nolint:gosec // path is the temp env file created by this test.
	if err != nil {
		t.Fatalf("读取 env：%v", err)
	}
	value := parseEnvExample(string(content))[key]
	if value == "" {
		t.Fatalf("env 缺少 %s", key)
	}
	return value
}

func redact(text, secret string) string {
	if secret == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "***")
}
