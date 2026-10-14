package idgen

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// BASE-05 规则 2：compose 为 6 个业务进程写死互不相同的 WORKER_ID，且都在 0–1023。
func TestComposeWorkerIDs(t *testing.T) {
	text := string(readRepoFile(t, filepath.Join("deploy", "docker-compose.yml")))
	services, workers := parseComposeWorkers(text)
	want := map[string]int{
		"gateway":       1,
		"payment":       2,
		"account":       3,
		"recon":         4,
		"channel-mock":  5,
		"merchant-demo": 6,
	}
	seen := map[int]string{}
	for name, id := range want {
		raw, ok := workers[name]
		if !ok {
			t.Errorf("service %s has no literal WORKER_ID", name)
			continue
		}
		if strings.Contains(raw, "$") {
			t.Errorf("service %s WORKER_ID = %q, want a literal", name, raw)
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > 1023 {
			t.Errorf("service %s WORKER_ID = %q, want 0–1023", name, raw)
			continue
		}
		if n != id {
			t.Errorf("service %s WORKER_ID = %d, want %d", name, n, id)
		}
		if prev, ok := seen[n]; ok {
			t.Errorf("WORKER_ID %d used by both %s and %s", n, prev, name)
		}
		seen[n] = name
	}
	skip := map[string]struct{}{
		"mysql": {}, "redis": {}, "kafka": {}, "kafka-init": {},
		"prometheus": {}, "jaeger": {}, "bootstrap": {}, "migrate": {}, "seed": {},
	}
	found := map[string]struct{}{}
	for _, name := range services {
		found[name] = struct{}{}
		if _, ok := skip[name]; ok {
			if _, has := workers[name]; has {
				t.Errorf("service %s must not set WORKER_ID", name)
			}
		}
	}
	for name := range want {
		if _, ok := found[name]; !ok {
			t.Errorf("compose services missing %s", name)
		}
	}
	for name := range skip {
		if _, ok := found[name]; !ok {
			t.Errorf("compose services missing %s", name)
		}
	}
}

func readRepoFile(t *testing.T, rel string) []byte {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			path := filepath.Join(dir, rel)
			content, err := os.ReadFile(path) //nolint:gosec // path is the module root plus a fixed relative path.
			if err != nil {
				t.Fatalf("读取 %s：%v", path, err)
			}
			return content
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("找不到 go.mod")
		}
		dir = parent
	}
}

func parseComposeWorkers(text string) (services []string, workers map[string]string) {
	workers = map[string]string{}
	inServices := false
	current := ""
	for _, line := range strings.Split(text, "\n") {
		if !inServices {
			if line == "services:" {
				inServices = true
			}
			continue
		}
		if line != "" && line[0] != ' ' && line[0] != '#' {
			break
		}
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "   ") {
			name := strings.TrimSpace(line)
			if name == "" || strings.HasPrefix(name, "#") {
				continue
			}
			current = strings.TrimSuffix(name, ":")
			services = append(services, current)
			continue
		}
		trim := strings.TrimSpace(line)
		if current == "" || !strings.HasPrefix(trim, "WORKER_ID:") {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(trim, "WORKER_ID:"))
		workers[current] = strings.Trim(raw, `"'`)
	}
	return services, workers
}
