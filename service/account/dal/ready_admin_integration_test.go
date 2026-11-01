//go:build integration

package dal

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/server"
)

// OPS-01 验收 3：account 的两个分片沿用已有 Ping。都可用时 /readyz 为 200；关掉一个分片后为 503，/healthz 仍为 200。
func TestAdminReadyzUsesBothShards(t *testing.T) {
	_, _, _, pools := openTwoSchemas(t)
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	signals := make(chan os.Signal, 1)
	srv := server.New(server.Config{
		Listener:     ln,
		DrainTimeout: 50 * time.Millisecond,
		Signals:      signals,
	})
	srv.RequireDB(pools.Ready, 2*server.CheckTimeout)

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()
	base := "http://" + ln.Addr().String()

	status, body := waitAdmin(t, base+"/healthz", http.StatusOK, "ok")
	if status != http.StatusOK || body != "ok" {
		t.Fatalf("healthz = %d %q", status, body)
	}
	status, body = waitAdmin(t, base+"/readyz", http.StatusOK, "ok")
	if status != http.StatusOK || body != "ok" {
		t.Fatalf("readyz = %d %q", status, body)
	}

	db1, err := pools.ShardDB(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	status, body = waitAdmin(t, base+"/readyz", http.StatusServiceUnavailable, "unavailable")
	if status != http.StatusServiceUnavailable || body != "unavailable" || readyBodyLeaks(body) {
		t.Fatalf("closed readyz = %d %q", status, body)
	}
	status, body = waitAdmin(t, base+"/healthz", http.StatusOK, "ok")
	if status != http.StatusOK || body != "ok" {
		t.Fatalf("healthz after close = %d %q", status, body)
	}

	signals <- syscall.SIGTERM
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admin server did not exit")
	}
}

func waitAdmin(t *testing.T, url string, wantStatus int, wantBody string) (int, string) {
	t.Helper()
	now := time.Now
	deadline := now().Add(4 * time.Second)
	var lastStatus int
	var lastBody string
	var lastErr error
	for {
		status, body, err := getAdmin(url)
		if err == nil && status == wantStatus && body == wantBody {
			return status, body
		}
		lastStatus, lastBody, lastErr = status, body, err
		if now().After(deadline) {
			t.Fatalf("GET %s: want %d %q, last %d %q err %v", url, wantStatus, wantBody, lastStatus, lastBody, lastErr)
		}
	}
}

func getAdmin(url string) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256))
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

func readyBodyLeaks(body string) bool {
	lower := strings.ToLower(body)
	return strings.Contains(body, "127.0.0.1") || strings.Contains(body, "@tcp(") || strings.Contains(lower, "password")
}
