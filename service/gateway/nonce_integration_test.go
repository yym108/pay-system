//go:build integration

package gateway

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yym108/pay-system/pkg/errno"
	payserver "github.com/yym108/pay-system/pkg/server"
)

// GW-04 验收 3：redis:8.10.2 上第一次 SET 成功，TTL 在 1 到 600 秒。
// 容器停掉后商户请求返回 10004，/readyz 为 503，/healthz 仍是 200。
func TestGW04RedisNonce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctr, err := testcontainers.Run(ctx, "redis:8.10.2",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("Ready to accept connections").WithStartupTimeout(time.Minute),
			wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute),
		)),
	)
	if err != nil {
		t.Fatal(err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if host == "localhost" || host == "::1" {
		host = "127.0.0.1"
	}
	mapped, err := ctr.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	addr := net.JoinHostPort(host, mapped.Port())
	store, ready, closer := OpenRedisNonce(addr)
	t.Cleanup(func() { _ = closer.Close() })
	client, ok := closer.(*redis.Client)
	if !ok {
		t.Fatalf("closer %T", closer)
	}

	const appID = gw03AppID
	const nonce = "RedisNonce012345678"
	claimed, err := store.Reserve(ctx, appID, nonce)
	if err != nil || !claimed {
		t.Fatalf("reserve claimed %v err %v", claimed, err)
	}
	key := fmt.Sprintf("gw:nonce:%s:%s", appID, nonce)
	sec, err := client.Do(ctx, "TTL", key).Int64()
	if err != nil || sec < 1 || sec > int64(nonceTTLSeconds) {
		t.Fatalf("ttl %d err %v", sec, err)
	}
	again, err := store.Reserve(ctx, appID, nonce)
	if err != nil || again {
		t.Fatalf("replay claimed %v err %v", again, err)
	}
	other, err := store.Reserve(ctx, "app_other_redis", nonce)
	if err != nil || !other {
		t.Fatalf("other app claimed %v err %v", other, err)
	}

	var lc net.ListenConfig
	adminLn, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminLn.Close() })
	signals := make(chan os.Signal, 1)
	proc := payserver.New(payserver.Config{
		Listener:     adminLn,
		DrainTimeout: 20 * time.Millisecond,
		Signals:      signals,
		AllowNoDB:    true,
	})
	proc.RegisterCheck(ready)
	runErr := make(chan error, 1)
	go func() { runErr <- proc.Run(context.Background()) }()
	base := "http://" + adminLn.Addr().String()
	waitText(t, base+"/healthz", http.StatusOK, "ok", 200*time.Millisecond, time.Second)
	waitText(t, base+"/readyz", http.StatusOK, "ok", 2*time.Second, 3*time.Second)

	look := gw02Lookup()
	httpAddr, hit := gw04Start(t, store, look, nil)
	body := []byte(`{"n":1}`)
	const path = "/v1/gw04"
	ts := strconv.FormatInt(gw04Now, 10)
	first := gw04Nonce()
	hdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, appID, ts, first, body)
	got := do(t, http.MethodPost, "http://"+httpAddr+path, body, hdr)
	if got.status != http.StatusOK {
		t.Fatalf("up status %d body %s", got.status, got.body)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	err = ctr.Terminate(stopCtx)
	stopCancel()
	if err != nil {
		t.Fatal(err)
	}

	before := nonceUnavailable.Load()
	downNonce := gw04Nonce()
	downHdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, appID, ts, downNonce, body)
	got = do(t, http.MethodPost, "http://"+httpAddr+path, body, downHdr)
	assertSignedErrno(t, got, errno.ErrUnavailable)
	if nonceUnavailable.Load() != before+1 {
		t.Fatalf("counter %d want %d", nonceUnavailable.Load(), before+1)
	}
	if n, _, _ := hit.snapshot(); n != 1 {
		t.Fatal("handler called after redis stopped")
	}
	status, text, err := getText(base+"/readyz", 2*time.Second)
	if err != nil || status != http.StatusServiceUnavailable || text != "unavailable" {
		t.Fatalf("readyz %d %q err %v", status, text, err)
	}
	status, text, err = getText(base+"/healthz", 2*time.Second)
	if err != nil || status != http.StatusOK || text != "ok" {
		t.Fatalf("healthz %d %q err %v", status, text, err)
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

func waitText(t *testing.T, url string, wantStatus int, wantBody string, budget, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	var last string
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		status, body, err := getText(url, budget)
		if err == nil && status == wantStatus && body == wantBody {
			return
		}
		if err != nil {
			last = err.Error()
		} else {
			last = body
		}
		timer.Reset(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Fatalf("GET %s: want %d %q, last %q", url, wantStatus, wantBody, last)
		case <-timer.C:
		}
	}
}

func getText(url string, budget time.Duration) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
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
