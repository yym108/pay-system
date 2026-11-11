package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// OPS-01 验收 1：SIGTERM 后 /readyz 立即 503；阻塞约 1 秒的在途请求仍返回 200；新连接被拒绝；服务器在上限内退出。
func TestShutdownReadyzAndInflight(t *testing.T) {
	adminLn := listenLocal(t)
	bizLn := listenLocal(t)
	var entered atomic.Bool
	var finished atomic.Bool
	biz := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered.Store(true)
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				finished.Store(true)
				writeText(w, http.StatusOK, "ok")
			case <-r.Context().Done():
			}
		}),
		ReadHeaderTimeout: time.Second,
	}
	signals := make(chan os.Signal, 1)
	srv := New(Config{
		Listener:     adminLn,
		DrainTimeout: 3 * time.Second,
		Signals:      signals,
		DB: func(context.Context) error {
			return nil
		},
	})
	srv.RegisterGroup(httpGroup(biz, bizLn))

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()
	waitGET(t, "http://"+adminLn.Addr().String()+"/healthz", http.StatusOK, "ok", time.Second)

	reqCtx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	respCh := make(chan heldResponse, 1)
	go func() {
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+bizLn.Addr().String()+"/hold", nil)
		if err != nil {
			respCh <- heldResponse{err: err}
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			respCh <- heldResponse{err: err}
			return
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		respCh <- heldResponse{status: resp.StatusCode, body: string(body)}
	}()
	waitFlag(t, &entered, true, time.Second)

	signals <- syscall.SIGTERM
	waitGET(t, "http://"+adminLn.Addr().String()+"/readyz", http.StatusServiceUnavailable, "unavailable", 300*time.Millisecond)
	if finished.Load() {
		t.Fatal("readyz 503 arrived only after the in-flight request finished")
	}
	waitGET(t, "http://"+adminLn.Addr().String()+"/healthz", http.StatusOK, "ok", 300*time.Millisecond)
	waitRefused(t, bizLn.Addr().String(), 500*time.Millisecond)
	if finished.Load() {
		t.Fatal("new connections were refused only after the in-flight request finished")
	}

	select {
	case got := <-respCh:
		if got.err != nil || got.status != http.StatusOK || got.body != "ok" {
			t.Fatalf("in-flight response = %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight request did not return 200 before the assertion timeout")
	}
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not exit within the drain limit")
	}
}

// OPS-01 验收 2：在途请求超过上限时，到点退出，不等到请求结束。
func TestShutdownDoesNotWaitPastDrain(t *testing.T) {
	adminLn := listenLocal(t)
	bizLn := listenLocal(t)
	release := make(chan struct{})
	var entered atomic.Bool
	var finished atomic.Bool
	biz := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered.Store(true)
			select {
			case <-release:
				finished.Store(true)
				writeText(w, http.StatusOK, "ok")
			case <-r.Context().Done():
			}
		}),
		ReadHeaderTimeout: time.Second,
	}
	var mu sync.Mutex
	var order []string
	mark := func(step string) {
		mu.Lock()
		order = append(order, step)
		mu.Unlock()
	}
	signals := make(chan os.Signal, 1)
	srv := New(Config{
		Listener:     adminLn,
		DrainTimeout: 40 * time.Millisecond,
		Signals:      signals,
		DB: func(context.Context) error {
			return nil
		},
	})
	srv.RegisterGroup(httpGroup(biz, bizLn))
	srv.RegisterFlusher(flusherFunc(func(context.Context) error {
		if finished.Load() {
			t.Error("flush waited for the in-flight request")
		}
		mark("flush")
		return nil
	}))
	srv.RegisterCloser(closerFunc(func() error {
		mark("close")
		return nil
	}))

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()
	waitGET(t, "http://"+adminLn.Addr().String()+"/healthz", http.StatusOK, "ok", time.Second)

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+bizLn.Addr().String()+"/hold", nil)
		if err != nil {
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	waitFlag(t, &entered, true, time.Second)
	signals <- syscall.SIGTERM

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server waited for the in-flight request")
	}
	if finished.Load() {
		t.Fatal("server exited only after the request finished")
	}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) < 2 || got[0] != "flush" || got[1] != "close" {
		t.Fatalf("shutdown order = %v", got)
	}
	close(release)
	cancel()
}

// OPS-01 验收 3：数据库不可用时 /readyz 是 503、/healthz 仍是 200；数据库恢复后 /readyz 是 200。
func TestReadyzFollowsDatabase(t *testing.T) {
	const leaked = "user:secret-password@tcp(10.9.8.7:3306)/pay_payment"
	adminLn := listenLocal(t)
	var up atomic.Bool
	signals := make(chan os.Signal, 1)
	srv := New(Config{
		Listener:     adminLn,
		DrainTimeout: 50 * time.Millisecond,
		Signals:      signals,
	})
	srv.RequireDB(func(context.Context) error {
		if up.Load() {
			return nil
		}
		return errors.New(leaked)
	}, 0)

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()
	base := "http://" + adminLn.Addr().String()
	waitGET(t, base+"/healthz", http.StatusOK, "ok", time.Second)

	body := waitGET(t, base+"/readyz", http.StatusServiceUnavailable, "unavailable", time.Second)
	if body != "unavailable" || containsSecret(body) {
		t.Fatalf("down body = %q", body)
	}
	health := waitGET(t, base+"/healthz", http.StatusOK, "ok", time.Second)
	if health != "ok" {
		t.Fatalf("healthz while database down = %q", health)
	}
	status, metrics := getOnce(t, base+"/metrics")
	if status != http.StatusNotFound {
		t.Fatalf("metrics status = %d", status)
	}
	status, pprofBody := getOnce(t, base+"/debug/pprof/")
	if status != http.StatusOK {
		t.Fatalf("pprof status = %d body = %q", status, pprofBody)
	}
	if containsSecret(metrics) || containsSecret(pprofBody) {
		t.Fatal("unexpected body leaked the database target")
	}

	up.Store(true)
	waitGET(t, base+"/readyz", http.StatusOK, "ok", time.Second)
	signals <- syscall.SIGTERM
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit")
	}
}

// OPS-05 验收：管理端口可采集 30 秒 CPU profile
func TestAdminPortCPUProfile(t *testing.T) {
	adminLn := listenLocal(t)
	signals := make(chan os.Signal, 1)
	srv := New(Config{
		Listener:     adminLn,
		DrainTimeout: 50 * time.Millisecond,
		Signals:      signals,
		DB:           func(context.Context) error { return nil },
	})
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()
	base := "http://" + adminLn.Addr().String()
	waitGET(t, base+"/healthz", http.StatusOK, "ok", time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/debug/pprof/profile?seconds=30", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(body) < 2 || body[0] != 0x1f || body[1] != 0x8b {
		t.Fatalf("profile status = %d len = %d", resp.StatusCode, len(body))
	}
	http.DefaultClient.CloseIdleConnections()

	signals <- syscall.SIGTERM
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestPprofSymbolAcceptsPost(t *testing.T) {
	srv := New(Config{DrainTimeout: time.Millisecond})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/debug/pprof/symbol", strings.NewReader("0+"))
	rec := httptest.NewRecorder()
	srv.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %q", rec.Code, rec.Body.String())
	}
}

func TestPprofNotOnOtherMux(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/only-this", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok")
	})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d body = %q", rec.Code, rec.Body.String())
	}
}

func TestPingCheckHidesTarget(t *testing.T) {
	db, err := sqlx.Open("mysql", "user:secret-password@tcp(127.0.0.1:1)/pay_payment")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = PingCheck(db)(ctx)
	if err == nil {
		t.Fatal("ping succeeded")
	}
	if containsSecret(err.Error()) || err.Error() != "unavailable" {
		t.Fatalf("ping error = %q", err.Error())
	}
}

// GW-04：AllowNoDB 默认关闭时，没有数据库仍然是 503。打开后没有检查仍是 503；
// 有检查时 /readyz 跟随该项，/healthz 在检查失败时仍是 200。
func TestReadyAllowNoDB(t *testing.T) {
	t.Run("closed ignores extra check", func(t *testing.T) {
		adminLn := listenLocal(t)
		signals := make(chan os.Signal, 1)
		srv := New(Config{Listener: adminLn, DrainTimeout: 20 * time.Millisecond, Signals: signals})
		srv.RegisterCheck(func(context.Context) error { return nil })
		runErr := make(chan error, 1)
		go func() { runErr <- srv.Run(context.Background()) }()
		base := "http://" + adminLn.Addr().String()
		waitGET(t, base+"/healthz", http.StatusOK, "ok", time.Second)
		waitGET(t, base+"/readyz", http.StatusServiceUnavailable, "unavailable", time.Second)
		stopServer(t, signals, runErr)
	})

	t.Run("open without checks", func(t *testing.T) {
		adminLn := listenLocal(t)
		signals := make(chan os.Signal, 1)
		srv := New(Config{
			Listener:     adminLn,
			DrainTimeout: 20 * time.Millisecond,
			Signals:      signals,
			AllowNoDB:    true,
		})
		runErr := make(chan error, 1)
		go func() { runErr <- srv.Run(context.Background()) }()
		base := "http://" + adminLn.Addr().String()
		waitGET(t, base+"/healthz", http.StatusOK, "ok", time.Second)
		waitGET(t, base+"/readyz", http.StatusServiceUnavailable, "unavailable", time.Second)
		stopServer(t, signals, runErr)
	})

	t.Run("open follows check", func(t *testing.T) {
		adminLn := listenLocal(t)
		signals := make(chan os.Signal, 1)
		var up atomic.Bool
		srv := New(Config{
			Listener:     adminLn,
			DrainTimeout: 20 * time.Millisecond,
			Signals:      signals,
			AllowNoDB:    true,
		})
		srv.RegisterCheck(func(context.Context) error {
			if up.Load() {
				return nil
			}
			return errors.New("redis down")
		})
		runErr := make(chan error, 1)
		go func() { runErr <- srv.Run(context.Background()) }()
		base := "http://" + adminLn.Addr().String()
		waitGET(t, base+"/healthz", http.StatusOK, "ok", time.Second)
		body := waitGET(t, base+"/readyz", http.StatusServiceUnavailable, "unavailable", time.Second)
		if body != "unavailable" {
			t.Fatalf("body %q", body)
		}
		waitGET(t, base+"/healthz", http.StatusOK, "ok", time.Second)
		up.Store(true)
		waitGET(t, base+"/readyz", http.StatusOK, "ok", time.Second)
		stopServer(t, signals, runErr)
	})
}

func stopServer(t *testing.T, signals chan os.Signal, runErr <-chan error) {
	t.Helper()
	signals <- syscall.SIGTERM
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit")
	}
}

func TestReadyzRequiresDatabase(t *testing.T) {
	adminLn := listenLocal(t)
	signals := make(chan os.Signal, 1)
	srv := New(Config{Listener: adminLn, DrainTimeout: 20 * time.Millisecond, Signals: signals})
	srv.RegisterCheck(func(context.Context) error { return nil })
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()
	base := "http://" + adminLn.Addr().String()
	waitGET(t, base+"/healthz", http.StatusOK, "ok", time.Second)
	waitGET(t, base+"/readyz", http.StatusServiceUnavailable, "unavailable", time.Second)
	signals <- syscall.SIGTERM
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit")
	}
}

type heldResponse struct {
	status int
	body   string
	err    error
}

type flusherFunc func(context.Context) error

func (f flusherFunc) Flush(ctx context.Context) error { return f(ctx) }

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

func httpGroup(srv *http.Server, ln net.Listener) Group {
	return FuncGroup(
		func() error {
			err := srv.Serve(ln)
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		},
		func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	)
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

func waitGET(t *testing.T, url string, wantStatus int, wantBody string, within time.Duration) string {
	t.Helper()
	now := time.Now
	deadline := now().Add(within)
	var last string
	for {
		status, body, err := doGET(url)
		if err == nil && status == wantStatus && body == wantBody {
			return body
		}
		if err != nil {
			last = err.Error()
		} else {
			last = body
		}
		if now().After(deadline) {
			t.Fatalf("GET %s: want %d %q, last %q", url, wantStatus, wantBody, last)
		}
	}
}

func getOnce(t *testing.T, url string) (int, string) {
	t.Helper()
	status, body, err := doGET(url)
	if err != nil {
		t.Fatal(err)
	}
	return status, body
}

func doGET(url string) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
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

func waitFlag(t *testing.T, flag *atomic.Bool, want bool, within time.Duration) {
	t.Helper()
	now := time.Now
	deadline := now().Add(within)
	for flag.Load() != want {
		if now().After(deadline) {
			t.Fatal("timed out waiting for handler")
		}
	}
}

func waitRefused(t *testing.T, addr string, within time.Duration) {
	t.Helper()
	now := time.Now
	deadline := now().Add(within)
	dialer := net.Dialer{Timeout: 50 * time.Millisecond}
	for {
		conn, err := dialer.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			return
		}
		_ = conn.Close()
		if now().After(deadline) {
			t.Fatalf("new connection to %s was accepted", addr)
		}
	}
}

func containsSecret(s string) bool {
	for _, part := range []string{"secret-password", "10.9.8.7", "@tcp(", "127.0.0.1:1"} {
		if len(s) >= len(part) && (s == part || stringContains(s, part)) {
			return true
		}
	}
	return false
}

func stringContains(s, part string) bool {
	return len(part) == 0 || (len(s) >= len(part) && (s == part || len(s) > 0 && (containsAt(s, part))))
}

func containsAt(s, part string) bool {
	for i := 0; i+len(part) <= len(s); i++ {
		if s[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
