package dal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
)

// unreachableDSN 指向本机未监听的端口，用来证明失败路径不回显错误。
//
//nolint:gosec // 不是生产口令。
const unreachableDSN = "user:x@tcp(127.0.0.1:1)/none"

// ACC-13 规则：ShardDB 只接受 0 和 1；New 不关闭传入的池。
func TestShardDBRangeAndNewDoesNotOwnClose(t *testing.T) {
	db0 := mustSQLX(t)
	db1 := mustSQLX(t)
	pools := New(db0, db1)
	if err := pools.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if closed(db0) || closed(db1) {
		t.Fatal("New closed a pool")
	}

	got, err := pools.ShardDB(0)
	if err != nil || got != db0 {
		t.Fatalf("ShardDB(0) = %v, %v", got, err)
	}
	got, err = pools.ShardDB(1)
	if err != nil || got != db1 {
		t.Fatalf("ShardDB(1) = %v, %v", got, err)
	}
	for _, shard := range []int{-1, 2, 3} {
		if _, err := pools.ShardDB(shard); err == nil {
			t.Fatalf("ShardDB(%d) accepted", shard)
		} else if strings.Contains(err.Error(), "10001") || strings.Contains(err.Error(), "dsn") {
			t.Fatalf("error text = %q", err.Error())
		}
	}
	if _, err := (*Pools)(nil).ShardDB(0); err == nil {
		t.Fatal("nil pools accepted")
	}
}

// ACC-13 规则：取消的 ctx 在打开池之前失败；负数池配置不连接数据库。
func TestOpenRejectsBeforeConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Open(ctx, "not-a-dsn", "also-not", dbx.PoolConfig{})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("canceled context was not rejected before connect")
	}

	_, err = Open(context.Background(), "not-a-dsn", "also-not", dbx.PoolConfig{MaxOpen: -1})
	if err == nil || !strings.Contains(err.Error(), "negative pool config") || strings.Contains(err.Error(), "10001") {
		t.Fatal("negative pool config was not rejected")
	}
}

// ACC-13 规则：readyz 失败时正文固定，不回显 ping 错误。
func TestReadyzHidesPingError(t *testing.T) {
	pools := New(mustSQLX(t), mustSQLX(t))
	h := ReadyHandler(pools)
	status, body := doReady(h, http.MethodGet, "/readyz")
	if status != http.StatusServiceUnavailable || body != "unavailable" {
		t.Fatalf("status=%d body=%q", status, body)
	}
	if strings.Contains(body, "127.0.0.1") || strings.Contains(strings.ToLower(body), "password") {
		t.Fatal("readyz leaked address or password")
	}

	status, body = doReady(h, http.MethodPost, "/readyz")
	if status == http.StatusOK || body == "ok" {
		t.Fatalf("POST status=%d body=%q", status, body)
	}
	status, _ = doReady(h, http.MethodGet, "/healthz")
	if status == http.StatusOK {
		t.Fatal("healthz implemented")
	}
	status, body = doReady(ReadyHandler(nil), http.MethodGet, "/readyz")
	if status != http.StatusServiceUnavailable || body != "unavailable" {
		t.Fatalf("nil pools status=%d body=%q", status, body)
	}
}

// ACC-13 规则：ReadyHandler 与 Close 并发不竞争；关闭后 ShardDB 返回错误。
func TestReadyHandlerCloseRace(t *testing.T) {
	db0, err := sqlx.Open("mysql", unreachableDSN)
	if err != nil {
		t.Fatal("open shard 0")
	}
	db1, err := sqlx.Open("mysql", unreachableDSN)
	if err != nil {
		t.Fatal("open shard 1")
	}
	t.Cleanup(func() {
		_ = db0.Close()
		_ = db1.Close()
	})
	pools := &Pools{db: [2]*sqlx.DB{db0, db1}, own: true}
	h := ReadyHandler(pools)

	// 请求 ctx 已取消，Ping 立即返回，读侧会反复经过 ShardDB。
	// 旧实现在这里和 Close 写 p.db 冲突，-race 能看见。
	reqCtx, cancel := context.WithCancel(context.Background())
	cancel()

	const readers = 8
	var started atomic.Int32
	var bad atomic.Bool
	var panicked atomic.Bool
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					panicked.Store(true)
				}
			}()
			started.Add(1)
			for range 400 {
				rec := httptest.NewRecorder()
				req := httptest.NewRequestWithContext(reqCtx, http.MethodGet, "/readyz", nil)
				h.ServeHTTP(rec, req)
				if rec.Body.String() != "unavailable" {
					bad.Store(true)
				}
			}
		}()
	}
	for started.Load() < readers {
	}
	if err := pools.Close(); err != nil {
		t.Fatal("close pools")
	}
	wg.Wait()
	if panicked.Load() {
		t.Fatal("readyz panicked")
	}
	if bad.Load() {
		t.Fatal("readyz body was not unavailable")
	}
	if _, err := pools.ShardDB(0); err == nil {
		t.Fatal("shard 0 still available")
	}
	if _, err := pools.ShardDB(1); err == nil {
		t.Fatal("shard 1 still available")
	}
}

func doReady(h http.Handler, method, path string) (int, string) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), method, path, nil)
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func mustSQLX(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := sqlx.Open("mysql", unreachableDSN)
	if err != nil {
		t.Fatalf("open = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func closed(db *sqlx.DB) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := db.PingContext(ctx)
	return err != nil && strings.Contains(err.Error(), "database is closed")
}
