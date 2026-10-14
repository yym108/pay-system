//go:build integration

package dal

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/dbx"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := testkit.Shutdown(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %s\n", testkit.Redact(err))
	}
	os.Exit(code)
}

// ACC-13 验收：两个库上，WithTx 的事务只能看到自己的库名。
func TestWithTxSeesOwnDatabase(t *testing.T) {
	ctx, s0, s1, pools := openTwoSchemas(t)
	assertDatabase(t, ctx, pools, 0, s0.Name)
	assertDatabase(t, ctx, pools, 1, s1.Name)
	if s0.Name == s1.Name {
		t.Fatal("two schemas share a name")
	}
}

// ACC-13 验收：两个池都能 Ping 时 /readyz 返回 200；关掉一个分片连接池后返回 503。
func TestReadyzWhenShardClosed(t *testing.T) {
	ctx, _, _, pools := openTwoSchemas(t)
	h := ReadyHandler(pools)
	status, body := doReady(h, http.MethodGet, "/readyz")
	if status != http.StatusOK || body != "ok" {
		t.Fatalf("ready status=%d body=%q", status, body)
	}

	db1, err := pools.ShardDB(1)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err := db1.Close(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	status, body = doReady(h, http.MethodGet, "/readyz")
	if status != http.StatusServiceUnavailable || body != "unavailable" {
		t.Fatalf("closed status=%d body=%q", status, body)
	}
	if strings.Contains(body, "127.0.0.1") || strings.Contains(strings.ToLower(body), "password") || strings.Contains(body, "@tcp(") {
		t.Fatal("readyz leaked address or password")
	}

	db0, err := pools.ShardDB(0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := db0.PingContext(pingCtx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

// ACC-13 规则：第二个分片打开失败时关闭第一个。
func TestOpenSecondFailureClosesFirst(t *testing.T) {
	ctx, s0, s1, _ := openTwoSchemas(t)
	if s0.DSN() == "" || s1.DSN() == "" {
		t.Fatal("missing schema dsn")
	}
	before := connCount(t, ctx, s0.DB, s0.Name)
	_, err := Open(ctx, s0.DSN(), "invalid", dbx.PoolConfig{MaxOpen: 1, MaxIdle: 1})
	if err == nil {
		t.Fatal("second open succeeded")
	}
	if strings.Contains(err.Error(), s0.DSN()) {
		t.Fatal("error leaked dsn")
	}
	waitConnCount(t, ctx, s0.DB, s0.Name, before)

	pools, err := Open(ctx, s0.DSN(), s1.DSN(), dbx.PoolConfig{MaxOpen: 2, MaxIdle: 1})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() {
		if cerr := pools.Close(); cerr != nil {
			t.Errorf("close opened pools: %s", testkit.Redact(cerr))
		}
	})
	db, err := pools.ShardDB(0)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	var name string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&name); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if name != s0.Name {
		t.Fatalf("opened shard 0 database = %s", name)
	}
}

func openTwoSchemas(t *testing.T) (context.Context, *testkit.Schema, *testkit.Schema, *Pools) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	s0, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, s0) })
	s1, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, s1) })
	assertRandomSchema(t, s0.Name)
	assertRandomSchema(t, s1.Name)
	return ctx, s0, s1, New(s0.DB, s1.DB)
}

func assertDatabase(t *testing.T, ctx context.Context, pools *Pools, shard int, want string) {
	t.Helper()
	db, err := pools.ShardDB(shard)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	err = dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		var name string
		if err := tx.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&name); err != nil {
			return err
		}
		if name != want {
			return fmt.Errorf("shard %d database = %s", shard, name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func assertRandomSchema(t *testing.T, name string) {
	t.Helper()
	if !regexp.MustCompile(`^tk_[0-9a-f]{16}$`).MatchString(name) {
		t.Fatalf("schema = %s", name)
	}
	switch name {
	case "pay_account_0", "pay_account_1", "pay_payment", "pay_recon", "pay_channel_mock":
		t.Fatalf("refusing fixed schema %s", name)
	}
}

func connCount(t *testing.T, ctx context.Context, db *sqlx.DB, schema string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.processlist WHERE db = ?", schema).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func waitConnCount(t *testing.T, ctx context.Context, db *sqlx.DB, schema string, want int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	last := connCount(t, ctx, db, schema)
	if last <= want {
		return
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatal(testkit.Redact(ctx.Err()))
		case <-deadline.C:
			t.Fatalf("connections = %d, want <= %d", last, want)
		case <-ticker.C:
			last = connCount(t, ctx, db, schema)
			if last <= want {
				return
			}
		}
	}
}

func closeSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}
