//go:build integration

package left

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
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

// BASE-11 验收 2：MySQL 已经接受连接之后，单个包建库并迁移的耗时小于 2 秒。
func TestSchemaStartupUnderTwoSeconds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	// 墙上耗时。容器启动和拉镜像在 Ready 返回前结束，不计入。
	start := time.Now()
	schema, err := my.OpenSchema(ctx, "payment")
	elapsed := time.Since(start)
	t.Logf("BASE-11 startup_ms=%d mode=%s", elapsed.Milliseconds(), my.Mode())
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, schema) })
	if elapsed >= 2*time.Second {
		t.Fatalf("startup %d ms, want < 2000", elapsed.Milliseconds())
	}
	if testkit.ReuseMySQL() && my.Mode() != "reuse" {
		t.Fatalf("mode = %s, want reuse", my.Mode())
	}
	if !testkit.ReuseMySQL() && my.Mode() != "container" {
		t.Fatalf("mode = %s, want container", my.Mode())
	}
}

// BASE-11 验收 1：与 right 包同时运行时，只能看见本包插入的标记行。
func TestParallelPackageDoesNotSeePeer(t *testing.T) {
	assertIsolated(t, "left", "right", "tk-left", "tk-right")
}

func assertIsolated(t *testing.T, id, peer, marker, peerMarker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	schema, err := my.OpenSchema(ctx, "payment")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, schema) })
	if !regexp.MustCompile(`^tk_[0-9a-f]{16}$`).MatchString(schema.Name) {
		t.Fatalf("schema = %s", schema.Name)
	}
	var current string
	if err := schema.DB.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&current); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if current != schema.Name {
		t.Fatalf("database = %s, schema = %s", current, schema.Name)
	}
	if err := testkit.InsertMerchant(ctx, schema.DB, testkit.Merchant{MerchantID: marker}); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	pair, err := testkit.NewPair(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := pair.Barrier(ctx, "ready", peer); err != nil {
		t.Fatal(err)
	}
	own := countMerchant(t, ctx, schema, marker)
	other := countMerchant(t, ctx, schema, peerMarker)
	total := countMerchant(t, ctx, schema, "")
	if own != 1 || other != 0 || total != 1 {
		t.Fatalf("own=%d other=%d total=%d schema=%s", own, other, total, schema.Name)
	}
	if err := pair.Barrier(ctx, "checked", peer); err != nil {
		t.Fatal(err)
	}
}

func countMerchant(t *testing.T, ctx context.Context, schema *testkit.Schema, id string) int {
	t.Helper()
	q := "SELECT COUNT(*) FROM merchant"
	args := []any{}
	if id != "" {
		q += " WHERE merchant_id = ?"
		args = append(args, id)
	}
	var n int
	if err := schema.DB.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func closeSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}
