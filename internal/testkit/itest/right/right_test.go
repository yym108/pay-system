//go:build integration

package right

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
}

// BASE-11 验收 1：与 left 包同时运行时，只能看见本包插入的标记行。
func TestParallelPackageDoesNotSeePeer(t *testing.T) {
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
	if err := testkit.InsertMerchant(ctx, schema.DB, testkit.Merchant{MerchantID: "tk-right"}); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	pair, err := testkit.NewPair("right")
	if err != nil {
		t.Fatal(err)
	}
	if err := pair.Barrier(ctx, "ready", "left"); err != nil {
		t.Fatal(err)
	}
	var own, other, total int
	if err := schema.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM merchant WHERE merchant_id = ?", "tk-right").Scan(&own); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err := schema.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM merchant WHERE merchant_id = ?", "tk-left").Scan(&other); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err := schema.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM merchant").Scan(&total); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if own != 1 || other != 0 || total != 1 {
		t.Fatalf("own=%d other=%d total=%d schema=%s", own, other, total, schema.Name)
	}
	if err := pair.Barrier(ctx, "checked", "left"); err != nil {
		t.Fatal(err)
	}
}

func closeSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}
