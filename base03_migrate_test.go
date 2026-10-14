package base

import (
	"os"
	"strings"
	"testing"
)

// BASE-03 规则 1：迁移按服务拆分，账户分片共用 migrations/account，设计稿已删除。
func TestBASE03MigrationLayout(t *testing.T) {
	files := []string{
		"migrations/account/000001_init.up.sql",
		"migrations/account/000001_init.down.sql",
		"migrations/payment/000001_init.up.sql",
		"migrations/payment/000001_init.down.sql",
		"migrations/recon/000001_init.up.sql",
		"migrations/recon/000001_init.down.sql",
		"migrations/channel_mock/000001_init.up.sql",
		"migrations/channel_mock/000001_init.down.sql",
	}
	for _, path := range files {
		content, err := os.ReadFile(path) //nolint:gosec // path 来自上面的固定列表。
		if err != nil {
			t.Errorf("缺少 %s：%v", path, err)
			continue
		}
		sqlText := stripSQLComments(string(content))
		if strings.Contains(path, ".up.sql") && !strings.Contains(sqlText, "CREATE TABLE") {
			t.Errorf("%s 没有 CREATE TABLE", path)
		}
		if strings.Contains(sqlText, "CREATE DATABASE") {
			t.Errorf("%s 不应 CREATE DATABASE，建库属于 bootstrap", path)
		}
	}
	if _, err := os.Stat("docs/schema/schema_draft.sql"); !os.IsNotExist(err) {
		t.Fatal("设计稿 docs/schema/schema_draft.sql 应该已经删除")
	}
	account, err := os.ReadFile("migrations/account/000001_init.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"CREATE TABLE account", "CREATE TABLE ledger_txn", "CREATE TABLE ledger_entry", "CREATE TABLE account_freeze", "CREATE TABLE transfer_branch"} {
		if !strings.Contains(string(account), table) {
			t.Errorf("账户迁移缺少 %s", table)
		}
	}
	if strings.Contains(string(account), "LIKE pay_account") || strings.Contains(string(account), "CREATE TABLE pay_account_1") {
		t.Error("账户迁移不应再写 pay_account_1 的 LIKE 复制")
	}
}

func stripSQLComments(sqlText string) string {
	var b strings.Builder
	for _, line := range strings.Split(sqlText, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}
