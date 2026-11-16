//go:build integration

package chaos_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/service/account/biz"
	"github.com/yym108/pay-system/service/account/dal"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = testkit.Shutdown(ctx)
	os.Exit(code)
}

// QA-02 验收 3：verify.sh 在正常数据上退出码 0；测试中用 SQL 篡改一笔余额、删除一张 PAY 凭证，分别使其退出码非 0 并指出对应检查项。
func TestVerifyScript(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	m, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatalf("testkit.Ready: %v", err)
	}

	paySchema, err := m.OpenSchema(ctx, "payment")
	if err != nil {
		t.Fatalf("open payment schema: %v", err)
	}
	defer func() { _ = paySchema.Close(context.Background()) }()

	acc0Schema, err := m.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatalf("open account 0 schema: %v", err)
	}
	defer func() { _ = acc0Schema.Close(context.Background()) }()

	acc1Schema, err := m.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatalf("open account 1 schema: %v", err)
	}
	defer func() { _ = acc1Schema.Close(context.Background()) }()

	// 准备一组正常闭合的数据：1 笔 SUCCESS 支付单 + 对应 PAY 凭证与账户余额。
	seedValidVerifyData(t, ctx, paySchema, acc0Schema, acc1Schema)

	// 用真实 biz.CheckBalance 驱动 mock admin server。
	pools, err := dal.Open(ctx, acc0Schema.DSN(), acc1Schema.DSN(), dbx.PoolConfig{})
	if err != nil {
		t.Fatalf("dal.Open: %v", err)
	}
	defer func() { _ = pools.Close() }()

	ids, err := idgen.New(1, clock.Real{})
	if err != nil {
		t.Fatalf("idgen.New: %v", err)
	}
	accSvc := biz.New(pools, ids, 1)

	adminSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/accounts/check":
			res, cerr := accSvc.CheckBalance(r.Context(), "", true)
			if cerr != nil {
				http.Error(w, cerr.Error(), http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"msg":  "ok",
				"data": map[string]any{
					"passed":   res.Passed,
					"failures": res.Failures,
				},
			})
		case "/admin/recon/run":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"msg":  "ok",
				"data": map[string]any{
					"batch": map[string]any{
						"batch_no":   "RB0000000000000000001",
						"status":     "DONE",
						"diff_count": 0,
					},
					"diffs":        []any{},
					"manual_total": 0,
					"manual_diffs": []any{},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer adminSrv.Close()

	repoRoot := findRepoRoot(t)
	scriptPath := filepath.Join(repoRoot, "test", "chaos", "verify.sh")
	mysqlHost, mysqlPort, mysqlUser, mysqlPass := parseDSNForCLI(t, paySchema.DSN())

	runVerify := func() (int, string) {
		//nolint:gosec // 测试内执行仓库固定路径脚本 test/chaos/verify.sh。
		cmd := exec.CommandContext(ctx, "sh", scriptPath)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(),
			"PAYMENT_DB="+paySchema.Name,
			"ACCOUNT_DB_0="+acc0Schema.Name,
			"ACCOUNT_DB_1="+acc1Schema.Name,
			"MYSQL_HOST="+mysqlHost,
			"MYSQL_PORT="+mysqlPort,
			"MYSQL_USER="+mysqlUser,
			"MYSQL_PASSWORD="+mysqlPass,
			"GATEWAY_ADMIN_URL="+adminSrv.URL,
			"ADMIN_TOKEN=test-admin-token-000000000000000000",
			"VERIFY_SKIP_ENV_FILE=1",
		)
		out, err := cmd.CombinedOutput()
		if err == nil {
			return 0, string(out)
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode(), string(out)
		}
		t.Fatalf("exec verify.sh: %v\n%s", err, string(out))
		return -1, ""
	}

	// 1. 正常数据应退出码 0。
	code, out := runVerify()
	if code != 0 {
		t.Fatalf("expected exit 0 on valid data, got %d:\n%s", code, out)
	}

	// 2. 用 SQL 篡改一笔余额，verify.sh 应退出码非 0 并指出对应检查项（CheckBalance / BALANCE_SUM）。
	if _, err := acc1Schema.DB.ExecContext(ctx, `UPDATE account SET balance = balance + 1 WHERE owner_id = 'M100001' AND account_type = 'MERCHANT_SETTLE'`); err != nil {
		t.Fatalf("tamper balance: %v", err)
	}
	code, out = runVerify()
	if code == 0 {
		t.Fatalf("expected non-zero exit after tampering balance, got 0:\n%s", out)
	}
	if !strings.Contains(out, "BALANCE_SUM") && !strings.Contains(out, "CheckBalance") {
		t.Fatalf("expected output to mention BALANCE_SUM or CheckBalance, got:\n%s", out)
	}
	// 恢复余额。
	if _, err := acc1Schema.DB.ExecContext(ctx, `UPDATE account SET balance = balance - 1 WHERE owner_id = 'M100001' AND account_type = 'MERCHANT_SETTLE'`); err != nil {
		t.Fatalf("restore balance: %v", err)
	}

	// 3. 删除一张 PAY 凭证，verify.sh 应退出码非 0 并指出对应检查项（PAY_LEDGER_TXN）。
	if _, err := acc1Schema.DB.ExecContext(ctx, `DELETE FROM ledger_txn WHERE biz_type = 'PAY' AND biz_no = 'PA0000000000000000001'`); err != nil {
		t.Fatalf("delete ledger_txn: %v", err)
	}
	code, out = runVerify()
	if code == 0 {
		t.Fatalf("expected non-zero exit after deleting PAY ledger_txn, got 0:\n%s", out)
	}
	if !strings.Contains(out, "PAY_LEDGER_TXN") {
		t.Fatalf("expected output to mention PAY_LEDGER_TXN, got:\n%s", out)
	}
}

func seedValidVerifyData(t *testing.T, ctx context.Context, paySchema, acc0Schema, acc1Schema *testkit.Schema) {
	t.Helper()

	// 1 笔 SUCCESS 支付单（M100001，分片 1）。
	_, err := paySchema.DB.ExecContext(ctx, `
		INSERT INTO payment_order (
			pay_no, merchant_id, out_trade_no, amount, fee_amount, refunded_amount,
			currency, subject, status, channel, expire_at, paid_at
		) VALUES (
			'PA0000000000000000001', 'M100001', 'T_VERIFY_001', 10000, 0, 0,
			'CNY', 'verify test', 'SUCCESS', 'mockpay', DATE_ADD(NOW(), INTERVAL 15 MINUTE), NOW()
		)`)
	if err != nil {
		t.Fatalf("insert payment_order: %v", err)
	}

	// 分片 1 的账户：M100001 结算户（CREDIT +10000）与 mockpay 应收户（DEBIT +10000）。
	_, err = acc1Schema.DB.ExecContext(ctx, `
		INSERT INTO account (
			account_no, owner_type, owner_id, account_type, currency, sub_no,
			balance_direction, balance, frozen_amount, allow_overdraft, status
		) VALUES
		('AC0000000000000000001', 'MERCHANT', 'M100001', 'MERCHANT_SETTLE', 'CNY', 0, 'CREDIT', 10000, 0, 0, 'ACTIVE'),
		('AC0000000000000000002', 'PLATFORM', 'mockpay', 'CHANNEL_RECEIVABLE', 'CNY', 0, 'DEBIT', 10000, 0, 1, 'ACTIVE')
	`)
	if err != nil {
		t.Fatalf("insert accounts: %v", err)
	}

	// 分片 1 的 PAY 凭证及分录。
	_, err = acc1Schema.DB.ExecContext(ctx, `
		INSERT INTO ledger_txn (txn_no, biz_type, biz_no, amount, entry_count)
		VALUES ('TX0000000000000000001', 'PAY', 'PA0000000000000000001', 10000, 2)
	`)
	if err != nil {
		t.Fatalf("insert ledger_txn: %v", err)
	}
	_, err = acc1Schema.DB.ExecContext(ctx, `
		INSERT INTO ledger_entry (txn_no, account_id, direction, amount, balance_after, biz_type, biz_no)
		SELECT 'TX0000000000000000001', a.id, 'DEBIT', 10000, 10000, 'PAY', 'PA0000000000000000001'
		FROM account a WHERE a.account_no = 'AC0000000000000000002'
	`)
	if err != nil {
		t.Fatalf("insert debit ledger_entry: %v", err)
	}
	_, err = acc1Schema.DB.ExecContext(ctx, `
		INSERT INTO ledger_entry (txn_no, account_id, direction, amount, balance_after, biz_type, biz_no)
		SELECT 'TX0000000000000000001', a.id, 'CREDIT', 10000, 10000, 'PAY', 'PA0000000000000000001'
		FROM account a WHERE a.account_no = 'AC0000000000000000001'
	`)
	if err != nil {
		t.Fatalf("insert credit ledger_entry: %v", err)
	}
	_ = acc0Schema
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.mod not found")
		}
		dir = parent
	}
}

func parseDSNForCLI(t *testing.T, dsn string) (host, port, user, pass string) {
	t.Helper()
	// 格式：user:pass@tcp(host:port)/dbname?query
	at := strings.Index(dsn, "@tcp(")
	if at < 0 {
		t.Fatalf("unexpected dsn format")
	}
	creds := dsn[:at]
	colon := strings.Index(creds, ":")
	if colon < 0 {
		user = creds
	} else {
		user = creds[:colon]
		pass = creds[colon+1:]
	}
	rest := dsn[at+len("@tcp("):]
	closeParen := strings.Index(rest, ")")
	if closeParen < 0 {
		t.Fatalf("unexpected dsn tcp part")
	}
	hp := rest[:closeParen]
	h, p, err := netSplitHostPort(hp)
	if err != nil {
		t.Fatalf("split host port %q: %v", hp, err)
	}
	_ = url.QueryEscape
	return h, p, user, pass
}

func netSplitHostPort(hp string) (string, string, error) {
	i := strings.LastIndex(hp, ":")
	if i < 0 {
		return hp, "3306", nil
	}
	return hp[:i], hp[i+1:], nil
}
