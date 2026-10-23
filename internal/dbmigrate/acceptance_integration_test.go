//go:build integration

package dbmigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yym108/pay-system/internal/bootstrap"
	"github.com/yym108/pay-system/internal/dbschema"
	"github.com/yym108/pay-system/internal/seed"
	"github.com/yym108/pay-system/pkg/secretbox"
)

// 集成测试口令，只存在于一次性容器里，不是生产密钥。
//
//nolint:gosec
const (
	testRootPassword        = "test-root-not-for-production"
	testAccountPassword     = "test-account-not-for-production"
	testPaymentPassword     = "test-payment-not-for-production"
	testReconPassword       = "test-recon-not-for-production"
	testChannelMockPassword = "test-channel-not-for-production"
	testMigratorPassword    = "test-migrator-not-for-production"
)

// BASE-03 验收 1：空库迁移成功，两个账户分片表结构一致，down 后再 up 成功。
func TestBASE03MigrateShardsAndDownUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newMySQL(t, ctx)
	reset(t, ctx, env)

	if err := upAll(ctx, env); err != nil {
		t.Fatalf("空库 up：%v", err)
	}
	assertShardsEqual(t, ctx, env.root)

	if err := downAll(ctx, env); err != nil {
		t.Fatalf("down：%v", err)
	}
	if err := upAll(ctx, env); err != nil {
		t.Fatalf("down 后 up：%v", err)
	}
	assertShardsEqual(t, ctx, env.root)
}

// BASE-03 验收 2：种子连续两次，账户和商户数量不变，结算账户只在所属分片。
func TestBASE03SeedIdempotentAndShardPlacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newMySQL(t, ctx)
	reset(t, ctx, env)
	if err := upAll(ctx, env); err != nil {
		t.Fatalf("up：%v", err)
	}

	key := bytesKey()
	cfg := seed.Config{
		AccountDSN0:         env.dsn(bootstrap.UserAccount, testAccountPassword, "pay_account_0"),
		AccountDSN1:         env.dsn(bootstrap.UserAccount, testAccountPassword, "pay_account_1"),
		PaymentDSN:          env.dsn(bootstrap.UserPayment, testPaymentPassword, "pay_payment"),
		MasterKey:           key,
		PlatformSubAccounts: seed.DefaultPlatformSubAccounts,
		SecretFormat:        seed.DefaultSecretFormat,
	}
	if _, err := seed.Run(ctx, cfg); err != nil {
		t.Fatalf("第一次 seed：%v", err)
	}
	accounts, merchants, ciphers := snapshotSeed(t, ctx, env)
	if accounts[0] != 25 || accounts[1] != 26 || merchants != 3 {
		t.Fatalf("第一次数量 shard0=%d shard1=%d merchants=%d，期望 25/26/3", accounts[0], accounts[1], merchants)
	}
	assertSeedFeeTier(t, ctx, env, "M100002", 60)
	if _, err := seed.Run(ctx, cfg); err != nil {
		t.Fatalf("第二次 seed：%v", err)
	}
	againAccounts, againMerchants, againCiphers := snapshotSeed(t, ctx, env)
	if againAccounts != accounts || againMerchants != merchants {
		t.Fatalf("重复 seed 后数量变化：%v/%d -> %v/%d", accounts, merchants, againAccounts, againMerchants)
	}
	if againCiphers["M100001"] == "" || againCiphers["M100001"] != ciphers["M100001"] {
		t.Fatal("重复 seed 不应改写已有密文")
	}
	assertSettlementPlacement(t, ctx, env)
	assertZeroBalances(t, ctx, env)
	// BASE-14 兼容：已入库的种子密文用 pkg/secretbox 解开，明文与演示格式一致。
	for _, id := range []string{"M100001", "M100002", "M100003"} {
		assertPlaintext(t, key, ciphers[id], "demo_secret_"+id+"_0123456789ab")
	}
	assertSeedFeeTier(t, ctx, env, "M100002", 60)
}

// BASE-03 验收 3：svc_recon 写入 pay_payment、pay_account_0 被拒绝。
func TestBASE03ReconWriteDenied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newMySQL(t, ctx)
	reset(t, ctx, env)
	if err := upAll(ctx, env); err != nil {
		t.Fatalf("up：%v", err)
	}

	recon, err := sql.Open("mysql", env.dsn(bootstrap.UserRecon, testReconPassword, "pay_recon"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recon.Close() })
	var n int
	if err := recon.QueryRowContext(ctx, "SELECT COUNT(*) FROM pay_payment.merchant").Scan(&n); err != nil {
		t.Fatalf("只读查询应成功：%v", err)
	}
	_, err = recon.ExecContext(ctx, "INSERT INTO pay_payment.merchant (merchant_id) VALUES ('denied')")
	assertDenied(t, err, "pay_payment")
	_, err = recon.ExecContext(ctx, "INSERT INTO pay_account_0.account (account_no) VALUES ('denied')")
	assertDenied(t, err, "pay_account_0")
}

type mysqlEnv struct {
	host string
	port string
	root *sql.DB
}

func newMySQL(t *testing.T, ctx context.Context) *mysqlEnv {
	t.Helper()
	ctr, err := mysql.Run(ctx, "mysql:8.4.11",
		mysql.WithUsername("root"),
		mysql.WithPassword(testRootPassword),
		mysql.WithDatabase("bootstrap"),
		// 临时服务器的日志是 port: 0。等到真正监听 3306，避免连上正在退出的进程。
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(2*time.Minute),
			wait.ForListeningPort("3306/tcp").WithStartupTimeout(2*time.Minute),
		)),
	)
	if err != nil {
		t.Fatalf("启动 mysql:8.4.11：%v", err)
	}
	t.Cleanup(terminateContainer(t, ctr)) //nolint:contextcheck // 清理时测试 context 可能已取消。
	dsn, err := ctr.ConnectionString(ctx, "charset=utf8mb4", "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	host, port := hostPort(t, dsn)
	root, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	if err := root.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	return &mysqlEnv{host: host, port: port, root: root}
}

func (e *mysqlEnv) dsn(user, password, schema string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&multiStatements=true",
		user, password, e.host, e.port, schema)
}

func reset(t *testing.T, ctx context.Context, env *mysqlEnv) {
	t.Helper()
	if _, err := env.root.ExecContext(ctx, `
		DROP DATABASE IF EXISTS pay_account_0;
		DROP DATABASE IF EXISTS pay_account_1;
		DROP DATABASE IF EXISTS pay_payment;
		DROP DATABASE IF EXISTS pay_recon;
		DROP DATABASE IF EXISTS pay_channel_mock`); err != nil {
		t.Fatalf("清理 schema：%v", err)
	}
	if err := bootstrap.Apply(ctx, bootstrap.Config{
		Host:                env.host,
		Port:                env.port,
		RootPassword:        testRootPassword,
		AccountPassword:     testAccountPassword,
		PaymentPassword:     testPaymentPassword,
		ReconPassword:       testReconPassword,
		ChannelMockPassword: testChannelMockPassword,
		MigratorPassword:    testMigratorPassword,
	}); err != nil {
		t.Fatalf("bootstrap：%v", err)
	}
}

func upAll(ctx context.Context, env *mysqlEnv) error {
	root := repoRoot()
	for _, schema := range Schemas(root) {
		url := MySQLURL(bootstrap.UserMigrator, testMigratorPassword, env.host, env.port, schema.Name)
		if err := Up(ctx, schema.Dir, url); err != nil {
			return fmt.Errorf("%s: %w", schema.Name, err)
		}
	}
	return nil
}

func downAll(ctx context.Context, env *mysqlEnv) error {
	root := repoRoot()
	for _, schema := range Schemas(root) {
		url := MySQLURL(bootstrap.UserMigrator, testMigratorPassword, env.host, env.port, schema.Name)
		if err := Down(ctx, schema.Dir, url); err != nil {
			return fmt.Errorf("%s: %w", schema.Name, err)
		}
	}
	return nil
}

func assertShardsEqual(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	diff, err := dbschema.DiffAccountShards(ctx, db)
	if err != nil {
		t.Fatalf("比较分片：%v", err)
	}
	if diff != "" {
		t.Fatalf("两个账户分片结构不一致：\n%s", diff)
	}
}

func snapshotSeed(t *testing.T, ctx context.Context, env *mysqlEnv) ([2]int, int, map[string]string) {
	t.Helper()
	var accounts [2]int
	for shard, schema := range []string{"pay_account_0", "pay_account_1"} {
		query, err := shardQuery(schema, "SELECT COUNT(*) FROM %s.account")
		if err != nil {
			t.Fatal(err)
		}
		if err := env.root.QueryRowContext(ctx, query).Scan(&accounts[shard]); err != nil {
			t.Fatal(err)
		}
	}
	var merchants int
	if err := env.root.QueryRowContext(ctx, "SELECT COUNT(*) FROM pay_payment.merchant").Scan(&merchants); err != nil {
		t.Fatal(err)
	}
	rows, err := env.root.QueryContext(ctx, "SELECT merchant_id, app_secret_cipher FROM pay_payment.merchant")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	ciphers := map[string]string{}
	for rows.Next() {
		var id string
		var cipher []byte
		if err := rows.Scan(&id, &cipher); err != nil {
			t.Fatal(err)
		}
		ciphers[id] = string(cipher)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return accounts, merchants, ciphers
}

func assertSettlementPlacement(t *testing.T, ctx context.Context, env *mysqlEnv) {
	t.Helper()
	for shard, schema := range []string{"pay_account_0", "pay_account_1"} {
		if err := eachSettlement(ctx, env.root, schema, func(owner string) {
			got, err := seed.ShardOf(owner)
			if err != nil {
				t.Fatal(err)
			}
			if got != shard {
				t.Errorf("%s 的结算账户出现在分片 %d", owner, shard)
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func eachSettlement(ctx context.Context, db *sql.DB, schema string, fn func(string)) error {
	query, err := shardQuery(schema, "SELECT owner_id FROM %s.account WHERE account_type = 'MERCHANT_SETTLE'")
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	seen := 0
	for rows.Next() {
		var owner string
		if err := rows.Scan(&owner); err != nil {
			return err
		}
		fn(owner)
		seen++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seen == 0 {
		return fmt.Errorf("%s 没有结算账户", schema)
	}
	return nil
}

func assertZeroBalances(t *testing.T, ctx context.Context, env *mysqlEnv) {
	t.Helper()
	for _, schema := range []string{"pay_account_0", "pay_account_1"} {
		badQuery, err := shardQuery(schema, "SELECT COUNT(*) FROM %s.account WHERE balance <> 0 OR frozen_amount <> 0")
		if err != nil {
			t.Fatal(err)
		}
		txnQuery, err := shardQuery(schema, "SELECT COUNT(*) FROM %s.ledger_txn")
		if err != nil {
			t.Fatal(err)
		}
		entryQuery, err := shardQuery(schema, "SELECT COUNT(*) FROM %s.ledger_entry")
		if err != nil {
			t.Fatal(err)
		}
		var bad, txns, entries int
		if err := env.root.QueryRowContext(ctx, badQuery).Scan(&bad); err != nil {
			t.Fatal(err)
		}
		if err := env.root.QueryRowContext(ctx, txnQuery).Scan(&txns); err != nil {
			t.Fatal(err)
		}
		if err := env.root.QueryRowContext(ctx, entryQuery).Scan(&entries); err != nil {
			t.Fatal(err)
		}
		if bad != 0 || txns != 0 || entries != 0 {
			t.Fatalf("%s 种子写了余额或分录：nonzero=%d txn=%d entry=%d", schema, bad, txns, entries)
		}
	}
}

// PAY-10 验收 7：M100002 只有一档 60bp，生效时间等于该商户 created_at。
func assertSeedFeeTier(t *testing.T, ctx context.Context, env *mysqlEnv, merchantID string, rate int) {
	t.Helper()
	var n, gotRate int
	var effective, created time.Time
	err := env.root.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(MIN(r.fee_rate_bp), -1)
FROM pay_payment.merchant_fee_rate r
WHERE r.merchant_id = ?`, merchantID).Scan(&n, &gotRate)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || gotRate != rate {
		t.Fatalf("%s 费率档数=%d 费率=%d，期望 1 档 %d", merchantID, n, gotRate, rate)
	}
	err = env.root.QueryRowContext(ctx, `
SELECT r.effective_from, m.created_at
FROM pay_payment.merchant_fee_rate r
JOIN pay_payment.merchant m ON m.merchant_id = r.merchant_id
WHERE r.merchant_id = ?`, merchantID).Scan(&effective, &created)
	if err != nil {
		t.Fatal(err)
	}
	if !effective.Equal(created) {
		t.Fatalf("%s 生效时间 %s 不等于 created_at %s", merchantID, effective.Format(time.RFC3339Nano), created.Format(time.RFC3339Nano))
	}
}

func assertPlaintext(t *testing.T, key []byte, stored, want string) {
	t.Helper()
	box, err := secretbox.New(key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := box.Open([]byte(stored))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("解密结果与演示明文不一致")
	}
}

// terminateContainer 不接收测试 context。清理执行时那个 context 可能已经取消。
func terminateContainer(t *testing.T, ctr testcontainers.Container) func() {
	t.Helper()
	return func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := ctr.Terminate(stopCtx); err != nil {
			t.Errorf("停止 mysql：%v", err)
		}
	}
}

func shardQuery(schema, format string) (string, error) {
	switch schema {
	case "pay_account_0", "pay_account_1":
	default:
		return "", fmt.Errorf("unexpected schema %s", schema)
	}
	return fmt.Sprintf(format, schema), nil
}

func assertDenied(t *testing.T, err error, target string) {
	t.Helper()
	if err == nil {
		t.Fatalf("svc_recon 写入 %s 未被拒绝", target)
	}
	var me *mysqlDriver.MySQLError
	if !errors.As(err, &me) || (me.Number != 1142 && me.Number != 1044) {
		t.Fatalf("svc_recon 写入 %s 的错误不是拒绝访问：%v", target, err)
	}
}

func bytesKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = 0x11
	}
	return key
}

func hostPort(t *testing.T, dsn string) (string, string) {
	t.Helper()
	_, rest, ok := strings.Cut(dsn, "@tcp(")
	if !ok {
		t.Fatalf("无法解析 DSN 主机")
	}
	addr, _, _ := strings.Cut(rest, ")")
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return host, port
}

func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("go.mod not found")
		}
		dir = parent
	}
}
