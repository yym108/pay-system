package seed

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql" // 注册 MySQL 驱动。
)

// 冲突时只把唯一键列写回自身，不更新余额和冻结金额。
// 已有商户的密文也不覆盖：nonce 随机，覆盖会让重复执行改变密文。
const insertAccountSQL = `
INSERT INTO account (
  account_no, owner_type, owner_id, account_type, currency, sub_no,
  balance_direction, balance, frozen_amount, allow_overdraft, status
) VALUES (?, ?, ?, ?, 'CNY', ?, ?, 0, 0, ?, 'ACTIVE')
ON DUPLICATE KEY UPDATE account_no = account_no`

const insertMerchantSQL = `
INSERT INTO merchant (
  merchant_id, merchant_name, app_id, app_secret_cipher, secret_version,
  notify_url, fee_rate_bp, status
) VALUES (?, ?, ?, ?, 1, '', ?, 'ACTIVE')
ON DUPLICATE KEY UPDATE merchant_id = merchant_id`

// DefaultSecretFormat 与 docs/05-api.md §3.7 的 M100001 测试向量对齐。
// 这是约定 §8 的演示默认值，不是生产密钥。
const DefaultSecretFormat = "demo_secret_%s_0123456789ab" //nolint:gosec // 演示格式，见约定 §8。

// DefaultPlatformSubAccounts 与约定 §8 的默认值一致。
const DefaultPlatformSubAccounts = 8

// Config 是一次种子执行的输入。调用方负责超时；本函数不读取环境变量。
type Config struct {
	AccountDSN0         string
	AccountDSN1         string
	PaymentDSN          string
	MasterKey           []byte
	PlatformSubAccounts int
	SecretFormat        string
	Load                bool
}

// Result 是执行后的行数，供命令行打印。测试应自己查库，不依赖这里的数字。
type Result struct {
	Accounts  [2]int
	Merchants int
}

type merchantSpec struct {
	id     string
	feeBP  int
	loaded bool
}

// Run 幂等写入平台子账户、演示商户，以及可选的压测商户。
// 不写分录，余额和冻结金额只在插入时写 0，重复执行不改已有余额。
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if cfg.PlatformSubAccounts <= 0 || cfg.PlatformSubAccounts > 1024 {
		return Result{}, fmt.Errorf("seed: PLATFORM_SUB_ACCOUNTS %d out of range", cfg.PlatformSubAccounts)
	}
	format := cfg.SecretFormat
	if format == "" {
		format = DefaultSecretFormat
	}
	if strings.Count(format, "%") != 1 || !strings.Contains(format, "%s") {
		return Result{}, fmt.Errorf("seed: secret format must contain exactly one %%s")
	}
	specs := demoMerchants()
	if cfg.Load {
		specs = append(specs, loadMerchants()...)
	}

	accountDB := [2]*sql.DB{}
	var err error
	accountDB[0], err = open(ctx, cfg.AccountDSN0)
	if err != nil {
		return Result{}, fmt.Errorf("seed: open account shard 0: %w", err)
	}
	defer func() { _ = accountDB[0].Close() }()
	accountDB[1], err = open(ctx, cfg.AccountDSN1)
	if err != nil {
		return Result{}, fmt.Errorf("seed: open account shard 1: %w", err)
	}
	defer func() { _ = accountDB[1].Close() }()
	paymentDB, err := open(ctx, cfg.PaymentDSN)
	if err != nil {
		return Result{}, fmt.Errorf("seed: open payment: %w", err)
	}
	defer func() { _ = paymentDB.Close() }()

	for shard := 0; shard < 2; shard++ {
		if err := seedPlatform(ctx, accountDB[shard], shard, cfg.PlatformSubAccounts); err != nil {
			return Result{}, err
		}
	}
	for _, spec := range specs {
		if err := seedMerchant(ctx, paymentDB, accountDB, cfg.MasterKey, format, spec); err != nil {
			return Result{}, err
		}
	}

	var result Result
	for shard := 0; shard < 2; shard++ {
		result.Accounts[shard], err = count(ctx, accountDB[shard], "SELECT COUNT(*) FROM account")
		if err != nil {
			return Result{}, err
		}
	}
	result.Merchants, err = count(ctx, paymentDB, "SELECT COUNT(*) FROM merchant")
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func seedPlatform(ctx context.Context, db *sql.DB, shard, subs int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("seed: begin shard %d: %w", shard, err)
	}
	defer func() { _ = tx.Rollback() }()

	platforms := []struct {
		accountType string
		ownerID     string
		direction   string
		overdraft   int
	}{
		{accountType: "CHANNEL_RECEIVABLE", ownerID: "mockpay", direction: "DEBIT", overdraft: 1},
		{accountType: "FEE_INCOME", ownerID: "PLATFORM", direction: "CREDIT", overdraft: 1},
		{accountType: "TRANSIT", ownerID: "PLATFORM", direction: "CREDIT", overdraft: 1},
	}
	for sub := 0; sub < subs; sub++ {
		for _, item := range platforms {
			accountNo, err := PlatformAccountNo(shard, item.accountType, sub)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, insertAccountSQL,
				accountNo, "PLATFORM", item.ownerID, item.accountType, sub, item.direction, item.overdraft,
			); err != nil {
				return fmt.Errorf("seed: platform account shard %d %s sub %d: %w", shard, item.accountType, sub, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("seed: commit shard %d: %w", shard, err)
	}
	return nil
}

func seedMerchant(ctx context.Context, payment *sql.DB, account [2]*sql.DB, key []byte, format string, spec merchantSpec) error {
	shard, err := ShardOf(spec.id)
	if err != nil {
		return err
	}
	plain := fmt.Sprintf(format, spec.id)
	cipherText, err := Seal(key, []byte(plain))
	if err != nil {
		return err
	}
	namePrefix := "演示商户"
	if spec.loaded {
		namePrefix = "压测商户"
	}
	appID := "app_" + strings.ToLower(spec.id)

	tx, err := payment.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("seed: begin merchant %s: %w", spec.id, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, insertMerchantSQL, spec.id, namePrefix+spec.id, appID, cipherText, spec.feeBP); err != nil {
		return fmt.Errorf("seed: insert merchant %s: %w", spec.id, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("seed: commit merchant %s: %w", spec.id, err)
	}

	accountNo, err := MerchantAccountNo(spec.id)
	if err != nil {
		return err
	}
	accTx, err := account[shard].BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("seed: begin settle %s: %w", spec.id, err)
	}
	defer func() { _ = accTx.Rollback() }()
	if _, err := accTx.ExecContext(ctx, insertAccountSQL,
		accountNo, "MERCHANT", spec.id, "MERCHANT_SETTLE", 0, "CREDIT", 0,
	); err != nil {
		return fmt.Errorf("seed: settle account %s: %w", spec.id, err)
	}
	if err := accTx.Commit(); err != nil {
		return fmt.Errorf("seed: commit settle %s: %w", spec.id, err)
	}
	return nil
}

func demoMerchants() []merchantSpec {
	return []merchantSpec{
		{id: "M100001", feeBP: 0},
		{id: "M100002", feeBP: 60},
		{id: "M100003", feeBP: 0},
	}
}

func loadMerchants() []merchantSpec {
	out := make([]merchantSpec, 0, 100)
	for n := 200000; n <= 200099; n++ {
		out = append(out, merchantSpec{id: fmt.Sprintf("M%d", n), feeBP: 0, loaded: true})
	}
	return out
}

func count(ctx context.Context, db *sql.DB, query string) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("seed: count: %w", err)
	}
	return n, nil
}
