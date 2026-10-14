package testkit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Execer 是夹具插入用到的执行接口。*sqlx.DB 和 *sqlx.Tx 都满足。
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Merchant 是 payment.merchant 的一行。零值字段按列的默认约束补齐。
// 这是测试夹具，不是商户注册，也不会加密密钥。
type Merchant struct {
	MerchantID    string
	MerchantName  string
	AppID         string
	SecretCipher  []byte
	SecretVersion int
	NotifyURL     string
	FeeRateBP     int
	Status        string
}

// Account 是 account.account 的一行。
// 余额方向和是否透支在调用方没写时按账户类型补齐，用来满足 CHECK。
// 不校验分片，不幂等，不写分录。这不是 ACC-01 开户。
type Account struct {
	AccountNo        string
	OwnerType        string
	OwnerID          string
	AccountType      string
	Currency         string
	SubNo            int
	BalanceDirection string
	Balance          int64
	FrozenAmount     int64
	AllowOverdraft   int
	Status           string
}

// InsertMerchant 用参数化 SQL 插入一行商户。重复插入返回底层唯一键错误。
func InsertMerchant(ctx context.Context, db Execer, row Merchant) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil {
		return errors.New("testkit: nil db")
	}
	prepared, err := prepareMerchant(row)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO merchant (
			merchant_id, merchant_name, app_id, app_secret_cipher, secret_version,
			notify_url, fee_rate_bp, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		prepared.MerchantID, prepared.MerchantName, prepared.AppID, prepared.SecretCipher,
		prepared.SecretVersion, prepared.NotifyURL, prepared.FeeRateBP, prepared.Status,
	)
	if err != nil {
		return fmt.Errorf("testkit: insert merchant: %w", err)
	}
	return nil
}

// InsertAccount 用参数化 SQL 插入一行账户。重复插入返回底层唯一键错误。
func InsertAccount(ctx context.Context, db Execer, row Account) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil {
		return errors.New("testkit: nil db")
	}
	prepared, err := prepareAccount(row)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO account (
			account_no, owner_type, owner_id, account_type, currency, sub_no,
			balance_direction, balance, frozen_amount, allow_overdraft, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		prepared.AccountNo, prepared.OwnerType, prepared.OwnerID, prepared.AccountType,
		prepared.Currency, prepared.SubNo, prepared.BalanceDirection, prepared.Balance,
		prepared.FrozenAmount, prepared.AllowOverdraft, prepared.Status,
	)
	if err != nil {
		return fmt.Errorf("testkit: insert account: %w", err)
	}
	return nil
}

func prepareMerchant(row Merchant) (Merchant, error) {
	if row.MerchantID == "" {
		return Merchant{}, errors.New("testkit: merchant_id is required")
	}
	if utf8.RuneCountInString(row.MerchantID) > 32 {
		return Merchant{}, errors.New("testkit: merchant_id too long")
	}
	if row.MerchantName == "" {
		row.MerchantName = "测试商户" + row.MerchantID
	}
	if utf8.RuneCountInString(row.MerchantName) > 128 {
		return Merchant{}, errors.New("testkit: merchant_name too long")
	}
	if row.AppID == "" {
		row.AppID = "app_" + strings.ToLower(row.MerchantID)
	}
	if utf8.RuneCountInString(row.AppID) > 32 {
		return Merchant{}, errors.New("testkit: app_id too long")
	}
	if len(row.SecretCipher) == 0 {
		row.SecretCipher = []byte("fixture")
	}
	if len(row.SecretCipher) > 256 {
		return Merchant{}, errors.New("testkit: app_secret_cipher too long")
	}
	if row.SecretVersion == 0 {
		row.SecretVersion = 1
	}
	if row.Status == "" {
		row.Status = "ACTIVE"
	}
	if utf8.RuneCountInString(row.NotifyURL) > 512 {
		return Merchant{}, errors.New("testkit: notify_url too long")
	}
	return row, nil
}

func prepareAccount(row Account) (Account, error) {
	if row.AccountNo == "" || row.OwnerID == "" || row.AccountType == "" {
		return Account{}, errors.New("testkit: account_no, owner_id and account_type are required")
	}
	if utf8.RuneCountInString(row.AccountNo) > 32 || utf8.RuneCountInString(row.OwnerID) > 32 {
		return Account{}, errors.New("testkit: account field too long")
	}
	direction, overdraft, owner, err := accountRule(row.AccountType)
	if err != nil {
		return Account{}, err
	}
	if row.OwnerType == "" {
		row.OwnerType = owner
	}
	if row.BalanceDirection == "" {
		row.BalanceDirection = direction
		row.AllowOverdraft = overdraft
	}
	if row.Currency == "" {
		row.Currency = "CNY"
	}
	if row.Status == "" {
		row.Status = "ACTIVE"
	}
	return row, nil
}

// accountRule 只复述迁移里的 CHECK，用来给夹具填空。不是开户服务。
func accountRule(accountType string) (direction string, overdraft int, owner string, err error) {
	switch accountType {
	case "MERCHANT_SETTLE":
		return "CREDIT", 0, "MERCHANT", nil
	case "CHANNEL_RECEIVABLE":
		return "DEBIT", 1, "PLATFORM", nil
	case "FEE_INCOME", "TRANSIT":
		return "CREDIT", 1, "PLATFORM", nil
	default:
		return "", 0, "", fmt.Errorf("testkit: unknown account_type %s", accountType)
	}
}
