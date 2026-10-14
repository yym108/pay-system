package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql" // 注册 MySQL 驱动。
)

// 五个本地账号。口令由调用方传入，本包不写死演示口令。
const (
	UserAccount     = "svc_account"
	UserPayment     = "svc_payment"
	UserRecon       = "svc_recon"
	UserChannelMock = "svc_channel_mock"
	UserMigrator    = "migrator"
)

// Config 是建库和授权的输入。Host 在宿主机上是 127.0.0.1，在 compose 网络内是 mysql。
type Config struct {
	Host                string
	Port                string
	RootPassword        string
	AccountPassword     string
	PaymentPassword     string
	ReconPassword       string
	ChannelMockPassword string
	MigratorPassword    string
}

// Apply 创建 5 个 schema，并按最小权限重建五个账号。
// 每次都 DROP 再 CREATE，避免历史上多授的权限留着。本地服务此时还没启动，断开旧连接可以接受。
func Apply(ctx context.Context, cfg Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == "" {
		cfg.Port = "3306"
	}
	passwords := []struct {
		name  string
		value string
	}{
		{"MYSQL_ROOT_PASSWORD", cfg.RootPassword},
		{"account password", cfg.AccountPassword},
		{"payment password", cfg.PaymentPassword},
		{"MYSQL_RECON_PASSWORD", cfg.ReconPassword},
		{"MYSQL_CHANNEL_MOCK_PASSWORD", cfg.ChannelMockPassword},
		{"MYSQL_MIGRATOR_PASSWORD", cfg.MigratorPassword},
	}
	for _, item := range passwords {
		if err := validatePassword(item.name, item.value); err != nil {
			return err
		}
	}

	db, err := sql.Open("mysql", fmt.Sprintf("root:%s@tcp(%s:%s)/?charset=utf8mb4&multiStatements=true&parseTime=true",
		cfg.RootPassword, cfg.Host, cfg.Port))
	if err != nil {
		return fmt.Errorf("bootstrap: open: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("bootstrap: ping: %w", err)
	}

	stmt, err := sqlText(cfg)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("bootstrap: apply: %s", redact(err.Error(),
			cfg.RootPassword, cfg.AccountPassword, cfg.PaymentPassword,
			cfg.ReconPassword, cfg.ChannelMockPassword, cfg.MigratorPassword))
	}
	return nil
}

func redact(message string, secrets ...string) string {
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		message = strings.ReplaceAll(message, secret, "***")
	}
	return message
}

func sqlText(cfg Config) (string, error) {
	var b strings.Builder
	for _, schema := range []string{"pay_account_0", "pay_account_1", "pay_payment", "pay_recon", "pay_channel_mock"} {
		fmt.Fprintf(&b, "CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;\n", schema)
		fmt.Fprintf(&b, "ALTER DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;\n", schema)
	}
	users := []struct {
		name     string
		password string
		grants   []string
	}{
		{
			name: UserAccount, password: cfg.AccountPassword,
			grants: []string{
				"GRANT SELECT, INSERT, UPDATE, DELETE ON `pay_account_0`.* TO 'svc_account'@'%'",
				"GRANT SELECT, INSERT, UPDATE, DELETE ON `pay_account_1`.* TO 'svc_account'@'%'",
			},
		},
		{
			name: UserPayment, password: cfg.PaymentPassword,
			grants: []string{
				"GRANT SELECT, INSERT, UPDATE, DELETE ON `pay_payment`.* TO 'svc_payment'@'%'",
			},
		},
		{
			name: UserRecon, password: cfg.ReconPassword,
			grants: []string{
				"GRANT SELECT, INSERT, UPDATE, DELETE ON `pay_recon`.* TO 'svc_recon'@'%'",
				"GRANT SELECT ON `pay_payment`.* TO 'svc_recon'@'%'",
				"GRANT SELECT ON `pay_account_0`.* TO 'svc_recon'@'%'",
				"GRANT SELECT ON `pay_account_1`.* TO 'svc_recon'@'%'",
			},
		},
		{
			name: UserChannelMock, password: cfg.ChannelMockPassword,
			grants: []string{
				"GRANT SELECT, INSERT, UPDATE, DELETE ON `pay_channel_mock`.* TO 'svc_channel_mock'@'%'",
			},
		},
		{
			name: UserMigrator, password: cfg.MigratorPassword,
			grants: []string{
				"GRANT ALL PRIVILEGES ON `pay_account_0`.* TO 'migrator'@'%'",
				"GRANT ALL PRIVILEGES ON `pay_account_1`.* TO 'migrator'@'%'",
				"GRANT ALL PRIVILEGES ON `pay_payment`.* TO 'migrator'@'%'",
				"GRANT ALL PRIVILEGES ON `pay_recon`.* TO 'migrator'@'%'",
				"GRANT ALL PRIVILEGES ON `pay_channel_mock`.* TO 'migrator'@'%'",
			},
		},
	}
	for _, user := range users {
		fmt.Fprintf(&b, "DROP USER IF EXISTS '%s'@'%%';\n", user.name)
		fmt.Fprintf(&b, "CREATE USER '%s'@'%%' IDENTIFIED BY '%s';\n", user.name, user.password)
		for _, grant := range user.grants {
			b.WriteString(grant)
			b.WriteString(";\n")
		}
	}
	return b.String(), nil
}

func validatePassword(name, password string) error {
	if password == "" {
		return fmt.Errorf("bootstrap: %s is empty", name)
	}
	if strings.ContainsAny(password, "'\\\"@/?# \t\r\n") {
		return fmt.Errorf("bootstrap: %s contains a character that cannot be embedded safely", name)
	}
	return nil
}

// SplitUserPassword 从 go-sql-driver DSN 取出 user 和 password。
// 只接受 user:password@tcp(host)/db 这种本地演示格式，不解析 URL 编码。
func SplitUserPassword(dsn string) (string, string, error) {
	user, rest, ok := strings.Cut(dsn, ":")
	if !ok || user == "" {
		return "", "", fmt.Errorf("bootstrap: dsn missing user")
	}
	password, _, ok := strings.Cut(rest, "@")
	if !ok || password == "" {
		return "", "", fmt.Errorf("bootstrap: dsn missing password")
	}
	if err := validatePassword("dsn password", password); err != nil {
		return "", "", err
	}
	return user, password, nil
}
