package dbmigrate

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql" // 注册 mysql:// 驱动，版本与镜像 migrate/migrate:v4.20.1 相同。
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// Schema 是一次迁移的目标和目录。账户两个分片共用 migrations/account。
type Schema struct {
	Name string
	Dir  string
}

// Schemas 是空库上要执行的全部迁移。顺序无关，没有跨库外键。
func Schemas(root string) []Schema {
	return []Schema{
		{Name: "pay_account_0", Dir: root + "/migrations/account"},
		{Name: "pay_account_1", Dir: root + "/migrations/account"},
		{Name: "pay_payment", Dir: root + "/migrations/payment"},
		{Name: "pay_recon", Dir: root + "/migrations/recon"},
		{Name: "pay_channel_mock", Dir: root + "/migrations/channel_mock"},
	}
}

// Up 把一个库迁到最新。已经是最新时不当作错误。
func Up(ctx context.Context, migrationsDir, databaseURL string) error {
	return run(ctx, migrationsDir, databaseURL, func(m *migrate.Migrate) error {
		err := m.Up()
		if errors.Is(err, migrate.ErrNoChange) {
			return nil
		}
		return err
	})
}

// Down 回滚该目录的全部版本。调用方应先 Up 过；空库上的 down 会返回错误。
func Down(ctx context.Context, migrationsDir, databaseURL string) error {
	return run(ctx, migrationsDir, databaseURL, func(m *migrate.Migrate) error {
		return m.Down()
	})
}

func run(ctx context.Context, migrationsDir, databaseURL string, fn func(*migrate.Migrate) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, err := fileURL(migrationsDir)
	if err != nil {
		return err
	}
	m, err := migrate.New(source, databaseURL)
	if err != nil {
		return fmt.Errorf("dbmigrate: open: %w", err)
	}
	defer func() {
		_, _ = m.Close()
	}()
	if err := fn(m); err != nil {
		return fmt.Errorf("dbmigrate: %w", err)
	}
	return nil
}

func fileURL(dir string) (string, error) {
	if strings.Contains(dir, "?") {
		return "", fmt.Errorf("dbmigrate: migration dir %q is not a local path", dir)
	}
	u := url.URL{Scheme: "file", Path: dir}
	return u.String(), nil
}

// MySQLURL 组装 golang-migrate 使用的数据库 URL。口令不得包含需要转义的字符，由 bootstrap 校验。
func MySQLURL(user, password, host, port, schema string) string {
	return fmt.Sprintf("mysql://%s:%s@tcp(%s:%s)/%s?multiStatements=true&charset=utf8mb4",
		user, password, host, port, schema)
}
