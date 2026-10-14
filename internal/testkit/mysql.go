//go:build integration

package testkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yym108/pay-system/internal/dbmigrate"
	"github.com/yym108/pay-system/pkg/dbx"
)

const mysqlImage = "mysql:8.4.11"

// containerRootPassword 只存在于本进程启动的一次性容器里。
//
//nolint:gosec // 不是生产密钥。
const containerRootPassword = "test-root-not-for-production"

// schemaRE 是本包允许删除的库名。固定业务库对不上这个表达式。
var schemaRE = regexp.MustCompile(`^tk_[0-9a-f]{16}$`)

var (
	mysqlOnce sync.Once
	shared    *MySQL
	sharedErr error
)

// MySQL 是当前测试进程共用的一台 MySQL。
// 库表由 OpenSchema 建在随机 schema 里，不写入 pay_account_0 这类固定库。
type MySQL struct {
	mode  string
	dsn   string
	admin *sqlx.DB
	ctr   testcontainers.Container
}

// Schema 是一次测试自己的库。Close 只删除这个库。
type Schema struct {
	Name string
	DB   *sqlx.DB

	// dsn 含口令。只通过 DSN 交给需要再开池的测试，不写进日志。
	dsn   string
	admin *sqlx.DB
}

// ReuseMySQL 报告调用方是否设置了 TEST_MYSQL_DSN。
// 读取只发生在本包。该变量不进入服务配置。
func ReuseMySQL() bool {
	return strings.TrimSpace(os.Getenv("TEST_MYSQL_DSN")) != ""
}

// Ready 在 MySQL 接受连接后返回。
// TEST_MYSQL_DSN 非空时直接连那台实例，不启动容器。
// 否则启动 mysql:8.4.11，等到它监听 3306 并 Ping 成功。
// 建库和迁移不在这里，调用方从本函数返回之后再计时。
func Ready(ctx context.Context) (*MySQL, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mysqlOnce.Do(func() {
		shared, sharedErr = connect(ctx)
	})
	if sharedErr != nil {
		return nil, sharedErr
	}
	return shared, nil
}

// Shutdown 关闭本进程的管理连接。容器只在本进程启动过时才停掉。
// 不删除调用方没有 Close 的以外的库；复用模式下也不停外部 MySQL。
func Shutdown(ctx context.Context) error {
	if shared == nil {
		return nil
	}
	err := shared.Close(ctx)
	shared = nil
	return err
}

// Mode 返回 reuse 或 container。不含地址和口令。
func (m *MySQL) Mode() string {
	if m == nil {
		return ""
	}
	return m.mode
}

// Close 关闭管理连接，并在本进程启动了容器时停掉它。
func (m *MySQL) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}
	var err error
	if m.admin != nil {
		if cerr := m.admin.Close(); cerr != nil {
			err = cerr
		}
		m.admin = nil
	}
	if m.ctr != nil {
		terminate(ctx, m.ctr)
		m.ctr = nil
	}
	return err
}

// OpenSchema 创建随机库并执行 migrations/<service>。
// service 只能是 account、payment、recon、channel_mock。
// 计时若从 Ready 返回后开始，包含本函数的建库、迁移和 dbx.Open。
func (m *MySQL) OpenSchema(ctx context.Context, service string) (*Schema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m == nil || m.admin == nil {
		return nil, errors.New("testkit: mysql is not ready")
	}
	dir, err := migrationDir(service)
	if err != nil {
		return nil, err
	}
	name, err := newSchemaName()
	if err != nil {
		return nil, err
	}
	// 库名已经过 ^tk_[0-9a-f]{16}$，不能用来注入。
	_, err = m.admin.ExecContext(ctx, "CREATE DATABASE `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci") //nolint:gosec // name matches schemaRE
	if err != nil {
		return nil, errors.New("testkit: create schema: " + redactString(err.Error()))
	}
	schema := &Schema{Name: name, admin: m.admin}
	ok := false
	defer func() {
		if !ok {
			_ = schema.drop(ctx)
		}
	}()
	migURL, err := migrateURL(m.dsn, name)
	if err != nil {
		return nil, err
	}
	rememberText(migURL)
	if err := dbmigrate.Up(ctx, dir, migURL); err != nil {
		return nil, errors.New("testkit: migrate " + service + ": " + redactString(err.Error()))
	}
	dsn, err := schemaDSN(m.dsn, name)
	if err != nil {
		return nil, err
	}
	rememberText(dsn)
	schema.dsn = dsn
	db, err := dbx.Open(ctx, dsn, dbx.PoolConfig{})
	if err != nil {
		return nil, errors.New("testkit: open schema: " + redactString(err.Error()))
	}
	schema.DB = db
	ok = true
	return schema, nil
}

// DSN 返回这个随机库的连接串。返回值含口令。
// 只给测试再调用 dbx.Open。不要写入日志、错误文本或测试输出。
func (s *Schema) DSN() string {
	if s == nil {
		return ""
	}
	return s.dsn
}

// Close 关闭连接池并删除这个随机库。
func (s *Schema) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.drop(ctx)
}

func (s *Schema) drop(ctx context.Context) error {
	if s.DB != nil {
		_ = s.DB.Close()
		s.DB = nil
	}
	if s.admin == nil {
		return nil
	}
	if !safeSchema(s.Name) {
		return errors.New("testkit: refuse to drop schema")
	}
	_, err := s.admin.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+s.Name+"`") //nolint:gosec // name matches schemaRE
	if err != nil {
		return errors.New("testkit: drop schema: " + redactString(err.Error()))
	}
	return nil
}

func connect(ctx context.Context) (*MySQL, error) {
	dsn := strings.TrimSpace(os.Getenv("TEST_MYSQL_DSN"))
	m := &MySQL{mode: "container"}
	if dsn != "" {
		m.mode = "reuse"
		rememberText(dsn)
		if cfg, err := mysqlDriver.ParseDSN(dsn); err == nil {
			rememberPassword(cfg.Passwd)
		}
	} else {
		var err error
		dsn, m.ctr, err = startMySQL(ctx)
		if err != nil {
			return nil, err
		}
		rememberText(dsn)
		rememberPassword(containerRootPassword)
	}
	admin, err := dbx.Open(ctx, dsn, dbx.PoolConfig{})
	if err != nil {
		terminate(ctx, m.ctr)
		return nil, errors.New("testkit: open mysql: " + redactString(err.Error()))
	}
	m.admin = admin
	m.dsn = dsn
	return m, nil
}

func startMySQL(ctx context.Context) (string, testcontainers.Container, error) {
	rememberPassword(containerRootPassword)
	ctr, err := mysql.Run(ctx, mysqlImage,
		mysql.WithUsername("root"),
		mysql.WithPassword(containerRootPassword),
		mysql.WithDatabase("tk_admin"),
		testcontainers.WithLogger(log.NewNoopLogger()),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(2*time.Minute),
			wait.ForListeningPort("3306/tcp").WithStartupTimeout(2*time.Minute),
		)),
	)
	if err != nil {
		if ctr != nil {
			terminate(ctx, ctr)
		}
		return "", nil, errors.New("testkit: start mysql: " + redactString(err.Error()))
	}
	dsn, err := ctr.ConnectionString(ctx, "charset=utf8mb4")
	if err != nil {
		terminate(ctx, ctr)
		return "", nil, errors.New("testkit: mysql address: " + redactString(err.Error()))
	}
	return dsn, ctr, nil
}

func terminate(ctx context.Context, ctr testcontainers.Container) {
	if ctr == nil {
		return
	}
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()
	_ = ctr.Terminate(stopCtx)
}

func migrationDir(service string) (string, error) {
	switch service {
	case "account", "payment", "recon", "channel_mock":
	default:
		return "", fmt.Errorf("testkit: unknown migration service %q", service)
	}
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, "migrations", service)
	info, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("testkit: migration dir: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("testkit: migration path is not a directory")
	}
	return dir, nil
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("testkit: repository root not found")
		}
		dir = parent
	}
}

func newSchemaName() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	name := "tk_" + hex.EncodeToString(buf[:])
	if !safeSchema(name) {
		return "", errors.New("testkit: generated schema name rejected")
	}
	return name, nil
}

func safeSchema(name string) bool {
	if !schemaRE.MatchString(name) {
		return false
	}
	switch name {
	case "pay_account_0", "pay_account_1", "pay_payment", "pay_recon", "pay_channel_mock",
		"mysql", "information_schema", "performance_schema", "sys", "tk_admin":
		return false
	default:
		return true
	}
}

func schemaDSN(adminDSN, schema string) (string, error) {
	if !safeSchema(schema) {
		return "", errors.New("testkit: refuse schema name")
	}
	cfg, err := mysqlDriver.ParseDSN(adminDSN)
	if err != nil {
		return "", errors.New("testkit: parse mysql dsn")
	}
	cfg.DBName = schema
	out := cfg.FormatDSN()
	if !strings.Contains(out, "charset=") {
		if strings.Contains(out, "?") {
			out += "&charset=utf8mb4"
		} else {
			out += "?charset=utf8mb4"
		}
	}
	return out, nil
}

// migrateURL 只给 golang-migrate 用。应用连接走 dbx.Open，不走这条 URL。
// 驱动会先去掉 mysql:// 再 QueryUnescape 用户和口令，所以这里要转义。
func migrateURL(adminDSN, schema string) (string, error) {
	if !safeSchema(schema) {
		return "", errors.New("testkit: refuse schema name")
	}
	cfg, err := mysqlDriver.ParseDSN(adminDSN)
	if err != nil {
		return "", errors.New("testkit: parse mysql dsn")
	}
	if cfg.Addr == "" {
		return "", errors.New("testkit: mysql dsn has no address")
	}
	if cfg.Net != "" && cfg.Net != "tcp" {
		return "", errors.New("testkit: mysql dsn must use tcp")
	}
	return fmt.Sprintf("mysql://%s:%s@tcp(%s)/%s?multiStatements=true&charset=utf8mb4",
		url.QueryEscape(cfg.User), url.QueryEscape(cfg.Passwd), cfg.Addr, schema), nil
}
