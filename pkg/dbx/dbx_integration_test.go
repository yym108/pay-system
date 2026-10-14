//go:build integration

package dbx

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

// 一次性容器口令，不是生产密钥。
//
//nolint:gosec
const testRootPassword = "test-root-not-for-production"

var (
	schemaNameRE = regexp.MustCompile(`^dbx_[0-9a-f]{16}$`)

	mysqlOnce     sync.Once
	adminDSN      string
	adminDB       *sqlx.DB
	stopMySQL     func()
	mysqlStartErr error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if adminDB != nil {
		_ = adminDB.Close()
	}
	if stopMySQL != nil {
		stopMySQL()
	}
	os.Exit(code)
}

// BASE-08 验收 1：fn 返回错误时数据未写入；fn panic 时回滚并重新抛出。
func TestWithTxErrorAndPanic(t *testing.T) {
	ctx, db := newSchema(t)
	mustExec(t, ctx, db, `CREATE TABLE dbx_event (
		id BIGINT NOT NULL PRIMARY KEY,
		note VARCHAR(32) NOT NULL
	) ENGINE=InnoDB`)

	t.Run("error", func(t *testing.T) {
		sentinel := errors.New("base-08 rollback")
		err := WithTx(ctx, db, func(tx *sqlx.Tx) error {
			if _, err := tx.ExecContext(ctx, "INSERT INTO dbx_event (id, note) VALUES (?, ?)", 1, "nope"); err != nil {
				return err
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("WithTx = %s", sanitize(err))
		}
		if n := countRows(t, ctx, db, "dbx_event"); n != 0 {
			t.Fatalf("rolled back insert still visible, rows=%d", n)
		}
	})

	t.Run("panic", func(t *testing.T) {
		sentinel := panicBox{value: "base-08"}
		defer func() {
			got := recover()
			if got != sentinel {
				t.Fatalf("panic = %#v", got)
			}
			if n := countRows(t, ctx, db, "dbx_event"); n != 0 {
				t.Fatalf("panic rollback left rows=%d", n)
			}
		}()
		_ = WithTx(ctx, db, func(tx *sqlx.Tx) error {
			if _, err := tx.ExecContext(ctx, "INSERT INTO dbx_event (id, note) VALUES (?, ?)", 2, "panic"); err != nil {
				return err
			}
			panic(sentinel)
		})
		t.Fatal("expected panic")
	})
}

// BASE-08 验收 2：两个事务交叉 SELECT ... FOR UPDATE 触发 1213，WithTx 重试后提交完整结果。
func TestWithTxDeadlockRetry(t *testing.T) {
	ctx, db := newSchema(t)
	mustExec(t, ctx, db, `CREATE TABLE dbx_row (
		id INT NOT NULL PRIMARY KEY,
		v INT NOT NULL
	) ENGINE=InnoDB`)
	mustExec(t, ctx, db, "INSERT INTO dbx_row (id, v) VALUES (1, 0), (2, 0)")

	for round := 1; round <= 20; round++ {
		if err := ctx.Err(); err != nil {
			t.Fatal(sanitize(err))
		}
		mustExec(t, ctx, db, "UPDATE dbx_row SET v=0")
		roundCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		attempts, errno, err := deadlockRound(roundCtx, db)
		cancel()
		v1, v2 := rowValues(t, ctx, db)
		if err == nil && (v1 != 11 || v2 != 22) {
			t.Fatalf("round %d committed partial values %d,%d", round, v1, v2)
		}
		if err != nil && (v1 != 0 || v2 != 0) {
			t.Fatalf("round %d error left values %d,%d", round, v1, v2)
		}
		if err == nil && attempts >= 2 && errno == 1213 && v1 == 11 && v2 == 22 {
			return
		}
	}
	t.Fatal("20 rounds did not observe a 1213 retry that committed both rows")
}

// BASE-08 验收 3：重复插入唯一键，IsDuplicate 为 true；CHECK 约束违反时 IsCheckViolation 为 true。
func TestDuplicateAndCheckViolation(t *testing.T) {
	ctx, db := newSchema(t)
	mustExec(t, ctx, db, `CREATE TABLE dbx_uniq (
		id INT NOT NULL PRIMARY KEY,
		k VARCHAR(64) NOT NULL,
		UNIQUE KEY uk_dbx_k (k)
	) ENGINE=InnoDB`)
	mustExec(t, ctx, db, "INSERT INTO dbx_uniq (id, k) VALUES (?, ?)", 1, "same")
	_, err := db.ExecContext(ctx, "INSERT INTO dbx_uniq (id, k) VALUES (?, ?)", 2, "same")
	if mysqlNumber(err) != 1062 || !IsDuplicate(err) || IsCheckViolation(err) {
		t.Fatalf("duplicate number=%d IsDuplicate=%v IsCheckViolation=%v", mysqlNumber(err), IsDuplicate(err), IsCheckViolation(err))
	}

	mustExec(t, ctx, db, `CREATE TABLE dbx_check (
		id INT NOT NULL PRIMARY KEY,
		v INT NOT NULL,
		CONSTRAINT ck_dbx_v CHECK (v >= 0)
	) ENGINE=InnoDB`)
	_, err = db.ExecContext(ctx, "INSERT INTO dbx_check (id, v) VALUES (?, ?)", 1, -1)
	if mysqlNumber(err) != 3819 || !IsCheckViolation(err) || IsDuplicate(err) {
		t.Fatalf("check number=%d IsCheckViolation=%v IsDuplicate=%v", mysqlNumber(err), IsCheckViolation(err), IsDuplicate(err))
	}
}

func TestWithTxTimeoutRollsBack(t *testing.T) {
	ctx, db := newSchema(t)
	mustExec(t, ctx, db, `CREATE TABLE dbx_event (
		id BIGINT NOT NULL PRIMARY KEY,
		note VARCHAR(32) NOT NULL
	) ENGINE=InnoDB`)
	txTimeout = 150 * time.Millisecond
	t.Cleanup(func() { txTimeout = defaultTxTimeout })

	inserted := false
	err := WithTx(ctx, db, func(tx *sqlx.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO dbx_event (id, note) VALUES (?, ?)", 1, "late"); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		inserted = true
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		<-timer.C
		return nil
	})
	if !inserted {
		t.Fatalf("insert did not happen: %s", sanitize(err))
	}
	if err == nil {
		t.Fatal("timed out transaction returned nil")
	}
	if n := countRows(t, ctx, db, "dbx_event"); n != 0 {
		t.Fatalf("timed out insert still visible, rows=%d", n)
	}
}

func TestOpenForcesSession(t *testing.T) {
	ctx, db := newSchema(t)
	var iso string
	if err := db.QueryRowContext(ctx, "SELECT @@transaction_isolation").Scan(&iso); err != nil {
		t.Fatal(sanitize(err))
	}
	if iso != "READ-COMMITTED" {
		t.Fatalf("transaction_isolation = %s", iso)
	}
	var ts time.Time
	if err := db.QueryRowContext(ctx, "SELECT TIMESTAMP('2026-10-15 12:00:00')").Scan(&ts); err != nil {
		t.Fatal(sanitize(err))
	}
	if ts.Location() == nil || ts.Location().String() != "Asia/Shanghai" || ts.Hour() != 12 {
		t.Fatalf("parsed time loc=%v hour=%d", ts.Location(), ts.Hour())
	}
}

func TestMustAffectOneMySQL(t *testing.T) {
	ctx, db := newSchema(t)
	mustExec(t, ctx, db, `CREATE TABLE dbx_event (
		id BIGINT NOT NULL PRIMARY KEY,
		note VARCHAR(32) NOT NULL
	) ENGINE=InnoDB`)
	mustExec(t, ctx, db, "INSERT INTO dbx_event (id, note) VALUES (?, ?)", 1, "old")
	res, err := db.ExecContext(ctx, "UPDATE dbx_event SET note=? WHERE id=?", "new", 1)
	if err != nil {
		t.Fatal(sanitize(err))
	}
	if err := MustAffectOne(res); err != nil {
		t.Fatal(err)
	}
	res, err = db.ExecContext(ctx, "UPDATE dbx_event SET note=? WHERE id=?", "missing", 999)
	if err != nil {
		t.Fatal(sanitize(err))
	}
	err = MustAffectOne(res)
	if err == nil {
		t.Fatal("0 rows should fail")
	}
	if strings.Contains(err.Error(), "UPDATE") || strings.Contains(err.Error(), "999") || strings.Contains(err.Error(), "missing") {
		t.Fatalf("error text leaked SQL or args: %s", err.Error())
	}
}

type panicBox struct {
	value string
}

func deadlockRound(ctx context.Context, db *sqlx.DB) (int32, uint32, error) {
	readyA := make(chan struct{})
	readyB := make(chan struct{})
	peerErr := make(chan error, 1)
	go func() {
		peerErr <- peerLock(ctx, db, readyA, readyB)
	}()

	var attempts atomic.Int32
	var firstErrno atomic.Uint32
	err := WithTx(ctx, db, func(tx *sqlx.Tx) error {
		n := attempts.Add(1)
		e := crossLock(ctx, tx, n, readyA, readyB)
		if e != nil && n == 1 {
			firstErrno.Store(uint32(mysqlNumber(e)))
		}
		return e
	})
	<-peerErr
	return attempts.Load(), firstErrno.Load(), err
}

func crossLock(ctx context.Context, tx *sqlx.Tx, n int32, readyA, readyB chan struct{}) error {
	var id int
	if err := tx.QueryRowContext(ctx, "SELECT id FROM dbx_row WHERE id=? FOR UPDATE", 1).Scan(&id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE dbx_row SET v=? WHERE id=?", 99, 1); err != nil {
		return err
	}
	if n == 1 {
		close(readyA)
		select {
		case <-readyB:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := tx.QueryRowContext(ctx, "SELECT id FROM dbx_row WHERE id=? FOR UPDATE", 2).Scan(&id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE dbx_row SET v=? WHERE id=?", 11, 1); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "UPDATE dbx_row SET v=? WHERE id=?", 22, 2)
	return err
}

func peerLock(ctx context.Context, db *sqlx.DB, readyA, readyB chan struct{}) error {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var id int
	if err := tx.QueryRowContext(ctx, "SELECT id FROM dbx_row WHERE id=? FOR UPDATE", 2).Scan(&id); err != nil {
		return err
	}
	// 多产生一点 undo，InnoDB 更倾向回滚锁更少的 WithTx。本事务结束时一律回滚。
	for i := 1; i <= 4; i++ {
		if _, err := tx.ExecContext(ctx, "UPDATE dbx_row SET v=? WHERE id=?", i, 2); err != nil {
			return err
		}
	}
	close(readyB)
	select {
	case <-readyA:
	case <-ctx.Done():
		return ctx.Err()
	}
	return tx.QueryRowContext(ctx, "SELECT id FROM dbx_row WHERE id=? FOR UPDATE", 1).Scan(&id)
}

func newSchema(t *testing.T) (context.Context, *sqlx.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	ensureMySQL(t)
	schema, err := newSchemaName()
	if err != nil || !safeSchema(schema) {
		t.Fatalf("schema: %s", sanitize(err))
	}
	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE `"+schema+"`"); err != nil { //nolint:gosec // name matches ^dbx_[0-9a-f]{16}$
		t.Fatal(sanitize(err))
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dropCancel()
		if _, err := adminDB.ExecContext(dropCtx, "DROP DATABASE IF EXISTS `"+schema+"`"); err != nil { //nolint:gosec // name matches ^dbx_[0-9a-f]{16}$
			t.Errorf("drop schema: %s", sanitize(err))
		}
	})
	dsn, err := dsnForSchema(adminDSN, schema)
	if err != nil {
		t.Fatal(sanitize(err))
	}
	db, err := Open(ctx, dsn, PoolConfig{})
	if err != nil {
		t.Fatal(sanitize(err))
	}
	t.Cleanup(func() { _ = db.Close() })
	return ctx, db
}

func ensureMySQL(t *testing.T) {
	t.Helper()
	mysqlOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if dsn := os.Getenv("TEST_MYSQL_DSN"); dsn != "" {
			adminDSN = dsn
		} else {
			var err error
			adminDSN, stopMySQL, err = startMySQL(ctx)
			if err != nil {
				mysqlStartErr = err
				return
			}
		}
		adminDB, mysqlStartErr = Open(ctx, adminDSN, PoolConfig{})
	})
	if mysqlStartErr != nil || adminDB == nil {
		t.Fatalf("mysql: %s", sanitize(mysqlStartErr))
	}
}

func startMySQL(ctx context.Context) (string, func(), error) {
	ctr, err := mysql.Run(ctx, "mysql:8.4.11",
		mysql.WithUsername("root"),
		mysql.WithPassword(testRootPassword),
		mysql.WithDatabase("dbx_admin"),
		// 临时服务器日志也有 ready，但 port 是 0。等到真正监听 3306。
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(2*time.Minute),
			wait.ForListeningPort("3306/tcp").WithStartupTimeout(2*time.Minute),
		)),
	)
	if err != nil {
		return "", nil, err
	}
	// 启动用的 ctx 在 TestMain 清理容器时已经取消，不能再传进去。
	stop := func() { //nolint:contextcheck // container cleanup runs after the startup context is canceled.
		stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := ctr.Terminate(stopCtx); err != nil {
			fmt.Fprintf(os.Stderr, "stop mysql: %s\n", sanitize(err))
		}
	}
	dsn, err := ctr.ConnectionString(ctx, "charset=utf8mb4")
	if err != nil {
		stop()
		return "", nil, err
	}
	return dsn, stop, nil
}

func dsnForSchema(dsn, schema string) (string, error) {
	if !safeSchema(schema) {
		return "", errors.New("dbx: refuse schema name")
	}
	cfg, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	cfg.DBName = schema
	cfg.ParseTime = false
	cfg.Loc = time.UTC
	cfg.Timeout = 9 * time.Second
	cfg.ReadTimeout = 9 * time.Second
	cfg.WriteTimeout = 9 * time.Second
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["transaction_isolation"] = "'REPEATABLE-READ'"
	cfg.Params["tx_isolation"] = "'REPEATABLE-READ'"
	return cfg.FormatDSN(), nil
}

func newSchemaName() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "dbx_" + hex.EncodeToString(b[:]), nil
}

func safeSchema(name string) bool {
	if !schemaNameRE.MatchString(name) {
		return false
	}
	switch name {
	case "pay_account_0", "pay_account_1", "pay_payment", "pay_recon", "pay_channel_mock", "mysql", "dbx_admin":
		return false
	default:
		return true
	}
}

func mustExec(t *testing.T, ctx context.Context, db *sqlx.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(sanitize(err))
	}
}

func countRows(t *testing.T, ctx context.Context, db *sqlx.DB, table string) int {
	t.Helper()
	if table != "dbx_event" && table != "dbx_row" && table != "dbx_uniq" && table != "dbx_check" {
		t.Fatalf("refuse table %q", table)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil { //nolint:gosec // table is one of the fixed names above
		t.Fatal(sanitize(err))
	}
	return n
}

func rowValues(t *testing.T, ctx context.Context, db *sqlx.DB) (int, int) {
	t.Helper()
	var v1, v2 int
	if err := db.QueryRowContext(ctx, "SELECT v FROM dbx_row WHERE id=?", 1).Scan(&v1); err != nil {
		t.Fatal(sanitize(err))
	}
	if err := db.QueryRowContext(ctx, "SELECT v FROM dbx_row WHERE id=?", 2).Scan(&v2); err != nil {
		t.Fatal(sanitize(err))
	}
	return v1, v2
}

func mysqlNumber(err error) uint16 {
	var me *mysqlDriver.MySQLError
	if errors.As(err, &me) && me != nil {
		return me.Number
	}
	return 0
}

func sanitize(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	for _, secret := range []string{testRootPassword, adminDSN, os.Getenv("TEST_MYSQL_DSN")} {
		if secret != "" {
			msg = strings.ReplaceAll(msg, secret, "[redacted]")
		}
	}
	if dsn := os.Getenv("TEST_MYSQL_DSN"); dsn != "" {
		if cfg, perr := mysqlDriver.ParseDSN(dsn); perr == nil && len(cfg.Passwd) >= 4 {
			msg = strings.ReplaceAll(msg, cfg.Passwd, "[redacted]")
		}
	}
	return msg
}
