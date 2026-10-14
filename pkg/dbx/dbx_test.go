package dbx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func TestMySQLErrorClassifiers(t *testing.T) {
	t.Parallel()
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	check := &mysql.MySQLError{Number: 3819, Message: "Check constraint"}
	deadlock := &mysql.MySQLError{Number: 1213, Message: "Deadlock"}
	lockWait := &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout"}
	other := &mysql.MySQLError{Number: 1064, Message: "syntax"}

	tests := []struct {
		name  string
		err   error
		dup   bool
		check bool
	}{
		{name: "1062", err: dup, dup: true},
		{name: "1062 wrapped", err: fmt.Errorf("insert: %w", dup), dup: true},
		{name: "1062 double wrapped", err: fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", dup)), dup: true},
		{name: "3819", err: check, check: true},
		{name: "3819 wrapped", err: fmt.Errorf("insert: %w", check), check: true},
		{name: "1213", err: deadlock},
		{name: "1205", err: lockWait},
		{name: "other number", err: other},
		{name: "plain text", err: errors.New("Error 1062: Duplicate entry")},
		{name: "nil", err: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsDuplicate(tt.err); got != tt.dup {
				t.Fatalf("IsDuplicate = %v, want %v", got, tt.dup)
			}
			if got := IsCheckViolation(tt.err); got != tt.check {
				t.Fatalf("IsCheckViolation = %v, want %v", got, tt.check)
			}
		})
	}

	if !isRetryable(fmt.Errorf("tx: %w", deadlock)) {
		t.Fatal("1213 wrapped should be retryable")
	}
	if !isRetryable(lockWait) {
		t.Fatal("1205 should be retryable")
	}
	if isRetryable(dup) || isRetryable(errors.New("1213")) || isRetryable(nil) {
		t.Fatal("non lock errors must not be retryable")
	}
}

func TestMustAffectOne(t *testing.T) {
	t.Parallel()
	if err := MustAffectOne(stubResult{rows: 1}); err != nil {
		t.Fatal(err)
	}
	err := MustAffectOne(stubResult{rows: 0})
	if err == nil {
		t.Fatal("0 rows should fail")
	}
	if strings.Contains(err.Error(), "SELECT") || strings.Contains(err.Error(), "?") {
		t.Fatalf("error text leaked SQL: %s", err.Error())
	}
	if err := MustAffectOne(stubResult{rows: 2}); err == nil {
		t.Fatal("2 rows should fail")
	}
	cause := errors.New("rows affected failed")
	if got := MustAffectOne(stubResult{err: cause}); !errors.Is(got, cause) {
		t.Fatalf("got %v, want %v", got, cause)
	}
	if err := MustAffectOne(nil); err == nil {
		t.Fatal("nil result should fail")
	}
}

func TestDefaultTxTimeout(t *testing.T) {
	if defaultTxTimeout != 3*time.Second {
		t.Fatalf("defaultTxTimeout = %s, want 3s", defaultTxTimeout)
	}
	if txTimeout != defaultTxTimeout {
		t.Fatalf("txTimeout = %s, want %s", txTimeout, defaultTxTimeout)
	}
	if maxTxAttempts != 4 {
		t.Fatalf("maxTxAttempts = %d, want 4", maxTxAttempts)
	}
	want := [3]time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	if retryBackoff != want {
		t.Fatalf("retryBackoff = %v, want %v", retryBackoff, want)
	}
}

func TestNormalizePool(t *testing.T) {
	t.Parallel()
	got, err := NormalizePool(PoolConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxOpen != 50 || got.MaxIdle != 10 || got.MaxLifetime != 30*time.Minute {
		t.Fatalf("zero pool = %+v, want 50 / 10 / 30m", got)
	}
	got, err = NormalizePool(PoolConfig{MaxOpen: 7, MaxIdle: 3, MaxLifetime: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxOpen != 7 || got.MaxIdle != 3 || got.MaxLifetime != time.Second {
		t.Fatalf("explicit pool = %+v", got)
	}
	for _, pool := range []PoolConfig{
		{MaxOpen: -1},
		{MaxIdle: -1},
		{MaxLifetime: -time.Second},
	} {
		_, err := NormalizePool(pool)
		if err == nil || !strings.Contains(err.Error(), "negative") {
			t.Fatalf("negative pool %+v: %v", pool, err)
		}
	}
}

func TestNormalizeDSNOverrides(t *testing.T) {
	t.Parallel()
	// 口令只放在输入里，失败信息不能把整段 DSN 打出来。
	in := "app:secret@tcp(127.0.0.1:3306)/pay_app?parseTime=false&loc=UTC&timeout=9s&readTimeout=9s&writeTimeout=9s&charset=utf8mb4&transaction_isolation='REPEATABLE-READ'&tx_isolation='REPEATABLE-READ'"
	out, err := NormalizeDSN(in)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "9s") || strings.Contains(out, "REPEATABLE") || strings.Contains(out, "parseTime=false") || strings.Contains(out, "tx_isolation") {
		t.Fatal("caller dsn params were not overridden")
	}
	for _, token := range []string{
		"parseTime=true",
		"loc=Asia%2FShanghai",
		"timeout=1s",
		"readTimeout=3s",
		"writeTimeout=3s",
		"transaction_isolation=%27READ-COMMITTED%27",
		"charset=utf8mb4",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("missing forced or preserved param %s", token)
		}
	}
	cfg, err := mysql.ParseDSN(out)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "app" || cfg.DBName != "pay_app" || cfg.Addr != "127.0.0.1:3306" {
		t.Fatal("user, database, or address changed")
	}
	if !cfg.ParseTime || cfg.Loc == nil || cfg.Loc.String() != "Asia/Shanghai" {
		t.Fatal("parseTime or loc was not forced")
	}
	if cfg.Timeout != time.Second || cfg.ReadTimeout != 3*time.Second || cfg.WriteTimeout != 3*time.Second {
		t.Fatal("timeouts were not forced")
	}
	if cfg.Params["transaction_isolation"] != "'READ-COMMITTED'" {
		t.Fatal("transaction_isolation was not forced")
	}
	if _, ok := cfg.Params["tx_isolation"]; ok {
		t.Fatal("tx_isolation should be removed")
	}
}

func TestNormalizeDSNBare(t *testing.T) {
	t.Parallel()
	out, err := NormalizeDSN("app:secret@tcp(127.0.0.1:3306)/pay_app")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(out)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ParseTime || cfg.Loc == nil || cfg.Loc.String() != "Asia/Shanghai" {
		t.Fatal("bare dsn was not forced")
	}
	if cfg.Timeout != time.Second || cfg.ReadTimeout != 3*time.Second || cfg.WriteTimeout != 3*time.Second {
		t.Fatal("bare dsn timeouts were not forced")
	}
	if cfg.Params["transaction_isolation"] != "'READ-COMMITTED'" {
		t.Fatal("bare dsn isolation was not forced")
	}
}

func TestNormalizeDSNInvalid(t *testing.T) {
	t.Parallel()
	if _, err := NormalizeDSN("not-a-dsn"); err == nil {
		t.Fatal("expected invalid dsn error")
	}
}

func TestOpenRejectsNegativePool(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Open(ctx, "app:secret@tcp(127.0.0.1:1)/none", PoolConfig{MaxOpen: -1})
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Open = %v, want negative pool error", err)
	}
}

func TestWithTxRejectsNilDB(t *testing.T) {
	t.Parallel()
	err := WithTx(context.Background(), nil, func(*sqlx.Tx) error { return nil })
	if err == nil {
		t.Fatal("nil db should fail")
	}
}

func TestWaitBackoffCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitBackoff(ctx, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

type stubResult struct {
	rows int64
	err  error
}

func (s stubResult) LastInsertId() (int64, error) { return 0, errors.New("no insert id") }

func (s stubResult) RowsAffected() (int64, error) { return s.rows, s.err }
