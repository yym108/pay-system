package biz

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/service/account/dal"
)

func init() {
	sql.Register("acccheck", accCheckDriver{})
}

var (
	accScripts   sync.Map
	accScriptSeq atomic.Uint64
)

type accCheckDriver struct{}

func (accCheckDriver) Open(name string) (driver.Conn, error) {
	v, ok := accScripts.Load(name)
	if !ok {
		return nil, errors.New("missing check script")
	}
	return &accConn{script: v.(*accScript)}, nil
}

type accScript struct {
	mu      sync.Mutex
	steps   []accStep
	queries []string
	bad     bool
}

type accStep struct {
	exec     bool
	err      error
	cols     []string
	rows     [][]driver.Value
	nextErr  error
	closeErr error
}

type accConn struct {
	script *accScript
}

func (c *accConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not used")
}

func (c *accConn) Close() error {
	c.script.mu.Lock()
	c.script.bad = true
	c.script.mu.Unlock()
	return nil
}

func (c *accConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin is not used")
}

func (c *accConn) ResetSession(context.Context) error {
	c.script.mu.Lock()
	bad := c.script.bad
	c.script.mu.Unlock()
	if bad {
		return driver.ErrBadConn
	}
	return nil
}

func (c *accConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	step, err := c.script.take(true, query)
	if err != nil {
		return nil, err
	}
	if step.err != nil {
		return nil, step.err
	}
	return driver.RowsAffected(0), nil
}

func (c *accConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	step, err := c.script.take(false, query)
	if err != nil {
		return nil, err
	}
	if step.err != nil {
		return nil, step.err
	}
	return &accRows{cols: step.cols, rows: step.rows, nextErr: step.nextErr, closeErr: step.closeErr}, nil
}

func (s *accScript) take(exec bool, query string) (accStep, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, query)
	if len(s.steps) == 0 {
		return accStep{}, errors.New("unexpected query")
	}
	step := s.steps[0]
	s.steps = s.steps[1:]
	if step.exec != exec {
		return accStep{}, errors.New("query kind mismatch")
	}
	return step, nil
}

type accRows struct {
	cols     []string
	rows     [][]driver.Value
	i        int
	nextErr  error
	closeErr error
}

func (r *accRows) Columns() []string { return r.cols }

func (r *accRows) Close() error { return r.closeErr }

func (r *accRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		if r.nextErr != nil {
			err := r.nextErr
			r.nextErr = nil
			return err
		}
		return io.EOF
	}
	row := r.rows[r.i]
	r.i++
	for i := range dest {
		dest[i] = row[i]
	}
	return nil
}

func openScriptDB(t *testing.T, steps []accStep) (*sqlx.DB, *accScript) {
	t.Helper()
	name := fmt.Sprintf("%s_%d", strings.ReplaceAll(t.Name(), "/", "_"), accScriptSeq.Add(1))
	script := &accScript{steps: steps}
	accScripts.Store(name, script)
	raw, err := sql.Open("acccheck", name)
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	raw.SetMaxIdleConns(1)
	t.Cleanup(func() {
		_ = raw.Close()
		accScripts.Delete(name)
	})
	return sqlx.NewDb(raw, "mysql"), script
}

func execStep() accStep { return accStep{exec: true} }

func queryStep(cols []string, rows ...[]driver.Value) accStep {
	return accStep{cols: cols, rows: rows}
}

func emptyQuery(cols ...string) accStep {
	return queryStep(cols)
}

func fullShardSteps(extra ...accStep) []accStep {
	steps := []accStep{execStep(), execStep()}
	steps = append(steps, extra...)
	return append(steps, execStep())
}

func balancedShardSteps() []accStep {
	return fullShardSteps(
		queryStep([]string{"id", "account_no", "balance_direction", "balance", "frozen_amount", "allow_overdraft"},
			[]driver.Value{int64(1), "AC1", directionCredit, int64(5), int64(0), int64(overdraftYes)},
			[]driver.Value{int64(2), "AC2", directionDebit, int64(5), int64(0), int64(overdraftYes)},
		),
		queryStep([]string{"account_id", "direction", "amount"},
			[]driver.Value{int64(1), directionCredit, int64(5)},
			[]driver.Value{int64(2), directionDebit, int64(5)},
		),
		queryStep([]string{"account_id", "balance_after"},
			[]driver.Value{int64(1), int64(5)},
			[]driver.Value{int64(2), int64(5)},
		),
		emptyQuery("account_id", "amount"),
		queryStep([]string{"id", "entry_count", "entry_id", "direction", "amount", "account_ok"},
			[]driver.Value{int64(1), int64(2), int64(10), directionDebit, int64(5), int64(1)},
			[]driver.Value{int64(1), int64(2), int64(11), directionCredit, int64(5), int64(1)},
			[]driver.Value{int64(2), int64(0), nil, nil, nil, int64(1)},
		),
		emptyQuery("amount"),
		emptyQuery("amount"),
		emptyQuery("balance"),
	)
}

func emptyShardSteps() []accStep {
	return fullShardSteps(
		emptyQuery("id", "account_no", "balance_direction", "balance", "frozen_amount", "allow_overdraft"),
		emptyQuery("account_id", "direction", "amount"),
		emptyQuery("account_id", "balance_after"),
		emptyQuery("account_id", "amount"),
		emptyQuery("id", "entry_count", "entry_id", "direction", "amount", "account_ok"),
		emptyQuery("amount"),
		emptyQuery("amount"),
		emptyQuery("balance"),
	)
}

func scriptService(t *testing.T, shard0, shard1 []accStep) *Service {
	t.Helper()
	db0, _ := openScriptDB(t, shard0)
	var db1 *sqlx.DB
	if shard1 != nil {
		db1, _ = openScriptDB(t, shard1)
	}
	return New(dal.New(db0, db1), nil, testSubCount)
}

// ACC-10：生产路径走真 SQL 扫描。快照语句按分片执行，失败计数加在同一次调用上。
func TestCheckBalanceSQL(t *testing.T) {
	t.Run("full pass", func(t *testing.T) {
		db0, script0 := openScriptDB(t, balancedShardSteps())
		db1, script1 := openScriptDB(t, emptyShardSteps())
		svc := New(dal.New(db0, db1), nil, testSubCount)
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "", true)
		if err != nil || !got.Passed || len(got.Failures) != 0 {
			t.Fatalf("got %+v err=%v", got, err)
		}
		for _, script := range []*accScript{script0, script1} {
			joined := strings.Join(script.queries, "\n")
			if !strings.Contains(joined, setRepeatableReadSQL) || !strings.Contains(joined, startConsistentSnapshotSQL) || !strings.Contains(joined, rollbackSnapshotSQL) {
				t.Fatalf("queries = %s", joined)
			}
			if len(script.steps) != 0 {
				t.Fatalf("unused steps = %d", len(script.steps))
			}
		}
		failDelta(t, before, 0)
	})

	t.Run("merchant missing and one account", func(t *testing.T) {
		// SET、START、账户，然后 ROLLBACK。没有账户时不再查分录。
		missing := []accStep{
			execStep(), execStep(),
			emptyQuery("id", "account_no", "balance_direction", "balance", "frozen_amount", "allow_overdraft"),
			execStep(),
		}
		svc := scriptService(t, missing, nil)
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "M100002", true)
		assertErrno(t, err, errno.ErrAccountNotFound)
		if got.Passed || len(got.Failures) != 0 {
			t.Fatalf("missing = %+v", got)
		}

		one := []accStep{
			execStep(), execStep(),
			queryStep([]string{"id", "account_no", "balance_direction", "balance", "frozen_amount", "allow_overdraft"},
				[]driver.Value{int64(7), "AC-M", directionCredit, int64(0), int64(0), int64(overdraftNo)},
			),
			emptyQuery("account_id", "direction", "amount"),
			emptyQuery("account_id", "balance_after"),
			emptyQuery("account_id", "amount"),
			execStep(),
		}
		svc = scriptService(t, one, nil)
		got, err = svc.CheckBalance(context.Background(), "M100002", true)
		if err != nil || !got.Passed || len(got.Failures) != 0 {
			t.Fatalf("merchant = %+v err=%v", got, err)
		}
		failDelta(t, before, 0)
	})

	t.Run("read error and rollback error", func(t *testing.T) {
		before := ledgerCheckFailures.Load()
		readErr := []accStep{
			execStep(), execStep(),
			{err: errors.New("SELECT boom")},
			execStep(),
		}
		svc := scriptService(t, readErr, emptyShardSteps())
		got, err := svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)
		if len(got.Failures) != 0 || strings.Contains(err.Error(), "SELECT") {
			t.Fatalf("got %+v err=%v", got, err)
		}

		rbErr := append(emptyShardSteps()[:len(emptyShardSteps())-1], accStep{exec: true, err: errors.New("rollback failed")})
		svc = scriptService(t, rbErr, emptyShardSteps())
		got, err = svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)
		if len(got.Failures) != 0 {
			t.Fatalf("failures = %+v", got.Failures)
		}
		failDelta(t, before, 0)
	})

	t.Run("scanned failure counts once", func(t *testing.T) {
		bad := fullShardSteps(
			queryStep([]string{"id", "account_no", "balance_direction", "balance", "frozen_amount", "allow_overdraft"},
				[]driver.Value{int64(1), "AC-BAD", directionCredit, int64(1), int64(0), int64(overdraftNo)},
			),
			emptyQuery("account_id", "direction", "amount"),
			emptyQuery("account_id", "balance_after"),
			emptyQuery("account_id", "amount"),
			queryStep([]string{"id", "entry_count", "entry_id", "direction", "amount", "account_ok"},
				[]driver.Value{int64(3), int64(2), int64(1), directionDebit, int64(1), int64(0)},
				[]driver.Value{int64(3), int64(2), int64(2), directionCredit, int64(1), int64(1)},
			),
			emptyQuery("amount"),
			emptyQuery("amount"),
			emptyQuery("balance"),
		)
		svc := scriptService(t, bad, emptyShardSteps())
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "", false)
		if err != nil || got.Passed {
			t.Fatalf("got %+v err=%v", got, err)
		}
		var balanceN, txnN int
		for _, f := range got.Failures {
			switch f.Check {
			case "BALANCE_SUM":
				balanceN++
				if f.AccountNo != "AC-BAD" || f.Shard == nil || *f.Shard != 0 || f.Expected != 0 || f.Actual != 1 {
					t.Fatalf("balance = %+v", f)
				}
			case "TXN_BALANCED":
				txnN++
				if f.Expected != 0 || f.Actual != 1 || f.AccountNo != "" {
					t.Fatalf("txn = %+v", f)
				}
			}
		}
		if balanceN != 1 || txnN != 1 {
			t.Fatalf("failures = %+v", got.Failures)
		}
		failDelta(t, before, 1)
	})

	t.Run("rows error and unknown direction", func(t *testing.T) {
		before := ledgerCheckFailures.Load()
		rowsErr := []accStep{
			execStep(), execStep(),
			{cols: []string{"id", "account_no", "balance_direction", "balance", "frozen_amount", "allow_overdraft"}, nextErr: errors.New("rows failed")},
			execStep(),
		}
		svc := scriptService(t, rowsErr, nil)
		_, err := svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)

		unknown := fullShardSteps(
			queryStep([]string{"id", "account_no", "balance_direction", "balance", "frozen_amount", "allow_overdraft"},
				[]driver.Value{int64(1), "AC-Z", "NOPE", int64(0), int64(0), int64(0)},
			),
			emptyQuery("account_id", "direction", "amount"),
			emptyQuery("account_id", "balance_after"),
			emptyQuery("account_id", "amount"),
			emptyQuery("id", "entry_count", "entry_id", "direction", "amount", "account_ok"),
			emptyQuery("amount"),
			emptyQuery("amount"),
			emptyQuery("balance"),
		)
		svc = scriptService(t, unknown, emptyShardSteps())
		got, err := svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)
		if len(got.Failures) != 0 {
			t.Fatalf("failures = %+v", got.Failures)
		}
		failDelta(t, before, 0)
	})
}
