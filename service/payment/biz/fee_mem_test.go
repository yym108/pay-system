package biz

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

func init() {
	sql.Register("payfee-mem", memDriver{})
}

var (
	memSeq    atomic.Int64
	memByName sync.Map
)

type memDriver struct{}

func (memDriver) Open(name string) (driver.Conn, error) {
	v, ok := memByName.Load(name)
	if !ok {
		return nil, errors.New("unknown fee mem")
	}
	return &memConn{st: v.(*feeMem)}, nil
}

type memConn struct {
	st *feeMem
}

func (c *memConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fee mem prepare")
}

func (c *memConn) Close() error { return nil }

func (c *memConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *memConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.st.mu.Lock()
	defer c.st.mu.Unlock()
	if c.st.beginErr != nil {
		return nil, c.st.beginErr
	}
	c.st.begun = append(c.st.begun, opts)
	return memTx{st: c.st}, nil
}

func (c *memConn) ResetSession(context.Context) error { return nil }

func (c *memConn) IsValid() bool { return true }

func (c *memConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.st.query(query, args)
}

func (c *memConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.st.exec(query, args)
}

type memTx struct {
	st *feeMem
}

func (memTx) Commit() error   { return nil }
func (memTx) Rollback() error { return nil }

type memResult struct{}

func (memResult) LastInsertId() (int64, error) { return 1, nil }
func (memResult) RowsAffected() (int64, error) { return 1, nil }

type memRows struct {
	cols  []string
	data  [][]driver.Value
	i     int
	err   error
	errAt int
}

func (r *memRows) Columns() []string { return r.cols }
func (r *memRows) Close() error      { return nil }

func (r *memRows) Next(dest []driver.Value) error {
	if r.err != nil && r.i == r.errAt {
		return r.err
	}
	if r.i >= len(r.data) {
		return io.EOF
	}
	row := r.data[r.i]
	r.i++
	if len(dest) != len(row) {
		return fmt.Errorf("scan width %d want %d", len(dest), len(row))
	}
	copy(dest, row)
	return nil
}

// feeMem 是单元测试里的费率库。它按 SQL 文本区分语句，不连 MySQL。
type feeMem struct {
	mu        sync.Mutex
	merchants map[string]bool
	rates     []FeeRate
	orders    []orderRow
	queryFail string
	execFail  error
	forceDup  bool
	badScan   string
	nextFail  string
	beginErr  error
	begun     []driver.TxOptions
}

func newFeeMem() *feeMem {
	return &feeMem{merchants: map[string]bool{}}
}

func openFeeMem(t *testing.T, st *feeMem) *sqlx.DB {
	t.Helper()
	name := fmt.Sprintf("fee-mem-%d", memSeq.Add(1))
	memByName.Store(name, st)
	raw, err := sql.Open("payfee-mem", name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return sqlx.NewDb(raw, "payfee-mem")
}

func (st *feeMem) query(query string, args []driver.NamedValue) (driver.Rows, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.queryFail != "" && strings.Contains(query, st.queryFail) {
		return nil, errors.New("fee mem query")
	}
	if st.nextFail != "" && strings.Contains(query, st.nextFail) {
		return &memRows{cols: columnsOf(query), err: errors.New("fee mem next"), errAt: 0}, nil
	}
	switch {
	case strings.Contains(query, "FOR UPDATE"):
		return st.lockRows(args)
	case strings.Contains(query, "effective_from DESC"):
		return st.latestRows(args)
	case strings.Contains(query, "effective_from = ?"):
		return st.rateAtRows(args)
	case strings.Contains(query, "ORDER BY merchant_id"):
		return st.rateListRows()
	case strings.Contains(query, "payment_order"):
		return st.orderRows(query, args)
	default:
		return nil, fmt.Errorf("unexpected query %s", query)
	}
}

func (st *feeMem) exec(query string, args []driver.NamedValue) (driver.Result, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !strings.Contains(query, "INSERT INTO merchant_fee_rate") {
		return nil, fmt.Errorf("unexpected exec %s", query)
	}
	if st.execFail != nil {
		return nil, st.execFail
	}
	id, rate, at, err := rateArgs(args)
	if err != nil {
		return nil, err
	}
	for _, row := range st.rates {
		if row.MerchantID == id && row.EffectiveFrom.Equal(at) {
			return nil, &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
		}
	}
	if st.forceDup {
		return nil, &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	}
	st.rates = append(st.rates, FeeRate{MerchantID: id, RateBP: int(rate), EffectiveFrom: at})
	return memResult{}, nil
}

func (st *feeMem) lockRows(args []driver.NamedValue) (driver.Rows, error) {
	id, err := oneString(args)
	if err != nil {
		return nil, err
	}
	rows := &memRows{cols: []string{"merchant_id"}}
	if st.merchants[id] {
		rows.data = [][]driver.Value{{id}}
	}
	return rows, nil
}

func (st *feeMem) latestRows(args []driver.NamedValue) (driver.Rows, error) {
	id, err := oneString(args)
	if err != nil {
		return nil, err
	}
	rows := &memRows{cols: rateCols()}
	var best FeeRate
	found := false
	for _, row := range st.rates {
		if row.MerchantID != id {
			continue
		}
		if !found || row.EffectiveFrom.After(best.EffectiveFrom) {
			best = row
			found = true
		}
	}
	if found {
		rows.data = [][]driver.Value{rateValues(best, st.badScan == "effective_from DESC")}
	}
	return rows, nil
}

func (st *feeMem) rateAtRows(args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("rateAt args %d", len(args))
	}
	id, err := argString(args[0].Value)
	if err != nil {
		return nil, err
	}
	at, err := argTime(args[1].Value)
	if err != nil {
		return nil, err
	}
	rows := &memRows{cols: rateCols()}
	for _, row := range st.rates {
		if row.MerchantID == id && row.EffectiveFrom.Equal(at) {
			rows.data = [][]driver.Value{rateValues(row, st.badScan == "effective_from = ?")}
			break
		}
	}
	return rows, nil
}

func (st *feeMem) rateListRows() (driver.Rows, error) {
	rows := &memRows{cols: rateCols()}
	for _, row := range st.rates {
		rows.data = append(rows.data, rateValues(row, st.badScan == "merchant_fee_rate"))
	}
	return rows, nil
}

func (st *feeMem) orderRows(query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("order args %d", len(args))
	}
	after, err := argInt(args[0].Value)
	if err != nil {
		return nil, err
	}
	i := 1
	var from, to time.Time
	hasFrom := strings.Contains(query, "created_at >=")
	hasTo := strings.Contains(query, "created_at <")
	if hasFrom {
		from, err = argTime(args[i].Value)
		if err != nil {
			return nil, err
		}
		i++
	}
	if hasTo {
		to, err = argTime(args[i].Value)
		if err != nil {
			return nil, err
		}
		i++
	}
	limit, err := argInt(args[i].Value)
	if err != nil {
		return nil, err
	}
	rows := &memRows{cols: []string{"id", "pay_no", "merchant_id", "amount", "fee_amount", "created_at"}}
	orders := append([]orderRow(nil), st.orders...)
	sort.Slice(orders, func(i, j int) bool { return orders[i].ID < orders[j].ID })
	n := 0
	for _, order := range orders {
		if after < 0 || order.ID <= uint64(after) { //nolint:gosec // after 来自分页参数，负数已在上一判断排除
			continue
		}
		if hasFrom && order.CreatedAt.Before(from) {
			continue
		}
		if hasTo && !order.CreatedAt.Before(to) {
			continue
		}
		amount := driver.Value(int64(order.Amount))
		if st.badScan == "payment_order" {
			amount = "bad"
		}
		rows.data = append(rows.data, []driver.Value{
			int64(order.ID), order.PayNo, order.MerchantID, amount, int64(order.FeeAmount), order.CreatedAt, //nolint:gosec // 测试订单 id 和金额都在 int64 正数范围
		})
		n++
		if int64(n) == limit {
			break
		}
	}
	return rows, nil
}

func columnsOf(query string) []string {
	switch {
	case strings.Contains(query, "payment_order"):
		return []string{"id", "pay_no", "merchant_id", "amount", "fee_amount", "created_at"}
	case strings.Contains(query, "FOR UPDATE"):
		return []string{"merchant_id"}
	default:
		return rateCols()
	}
}

func rateCols() []string {
	return []string{"merchant_id", "fee_rate_bp", "effective_from"}
}

func rateValues(row FeeRate, bad bool) []driver.Value {
	rate := driver.Value(int64(row.RateBP))
	if bad {
		rate = "bad"
	}
	return []driver.Value{row.MerchantID, rate, row.EffectiveFrom}
}

func rateArgs(args []driver.NamedValue) (string, int64, time.Time, error) {
	if len(args) != 3 {
		return "", 0, time.Time{}, fmt.Errorf("insert args %d", len(args))
	}
	id, err := argString(args[0].Value)
	if err != nil {
		return "", 0, time.Time{}, err
	}
	rate, err := argInt(args[1].Value)
	if err != nil {
		return "", 0, time.Time{}, err
	}
	at, err := argTime(args[2].Value)
	if err != nil {
		return "", 0, time.Time{}, err
	}
	return id, rate, at, nil
}

func oneString(args []driver.NamedValue) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("args %d", len(args))
	}
	return argString(args[0].Value)
}

func argString(v driver.Value) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("string arg %T", v)
	}
	return s, nil
}

func argInt(v driver.Value) (int64, error) {
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("int arg %T", v)
	}
	return n, nil
}

func argTime(v driver.Value) (time.Time, error) {
	at, ok := v.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("time arg %T", v)
	}
	return at, nil
}

// PAY-10：内存库走真实 SQL 适配。更晚的档追加成功；核对只报告差额。
func TestMemAppendAndCheck(t *testing.T) {
	st := newFeeMem()
	st.merchants["M100002"] = true
	svc := New(openFeeMem(t, st))
	ctx := context.Background()
	t0 := feeAt(10, 0, 0, 400000)
	first, err := svc.AppendFeeRate(ctx, "M100002", 60, t0)
	if err != nil || first.Duplicated || first.RateBP != 60 {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	if first.EffectiveFrom.UnixMilli() != t0.Truncate(time.Millisecond).UnixMilli() {
		t.Fatalf("effective = %s", first.EffectiveFrom)
	}
	t1 := feeAt(11, 0, 0, 0)
	second, err := svc.AppendFeeRate(ctx, "M100002", 0, t1)
	if err != nil || second.Duplicated || second.RateBP != 0 {
		t.Fatalf("second = %+v err=%v", second, err)
	}
	if len(st.begun) != 2 || st.begun[0].ReadOnly || st.begun[0].Isolation != driver.IsolationLevel(sql.LevelReadCommitted) {
		t.Fatalf("append tx = %+v", st.begun)
	}

	want, err := money.Fee(10000, 60)
	if err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.orders = []orderRow{
		{ID: 2, PayNo: "PA2", MerchantID: "M100002", Amount: 10000, FeeAmount: want, CreatedAt: t0},
		{ID: 1, PayNo: "PA1", MerchantID: "M100002", Amount: 10000, FeeAmount: 0, CreatedAt: t0.Add(-time.Millisecond)},
		{ID: 3, PayNo: "PA3", MerchantID: "M100002", Amount: 10000, FeeAmount: 0, CreatedAt: t1},
	}
	st.mu.Unlock()

	got, err := svc.CheckOrderFees(ctx, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Scanned != 3 || got.Mismatched != 1 || len(got.Details) != 1 {
		t.Fatalf("got = %+v", got)
	}
	if got.Details[0].PayNo != "PA1" || got.Details[0].RateBP != -1 || got.Details[0].ExpectedFee != 0 {
		t.Fatalf("detail = %+v", got.Details[0])
	}
	last := st.begun[len(st.begun)-1]
	if !last.ReadOnly || last.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead) {
		t.Fatalf("check tx = %+v", last)
	}

	window, err := svc.CheckOrderFees(ctx, t0.Truncate(time.Millisecond), t1)
	if err != nil {
		t.Fatal(err)
	}
	if window.Scanned != 1 || window.Mismatched != 0 {
		t.Fatalf("window = %+v", window)
	}
	fromOnly, err := svc.CheckOrderFees(ctx, t1, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if fromOnly.Scanned != 1 || fromOnly.Mismatched != 0 {
		t.Fatalf("fromOnly = %+v", fromOnly)
	}
	toOnly, err := svc.CheckOrderFees(ctx, time.Time{}, t0.Truncate(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if toOnly.Scanned != 1 || toOnly.Mismatched != 1 {
		t.Fatalf("toOnly = %+v", toOnly)
	}
	empty, err := svc.CheckOrderFees(ctx, t1, t1)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Scanned != 0 || empty.Mismatched != 0 {
		t.Fatalf("empty = %+v", empty)
	}
}

func TestMemAppendDuplicateAndReject(t *testing.T) {
	st := newFeeMem()
	st.merchants["M100002"] = true
	svc := New(openFeeMem(t, st))
	ctx := context.Background()
	t0 := feeAt(10, 0, 0, 0)
	if _, err := svc.AppendFeeRate(ctx, "M100002", 60, t0); err != nil {
		t.Fatal(err)
	}
	again, err := svc.AppendFeeRate(ctx, "M100002", 60, t0)
	if err != nil || !again.Duplicated || again.RateBP != 60 || again.MerchantID != "M100002" {
		t.Fatalf("again = %+v err=%v", again, err)
	}
	_, err = svc.AppendFeeRate(ctx, "M100002", 80, t0)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	_, err = svc.AppendFeeRate(ctx, "M100002", 10, t0.Add(-time.Millisecond))
	assertErrno(t, err, errno.ErrInvalidParam)
	st.mu.Lock()
	n := len(st.rates)
	rate := st.rates[0].RateBP
	st.mu.Unlock()
	if n != 1 || rate != 60 {
		t.Fatalf("rows=%d rate=%d", n, rate)
	}

	_, err = svc.AppendFeeRate(ctx, "M100099", 60, t0)
	assertErrno(t, err, errno.ErrMerchantInvalid)
}

func TestMemSQLErrors(t *testing.T) {
	t0 := feeAt(10, 0, 0, 0)
	cases := []struct {
		name string
		set  func(*feeMem)
		run  func(*Service) error
		want *errno.Error
		ctx  error
	}{
		{
			name: "begin",
			set:  func(st *feeMem) { st.beginErr = errors.New("begin") },
			run: func(svc *Service) error {
				_, err := svc.AppendFeeRate(context.Background(), "M100002", 60, t0)
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "check begin canceled",
			set:  func(st *feeMem) { st.beginErr = context.Canceled },
			run: func(svc *Service) error {
				_, err := svc.CheckOrderFees(context.Background(), time.Time{}, time.Time{})
				return err
			},
			ctx: context.Canceled,
		},
		{
			name: "lock",
			set: func(st *feeMem) {
				st.merchants["M100002"] = true
				st.queryFail = "FOR UPDATE"
			},
			run: func(svc *Service) error {
				_, err := svc.AppendFeeRate(context.Background(), "M100002", 60, t0)
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "latest",
			set: func(st *feeMem) {
				st.merchants["M100002"] = true
				st.queryFail = "effective_from DESC"
			},
			run: func(svc *Service) error {
				_, err := svc.AppendFeeRate(context.Background(), "M100002", 60, t0)
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "insert",
			set: func(st *feeMem) {
				st.merchants["M100002"] = true
				st.execFail = errors.New("insert")
			},
			run: func(svc *Service) error {
				_, err := svc.AppendFeeRate(context.Background(), "M100002", 60, t0)
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "dup miss",
			set: func(st *feeMem) {
				st.merchants["M100002"] = true
				st.forceDup = true
			},
			run: func(svc *Service) error {
				_, err := svc.AppendFeeRate(context.Background(), "M100002", 60, t0)
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "rate at",
			set: func(st *feeMem) {
				st.merchants["M100002"] = true
				st.forceDup = true
				st.queryFail = "effective_from = ?"
			},
			run: func(svc *Service) error {
				_, err := svc.AppendFeeRate(context.Background(), "M100002", 60, t0)
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "bad latest scan",
			set: func(st *feeMem) {
				st.merchants["M100002"] = true
				st.rates = []FeeRate{{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t0}}
				st.badScan = "effective_from DESC"
			},
			run: func(svc *Service) error {
				_, err := svc.AppendFeeRate(context.Background(), "M100002", 80, t0.Add(time.Hour))
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "bad rateAt scan",
			set: func(st *feeMem) {
				st.merchants["M100002"] = true
				st.rates = []FeeRate{{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t0}}
				st.badScan = "effective_from = ?"
			},
			run: func(svc *Service) error {
				_, err := svc.AppendFeeRate(context.Background(), "M100002", 60, t0)
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "list rates",
			set:  func(st *feeMem) { st.queryFail = "ORDER BY merchant_id" },
			run: func(svc *Service) error {
				_, err := svc.CheckOrderFees(context.Background(), time.Time{}, time.Time{})
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "list rates scan",
			set: func(st *feeMem) {
				st.rates = []FeeRate{{MerchantID: "M100002", RateBP: 60, EffectiveFrom: t0}}
				st.badScan = "merchant_fee_rate"
			},
			run: func(svc *Service) error {
				_, err := svc.CheckOrderFees(context.Background(), time.Time{}, time.Time{})
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "list rates next",
			set:  func(st *feeMem) { st.nextFail = "ORDER BY merchant_id" },
			run: func(svc *Service) error {
				_, err := svc.CheckOrderFees(context.Background(), time.Time{}, time.Time{})
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "list orders",
			set:  func(st *feeMem) { st.queryFail = "payment_order" },
			run: func(svc *Service) error {
				_, err := svc.CheckOrderFees(context.Background(), time.Time{}, time.Time{})
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "list orders scan",
			set: func(st *feeMem) {
				st.orders = []orderRow{{ID: 1, PayNo: "PA1", MerchantID: "M100002", Amount: 10000, CreatedAt: t0}}
				st.badScan = "payment_order"
			},
			run: func(svc *Service) error {
				_, err := svc.CheckOrderFees(context.Background(), time.Time{}, time.Time{})
				return err
			},
			want: errno.ErrInternal,
		},
		{
			name: "list orders next",
			set:  func(st *feeMem) { st.nextFail = "payment_order" },
			run: func(svc *Service) error {
				_, err := svc.CheckOrderFees(context.Background(), time.Time{}, time.Time{})
				return err
			},
			want: errno.ErrInternal,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newFeeMem()
			tc.set(st)
			err := tc.run(New(openFeeMem(t, st)))
			if tc.ctx != nil {
				if !errors.Is(err, tc.ctx) {
					t.Fatalf("error = %v", err)
				}
				return
			}
			assertErrno(t, err, tc.want)
		})
	}
}
