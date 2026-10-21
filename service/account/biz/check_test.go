package biz

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/dal"
)

type memBooks struct {
	byShard  map[int]shardBook
	errShard map[int]error
	calls    []int
}

func (m *memBooks) loadShard(ctx context.Context, shard int, _ string) (shardBook, error) {
	if err := ctx.Err(); err != nil {
		return shardBook{}, err
	}
	m.calls = append(m.calls, shard)
	if m.errShard != nil {
		if err := m.errShard[shard]; err != nil {
			return shardBook{}, err
		}
	}
	if m.byShard == nil {
		return shardBook{}, nil
	}
	return m.byShard[shard], nil
}

type panicBooks struct{}

func (panicBooks) loadShard(context.Context, int, string) (shardBook, error) {
	panic("balance check queried")
}

func sp(v int) *int {
	return &v
}

func failDelta(t *testing.T, before, want uint64) {
	t.Helper()
	if got := ledgerCheckFailures.Load() - before; got != want {
		t.Fatalf("counter delta = %d, want %d", got, want)
	}
}

func assertFailures(t *testing.T, got, want []BalanceFailure) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d failures:\n%+v\nwant:\n%+v", len(got), got, want)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Check != w.Check || g.AccountNo != w.AccountNo || g.Expected != w.Expected || g.Actual != w.Actual {
			t.Fatalf("failure %d = %+v, want %+v", i, g, w)
		}
		switch {
		case w.Shard == nil && g.Shard != nil:
			t.Fatalf("failure %d shard = %d, want nil", i, *g.Shard)
		case w.Shard != nil && (g.Shard == nil || *g.Shard != *w.Shard):
			t.Fatalf("failure %d shard = %v, want %d", i, g.Shard, *w.Shard)
		}
	}
}

// ACC-10：商户号不合法时 10001，先于 nil service 和已取消的 ctx，且不查询。
func TestCheckBalanceRejectsParams(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	svc := &Service{balance: panicBooks{}}
	before := ledgerCheckFailures.Load()

	cases := []struct {
		name       string
		svc        *Service
		ctx        context.Context
		merchantID string
	}{
		{name: "nil service canceled", svc: nil, ctx: canceled, merchantID: "nope"},
		{name: "nil service nil ctx", svc: nil, ctx: nil, merchantID: "M10002"},
		{name: "seam would panic", svc: svc, ctx: canceled, merchantID: "M100002 "},
		{name: "too short", svc: svc, ctx: context.Background(), merchantID: "M10002"},
		{name: "too long", svc: svc, ctx: context.Background(), merchantID: "M1000002"},
		{name: "bad prefix", svc: svc, ctx: context.Background(), merchantID: "X100002"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.svc.CheckBalance(tc.ctx, tc.merchantID, true)
			assertErrno(t, err, errno.ErrInvalidParam)
			if got.Passed || len(got.Failures) != 0 {
				t.Fatalf("result = %+v", got)
			}
		})
	}

	var nilSvc *Service
	_, err := nilSvc.CheckBalance(canceled, "", false)
	assertErrno(t, err, errno.ErrInternal)
	failDelta(t, before, 0)
}

// ACC-10：参数合法之后，nil 依赖是 10003，已取消的 ctx 原样返回，不开事务。
func TestCheckBalanceNilAndCanceled(t *testing.T) {
	before := ledgerCheckFailures.Load()
	var nilSvc *Service
	_, err := nilSvc.CheckBalance(context.Background(), "", false)
	assertErrno(t, err, errno.ErrInternal)
	_, err = nilSvc.CheckBalance(context.Background(), "M100002", true)
	assertErrno(t, err, errno.ErrInternal)

	live := &Service{balance: panicBooks{}}
	// SA1012 不允许把 nil context 当参数。这里正是要证明 nil ctx 返回 10003。
	_, err = live.CheckBalance(nil, "", false) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)
	_, err = live.CheckBalance(nil, "M100002", true) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	// 取消先于 nil pools。缝如果被调用会 panic。
	_, err = live.CheckBalance(canceled, "M100002", false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	_, err = live.CheckBalance(canceled, "", true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("full canceled = %v", err)
	}

	deadline, stop := context.WithDeadline(context.Background(), openTestTime)
	defer stop()
	_, err = live.CheckBalance(deadline, "M100001", false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline = %v", err)
	}

	noPools := &Service{}
	_, err = noPools.CheckBalance(context.Background(), "", false)
	assertErrno(t, err, errno.ErrInternal)
	_, err = noPools.CheckBalance(context.Background(), "M100002", false)
	assertErrno(t, err, errno.ErrInternal)

	// 生产路径缝为 nil。两个池都是 nil 时 ShardDB 失败，整次 10003，不查询业务表。
	shardFail := New(dal.New(nil, nil), nil, testSubCount)
	got, err := shardFail.CheckBalance(context.Background(), "", false)
	assertErrno(t, err, errno.ErrInternal)
	if len(got.Failures) != 0 {
		t.Fatalf("failures = %+v", got.Failures)
	}
	failDelta(t, before, 0)
}

func manyBooks() map[int]shardBook {
	return map[int]shardBook{
		0: {
			Accounts: []checkAccount{
				{ID: 1, AccountNo: "ACCT-A", BalanceDirection: directionCredit, Balance: 1, FrozenAmount: 5, AllowOverdraft: overdraftNo},
				{ID: 2, AccountNo: "ACCT-B", BalanceDirection: directionDebit, Balance: 5, AllowOverdraft: overdraftYes},
				{ID: 3, AccountNo: "ACCT-C", BalanceDirection: directionCredit, Balance: -8, AllowOverdraft: overdraftYes},
				{ID: 4, AccountNo: "ACCT-D", BalanceDirection: directionCredit, FrozenAmount: -3, AllowOverdraft: overdraftYes},
				{ID: 5, AccountNo: "ACCT-E", BalanceDirection: directionCredit, Balance: 12, FrozenAmount: 12, AllowOverdraft: overdraftNo},
			},
			Entries: []checkLeg{
				{AccountID: 2, Direction: directionDebit, Amount: 10},
				{AccountID: 2, Direction: directionCredit, Amount: 3},
				{AccountID: 3, Direction: directionDebit, Amount: 8},
				{AccountID: 5, Direction: directionCredit, Amount: 12},
			},
			Lasts: []checkLast{
				{AccountID: 2, BalanceAfter: 7},
				{AccountID: 3, BalanceAfter: -8},
				{AccountID: 5, BalanceAfter: 12},
			},
			Frozens: []checkFreeze{
				{AccountID: 5, Amount: 5},
				{AccountID: 5, Amount: 7},
			},
			Txns: []checkTxn{
				{ID: 10, EntryCount: 2, Entries: []checkTxnEntry{
					{Direction: directionDebit, Amount: 5, AccountExists: true},
					{Direction: directionCredit, Amount: 4, AccountExists: true},
				}},
				{ID: 11, EntryCount: 0},
				{ID: 12, EntryCount: 5, Entries: []checkTxnEntry{
					{Direction: directionDebit, Amount: 1, AccountExists: true},
					{Direction: directionCredit, Amount: 1, AccountExists: false},
				}},
			},
			ConfirmedTransferFreezes: []money.Amount{10},
			ConfirmedPayeeAmounts:    []money.Amount{4},
			TransitBalances:          []money.Amount{1},
		},
		1: {
			Accounts: []checkAccount{
				{ID: 1, AccountNo: "ACCT-F", BalanceDirection: directionCredit, AllowOverdraft: overdraftNo},
			},
			TransitBalances: []money.Amount{2},
		},
	}
}

// ACC-10：公式、失败顺序、商户模式、读取错误和溢出。计数一次，10003 不加。
func TestCheckBalanceFormulas(t *testing.T) {
	t.Run("all failures once", func(t *testing.T) {
		src := &memBooks{byShard: manyBooks()}
		svc := &Service{balance: src}
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "", true)
		if err != nil {
			t.Fatal(err)
		}
		if got.Passed {
			t.Fatal("passed")
		}
		assertFailures(t, got.Failures, []BalanceFailure{
			{Shard: sp(0), AccountNo: "ACCT-A", Check: "BALANCE_SUM", Expected: 0, Actual: 1},
			{Shard: sp(0), AccountNo: "ACCT-A", Check: "LAST_BALANCE_AFTER", Expected: 0, Actual: 1},
			{Shard: sp(0), AccountNo: "ACCT-A", Check: "FROZEN_SUM", Expected: 0, Actual: 5},
			{Shard: sp(0), AccountNo: "ACCT-A", Check: "AVAILABLE_NON_NEGATIVE", Expected: 0, Actual: -4},
			{Shard: sp(0), AccountNo: "ACCT-B", Check: "BALANCE_SUM", Expected: 7, Actual: 5},
			{Shard: sp(0), AccountNo: "ACCT-B", Check: "LAST_BALANCE_AFTER", Expected: 7, Actual: 5},
			{Shard: sp(0), AccountNo: "ACCT-D", Check: "FROZEN_SUM", Expected: 0, Actual: -3},
			{Shard: sp(0), AccountNo: "", Check: "TXN_BALANCED", Expected: 5, Actual: 4},
			{Shard: sp(0), AccountNo: "", Check: "TXN_BALANCED", Expected: 5, Actual: 2},
			{Shard: sp(0), AccountNo: "", Check: "TXN_BALANCED", Expected: 0, Actual: 1},
			{Shard: sp(0), AccountNo: "", Check: "GLOBAL_BALANCED", Expected: 18, Actual: 15},
			{Shard: sp(0), AccountNo: "", Check: "TRANSIT_EXPECTED", Expected: 6, Actual: 1},
			{Shard: sp(1), AccountNo: "", Check: "TRANSIT_EXPECTED", Expected: 0, Actual: 2},
			{Check: "TRANSIT_ZERO", Expected: 0, Actual: 3},
		})
		if len(src.calls) != 2 || src.calls[0] != 0 || src.calls[1] != 1 {
			t.Fatalf("calls = %v", src.calls)
		}
		failDelta(t, before, 1)
	})

	t.Run("debit matches and overdraft skips", func(t *testing.T) {
		svc := &Service{balance: &memBooks{byShard: map[int]shardBook{
			0: {Accounts: []checkAccount{
				{ID: 1, AccountNo: "ACCT-OK", BalanceDirection: directionDebit, Balance: 4, AllowOverdraft: overdraftYes},
				{ID: 2, AccountNo: "ACCT-CR", BalanceDirection: directionCredit, Balance: 4, AllowOverdraft: overdraftYes},
			}, Entries: []checkLeg{
				{AccountID: 1, Direction: directionDebit, Amount: 9},
				{AccountID: 1, Direction: directionCredit, Amount: 5},
				{AccountID: 2, Direction: directionCredit, Amount: 4},
			}, Lasts: []checkLast{
				{AccountID: 1, BalanceAfter: 4},
				{AccountID: 2, BalanceAfter: 4},
			}},
			1: {},
		}}}
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "", false)
		if err != nil || !got.Passed || len(got.Failures) != 0 {
			t.Fatalf("got %+v err=%v", got, err)
		}
		failDelta(t, before, 0)
	})

	t.Run("unknown direction", func(t *testing.T) {
		svc := &Service{balance: &memBooks{byShard: map[int]shardBook{
			0: {Accounts: []checkAccount{
				{ID: 1, AccountNo: "ACCT-A", BalanceDirection: directionCredit, Balance: 1},
				{ID: 2, AccountNo: "ACCT-Z", BalanceDirection: "NOPE"},
			}},
			1: {},
		}}}
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)
		if len(got.Failures) != 0 {
			t.Fatalf("failures = %+v", got.Failures)
		}
		failDelta(t, before, 0)
	})

	t.Run("unknown entry direction", func(t *testing.T) {
		svc := &Service{balance: &memBooks{byShard: map[int]shardBook{
			0: {Entries: []checkLeg{{AccountID: 1, Direction: "NOPE", Amount: 1}}},
			1: {},
		}}}
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)
		if len(got.Failures) != 0 {
			t.Fatalf("failures = %+v", got.Failures)
		}
		failDelta(t, before, 0)
	})

	t.Run("merchant skips voucher and transit", func(t *testing.T) {
		src := &memBooks{
			byShard: map[int]shardBook{
				0: {
					Accounts: []checkAccount{
						{ID: 7, AccountNo: "ACCT-M", BalanceDirection: directionCredit, AllowOverdraft: overdraftNo},
					},
					Txns: []checkTxn{{ID: 1, EntryCount: 2, Entries: []checkTxnEntry{
						{Direction: directionDebit, Amount: 3, AccountExists: true},
					}}},
					TransitBalances: []money.Amount{money.Amount(math.MaxInt64), 1},
				},
				1: {},
			},
			errShard: map[int]error{1: errors.New("shard 1 should not be read")},
		}
		svc := &Service{balance: src}
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "M100002", true)
		if err != nil || !got.Passed || len(got.Failures) != 0 {
			t.Fatalf("got %+v err=%v", got, err)
		}
		if len(src.calls) != 1 || src.calls[0] != 0 {
			t.Fatalf("calls = %v", src.calls)
		}
		empty := &Service{balance: &memBooks{}}
		miss, err := empty.CheckBalance(context.Background(), "M100002", true)
		assertErrno(t, err, errno.ErrAccountNotFound)
		if miss.Passed || len(miss.Failures) != 0 {
			t.Fatalf("missing = %+v", miss)
		}
		two := &Service{balance: &memBooks{byShard: map[int]shardBook{0: {Accounts: []checkAccount{
			{ID: 1, AccountNo: "A", BalanceDirection: directionCredit},
			{ID: 2, AccountNo: "B", BalanceDirection: directionCredit},
		}}}}}
		_, err = two.CheckBalance(context.Background(), "M100002", false)
		assertErrno(t, err, errno.ErrInternal)
		failDelta(t, before, 0)
	})

	t.Run("read error discards failures", func(t *testing.T) {
		src := &memBooks{
			byShard:  manyBooks(),
			errShard: map[int]error{1: errors.New("db read failed")},
		}
		svc := &Service{balance: src}
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)
		if len(got.Failures) != 0 {
			t.Fatalf("failures = %+v", got.Failures)
		}
		if strings.Contains(err.Error(), "select") || strings.Contains(err.Error(), "SELECT") {
			t.Fatalf("Error() leaked SQL: %s", err.Error())
		}
		failDelta(t, before, 0)
	})

	t.Run("overflow", func(t *testing.T) {
		svc := &Service{balance: &memBooks{byShard: map[int]shardBook{
			0: {
				Accounts: []checkAccount{{ID: 1, AccountNo: "ACCT-O", BalanceDirection: directionCredit, AllowOverdraft: overdraftYes}},
				Entries: []checkLeg{
					{AccountID: 1, Direction: directionCredit, Amount: money.Amount(math.MaxInt64)},
					{AccountID: 1, Direction: directionCredit, Amount: 1},
				},
			},
			1: {},
		}}}
		before := ledgerCheckFailures.Load()
		got, err := svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)
		if len(got.Failures) != 0 {
			t.Fatalf("failures = %+v", got.Failures)
		}
		failDelta(t, before, 0)
	})

	t.Run("quiescent false does not add transit", func(t *testing.T) {
		huge := money.Amount(math.MaxInt64)
		books := map[int]shardBook{
			0: {ConfirmedTransferFreezes: []money.Amount{huge}, TransitBalances: []money.Amount{huge}},
			1: {ConfirmedTransferFreezes: []money.Amount{huge}, TransitBalances: []money.Amount{huge}},
		}
		before := ledgerCheckFailures.Load()
		got, err := (&Service{balance: &memBooks{byShard: books}}).CheckBalance(context.Background(), "", false)
		if err != nil || !got.Passed || len(got.Failures) != 0 {
			t.Fatalf("quiet false: %+v err=%v", got, err)
		}
		got, err = (&Service{balance: &memBooks{byShard: books}}).CheckBalance(context.Background(), "", true)
		assertErrno(t, err, errno.ErrInternal)
		if len(got.Failures) != 0 {
			t.Fatalf("failures = %+v", got.Failures)
		}
		failDelta(t, before, 0)
	})

	t.Run("empty full passes", func(t *testing.T) {
		before := ledgerCheckFailures.Load()
		got, err := (&Service{balance: &memBooks{}}).CheckBalance(context.Background(), "", true)
		if err != nil || !got.Passed || len(got.Failures) != 0 {
			t.Fatalf("got %+v err=%v", got, err)
		}
		failDelta(t, before, 0)
	})

	t.Run("duplicate account is internal", func(t *testing.T) {
		svc := &Service{balance: &memBooks{byShard: map[int]shardBook{
			0: {Accounts: []checkAccount{
				{ID: 3, AccountNo: "A", BalanceDirection: directionCredit},
				{ID: 3, AccountNo: "B", BalanceDirection: directionCredit},
			}},
			1: {},
		}}}
		before := ledgerCheckFailures.Load()
		_, err := svc.CheckBalance(context.Background(), "", false)
		assertErrno(t, err, errno.ErrInternal)
		failDelta(t, before, 0)
	})
}

// ACC-10：失败打一条 ERROR。成功和 10003 不打这条。字段只用已有名字。
func TestCheckBalanceLogsOnce(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	bad := &Service{balance: &memBooks{byShard: map[int]shardBook{
		0: {Accounts: []checkAccount{{ID: 1, AccountNo: "ACCT-LOG-1", BalanceDirection: directionCredit, Balance: 2}}},
	}}}
	got, err := bad.CheckBalance(context.Background(), "M100002", true)
	if err != nil || got.Passed {
		t.Fatalf("got %+v err=%v", got, err)
	}
	text := buf.String()
	if !strings.Contains(text, "balance check failed") || !strings.Contains(text, "merchant_id=M100002") {
		t.Fatalf("log = %s", text)
	}
	if strings.Contains(text, "ACCT-LOG-1") || strings.Contains(text, "SELECT") {
		t.Fatalf("log leaked: %s", text)
	}
	if strings.Count(text, "balance check failed") != 1 {
		t.Fatalf("log lines = %s", text)
	}

	buf.Reset()
	_, err = bad.CheckBalance(context.Background(), "", false)
	if err != nil {
		t.Fatal(err)
	}
	full := buf.String()
	if !strings.Contains(full, "balance check failed") || strings.Contains(full, "merchant_id") {
		t.Fatalf("full log = %s", full)
	}

	buf.Reset()
	_, err = (&Service{balance: &memBooks{}}).CheckBalance(context.Background(), "M100002", false)
	assertErrno(t, err, errno.ErrAccountNotFound)
	if buf.Len() != 0 {
		t.Fatalf("40001 logged %s", buf.String())
	}

	t.Run("context ids", func(t *testing.T) {
		const traceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
		const spanHex = "00f067aa0ba902b7"
		const requestID = "req-acc10"
		tid, err := trace.TraceIDFromHex(traceHex)
		if err != nil {
			t.Fatal(err)
		}
		sid, err := trace.SpanIDFromHex(spanHex)
		if err != nil {
			t.Fatal(err)
		}
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    tid,
			SpanID:     sid,
			TraceFlags: trace.FlagsSampled,
			Remote:     true,
		})
		ctx := logx.WithRequestID(trace.ContextWithRemoteSpanContext(context.Background(), sc), requestID)

		var jsonBuf bytes.Buffer
		prev := slog.Default()
		logx.SetDefault("account", &jsonBuf)
		t.Cleanup(func() {
			logx.SetDefault("account", io.Discard)
			slog.SetDefault(prev)
		})

		got, err := bad.CheckBalance(ctx, "M100002", true)
		if err != nil || got.Passed {
			t.Fatalf("got %+v err=%v", got, err)
		}
		out := jsonBuf.String()
		if !strings.Contains(out, `"trace_id":"`+traceHex+`"`) ||
			!strings.Contains(out, `"span_id":"`+spanHex+`"`) ||
			!strings.Contains(out, `"request_id":"`+requestID+`"`) {
			t.Fatalf("ids: %s", out)
		}
		if strings.Contains(out, `"trace_id":""`) || strings.Contains(out, `"span_id":""`) || strings.Contains(out, `"request_id":""`) {
			t.Fatalf("empty ids: %s", out)
		}
	})
}

// ACC-10：快照语句是 REPEATABLE READ 一致性读，最后一条分录用 max(id)。
func TestCheckBalanceSnapshotSQL(t *testing.T) {
	if setRepeatableReadSQL != "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" {
		t.Fatalf("isolation SQL = %s", setRepeatableReadSQL)
	}
	if startConsistentSnapshotSQL != "START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT" {
		t.Fatalf("start SQL = %s", startConsistentSnapshotSQL)
	}
	if rollbackSnapshotSQL != "ROLLBACK" {
		t.Fatalf("rollback SQL = %s", rollbackSnapshotSQL)
	}
	for _, q := range []string{selectLastFullSQL, selectLastMerchantSQL} {
		if !strings.Contains(q, "MAX(id)") || strings.Contains(q, "created_at") {
			t.Fatalf("last entry SQL = %s", q)
		}
	}
	if !strings.Contains(selectCheckMerchantSQL, "sub_no = 0") || !strings.Contains(selectCheckMerchantSQL, "ORDER BY id") {
		t.Fatalf("merchant SQL = %s", selectCheckMerchantSQL)
	}
	for _, q := range []string{selectConfirmedTransferFreezeSQL, selectConfirmedPayeeSQL, selectTransitBalanceSQL} {
		if strings.Contains(q, "REFUND") || strings.Contains(q, "LOCAL") {
			t.Fatalf("transit SQL includes an excluded row: %s", q)
		}
	}
	if !strings.Contains(selectConfirmedTransferFreezeSQL, "TRANSFER") || !strings.Contains(selectConfirmedTransferFreezeSQL, "CONFIRMED") {
		t.Fatal("freeze SQL")
	}
	if !strings.Contains(selectConfirmedPayeeSQL, "PAYEE") || !strings.Contains(selectConfirmedPayeeSQL, "CONFIRMED") {
		t.Fatal("payee SQL")
	}
	if !strings.Contains(selectTransitBalanceSQL, "TRANSIT") {
		t.Fatal("transit SQL")
	}
}
