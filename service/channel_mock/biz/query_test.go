package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

func queryReq(txnType, reqNo string) QueryRequest {
	return QueryRequest{TxnType: txnType, ReqNo: reqNo}
}

func putTxn(e *env, t dal.Txn) dal.Txn {
	if t.TxnType == "" {
		t.TxnType = dal.TxnTypePay
	}
	if t.Scenario == "" {
		t.Scenario = scenario.Normal.String()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = baseTime
	}
	return e.store.put(t)
}

// CH-02 验收 2：未受理的 req_no 返回 NOT_FOUND，且不插入行。
func TestQueryNotFoundDoesNotInsert(t *testing.T) {
	e := newEnv(t)
	got, err := e.svc.Query(context.Background(), queryReq(dal.TxnTypePay, "PA_MISSING"))
	if err != nil {
		t.Fatal(err)
	}
	if got != (QueryResult{}) {
		t.Fatalf("result = %+v, want zero (not found)", got)
	}
	if e.store.count() != 0 || e.store.inserts != 0 || e.store.gets != 1 || e.ids.n != 0 || e.sched.len() != 0 {
		t.Fatalf("rows=%d inserts=%d gets=%d ids=%d scheduled=%d",
			e.store.count(), e.store.inserts, e.store.gets, e.ids.n, e.sched.len())
	}
	if len(e.hook.list()) != 0 {
		t.Fatal("hook called")
	}
}

// CH-02 验收 3：PROCESSING 返回当前行，finished_at 为空（零值，由 handler 省略）。
func TestQueryProcessing(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	accepted, err := e.svc.Pay(ctx, payReq("PA_PROC", 10000))
	if err != nil {
		t.Fatal(err)
	}
	scheduled := e.sched.len()
	got, err := e.svc.Query(ctx, queryReq(dal.TxnTypePay, "PA_PROC"))
	if err != nil {
		t.Fatal(err)
	}
	want := QueryResult{
		Found: true, Status: dal.StatusProcessing, Amount: 10000, ChannelTxnNo: accepted.ChannelTxnNo,
	}
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
	if !got.FinishedAt.IsZero() {
		t.Fatal("processing finished_at is set")
	}
	if e.sched.len() != scheduled || len(e.hook.list()) != 0 {
		t.Fatal("query scheduled or called the hook")
	}
	row, _ := e.store.row("PA_PROC")
	if row.Status != dal.StatusProcessing || !row.FinishedAt.IsZero() {
		t.Fatalf("row changed: %+v", row)
	}
}

// CH-02 验收 4：FAIL（金额末两位 01）完成后查单返回 FAILED，带 finished_at。
func TestQueryFailAfterFinish(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	accepted, err := e.svc.Pay(ctx, payReq("PA_FAIL", 10001))
	if err != nil {
		t.Fatal(err)
	}
	e.clk.Advance(2 * time.Second)
	if n := e.sched.RunDue(e.clk.Now()); n != 1 {
		t.Fatalf("ran %d tasks", n)
	}
	row, ok := e.store.row("PA_FAIL")
	if !ok || row.Status != dal.StatusFailed || row.Scenario != scenario.Fail.String() || row.FinishedAt.IsZero() || row.BillDate != "" {
		t.Fatalf("row = %+v", row)
	}
	hooks := len(e.hook.list())
	got, err := e.svc.Query(ctx, queryReq(dal.TxnTypePay, "PA_FAIL"))
	if err != nil {
		t.Fatal(err)
	}
	want := QueryResult{
		Found: true, Status: dal.StatusFailed, Amount: 10001,
		ChannelTxnNo: accepted.ChannelTxnNo, FinishedAt: row.FinishedAt,
	}
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
	if e.sched.len() != 0 || len(e.hook.list()) != hooks {
		t.Fatal("query scheduled or called the hook again")
	}
}

// CH-02 验收 5：amount 为 0 的行原样返回 0。
func TestQueryAmountZero(t *testing.T) {
	e := newEnv(t)
	row := putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_ZERO", ReqNo: "PA_ZERO", Amount: 0, Status: dal.StatusClosed,
	})
	got, err := e.svc.Query(context.Background(), queryReq(dal.TxnTypePay, "PA_ZERO"))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Amount != 0 || got.Status != dal.StatusClosed || got.ChannelTxnNo != row.ChannelTxnNo {
		t.Fatalf("result = %+v", got)
	}
	if e.store.inserts != 0 {
		t.Fatal("query inserted")
	}
}

// CH-02 验收 6：同一 req_no 的 PAY 与 REFUND 互不串单。
func TestQueryPayAndRefundDoNotCross(t *testing.T) {
	e := newEnv(t)
	pay := putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_PAY", TxnType: dal.TxnTypePay, ReqNo: "SAME", Amount: 10000, Status: dal.StatusSuccess,
		FinishedAt: baseTime,
	})
	refund := putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_RF", TxnType: dal.TxnTypeRefund, ReqNo: "SAME", Amount: 3000, Status: dal.StatusProcessing,
	})
	onlyPay := putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_ONLY_PAY", TxnType: dal.TxnTypePay, ReqNo: "ONLY_PAY", Amount: 500, Status: dal.StatusSuccess,
	})
	onlyRefund := putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_ONLY_RF", TxnType: dal.TxnTypeRefund, ReqNo: "ONLY_RF", Amount: 700, Status: dal.StatusFailed,
		FinishedAt: baseTime.Add(time.Second),
	})
	cases := []struct {
		name    string
		txnType string
		reqNo   string
		want    QueryResult
	}{
		{"pay same", dal.TxnTypePay, "SAME", QueryResult{Found: true, Status: pay.Status, Amount: pay.Amount, ChannelTxnNo: pay.ChannelTxnNo, FinishedAt: pay.FinishedAt}},
		{"refund same", dal.TxnTypeRefund, "SAME", QueryResult{Found: true, Status: refund.Status, Amount: refund.Amount, ChannelTxnNo: refund.ChannelTxnNo}},
		{"pay only", dal.TxnTypePay, "ONLY_PAY", QueryResult{Found: true, Status: onlyPay.Status, Amount: onlyPay.Amount, ChannelTxnNo: onlyPay.ChannelTxnNo}},
		{"refund of only pay", dal.TxnTypeRefund, "ONLY_PAY", QueryResult{}},
		{"refund only", dal.TxnTypeRefund, "ONLY_RF", QueryResult{Found: true, Status: onlyRefund.Status, Amount: onlyRefund.Amount, ChannelTxnNo: onlyRefund.ChannelTxnNo, FinishedAt: onlyRefund.FinishedAt}},
		{"pay of only refund", dal.TxnTypePay, "ONLY_RF", QueryResult{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := e.svc.Query(context.Background(), queryReq(c.txnType, c.reqNo))
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("result = %+v, want %+v", got, c.want)
			}
		})
	}
	if e.store.inserts != 0 || e.sched.len() != 0 || len(e.hook.list()) != 0 {
		t.Fatal("query wrote, scheduled, or called the hook")
	}
}

// CH-02 验收 8：查单不调用调度器、不调用 OnFinished。
func TestQueryDoesNotScheduleOrNotify(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_SLOW", ReqNo: "PA_SLOW", Amount: 10004, Status: dal.StatusSuccess,
		Scenario: scenario.Slow.String(), FinishedAt: baseTime,
	})
	if _, err := e.svc.Query(ctx, queryReq(dal.TxnTypePay, "PA_SLOW")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Pay(ctx, payReq("PA_NEW", 10002)); err != nil {
		t.Fatal(err)
	}
	hooks := len(e.hook.list())
	scheduled := e.sched.len()
	inserts := e.store.inserts
	if _, err := e.svc.Query(ctx, queryReq(dal.TxnTypePay, "PA_NEW")); err != nil {
		t.Fatal(err)
	}
	if len(e.hook.list()) != hooks || e.sched.len() != scheduled || e.store.inserts != inserts {
		t.Fatalf("hooks %d→%d scheduled %d→%d inserts %d→%d",
			hooks, len(e.hook.list()), scheduled, e.sched.len(), inserts, e.store.inserts)
	}
}

// CH-02 验收 9：scenario=SLOW 且已是 SUCCESS 时查单仍返回 SUCCESS，不推迟。
func TestQuerySlowAlreadySuccess(t *testing.T) {
	e := newEnv(t)
	finished := baseTime.Add(30 * time.Second)
	row := putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_SLOW", ReqNo: "PA_SLOW", Amount: 10004, Status: dal.StatusSuccess,
		Scenario: scenario.Slow.String(), FinishedAt: finished, NotifySentCount: 4,
	})
	got, err := e.svc.Query(context.Background(), queryReq(dal.TxnTypePay, "PA_SLOW"))
	if err != nil {
		t.Fatal(err)
	}
	want := QueryResult{
		Found: true, Status: dal.StatusSuccess, Amount: row.Amount,
		ChannelTxnNo: row.ChannelTxnNo, FinishedAt: finished,
	}
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
	if e.sched.len() != 0 || len(e.hook.list()) != 0 {
		t.Fatal("slow query scheduled or notified")
	}
}

func TestQueryRejectsInvalidRequest(t *testing.T) {
	cases := []struct {
		name string
		req  QueryRequest
	}{
		{"empty txn_type", queryReq("", "PA1")},
		{"lower pay", queryReq("pay", "PA1")},
		{"unknown txn_type", queryReq("CLOSE", "PA1")},
		{"empty req_no", queryReq(dal.TxnTypePay, "")},
		{"req_no 33 runes", queryReq(dal.TxnTypeRefund, strings.Repeat("单", 33))},
		{"req_no U+FFFD", queryReq(dal.TxnTypePay, "PA\uFFFD")},
		{"req_no invalid utf8", queryReq(dal.TxnTypePay, "PA\xff")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			_, err := e.svc.Query(context.Background(), c.req)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("err = %v, want ErrInvalidRequest", err)
			}
			if e.store.inserts != 0 || e.store.gets != 0 || e.ids.n != 0 || e.sched.len() != 0 {
				t.Fatal("invalid query touched store, id generator or scheduler")
			}
		})
	}
}

func TestQueryAcceptsBoundaryReqNo(t *testing.T) {
	e := newEnv(t)
	reqNo := strings.Repeat("单", 32)
	row := putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_BOUND", TxnType: dal.TxnTypeRefund, ReqNo: reqNo, Amount: 1, Status: dal.StatusProcessing,
	})
	got, err := e.svc.Query(context.Background(), queryReq(dal.TxnTypeRefund, reqNo))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.ChannelTxnNo != row.ChannelTxnNo || got.Amount != money.Amount(1) {
		t.Fatalf("result = %+v", got)
	}
}

func TestQueryStoreErrorAndCanceled(t *testing.T) {
	t.Run("store error", func(t *testing.T) {
		e := newEnv(t)
		e.store.getErr = errStore
		_, err := e.svc.Query(context.Background(), queryReq(dal.TxnTypePay, "PA1"))
		assertInternal(t, err)
		if !errors.Is(err, errStore) {
			t.Fatalf("err = %v", err)
		}
		if e.store.gets != 1 || e.store.inserts != 0 || e.sched.len() != 0 {
			t.Fatal("store error was treated as a write")
		}
	})
	t.Run("canceled", func(t *testing.T) {
		e := newEnv(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := e.svc.Query(ctx, queryReq(dal.TxnTypePay, "PA1"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
		if e.store.gets != 0 {
			t.Fatal("read with canceled context")
		}
	})
	t.Run("deadline", func(t *testing.T) {
		e := newEnv(t)
		ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
		defer cancel()
		_, err := e.svc.Query(ctx, queryReq(dal.TxnTypePay, "PA1"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", err)
		}
		if e.store.gets != 0 {
			t.Fatal("read with expired context")
		}
	})
	t.Run("canceled beats invalid", func(t *testing.T) {
		e := newEnv(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := e.svc.Query(ctx, queryReq("", ""))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	})
}

// 省略规则看列是不是 NULL，不看 status。PROCESSING 但列有值时照样返回。
func TestQueryReturnsFinishedAtWhenColumnSet(t *testing.T) {
	e := newEnv(t)
	finished := baseTime.Add(time.Millisecond)
	putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_ODD", ReqNo: "PA_ODD", Amount: 10, Status: dal.StatusProcessing, FinishedAt: finished,
	})
	got, err := e.svc.Query(context.Background(), queryReq(dal.TxnTypePay, "PA_ODD"))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Status != dal.StatusProcessing || !got.FinishedAt.Equal(finished) {
		t.Fatalf("result = %+v", got)
	}
}
