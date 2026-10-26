//go:build integration

package biz

import (
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

func (e dbEnv) updatedAt(t *testing.T, txnType, reqNo string) time.Time {
	t.Helper()
	var ts time.Time
	err := e.raw.QueryRowContext(e.ctx,
		`SELECT updated_at FROM mock_txn WHERE txn_type = ? AND req_no = ?`, txnType, reqNo).Scan(&ts)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return ts
}

func (e dbEnv) countExact(t *testing.T, txnType, reqNo string) int {
	t.Helper()
	var n int
	err := e.raw.QueryRowContext(e.ctx,
		`SELECT COUNT(*) FROM mock_txn WHERE txn_type = ? AND req_no = ?`, txnType, reqNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func (e dbEnv) insertTxn(t *testing.T, row dal.Txn) {
	t.Helper()
	var finished any
	if !row.FinishedAt.IsZero() {
		finished = row.FinishedAt
	}
	_, err := e.raw.ExecContext(e.ctx, `
INSERT INTO mock_txn (
  channel_txn_no, txn_type, req_no, amount, status, scenario, notify_url, notify_sent_count, finished_at, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.ChannelTxnNo, row.TxnType, row.ReqNo, int64(row.Amount), row.Status, row.Scenario,
		row.NotifyURL, row.NotifySentCount, finished, row.CreatedAt)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func sameTxn(a, b dal.Txn) bool {
	return a.ID == b.ID && a.ChannelTxnNo == b.ChannelTxnNo && a.TxnType == b.TxnType && a.ReqNo == b.ReqNo &&
		a.Amount == b.Amount && a.Status == b.Status && a.Scenario == b.Scenario && a.NotifyURL == b.NotifyURL &&
		a.NotifySentCount == b.NotifySentCount && a.FinishedAt.Equal(b.FinishedAt) && a.BillDate == b.BillDate &&
		a.CreatedAt.Equal(b.CreatedAt)
}

func newQuerySvc(t *testing.T, e dbEnv, hook *hookRecorder, sched *manualSched) (*Service, *clock.Fake) {
	t.Helper()
	clk := &clock.Fake{}
	clk.Set(baseTime)
	svc := New(e.store, newIDs(t, clock.Real{}), clk, sched, Options{
		DelayMinMS: 200,
		DelayMaxMS: 2000,
		OnFinished: hook.fn,
	})
	return svc, clk
}

func finishDue(t *testing.T, clk *clock.Fake, sched *manualSched) {
	t.Helper()
	clk.Advance(2 * time.Second)
	if n := sched.RunDue(clk.Now()); n != 1 {
		t.Fatalf("ran %d tasks", n)
	}
}

// CH-02 验收 1：金额末两位 02（DROP_NOTIFY）受理后等到 SUCCESS，查单返回同一金额和流水号，行不变。
func TestQueryDropNotifyMySQL(t *testing.T) {
	e := openMock(t)
	hook := &hookRecorder{}
	sched := &manualSched{}
	svc, clk := newQuerySvc(t, e, hook, sched)
	const reqNo = "PA_DROP"
	const amount = int64(10002)
	accepted, err := svc.Pay(e.ctx, payReq(reqNo, amountOf(amount)))
	if err != nil {
		t.Fatal(err)
	}
	finishDue(t, clk, sched)
	before := e.row(t, reqNo)
	if before.Status != dal.StatusSuccess || before.Scenario != scenario.DropNotify.String() ||
		before.Amount != amountOf(amount) || before.ChannelTxnNo != accepted.ChannelTxnNo ||
		before.FinishedAt.IsZero() || before.NotifySentCount != 0 {
		t.Fatalf("before = %+v", before)
	}
	updated := e.updatedAt(t, dal.TxnTypePay, reqNo)
	hooks := len(hook.list())
	got, err := svc.Query(e.ctx, queryReq(dal.TxnTypePay, reqNo))
	if err != nil {
		t.Fatal(err)
	}
	want := QueryResult{
		Found: true, Status: dal.StatusSuccess, Amount: before.Amount,
		ChannelTxnNo: before.ChannelTxnNo, FinishedAt: before.FinishedAt,
	}
	if got != want {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
	after := e.row(t, reqNo)
	if !sameTxn(before, after) {
		t.Fatalf("row changed\nbefore %+v\nafter  %+v", before, after)
	}
	if !e.updatedAt(t, dal.TxnTypePay, reqNo).Equal(updated) {
		t.Fatal("updated_at changed")
	}
	if len(hook.list()) != hooks || sched.len() != 0 {
		t.Fatal("query scheduled or called the hook")
	}
}

// CH-02 验收 2：未受理的 req_no 返回 NOT_FOUND，表中没有该行。
func TestQueryNotFoundMySQL(t *testing.T) {
	e := openMock(t)
	svc, _ := newQuerySvc(t, e, &hookRecorder{}, &manualSched{})
	for _, txnType := range []string{dal.TxnTypePay, dal.TxnTypeRefund} {
		got, err := svc.Query(e.ctx, queryReq(txnType, "PA_NONE"))
		if err != nil {
			t.Fatal(err)
		}
		if got != (QueryResult{}) {
			t.Fatalf("%s result = %+v", txnType, got)
		}
		if n := e.countExact(t, txnType, "PA_NONE"); n != 0 {
			t.Fatalf("%s rows = %d", txnType, n)
		}
	}
	if n := e.count(t, "PA_NONE"); n != 0 {
		t.Fatalf("rows = %d", n)
	}
}

// CH-02 验收 3：尚未完成时查单返回 PROCESSING，finished_at 为空，不推进。
func TestQueryProcessingMySQL(t *testing.T) {
	e := openMock(t)
	hook := &hookRecorder{}
	sched := &manualSched{}
	svc, _ := newQuerySvc(t, e, hook, sched)
	accepted, err := svc.Pay(e.ctx, payReq("PA_PROC", 10000))
	if err != nil {
		t.Fatal(err)
	}
	before := e.row(t, "PA_PROC")
	updated := e.updatedAt(t, dal.TxnTypePay, "PA_PROC")
	got, err := svc.Query(e.ctx, queryReq(dal.TxnTypePay, "PA_PROC"))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Status != dal.StatusProcessing || !got.FinishedAt.IsZero() ||
		got.Amount != 10000 || got.ChannelTxnNo != accepted.ChannelTxnNo {
		t.Fatalf("result = %+v", got)
	}
	if !sameTxn(before, e.row(t, "PA_PROC")) || !e.updatedAt(t, dal.TxnTypePay, "PA_PROC").Equal(updated) {
		t.Fatal("row changed")
	}
	if sched.len() != 1 || len(hook.list()) != 0 {
		t.Fatal("query scheduled again or called the hook")
	}
}

// CH-02 验收 4：FAIL 完成后查单返回 FAILED 和 finished_at，不是 SUCCESS。
func TestQueryFailMySQL(t *testing.T) {
	e := openMock(t)
	hook := &hookRecorder{}
	sched := &manualSched{}
	svc, clk := newQuerySvc(t, e, hook, sched)
	accepted, err := svc.Pay(e.ctx, payReq("PA_FAIL", 10001))
	if err != nil {
		t.Fatal(err)
	}
	finishDue(t, clk, sched)
	before := e.row(t, "PA_FAIL")
	if before.Status != dal.StatusFailed || before.Scenario != scenario.Fail.String() ||
		before.FinishedAt.IsZero() || before.BillDate != "" {
		t.Fatalf("before = %+v", before)
	}
	updated := e.updatedAt(t, dal.TxnTypePay, "PA_FAIL")
	got, err := svc.Query(e.ctx, queryReq(dal.TxnTypePay, "PA_FAIL"))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Status != dal.StatusFailed || got.Amount != 10001 ||
		got.ChannelTxnNo != accepted.ChannelTxnNo || !got.FinishedAt.Equal(before.FinishedAt) {
		t.Fatalf("result = %+v", got)
	}
	if !sameTxn(before, e.row(t, "PA_FAIL")) || !e.updatedAt(t, dal.TxnTypePay, "PA_FAIL").Equal(updated) {
		t.Fatal("row changed")
	}
	if len(hook.list()) != 1 || sched.len() != 0 {
		t.Fatal("query scheduled or called the hook again")
	}
}

// CH-02 验收 5：直接插入 amount=0 的行，查单返回 0，不改行。
func TestQueryAmountZeroMySQL(t *testing.T) {
	e := openMock(t)
	e.insertTxn(t, dal.Txn{
		ChannelTxnNo: "MC_ZERO", TxnType: dal.TxnTypePay, ReqNo: "PA_ZERO", Amount: 0,
		Status: dal.StatusClosed, Scenario: scenario.Normal.String(), NotifySentCount: 2, CreatedAt: baseTime,
	})
	svc, _ := newQuerySvc(t, e, &hookRecorder{}, &manualSched{})
	before, ok, err := e.store.Get(e.ctx, dal.TxnTypePay, "PA_ZERO")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	updated := e.updatedAt(t, dal.TxnTypePay, "PA_ZERO")
	got, err := svc.Query(e.ctx, queryReq(dal.TxnTypePay, "PA_ZERO"))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Amount != 0 || got.Status != dal.StatusClosed || got.ChannelTxnNo != "MC_ZERO" || !got.FinishedAt.IsZero() {
		t.Fatalf("result = %+v", got)
	}
	after, _, err := e.store.Get(e.ctx, dal.TxnTypePay, "PA_ZERO")
	if err != nil {
		t.Fatal(err)
	}
	if !sameTxn(before, after) || !e.updatedAt(t, dal.TxnTypePay, "PA_ZERO").Equal(updated) {
		t.Fatal("row changed")
	}
}

// CH-02 验收 6：PAY 与 REFUND 按 (txn_type, req_no) 分开，只有一边有行时另一边是 NOT_FOUND。
func TestQueryPayRefundIsolatedMySQL(t *testing.T) {
	e := openMock(t)
	finished := baseTime.Add(time.Second)
	e.insertTxn(t, dal.Txn{
		ChannelTxnNo: "MC_PAY", TxnType: dal.TxnTypePay, ReqNo: "SAME", Amount: 10000,
		Status: dal.StatusSuccess, Scenario: scenario.Normal.String(), FinishedAt: finished, CreatedAt: baseTime,
	})
	e.insertTxn(t, dal.Txn{
		ChannelTxnNo: "MC_RF", TxnType: dal.TxnTypeRefund, ReqNo: "SAME", Amount: 3000,
		Status: dal.StatusProcessing, Scenario: scenario.Normal.String(), CreatedAt: baseTime,
	})
	e.insertTxn(t, dal.Txn{
		ChannelTxnNo: "MC_ONLY_PAY", TxnType: dal.TxnTypePay, ReqNo: "ONLY_PAY", Amount: 500,
		Status: dal.StatusSuccess, Scenario: scenario.Normal.String(), CreatedAt: baseTime,
	})
	e.insertTxn(t, dal.Txn{
		ChannelTxnNo: "MC_ONLY_RF", TxnType: dal.TxnTypeRefund, ReqNo: "ONLY_RF", Amount: 700,
		Status: dal.StatusFailed, Scenario: scenario.Fail.String(), FinishedAt: finished, CreatedAt: baseTime,
	})
	svc, _ := newQuerySvc(t, e, &hookRecorder{}, &manualSched{})
	cases := []struct {
		txnType string
		reqNo   string
		found   bool
		status  string
		amount  int64
		no      string
	}{
		{dal.TxnTypePay, "SAME", true, dal.StatusSuccess, 10000, "MC_PAY"},
		{dal.TxnTypeRefund, "SAME", true, dal.StatusProcessing, 3000, "MC_RF"},
		{dal.TxnTypeRefund, "ONLY_PAY", false, "", 0, ""},
		{dal.TxnTypePay, "ONLY_RF", false, "", 0, ""},
		{dal.TxnTypePay, "ONLY_PAY", true, dal.StatusSuccess, 500, "MC_ONLY_PAY"},
		{dal.TxnTypeRefund, "ONLY_RF", true, dal.StatusFailed, 700, "MC_ONLY_RF"},
	}
	for _, c := range cases {
		t.Run(c.txnType+"/"+c.reqNo, func(t *testing.T) {
			got, err := svc.Query(e.ctx, queryReq(c.txnType, c.reqNo))
			if err != nil {
				t.Fatal(err)
			}
			if c.found {
				if !got.Found || got.Status != c.status || int64(got.Amount) != c.amount || got.ChannelTxnNo != c.no {
					t.Fatalf("result = %+v", got)
				}
				return
			}
			if got != (QueryResult{}) {
				t.Fatalf("result = %+v, want not found", got)
			}
			if n := e.countExact(t, c.txnType, c.reqNo); n != 0 {
				t.Fatalf("rows = %d", n)
			}
		})
	}
}

// CH-02 验收 9：已是 SUCCESS 的 SLOW 行查单直接返回 SUCCESS。
func TestQuerySlowSuccessMySQL(t *testing.T) {
	e := openMock(t)
	// DATETIME(3) 只保留毫秒。写入前截断，读回才能与原值 Equal。
	finished := baseTime.Add(30 * time.Second).Truncate(time.Millisecond)
	e.insertTxn(t, dal.Txn{
		ChannelTxnNo: "MC_SLOW", TxnType: dal.TxnTypePay, ReqNo: "PA_SLOW", Amount: 10004,
		Status: dal.StatusSuccess, Scenario: scenario.Slow.String(), NotifySentCount: 4,
		FinishedAt: finished, CreatedAt: baseTime,
	})
	hook := &hookRecorder{}
	sched := &manualSched{}
	svc, _ := newQuerySvc(t, e, hook, sched)
	before, ok, err := e.store.Get(e.ctx, dal.TxnTypePay, "PA_SLOW")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	updated := e.updatedAt(t, dal.TxnTypePay, "PA_SLOW")
	got, err := svc.Query(e.ctx, queryReq(dal.TxnTypePay, "PA_SLOW"))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || got.Status != dal.StatusSuccess || got.Amount != 10004 ||
		got.ChannelTxnNo != "MC_SLOW" || !got.FinishedAt.Equal(finished) {
		t.Fatalf("result = %+v", got)
	}
	after, _, err := e.store.Get(e.ctx, dal.TxnTypePay, "PA_SLOW")
	if err != nil {
		t.Fatal(err)
	}
	if !sameTxn(before, after) || !e.updatedAt(t, dal.TxnTypePay, "PA_SLOW").Equal(updated) {
		t.Fatal("row changed")
	}
	if len(hook.list()) != 0 || sched.len() != 0 {
		t.Fatal("query scheduled or notified")
	}
}
