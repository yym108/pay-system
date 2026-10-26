//go:build integration

package biz

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

func (e dbEnv) nullTimes(t *testing.T, reqNo string) (finished, bill sql.NullTime) {
	t.Helper()
	err := e.raw.QueryRowContext(e.ctx,
		`SELECT finished_at, bill_date FROM mock_txn WHERE txn_type = 'PAY' AND req_no = ?`, reqNo).
		Scan(&finished, &bill)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return finished, bill
}

// CH-03 验收 1：规则表每一行（真实库）。普通关单不写 finished_at；CLOSE_RACE 写入 finished_at 与 bill_date。
func TestCloseRulesMySQL(t *testing.T) {
	const reqNo = "PA_RULE"
	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"processing", func(t *testing.T) {
			e := openMock(t)
			hook := &hookRecorder{}
			sched := &manualSched{}
			svc, _ := newQuerySvc(t, e, hook, sched)
			accepted, err := svc.Pay(e.ctx, payReq(reqNo, 10000))
			if err != nil {
				t.Fatal(err)
			}
			res, err := svc.Close(e.ctx, closeReq(reqNo))
			if err != nil {
				t.Fatal(err)
			}
			if res != okClosed() {
				t.Fatalf("close = %+v", res)
			}
			row := e.row(t, reqNo)
			finished, bill := e.nullTimes(t, reqNo)
			if row.Status != dal.StatusClosed || finished.Valid || bill.Valid || row.Amount != 10000 ||
				row.ChannelTxnNo != accepted.ChannelTxnNo || row.NotifySentCount != 0 || len(hook.list()) != 0 || sched.len() != 1 {
				t.Fatalf("row = %+v finished = %v bill = %v hooks = %d", row, finished, bill, len(hook.list()))
			}
		}},
		{"close_race", func(t *testing.T) {
			e := openMock(t)
			hook := &hookRecorder{}
			sched := &manualSched{}
			svc, clk := newQuerySvc(t, e, hook, sched)
			accepted, err := svc.Pay(e.ctx, payReq(reqNo, 10009))
			if err != nil {
				t.Fatal(err)
			}
			res, err := svc.Close(e.ctx, closeReq(reqNo))
			if err != nil {
				t.Fatal(err)
			}
			row := e.row(t, reqNo)
			wantAt := clk.Now().Truncate(time.Millisecond)
			calls := hook.list()
			if res != alreadyPaid() || row.Status != dal.StatusSuccess || !row.FinishedAt.Equal(wantAt) ||
				row.BillDate != "2026-10-23" || row.Amount != 10009 || row.ChannelTxnNo != accepted.ChannelTxnNo ||
				row.Scenario != scenario.CloseRace.String() || row.NotifySentCount != 0 ||
				len(calls) != 1 || calls[0].Status != dal.StatusSuccess || !calls[0].FinishedAt.Equal(wantAt) {
				t.Fatalf("close = %+v row = %+v hooks = %+v", res, row, calls)
			}
		}},
		{"success", func(t *testing.T) {
			e := openMock(t)
			hook := &hookRecorder{}
			sched := &manualSched{}
			svc, clk := newQuerySvc(t, e, hook, sched)
			if _, err := svc.Pay(e.ctx, payReq(reqNo, 10000)); err != nil {
				t.Fatal(err)
			}
			finishDue(t, clk, sched)
			before := e.row(t, reqNo)
			updated := e.updatedAt(t, dal.TxnTypePay, reqNo)
			res, err := svc.Close(e.ctx, closeReq(reqNo))
			if err != nil {
				t.Fatal(err)
			}
			row := e.row(t, reqNo)
			if res != alreadyPaid() || !sameTxn(before, row) || !e.updatedAt(t, dal.TxnTypePay, reqNo).Equal(updated) ||
				len(hook.list()) != 1 {
				t.Fatalf("close = %+v row = %+v", res, row)
			}
		}},
		{"closed", func(t *testing.T) {
			e := openMock(t)
			hook := &hookRecorder{}
			sched := &manualSched{}
			svc, _ := newQuerySvc(t, e, hook, sched)
			e.insertTxn(t, dal.Txn{
				ChannelTxnNo: "MC_CLOSED", TxnType: dal.TxnTypePay, ReqNo: reqNo, Amount: 10000,
				Status: dal.StatusClosed, Scenario: scenario.Normal.String(),
				NotifyURL: "http://gateway:8080/callback/mockpay", NotifySentCount: 4, CreatedAt: baseTime,
			})
			before := e.row(t, reqNo)
			updated := e.updatedAt(t, dal.TxnTypePay, reqNo)
			res, err := svc.Close(e.ctx, closeReq(reqNo))
			if err != nil {
				t.Fatal(err)
			}
			row := e.row(t, reqNo)
			finished, bill := e.nullTimes(t, reqNo)
			if res != okClosed() || !sameTxn(before, row) || finished.Valid || bill.Valid ||
				!e.updatedAt(t, dal.TxnTypePay, reqNo).Equal(updated) || len(hook.list()) != 0 {
				t.Fatalf("close = %+v row = %+v", res, row)
			}
		}},
		{"failed", func(t *testing.T) {
			e := openMock(t)
			hook := &hookRecorder{}
			sched := &manualSched{}
			svc, clk := newQuerySvc(t, e, hook, sched)
			if _, err := svc.Pay(e.ctx, payReq(reqNo, 10001)); err != nil {
				t.Fatal(err)
			}
			finishDue(t, clk, sched)
			before := e.row(t, reqNo)
			updated := e.updatedAt(t, dal.TxnTypePay, reqNo)
			res, err := svc.Close(e.ctx, closeReq(reqNo))
			if err != nil {
				t.Fatal(err)
			}
			row := e.row(t, reqNo)
			if res != okFailed() || !sameTxn(before, row) || row.Status != dal.StatusFailed || row.BillDate != "" ||
				!e.updatedAt(t, dal.TxnTypePay, reqNo).Equal(updated) || len(hook.list()) != 1 {
				t.Fatalf("close = %+v row = %+v", res, row)
			}
		}},
		{"missing", func(t *testing.T) {
			e := openMock(t)
			hook := &hookRecorder{}
			sched := &manualSched{}
			svc, clk := newQuerySvc(t, e, hook, sched)
			res, err := svc.Close(e.ctx, closeReq(reqNo))
			if err != nil {
				t.Fatal(err)
			}
			row := e.row(t, reqNo)
			finished, bill := e.nullTimes(t, reqNo)
			wantAt := clk.Now().Truncate(time.Millisecond)
			if res != okClosed() || row.TxnType != dal.TxnTypePay || row.Amount != 0 || row.Status != dal.StatusClosed ||
				row.Scenario != scenario.Normal.String() || row.NotifyURL != "" || finished.Valid || bill.Valid ||
				row.NotifySentCount != 0 || !row.CreatedAt.Equal(wantAt) || row.ChannelTxnNo == "" ||
				e.countExact(t, dal.TxnTypePay, reqNo) != 1 || sched.len() != 0 || len(hook.list()) != 0 {
				t.Fatalf("close = %+v row = %+v finished = %v bill = %v", res, row, finished, bill)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, c.run)
	}
}

// CH-03 验收 2：未知 req_no 关单后再受理返回 ORDER_CLOSED；查单 amount 为 0、status 为 CLOSED。
func TestCloseUnknownThenPayMySQL(t *testing.T) {
	e := openMock(t)
	hook := &hookRecorder{}
	sched := &manualSched{}
	svc, _ := newQuerySvc(t, e, hook, sched)
	const reqNo = "PA_NONE"
	if _, err := svc.Close(e.ctx, closeReq(reqNo)); err != nil {
		t.Fatal(err)
	}
	tomb := e.row(t, reqNo)
	finished, bill := e.nullTimes(t, reqNo)
	if tomb.Amount != 0 || tomb.Scenario != scenario.Normal.String() || tomb.NotifyURL != "" || finished.Valid || bill.Valid {
		t.Fatalf("tombstone = %+v finished = %v bill = %v", tomb, finished, bill)
	}
	q, err := svc.Query(e.ctx, queryReq(dal.TxnTypePay, reqNo))
	if err != nil {
		t.Fatal(err)
	}
	if !q.Found || q.Amount != 0 || q.Status != dal.StatusClosed || q.ChannelTxnNo != tomb.ChannelTxnNo || !q.FinishedAt.IsZero() {
		t.Fatalf("query = %+v", q)
	}
	for _, amount := range []int64{1, 10009} {
		got, err := svc.Pay(e.ctx, payReq(reqNo, money.Amount(amount)))
		if err != nil {
			t.Fatal(err)
		}
		want := PayResult{Result: ResultRejected, Reason: ReasonOrderClosed, Status: dal.StatusClosed}
		if got != want {
			t.Fatalf("pay %d = %+v", amount, got)
		}
	}
	row := e.row(t, reqNo)
	if row.Amount != 0 || row.Status != dal.StatusClosed || row.ChannelTxnNo != tomb.ChannelTxnNo ||
		row.Scenario != scenario.Normal.String() || row.NotifyURL != "" || !row.FinishedAt.IsZero() ||
		row.NotifySentCount != 0 || sched.len() != 0 || len(hook.list()) != 0 {
		t.Fatalf("row changed: %+v", row)
	}
}

// CH-03：关单不取消已排程的自动完成。到期任务 CAS 得 0 行，不得把 CLOSED 改回 SUCCESS。
func TestCloseThenScheduledFinishMySQL(t *testing.T) {
	e := openMock(t)
	hook := &hookRecorder{}
	sched := &manualSched{}
	svc, clk := newQuerySvc(t, e, hook, sched)
	if _, err := svc.Pay(e.ctx, payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	if sched.len() != 1 {
		t.Fatalf("scheduled = %d", sched.len())
	}
	if _, err := svc.Close(e.ctx, closeReq("PA001")); err != nil {
		t.Fatal(err)
	}
	if sched.len() != 1 {
		t.Fatalf("close canceled the timer, scheduled = %d", sched.len())
	}
	finishDue(t, clk, sched)
	row := e.row(t, "PA001")
	finished, bill := e.nullTimes(t, "PA001")
	if row.Status != dal.StatusClosed || finished.Valid || bill.Valid || len(hook.list()) != 0 {
		t.Fatalf("row = %+v finished = %v hooks = %d", row, finished, len(hook.list()))
	}
}

// CH-03：CLOSE_RACE 关单调钩子一次；定时器后到不再调。自动完成先成功时关单也不再调。
func TestCloseRaceHookMySQL(t *testing.T) {
	t.Run("close wins", func(t *testing.T) {
		e := openMock(t)
		hook := &hookRecorder{}
		sched := &manualSched{}
		svc, clk := newQuerySvc(t, e, hook, sched)
		if _, err := svc.Pay(e.ctx, payReq("PA_RACE", 10009)); err != nil {
			t.Fatal(err)
		}
		res, err := svc.Close(e.ctx, closeReq("PA_RACE"))
		if err != nil {
			t.Fatal(err)
		}
		row := e.row(t, "PA_RACE")
		calls := hook.list()
		if res != alreadyPaid() || len(calls) != 1 || calls[0].Status != dal.StatusSuccess ||
			!calls[0].FinishedAt.Equal(row.FinishedAt) || row.BillDate != "2026-10-23" {
			t.Fatalf("close = %+v row = %+v hooks = %+v", res, row, calls)
		}
		finishDue(t, clk, sched)
		got := e.row(t, "PA_RACE")
		if got.Status != dal.StatusSuccess || !got.FinishedAt.Equal(row.FinishedAt) || len(hook.list()) != 1 {
			t.Fatalf("after timer: %+v hooks = %d", got, len(hook.list()))
		}
	})
	t.Run("finish wins", func(t *testing.T) {
		e := openMock(t)
		hook := &hookRecorder{}
		sched := &manualSched{}
		svc, clk := newQuerySvc(t, e, hook, sched)
		if _, err := svc.Pay(e.ctx, payReq("PA_RACE", 10009)); err != nil {
			t.Fatal(err)
		}
		finishDue(t, clk, sched)
		before := e.row(t, "PA_RACE")
		res, err := svc.Close(e.ctx, closeReq("PA_RACE"))
		if err != nil {
			t.Fatal(err)
		}
		row := e.row(t, "PA_RACE")
		if res != alreadyPaid() || !sameTxn(before, row) || len(hook.list()) != 1 {
			t.Fatalf("close = %+v row = %+v hooks = %d", res, row, len(hook.list()))
		}
	})
}

// CH-03：NEVER_FINISH 仍可关成 CLOSED，关单不新增调度。这不是 CH-06 的其余场景。
func TestCloseNeverFinishMySQL(t *testing.T) {
	e := openMock(t)
	hook := &hookRecorder{}
	sched := &manualSched{}
	svc, _ := newQuerySvc(t, e, hook, sched)
	if _, err := svc.Pay(e.ctx, payReq("PA_NEVER", 10008)); err != nil {
		t.Fatal(err)
	}
	if sched.len() != 0 {
		t.Fatal("NEVER_FINISH was scheduled")
	}
	res, err := svc.Close(e.ctx, closeReq("PA_NEVER"))
	if err != nil {
		t.Fatal(err)
	}
	row := e.row(t, "PA_NEVER")
	finished, bill := e.nullTimes(t, "PA_NEVER")
	if res != okClosed() || row.Status != dal.StatusClosed || row.Scenario != scenario.NeverFinish.String() ||
		finished.Valid || bill.Valid || sched.len() != 0 || len(hook.list()) != 0 {
		t.Fatalf("close = %+v row = %+v", res, row)
	}
}

// CH-03：同号的 REFUND 行不关，也不挡住 PAY 防悬挂行。
func TestCloseIgnoresRefundMySQL(t *testing.T) {
	e := openMock(t)
	hook := &hookRecorder{}
	sched := &manualSched{}
	svc, _ := newQuerySvc(t, e, hook, sched)
	e.insertTxn(t, dal.Txn{
		ChannelTxnNo: "MC_RF", TxnType: dal.TxnTypeRefund, ReqNo: "SAME", Amount: 3000,
		Status: dal.StatusProcessing, Scenario: scenario.Normal.String(),
		NotifyURL: "http://gateway:8080/callback/mockpay", CreatedAt: baseTime,
	})
	res, err := svc.Close(e.ctx, closeReq("SAME"))
	if err != nil {
		t.Fatal(err)
	}
	if res != okClosed() {
		t.Fatalf("close = %+v", res)
	}
	pay := e.row(t, "SAME")
	refund, ok, err := e.store.Get(e.ctx, dal.TxnTypeRefund, "SAME")
	if err != nil || !ok {
		t.Fatalf("refund ok = %v err = %v", ok, err)
	}
	if pay.TxnType != dal.TxnTypePay || pay.Amount != 0 || pay.Status != dal.StatusClosed ||
		refund.Status != dal.StatusProcessing || refund.Amount != 3000 || refund.ChannelTxnNo != "MC_RF" ||
		e.countExact(t, dal.TxnTypePay, "SAME") != 1 || len(hook.list()) != 0 {
		t.Fatalf("pay = %+v refund = %+v", pay, refund)
	}
}

// CH-03：两个并发关单（真实库）。缺失 req_no 只有 1 行；同一笔 PROCESSING 只有 1 行 CLOSED。
func TestCloseConcurrentMySQL(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		e := openMock(t)
		hook := &hookRecorder{}
		sched := &manualSched{}
		svc, _ := newQuerySvc(t, e, hook, sched)
		const n = 2
		const reqNo = "PA_MISS"
		results := make([]CloseResult, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				results[i], errs[i] = svc.Close(e.ctx, closeReq(reqNo))
			}(i)
		}
		close(start)
		wg.Wait()
		for i := 0; i < n; i++ {
			if errs[i] != nil || results[i] != okClosed() {
				t.Fatalf("goroutine %d: %+v %v", i, results[i], errs[i])
			}
		}
		if e.countExact(t, dal.TxnTypePay, reqNo) != 1 || len(hook.list()) != 0 {
			t.Fatalf("rows = %d hooks = %d", e.countExact(t, dal.TxnTypePay, reqNo), len(hook.list()))
		}
		row := e.row(t, reqNo)
		if row.Amount != 0 || row.Status != dal.StatusClosed {
			t.Fatalf("row = %+v", row)
		}
	})
	t.Run("processing", func(t *testing.T) {
		e := openMock(t)
		hook := &hookRecorder{}
		sched := &manualSched{}
		svc, _ := newQuerySvc(t, e, hook, sched)
		accepted, err := svc.Pay(e.ctx, payReq("PA_CONC", 10000))
		if err != nil {
			t.Fatal(err)
		}
		const n = 2
		results := make([]CloseResult, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				results[i], errs[i] = svc.Close(e.ctx, closeReq("PA_CONC"))
			}(i)
		}
		close(start)
		wg.Wait()
		for i := 0; i < n; i++ {
			if errs[i] != nil || results[i] != okClosed() {
				t.Fatalf("goroutine %d: %+v %v", i, results[i], errs[i])
			}
		}
		if e.countExact(t, dal.TxnTypePay, "PA_CONC") != 1 || len(hook.list()) != 0 {
			t.Fatalf("rows = %d hooks = %d", e.countExact(t, dal.TxnTypePay, "PA_CONC"), len(hook.list()))
		}
		row := e.row(t, "PA_CONC")
		finished, bill := e.nullTimes(t, "PA_CONC")
		if row.Status != dal.StatusClosed || finished.Valid || bill.Valid || row.Amount != 10000 ||
			row.ChannelTxnNo != accepted.ChannelTxnNo || row.NotifySentCount != 0 {
			t.Fatalf("row = %+v finished = %v", row, finished)
		}
	})
}
