package biz

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

func closeReq(reqNo string) CloseRequest { return CloseRequest{ReqNo: reqNo} }

func okClosed() CloseResult {
	return CloseResult{Result: ResultOK, Status: dal.StatusClosed}
}

func alreadyPaid() CloseResult {
	return CloseResult{Result: ResultAlreadyPaid, Status: dal.StatusSuccess}
}

func okFailed() CloseResult {
	return CloseResult{Result: ResultOK, Status: dal.StatusFailed}
}

func payAndMaybeFinish(t *testing.T, e *env, reqNo string, amount int64, finish bool) dal.Txn {
	t.Helper()
	if _, err := e.svc.Pay(context.Background(), payReq(reqNo, money.Amount(amount))); err != nil {
		t.Fatal(err)
	}
	if finish {
		e.clk.Advance(2 * time.Second)
		if n := e.sched.RunDue(e.clk.Now()); n != 1 {
			t.Fatalf("ran %d", n)
		}
	}
	row, ok := e.store.row(reqNo)
	if !ok {
		t.Fatal("missing row")
	}
	return row
}

// CH-03 验收 1：规则表每一行。普通关单不写 finished_at、不调钩子；CLOSE_RACE 写终态并调一次钩子。
func TestCloseRules(t *testing.T) {
	const reqNo = "PA_RULE"
	cases := []struct {
		name    string
		prepare func(t *testing.T, e *env) (before dal.Txn, priorHooks int)
		want    CloseResult
		check   func(t *testing.T, e *env, before dal.Txn, priorHooks int)
	}{
		{
			name: "processing",
			prepare: func(t *testing.T, e *env) (dal.Txn, int) {
				return payAndMaybeFinish(t, e, reqNo, 10000, false), 0
			},
			want: okClosed(),
			check: func(t *testing.T, e *env, before dal.Txn, _ int) {
				row, _ := e.store.row(reqNo)
				if row.Status != dal.StatusClosed || !row.FinishedAt.IsZero() || row.BillDate != "" ||
					row.Amount != before.Amount || row.ChannelTxnNo != before.ChannelTxnNo ||
					row.NotifySentCount != before.NotifySentCount || row.Scenario != before.Scenario ||
					e.store.closes != 1 || len(e.hook.list()) != 0 || e.sched.len() != 1 {
					t.Fatalf("row=%+v closes=%d hooks=%d scheduled=%d", row, e.store.closes, len(e.hook.list()), e.sched.len())
				}
			},
		},
		{
			name: "close_race",
			prepare: func(t *testing.T, e *env) (dal.Txn, int) {
				return payAndMaybeFinish(t, e, reqNo, 10009, false), 0
			},
			want: alreadyPaid(),
			check: func(t *testing.T, e *env, before dal.Txn, _ int) {
				row, _ := e.store.row(reqNo)
				wantAt := e.clk.Now().Truncate(time.Millisecond)
				calls := e.hook.list()
				if row.Status != dal.StatusSuccess || !row.FinishedAt.Equal(wantAt) || row.BillDate != "2026-10-23" ||
					row.Amount != 10009 || row.ChannelTxnNo != before.ChannelTxnNo || row.NotifySentCount != 0 ||
					row.Scenario != scenario.CloseRace.String() || e.store.closes != 0 ||
					len(calls) != 1 || calls[0].Status != dal.StatusSuccess || !calls[0].FinishedAt.Equal(wantAt) {
					t.Fatalf("row=%+v hooks=%+v closes=%d", row, calls, e.store.closes)
				}
			},
		},
		{
			name: "success",
			prepare: func(t *testing.T, e *env) (dal.Txn, int) {
				row := payAndMaybeFinish(t, e, reqNo, 10000, true)
				return row, len(e.hook.list())
			},
			want: alreadyPaid(),
			check: func(t *testing.T, e *env, before dal.Txn, priorHooks int) {
				row, _ := e.store.row(reqNo)
				if row.Status != before.Status || !row.FinishedAt.Equal(before.FinishedAt) || row.BillDate != before.BillDate ||
					row.Amount != before.Amount || row.ChannelTxnNo != before.ChannelTxnNo ||
					row.NotifySentCount != before.NotifySentCount || len(e.hook.list()) != priorHooks || e.store.closes != 0 {
					t.Fatalf("row=%+v before=%+v hooks=%d", row, before, len(e.hook.list()))
				}
			},
		},
		{
			name: "closed",
			prepare: func(t *testing.T, e *env) (dal.Txn, int) {
				row := payAndMaybeFinish(t, e, reqNo, 10000, false)
				e.store.setStatus(reqNo, dal.StatusClosed)
				row.Status = dal.StatusClosed
				return row, 0
			},
			want: okClosed(),
			check: func(t *testing.T, e *env, before dal.Txn, _ int) {
				row, _ := e.store.row(reqNo)
				if row.Status != dal.StatusClosed || !row.FinishedAt.IsZero() || row.Amount != before.Amount ||
					row.ChannelTxnNo != before.ChannelTxnNo || row.NotifySentCount != 0 ||
					len(e.hook.list()) != 0 || e.store.closes != 0 {
					t.Fatalf("row=%+v closes=%d", row, e.store.closes)
				}
			},
		},
		{
			name: "failed",
			prepare: func(t *testing.T, e *env) (dal.Txn, int) {
				row := payAndMaybeFinish(t, e, reqNo, 10001, true)
				return row, len(e.hook.list())
			},
			want: okFailed(),
			check: func(t *testing.T, e *env, before dal.Txn, priorHooks int) {
				row, _ := e.store.row(reqNo)
				if row.Status != dal.StatusFailed || !row.FinishedAt.Equal(before.FinishedAt) || row.BillDate != "" ||
					row.Amount != before.Amount || row.NotifySentCount != before.NotifySentCount ||
					len(e.hook.list()) != priorHooks || e.store.closes != 0 {
					t.Fatalf("row=%+v hooks=%d", row, len(e.hook.list()))
				}
			},
		},
		{
			name: "missing",
			prepare: func(t *testing.T, e *env) (dal.Txn, int) {
				return dal.Txn{}, 0
			},
			want: okClosed(),
			check: func(t *testing.T, e *env, _ dal.Txn, _ int) {
				row, ok := e.store.row(reqNo)
				wantAt := baseTime.Truncate(time.Millisecond)
				if !ok || row.TxnType != dal.TxnTypePay || row.Amount != 0 || row.Status != dal.StatusClosed ||
					row.Scenario != scenario.Normal.String() || row.NotifyURL != "" || !row.FinishedAt.IsZero() ||
					row.BillDate != "" || row.NotifySentCount != 0 || !strings.HasPrefix(row.ChannelTxnNo, "MC") ||
					!row.CreatedAt.Equal(wantAt) || e.store.inserts != 1 || e.store.closes != 0 ||
					e.sched.len() != 0 || len(e.hook.list()) != 0 {
					t.Fatalf("row=%+v ok=%v inserts=%d", row, ok, e.store.inserts)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			before, hooks := c.prepare(t, e)
			res, err := e.svc.Close(context.Background(), closeReq(reqNo))
			if err != nil {
				t.Fatal(err)
			}
			if res != c.want {
				t.Fatalf("result = %+v, want %+v", res, c.want)
			}
			c.check(t, e, before, hooks)
			again, err := e.svc.Close(context.Background(), closeReq(reqNo))
			if err != nil {
				t.Fatal(err)
			}
			if again != c.want {
				t.Fatalf("repeat = %+v", again)
			}
		})
	}
}

// CH-03 验收 2：未知 req_no 关单后再受理，不论金额都是 ORDER_CLOSED；墓碑金额仍为 0。
func TestCloseUnknownThenPay(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	res, err := e.svc.Close(ctx, closeReq("PA_NONE"))
	if err != nil {
		t.Fatal(err)
	}
	if res != okClosed() {
		t.Fatalf("close = %+v", res)
	}
	tomb, _ := e.store.row("PA_NONE")
	for _, amount := range []int64{1, 10009} {
		got, err := e.svc.Pay(ctx, payReq("PA_NONE", money.Amount(amount)))
		if err != nil {
			t.Fatal(err)
		}
		want := PayResult{Result: ResultRejected, Reason: ReasonOrderClosed, Status: dal.StatusClosed}
		if got != want {
			t.Fatalf("pay %d = %+v", amount, got)
		}
	}
	row, _ := e.store.row(reqNoOf(tomb))
	if row.Amount != 0 || row.Status != dal.StatusClosed || row.Scenario != scenario.Normal.String() ||
		row.NotifyURL != "" || !row.FinishedAt.IsZero() || row.BillDate != "" ||
		row.ChannelTxnNo != tomb.ChannelTxnNo || row.NotifySentCount != 0 || e.sched.len() != 0 || len(e.hook.list()) != 0 {
		t.Fatalf("tombstone changed: %+v", row)
	}
	q, err := e.svc.Query(ctx, queryReq(dal.TxnTypePay, "PA_NONE"))
	if err != nil {
		t.Fatal(err)
	}
	if !q.Found || q.Amount != 0 || q.Status != dal.StatusClosed || q.ChannelTxnNo != tomb.ChannelTxnNo || !q.FinishedAt.IsZero() {
		t.Fatalf("query = %+v", q)
	}
}

func reqNoOf(t dal.Txn) string { return t.ReqNo }

// CH-03：已排程的自动完成在关单之后跑到期任务，行保持 CLOSED，不写 finished_at，不再调钩子。
func TestCloseThenScheduledFinish(t *testing.T) {
	e := newEnv(t)
	payAndMaybeFinish(t, e, "PA001", 10000, false)
	if e.sched.len() != 1 {
		t.Fatalf("scheduled = %d", e.sched.len())
	}
	res, err := e.svc.Close(context.Background(), closeReq("PA001"))
	if err != nil {
		t.Fatal(err)
	}
	if res != okClosed() || e.sched.len() != 1 {
		t.Fatalf("close = %+v scheduled = %d", res, e.sched.len())
	}
	e.clk.Advance(2 * time.Second)
	if n := e.sched.RunDue(e.clk.Now()); n != 1 {
		t.Fatalf("ran %d", n)
	}
	row, _ := e.store.row("PA001")
	if row.Status != dal.StatusClosed || !row.FinishedAt.IsZero() || row.BillDate != "" || len(e.hook.list()) != 0 {
		t.Fatalf("row = %+v hooks = %d", row, len(e.hook.list()))
	}
}

// CH-03：CLOSE_RACE 关单调钩子恰好一次；自动完成先写 SUCCESS 时关单不再增加钩子。
func TestCloseRaceHookOnce(t *testing.T) {
	t.Run("close wins", func(t *testing.T) {
		e := newEnv(t)
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e.svc.opts.OnFinished = func(ctx context.Context, row dal.Txn) {
			cancel()
			if ctx.Err() != nil {
				t.Errorf("hook ctx err = %v", ctx.Err())
			}
			if _, ok := ctx.Deadline(); ok {
				t.Error("hook ctx has deadline")
			}
			e.hook.fn(ctx, row)
		}
		payAndMaybeFinish(t, e, "PA_RACE", 10009, false)
		res, err := e.svc.Close(parent, closeReq("PA_RACE"))
		if err != nil {
			t.Fatal(err)
		}
		if res != alreadyPaid() {
			t.Fatalf("close = %+v", res)
		}
		calls := e.hook.list()
		row, _ := e.store.row("PA_RACE")
		if len(calls) != 1 || calls[0].Status != dal.StatusSuccess || !calls[0].FinishedAt.Equal(row.FinishedAt) {
			t.Fatalf("hooks = %+v row = %+v", calls, row)
		}
		e.clk.Advance(2 * time.Second)
		if n := e.sched.RunDue(e.clk.Now()); n != 1 {
			t.Fatalf("ran %d", n)
		}
		if len(e.hook.list()) != 1 {
			t.Fatalf("hooks after finish = %d", len(e.hook.list()))
		}
		if got, _ := e.store.row("PA_RACE"); got.Status != dal.StatusSuccess || !got.FinishedAt.Equal(row.FinishedAt) {
			t.Fatalf("finish overwrote: %+v", got)
		}
	})
	t.Run("finish wins", func(t *testing.T) {
		e := newEnv(t)
		payAndMaybeFinish(t, e, "PA_RACE", 10009, true)
		if len(e.hook.list()) != 1 {
			t.Fatalf("hooks = %d", len(e.hook.list()))
		}
		before, _ := e.store.row("PA_RACE")
		res, err := e.svc.Close(context.Background(), closeReq("PA_RACE"))
		if err != nil {
			t.Fatal(err)
		}
		if res != alreadyPaid() || len(e.hook.list()) != 1 {
			t.Fatalf("close = %+v hooks = %d", res, len(e.hook.list()))
		}
		row, _ := e.store.row("PA_RACE")
		if row.Status != dal.StatusSuccess || !row.FinishedAt.Equal(before.FinishedAt) || row.NotifySentCount != before.NotifySentCount {
			t.Fatalf("row changed: %+v", row)
		}
	})
}

// CH-03：NEVER_FINISH 仍是 PROCESSING 时可以关成 CLOSED，关单不新增调度。
func TestCloseNeverFinish(t *testing.T) {
	e := newEnv(t)
	row := payAndMaybeFinish(t, e, "PA_NEVER", 10008, false)
	if row.Status != dal.StatusProcessing || row.Scenario != scenario.NeverFinish.String() || e.sched.len() != 0 {
		t.Fatalf("before = %+v scheduled = %d", row, e.sched.len())
	}
	res, err := e.svc.Close(context.Background(), closeReq("PA_NEVER"))
	if err != nil {
		t.Fatal(err)
	}
	if res != okClosed() {
		t.Fatalf("close = %+v", res)
	}
	got, _ := e.store.row("PA_NEVER")
	if got.Status != dal.StatusClosed || !got.FinishedAt.IsZero() || got.BillDate != "" ||
		e.sched.len() != 0 || len(e.hook.list()) != 0 || got.ChannelTxnNo != row.ChannelTxnNo {
		t.Fatalf("after = %+v scheduled = %d", got, e.sched.len())
	}
}

// CH-03：两个并发关单。缺失 req_no 最终 1 行；同一笔 PROCESSING 最终 1 行 CLOSED。
func TestCloseConcurrent(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		e := newEnv(t)
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
				results[i], errs[i] = e.svc.Close(context.Background(), closeReq("PA_MISS"))
			}(i)
		}
		close(start)
		wg.Wait()
		for i := 0; i < n; i++ {
			if errs[i] != nil || results[i] != okClosed() {
				t.Fatalf("goroutine %d: %+v %v", i, results[i], errs[i])
			}
		}
		if e.store.count() != 1 || len(e.hook.list()) != 0 {
			t.Fatalf("rows = %d hooks = %d", e.store.count(), len(e.hook.list()))
		}
		row, _ := e.store.row("PA_MISS")
		if row.Status != dal.StatusClosed || row.Amount != 0 {
			t.Fatalf("row = %+v", row)
		}
	})
	t.Run("processing", func(t *testing.T) {
		e := newEnv(t)
		before := payAndMaybeFinish(t, e, "PA_CONC", 10000, false)
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
				results[i], errs[i] = e.svc.Close(context.Background(), closeReq("PA_CONC"))
			}(i)
		}
		close(start)
		wg.Wait()
		for i := 0; i < n; i++ {
			if errs[i] != nil || results[i] != okClosed() {
				t.Fatalf("goroutine %d: %+v %v", i, results[i], errs[i])
			}
		}
		if e.store.count() != 1 || len(e.hook.list()) != 0 {
			t.Fatalf("rows = %d hooks = %d", e.store.count(), len(e.hook.list()))
		}
		row, _ := e.store.row("PA_CONC")
		if row.Status != dal.StatusClosed || !row.FinishedAt.IsZero() || row.Amount != before.Amount ||
			row.ChannelTxnNo != before.ChannelTxnNo || row.NotifySentCount != before.NotifySentCount {
			t.Fatalf("row = %+v", row)
		}
	})
}

// CH-03：1062 后再读，按读到的当前状态走规则表，不把冲突当错误抛出。
func TestCloseDuplicateThenReread(t *testing.T) {
	cases := []struct {
		name   string
		amount int64
		finish bool
		preset string
		want   CloseResult
		hooks  int
	}{
		{name: "processing", amount: 10000, want: okClosed()},
		{name: "close_race", amount: 10009, want: alreadyPaid(), hooks: 1},
		{name: "success", amount: 10000, finish: true, want: alreadyPaid(), hooks: 1},
		{name: "failed", amount: 10001, finish: true, want: okFailed(), hooks: 1},
		{name: "closed", amount: 10000, preset: dal.StatusClosed, want: okClosed()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			payAndMaybeFinish(t, e, "PA001", c.amount, c.finish)
			if c.preset != "" {
				e.store.setStatus("PA001", c.preset)
			}
			before, _ := e.store.row("PA001")
			e.store.hideGets = 1
			res, err := e.svc.Close(context.Background(), closeReq("PA001"))
			if err != nil {
				t.Fatal(err)
			}
			if res != c.want {
				t.Fatalf("result = %+v, want %+v", res, c.want)
			}
			if len(e.hook.list()) != c.hooks {
				t.Fatalf("hooks = %d, want %d", len(e.hook.list()), c.hooks)
			}
			row, _ := e.store.row("PA001")
			if row.ChannelTxnNo != before.ChannelTxnNo || row.Amount != before.Amount || e.store.count() != 1 {
				t.Fatalf("row changed: %+v", row)
			}
		})
	}
}

// CH-03：1062 但按 (PAY, req_no) 读不到行（流水号撞车）是内部错误，不关别的行。
func TestCloseDuplicateWithoutRow(t *testing.T) {
	e := newEnv(t)
	const no = "MC00000000000000000001"
	e.store.put(dal.Txn{
		ChannelTxnNo: no, TxnType: dal.TxnTypePay, ReqNo: "OTHER", Amount: 10, Status: dal.StatusProcessing,
	})
	_, err := e.svc.Close(context.Background(), closeReq("PA_NEW"))
	if !errors.Is(err, errDuplicateWithoutRow) {
		t.Fatalf("err = %v", err)
	}
	if _, ok := e.store.row("PA_NEW"); ok {
		t.Fatal("inserted the missing req_no")
	}
	row, _ := e.store.row("OTHER")
	if row.Status != dal.StatusProcessing || row.ChannelTxnNo != no || len(e.hook.list()) != 0 {
		t.Fatalf("other row = %+v", row)
	}
}

// CH-03：插入报非 1062 时再读。流水号相同视为墓碑已写入；读失败、读不到或是别的行则返回原错误。
func TestCloseInsertUncertainReread(t *testing.T) {
	errRead := errors.New("read down")
	t.Run("match", func(t *testing.T) {
		e := newEnv(t)
		e.store.insertCommitErr = errStore
		res, err := e.svc.Close(context.Background(), closeReq("PA_NEW"))
		if err != nil {
			t.Fatal(err)
		}
		if res != okClosed() {
			t.Fatalf("result = %+v", res)
		}
		row, ok := e.store.row("PA_NEW")
		if !ok || row.Status != dal.StatusClosed || row.Amount != 0 || !strings.HasPrefix(row.ChannelTxnNo, "MC") ||
			len(e.hook.list()) != 0 || e.sched.len() != 0 {
			t.Fatalf("row = %+v ok = %v", row, ok)
		}
	})
	t.Run("not written", func(t *testing.T) {
		e := newEnv(t)
		e.store.insertErr = errStore
		_, err := e.svc.Close(context.Background(), closeReq("PA_NEW"))
		if !errors.Is(err, errStore) {
			t.Fatalf("err = %v", err)
		}
		if e.store.count() != 0 || len(e.hook.list()) != 0 {
			t.Fatal("row or hook appeared")
		}
	})
	t.Run("read fails", func(t *testing.T) {
		e := newEnv(t)
		e.store.insertCommitErr = errStore
		e.store.getErrs = []error{nil, errRead}
		_, err := e.svc.Close(context.Background(), closeReq("PA_NEW"))
		if !errors.Is(err, errStore) || errors.Is(err, errRead) {
			t.Fatalf("err = %v", err)
		}
		if len(e.hook.list()) != 0 {
			t.Fatal("hook called")
		}
	})
	t.Run("other row", func(t *testing.T) {
		e := newEnv(t)
		e.store.insertCommitErr = errStore
		e.store.afterInsert = func() {
			e.store.update("PA_NEW", func(row *dal.Txn) { row.ChannelTxnNo = "MC_OTHER" })
		}
		_, err := e.svc.Close(context.Background(), closeReq("PA_NEW"))
		if !errors.Is(err, errStore) {
			t.Fatalf("err = %v", err)
		}
		if len(e.hook.list()) != 0 {
			t.Fatal("hook called")
		}
	})
}

// CH-03：CAS 得 0 行后再读，按新状态回答，没有不确定的写入时不调钩子。
func TestCloseCASMissReread(t *testing.T) {
	cases := []struct {
		name   string
		status string
		want   CloseResult
	}{
		{name: "success", status: dal.StatusSuccess, want: alreadyPaid()},
		{name: "closed", status: dal.StatusClosed, want: okClosed()},
		{name: "failed", status: dal.StatusFailed, want: okFailed()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			payAndMaybeFinish(t, e, "PA001", 10000, false)
			e.store.beforeClose = func() { e.store.setStatus("PA001", c.status) }
			res, err := e.svc.Close(context.Background(), closeReq("PA001"))
			if err != nil {
				t.Fatal(err)
			}
			if res != c.want || len(e.hook.list()) != 0 {
				t.Fatalf("result = %+v hooks = %d", res, len(e.hook.list()))
			}
			row, _ := e.store.row("PA001")
			if row.Status != c.status {
				t.Fatalf("status = %s", row.Status)
			}
		})
	}
}

// CH-03：写入结果不确定时，再读与这次尝试一致才算生效。普通关单不调钩子；CLOSE_RACE 一致才调一次。
func TestCloseUncertainWrite(t *testing.T) {
	errRead := errors.New("read down")
	t.Run("ordinary match", func(t *testing.T) {
		e := newEnv(t)
		payAndMaybeFinish(t, e, "PA001", 10000, false)
		e.store.closeCommitErrs = []error{errStore}
		res, err := e.svc.Close(context.Background(), closeReq("PA001"))
		if err != nil {
			t.Fatal(err)
		}
		row, _ := e.store.row("PA001")
		if res != okClosed() || row.Status != dal.StatusClosed || !row.FinishedAt.IsZero() || len(e.hook.list()) != 0 {
			t.Fatalf("result = %+v row = %+v", res, row)
		}
	})
	t.Run("ordinary mismatch", func(t *testing.T) {
		e := newEnv(t)
		payAndMaybeFinish(t, e, "PA001", 10000, false)
		e.store.closeCommitErrs = []error{errStore}
		e.store.beforeGetByID = func() {
			e.store.update("PA001", func(row *dal.Txn) {
				row.Status = dal.StatusSuccess
				row.FinishedAt = baseTime
			})
		}
		res, err := e.svc.Close(context.Background(), closeReq("PA001"))
		if err != nil {
			t.Fatal(err)
		}
		row, _ := e.store.row("PA001")
		if res != alreadyPaid() || row.Status != dal.StatusSuccess || len(e.hook.list()) != 0 {
			t.Fatalf("result = %+v row = %+v hooks = %d", res, row, len(e.hook.list()))
		}
	})
	t.Run("ordinary not applied", func(t *testing.T) {
		e := newEnv(t)
		payAndMaybeFinish(t, e, "PA001", 10000, false)
		e.store.closeErrs = []error{errStore}
		_, err := e.svc.Close(context.Background(), closeReq("PA001"))
		if !errors.Is(err, errStore) {
			t.Fatalf("err = %v", err)
		}
		row, _ := e.store.row("PA001")
		if row.Status != dal.StatusProcessing || len(e.hook.list()) != 0 {
			t.Fatalf("row = %+v", row)
		}
	})
	t.Run("race match", func(t *testing.T) {
		e := newEnv(t)
		payAndMaybeFinish(t, e, "PA_RACE", 10009, false)
		e.store.finishCommitErrs = []error{errStore}
		res, err := e.svc.Close(context.Background(), closeReq("PA_RACE"))
		if err != nil {
			t.Fatal(err)
		}
		row, _ := e.store.row("PA_RACE")
		calls := e.hook.list()
		if res != alreadyPaid() || len(calls) != 1 || calls[0].Status != dal.StatusSuccess ||
			!calls[0].FinishedAt.Equal(row.FinishedAt) || row.BillDate != "2026-10-23" {
			t.Fatalf("result = %+v row = %+v hooks = %+v", res, row, calls)
		}
	})
	t.Run("race mismatch", func(t *testing.T) {
		e := newEnv(t)
		payAndMaybeFinish(t, e, "PA_RACE", 10009, false)
		e.store.finishCommitErrs = []error{errStore}
		e.store.beforeGetByID = func() {
			e.store.update("PA_RACE", func(row *dal.Txn) { row.FinishedAt = row.FinishedAt.Add(time.Second) })
		}
		res, err := e.svc.Close(context.Background(), closeReq("PA_RACE"))
		if err != nil {
			t.Fatal(err)
		}
		if res != alreadyPaid() || len(e.hook.list()) != 0 {
			t.Fatalf("result = %+v hooks = %d", res, len(e.hook.list()))
		}
	})
	t.Run("reread fails", func(t *testing.T) {
		e := newEnv(t)
		payAndMaybeFinish(t, e, "PA001", 10000, false)
		e.store.closeCommitErrs = []error{errStore}
		e.store.getByIDErrs = []error{errRead}
		_, err := e.svc.Close(context.Background(), closeReq("PA001"))
		if !errors.Is(err, errStore) || errors.Is(err, errRead) {
			t.Fatalf("err = %v", err)
		}
		if len(e.hook.list()) != 0 {
			t.Fatal("hook called")
		}
	})
}

// CH-03：同号 REFUND 行不关、不当成已存在。先读失败也不写行。
func TestCloseIgnoresRefundAndReadError(t *testing.T) {
	e := newEnv(t)
	refund := putTxn(e, dal.Txn{
		ChannelTxnNo: "MC_RF", TxnType: dal.TxnTypeRefund, ReqNo: "SAME", Amount: 3000, Status: dal.StatusProcessing,
	})
	res, err := e.svc.Close(context.Background(), closeReq("SAME"))
	if err != nil {
		t.Fatal(err)
	}
	if res != okClosed() {
		t.Fatalf("close = %+v", res)
	}
	pay, _ := e.store.row("SAME")
	got, _ := e.store.rowOf(dal.TxnTypeRefund, "SAME")
	if pay.TxnType != dal.TxnTypePay || pay.Amount != 0 || pay.Status != dal.StatusClosed ||
		got.Status != refund.Status || got.Amount != refund.Amount || got.ChannelTxnNo != refund.ChannelTxnNo {
		t.Fatalf("pay = %+v refund = %+v", pay, got)
	}

	e.store.getErr = errStore
	_, err = e.svc.Close(context.Background(), closeReq("PA_DOWN"))
	if !errors.Is(err, errStore) {
		t.Fatalf("err = %v", err)
	}
	if _, ok := e.store.row("PA_DOWN"); ok {
		t.Fatal("inserted after read error")
	}
}

func TestCloseRejectsBadReqNo(t *testing.T) {
	cases := []string{"", strings.Repeat("a", 33), "A\uFFFD", "A\xff"}
	for _, reqNo := range cases {
		t.Run(reqNo, func(t *testing.T) {
			e := newEnv(t)
			_, err := e.svc.Close(context.Background(), closeReq(reqNo))
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("err = %v", err)
			}
			if e.store.count() != 0 || e.store.inserts != 0 || e.ids.n != 0 {
				t.Fatal("invalid request wrote a row")
			}
		})
	}
}

func TestCloseCanceledContext(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.svc.Close(ctx, closeReq("PA001"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if e.store.inserts != 0 || e.ids.n != 0 {
		t.Fatal("canceled request wrote a row")
	}
}

// CH-03：关单不调用 account。biz 没有账户依赖，close.go 不引用账户服务。
func TestCloseDoesNotCallAccount(t *testing.T) {
	src, err := os.ReadFile("close.go")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(src, []byte("CheckBalance")) || bytes.Contains(src, []byte("service/account")) {
		t.Fatal("close.go references account")
	}
	e := newEnv(t)
	rt := reflect.TypeOf(e.svc).Elem()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if strings.Contains(strings.ToLower(f.Type.String()), "account") {
			t.Fatalf("field %s type %s", f.Name, f.Type)
		}
	}
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Close(context.Background(), closeReq("PA001")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Close(context.Background(), closeReq("PA_NONE")); err != nil {
		t.Fatal(err)
	}
}
