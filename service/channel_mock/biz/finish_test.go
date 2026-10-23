package biz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

// CH-01 规则 4：delay = MIN + crc32(req_no) % (MAX − MIN)，MAX == MIN 时取 MIN。
func TestDelay(t *testing.T) {
	for i := 0; i < 2000; i++ {
		reqNo := fmt.Sprintf("PA%019d", i)
		got := Delay(reqNo, 200, 2000)
		want := time.Duration(200+int64(crc32.ChecksumIEEE([]byte(reqNo))%1800)) * time.Millisecond
		if got != want {
			t.Fatalf("Delay(%s) = %v, want %v", reqNo, got, want)
		}
		if got < 200*time.Millisecond || got > 1999*time.Millisecond {
			t.Fatalf("Delay(%s) = %v out of [200ms, 1999ms]", reqNo, got)
		}
		if again := Delay(reqNo, 200, 2000); again != got {
			t.Fatal("Delay is not deterministic")
		}
	}
	if got := Delay("PA001", 500, 500); got != 500*time.Millisecond {
		t.Fatalf("MIN == MAX: %v", got)
	}
	if got := Delay("PA001", 0, 0); got != 0 {
		t.Fatalf("0, 0: %v", got)
	}
	if got := Delay("PA001", 0, 1); got != 0 {
		t.Fatalf("0, 1: %v", got)
	}
}

// CH-01 验收 2 前半：NORMAL 在 delay 到期前保持 PROCESSING，到期后 SUCCESS，全程不超过 2 秒。
func TestNormalFinishesWithinTwoSeconds(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	created := baseTime.Truncate(time.Millisecond)
	due := created.Add(Delay("PA001", 200, 2000))

	if n := e.sched.RunDue(due.Add(-time.Millisecond)); n != 0 {
		t.Fatalf("ran %d tasks before due", n)
	}
	e.clk.Set(due)
	if n := e.sched.RunDue(due); n != 1 {
		t.Fatalf("ran %d tasks at due", n)
	}
	row, _ := e.store.row("PA001")
	if row.Status != dal.StatusSuccess || !row.FinishedAt.Equal(due) || row.BillDate != "2026-10-23" {
		t.Fatalf("row = %+v", row)
	}
	if d := row.FinishedAt.Sub(row.CreatedAt); d >= 2*time.Second {
		t.Fatalf("finished after %v", d)
	}
	calls := e.hook.list()
	if len(calls) != 1 || calls[0].ID != row.ID || calls[0].Status != dal.StatusSuccess ||
		calls[0].ChannelTxnNo != row.ChannelTxnNo || !calls[0].FinishedAt.Equal(due) {
		t.Fatalf("hook calls = %+v", calls)
	}
}

// CH-01 规则 4 与 CH-06 表：只有 FAIL 置 FAILED；BILL_MISSING 成功但不写 bill_date；NEVER_FINISH 不调度。
func TestScenarioOutcomes(t *testing.T) {
	cases := []struct {
		amount   money.Amount
		scenario string
		status   string
		billed   bool
	}{
		{10000, "NORMAL", dal.StatusSuccess, true},
		{10001, "FAIL", dal.StatusFailed, false},
		{10002, "DROP_NOTIFY", dal.StatusSuccess, true},
		{10003, "DUP_NOTIFY", dal.StatusSuccess, true},
		{10004, "SLOW", dal.StatusSuccess, true},
		{10005, "ACCEPT_TIMEOUT", dal.StatusSuccess, true},
		{10006, "BILL_MISSING", dal.StatusSuccess, false},
		{10007, "BILL_AMOUNT_DIFF", dal.StatusSuccess, true},
		{10008, "NEVER_FINISH", dal.StatusProcessing, false},
		{10009, "CLOSE_RACE", dal.StatusSuccess, true},
	}
	for _, c := range cases {
		t.Run(c.scenario, func(t *testing.T) {
			e := newEnv(t)
			reqNo := fmt.Sprintf("PA%d", c.amount)
			if _, err := e.svc.Pay(context.Background(), payReq(reqNo, c.amount)); err != nil {
				t.Fatal(err)
			}
			if c.scenario == "NEVER_FINISH" && e.sched.len() != 0 {
				t.Fatal("NEVER_FINISH was scheduled")
			}
			e.clk.Advance(2 * time.Second)
			e.sched.RunDue(e.clk.Now())
			row, _ := e.store.row(reqNo)
			if row.Scenario != c.scenario || row.Status != c.status {
				t.Fatalf("row = %+v", row)
			}
			if c.status == dal.StatusProcessing {
				if !row.FinishedAt.IsZero() || len(e.hook.list()) != 0 {
					t.Fatalf("unfinished row = %+v", row)
				}
				return
			}
			if !row.FinishedAt.Equal(e.clk.Now().Truncate(time.Millisecond)) {
				t.Fatalf("finished_at = %v", row.FinishedAt)
			}
			if c.billed != (row.BillDate == "2026-10-23") || (!c.billed && row.BillDate != "") {
				t.Fatalf("bill_date = %q, billed = %v", row.BillDate, c.billed)
			}
			if calls := e.hook.list(); len(calls) != 1 || calls[0].Status != c.status {
				t.Fatalf("hook calls = %+v", calls)
			}
		})
	}
}

// CAS 只在 PROCESSING 时生效：别处（CH-03 关单）先改成 CLOSED 时，自动完成不覆盖，也不调用钩子。
func TestFinishLosesCASToClose(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	e.store.setStatus("PA001", dal.StatusClosed)
	e.clk.Advance(2 * time.Second)
	e.sched.RunDue(e.clk.Now())
	row, _ := e.store.row("PA001")
	if row.Status != dal.StatusClosed || !row.FinishedAt.IsZero() || row.BillDate != "" {
		t.Fatalf("row = %+v", row)
	}
	if len(e.hook.list()) != 0 {
		t.Fatal("hook called after lost CAS")
	}
	if e.sched.len() != 0 {
		t.Fatal("lost CAS was rescheduled")
	}
}

// 完成时写库失败不丢任务：按重试间隔重新调度，之后成功，钩子只调用一次。
func TestFinishRetriesOnStoreError(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	e.store.finishErrs = []error{errStore, errStore}
	e.clk.Advance(2 * time.Second)
	for i := 0; i < 2; i++ {
		if n := e.sched.RunDue(e.clk.Now()); n != 1 {
			t.Fatalf("round %d ran %d tasks", i, n)
		}
		row, _ := e.store.row("PA001")
		if row.Status != dal.StatusProcessing {
			t.Fatalf("round %d: status = %s", i, row.Status)
		}
		dues := e.sched.dues()
		if len(dues) != 1 || !dues[0].Equal(e.clk.Now().Add(finishRetryDelay)) {
			t.Fatalf("round %d: dues = %v", i, dues)
		}
		e.clk.Advance(finishRetryDelay)
	}
	e.sched.RunDue(e.clk.Now())
	row, _ := e.store.row("PA001")
	if row.Status != dal.StatusSuccess {
		t.Fatalf("status = %s", row.Status)
	}
	if len(e.hook.list()) != 1 {
		t.Fatalf("hook calls = %d", len(e.hook.list()))
	}
}

// 同一笔被调度两次（例如重启扫描与原调度重叠）时，CAS 保证只完成一次、钩子只调用一次。
func TestDuplicateScheduleFinishesOnce(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	if n, err := e.svc.Recover(context.Background()); err != nil || n != 1 {
		t.Fatalf("recover = %d, %v", n, err)
	}
	e.clk.Advance(2 * time.Second)
	if n := e.sched.RunDue(e.clk.Now()); n != 2 {
		t.Fatalf("ran %d", n)
	}
	if len(e.hook.list()) != 1 {
		t.Fatalf("hook calls = %d", len(e.hook.list()))
	}
}

// bill_date 是 finished_at 的东八区日期；时间先截到毫秒，避免 DATETIME(3) 进位跨天。
func TestBillDateUsesShanghaiAndMillis(t *testing.T) {
	cases := []struct {
		name     string
		finish   time.Time
		wantDate string
	}{
		// UTC 15:59:59.9996 = 东八区 23:59:59.9996。四舍五入到毫秒会进到次日 00:00:00.000。
		{"before midnight", time.Date(2026, 10, 23, 15, 59, 59, 999600000, time.UTC), "2026-10-23"},
		{"at midnight", time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC), "2026-10-24"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
				t.Fatal(err)
			}
			e.clk.Set(c.finish)
			e.sched.RunDue(c.finish)
			row, _ := e.store.row("PA001")
			want := c.finish.Truncate(time.Millisecond)
			if !row.FinishedAt.Equal(want) || row.BillDate != c.wantDate {
				t.Fatalf("finished_at = %v, bill_date = %s", row.FinishedAt, row.BillDate)
			}
			if row.FinishedAt.Location().String() != "Asia/Shanghai" {
				t.Fatalf("finished_at location = %v", row.FinishedAt.Location())
			}
		})
	}
}

// CH-01 规则 5：重启后扫描全部 PROCESSING 重建调度；已过期的立即完成，未到期的按原 due 完成。
func TestRecoverReschedulesProcessing(t *testing.T) {
	e := newEnv(t)
	overdue := e.store.put(dal.Txn{ChannelTxnNo: "MC1", TxnType: dal.TxnTypePay, ReqNo: "PA_OLD",
		Amount: 10000, Status: dal.StatusProcessing, Scenario: "NORMAL", CreatedAt: baseTime.Add(-time.Hour)})
	future := e.store.put(dal.Txn{ChannelTxnNo: "MC2", TxnType: dal.TxnTypePay, ReqNo: "PA_NEW",
		Amount: 10001, Status: dal.StatusProcessing, Scenario: "FAIL", CreatedAt: baseTime.Truncate(time.Millisecond)})
	e.store.put(dal.Txn{ChannelTxnNo: "MC3", TxnType: dal.TxnTypePay, ReqNo: "PA_NEVER",
		Amount: 10008, Status: dal.StatusProcessing, Scenario: "NEVER_FINISH", CreatedAt: baseTime.Add(-time.Hour)})
	for i, st := range []string{dal.StatusSuccess, dal.StatusFailed, dal.StatusClosed} {
		e.store.put(dal.Txn{ChannelTxnNo: fmt.Sprintf("MCX%d", i), TxnType: dal.TxnTypePay, ReqNo: fmt.Sprintf("PA_DONE%d", i),
			Amount: 10000, Status: st, Scenario: "NORMAL", CreatedAt: baseTime.Add(-time.Hour)})
	}

	n, err := e.svc.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("recovered %d, want 2", n)
	}
	if ran := e.sched.RunDue(e.clk.Now()); ran != 1 {
		t.Fatalf("overdue ran %d", ran)
	}
	row, _ := e.store.row(overdue.ReqNo)
	if row.Status != dal.StatusSuccess || !row.FinishedAt.Equal(baseTime.Truncate(time.Millisecond)) {
		t.Fatalf("overdue row = %+v", row)
	}
	row, _ = e.store.row(future.ReqNo)
	if row.Status != dal.StatusProcessing {
		t.Fatalf("future row finished early: %+v", row)
	}
	dues := e.sched.dues()
	if len(dues) != 1 || !dues[0].Equal(future.CreatedAt.Add(Delay(future.ReqNo, 200, 2000))) {
		t.Fatalf("dues = %v", dues)
	}
	e.clk.Set(dues[0])
	e.sched.RunDue(dues[0])
	row, _ = e.store.row(future.ReqNo)
	if row.Status != dal.StatusFailed || row.BillDate != "" {
		t.Fatalf("future row = %+v", row)
	}
	row, _ = e.store.row("PA_NEVER")
	if row.Status != dal.StatusProcessing {
		t.Fatalf("NEVER_FINISH row = %+v", row)
	}
	if len(e.hook.list()) != 2 {
		t.Fatalf("hook calls = %d", len(e.hook.list()))
	}
}

// 扫描按 id 分页，超过一页的 PROCESSING 也全部重建。
func TestRecoverPaginates(t *testing.T) {
	e := newEnv(t)
	const total = 2*recoverBatch + 3
	for i := 0; i < total; i++ {
		e.store.put(dal.Txn{ChannelTxnNo: fmt.Sprintf("MC%d", i), TxnType: dal.TxnTypePay,
			ReqNo: fmt.Sprintf("PA%d", i), Amount: 10000, Status: dal.StatusProcessing,
			Scenario: "NORMAL", CreatedAt: baseTime})
	}
	n, err := e.svc.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != total || e.sched.len() != total {
		t.Fatalf("recovered %d, scheduled %d, want %d", n, e.sched.len(), total)
	}
	if e.store.lists != 3 {
		t.Fatalf("list calls = %d, want 3", e.store.lists)
	}
}

func TestRecoverErrors(t *testing.T) {
	e := newEnv(t)
	e.store.listErr = errStore
	if _, err := e.svc.Recover(context.Background()); !errors.Is(err, errStore) {
		t.Fatalf("err = %v", err)
	}
	e = newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.svc.Recover(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

// 未设置钩子时完成照常进行。
func TestFinishWithoutHook(t *testing.T) {
	e := newEnv(t)
	e.svc = New(e.store, e.ids, e.clk, e.sched, Options{DelayMinMS: 200, DelayMaxMS: 2000})
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	e.clk.Advance(2 * time.Second)
	e.sched.RunDue(e.clk.Now())
	if row, _ := e.store.row("PA001"); row.Status != dal.StatusSuccess {
		t.Fatalf("row = %+v", row)
	}
}

// 审查 P3：Finish 已提交但返回错误（提交结果不确定）时，重试的 CAS 得 0 行。
// 按 id 读回，状态与 finished_at 和上次尝试一致就视为已生效，钩子只调用一次，带首次尝试的完成时间。
func TestFinishUncertainCommitCallsHookOnce(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	e.store.finishCommitErrs = []error{errStore}
	e.clk.Advance(2 * time.Second)
	firstTry := e.clk.Now().Truncate(time.Millisecond)
	e.sched.RunDue(e.clk.Now())
	if len(e.hook.list()) != 0 {
		t.Fatal("hook called before commit was confirmed")
	}
	if dues := e.sched.dues(); len(dues) != 1 || !dues[0].Equal(e.clk.Now().Add(finishRetryDelay)) {
		t.Fatalf("dues = %v", dues)
	}
	e.clk.Advance(finishRetryDelay)
	e.sched.RunDue(e.clk.Now())
	calls := e.hook.list()
	if len(calls) != 1 || calls[0].Status != dal.StatusSuccess || !calls[0].FinishedAt.Equal(firstTry) ||
		calls[0].BillDate != "2026-10-23" {
		t.Fatalf("hook calls = %+v", calls)
	}
	if e.store.getByIDs != 1 || e.sched.len() != 0 {
		t.Fatalf("getByIDs = %d, scheduled = %d", e.store.getByIDs, e.sched.len())
	}
	// 再次被调度（如重启扫描重叠）也不会重复调用：这次没有不确定的尝试，0 行直接跳过。
	e.svc.schedule(context.Background(), calls[0])
	e.sched.RunDue(e.clk.Now())
	if len(e.hook.list()) != 1 {
		t.Fatalf("hook calls = %d", len(e.hook.list()))
	}
}

// 不确定的尝试实际没有生效、之后被别处改走时，读回的行与尝试不一致，不调用钩子、不再重试。
func TestFinishUncertainCommitThenChangedElsewhere(t *testing.T) {
	cases := []struct {
		name   string
		change func(e *env)
	}{
		{"closed", func(e *env) { e.store.setStatus("PA001", dal.StatusClosed) }},
		{"finished with another time", func(e *env) {
			e.store.mu.Lock()
			defer e.store.mu.Unlock()
			r := e.store.rows[key(dal.TxnTypePay, "PA001")]
			r.Status = dal.StatusSuccess
			r.FinishedAt = baseTime.Add(time.Minute)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
				t.Fatal(err)
			}
			e.store.finishErrs = []error{errStore}
			e.clk.Advance(2 * time.Second)
			e.sched.RunDue(e.clk.Now())
			c.change(e)
			e.clk.Advance(finishRetryDelay)
			e.sched.RunDue(e.clk.Now())
			if len(e.hook.list()) != 0 {
				t.Fatalf("hook calls = %+v", e.hook.list())
			}
			if e.store.getByIDs != 1 || e.sched.len() != 0 {
				t.Fatalf("getByIDs = %d, scheduled = %d", e.store.getByIDs, e.sched.len())
			}
		})
	}
}

// 按 id 读回失败时不猜结果，继续按重试间隔重新调度，读到后再判断。
func TestFinishGetByIDErrorReschedules(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	e.store.finishCommitErrs = []error{errStore}
	e.store.getByIDErrs = []error{errStore}
	e.clk.Advance(2 * time.Second)
	e.sched.RunDue(e.clk.Now())
	e.clk.Advance(finishRetryDelay)
	e.sched.RunDue(e.clk.Now())
	if len(e.hook.list()) != 0 {
		t.Fatal("hook called without confirmation")
	}
	if dues := e.sched.dues(); len(dues) != 1 || !dues[0].Equal(e.clk.Now().Add(finishRetryDelay)) {
		t.Fatalf("dues = %v", dues)
	}
	e.clk.Advance(finishRetryDelay)
	e.sched.RunDue(e.clk.Now())
	if len(e.hook.list()) != 1 || e.store.getByIDs != 2 || e.sched.len() != 0 {
		t.Fatalf("hook calls = %d, getByIDs = %d, scheduled = %d", len(e.hook.list()), e.store.getByIDs, e.sched.len())
	}
}

// 审查 P3：钩子拿到的 ctx 不随 finish 返回而取消，也没有截止时间（钩子自己设超时）。
func TestFinishHookContextOutlivesFinish(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := e.svc.Pay(ctx, payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	cancel()
	e.clk.Advance(2 * time.Second)
	e.sched.RunDue(e.clk.Now())
	ctxs := e.hook.contexts()
	if len(ctxs) != 1 {
		t.Fatalf("hook calls = %d", len(ctxs))
	}
	if err := ctxs[0].Err(); err != nil {
		t.Fatalf("hook ctx err = %v after finish returned", err)
	}
	if _, ok := ctxs[0].Deadline(); ok {
		t.Fatal("hook ctx has a deadline")
	}
}

// 审查 P3：完成日志用约定 §7.5 的 from_status、to_status。
func TestFinishLogsStatusTransition(t *testing.T) {
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	var buf bytes.Buffer
	logx.SetDefault("channel-mock", &buf)

	e := newEnv(t)
	if _, err := e.svc.Pay(context.Background(), payReq("PA001", 10001)); err != nil {
		t.Fatal(err)
	}
	e.clk.Advance(2 * time.Second)
	e.sched.RunDue(e.clk.Now())
	out := buf.String()
	for _, want := range []string{`"msg":"mock txn finished"`, `"from_status":"PROCESSING"`, `"to_status":"FAILED"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, `"status":`) {
		t.Fatalf("log still uses status field:\n%s", out)
	}
}
