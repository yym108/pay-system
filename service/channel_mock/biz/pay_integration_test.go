//go:build integration

package biz

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := testkit.Shutdown(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %s\n", testkit.Redact(err))
	}
	os.Exit(code)
}

type dbEnv struct {
	ctx   context.Context
	raw   *sqlx.DB
	store *dal.DB
}

func openMock(t *testing.T) dbEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	schema, err := my.OpenSchema(ctx, "channel_mock")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer closeCancel()
		if err := schema.Close(closeCtx); err != nil {
			t.Errorf("close schema: %s", testkit.Redact(err))
		}
	})
	return dbEnv{ctx: ctx, raw: schema.DB, store: dal.New(schema.DB)}
}

func amountOf(v int64) money.Amount { return money.Amount(v) }

func newIDs(t *testing.T, clk idgen.Clock) *idgen.Generator {
	t.Helper()
	g, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func (e dbEnv) row(t *testing.T, reqNo string) dal.Txn {
	t.Helper()
	r, ok, err := e.store.Get(e.ctx, dal.TxnTypePay, reqNo)
	if err != nil || !ok {
		t.Fatalf("get %s: ok = %v, err = %v", reqNo, ok, err)
	}
	return r
}

func (e dbEnv) count(t *testing.T, reqNo string) int {
	t.Helper()
	var n int
	if err := e.raw.QueryRowContext(e.ctx, `SELECT COUNT(*) FROM mock_txn WHERE req_no = ?`, reqNo).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e dbEnv) insertRaw(t *testing.T, reqNo, status, scenario string, amount int64, created time.Time) {
	t.Helper()
	_, err := e.raw.ExecContext(e.ctx, `
INSERT INTO mock_txn (channel_txn_no, txn_type, req_no, amount, status, scenario, notify_url, created_at)
VALUES (?, 'PAY', ?, ?, ?, ?, 'http://gateway:8080/callback/mockpay', ?)`,
		"MC"+reqNo, reqNo, amount, status, scenario, created)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

// waitFinal 轮询直到该笔离开 PROCESSING，超过 limit 失败。
func (e dbEnv) waitFinal(t *testing.T, reqNo string, limit time.Duration) dal.Txn {
	t.Helper()
	timeout := time.After(limit)
	for {
		r := e.row(t, reqNo)
		if r.Status != dal.StatusProcessing {
			return r
		}
		select {
		case <-timeout:
			t.Fatalf("%s still PROCESSING after %v", reqNo, limit)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// CH-01 验收 1（真实库）：50 个并发同一 req_no 只落一行，全部拿到同一 channel_txn_no。
func TestPayConcurrentSameReqNoMySQL(t *testing.T) {
	e := openMock(t)
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := New(e.store, newIDs(t, clock.Real{}), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 2000})
	const n = 50
	results := make([]PayResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = svc.Pay(e.ctx, payReq("PA_CONC", 10000))
		}(i)
	}
	close(start)
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %s", i, testkit.Redact(errs[i]))
		}
		if results[i].Result != ResultOK || results[i].ChannelTxnNo != results[0].ChannelTxnNo {
			t.Fatalf("goroutine %d: %+v vs %+v", i, results[i], results[0])
		}
	}
	if c := e.count(t, "PA_CONC"); c != 1 {
		t.Fatalf("rows = %d", c)
	}
	if sched.len() != 1 {
		t.Fatalf("scheduled = %d", sched.len())
	}
	if got := e.row(t, "PA_CONC"); got.ChannelTxnNo != results[0].ChannelTxnNo {
		t.Fatalf("row channel_txn_no = %s", got.ChannelTxnNo)
	}
}

// CH-01 规则 2（真实库）：金额冲突与 CLOSED 都不改原行。
func TestPayRejectsMySQL(t *testing.T) {
	e := openMock(t)
	clk := &clock.Fake{}
	clk.Set(baseTime)
	svc := New(e.store, newIDs(t, clock.Real{}), clk, &manualSched{}, Options{DelayMinMS: 200, DelayMaxMS: 2000})
	first, err := svc.Pay(e.ctx, payReq("PA001", 10000))
	if err != nil {
		t.Fatal(err)
	}
	res, err := svc.Pay(e.ctx, payReq("PA001", 10001))
	if err != nil {
		t.Fatal(err)
	}
	if res != (PayResult{Result: ResultRejected, Reason: ReasonParamConflict, Status: dal.StatusProcessing}) {
		t.Fatalf("conflict = %+v", res)
	}
	if r := e.row(t, "PA001"); r.Amount != 10000 || r.ChannelTxnNo != first.ChannelTxnNo {
		t.Fatalf("row changed: %+v", r)
	}

	e.insertRaw(t, "PA_CLOSED", dal.StatusClosed, "NORMAL", 10000, baseTime)
	for _, amount := range []int64{10000, 10001} {
		res, err := svc.Pay(e.ctx, payReq("PA_CLOSED", amountOf(amount)))
		if err != nil {
			t.Fatal(err)
		}
		if res != (PayResult{Result: ResultRejected, Reason: ReasonOrderClosed, Status: dal.StatusClosed}) {
			t.Fatalf("closed amount %d = %+v", amount, res)
		}
	}
	if r := e.row(t, "PA_CLOSED"); r.Status != dal.StatusClosed || r.Amount != 10000 || !r.FinishedAt.IsZero() {
		t.Fatalf("closed row changed: %+v", r)
	}
}

// CH-01 验收 2 前半（真实库、真实定时器）：NORMAL 2 秒内变为 SUCCESS，写 finished_at 与 bill_date。
func TestNormalCompletesWithinTwoSecondsRealTimer(t *testing.T) {
	e := openMock(t)
	sched := NewTimerScheduler(clock.Real{})
	t.Cleanup(sched.Stop)
	svc := New(e.store, newIDs(t, clock.Real{}), clock.Real{}, sched, Options{DelayMinMS: 200, DelayMaxMS: 2000})
	accepted := clock.Real{}.Now()
	res, err := svc.Pay(e.ctx, payReq("PA_REAL", 10000))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != dal.StatusProcessing {
		t.Fatalf("accept status = %s", res.Status)
	}
	r := e.waitFinal(t, "PA_REAL", 2*time.Second)
	elapsed := clock.Real{}.Now().Sub(accepted)
	if r.Status != dal.StatusSuccess || r.FinishedAt.IsZero() {
		t.Fatalf("row = %+v", r)
	}
	if d := r.FinishedAt.Sub(r.CreatedAt); d < Delay("PA_REAL", 200, 2000) || d >= 2*time.Second {
		t.Fatalf("finished_at - created_at = %v", d)
	}
	if r.BillDate != r.FinishedAt.In(shanghaiTZ).Format("2006-01-02") {
		t.Fatalf("bill_date = %s, finished_at = %v", r.BillDate, r.FinishedAt)
	}
	t.Logf("NORMAL finished %v after accept (delay %v)", elapsed, Delay("PA_REAL", 200, 2000))
}

// CH-01 验收 2 后半（真实库、真实定时器）：实例 A 受理后停止（未完成），实例 B 启动扫描 PROCESSING 并完成；
// 早已过期的行立即完成，NEVER_FINISH 保持 PROCESSING。
func TestRestartRecoversRealTimer(t *testing.T) {
	e := openMock(t)
	ids := newIDs(t, clock.Real{})
	opts := Options{DelayMinMS: 200, DelayMaxMS: 2000}

	schedA := NewTimerScheduler(clock.Real{})
	svcA := New(e.store, ids, clock.Real{}, schedA, opts)
	reqs := map[string]int64{"PA_R_NORMAL": 10000, "PA_R_FAIL": 10001, "PA_R_MISSING": 10006, "PA_R_NEVER": 10008}
	for reqNo, amount := range reqs {
		if _, err := svcA.Pay(e.ctx, payReq(reqNo, amountOf(amount))); err != nil {
			t.Fatal(err)
		}
	}
	schedA.Stop() // 模拟进程退出：未到期的定时器全部丢失。
	for reqNo := range reqs {
		if r := e.row(t, reqNo); r.Status != dal.StatusProcessing {
			t.Fatalf("%s finished before restart: %+v", reqNo, r)
		}
	}
	e.insertRaw(t, "PA_R_OVERDUE", dal.StatusProcessing, "NORMAL", 10000, clock.Real{}.Now().Add(-time.Hour))

	hook := &hookRecorder{}
	schedB := NewTimerScheduler(clock.Real{})
	t.Cleanup(schedB.Stop)
	opts.OnFinished = hook.fn
	svcB := New(e.store, ids, clock.Real{}, schedB, opts)
	restarted := clock.Real{}.Now()
	n, err := svcB.Recover(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("recovered %d, want 4 (NEVER_FINISH not scheduled)", n)
	}

	over := e.waitFinal(t, "PA_R_OVERDUE", time.Second)
	if over.Status != dal.StatusSuccess || over.FinishedAt.Sub(restarted) > 500*time.Millisecond {
		t.Fatalf("overdue row = %+v (restart at %v)", over, restarted)
	}
	want := map[string]struct {
		status string
		billed bool
	}{
		"PA_R_NORMAL":  {dal.StatusSuccess, true},
		"PA_R_FAIL":    {dal.StatusFailed, false},
		"PA_R_MISSING": {dal.StatusSuccess, false},
	}
	for reqNo, w := range want {
		r := e.waitFinal(t, reqNo, 2*time.Second)
		if r.Status != w.status || (r.BillDate != "") != w.billed {
			t.Fatalf("%s = %+v", reqNo, r)
		}
		if d := r.FinishedAt.Sub(r.CreatedAt); d >= 2*time.Second+500*time.Millisecond {
			t.Fatalf("%s finished %v after created", reqNo, d)
		}
	}
	if r := e.row(t, "PA_R_NEVER"); r.Status != dal.StatusProcessing || !r.FinishedAt.IsZero() {
		t.Fatalf("NEVER_FINISH row = %+v", r)
	}
	if len(hook.list()) != 4 {
		t.Fatalf("hook calls = %d, want 4", len(hook.list()))
	}
}

// CH-01 规则 5（真实库、手动调度）：重启扫描按 created_at + delay 重建 due，过期的立即到期。
func TestRecoverRebuildsDueMySQL(t *testing.T) {
	e := openMock(t)
	created := baseTime.Truncate(time.Millisecond)
	e.insertRaw(t, "PA_OLD", dal.StatusProcessing, "NORMAL", 10000, created.Add(-time.Hour))
	e.insertRaw(t, "PA_NEW", dal.StatusProcessing, "BILL_MISSING", 10006, created)
	e.insertRaw(t, "PA_DONE", dal.StatusSuccess, "NORMAL", 10000, created.Add(-time.Hour))
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := New(e.store, newIDs(t, clock.Real{}), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 2000})
	n, err := svc.Recover(e.ctx)
	if err != nil || n != 2 {
		t.Fatalf("recover = %d, %v", n, err)
	}
	if ran := sched.RunDue(clk.Now()); ran != 1 {
		t.Fatalf("ran %d at restart", ran)
	}
	if r := e.row(t, "PA_OLD"); r.Status != dal.StatusSuccess || !r.FinishedAt.Equal(created) || r.BillDate != "2026-10-23" {
		t.Fatalf("PA_OLD = %+v", r)
	}
	if r := e.row(t, "PA_NEW"); r.Status != dal.StatusProcessing {
		t.Fatalf("PA_NEW finished early: %+v", r)
	}
	due := created.Add(Delay("PA_NEW", 200, 2000))
	clk.Set(due)
	sched.RunDue(due)
	if r := e.row(t, "PA_NEW"); r.Status != dal.StatusSuccess || !r.FinishedAt.Equal(due) || r.BillDate != "" {
		t.Fatalf("PA_NEW = %+v", r)
	}
}

// CAS（真实库）：自动完成与外部关单（CH-03 的 SQL 形态）并发，每笔只有一方生效；
// 关单赢时不写 finished_at、不调用钩子。
func TestFinishRacesWithCloseMySQL(t *testing.T) {
	e := openMock(t)
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	hook := &hookRecorder{}
	svc := New(e.store, newIDs(t, clock.Real{}), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 2000, OnFinished: hook.fn})
	const n = 30
	for i := 0; i < n; i++ {
		if _, err := svc.Pay(e.ctx, payReq(fmt.Sprintf("PA_RACE%02d", i), 10000)); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(2 * time.Second)
	sched.mu.Lock()
	tasks := append([]schedTask(nil), sched.tasks...)
	sched.tasks = nil
	sched.mu.Unlock()
	if len(tasks) != n {
		t.Fatalf("tasks = %d", len(tasks))
	}

	closed := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			tasks[i].task()
		}()
		go func(i int) {
			defer wg.Done()
			r := e.row(t, fmt.Sprintf("PA_RACE%02d", i))
			res, err := e.raw.ExecContext(e.ctx,
				`UPDATE mock_txn SET status = 'CLOSED' WHERE id = ? AND status = 'PROCESSING'`, r.ID)
			if err != nil {
				t.Error(err)
				return
			}
			closed[i], _ = res.RowsAffected()
		}(i)
	}
	wg.Wait()

	hooked := map[uint64]int{}
	for _, c := range hook.list() {
		hooked[c.ID]++
	}
	var closeWins, finishWins int
	for i := 0; i < n; i++ {
		r := e.row(t, fmt.Sprintf("PA_RACE%02d", i))
		switch r.Status {
		case dal.StatusClosed:
			closeWins++
			var fin sql.NullTime
			if err := e.raw.QueryRowContext(e.ctx, `SELECT finished_at FROM mock_txn WHERE id = ?`, r.ID).Scan(&fin); err != nil {
				t.Fatal(err)
			}
			if closed[i] != 1 || fin.Valid || hooked[r.ID] != 0 {
				t.Fatalf("%d: close won but closed=%d finished_at=%v hook=%d", i, closed[i], fin, hooked[r.ID])
			}
		case dal.StatusSuccess:
			finishWins++
			if closed[i] != 0 || hooked[r.ID] != 1 {
				t.Fatalf("%d: finish won but closed=%d hook=%d", i, closed[i], hooked[r.ID])
			}
		default:
			t.Fatalf("%d: status = %s", i, r.Status)
		}
	}
	t.Logf("close won %d, finish won %d", closeWins, finishWins)
}
