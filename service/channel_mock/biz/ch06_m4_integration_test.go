//go:build integration

package biz

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

// CH-06 验收：SLOW 自动完成后查单立即 SUCCESS，bill_date 与 NORMAL 相同；finished_at+30s 之前 HTTP 0 次，到点发出第 1 次。
func TestScenarioSlowNotifyAfter30sMySQL(t *testing.T) {
	e := openMock(t)
	srv, _, hits := scenarioServer(t, nil, http.StatusOK, []byte("SUCCESS"))
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	ids := newIDs(t, clk)
	opts := Options{DelayMinMS: 200, DelayMaxMS: 200}
	slowSvc := New(e.store, ids, clk, sched, opts)
	normalSvc := New(e.store, ids, clk, sched, opts)
	slowSvc.SetOnFinished(NewNotifier(e.store, newIDs(t, clk), clk, sched, []byte(notifyKey)).Handle)

	slow := acceptScenario(t, e, slowSvc, "PA_SLOW", 10004, srv.URL)
	normal := acceptScenario(t, e, normalSvc, "PA_NORM", 10000, srv.URL)
	if slow.Scenario != scenario.Slow.String() || normal.Scenario != scenario.Normal.String() {
		t.Fatalf("scenario slow=%s normal=%s", slow.Scenario, normal.Scenario)
	}
	if !slow.CreatedAt.Equal(normal.CreatedAt) {
		t.Fatalf("created_at slow=%v normal=%v", slow.CreatedAt, normal.CreatedAt)
	}
	due := slow.CreatedAt.Add(Delay(slow.ReqNo, 200, 200))
	if !due.Equal(normal.CreatedAt.Add(Delay(normal.ReqNo, 200, 200))) {
		t.Fatal("finish dues differ")
	}
	clk.Set(due.Add(-time.Millisecond))
	if n := sched.RunDue(clk.Now()); n != 0 {
		t.Fatalf("early finish %d", n)
	}
	clk.Set(due)
	if n := sched.RunDue(clk.Now()); n != 2 {
		t.Fatalf("finish ran %d", n)
	}
	if hits.Load() != 0 {
		t.Fatal("http inside finish")
	}
	slowRow := e.row(t, "PA_SLOW")
	normalRow := e.row(t, "PA_NORM")
	wantDate := slowRow.FinishedAt.In(shanghai).Format(billDateLayout)
	if slowRow.Status != dal.StatusSuccess || normalRow.Status != dal.StatusSuccess ||
		slowRow.BillDate == "" || slowRow.BillDate != normalRow.BillDate || slowRow.BillDate != wantDate ||
		!slowRow.FinishedAt.Equal(normalRow.FinishedAt) || slowRow.NotifySentCount != 0 {
		t.Fatalf("slow=%+v normal=%+v want date %s", slowRow, normalRow, wantDate)
	}
	assertQuerySuccess(t, slowSvc, e, "PA_SLOW")
	assertQuerySuccess(t, normalSvc, e, "PA_NORM")

	notifyDue := slowRow.FinishedAt.Add(slowFirstDelay)
	wantDue(t, sched, notifyDue)
	clk.Set(notifyDue.Add(-time.Millisecond))
	if n := sched.RunDue(clk.Now()); n != 0 || hits.Load() != 0 || e.row(t, "PA_SLOW").NotifySentCount != 0 {
		t.Fatalf("early n=%d hits=%d count=%d", n, hits.Load(), e.row(t, "PA_SLOW").NotifySentCount)
	}
	clk.Set(notifyDue)
	if n := sched.RunDue(clk.Now()); n != 1 {
		t.Fatalf("notify ran %d", n)
	}
	if hits.Load() != 1 || e.row(t, "PA_SLOW").NotifySentCount != 1 || sched.len() != 0 {
		t.Fatalf("hits=%d count=%d scheduled=%d", hits.Load(), e.row(t, "PA_SLOW").NotifySentCount, sched.len())
	}
}

func assertQuerySuccess(t *testing.T, svc *Service, e dbEnv, reqNo string) {
	t.Helper()
	q, err := svc.Query(e.ctx, QueryRequest{TxnType: dal.TxnTypePay, ReqNo: reqNo})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !q.Found || q.Status != dal.StatusSuccess {
		t.Fatalf("query %s = %+v", reqNo, q)
	}
}

// CH-06 验收：SLOW 首次 500，第 2 次在 5 秒后 SUCCESS，之后不再发，notify_sent_count=2。
func TestScenarioSlowRetryFiveSecondsMySQL(t *testing.T) {
	e := openMock(t)
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("read: %v", err)
		}
		i := int(n.Add(1))
		if i == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			if _, err := w.Write([]byte("NO")); err != nil {
				t.Errorf("write: %v", err)
			}
			return
		}
		if _, err := w.Write([]byte("SUCCESS")); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := New(e.store, newIDs(t, clk), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 200})
	svc.SetOnFinished(NewNotifier(e.store, newIDs(t, clk), clk, sched, []byte(notifyKey)).Handle)
	row := acceptScenario(t, e, svc, "PA_SLOW_RETRY", 10004, srv.URL)
	due := row.CreatedAt.Add(Delay(row.ReqNo, 200, 200))
	clk.Set(due)
	if ran := sched.RunDue(clk.Now()); ran != 1 || n.Load() != 0 {
		t.Fatalf("finish ran=%d hits=%d", ran, n.Load())
	}
	finished := e.row(t, "PA_SLOW_RETRY")
	if finished.Status != dal.StatusSuccess || finished.NotifySentCount != 0 {
		t.Fatalf("finished = %+v", finished)
	}
	notifyDue := finished.FinishedAt.Add(slowFirstDelay)
	clk.Set(notifyDue)
	if ran := sched.RunDue(clk.Now()); ran != 1 || n.Load() != 1 || e.row(t, "PA_SLOW_RETRY").NotifySentCount != 1 {
		t.Fatalf("first ran=%d hits=%d count=%d", ran, n.Load(), e.row(t, "PA_SLOW_RETRY").NotifySentCount)
	}
	wantDue(t, sched, clk.Now().Add(5*time.Second))
	clk.Advance(5 * time.Second)
	if ran := sched.RunDue(clk.Now()); ran != 1 || n.Load() != 2 || e.row(t, "PA_SLOW_RETRY").NotifySentCount != 2 || sched.len() != 0 {
		t.Fatalf("second ran=%d hits=%d count=%d scheduled=%d", ran, n.Load(), e.row(t, "PA_SLOW_RETRY").NotifySentCount, sched.len())
	}
	clk.Advance(time.Minute)
	if ran := sched.RunDue(clk.Now()); ran != 0 || n.Load() != 2 || e.row(t, "PA_SLOW_RETRY").NotifySentCount != 2 {
		t.Fatalf("later ran=%d hits=%d count=%d", ran, n.Load(), e.row(t, "PA_SLOW_RETRY").NotifySentCount)
	}
}

// CH-06 验收：ACCEPT_TIMEOUT 在注入时钟 +5s 之前不返回；行已经在；+5s 后 RunDue 返回 OK；完成延迟长于 5 秒，返回时仍是 PROCESSING；再完成则 SUCCESS。阻塞期间再次 Pay 立刻返回同一 channel_txn_no。
func TestPayAcceptTimeoutHoldsFiveSecondsMySQL(t *testing.T) {
	e := openMock(t)
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := New(e.store, newIDs(t, clk), clk, sched, Options{DelayMinMS: 60000, DelayMaxMS: 60000})
	ch := make(chan payOut, 1)
	go func() {
		res, err := svc.Pay(e.ctx, payReq("PA_AT", 10005))
		ch <- payOut{res, err}
	}()
	waitCond(t, func() bool {
		_, ok, err := e.store.Get(e.ctx, dal.TxnTypePay, "PA_AT")
		return err == nil && ok && sched.len() == 2
	})
	select {
	case got := <-ch:
		t.Fatalf("returned before +5s: %+v %v", got.res, got.err)
	default:
	}
	row := e.row(t, "PA_AT")
	if row.Scenario != scenario.AcceptTimeout.String() || row.Status != dal.StatusProcessing {
		t.Fatalf("row = %+v", row)
	}
	second, err := svc.Pay(e.ctx, payReq("PA_AT", 10005))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if second.Result != ResultOK || second.Status != dal.StatusProcessing || second.ChannelTxnNo != row.ChannelTxnNo {
		t.Fatalf("replay = %+v, want %s", second, row.ChannelTxnNo)
	}
	select {
	case got := <-ch:
		t.Fatalf("replay waited or unblocked the first call: %+v %v", got.res, got.err)
	default:
	}
	if sched.len() != 2 {
		t.Fatalf("scheduled = %d", sched.len())
	}
	clk.Set(row.CreatedAt.Add(acceptHold).Add(-time.Millisecond))
	if n := sched.RunDue(clk.Now()); n != 0 {
		t.Fatalf("early ran %d", n)
	}
	select {
	case got := <-ch:
		t.Fatalf("returned before due: %+v %v", got.res, got.err)
	default:
	}
	clk.Set(row.CreatedAt.Add(acceptHold))
	if n := sched.RunDue(clk.Now()); n != 1 {
		t.Fatalf("hold ran %d", n)
	}
	select {
	case got := <-ch:
		if got.err != nil || got.res.Result != ResultOK || got.res.Status != dal.StatusProcessing || got.res.ChannelTxnNo != row.ChannelTxnNo {
			t.Fatalf("result = %+v %v", got.res, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not return after RunDue")
	}
	if got := e.row(t, "PA_AT"); got.Status != dal.StatusProcessing {
		t.Fatalf("status = %s", got.Status)
	}
	finishDue := row.CreatedAt.Add(Delay(row.ReqNo, 60000, 60000))
	clk.Set(finishDue)
	if n := sched.RunDue(clk.Now()); n != 1 {
		t.Fatalf("finish ran %d", n)
	}
	if got := e.row(t, "PA_AT"); got.Status != dal.StatusSuccess {
		t.Fatalf("status = %s", got.Status)
	}
}
