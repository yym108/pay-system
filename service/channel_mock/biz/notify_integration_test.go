//go:build integration

package biz

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

// CH-05 验收（MySQL）：平台前两次 500，第三次 body 为 SUCCESS，停止，notify_sent_count = 3。
func TestNotifyRetriesUntilThirdSuccessMySQL(t *testing.T) {
	e := openMock(t)
	var n atomic.Int32
	var bodies notifyCap
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read: %v", err)
			http.Error(w, "read", http.StatusInternalServerError)
			return
		}
		bodies.add(notifyHit{
			body: body,
			ts:   r.Header.Get(sign.HeaderMockTimestamp),
			sig:  r.Header.Get(sign.HeaderMockSignature),
		})
		i := int(n.Add(1))
		if i < 3 {
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

	finished := time.Date(2026, 10, 26, 11, 22, 33, 123000000, shanghaiTZ)
	row := e.insertNotifyRow(t, "PA_CH05", srv.URL, finished)
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	key := []byte(notifyKey)
	notifier := NewNotifier(e.store, newIDs(t, clk), clk, sched, key)
	notifier.Handle(e.ctx, row)
	if n.Load() != 0 {
		t.Fatal("Handle sent HTTP")
	}
	waits := []time.Duration{0, 5 * time.Second, 15 * time.Second}
	for _, wait := range waits {
		if wait > 0 {
			clk.Advance(wait)
		}
		if ran := sched.RunDue(clk.Now()); ran != 1 {
			t.Fatalf("ran %d", ran)
		}
	}
	if sched.len() != 0 || n.Load() != 3 {
		t.Fatalf("scheduled=%d hits=%d", sched.len(), n.Load())
	}
	clk.Advance(time.Minute)
	if ran := sched.RunDue(clk.Now()); ran != 0 {
		t.Fatalf("extra %d", ran)
	}
	got := e.row(t, "PA_CH05")
	if got.NotifySentCount != 3 || got.Status != dal.StatusSuccess || got.Amount != 10000 {
		t.Fatalf("row = %+v", got)
	}
	verifyHits(t, key, bodies.list())
	body := string(bodies.list()[2].body)
	if !strings.Contains(body, `"status":"SUCCESS"`) ||
		!strings.Contains(body, `"finished_at":"2026-10-26T11:22:33.123+08:00"`) ||
		!strings.Contains(body, `"amount":10000`) {
		t.Fatalf("body = %s", body)
	}
}

// CH-05：通知器不在调用方的事务里发 HTTP。计数在自己的短事务提交后，别的连接才看得见，然后才 POST。
func TestNotifyHTTPNotInsideCallerTxMySQL(t *testing.T) {
	e := openMock(t)
	var hits atomic.Int32
	var seen atomic.Int64
	seen.Store(-1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var c int64
		err := e.raw.QueryRowContext(r.Context(),
			`SELECT notify_sent_count FROM mock_txn WHERE req_no = ?`, "PA_TX").Scan(&c)
		if err != nil {
			t.Errorf("probe: %s", testkit.Redact(err))
		}
		seen.Store(c)
		hits.Add(1)
		if _, err := w.Write([]byte("SUCCESS")); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	finished := time.Date(2026, 10, 26, 8, 0, 0, 0, shanghaiTZ)
	row := e.insertNotifyRow(t, "PA_TX", srv.URL, finished)
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	notifier := NewNotifier(e.store, newIDs(t, clk), clk, sched, []byte(notifyKey))
	err := dbx.WithTx(e.ctx, e.raw, func(tx *sqlx.Tx) error {
		notifier.Handle(e.ctx, row)
		if hits.Load() != 0 {
			return errors.New("http inside caller tx")
		}
		var c int
		if err := tx.QueryRowContext(e.ctx, `SELECT notify_sent_count FROM mock_txn WHERE id = ?`, row.ID).Scan(&c); err != nil {
			return err
		}
		if c != 0 {
			return fmt.Errorf("count inside caller tx = %d", c)
		}
		return nil
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if hits.Load() != 0 {
		t.Fatal("http before RunDue")
	}
	if ran := sched.RunDue(clk.Now()); ran != 1 {
		t.Fatalf("ran %d", ran)
	}
	if hits.Load() != 1 || seen.Load() != 1 || sched.len() != 0 {
		t.Fatalf("hits=%d seen=%d scheduled=%d", hits.Load(), seen.Load(), sched.len())
	}
	if got := e.row(t, "PA_TX"); got.NotifySentCount != 1 || got.Status != dal.StatusSuccess {
		t.Fatalf("row = %+v", got)
	}
}

func (e dbEnv) insertNotifyRow(t *testing.T, reqNo, notifyURL string, finished time.Time) dal.Txn {
	t.Helper()
	_, err := e.raw.ExecContext(e.ctx, `
INSERT INTO mock_txn (channel_txn_no, txn_type, req_no, amount, status, scenario, notify_url, notify_sent_count, finished_at, created_at)
VALUES (?, 'PAY', ?, 10000, 'SUCCESS', 'NORMAL', ?, 0, ?, ?)`,
		"MC"+reqNo, reqNo, notifyURL, finished, finished.Add(-time.Second))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return e.row(t, reqNo)
}

func newScenarioPair(t *testing.T, e dbEnv, clk *clock.Fake, sched *manualSched) *Service {
	t.Helper()
	svc := New(e.store, newIDs(t, clk), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 2000})
	n := NewNotifier(e.store, newIDs(t, clk), clk, sched, []byte(notifyKey))
	svc.SetOnFinished(n.Handle)
	return svc
}

func acceptScenario(t *testing.T, e dbEnv, svc *Service, reqNo string, amount int64, notifyURL string) dal.Txn {
	t.Helper()
	res, err := svc.Pay(e.ctx, PayRequest{
		ReqNo:     reqNo,
		Amount:    amountOf(amount),
		Subject:   "demo",
		NotifyURL: notifyURL,
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if res.Result != ResultOK || res.Status != dal.StatusProcessing {
		t.Fatalf("accept = %+v", res)
	}
	return e.row(t, reqNo)
}

// runFinish 把受理时排上的自动完成跑到注入时钟的到期点。到期前不跑。
func runFinish(t *testing.T, clk *clock.Fake, sched *manualSched, row dal.Txn) {
	t.Helper()
	due := row.CreatedAt.Add(Delay(row.ReqNo, 200, 2000))
	if n := sched.RunDue(due.Add(-time.Millisecond)); n != 0 {
		t.Fatalf("early finish %d", n)
	}
	clk.Set(due)
	if n := sched.RunDue(clk.Now()); n != 1 {
		t.Fatalf("finish ran %d", n)
	}
}

func scenarioServer(t *testing.T, gate *flightGate, status int, body []byte) (*httptest.Server, *notifyCap, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	var seq atomic.Int32
	cap := &notifyCap{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read: %v", err)
			http.Error(w, "read", http.StatusInternalServerError)
			return
		}
		n := int(seq.Add(1))
		cap.add(notifyHit{
			body: raw,
			ts:   r.Header.Get(sign.HeaderMockTimestamp),
			sig:  r.Header.Get(sign.HeaderMockSignature),
		})
		hits.Add(1)
		if gate != nil && n <= 5 {
			gate.arrive(t)
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
		}
		if _, err := w.Write(body); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, cap, &hits
}

// CH-06 M3 FAIL：受理 10001 分，到期后 FAILED、bill_date 为空；平台 SUCCESS 时只回调 1 次，body status 为 FAILED。
func TestScenarioFailNotifyOnceMySQL(t *testing.T) {
	e := openMock(t)
	srv, cap, hits := scenarioServer(t, nil, http.StatusOK, []byte("SUCCESS"))
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := newScenarioPair(t, e, clk, sched)
	row := acceptScenario(t, e, svc, "PA_FAIL", 10001, srv.URL)
	if row.Scenario != scenario.Fail.String() || sched.len() != 1 {
		t.Fatalf("scenario=%s scheduled=%d", row.Scenario, sched.len())
	}
	runFinish(t, clk, sched, row)
	if hits.Load() != 0 {
		t.Fatal("http inside finish")
	}
	got := e.row(t, "PA_FAIL")
	if got.Status != dal.StatusFailed || got.FinishedAt.IsZero() || got.BillDate != "" || got.NotifySentCount != 0 {
		t.Fatalf("after finish = %+v", got)
	}
	if ran := sched.RunDue(clk.Now()); ran != 1 {
		t.Fatalf("notify ran %d", ran)
	}
	if hits.Load() != 1 || gotNotifyCount(t, e, "PA_FAIL") != 1 || sched.len() != 0 {
		t.Fatalf("hits=%d count=%d scheduled=%d", hits.Load(), gotNotifyCount(t, e, "PA_FAIL"), sched.len())
	}
	body := string(cap.list()[0].body)
	if !strings.Contains(body, `"status":"FAILED"`) || !strings.Contains(body, `"amount":10001`) {
		t.Fatalf("body = %s", body)
	}
	verifyHits(t, []byte(notifyKey), cap.list())
	clk.Advance(time.Minute)
	if ran := sched.RunDue(clk.Now()); ran != 0 || hits.Load() != 1 {
		t.Fatalf("second ran=%d hits=%d", ran, hits.Load())
	}
	final := e.row(t, "PA_FAIL")
	if final.Status != dal.StatusFailed || final.BillDate != "" || final.NotifySentCount != 1 {
		t.Fatalf("final = %+v", final)
	}
}

// CH-06 M3 DROP_NOTIFY：受理 10002 分，到期 SUCCESS，bill_date 为 finished_at 的东八区日期，HTTP 0 次。
func TestScenarioDropNotifyNoHTTPMySQL(t *testing.T) {
	e := openMock(t)
	srv, _, hits := scenarioServer(t, nil, http.StatusOK, []byte("SUCCESS"))
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := newScenarioPair(t, e, clk, sched)
	row := acceptScenario(t, e, svc, "PA_DROP", 10002, srv.URL)
	if row.Scenario != scenario.DropNotify.String() {
		t.Fatalf("scenario = %s", row.Scenario)
	}
	runFinish(t, clk, sched, row)
	got := e.row(t, "PA_DROP")
	wantDate := got.FinishedAt.In(shanghai).Format(billDateLayout)
	if got.Status != dal.StatusSuccess || got.BillDate != wantDate || got.BillDate == "" || got.NotifySentCount != 0 {
		t.Fatalf("row = %+v, want date %s", got, wantDate)
	}
	if hits.Load() != 0 || sched.len() != 0 {
		t.Fatalf("hits=%d scheduled=%d", hits.Load(), sched.len())
	}
	clk.Advance(time.Minute)
	if ran := sched.RunDue(clk.Now()); ran != 0 || hits.Load() != 0 {
		t.Fatalf("late ran=%d hits=%d", ran, hits.Load())
	}
	if again := e.row(t, "PA_DROP"); again.NotifySentCount != 0 || again.Status != dal.StatusSuccess {
		t.Fatalf("again = %+v", again)
	}
}

// CH-06 M3 DUP_NOTIFY：受理 10003 分。第一波 5 次同时在途；平台 SUCCESS 仍发满 10 次。不改状态和 bill_date。
func TestScenarioDupNotifyTenMySQL(t *testing.T) {
	e := openMock(t)
	gate := newFlightGate(5)
	srv, cap, hits := scenarioServer(t, gate, http.StatusOK, []byte("SUCCESS"))
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := newScenarioPair(t, e, clk, sched)
	row := acceptScenario(t, e, svc, "PA_DUP", 10003, srv.URL)
	if row.Scenario != scenario.DupNotify.String() {
		t.Fatalf("scenario = %s", row.Scenario)
	}
	runFinish(t, clk, sched, row)
	if hits.Load() != 0 || sched.len() != 1 {
		t.Fatalf("hits=%d scheduled=%d", hits.Load(), sched.len())
	}
	done := e.row(t, "PA_DUP")
	wantDate := done.FinishedAt.In(shanghai).Format(billDateLayout)
	if done.Status != dal.StatusSuccess || done.BillDate != wantDate || done.NotifySentCount != 0 {
		t.Fatalf("finished = %+v", done)
	}
	if ran := sched.RunDue(clk.Now()); ran != 1 {
		t.Fatalf("wave ran %d", ran)
	}
	if gate.max.Load() != 5 || hits.Load() != 5 || e.row(t, "PA_DUP").NotifySentCount != 5 {
		t.Fatalf("max=%d hits=%d count=%d", gate.max.Load(), hits.Load(), e.row(t, "PA_DUP").NotifySentCount)
	}
	if ran := sched.RunDue(clk.Now()); ran != 0 || hits.Load() != 5 {
		t.Fatalf("sixth ran=%d hits=%d", ran, hits.Load())
	}
	wantDupTail(t, sched, clk.Now())
	clk.Advance(5 * time.Second)
	if ran := sched.RunDue(clk.Now()); ran != 5 {
		t.Fatalf("tail ran %d", ran)
	}
	if hits.Load() != 10 || sched.len() != 0 {
		t.Fatalf("hits=%d scheduled=%d", hits.Load(), sched.len())
	}
	final := e.row(t, "PA_DUP")
	if final.NotifySentCount != 10 || final.Status != dal.StatusSuccess || final.BillDate != done.BillDate ||
		!final.FinishedAt.Equal(done.FinishedAt) {
		t.Fatalf("final = %+v, was %+v", final, done)
	}
	verifyHits(t, []byte(notifyKey), cap.list())
	for _, h := range cap.list() {
		if !strings.Contains(string(h.body), `"status":"SUCCESS"`) || !strings.Contains(string(h.body), `"amount":10003`) {
			t.Fatalf("body = %s", h.body)
		}
	}
	clk.Advance(time.Minute)
	if ran := sched.RunDue(clk.Now()); ran != 0 || e.row(t, "PA_DUP").NotifySentCount != 10 {
		t.Fatalf("extra ran=%d count=%d", ran, e.row(t, "PA_DUP").NotifySentCount)
	}
}

// CH-06 M3 NEVER_FINISH：受理 10008 分。推进时钟后仍 PROCESSING，无完成任务，HTTP 0 次；关单后 CLOSED，finished_at 仍空。
func TestScenarioNeverFinishStaysProcessingMySQL(t *testing.T) {
	e := openMock(t)
	srv, _, hits := scenarioServer(t, nil, http.StatusOK, []byte("SUCCESS"))
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := newScenarioPair(t, e, clk, sched)
	row := acceptScenario(t, e, svc, "PA_NEVER", 10008, srv.URL)
	if row.Scenario != scenario.NeverFinish.String() || sched.len() != 0 {
		t.Fatalf("scenario=%s scheduled=%d", row.Scenario, sched.len())
	}
	clk.Advance(time.Hour)
	if ran := sched.RunDue(clk.Now()); ran != 0 || hits.Load() != 0 {
		t.Fatalf("ran=%d hits=%d", ran, hits.Load())
	}
	got := e.row(t, "PA_NEVER")
	if got.Status != dal.StatusProcessing || !got.FinishedAt.IsZero() || got.BillDate != "" || got.NotifySentCount != 0 {
		t.Fatalf("row = %+v", got)
	}
	res, err := svc.Close(e.ctx, closeReq("PA_NEVER"))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if res != okClosed() {
		t.Fatalf("close = %+v", res)
	}
	clk.Advance(time.Hour)
	if ran := sched.RunDue(clk.Now()); ran != 0 || hits.Load() != 0 || sched.len() != 0 {
		t.Fatalf("after close ran=%d hits=%d scheduled=%d", ran, hits.Load(), sched.len())
	}
	closed := e.row(t, "PA_NEVER")
	finished, bill := e.nullTimes(t, "PA_NEVER")
	if closed.Status != dal.StatusClosed || !closed.FinishedAt.IsZero() || closed.BillDate != "" ||
		closed.NotifySentCount != 0 || finished.Valid || bill.Valid {
		t.Fatalf("closed = %+v finished=%v bill=%v", closed, finished.Valid, bill.Valid)
	}
}

func gotNotifyCount(t *testing.T, e dbEnv, reqNo string) int {
	t.Helper()
	return e.row(t, reqNo).NotifySentCount
}
