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
