//go:build integration

package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

// dueSched 按注入时钟的 due 执行。RunDue 先摘下到期任务再执行，避免任务里再 Schedule 时死锁。
type dueSched struct {
	mu    sync.Mutex
	tasks []dueTask
}

type dueTask struct {
	due  time.Time
	task func()
}

func (s *dueSched) Schedule(due time.Time, task func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, dueTask{due: due, task: task})
}

func (s *dueSched) RunDue(now time.Time) int {
	s.mu.Lock()
	var run []func()
	var keep []dueTask
	for _, task := range s.tasks {
		if !task.due.After(now) {
			run = append(run, task.task)
		} else {
			keep = append(keep, task)
		}
	}
	s.tasks = keep
	s.mu.Unlock()
	for _, fn := range run {
		fn()
	}
	return len(run)
}

func (s *dueSched) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.tasks)
}

// CH-06 验收：受理 HTTP 在注入时钟到 created_at+5s 之前没有响应；RunDue 放行后 HTTP 200，result 是 OK。走 8 秒受理上下文，不让 3 秒上限先取消。
func TestAcceptTimeoutHTTPMySQL(t *testing.T) {
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
	store := dal.New(schema.DB)
	clk := testClock()
	sched := &dueSched{}
	ids, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	svc := biz.New(store, ids, clk, sched, biz.Options{DelayMinMS: 60000, DelayMaxMS: 60000})
	srv := newTestServerWithClock(t, svc, testKey, clk)

	const reqNo = "PA_AT_HTTP"
	body := []byte(`{"req_no":"PA_AT_HTTP","amount":10005,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`)
	ts := sign.FormatTimestamp(clk.Now())
	type httpOut struct {
		code int
		body []byte
	}
	ch := make(chan httpOut, 1)
	go func() {
		w := signedPost(t, srv, testKey, ts, body)
		resp := w.Result()
		ch <- httpOut{code: resp.StatusCode(), body: append([]byte(nil), resp.Body()...)}
	}()
	var row dal.Txn
	waitRow(t, func() bool {
		got, ok, err := store.Get(ctx, dal.TxnTypePay, reqNo)
		if err != nil || !ok || sched.len() != 2 {
			return false
		}
		row = got
		return true
	})
	select {
	case got := <-ch:
		t.Fatalf("response before +5s: status=%d body=%s", got.code, got.body)
	default:
	}
	clk.Set(row.CreatedAt.Add(5*time.Second - time.Millisecond))
	if n := sched.RunDue(clk.Now()); n != 0 {
		t.Fatalf("early ran %d", n)
	}
	select {
	case got := <-ch:
		t.Fatalf("response before due: status=%d body=%s", got.code, got.body)
	default:
	}
	clk.Set(row.CreatedAt.Add(5 * time.Second))
	if n := sched.RunDue(clk.Now()); n != 1 {
		t.Fatalf("hold ran %d", n)
	}
	select {
	case got := <-ch:
		if got.code != http.StatusOK {
			t.Fatalf("status = %d body = %s", got.code, got.body)
		}
		var m map[string]any
		if err := json.Unmarshal(got.body, &m); err != nil {
			t.Fatalf("body = %s: %v", got.body, err)
		}
		if m["result"] != biz.ResultOK {
			t.Fatalf("body = %s", got.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no HTTP response after RunDue")
	}
	got, ok, err := store.Get(ctx, dal.TxnTypePay, reqNo)
	if err != nil || !ok {
		t.Fatalf("get after response: ok=%v err=%v", ok, err)
	}
	if got.Status != dal.StatusProcessing || got.Scenario != "ACCEPT_TIMEOUT" {
		t.Fatalf("row = %+v", got)
	}
}

func waitRow(t *testing.T, cond func() bool) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case <-timeout:
			t.Fatal("row not ready")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
