//go:build integration

package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

var finishedAtRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+08:00$`)

type queryHTTPEnv struct {
	ctx   context.Context
	raw   *sqlx.DB
	store *dal.DB
	svc   *biz.Service
	srv   *server.Hertz
	hooks atomic.Int32
	sched *biz.TimerScheduler
}

func openQueryHTTP(t *testing.T) *queryHTTPEnv {
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
	clk := clock.Real{}
	ids, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	env := &queryHTTPEnv{ctx: ctx, raw: schema.DB, store: dal.New(schema.DB)}
	env.sched = biz.NewTimerScheduler(clk)
	t.Cleanup(env.sched.Stop)
	env.svc = biz.New(env.store, ids, clk, env.sched, biz.Options{
		DelayMinMS: 0,
		DelayMaxMS: 0,
		OnFinished: func(context.Context, dal.Txn) { env.hooks.Add(1) },
	})
	env.srv = server.New(server.WithDisablePrintRoute(true))
	New(env.svc, []byte(testKey), clk).Register(env.srv)
	return env
}

func (e *queryHTTPEnv) signed(t *testing.T, path string, body []byte) map[string]json.RawMessage {
	t.Helper()
	ts := sign.FormatTimestamp(clock.Real{}.Now())
	w := postSigned(t, e.srv, path, testKey, ts, body)
	resp := w.Result()
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.StatusCode(), resp.Body())
	}
	gotTS := string(resp.Header.Peek(sign.HeaderMockTimestamp))
	sig := string(resp.Header.Peek(sign.HeaderMockSignature))
	if err := sign.VerifyChannel([]byte(testKey), sign.ChannelMessage{Timestamp: gotTS, Body: resp.Body()}, sig); err != nil {
		t.Fatalf("response signature: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(resp.Body(), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func (e *queryHTTPEnv) updatedAt(t *testing.T, txnType, reqNo string) time.Time {
	t.Helper()
	var ts time.Time
	err := e.raw.QueryRowContext(e.ctx,
		`SELECT updated_at FROM mock_txn WHERE txn_type = ? AND req_no = ?`, txnType, reqNo).Scan(&ts)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return ts
}

func (e *queryHTTPEnv) waitHooks(t *testing.T, reqNo, status string, hooks int32) dal.Txn {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		row, ok, err := e.store.Get(e.ctx, dal.TxnTypePay, reqNo)
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if ok && row.Status == status && e.hooks.Load() == hooks {
			return row
		}
		select {
		case <-timeout:
			t.Fatalf("%s status=%s hooks=%d", reqNo, row.Status, e.hooks.Load())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// CH-02 验收 1：DROP_NOTIFY 受理并完成后，HTTP 查单返回 OK / SUCCESS，签名可验，行不变。
func TestQueryDropNotifyHTTP(t *testing.T) {
	e := openQueryHTTP(t)
	payBody := []byte(`{"req_no":"PA_DROP","amount":10002,"subject":"demo","notify_url":"http://gateway:8080/callback/mockpay"}`)
	accepted := e.signed(t, PayPath, payBody)
	if string(accepted["result"]) != `"OK"` || string(accepted["status"]) != `"PROCESSING"` {
		t.Fatalf("accept = %s", accepted)
	}
	before := e.waitHooks(t, "PA_DROP", dal.StatusSuccess, 1)
	if before.Scenario != "DROP_NOTIFY" || before.Amount != 10002 || before.FinishedAt.IsZero() || before.NotifySentCount != 0 {
		t.Fatalf("before = %+v", before)
	}
	updated := e.updatedAt(t, dal.TxnTypePay, "PA_DROP")
	hooks := e.hooks.Load()
	pending := e.sched.Pending()
	got := e.signed(t, QueryPath, []byte(`{"txn_type":"PAY","req_no":"PA_DROP"}`))
	if string(got["result"]) != `"OK"` || string(got["status"]) != `"SUCCESS"` || string(got["amount"]) != "10002" {
		t.Fatalf("query = %s", got)
	}
	if string(got["channel_txn_no"]) != fmt.Sprintf("%q", before.ChannelTxnNo) {
		t.Fatalf("channel_txn_no = %s", got["channel_txn_no"])
	}
	var finished string
	if err := json.Unmarshal(got["finished_at"], &finished); err != nil {
		t.Fatal(err)
	}
	if !finishedAtRE.MatchString(finished) {
		t.Fatalf("finished_at = %s", finished)
	}
	wantFinished := before.FinishedAt.In(time.FixedZone("Asia/Shanghai", 8*3600)).Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z07:00")
	if finished != wantFinished {
		t.Fatalf("finished_at = %s, want %s", finished, wantFinished)
	}
	if _, ok := got["reason"]; ok {
		t.Fatal("query body has reason")
	}
	after, ok, err := e.store.Get(e.ctx, dal.TxnTypePay, "PA_DROP")
	if err != nil || !ok {
		t.Fatalf("get after: ok=%v err=%v", ok, err)
	}
	if after.Status != before.Status || after.Amount != before.Amount || after.Scenario != before.Scenario ||
		!after.FinishedAt.Equal(before.FinishedAt) || after.NotifySentCount != before.NotifySentCount ||
		after.ChannelTxnNo != before.ChannelTxnNo {
		t.Fatalf("row changed\nbefore %+v\nafter %+v", before, after)
	}
	if !e.updatedAt(t, dal.TxnTypePay, "PA_DROP").Equal(updated) {
		t.Fatal("updated_at changed")
	}
	if e.hooks.Load() != hooks || e.sched.Pending() != pending {
		t.Fatal("query scheduled or called the hook")
	}
}
