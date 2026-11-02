package biz

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

// leakedKey 放在 URL 查询里。日志打出完整 URL 时测试会看到它。
const leakedKey = "channel-key-material-9f3a"

// notifyWhen 的纳秒是 123.5ms：不先截断再格式化会被四舍五入成 .124。
var notifyWhen = time.Date(2026, 10, 23, 10, 0, 0, 123500000, shanghaiTZ)

const notifyKey = "test-channel-key"

type notifyHit struct {
	body []byte
	ts   string
	sig  string
}

type notifyCap struct {
	mu   sync.Mutex
	hits []notifyHit
}

func (c *notifyCap) add(h notifyHit) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hits = append(c.hits, h)
}

func (c *notifyCap) list() []notifyHit {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]notifyHit, len(c.hits))
	copy(out, c.hits)
	return out
}

type notifyBox struct {
	mu         sync.Mutex
	row        dal.Txn
	has        bool
	miss       bool
	missStatus string
	missCount  int
	missSet    bool
	dupFull    bool
	addErr     error
	getErr     error
	adds       int
	dupAdds    int
	inAdd      atomic.Bool
}

func (b *notifyBox) AddNotifySent(ctx context.Context, id uint64, expected int) (bool, error) {
	b.inAdd.Store(true)
	defer b.inAdd.Store(false)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.adds++
	if b.addErr != nil {
		return false, b.addErr
	}
	if !b.has || b.row.ID != id {
		return false, nil
	}
	if b.miss || b.row.NotifySentCount != expected {
		if b.missStatus != "" {
			b.row.Status = b.missStatus
		}
		if b.missSet {
			b.row.NotifySentCount = b.missCount
		}
		return false, nil
	}
	b.row.NotifySentCount++
	return true, nil
}

func (b *notifyBox) AddDupNotifySent(ctx context.Context, id uint64, limit int) (bool, error) {
	b.inAdd.Store(true)
	defer b.inAdd.Store(false)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dupAdds++
	if b.addErr != nil {
		return false, b.addErr
	}
	if !b.has || b.row.ID != id || b.dupFull || b.row.NotifySentCount >= limit {
		return false, nil
	}
	b.row.NotifySentCount++
	return true, nil
}

func (b *notifyBox) GetByID(ctx context.Context, id uint64) (dal.Txn, bool, error) {
	if err := ctx.Err(); err != nil {
		return dal.Txn{}, false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.getErr != nil {
		return dal.Txn{}, false, b.getErr
	}
	if !b.has || b.row.ID != id {
		return dal.Txn{}, false, nil
	}
	return b.row, true, nil
}

func (b *notifyBox) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.row.NotifySentCount
}

type notifyRig struct {
	box   *notifyBox
	clk   *clock.Fake
	sched *manualSched
	n     *Notifier
	cap   *notifyCap
	srv   *httptest.Server
}

func newNotifyRig(t *testing.T, row dal.Txn, h func(http.ResponseWriter, *http.Request, []byte, int)) *notifyRig {
	t.Helper()
	cap := &notifyCap{}
	var seq atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			http.Error(w, "read", http.StatusInternalServerError)
			return
		}
		n := int(seq.Add(1))
		cap.add(notifyHit{
			body: body,
			ts:   r.Header.Get(sign.HeaderMockTimestamp),
			sig:  r.Header.Get(sign.HeaderMockSignature),
		})
		h(w, r, body, n)
	}))
	t.Cleanup(srv.Close)
	if row.NotifyURL == "" {
		row.NotifyURL = srv.URL
	}
	box := &notifyBox{row: row, has: true}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	n := NewNotifier(box, clockIDs(t, clk), clk, sched, []byte(notifyKey))
	return &notifyRig{box: box, clk: clk, sched: sched, n: n, cap: cap, srv: srv}
}

func clockIDs(t *testing.T, clk *clock.Fake) *idgen.Generator {
	t.Helper()
	g, err := idgen.New(4, clk)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func notifySample(status, scen string) dal.Txn {
	return dal.Txn{
		ID:           7,
		ChannelTxnNo: "MC1",
		TxnType:      dal.TxnTypePay,
		ReqNo:        "A<B&C",
		Amount:       10000,
		Status:       status,
		Scenario:     scen,
		FinishedAt:   notifyWhen,
	}
}

func writeBytes(t *testing.T, w http.ResponseWriter, b []byte) {
	t.Helper()
	if _, err := w.Write(b); err != nil {
		t.Errorf("write: %v", err)
	}
}

func runOne(t *testing.T, clk *clock.Fake, sched *manualSched) {
	t.Helper()
	if n := sched.RunDue(clk.Now()); n != 1 {
		t.Fatalf("ran %d", n)
	}
}

func wantDue(t *testing.T, sched *manualSched, due time.Time) {
	t.Helper()
	got := sched.dues()
	if len(got) != 1 || !got[0].Equal(due) {
		t.Fatalf("dues = %v, want %v", got, due)
	}
}

func verifyHits(t *testing.T, key []byte, hits []notifyHit) {
	t.Helper()
	seen := map[string]struct{}{}
	for i, h := range hits {
		if !strings.HasPrefix(string(notifyIDOf(t, h.body)), "CN") || len(notifyIDOf(t, h.body)) != 21 {
			t.Fatalf("hit %d notify_id in %s", i, h.body)
		}
		id := notifyIDOf(t, h.body)
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate notify_id %s", id)
		}
		seen[id] = struct{}{}
		if err := sign.VerifyChannel(key, sign.ChannelMessage{Timestamp: h.ts, Body: h.body}, h.sig); err != nil {
			t.Fatalf("hit %d verify: %v body=%s", i, err, h.body)
		}
	}
}

func notifyIDOf(t *testing.T, body []byte) string {
	t.Helper()
	const key = `"notify_id":"`
	i := bytes.Index(body, []byte(key))
	if i < 0 {
		t.Fatalf("no notify_id in %s", body)
	}
	rest := body[i+len(key):]
	j := bytes.IndexByte(rest, '"')
	if j < 0 {
		t.Fatalf("notify_id not closed in %s", body)
	}
	return string(rest[:j])
}

func closedNotifyURL(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return "http://" + addr + "/cb?password=" + leakedKey
}

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	var buf bytes.Buffer
	logx.SetDefault("channel-mock", &buf)
	return &buf
}

// CH-05 验收：平台前两次 500，第三次 body 为 SUCCESS，停止，notify_sent_count = 3。
func TestNotifyRetriesUntilThirdSuccess(t *testing.T) {
	row := notifySample(dal.StatusSuccess, scenario.Normal.String())
	rig := newNotifyRig(t, row, func(w http.ResponseWriter, _ *http.Request, _ []byte, n int) {
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			writeBytes(t, w, []byte("NO"))
			return
		}
		writeBytes(t, w, []byte("SUCCESS"))
	})
	ctx := context.Background()
	rig.n.Handle(ctx, rig.box.row)
	if len(rig.cap.list()) != 0 {
		t.Fatal("Handle sent HTTP")
	}
	wantDue(t, rig.sched, rig.clk.Now())
	runOne(t, rig.clk, rig.sched)
	wantDue(t, rig.sched, rig.clk.Now().Add(5*time.Second))
	if rig.box.count() != 1 {
		t.Fatalf("count = %d", rig.box.count())
	}
	rig.clk.Advance(5 * time.Second)
	runOne(t, rig.clk, rig.sched)
	wantDue(t, rig.sched, rig.clk.Now().Add(15*time.Second))
	if rig.box.count() != 2 {
		t.Fatalf("count = %d", rig.box.count())
	}
	rig.clk.Advance(15 * time.Second)
	runOne(t, rig.clk, rig.sched)
	if rig.box.count() != 3 || rig.sched.len() != 0 || len(rig.cap.list()) != 3 {
		t.Fatalf("count=%d scheduled=%d hits=%d", rig.box.count(), rig.sched.len(), len(rig.cap.list()))
	}
	rig.clk.Advance(time.Minute)
	if n := rig.sched.RunDue(rig.clk.Now()); n != 0 {
		t.Fatalf("extra run %d", n)
	}
	verifyHits(t, []byte(notifyKey), rig.cap.list())
	hits := rig.cap.list()
	if notifyIDOf(t, hits[0].body) == notifyIDOf(t, hits[1].body) {
		t.Fatal("notify_id reused")
	}
}

// CH-05：第一次就 SUCCESS，计数 1，没有后续任务。
func TestNotifyStopsOnFirstSuccess(t *testing.T) {
	rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
		writeBytes(t, w, []byte("SUCCESS"))
	})
	rig.n.Handle(context.Background(), rig.box.row)
	runOne(t, rig.clk, rig.sched)
	if rig.box.count() != 1 || rig.sched.len() != 0 || len(rig.cap.list()) != 1 {
		t.Fatalf("count=%d scheduled=%d hits=%d", rig.box.count(), rig.sched.len(), len(rig.cap.list()))
	}
}

// CH-05：连续 5 次都不是 SUCCESS，计数 5，没有第 6 次。
func TestNotifyStopsAfterFiveFailures(t *testing.T) {
	rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
		w.WriteHeader(http.StatusInternalServerError)
		writeBytes(t, w, []byte("NO"))
	})
	rig.n.Handle(context.Background(), rig.box.row)
	waits := []time.Duration{0, 5 * time.Second, 15 * time.Second, 30 * time.Second, 60 * time.Second}
	for i, wait := range waits {
		if wait > 0 {
			wantDue(t, rig.sched, rig.clk.Now().Add(wait))
			rig.clk.Advance(wait)
		}
		runOne(t, rig.clk, rig.sched)
		if rig.box.count() != i+1 {
			t.Fatalf("after %d count=%d", i+1, rig.box.count())
		}
	}
	if rig.sched.len() != 0 || len(rig.cap.list()) != 5 {
		t.Fatalf("scheduled=%d hits=%d", rig.sched.len(), len(rig.cap.list()))
	}
	rig.clk.Advance(time.Minute)
	if n := rig.sched.RunDue(rig.clk.Now()); n != 0 || rig.box.count() != 5 {
		t.Fatalf("sixth n=%d count=%d", n, rig.box.count())
	}
	verifyHits(t, []byte(notifyKey), rig.cap.list())
}

// CH-05：body 字段顺序固定，不转义 HTML，finished_at 先截到毫秒再标 +08:00。
func TestNotifyBodyIsFixedOrder(t *testing.T) {
	rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
		writeBytes(t, w, []byte("SUCCESS"))
	})
	rig.n.Handle(context.Background(), rig.box.row)
	runOne(t, rig.clk, rig.sched)
	hits := rig.cap.list()
	if len(hits) != 1 {
		t.Fatalf("hits = %d", len(hits))
	}
	id := notifyIDOf(t, hits[0].body)
	want := `{"notify_id":"` + id + `","txn_type":"PAY","req_no":"A<B&C","channel_txn_no":"MC1","amount":10000,"status":"SUCCESS","finished_at":"2026-10-23T10:00:00.123+08:00"}`
	if string(hits[0].body) != want {
		t.Fatalf("body = %s", hits[0].body)
	}
	if strings.Contains(string(hits[0].body), `\u003c`) || strings.Contains(string(hits[0].body), `\u0026`) {
		t.Fatalf("html escaped: %s", hits[0].body)
	}
}

// CH-05：FAIL 的 body status 为 FAILED，finished_at 带毫秒和 +08:00。
func TestNotifyFailBodyIsFailed(t *testing.T) {
	row := notifySample(dal.StatusFailed, scenario.Fail.String())
	row.FinishedAt = time.Date(2026, 10, 22, 16, 0, 0, 123500000, time.UTC)
	row.ReqNo = "PAFAIL"
	rig := newNotifyRig(t, row, func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
		writeBytes(t, w, []byte("SUCCESS"))
	})
	rig.n.Handle(context.Background(), rig.box.row)
	runOne(t, rig.clk, rig.sched)
	hits := rig.cap.list()
	if len(hits) != 1 {
		t.Fatalf("hits = %d", len(hits))
	}
	id := notifyIDOf(t, hits[0].body)
	want := `{"notify_id":"` + id + `","txn_type":"PAY","req_no":"PAFAIL","channel_txn_no":"MC1","amount":10000,"status":"FAILED","finished_at":"2026-10-23T00:00:00.123+08:00"}`
	if string(hits[0].body) != want {
		t.Fatalf("body = %s", hits[0].body)
	}
	if rig.box.count() != 1 || rig.sched.len() != 0 {
		t.Fatalf("count=%d scheduled=%d", rig.box.count(), rig.sched.len())
	}
}

// CH-05：DROP_NOTIFY、空 notify_url、finished_at 为零、非终态都不发，计数不变。
// SLOW 的首次延迟是 CH-06，不在这里当成不发送。DUP_NOTIFY 的 10 次发送也是 CH-06。
func TestNotifySkipsWithoutSending(t *testing.T) {
	cases := []struct {
		name     string
		status   string
		scenario string
		url      string
		finished time.Time
		count    int
	}{
		{name: "drop", status: dal.StatusSuccess, scenario: scenario.DropNotify.String(), url: "http://keep", finished: notifyWhen},
		{name: "empty url", status: dal.StatusSuccess, scenario: scenario.Normal.String(), url: "", finished: notifyWhen},
		{name: "zero finished", status: dal.StatusSuccess, scenario: scenario.Normal.String(), url: "http://keep", finished: time.Time{}},
		{name: "processing", status: dal.StatusProcessing, scenario: scenario.Normal.String(), url: "http://keep", finished: notifyWhen},
		{name: "closed", status: dal.StatusClosed, scenario: scenario.Normal.String(), url: "http://keep", finished: notifyWhen},
		{name: "already five", status: dal.StatusSuccess, scenario: scenario.Normal.String(), url: "http://keep", finished: notifyWhen, count: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := notifySample(tc.status, tc.scenario)
			row.NotifyURL = tc.url
			row.FinishedAt = tc.finished
			row.NotifySentCount = tc.count
			box := &notifyBox{row: row, has: true}
			clk := &clock.Fake{}
			clk.Set(baseTime)
			sched := &manualSched{}
			n := NewNotifier(box, clockIDs(t, clk), clk, sched, []byte(notifyKey))
			n.Handle(context.Background(), row)
			if sched.len() != 0 || box.adds != 0 || box.count() != tc.count {
				t.Fatalf("scheduled=%d adds=%d count=%d", sched.len(), box.adds, box.count())
			}
		})
	}
}

// CH-06 验收：SLOW 的首次 due 是 finished_at 之后 30 秒。到这之前 HTTP 0 次、不加计数；到点且对方 body 恰好是 SUCCESS 后计数为 1。
func TestNotifySlowFirstDueIsFinishedAtPlus30s(t *testing.T) {
	rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Slow.String()), func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
		writeBytes(t, w, []byte("SUCCESS"))
	})
	rig.n.Handle(context.Background(), rig.box.row)
	due := rig.box.row.FinishedAt.Add(slowFirstDelay)
	wantDue(t, rig.sched, due)
	if len(rig.cap.list()) != 0 || rig.box.adds != 0 || rig.box.count() != 0 {
		t.Fatalf("hits=%d adds=%d count=%d", len(rig.cap.list()), rig.box.adds, rig.box.count())
	}
	rig.clk.Set(due.Add(-time.Millisecond))
	if n := rig.sched.RunDue(rig.clk.Now()); n != 0 || len(rig.cap.list()) != 0 || rig.box.adds != 0 || rig.box.count() != 0 {
		t.Fatalf("early n=%d hits=%d adds=%d count=%d", n, len(rig.cap.list()), rig.box.adds, rig.box.count())
	}
	rig.clk.Set(due)
	runOne(t, rig.clk, rig.sched)
	if rig.box.count() != 1 || len(rig.cap.list()) != 1 || rig.sched.len() != 0 {
		t.Fatalf("count=%d hits=%d scheduled=%d", rig.box.count(), len(rig.cap.list()), rig.sched.len())
	}
}

// CH-05：NORMAL、未登记场景、ACCEPT_TIMEOUT、BILL_MISSING、BILL_AMOUNT_DIFF、CLOSE_RACE 走普通发送。
func TestNotifySendsOrdinaryScenarios(t *testing.T) {
	scenarios := []string{
		scenario.Normal.String(),
		scenario.AcceptTimeout.String(),
		scenario.BillMissing.String(),
		scenario.BillAmountDiff.String(),
		scenario.CloseRace.String(),
		"NOT_REGISTERED",
	}
	for _, sc := range scenarios {
		t.Run(sc, func(t *testing.T) {
			rig := newNotifyRig(t, notifySample(dal.StatusSuccess, sc), func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
				writeBytes(t, w, []byte("SUCCESS"))
			})
			rig.n.Handle(context.Background(), rig.box.row)
			runOne(t, rig.clk, rig.sched)
			if rig.box.count() != 1 || len(rig.cap.list()) != 1 || rig.sched.len() != 0 {
				t.Fatalf("count=%d hits=%d scheduled=%d", rig.box.count(), len(rig.cap.list()), rig.sched.len())
			}
		})
	}
}

// CH-05：空密钥不发请求、不加计数，1 秒后再排同一次；日志里没有密钥。
func TestNotifyEmptyKeyDoesNotSend(t *testing.T) {
	buf := captureLog(t)
	row := notifySample(dal.StatusSuccess, scenario.Normal.String())
	row.NotifyURL = closedNotifyURL(t)
	box := &notifyBox{row: row, has: true}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	n := NewNotifier(box, clockIDs(t, clk), clk, sched, nil)
	n.Handle(context.Background(), row)
	runOne(t, clk, sched)
	if box.count() != 0 || box.adds != 0 {
		t.Fatalf("count=%d adds=%d", box.count(), box.adds)
	}
	wantDue(t, sched, clk.Now().Add(time.Second))
	clk.Advance(time.Second)
	runOne(t, clk, sched)
	if box.count() != 0 || box.adds != 0 || sched.len() != 1 {
		t.Fatalf("count=%d adds=%d scheduled=%d", box.count(), box.adds, sched.len())
	}
	out := buf.String()
	if !strings.Contains(out, "sign: empty key") || !strings.Contains(out, `"level":"ERROR"`) {
		t.Fatalf("log = %s", out)
	}
	if strings.Contains(out, leakedKey) || strings.Contains(out, "X-Mock-Signature") {
		t.Fatalf("log leaked secret: %s", out)
	}
}

// CH-05：发号失败不算发送，不加计数，1 秒后再排同一次。
func TestNotifyIDGenFailureDoesNotSend(t *testing.T) {
	buf := captureLog(t)
	row := notifySample(dal.StatusSuccess, scenario.Normal.String())
	row.NotifyURL = closedNotifyURL(t)
	box := &notifyBox{row: row, has: true}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	n := NewNotifier(box, errIDs{err: idgen.ErrClockRollback}, clk, sched, []byte(notifyKey))
	n.Handle(context.Background(), row)
	runOne(t, clk, sched)
	if box.count() != 0 || box.adds != 0 {
		t.Fatalf("count=%d adds=%d", box.count(), box.adds)
	}
	wantDue(t, sched, clk.Now().Add(time.Second))
	out := buf.String()
	if !strings.Contains(out, idgen.ErrClockRollback.Error()) || strings.Contains(out, leakedKey) || strings.Contains(out, notifyKey) {
		t.Fatalf("log = %s", out)
	}
}

type errIDs struct{ err error }

func (e errIDs) NewNo(context.Context, string) (string, error) { return "", e.err }

// CH-05：条件更新影响 0 行时不发 HTTP。次数已满或不再是终态则停止。
func TestNotifyClaimMissDoesNotSend(t *testing.T) {
	t.Run("status changed", func(t *testing.T) {
		rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(http.ResponseWriter, *http.Request, []byte, int) {
			t.Fatal("sent HTTP")
		})
		rig.box.miss = true
		rig.box.missStatus = dal.StatusClosed
		rig.n.Handle(context.Background(), rig.box.row)
		runOne(t, rig.clk, rig.sched)
		if rig.box.count() != 0 || rig.sched.len() != 0 || len(rig.cap.list()) != 0 {
			t.Fatalf("count=%d scheduled=%d hits=%d", rig.box.count(), rig.sched.len(), len(rig.cap.list()))
		}
	})
	t.Run("count already five", func(t *testing.T) {
		rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(http.ResponseWriter, *http.Request, []byte, int) {
			t.Fatal("sent HTTP")
		})
		rig.box.miss = true
		rig.box.missSet = true
		rig.box.missCount = 5
		rig.n.Handle(context.Background(), rig.box.row)
		runOne(t, rig.clk, rig.sched)
		if len(rig.cap.list()) != 0 || rig.sched.len() != 0 {
			t.Fatalf("hits=%d scheduled=%d", len(rig.cap.list()), rig.sched.len())
		}
	})
}

// CH-05：更新报错不发 HTTP，1 秒后按原计数重试，不把这次算进 5 次。
func TestNotifyUpdateErrorDoesNotSend(t *testing.T) {
	rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(http.ResponseWriter, *http.Request, []byte, int) {
		t.Fatal("sent HTTP")
	})
	rig.box.addErr = errors.New("db down")
	rig.n.Handle(context.Background(), rig.box.row)
	runOne(t, rig.clk, rig.sched)
	if rig.box.count() != 0 || len(rig.cap.list()) != 0 {
		t.Fatalf("count=%d hits=%d", rig.box.count(), len(rig.cap.list()))
	}
	wantDue(t, rig.sched, rig.clk.Now().Add(time.Second))
}

// CH-05：同一 id 已有发送链时再次 Handle 直接返回，不把次数打双。
func TestNotifyDuplicateHandleDoesNotDouble(t *testing.T) {
	rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
		writeBytes(t, w, []byte("SUCCESS"))
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		rig.n.Handle(ctx, rig.box.row)
	}()
	go func() {
		defer wg.Done()
		rig.n.Handle(ctx, rig.box.row)
	}()
	wg.Wait()
	if rig.sched.len() != 1 {
		t.Fatalf("scheduled = %d", rig.sched.len())
	}
	runOne(t, rig.clk, rig.sched)
	rig.n.Handle(ctx, rig.box.row)
	if rig.box.count() != 1 || rig.sched.len() != 0 || len(rig.cap.list()) != 1 {
		t.Fatalf("count=%d scheduled=%d hits=%d", rig.box.count(), rig.sched.len(), len(rig.cap.list()))
	}
}

// CH-05：大小写、引号、尾部换行、非 200、超过 1024 字节都不算送达，但这次已经发送。
func TestNotifyAckMustBeExact(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		long   bool
	}{
		{name: "lowercase", status: http.StatusOK, body: "success"},
		{name: "quoted", status: http.StatusOK, body: `"SUCCESS"`},
		{name: "newline", status: http.StatusOK, body: "SUCCESS\n"},
		{name: "http 500 with SUCCESS", status: http.StatusInternalServerError, body: "SUCCESS"},
		{name: "too long", status: http.StatusOK, long: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
				if tc.status != http.StatusOK {
					w.WriteHeader(tc.status)
				}
				if tc.long {
					writeBytes(t, w, bytes.Repeat([]byte("S"), notifyMaxBody+1))
					return
				}
				writeBytes(t, w, []byte(tc.body))
			})
			rig.n.Handle(context.Background(), rig.box.row)
			runOne(t, rig.clk, rig.sched)
			if rig.box.count() != 1 || len(rig.cap.list()) != 1 {
				t.Fatalf("count=%d hits=%d", rig.box.count(), len(rig.cap.list()))
			}
			wantDue(t, rig.sched, rig.clk.Now().Add(5*time.Second))
		})
	}
}

// CH-05：不跟随重定向。3xx 不是送达，Location 不会被再请求。
func TestNotifyRedirectIsNotDelivered(t *testing.T) {
	var followed atomic.Int32
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		followed.Add(1)
		writeBytes(t, w, []byte("SUCCESS"))
	}))
	t.Cleanup(okSrv.Close)
	rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(w http.ResponseWriter, r *http.Request, _ []byte, _ int) {
		http.Redirect(w, r, okSrv.URL, http.StatusFound)
	})
	rig.n.Handle(context.Background(), rig.box.row)
	runOne(t, rig.clk, rig.sched)
	if followed.Load() != 0 || rig.box.count() != 1 {
		t.Fatalf("followed=%d count=%d", followed.Load(), rig.box.count())
	}
	wantDue(t, rig.sched, rig.clk.Now().Add(5*time.Second))
}

// CH-05：连接失败算已经发送，并按 5 秒重试；日志不含 URL 里的口令。
func TestNotifyConnFailCountsAsSent(t *testing.T) {
	buf := captureLog(t)
	row := notifySample(dal.StatusSuccess, scenario.Normal.String())
	row.NotifyURL = closedNotifyURL(t)
	box := &notifyBox{row: row, has: true}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	n := NewNotifier(box, clockIDs(t, clk), clk, sched, []byte(notifyKey))
	n.Handle(context.Background(), row)
	runOne(t, clk, sched)
	if box.count() != 1 {
		t.Fatalf("count = %d", box.count())
	}
	wantDue(t, sched, clk.Now().Add(5*time.Second))
	if strings.Contains(buf.String(), leakedKey) || strings.Contains(buf.String(), notifyKey) {
		t.Fatalf("log leaked: %s", buf.String())
	}
}

// CH-05：响应体读失败算未送达，这次已经发送。
func TestNotifyReadErrorCountsAsSent(t *testing.T) {
	row := notifySample(dal.StatusSuccess, scenario.Normal.String())
	row.NotifyURL = "http://notify.invalid/cb"
	box := &notifyBox{row: row, has: true}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	n := NewNotifier(box, clockIDs(t, clk), clk, sched, []byte(notifyKey))
	n.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(errReader{}),
			Header:     make(http.Header),
		}, nil
	})
	n.Handle(context.Background(), row)
	runOne(t, clk, sched)
	if box.count() != 1 {
		t.Fatalf("count = %d", box.count())
	}
	wantDue(t, sched, clk.Now().Add(5*time.Second))
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read boom") }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// CH-05：单次请求带上下文超时，不使用 time.Sleep 去等它。
func TestNotifyRequestHasDeadline(t *testing.T) {
	row := notifySample(dal.StatusSuccess, scenario.Normal.String())
	row.NotifyURL = "http://notify.invalid/cb"
	box := &notifyBox{row: row, has: true}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	n := NewNotifier(box, clockIDs(t, clk), clk, sched, []byte(notifyKey))
	n.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("request has no deadline")
		}
		if r.Header.Get(sign.HeaderMockTimestamp) == "" || r.Header.Get(sign.HeaderMockSignature) == "" {
			t.Error("missing channel signature headers")
		}
		return nil, errors.New("stop")
	})
	n.Handle(context.Background(), row)
	runOne(t, clk, sched)
	if box.count() != 1 {
		t.Fatalf("count = %d", box.count())
	}
}

// CH-05：Handle 立刻返回。HTTP 不发生在计数更新期间，也不发生在调用方还没放行调度之前。
func TestNotifyHTTPOutsideCallerAndUpdate(t *testing.T) {
	var inCaller atomic.Bool
	var box *notifyBox
	rig := newNotifyRig(t, notifySample(dal.StatusSuccess, scenario.Normal.String()), func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int) {
		if inCaller.Load() {
			t.Error("http inside caller")
		}
		if box != nil && box.inAdd.Load() {
			t.Error("http inside count update")
		}
		writeBytes(t, w, []byte("SUCCESS"))
	})
	box = rig.box
	inCaller.Store(true)
	rig.n.Handle(context.Background(), rig.box.row)
	if len(rig.cap.list()) != 0 || rig.box.adds != 0 {
		t.Fatal("Handle did the send")
	}
	inCaller.Store(false)
	runOne(t, rig.clk, rig.sched)
	if rig.box.count() != 1 || len(rig.cap.list()) != 1 {
		t.Fatalf("count=%d hits=%d", rig.box.count(), len(rig.cap.list()))
	}
}

// CH-05：普通关单仍不调用钩子，通知器不会因此发 HTTP。已排的自动完成到期也不再通知。
func TestOrdinaryCloseDoesNotNotify(t *testing.T) {
	e := newEnv(t)
	hits, srv := notifyOKServer(t, nil)
	if _, err := e.svc.Pay(context.Background(), payReq("PA_CLOSE", 10000)); err != nil {
		t.Fatal(err)
	}
	box := &notifyBox{}
	n := NewNotifier(box, clockIDs(t, e.clk), e.clk, e.sched, []byte(notifyKey))
	e.svc.SetOnFinished(n.Handle)
	res, err := e.svc.Close(context.Background(), closeReq("PA_CLOSE"))
	if err != nil {
		t.Fatal(err)
	}
	if res != (CloseResult{Result: ResultOK, Status: dal.StatusClosed}) {
		t.Fatalf("close = %+v", res)
	}
	if hits.Load() != 0 || e.sched.len() != 1 {
		t.Fatalf("hits=%d scheduled=%d", hits.Load(), e.sched.len())
	}
	if !e.sched.dues()[0].After(e.clk.Now()) {
		t.Fatal("notify queued at close time")
	}
	e.clk.Advance(2 * time.Second)
	if ran := e.sched.RunDue(e.clk.Now()); ran != 1 {
		t.Fatalf("ran %d", ran)
	}
	if hits.Load() != 0 || box.adds != 0 {
		t.Fatalf("hits=%d adds=%d", hits.Load(), box.adds)
	}
	row, _ := e.store.row("PA_CLOSE")
	if row.Status != dal.StatusClosed || !row.FinishedAt.IsZero() || row.NotifySentCount != 0 {
		t.Fatalf("row = %+v", row)
	}
	if srv.URL == "" {
		t.Fatal("server not started")
	}
}

// CH-05：CLOSE_RACE 走同一个 OnFinished，只入队；Close 返回前不发 HTTP。
func TestCloseRaceNotifyIsQueued(t *testing.T) {
	e := newEnv(t)
	var cap notifyCap
	_, srv := notifyOKServer(t, &cap)
	if _, err := e.svc.Pay(context.Background(), payReq("PA_RACE", 10009)); err != nil {
		t.Fatal(err)
	}
	e.store.update("PA_RACE", func(row *dal.Txn) { row.NotifyURL = srv.URL })
	stored, ok := e.store.row("PA_RACE")
	if !ok {
		t.Fatal("missing row")
	}
	box := &notifyBox{row: stored, has: true}
	n := NewNotifier(box, clockIDs(t, e.clk), e.clk, e.sched, []byte(notifyKey))
	e.svc.SetOnFinished(n.Handle)
	res, err := e.svc.Close(context.Background(), closeReq("PA_RACE"))
	if err != nil {
		t.Fatal(err)
	}
	if res != (CloseResult{Result: ResultAlreadyPaid, Status: dal.StatusSuccess}) {
		t.Fatalf("close = %+v", res)
	}
	if len(cap.list()) != 0 {
		t.Fatal("http before Close returned")
	}
	// 自动完成还在将来；现在到期的只有回调。
	if ran := e.sched.RunDue(e.clk.Now()); ran != 1 {
		t.Fatalf("ran %d", ran)
	}
	if len(cap.list()) != 1 || box.count() != 1 {
		t.Fatalf("hits=%d count=%d", len(cap.list()), box.count())
	}
	if !strings.Contains(string(cap.list()[0].body), `"status":"SUCCESS"`) {
		t.Fatalf("body = %s", cap.list()[0].body)
	}
}

func notifyOKServer(t *testing.T, cap *notifyCap) (*atomic.Int32, *httptest.Server) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read: %v", err)
			http.Error(w, "read", http.StatusInternalServerError)
			return
		}
		if cap != nil {
			cap.add(notifyHit{
				body: body,
				ts:   r.Header.Get(sign.HeaderMockTimestamp),
				sig:  r.Header.Get(sign.HeaderMockSignature),
			})
		}
		hits.Add(1)
		writeBytes(t, w, []byte("SUCCESS"))
	}))
	t.Cleanup(srv.Close)
	return &hits, srv
}

// flightGate 把已到达的请求挡住，直到 limit 个都在处理函数里，再一起放行。
// 计时器只在没凑齐时让测试失败，不用 Sleep 等待成功。
type flightGate struct {
	limit   int
	max     atomic.Int32
	cur     atomic.Int32
	release chan struct{}
	once    sync.Once
}

func newFlightGate(limit int) *flightGate {
	return &flightGate{limit: limit, release: make(chan struct{})}
}

func (g *flightGate) arrive(t *testing.T) {
	t.Helper()
	n := g.cur.Add(1)
	for {
		old := g.max.Load()
		if n <= old || g.max.CompareAndSwap(old, n) {
			break
		}
	}
	if int(n) == g.limit {
		g.once.Do(func() { close(g.release) })
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-g.release:
	case <-timer.C:
		t.Errorf("in flight = %d, want %d", g.cur.Load(), g.limit)
	}
}

func dupSample() dal.Txn {
	row := notifySample(dal.StatusSuccess, scenario.DupNotify.String())
	row.Amount = 10003
	return row
}

func wantDupTail(t *testing.T, sched *manualSched, now time.Time) {
	t.Helper()
	got := sched.dues()
	if len(got) != 5 {
		t.Fatalf("dues = %v", got)
	}
	for i := 1; i <= 5; i++ {
		want := now.Add(time.Duration(i) * time.Second)
		if !got[i-1].Equal(want) {
			t.Fatalf("due %d = %s, want %s", i, got[i-1], want)
		}
	}
}

// CH-06 DUP_NOTIFY：第一波 5 路同时在途；平台一直 SUCCESS 仍是 10 次，notify_id 各不相同且 VerifyChannel 通过。
// 第一波结束计数是 5，时钟不推进没有第 6 次；推进 5 秒后正好 10 次。不改状态、finished_at、bill_date。
func TestDupNotifyOverlapsThenTenOnSuccess(t *testing.T) {
	gate := newFlightGate(5)
	row := dupSample()
	status, finished, bill := row.Status, row.FinishedAt, row.BillDate
	rig := newNotifyRig(t, row, func(w http.ResponseWriter, _ *http.Request, _ []byte, n int) {
		if n <= 5 {
			gate.arrive(t)
		}
		writeBytes(t, w, []byte("SUCCESS"))
	})
	ctx := context.Background()
	rig.n.Handle(ctx, rig.box.row)
	rig.n.Handle(ctx, rig.box.row)
	if rig.sched.len() != 1 || len(rig.cap.list()) != 0 {
		t.Fatalf("scheduled=%d hits=%d", rig.sched.len(), len(rig.cap.list()))
	}
	runOne(t, rig.clk, rig.sched)
	if gate.max.Load() != 5 {
		t.Fatalf("max in flight = %d", gate.max.Load())
	}
	if rig.box.count() != 5 || len(rig.cap.list()) != 5 || rig.box.adds != 0 || rig.box.dupAdds != 5 {
		t.Fatalf("count=%d hits=%d adds=%d dupAdds=%d", rig.box.count(), len(rig.cap.list()), rig.box.adds, rig.box.dupAdds)
	}
	if ran := rig.sched.RunDue(rig.clk.Now()); ran != 0 || len(rig.cap.list()) != 5 {
		t.Fatalf("sixth without clock ran=%d hits=%d", ran, len(rig.cap.list()))
	}
	wantDupTail(t, rig.sched, rig.clk.Now())
	rig.clk.Advance(5 * time.Second)
	if ran := rig.sched.RunDue(rig.clk.Now()); ran != 5 {
		t.Fatalf("tail ran %d", ran)
	}
	if rig.box.count() != 10 || len(rig.cap.list()) != 10 || rig.sched.len() != 0 || rig.box.dupAdds != 10 {
		t.Fatalf("count=%d hits=%d scheduled=%d dupAdds=%d", rig.box.count(), len(rig.cap.list()), rig.sched.len(), rig.box.dupAdds)
	}
	rig.clk.Advance(time.Minute)
	if ran := rig.sched.RunDue(rig.clk.Now()); ran != 0 || rig.box.count() != 10 {
		t.Fatalf("extra ran=%d count=%d", ran, rig.box.count())
	}
	verifyHits(t, []byte(notifyKey), rig.cap.list())
	for _, h := range rig.cap.list() {
		if !strings.Contains(string(h.body), `"status":"SUCCESS"`) || !strings.Contains(string(h.body), `"amount":10003`) {
			t.Fatalf("body = %s", h.body)
		}
	}
	if rig.box.row.Status != status || !rig.box.row.FinishedAt.Equal(finished) || rig.box.row.BillDate != bill {
		t.Fatalf("row changed: %+v", rig.box.row)
	}
}

// CH-06 DUP_NOTIFY：平台一直 500 也是 10 次，不走 5s/15s/30s/60s。
func TestDupNotifyTenOnHTTP500(t *testing.T) {
	gate := newFlightGate(5)
	rig := newNotifyRig(t, dupSample(), func(w http.ResponseWriter, _ *http.Request, _ []byte, n int) {
		if n <= 5 {
			gate.arrive(t)
		}
		w.WriteHeader(http.StatusInternalServerError)
		writeBytes(t, w, []byte("NO"))
	})
	rig.n.Handle(context.Background(), rig.box.row)
	runOne(t, rig.clk, rig.sched)
	if gate.max.Load() != 5 || rig.box.count() != 5 || len(rig.cap.list()) != 5 {
		t.Fatalf("max=%d count=%d hits=%d", gate.max.Load(), rig.box.count(), len(rig.cap.list()))
	}
	wantDupTail(t, rig.sched, rig.clk.Now())
	if ran := rig.sched.RunDue(rig.clk.Now()); ran != 0 {
		t.Fatalf("sixth without clock ran %d", ran)
	}
	rig.clk.Advance(5 * time.Second)
	if ran := rig.sched.RunDue(rig.clk.Now()); ran != 5 {
		t.Fatalf("tail ran %d", ran)
	}
	if rig.box.count() != 10 || len(rig.cap.list()) != 10 || rig.sched.len() != 0 {
		t.Fatalf("count=%d hits=%d scheduled=%d", rig.box.count(), len(rig.cap.list()), rig.sched.len())
	}
	verifyHits(t, []byte(notifyKey), rig.cap.list())
	rig.clk.Advance(time.Minute)
	if ran := rig.sched.RunDue(rig.clk.Now()); ran != 0 || rig.box.count() != 10 {
		t.Fatalf("extra ran=%d count=%d", ran, rig.box.count())
	}
}

// CH-06 DUP_NOTIFY：空密钥不加计数，不发 HTTP，1 秒后再补这一次，不在调用栈上自旋。
func TestDupNotifyEmptyKeyDoesNotCount(t *testing.T) {
	buf := captureLog(t)
	row := dupSample()
	row.NotifyURL = closedNotifyURL(t)
	box := &notifyBox{row: row, has: true}
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	n := NewNotifier(box, clockIDs(t, clk), clk, sched, nil)
	n.Handle(context.Background(), row)
	if sched.len() != 1 {
		t.Fatalf("scheduled = %d", sched.len())
	}
	runOne(t, clk, sched)
	if box.count() != 0 || box.adds != 0 || box.dupAdds != 0 || sched.len() != 5 {
		t.Fatalf("count=%d adds=%d dupAdds=%d scheduled=%d", box.count(), box.adds, box.dupAdds, sched.len())
	}
	for _, due := range sched.dues() {
		if !due.Equal(clk.Now().Add(time.Second)) {
			t.Fatalf("due = %s", due)
		}
	}
	if ran := sched.RunDue(clk.Now()); ran != 0 || box.count() != 0 {
		t.Fatalf("spun ran=%d count=%d", ran, box.count())
	}
	out := buf.String()
	if !strings.Contains(out, "sign: empty key") || !strings.Contains(out, `"level":"ERROR"`) {
		t.Fatalf("log = %s", out)
	}
	if strings.Contains(out, leakedKey) || strings.Contains(out, notifyKey) || strings.Contains(out, "X-Mock-Signature") {
		t.Fatalf("log leaked: %s", out)
	}
}

// CH-06 DUP_NOTIFY：更新报错不 POST、不加计数，1 秒后只补这一次。已经到上限则不再发。
func TestDupNotifyClaimFailureDoesNotSend(t *testing.T) {
	t.Run("update error", func(t *testing.T) {
		rig := newNotifyRig(t, dupSample(), func(http.ResponseWriter, *http.Request, []byte, int) {
			t.Error("sent HTTP")
		})
		rig.box.addErr = errors.New("db down")
		rig.n.Handle(context.Background(), rig.box.row)
		runOne(t, rig.clk, rig.sched)
		if rig.box.count() != 0 || len(rig.cap.list()) != 0 || rig.box.dupAdds != 5 {
			t.Fatalf("count=%d hits=%d dupAdds=%d", rig.box.count(), len(rig.cap.list()), rig.box.dupAdds)
		}
		dues := rig.sched.dues()
		if len(dues) != 5 {
			t.Fatalf("dues = %v", dues)
		}
		for _, due := range dues {
			if !due.Equal(rig.clk.Now().Add(time.Second)) {
				t.Fatalf("due = %s", due)
			}
		}
	})
	t.Run("already full", func(t *testing.T) {
		rig := newNotifyRig(t, dupSample(), func(http.ResponseWriter, *http.Request, []byte, int) {
			t.Error("sent HTTP")
		})
		rig.box.dupFull = true
		rig.n.Handle(context.Background(), rig.box.row)
		runOne(t, rig.clk, rig.sched)
		if rig.box.count() != 0 || len(rig.cap.list()) != 0 || rig.sched.len() != 0 || rig.box.dupAdds != 5 {
			t.Fatalf("count=%d hits=%d scheduled=%d dupAdds=%d", rig.box.count(), len(rig.cap.list()), rig.sched.len(), rig.box.dupAdds)
		}
	})
}
