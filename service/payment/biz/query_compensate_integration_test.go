//go:build integration

package biz

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/outbox"
	cbiz "github.com/yym108/pay-system/service/channel_mock/biz"         //nolint:depguard // PAY-05 验收要打到真实查单、关单与退款实现。生产代码不引用该包。
	cdal "github.com/yym108/pay-system/service/channel_mock/dal"         //nolint:depguard // 同上，只在本测试里读 mock_txn 并启动渠道服务。
	chandler "github.com/yym108/pay-system/service/channel_mock/handler" //nolint:depguard // 同上，进程内 HTTP 使用渠道 handler。
)

// 支付场景由金额末两位决定（CH-06）。
const (
	amountNormal        = money.Amount(10000)
	amountDropNotify    = money.Amount(10002)
	amountSlow          = money.Amount(10004)
	amountAcceptTimeout = money.Amount(10005)
	// 退款只认 FAIL 与 DROP_NOTIFY：3002 成功但不回调，1001 失败。
	refundDropNotify = money.Amount(3002)
	refundFail       = money.Amount(1001)
	// mockFinishDelay 是渠道自动完成的固定延迟，见 startQueryMock。
	mockFinishDelay = 3 * time.Second
)

// testQueryPolicy 是规则 5 的测试配置：每一档都是 2 秒。
func testQueryPolicy() QueryPolicy {
	plan := make([]time.Duration, 8)
	for i := range plan {
		plan[i] = 2 * time.Second
	}
	return QueryPolicy{Backoff: plan, Tail: 2 * time.Second}
}

type stepTask struct {
	due time.Time
	fn  func()
}

// stepSched 是手动调度器：推进假时钟后由测试调用 RunDue 执行到期任务。
// TimerScheduler 用真实 time.AfterFunc，推进假时钟不会触发。
type stepSched struct {
	clk   *clock.Fake
	mu    sync.Mutex
	tasks []stepTask
}

func (s *stepSched) Schedule(due time.Time, task func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, stepTask{due: due, fn: task})
}

func (s *stepSched) take(all bool) []func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clk.Now()
	var run []func()
	keep := s.tasks[:0]
	for _, task := range s.tasks {
		if all || !task.due.After(now) {
			run = append(run, task.fn)
			continue
		}
		keep = append(keep, task)
	}
	s.tasks = keep
	return run
}

// RunDue 执行到期任务，任务新排的到期任务也一并执行，返回执行个数。
func (s *stepSched) RunDue() int {
	n := 0
	for range 100 {
		fns := s.take(false)
		if len(fns) == 0 {
			break
		}
		for _, fn := range fns {
			fn()
			n++
		}
	}
	return n
}

// Flush 不看到期时间执行全部任务，用来在清理时放行 ACCEPT_TIMEOUT 的等待。
func (s *stepSched) Flush() {
	for range 20 {
		fns := s.take(true)
		if len(fns) == 0 {
			return
		}
		for _, fn := range fns {
			fn()
		}
	}
}

func (s *stepSched) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.tasks)
}

// queryMock 是进程内 channel_mock，按 txn_type:req_no 统计 /mockpay/v1/query 次数。
type queryMock struct {
	url   string
	db    *sqlx.DB
	sched *stepSched
	mu    sync.Mutex
	hits  map[string]int
}

func (m *queryMock) queries(txnType, reqNo string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hits[txnType+":"+reqNo]
}

func (m *queryMock) count(c *app.RequestContext) {
	if string(c.Path()) != channelQueryPath {
		return
	}
	var body channelQueryBody
	if json.Unmarshal(c.Request.Body(), &body) != nil {
		return
	}
	m.mu.Lock()
	m.hits[body.TxnType+":"+body.ReqNo]++
	m.mu.Unlock()
}

// startQueryMock 启动 channel_mock：自动完成固定 3 秒，调度器手动推进。
// notify 为 true 时装上真实回调发送器。
func startQueryMock(t *testing.T, clk *clock.Fake, notify bool) *queryMock {
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
			t.Errorf("close channel schema: %s", testkit.Redact(err))
		}
	})
	ids, err := idgen.New(5, clk)
	if err != nil {
		t.Fatal(err)
	}
	m := &queryMock{db: schema.DB, sched: &stepSched{clk: clk}, hits: map[string]int{}}
	store := cdal.New(schema.DB)
	ms := int(mockFinishDelay / time.Millisecond)
	svc := cbiz.New(store, ids, clk, m.sched, cbiz.Options{DelayMinMS: ms, DelayMaxMS: ms})
	if notify {
		svc.SetOnFinished(cbiz.NewNotifier(store, ids, clk, m.sched, []byte(testChannelKey)).Handle)
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithExitWaitTime(time.Second),
		server.WithDisablePrintRoute(true),
	)
	srv.Use(func(ctx context.Context, c *app.RequestContext) {
		m.count(c)
		c.Next(ctx)
	})
	chandler.New(svc, []byte(testChannelKey), clk).Register(srv)
	go func() { _ = srv.Run() }()
	waitListen(t, ln.Addr().String())
	t.Cleanup(func() {
		shut, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := srv.Shutdown(shut); err != nil {
			t.Errorf("shutdown channel http: %v", err)
		}
	})
	// 后注册先执行：先放行还挂着的受理，再关 HTTP。
	t.Cleanup(m.sched.Flush)
	m.url = "http://" + ln.Addr().String()
	return m
}

type queryEnv struct {
	payEnv
	clk  *clock.Fake
	base time.Time
	mock *queryMock
	p    *Payments
}

// newQueryEnv 用真实时间作基准的假时钟：网关按真实时钟验签，推进几十秒仍在 ±300 秒窗口内。
func newQueryEnv(t *testing.T, merchantID string, notify bool) queryEnv {
	t.Helper()
	env := openPay(t)
	base := clock.Real{}.Now().Truncate(time.Millisecond)
	clk := &clock.Fake{}
	clk.Set(base)
	mock := startQueryMock(t, clk, notify)
	p := newClosePayments(t, env, merchantID, base, clk)
	wireQuery(t, p, mock.url, clk, testChannelNotifyURL)
	return queryEnv{payEnv: env, clk: clk, base: base, mock: mock, p: p}
}

func wireQuery(t *testing.T, p *Payments, rawURL string, clk *clock.Fake, notifyURL string) *ChannelClient {
	t.Helper()
	client, err := NewChannelClient(ChannelClientConfig{BaseURL: rawURL, Key: []byte(testChannelKey), Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	p.UsePay(client, p, notifyURL)
	p.UseClose(client)
	p.UseQuery(client, testQueryPolicy())
	return client
}

func (e queryEnv) batch(t *testing.T) {
	t.Helper()
	if err := e.p.RunQueryBatch(e.ctx); err != nil {
		t.Fatalf("batch: %v", err)
	}
}

func (e queryEnv) pay(t *testing.T, merchantID, outTradeNo string, amount money.Amount) PaymentOrder {
	t.Helper()
	order := createCloseOrder(t, e.payEnv, e.p, merchantID, outTradeNo, amount)
	got, err := e.p.Pay(e.ctx, payReq(order))
	if err != nil || got.Status != payStatusPaying {
		t.Fatalf("pay = %+v err=%v", got, err)
	}
	return got
}

// paySucceeded 让一笔 NORMAL 支付经查单变成 SUCCESS，退款用例以它为原单。
func (e queryEnv) paySucceeded(t *testing.T, merchantID, outTradeNo string) PaymentOrder {
	t.Helper()
	order := e.pay(t, merchantID, outTradeNo, amountNormal)
	e.clk.Advance(payNextQueryDelay)
	e.mock.sched.RunDue()
	e.batch(t)
	if got := loadPayment(t, e.payEnv, merchantID, outTradeNo); got.status != payStatusSuccess {
		t.Fatalf("pay not SUCCESS: %+v", got)
	}
	return order
}

type queryTxnRow struct {
	status     string
	queryCount int
	lastQuery  sql.NullTime
	nextQuery  sql.NullTime
	version    uint64
	updated    time.Time
	txnNo      sql.NullString
}

func loadQueryTxn(t *testing.T, env payEnv, bizNo string) queryTxnRow {
	t.Helper()
	var row queryTxnRow
	err := env.db.QueryRowContext(env.ctx, `
SELECT status, query_count, last_query_at, next_query_at, version, updated_at, channel_txn_no
FROM channel_txn WHERE biz_no = ?`, bizNo).Scan(
		&row.status, &row.queryCount, &row.lastQuery, &row.nextQuery, &row.version, &row.updated, &row.txnNo,
	)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

type mockRow struct {
	count        int
	status       string
	channelTxnNo string
	amount       int64
	finished     sql.NullTime
}

func loadMockRow(t *testing.T, ctx context.Context, db *sqlx.DB, txnType, reqNo string) mockRow {
	t.Helper()
	var row mockRow
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mock_txn WHERE txn_type = ? AND req_no = ?`, txnType, reqNo).Scan(&row.count)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if row.count == 0 {
		return row
	}
	err = db.QueryRowContext(ctx, `
SELECT status, channel_txn_no, amount, finished_at
FROM mock_txn WHERE txn_type = ? AND req_no = ?`, txnType, reqNo).Scan(&row.status, &row.channelTxnNo, &row.amount, &row.finished)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

// PAY-05 验收 1（A4）：DROP_NOTIFY 不回调，测试配置下按每秒一次的节奏，30 秒内经查单变为 SUCCESS。
func TestQueryCompensateDropNotifySucceeds(t *testing.T) {
	const merchantID = "M100090"
	e := newQueryEnv(t, merchantID, true)
	order := e.pay(t, merchantID, "PAY05-DROP", amountDropNotify)
	var elapsed time.Duration
	for elapsed < 30*time.Second {
		e.clk.Advance(time.Second)
		elapsed += time.Second
		e.mock.sched.RunDue()
		e.batch(t)
		if loadPayment(t, e.payEnv, merchantID, "PAY05-DROP").status == payStatusSuccess {
			break
		}
	}
	stored := loadPayment(t, e.payEnv, merchantID, "PAY05-DROP")
	if stored.status != payStatusSuccess || elapsed > 30*time.Second || stored.amount != int64(amountDropNotify) {
		t.Fatalf("after %s order = %+v", elapsed, stored)
	}
	mock := loadMockRow(t, e.ctx, e.mock.db, txnTypePay, order.PayNo)
	if mock.status != payStatusSuccess || !mock.finished.Valid {
		t.Fatalf("mock = %+v", mock)
	}
	if !stored.paidAt.Valid || !stored.paidAt.Time.Equal(mock.finished.Time) {
		t.Fatalf("paid_at = %+v finished = %+v", stored.paidAt, mock.finished)
	}
	txn := loadAppliedTxn(t, e.payEnv, order.PayNo)
	if txn.status != payStatusSuccess || txn.notifyCount != 0 || txn.nextQuery.Valid || txn.txnNo.String != mock.channelTxnNo {
		t.Fatalf("txn = %+v", txn)
	}
	events := loadOutbox(t, e.payEnv, order.PayNo)
	if len(events) != 1 || events[0].eventType != outbox.EventPaymentSucceeded {
		t.Fatalf("outbox = %+v", events)
	}
	if n := e.mock.queries(txnTypePay, order.PayNo); n != 1 {
		t.Fatalf("queries = %d", n)
	}
	// 终态之后不再抢占这条流水。
	e.clk.Advance(time.Hour)
	e.batch(t)
	if n := e.mock.queries(txnTypePay, order.PayNo); n != 1 {
		t.Fatalf("queried after terminal: %d", n)
	}
}

// slowQuerier 在调用真实渠道前等 gate 关闭，模拟渠道慢；entered 按进入顺序记下 req_no。
type slowQuerier struct {
	inner   channelQuerier
	gate    chan struct{}
	mu      sync.Mutex
	entered []string
}

func (s *slowQuerier) QueryTxn(ctx context.Context, txnType, reqNo string) (channelQueryResult, error) {
	s.mu.Lock()
	s.entered = append(s.entered, reqNo)
	s.mu.Unlock()
	select {
	case <-s.gate:
	case <-ctx.Done():
		return channelQueryResult{}, nil
	}
	return s.inner.QueryTxn(ctx, txnType, reqNo)
}

func (s *slowQuerier) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.entered...)
}

// PAY-05 规则 1、2：渠道慢使一个批次超过退避间隔。排在并发槽之后、还没开始的任务发现抢占时写的
// next_query_at 已到期（另一个实例已接手进入下一档），跳过不查；同一行同一档仍只查一次。
func TestQueryCompensateSlowBatchSkipsReclaimedRows(t *testing.T) {
	const merchantID = "M100097"
	e := newQueryEnv(t, merchantID, false)
	orders := make([]PaymentOrder, queryConcurrency+1)
	for i := range orders {
		orders[i] = e.pay(t, merchantID, fmt.Sprintf("PAY05-SLOW-%d", i), amountNeverFinish)
	}
	client := wireQuery(t, e.p, e.mock.url, e.clk, testChannelNotifyURL)
	slow := &slowQuerier{inner: client, gate: make(chan struct{})}
	e.p.UseQuery(slow, testQueryPolicy())
	ids, err := idgen.New(9, e.clk)
	if err != nil {
		t.Fatal(err)
	}
	other := NewPayments(NewMerchants(e.db, nil, e.clk, 0), ids, e.clk, "merchant-demo", e.db)
	wireQuery(t, other, e.mock.url, e.clk, testChannelNotifyURL)

	// 第 1 档：本实例抢占全部 11 行，10 个任务卡在慢渠道上，第 11 个在等并发槽。
	e.clk.Advance(payNextQueryDelay)
	done := make(chan error, 1)
	go func() { done <- e.p.RunQueryBatch(e.ctx) }()
	if !pollFor(5*time.Second, func() bool { return len(slow.seen()) == queryConcurrency }) {
		close(slow.gate)
		t.Fatalf("entered = %d", len(slow.seen()))
	}
	inFlight := map[string]bool{}
	for _, no := range slow.seen() {
		inFlight[no] = true
	}

	// 第 1 档的间隔到了：另一个实例接手全部 11 行，查一次，进入第 2 档。
	e.clk.Advance(queryDelay(testQueryPolicy(), 1))
	if err := other.RunQueryBatch(e.ctx); err != nil {
		close(slow.gate)
		t.Fatal(err)
	}
	before := queryClaimSkipped.Load()
	close(slow.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := queryClaimSkipped.Load(); got != before+1 {
		t.Fatalf("skipped = %d, want %d", got, before+1)
	}
	if n := len(slow.seen()); n != queryConcurrency {
		t.Fatalf("slow instance started %d tasks", n)
	}
	queued := 0
	for _, order := range orders {
		// 卡住的 10 行：本实例第 1 档一次、另一个实例第 2 档一次。等槽的那行只有另一个实例查过。
		want := 1
		if inFlight[order.PayNo] {
			want = 2
		} else {
			queued++
		}
		if n := e.mock.queries(txnTypePay, order.PayNo); n != want {
			t.Fatalf("%s queried %d times, want %d", order.PayNo, n, want)
		}
		if row := loadQueryTxn(t, e.payEnv, order.PayNo); row.status != txnStatusProcessing || row.queryCount != 2 {
			t.Fatalf("%s row = %+v", order.PayNo, row)
		}
	}
	if queued != 1 {
		t.Fatalf("queued rows = %d", queued)
	}
}

// PAY-05 验收 2：两个 payment 实例、四个并发批次，同一条记录在同一档只被查询一次。
func TestQueryCompensateTwoInstancesQueryOncePerStep(t *testing.T) {
	const merchantID = "M100091"
	e := newQueryEnv(t, merchantID, false)
	orders := make([]PaymentOrder, 6)
	for i := range orders {
		orders[i] = e.pay(t, merchantID, fmt.Sprintf("PAY05-TWO-%d", i), amountNeverFinish)
	}
	ids, err := idgen.New(8, e.clk)
	if err != nil {
		t.Fatal(err)
	}
	other := NewPayments(NewMerchants(e.db, nil, e.clk, 0), ids, e.clk, "merchant-demo", e.db)
	wireQuery(t, other, e.mock.url, e.clk, testChannelNotifyURL)
	instances := []*Payments{e.p, other, e.p, other}

	policy := testQueryPolicy()
	advance := payNextQueryDelay
	for step := 1; step <= 3; step++ {
		e.clk.Advance(advance)
		now := e.clk.Now()
		start := make(chan struct{})
		var g errgroup.Group
		for _, inst := range instances {
			g.Go(func() error {
				<-start
				return inst.RunQueryBatch(e.ctx)
			})
		}
		close(start)
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		advance = queryDelay(policy, step)
		for _, order := range orders {
			if n := e.mock.queries(txnTypePay, order.PayNo); n != step {
				t.Fatalf("step %d %s queried %d times", step, order.PayNo, n)
			}
			row := loadQueryTxn(t, e.payEnv, order.PayNo)
			if row.status != txnStatusProcessing || row.queryCount != step || !row.lastQuery.Valid || !row.lastQuery.Time.Equal(now) ||
				!row.nextQuery.Valid || !row.nextQuery.Time.Equal(now.Add(advance)) {
				t.Fatalf("step %d row = %+v now %s", step, row, now)
			}
		}
	}
}

// PAY-05 验收 3：Pay 停在 pay.after_tx1（渠道无记录）。60 秒内查无此单不处理，超过 60 秒关单：
// 订单 CLOSED，本地流水 CLOSED，渠道侧写入 CLOSED 的防悬挂行（amount 0），不写 outbox。
func TestQueryCompensateClosesWhenChannelHasNoRow(t *testing.T) {
	const merchantID = "M100092"
	e := newQueryEnv(t, merchantID, false)
	order := createCloseOrder(t, e.payEnv, e.p, merchantID, "PAY05-LOST", amountNormal)
	entered := make(chan struct{})
	if err := fault.Init(faultPayAfterTx1, fault.WithKill(func() {
		close(entered)
		runtime.Goexit()
	})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fault.Init(""); err != nil {
			t.Errorf("clear fault: %v", err)
		}
	})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		_, _ = e.p.Pay(e.ctx, payReq(order))
	}()
	<-entered
	<-exited
	if err := fault.Init(""); err != nil {
		t.Fatal(err)
	}
	if got := loadPayment(t, e.payEnv, merchantID, "PAY05-LOST"); got.status != payStatusPaying {
		t.Fatalf("after crash order = %+v", got)
	}

	e.clk.Advance(payNextQueryDelay)
	e.batch(t)
	if got := loadPayment(t, e.payEnv, merchantID, "PAY05-LOST"); got.status != payStatusPaying {
		t.Fatalf("closed before 60s: %+v", got)
	}
	if row := loadMockRow(t, e.ctx, e.mock.db, txnTypePay, order.PayNo); row.count != 0 {
		t.Fatalf("mock before 60s = %+v", row)
	}

	e.clk.Set(e.base.Add(queryNotFoundGrace + time.Second))
	e.batch(t)
	stored := loadPayment(t, e.payEnv, merchantID, "PAY05-LOST")
	if stored.status != payStatusClosed || !stored.closedAt.Valid {
		t.Fatalf("order = %+v", stored)
	}
	if count, status := loadLocalTxnStatus(t, e.payEnv, order.PayNo); count != 1 || status != payStatusClosed {
		t.Fatalf("channel_txn count=%d status=%s", count, status)
	}
	mock := loadMockRow(t, e.ctx, e.mock.db, txnTypePay, order.PayNo)
	if mock.count != 1 || mock.status != payStatusClosed || mock.amount != 0 {
		t.Fatalf("mock = %+v", mock)
	}
	if n := len(loadOutbox(t, e.payEnv, order.PayNo)); n != 0 {
		t.Fatalf("outbox = %d", n)
	}
	if n := e.mock.queries(txnTypePay, order.PayNo); n != 2 {
		t.Fatalf("queries = %d", n)
	}
}

// PAY-05 验收 4：SLOW 先经查单成功；finished_at + 30 秒的回调经 GW-06 到达，按重复处理：
// 200 SUCCESS、notify_count + 1、outbox 仍只有 1 条，渠道不再重试。
func TestQueryCompensateSlowThenDuplicateCallback(t *testing.T) {
	const merchantID = "M100093"
	e := newQueryEnv(t, merchantID, true)
	wireQuery(t, e.p, e.mock.url, e.clk, startGateway(t, e.p))
	order := e.pay(t, merchantID, "PAY05-SLOW", amountSlow)

	e.clk.Advance(mockFinishDelay)
	e.mock.sched.RunDue()
	mock := loadMockRow(t, e.ctx, e.mock.db, txnTypePay, order.PayNo)
	if mock.status != payStatusSuccess || !mock.finished.Valid {
		t.Fatalf("mock = %+v", mock)
	}
	e.clk.Set(e.base.Add(payNextQueryDelay))
	e.batch(t)
	if got := loadPayment(t, e.payEnv, merchantID, "PAY05-SLOW"); got.status != payStatusSuccess {
		t.Fatalf("order = %+v", got)
	}
	before := loadAppliedTxn(t, e.payEnv, order.PayNo)
	if before.status != payStatusSuccess || before.notifyCount != 0 {
		t.Fatalf("txn after query = %+v", before)
	}
	if n := len(loadOutbox(t, e.payEnv, order.PayNo)); n != 1 {
		t.Fatalf("outbox after query = %d", n)
	}
	if row := loadMockTxn(t, e.ctx, e.mock.db, order.PayNo); row.notifySent != 0 {
		t.Fatalf("callback before finished+30s: %+v", row)
	}

	e.clk.Set(mock.finished.Time.Add(30 * time.Second))
	if ran := e.mock.sched.RunDue(); ran != 1 {
		t.Fatalf("ran = %d", ran)
	}
	after := loadAppliedTxn(t, e.payEnv, order.PayNo)
	if after.status != payStatusSuccess || after.notifyCount != 1 {
		t.Fatalf("txn after callback = %+v", after)
	}
	events := loadOutbox(t, e.payEnv, order.PayNo)
	if len(events) != 1 || events[0].eventType != outbox.EventPaymentSucceeded {
		t.Fatalf("outbox = %+v", events)
	}
	if row := loadMockTxn(t, e.ctx, e.mock.db, order.PayNo); row.notifySent != 1 || row.status != payStatusSuccess {
		t.Fatalf("mock after callback = %+v", row)
	}
	if n := e.mock.sched.Pending(); n != 0 {
		t.Fatalf("channel retries pending = %d", n)
	}
}

// PAY-05 验收 5（由 PAY-03 移来）：ACCEPT_TIMEOUT 受理挂住 5 秒，Pay 在 3 秒以上、5 秒以内返回 PAYING；
// 查单得到 SUCCESS，订单最终 SUCCESS，并回填渠道流水号。
func TestQueryCompensateAcceptTimeoutSucceeds(t *testing.T) {
	const merchantID = "M100094"
	e := newQueryEnv(t, merchantID, false)
	order := createCloseOrder(t, e.payEnv, e.p, merchantID, "PAY05-ACCEPT", amountAcceptTimeout)
	wall := clock.Real{}
	start := wall.Now()
	got, err := e.p.Pay(e.ctx, payReq(order))
	took := wall.Now().Sub(start)
	if err != nil || got.Status != payStatusPaying {
		t.Fatalf("pay = %+v err=%v", got, err)
	}
	if took < 3*time.Second || took >= 5*time.Second {
		t.Fatalf("pay took %s", took)
	}
	if row := loadQueryTxn(t, e.payEnv, order.PayNo); row.txnNo.Valid {
		t.Fatalf("channel_txn_no before query = %+v", row)
	}

	e.clk.Advance(5 * time.Second)
	e.mock.sched.RunDue()
	e.clk.Set(e.base.Add(payNextQueryDelay))
	e.batch(t)
	stored := loadPayment(t, e.payEnv, merchantID, "PAY05-ACCEPT")
	if stored.status != payStatusSuccess || stored.amount != int64(amountAcceptTimeout) {
		t.Fatalf("order = %+v", stored)
	}
	mock := loadMockRow(t, e.ctx, e.mock.db, txnTypePay, order.PayNo)
	row := loadQueryTxn(t, e.payEnv, order.PayNo)
	if row.status != payStatusSuccess || !row.txnNo.Valid || row.txnNo.String != mock.channelTxnNo {
		t.Fatalf("txn = %+v mock = %+v", row, mock)
	}
	if n := len(loadOutbox(t, e.payEnv, order.PayNo)); n != 1 {
		t.Fatalf("outbox = %d", n)
	}
}

// PAY-05 验收 6：退款流水查得 SUCCESS / FAILED 时调用 ApplyRefundResult，退款单进入相同终态。
func TestQueryCompensateRefundTerminal(t *testing.T) {
	const merchantID = "M100095"
	e := newQueryEnv(t, merchantID, false)
	order := e.paySucceeded(t, merchantID, "PAY05-REF")
	client := wireQuery(t, e.p, e.mock.url, e.clk, testChannelNotifyURL)
	e.p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, client, nil, testChannelNotifyURL)
	ok, err := e.p.CreateRefund(e.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R05_OK", Amount: refundDropNotify})
	if err != nil || ok.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", ok, err)
	}
	failed, err := e.p.CreateRefund(e.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R05_FAIL", Amount: refundFail})
	if err != nil || failed.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", failed, err)
	}

	e.clk.Advance(mockFinishDelay)
	e.mock.sched.RunDue()
	e.clk.Advance(payNextQueryDelay - mockFinishDelay)
	e.batch(t)

	okRow := loadRefundRow(t, e.payEnv, ok.RefundNo)
	okMock := loadMockRow(t, e.ctx, e.mock.db, txnTypeRefund, ok.RefundNo)
	if okRow.status != payResultSuccess || !okRow.succeededAt.Valid || !okMock.finished.Valid || !okRow.succeededAt.Time.Equal(okMock.finished.Time) {
		t.Fatalf("ok refund = %+v mock = %+v", okRow, okMock)
	}
	if events := loadOutbox(t, e.payEnv, ok.RefundNo); len(events) != 1 || events[0].eventType != "refund.succeeded" {
		t.Fatalf("ok outbox = %+v", events)
	}
	failRow := loadRefundRow(t, e.payEnv, failed.RefundNo)
	if failRow.status != refundStatusFailed || failRow.failCode.String != failCodeChannelFailed {
		t.Fatalf("failed refund = %+v", failRow)
	}
	if events := loadOutbox(t, e.payEnv, failed.RefundNo); len(events) != 1 || events[0].eventType != "refund.failed" {
		t.Fatalf("failed outbox = %+v", events)
	}
	if got := payRefundedAmount(t, e.payEnv, order.PayNo); got != int64(refundDropNotify) {
		t.Fatalf("refunded = %d", got)
	}
	for _, no := range []string{ok.RefundNo, failed.RefundNo} {
		row := loadQueryTxn(t, e.payEnv, no)
		if row.status == txnStatusProcessing || row.nextQuery.Valid || row.queryCount != 1 {
			t.Fatalf("%s txn = %+v", no, row)
		}
		if n := e.mock.queries(txnTypeRefund, no); n != 1 {
			t.Fatalf("%s queries = %d", no, n)
		}
	}
}

// PAY-05 规则 3：退款流水查无此单、创建超过 60 秒时按 REF-02 的同一段代码重新提交；之后查单推进为 SUCCESS。
func TestQueryCompensateRefundNotFoundResubmits(t *testing.T) {
	const merchantID = "M100096"
	e := newQueryEnv(t, merchantID, false)
	order := e.paySucceeded(t, merchantID, "PAY05-REF-LOST")
	client := wireQuery(t, e.p, e.mock.url, e.clk, testChannelNotifyURL)
	// 受理结果未知且请求没有到达渠道：退款 PROCESSING，流水号为空，渠道无记录。
	e.p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, &fakeRefunder{}, nil, testChannelNotifyURL)
	created := e.clk.Now()
	refund, err := e.p.CreateRefund(e.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R05_LOST", Amount: 3000})
	if err != nil || refund.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", refund, err)
	}
	e.p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, client, nil, testChannelNotifyURL)

	e.clk.Advance(payNextQueryDelay)
	e.batch(t)
	if row := loadMockRow(t, e.ctx, e.mock.db, txnTypeRefund, refund.RefundNo); row.count != 0 {
		t.Fatalf("resubmitted before 60s: %+v", row)
	}

	e.clk.Set(created.Add(queryNotFoundGrace + time.Second))
	e.batch(t)
	mock := loadMockRow(t, e.ctx, e.mock.db, txnTypeRefund, refund.RefundNo)
	if mock.count != 1 || mock.status != txnStatusProcessing || mock.amount != 3000 {
		t.Fatalf("mock after resubmit = %+v", mock)
	}
	if row := loadQueryTxn(t, e.payEnv, refund.RefundNo); row.txnNo.String != mock.channelTxnNo || row.status != txnStatusProcessing {
		t.Fatalf("txn after resubmit = %+v", row)
	}

	e.clk.Advance(mockFinishDelay)
	e.mock.sched.RunDue()
	e.batch(t)
	if row := loadRefundRow(t, e.payEnv, refund.RefundNo); row.status != payResultSuccess {
		t.Fatalf("refund = %+v", row)
	}
	if events := loadOutbox(t, e.payEnv, refund.RefundNo); len(events) != 1 || events[0].eventType != "refund.succeeded" {
		t.Fatalf("outbox = %+v", events)
	}
	if n := e.mock.queries(txnTypeRefund, refund.RefundNo); n != 3 {
		t.Fatalf("queries = %d", n)
	}
	// 退款成功不释放额度：refunded_amount 仍是这笔退款的金额。
	if got := payRefundedAmount(t, e.payEnv, order.PayNo); got != 3000 {
		t.Fatalf("refunded = %d", got)
	}
}

// PAY-05 规则 3：退款流水查无此单、重提时渠道受理拒绝（原支付在渠道侧已不是 SUCCESS）：
// 退款单 FAILED（CHANNEL_REJECTED，fail_msg 为渠道 reason），refunded_amount 只恢复一次，outbox 恰好 1 条 refund.failed。
func TestQueryCompensateRefundResubmitRejected(t *testing.T) {
	const merchantID = "M100098"
	e := newQueryEnv(t, merchantID, false)
	order := e.paySucceeded(t, merchantID, "PAY05-REF-REJ")
	client := wireQuery(t, e.p, e.mock.url, e.clk, testChannelNotifyURL)
	e.p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, &fakeRefunder{}, nil, testChannelNotifyURL)
	refund, err := e.p.CreateRefund(e.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R05_REJ", Amount: 3000})
	if err != nil || refund.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", refund, err)
	}
	if got := payRefundedAmount(t, e.payEnv, order.PayNo); got != 3000 {
		t.Fatalf("refunded after create = %d", got)
	}
	e.p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, client, nil, testChannelNotifyURL)
	// 渠道侧原支付改成 FAILED，重提时渠道返回 REJECTED（ORIG_NOT_SUCCESS）。
	if _, err := e.mock.db.ExecContext(e.ctx,
		`UPDATE mock_txn SET status = 'FAILED' WHERE txn_type = 'PAY' AND req_no = ?`, order.PayNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}

	e.clk.Advance(queryNotFoundGrace + time.Second)
	e.batch(t)
	row := loadRefundRow(t, e.payEnv, refund.RefundNo)
	if row.status != refundStatusFailed || row.failCode.String != failCodeChannelRejected || row.failMsg.String != "ORIG_NOT_SUCCESS" {
		t.Fatalf("refund = %+v", row)
	}
	if mock := loadMockRow(t, e.ctx, e.mock.db, txnTypeRefund, refund.RefundNo); mock.count != 0 {
		t.Fatalf("mock refund row = %+v", mock)
	}
	txn := loadQueryTxn(t, e.payEnv, refund.RefundNo)
	if txn.status != payStatusFailed || txn.nextQuery.Valid {
		t.Fatalf("txn = %+v", txn)
	}

	// 之后的批次不再抢占这条流水，额度不会被再释放一次。
	for range 3 {
		e.clk.Advance(time.Hour)
		e.batch(t)
	}
	if got := payRefundedAmount(t, e.payEnv, order.PayNo); got != 0 {
		t.Fatalf("refunded = %d", got)
	}
	if events := loadOutbox(t, e.payEnv, refund.RefundNo); len(events) != 1 || events[0].eventType != "refund.failed" {
		t.Fatalf("outbox = %+v", events)
	}
	if again := loadRefundRow(t, e.payEnv, refund.RefundNo); again.version != row.version || !again.updated.Equal(row.updated) {
		t.Fatalf("refund changed after terminal: %+v -> %+v", row, again)
	}
	if n := e.mock.queries(txnTypeRefund, refund.RefundNo); n != 1 {
		t.Fatalf("queries = %d", n)
	}
}

type seedTxn struct {
	bizNo      string
	status     string
	queryCount int
	nextQuery  any
	createdAt  time.Time
}

func insertSeedTxn(t *testing.T, env payEnv, s seedTxn, now time.Time) {
	t.Helper()
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO channel_txn (
  channel, txn_type, biz_no, channel_txn_no, amount, status,
  notify_count, query_count, next_query_at, version, created_at, updated_at
) VALUES ('mockpay', 'PAY', ?, NULL, 100, ?, 0, ?, ?, 0, ?, ?)`,
		s.bizNo, s.status, s.queryCount, s.nextQuery, s.createdAt, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

// PAY-05 规则 1、4：抢占按 next_query_at 取到期的 PROCESSING，每批最多 100 行；
// query_count + 1、写 last_query_at、按新档位推后 next_query_at，不改 version；
// 创建超过 24 小时的行置 next_query_at = NULL 并计数，不再查单；恰好 24 小时仍查。
func TestQueryClaimBatchMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	policy := QueryPolicy{Backoff: []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second}, Tail: 9 * time.Second}
	young := now.Add(-time.Hour)
	seeds := []seedTxn{
		{bizNo: "PA_EXHAUSTED", status: txnStatusProcessing, queryCount: 30, nextQuery: now.Add(-300 * time.Second), createdAt: now.Add(-queryGiveUpAfter - time.Millisecond)},
		{bizNo: "PA_EDGE_24H", status: txnStatusProcessing, queryCount: 29, nextQuery: now.Add(-299 * time.Second), createdAt: now.Add(-queryGiveUpAfter)},
		{bizNo: "PA_NOT_DUE", status: txnStatusProcessing, nextQuery: now.Add(time.Millisecond), createdAt: young},
		{bizNo: "PA_NULL_NEXT", status: txnStatusProcessing, nextQuery: nil, createdAt: young},
		{bizNo: "PA_DONE", status: payStatusSuccess, nextQuery: now.Add(-time.Hour), createdAt: young},
	}
	for i := range 105 {
		seeds = append(seeds, seedTxn{
			bizNo: fmt.Sprintf("PA_DUE_%03d", i), status: txnStatusProcessing, queryCount: i % 4,
			nextQuery: now.Add(-time.Duration(105-i) * time.Second), createdAt: young,
		})
	}
	for _, s := range seeds {
		insertSeedTxn(t, env, s, now)
	}
	stale := now.Add(-time.Hour)

	q := &fakeQuerier{res: found(txnStatusProcessing, 100, true, time.Time{}, "")}
	ids, err := idgen.New(7, clk)
	if err != nil {
		t.Fatal(err)
	}
	p := NewPayments(nil, ids, clk, "", env.db)
	p.UseQuery(q, policy)
	beforeExhausted := queryExhausted.Load()
	if err := p.RunQueryBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	calls, _, _ := q.snapshot()
	if len(calls) != queryBatchSize-1 {
		t.Fatalf("first batch queried %d", len(calls))
	}
	if queryExhausted.Load() != beforeExhausted+1 {
		t.Fatalf("exhausted = %d, before %d", queryExhausted.Load(), beforeExhausted)
	}
	ex := loadQueryTxn(t, env, "PA_EXHAUSTED")
	if ex.nextQuery.Valid || ex.queryCount != 30 || ex.lastQuery.Valid || !ex.updated.Equal(now) || ex.status != txnStatusProcessing {
		t.Fatalf("exhausted row = %+v", ex)
	}
	edge := loadQueryTxn(t, env, "PA_EDGE_24H")
	if edge.queryCount != 30 || !edge.nextQuery.Valid || !edge.nextQuery.Time.Equal(now.Add(9*time.Second)) {
		t.Fatalf("24h edge row = %+v", edge)
	}
	for i := range 105 {
		bizNo := fmt.Sprintf("PA_DUE_%03d", i)
		row := loadQueryTxn(t, env, bizNo)
		if i >= 98 {
			if row.queryCount != i%4 || row.lastQuery.Valid || !row.updated.Equal(stale) {
				t.Fatalf("%s claimed beyond limit: %+v", bizNo, row)
			}
			continue
		}
		want := i%4 + 1
		if row.queryCount != want || !row.lastQuery.Valid || !row.lastQuery.Time.Equal(now) ||
			!row.nextQuery.Valid || !row.nextQuery.Time.Equal(now.Add(queryDelay(policy, want))) ||
			row.version != 0 || !row.updated.Equal(now) {
			t.Fatalf("%s row = %+v", bizNo, row)
		}
	}
	for _, bizNo := range []string{"PA_NOT_DUE", "PA_NULL_NEXT", "PA_DONE"} {
		if row := loadQueryTxn(t, env, bizNo); row.queryCount != 0 || row.lastQuery.Valid || !row.updated.Equal(stale) {
			t.Fatalf("%s touched: %+v", bizNo, row)
		}
	}

	if err := p.RunQueryBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	if calls, _, _ = q.snapshot(); len(calls) != queryBatchSize-1+7 {
		t.Fatalf("second batch total %d", len(calls))
	}
	if err := p.RunQueryBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	if calls, _, _ = q.snapshot(); len(calls) != queryBatchSize-1+7 {
		t.Fatalf("third batch total %d", len(calls))
	}
	if queryExhausted.Load() != beforeExhausted+1 {
		t.Fatalf("exhausted counted twice: %d", queryExhausted.Load())
	}
}
