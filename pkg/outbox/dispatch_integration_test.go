//go:build integration

package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/mq"
)

func openOutboxFor(t *testing.T, limit time.Duration) (context.Context, *sqlx.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	schema, err := my.OpenSchema(ctx, "payment")
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
	return ctx, schema.DB
}

func startBoxKafka(t *testing.T, ctx context.Context) *testkit.Kafka {
	t.Helper()
	k, err := testkit.StartKafka(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { //nolint:contextcheck // 清理发生在测试 ctx 取消之后。
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := k.Close(cctx); err != nil {
			t.Errorf("close kafka: %s", testkit.Redact(err))
		}
	})
	return k
}

func openProducer(t *testing.T, broker string) *mq.Producer {
	t.Helper()
	p, err := mq.OpenProducer([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

type outboxRow struct {
	status string
	retry  int
	next   time.Time
	sent   sql.NullTime
	last   sql.NullString
}

func insertPending(t *testing.T, ctx context.Context, db *sqlx.DB, eventID, topic string, payload []byte, next time.Time, retry int) uint64 {
	t.Helper()
	res, err := db.ExecContext(ctx, `
INSERT INTO outbox (
    event_id, event_type, topic, msg_key, biz_no, payload, status, retry_count, next_retry_at, created_at, updated_at
) VALUES (?, 'payment.succeeded', ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?)`,
		eventID, topic, eventID, eventID, payload, retry, next, next, next)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if affected != 1 {
		t.Fatalf("inserted %d", affected)
	}
	var id uint64
	if err = db.QueryRowContext(ctx, `SELECT id FROM outbox WHERE event_id = ?`, eventID).Scan(&id); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return id
}

func loadEvent(t *testing.T, ctx context.Context, db *sqlx.DB, eventID string) outboxRow {
	t.Helper()
	var row outboxRow
	err := db.QueryRowContext(ctx, `
SELECT status, retry_count, next_retry_at, sent_at, last_error
FROM outbox WHERE event_id = ?`, eventID).Scan(&row.status, &row.retry, &row.next, &row.sent, &row.last)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

func payloadWithTrace(eventID string) []byte {
	return []byte(`{"event_id":"` + eventID + `","event_type":"payment.succeeded","version":1,"trace_context":{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},"data":{}}`)
}

func countFault(t *testing.T) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	if err := fault.Init("outbox.after_send", fault.WithKill(func() { n.Add(1) })); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fault.Init(""); err != nil {
			t.Errorf("reset fault: %v", err)
		}
	})
	return &n
}

type okSender struct{}

func (okSender) Send(_ context.Context, batch []Delivery) []error {
	if len(batch) == 0 {
		return nil
	}
	return make([]error, len(batch))
}

type failSender struct{ err error }

func (f failSender) Send(_ context.Context, batch []Delivery) []error {
	if len(batch) == 0 {
		return nil
	}
	out := make([]error, len(batch))
	for i := range out {
		out[i] = f.err
	}
	return out
}

type gateSender struct {
	entered      chan struct{}
	release      <-chan struct{}
	once         sync.Once
	err          error
	beforeReturn func()
}

func (g *gateSender) Send(ctx context.Context, batch []Delivery) []error {
	g.once.Do(func() { close(g.entered) })
	select {
	case <-g.release:
	case <-ctx.Done():
		return fillErr(batch, ctx.Err())
	}
	if g.beforeReturn != nil {
		g.beforeReturn()
	}
	if g.err != nil {
		return fillErr(batch, g.err)
	}
	return make([]error, len(batch))
}

func fillErr(batch []Delivery, err error) []error {
	if len(batch) == 0 {
		return nil
	}
	out := make([]error, len(batch))
	for i := range out {
		out[i] = err
	}
	return out
}

func waitCh(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal(what)
	}
}

func createTopic(t *testing.T, broker, name string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal("kafka client")
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Version = 7
	req.TimeoutMillis = 15000
	req.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             name,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal("create topic")
	}
	for _, topic := range resp.Topics {
		if topic.ErrorCode == 0 {
			continue
		}
		if errors.Is(kerr.ErrorForCode(topic.ErrorCode), kerr.TopicAlreadyExists) {
			continue
		}
		t.Fatalf("create topic code %d", topic.ErrorCode)
	}
}

func consumeCounts(t *testing.T, broker, topic string, want int) map[string]int {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal("kafka client")
	}
	defer cl.Close()
	seen := map[string]int{}
	readCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for len(seen) < want && readCtx.Err() == nil {
		fetches := cl.PollFetches(readCtx)
		fetches.EachRecord(func(rec *kgo.Record) {
			noteRecord(t, seen, rec)
		})
	}
	extra, extraCancel := context.WithTimeout(context.Background(), 2*time.Second)
	fetches := cl.PollFetches(extra)
	extraCancel()
	fetches.EachRecord(func(rec *kgo.Record) {
		noteRecord(t, seen, rec)
	})
	return seen
}

func noteRecord(t *testing.T, seen map[string]int, rec *kgo.Record) {
	t.Helper()
	if !hasHeader(rec, "event_id") || !hasHeader(rec, "event_type") || !hasHeader(rec, "traceparent") {
		t.Fatal("headers incomplete")
	}
	id := headerValue(rec, "event_id")
	if id == "" || headerValue(rec, "event_type") == "" {
		t.Fatal("empty event header")
	}
	seen[id]++
}

func hasHeader(rec *kgo.Record, key string) bool {
	for _, h := range rec.Headers {
		if h.Key == key {
			return true
		}
	}
	return false
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// MQ-02 验收 1：Kafka 停止 2 分钟期间事件积压，恢复后全部投递，pay_outbox_pending 回落到 0。
func TestMQ02Accept1KafkaDownTwoMinutes(t *testing.T) {
	ctx, db := openOutboxFor(t, 8*time.Minute)
	k := startBoxKafka(t, ctx)
	const topic = "pay.mq02.accept1"
	const n = 3
	createTopic(t, k.Broker, topic)
	producer := openProducer(t, k.Broker)
	at, clk := clockAt()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("EV%019d", i+1)
		insertPending(t, ctx, db, ids[i], topic, payloadWithTrace(ids[i]), at, 0)
	}
	d := NewDispatcher(db, NewMQSender(producer), clk)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := k.Stop(stopCtx)
	stopCancel()
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err = d.RunBatch(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	for _, id := range ids {
		row := loadEvent(t, ctx, db, id)
		if row.status != statusPending || row.retry != 1 || row.sent.Valid {
			t.Fatalf("after outage round status=%s retry=%d", row.status, row.retry)
		}
		if !row.next.Equal(at.Add(2 * time.Second)) {
			t.Fatalf("backoff next = %s", row.next)
		}
		if strings.Contains(row.last.String, "127.0.0.1") || strings.Contains(row.last.String, "dial") {
			t.Fatal("last_error contains an address")
		}
	}
	if err = d.RefreshGauges(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if Pending(topic) <= 0 {
		t.Fatalf("pending = %d", Pending(topic))
	}
	// 这两分钟是故障窗口。期间不再领取，避免退避被打到 5 分钟。
	window := time.NewTimer(2 * time.Minute)
	select {
	case <-window.C:
	case <-ctx.Done():
		window.Stop()
		t.Fatal("context ended before the outage window")
	}
	for _, id := range ids {
		row := loadEvent(t, ctx, db, id)
		if row.status != statusPending || row.retry != 1 {
			t.Fatalf("during window status=%s retry=%d", row.status, row.retry)
		}
	}
	startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	err = k.Start(startCtx)
	startCancel()
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	waitProducer(t, producer)
	clk.Advance(2 * time.Second)
	for try := 0; try < 5; try++ {
		var left int
		if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE status <> 'SENT'`).Scan(&left); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if left == 0 {
			break
		}
		var due time.Time
		if err = db.QueryRowContext(ctx, `SELECT MIN(next_retry_at) FROM outbox WHERE status = 'PENDING'`).Scan(&due); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if clk.Now().Before(due) {
			clk.Set(due)
		}
		if err = d.RunBatch(ctx); err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
	for _, id := range ids {
		row := loadEvent(t, ctx, db, id)
		if row.status != statusSent || row.retry != 1 || !row.sent.Valid {
			t.Fatalf("recovered status=%s retry=%d sent=%v", row.status, row.retry, row.sent.Valid)
		}
	}
	seen := consumeCounts(t, k.Broker, topic, n)
	if len(seen) != n {
		t.Fatalf("kafka events = %d", len(seen))
	}
	for _, id := range ids {
		if seen[id] != 1 {
			t.Fatalf("%s count = %d", id, seen[id])
		}
	}
	if err = d.RefreshGauges(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if PendingTotal() != 0 {
		t.Fatalf("pending total = %d", PendingTotal())
	}
}

func waitProducer(t *testing.T, p *mq.Producer) {
	t.Helper()
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		pingCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := p.Ready(pingCtx)
		cancel()
		if err == nil {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("producer not ready")
		case <-tick.C:
		}
	}
}

// MQ-02 验收 2：不注入故障，两个投递器并发运行 1 万条事件，每个 event_id 恰好 1 次，全部 SENT。
func TestMQ02Accept2TwoDispatchersOnce(t *testing.T) {
	ctx, db := openOutboxFor(t, 6*time.Minute)
	k := startBoxKafka(t, ctx)
	const topic = "pay.mq02.accept2"
	const n = 10000
	createTopic(t, k.Broker, topic)
	at, clk := clockAt()
	insertPendingBulk(t, ctx, db, topic, n, at)
	d1 := NewDispatcher(db, NewMQSender(openProducer(t, k.Broker)), clk)
	d2 := NewDispatcher(db, NewMQSender(openProducer(t, k.Broker)), clk)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	loop := func(d *Dispatcher) {
		defer wg.Done()
		for runCtx.Err() == nil {
			if err := d.RunBatch(runCtx); err != nil && runCtx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			timer := time.NewTimer(5 * time.Millisecond)
			select {
			case <-runCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
	wg.Add(2)
	go loop(d1)
	go loop(d2)
	deadline := time.NewTimer(4 * time.Minute)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		var left int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE status <> 'SENT'`).Scan(&left); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if left == 0 {
			break
		}
		select {
		case err := <-errCh:
			t.Fatal(testkit.Redact(err))
		case <-deadline.C:
			t.Fatalf("left %d", left)
		case <-tick.C:
		}
	}
	cancel()
	wg.Wait()
	var sent, maxRetry int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(retry_count), 0) FROM outbox WHERE status = 'SENT'`).Scan(&sent, &maxRetry); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if sent != n || maxRetry != 0 {
		t.Fatalf("sent = %d retry = %d", sent, maxRetry)
	}
	seen := consumeCounts(t, k.Broker, topic, n)
	if len(seen) != n {
		t.Fatalf("unique = %d", len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("%s count = %d", id, count)
		}
	}
}

func insertPendingBulk(t *testing.T, ctx context.Context, db *sqlx.DB, topic string, n int, next time.Time) {
	t.Helper()
	const chunk = 200
	for start := 0; start < n; start += chunk {
		end := start + chunk
		if end > n {
			end = n
		}
		var b strings.Builder
		b.WriteString(`INSERT INTO outbox (
    event_id, event_type, topic, msg_key, biz_no, payload, status, retry_count, next_retry_at, created_at, updated_at
) VALUES `)
		args := make([]any, 0, (end-start)*11)
		for i := start; i < end; i++ {
			if i > start {
				b.WriteByte(',')
			}
			b.WriteString("(?,?,?,?,?,?,?,?,?,?,?)")
			id := fmt.Sprintf("EV%019d", i+1)
			args = append(args, id, "payment.succeeded", topic, id, id, payloadWithTrace(id), statusPending, 0, next, next, next)
		}
		if _, err := db.ExecContext(ctx, b.String(), args...); err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
}

// MQ-02 验收 3：注入发送失败的假生产者，20 次后状态为 DEAD，pay_outbox_dead 为 1。
func TestMQ02Accept3DeadAfterTwenty(t *testing.T) {
	ctx, db := openOutboxFor(t, time.Minute)
	at, clk := clockAt()
	const eventID = "EV0000000000000000020"
	const topic = "pay.mq02.accept3"
	insertPending(t, ctx, db, eventID, topic, payloadWithTrace(eventID), at, 0)
	faults := countFault(t)
	d := NewDispatcher(db, failSender{err: errors.New("unavailable")}, clk)
	for i := 1; i <= 20; i++ {
		base := clk.Now().Truncate(time.Millisecond).In(shanghai)
		if err := d.RunBatch(ctx); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		row := loadEvent(t, ctx, db, eventID)
		wantNext := base.Add(backoff(i))
		if row.retry != i || !row.next.Equal(wantNext) || row.last.String != "unavailable" {
			t.Fatalf("try %d status=%s retry=%d next=%s", i, row.status, row.retry, row.next)
		}
		if i < 20 && row.status != statusPending {
			t.Fatalf("try %d status=%s", i, row.status)
		}
		if i == 20 && row.status != statusDead {
			t.Fatalf("status=%s", row.status)
		}
		if i == 1 && !row.next.Equal(base.Add(2*time.Second)) {
			t.Fatal("first interval")
		}
		if i == 9 && !row.next.Equal(base.Add(5*time.Minute)) {
			t.Fatal("cap")
		}
		if i < 20 {
			clk.Set(row.next)
		}
	}
	if faults.Load() != 20 {
		t.Fatalf("fault calls = %d", faults.Load())
	}
	if err := d.RefreshGauges(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if Dead(topic) != 1 || Pending(topic) != 0 {
		t.Fatalf("dead=%d pending=%d", Dead(topic), Pending(topic))
	}
}

// MQ-02 验收 4：发送期间不持有行锁，另一个连接 FOR UPDATE NOWAIT 立即成功。
func TestMQ02Accept4NoRowLockDuringSend(t *testing.T) {
	ctx, db := openOutboxFor(t, time.Minute)
	at, clk := clockAt()
	const eventID = "EV0000000000000000004"
	id := insertPending(t, ctx, db, eventID, "pay.mq02.accept4", payloadWithTrace(eventID), at, 0)
	entered := make(chan struct{})
	release := make(chan struct{})
	d := NewDispatcher(db, &gateSender{entered: entered, release: release}, clk)
	errCh := make(chan error, 1)
	go func() { errCh <- d.RunBatch(ctx) }()
	waitCh(t, entered, "send did not start")
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	tx, err := db.BeginTxx(probeCtx, nil)
	if err != nil {
		cancel()
		t.Fatal("begin probe")
	}
	var got uint64
	err = tx.QueryRowContext(probeCtx, `SELECT id FROM outbox WHERE id = ? FOR UPDATE NOWAIT`, id).Scan(&got)
	_ = tx.Rollback()
	cancel()
	if err != nil || got != id {
		t.Fatal("nowait failed")
	}
	close(release)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err = <-errCh:
	case <-timer.C:
		t.Fatal("batch did not finish")
	}
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

// MQ-02 验收 5：实例 A 领取后阻塞，Fake 时钟推进 31 秒，实例 B 领取同一行并置为 SENT。
func TestMQ02Accept5ExpiredLeaseTakenOver(t *testing.T) {
	ctx, db := openOutboxFor(t, time.Minute)
	_, clk := clockAt()
	const eventID = "EV0000000000000000005"
	const topic = "pay.mq02.accept5"
	insertPending(t, ctx, db, eventID, topic, payloadWithTrace(eventID), clk.Now(), 0)
	entered := make(chan struct{})
	release := make(chan struct{})
	a := NewDispatcher(db, &gateSender{entered: entered, release: release}, clk)
	errA := make(chan error, 1)
	go func() { errA <- a.RunBatch(ctx) }()
	waitCh(t, entered, "A did not send")
	clk.Advance(31 * time.Second)
	nowB := clk.Now().Truncate(time.Millisecond).In(shanghai)
	leaseB := nowB.Add(leaseDuration)
	b := NewDispatcher(db, okSender{}, clk)
	if err := b.RunBatch(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	assertLease(t, ctx, db, eventID, statusSent, 0, leaseB)
	close(release)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-errA:
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
	case <-timer.C:
		t.Fatal("A did not finish")
	}
	assertLease(t, ctx, db, eventID, statusSent, 0, leaseB)
}

// MQ-02 验收 6：B 领取之后、回写之前放行 A。A 的成功和失败回写都影响 0 行，行保持 B 写入的值。
func TestMQ02Accept6StaleWritebackMisses(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		runStaleWriteback(t, "EV0000000000000000061", nil)
	})
	t.Run("failure", func(t *testing.T) {
		runStaleWriteback(t, "EV0000000000000000062", errors.New("unavailable"))
	})
}

func runStaleWriteback(t *testing.T, eventID string, aErr error) {
	t.Helper()
	ctx, db := openOutboxFor(t, time.Minute)
	_, clk := clockAt()
	const topic = "pay.mq02.accept6"
	insertPending(t, ctx, db, eventID, topic, payloadWithTrace(eventID), clk.Now(), 0)
	aRelease := make(chan struct{})
	aEntered := make(chan struct{})
	aDone := make(chan struct{})
	var gotA error
	a := NewDispatcher(db, &gateSender{entered: aEntered, release: aRelease, err: aErr}, clk)
	go func() {
		gotA = a.RunBatch(ctx)
		close(aDone)
	}()
	waitCh(t, aEntered, "A did not send")
	clk.Advance(31 * time.Second)
	nowB := clk.Now().Truncate(time.Millisecond).In(shanghai)
	leaseB := nowB.Add(leaseDuration)
	opened := make(chan struct{})
	close(opened)
	var once sync.Once
	b := NewDispatcher(db, &gateSender{
		entered: make(chan struct{}),
		release: opened,
		beforeReturn: func() {
			once.Do(func() { close(aRelease) })
			<-aDone
		},
	}, clk)
	errCh := make(chan error, 1)
	go func() { errCh <- b.RunBatch(ctx) }()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
	case <-timer.C:
		t.Fatal("B did not finish")
	}
	if gotA != nil {
		t.Fatal(testkit.Redact(gotA))
	}
	assertLease(t, ctx, db, eventID, statusSent, 0, leaseB)
}

func assertLease(t *testing.T, ctx context.Context, db *sqlx.DB, eventID, status string, retry int, lease time.Time) {
	t.Helper()
	row := loadEvent(t, ctx, db, eventID)
	if row.status != status || row.retry != retry || !row.next.Equal(lease) {
		t.Fatalf("status=%s retry=%d next=%s want %s %d %s", row.status, row.retry, row.next, status, retry, lease)
	}
}

// MQ-02：0 行回写不报错。
func TestMQ02ZeroRowWriteback(t *testing.T) {
	ctx, db := openOutboxFor(t, time.Minute)
	at, clk := clockAt()
	const eventID = "EV0000000000000000070"
	id := insertPending(t, ctx, db, eventID, "pay.mq02.zero", []byte(`{}`), at, 0)
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	d := NewDispatcher(db, okSender{}, clk)
	row := claimed{
		id: id, eventID: eventID, eventType: "payment.succeeded", topic: "pay.mq02.zero",
	}
	wrong := at.Add(time.Hour)
	if err := d.writeOne(ctx, row, wrong, nil); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err := d.writeOne(ctx, row, wrong, errors.New("dial tcp 127.0.0.1:1")); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got := loadEvent(t, ctx, db, eventID)
	if got.status != statusPending || got.retry != 0 || !got.next.Equal(at) || got.last.Valid {
		t.Fatalf("status=%s retry=%d last=%v", got.status, got.retry, got.last.Valid)
	}
	text := buf.String()
	if strings.Contains(text, "ERROR") || strings.Contains(text, "127.0.0.1") || strings.Contains(text, "dial") {
		t.Fatal("zero-row writeback logged an error")
	}
}

// MQ-02：traceparent 缺失仍发送，行成为 SENT。
func TestMQ02TraceparentMissingStillSendsOnRow(t *testing.T) {
	ctx, db := openOutboxFor(t, time.Minute)
	at, clk := clockAt()
	const eventID = "EV0000000000000000071"
	insertPending(t, ctx, db, eventID, "pay.mq02.trace", []byte(`{}`), at, 0)
	var got []Delivery
	sender := funcSender(func(_ context.Context, batch []Delivery) []error {
		got = append(got, batch...)
		return make([]error, len(batch))
	})
	faults := countFault(t)
	d := NewDispatcher(db, sender, clk)
	if err := d.RunBatch(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if len(got) != 1 || got[0].Traceparent != "" || string(got[0].Value) != `{}` {
		t.Fatalf("sent = %+v", got)
	}
	if faults.Load() != 1 {
		t.Fatalf("fault calls = %d", faults.Load())
	}
	row := loadEvent(t, ctx, db, eventID)
	if row.status != statusSent || row.retry != 0 || !row.sent.Valid {
		t.Fatalf("status=%s retry=%d", row.status, row.retry)
	}
}

type funcSender func(context.Context, []Delivery) []error

func (f funcSender) Send(ctx context.Context, batch []Delivery) []error {
	return f(ctx, batch)
}

// MQ-02：没有到期行不发送，也不打故障点。
func TestMQ02NoDueSkipsSendAndFault(t *testing.T) {
	ctx, db := openOutboxFor(t, time.Minute)
	_, clk := clockAt()
	faults := countFault(t)
	calls := 0
	d := NewDispatcher(db, funcSender(func(context.Context, []Delivery) []error {
		calls++
		return nil
	}), clk)
	if err := d.RunBatch(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	insertPending(t, ctx, db, "EV0000000000000000072", "pay.mq02.future", []byte(`{}`), clk.Now().Add(time.Hour), 0)
	if err := d.RunBatch(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if calls != 0 || faults.Load() != 0 {
		t.Fatalf("calls=%d fault=%d", calls, faults.Load())
	}
}

// MQ-02：gauge 刷新把掉到 0 的 topic 写成 0。
func TestMQ02GaugeRefreshZerosMissingTopic(t *testing.T) {
	ctx, db := openOutboxFor(t, time.Minute)
	at, clk := clockAt()
	const topic = "pay.mq02.gauge"
	const eventID = "EV0000000000000000073"
	insertPending(t, ctx, db, eventID, topic, []byte(`{}`), at, 0)
	d := NewDispatcher(db, okSender{}, clk)
	if err := d.RefreshGauges(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if Pending(topic) != 1 {
		t.Fatalf("pending = %d", Pending(topic))
	}
	if _, err := db.ExecContext(ctx, `UPDATE outbox SET status = 'SENT', updated_at = ? WHERE event_id = ?`, at, eventID); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err := d.RefreshGauges(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if Pending(topic) != 0 {
		t.Fatalf("stale pending = %d", Pending(topic))
	}
}

// MQ-02：失败日志可以有 event_id、event_type、topic，不写带地址的错误文本。
func TestMQ02FailureLogOmitsAddress(t *testing.T) {
	ctx, db := openOutboxFor(t, time.Minute)
	at, clk := clockAt()
	const eventID = "EV0000000000000000074"
	const topic = "pay.mq02.log"
	insertPending(t, ctx, db, eventID, topic, []byte(`{}`), at, 0)
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	d := NewDispatcher(db, failSender{err: errors.New("dial tcp 127.0.0.1:1")}, clk)
	if err := d.RunBatch(ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	text := buf.String()
	if strings.Contains(text, "127.0.0.1") || strings.Contains(text, "dial") || strings.Contains(text, "ERROR") {
		t.Fatal("failure log leaked an address")
	}
	if !strings.Contains(text, eventID) || !strings.Contains(text, topic) || !strings.Contains(text, "payment.succeeded") {
		t.Fatal("failure log missing event fields")
	}
}
