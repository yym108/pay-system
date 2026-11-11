package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/pkg/obs"
)

const (
	statusSent = "SENT"
	statusDead = "DEAD"

	claimLimit    = 100
	leaseDuration = 30 * time.Second
	deadAfter     = 20
	backoffCapAt  = 9
	backoffCap    = 5 * time.Minute
	lastErrorMax  = 512

	faultAfterSend = "outbox.after_send"
)

// errClaimMismatch 表示领取更新的行数和选中的行数不一致。事务回滚，不发送。
var errClaimMismatch = errors.New("outbox: claim mismatch")

// claimSelectSQL 只取回写需要的列。now 由调用方绑定，不用 SQL NOW()。
const claimSelectSQL = `
SELECT id, event_id, event_type, topic, msg_key, payload, retry_count
FROM outbox
WHERE status = 'PENDING' AND next_retry_at <= ?
ORDER BY id
LIMIT 100
FOR UPDATE SKIP LOCKED`

// claimUpdateSQL 把租约写进 next_retry_at。
// updated_at 显式写成注入时钟：这一列 ON UPDATE CURRENT_TIMESTAMP，不写就会变成数据库时间。
const claimUpdateSQL = `
UPDATE outbox
SET next_retry_at = ?, updated_at = ?
WHERE id IN (?)`

const sentSQL = `
UPDATE outbox
SET status = 'SENT', sent_at = ?, updated_at = ?
WHERE id = ? AND status = 'PENDING' AND next_retry_at = ?`

const failSQL = `
UPDATE outbox
SET status = ?, retry_count = ?, next_retry_at = ?, last_error = ?, updated_at = ?
WHERE id = ? AND status = 'PENDING' AND next_retry_at = ?`

const gaugeSQL = `
SELECT topic, status, COUNT(*)
FROM outbox
WHERE status IN ('PENDING', 'DEAD')
GROUP BY topic, status`

// Delivery 是一条待发送的 outbox 记录。Value 是 payload 原字节。
type Delivery struct {
	Topic       string
	Key         []byte
	Value       []byte
	EventID     string
	EventType   string
	Traceparent string
}

// Sender 在事务外批量发送。返回与 batch 等长的逐条错误。空 batch 返回 nil。
type Sender interface {
	Send(ctx context.Context, batch []Delivery) []error
}

// Dispatcher 领取 outbox、事务外发送、按租约回写。
type Dispatcher struct {
	db     *sqlx.DB
	sender Sender
	clk    clock.Clock
}

type claimed struct {
	id         uint64
	eventID    string
	eventType  string
	topic      string
	msgKey     string
	payload    []byte
	retryCount int
}

// NewDispatcher 装配投递器。db、sender、clk 为空时 RunBatch 在领取前返回错误。
func NewDispatcher(db *sqlx.DB, sender Sender, clk clock.Clock) *Dispatcher {
	return &Dispatcher{db: db, sender: sender, clk: clk}
}

// NewMQSender 用同一个 Producer 批量发送。p 为空时返回 nil，调用方不会拨号。
func NewMQSender(p *mq.Producer) Sender {
	if p == nil {
		return nil
	}
	return mqSender{producer: p}
}

type mqSender struct {
	producer *mq.Producer
}

func (s mqSender) Send(ctx context.Context, batch []Delivery) []error {
	if len(batch) == 0 {
		return nil
	}
	msgs := make([]mq.Message, len(batch))
	for i := range batch {
		msgs[i] = mq.Message{
			Topic:       batch[i].Topic,
			Key:         append([]byte(nil), batch[i].Key...),
			Value:       append([]byte(nil), batch[i].Value...),
			EventID:     batch[i].EventID,
			EventType:   batch[i].EventType,
			Traceparent: batch[i].Traceparent,
		}
	}
	errs := s.producer.ProduceBatch(ctx, msgs)
	if len(errs) == len(batch) {
		return errs
	}
	out := make([]error, len(batch))
	for i := range out {
		if i < len(errs) {
			out[i] = errs[i]
			continue
		}
		out[i] = mq.ErrUnavailable
	}
	return out
}

// RunBatch 领取至多 100 行，事务外发送，再逐条回写。
// 没有到期行时提交空事务并返回，不发送，不打故障点。
// 批次 ctx 取消后，已经进入发送或回写的这一批仍跑完：发送受 5 秒超时，回写受事务 3 秒。
func (d *Dispatcher) RunBatch(ctx context.Context) error {
	if d == nil || d.db == nil || d.sender == nil || d.clk == nil {
		return errNilDependency
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := d.now()
	lease := now.Add(leaseDuration)
	rows, err := d.claim(ctx, now, lease)
	if err != nil || len(rows) == 0 {
		return err
	}
	work := context.WithoutCancel(ctx)
	errs := d.sender.Send(work, deliveries(rows))
	fault.Point(work, faultAfterSend)
	return d.writeBack(work, rows, lease, errs)
}

func (d *Dispatcher) now() time.Time {
	return d.clk.Now().Truncate(time.Millisecond).In(shanghai)
}

func (d *Dispatcher) claim(ctx context.Context, now, lease time.Time) ([]claimed, error) {
	var got []claimed
	err := dbx.WithTx(ctx, d.db, func(tx *sqlx.Tx) error {
		got = nil
		batch, err := selectDue(ctx, tx, now)
		if err != nil || len(batch) == 0 {
			return err
		}
		if err = leaseRows(ctx, tx, batch, lease, now); err != nil {
			return err
		}
		got = batch
		return nil
	})
	if err != nil {
		return nil, err
	}
	return got, nil
}

func selectDue(ctx context.Context, tx *sqlx.Tx, now time.Time) ([]claimed, error) {
	rows, err := tx.QueryContext(ctx, claimSelectSQL, now)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]claimed, 0, claimLimit)
	for rows.Next() {
		var row claimed
		if err = rows.Scan(&row.id, &row.eventID, &row.eventType, &row.topic, &row.msgKey, &row.payload, &row.retryCount); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func leaseRows(ctx context.Context, tx *sqlx.Tx, batch []claimed, lease, now time.Time) error {
	ids := make([]uint64, len(batch))
	for i := range batch {
		ids[i] = batch[i].id
	}
	query, args, err := sqlx.In(claimUpdateSQL, lease, now, ids)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, tx.Rebind(query), args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != int64(len(ids)) {
		return errClaimMismatch
	}
	return nil
}

func deliveries(rows []claimed) []Delivery {
	out := make([]Delivery, len(rows))
	for i := range rows {
		out[i] = Delivery{
			Topic:       rows[i].topic,
			Key:         []byte(rows[i].msgKey),
			Value:       append([]byte(nil), rows[i].payload...),
			EventID:     rows[i].eventID,
			EventType:   rows[i].eventType,
			Traceparent: traceparentOf(rows[i].payload),
		}
	}
	return out
}

func traceparentOf(payload []byte) string {
	var env struct {
		TraceContext json.RawMessage `json:"trace_context"`
	}
	if json.Unmarshal(payload, &env) != nil || len(env.TraceContext) == 0 || string(env.TraceContext) == "null" {
		return ""
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(env.TraceContext, &fields) != nil {
		return ""
	}
	raw, ok := fields["traceparent"]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}

func (d *Dispatcher) writeBack(ctx context.Context, rows []claimed, lease time.Time, errs []error) error {
	var first error
	for i := range rows {
		if err := d.writeOne(ctx, rows[i], lease, sendErrAt(errs, i)); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func sendErrAt(errs []error, i int) error {
	if i < len(errs) {
		return errs[i]
	}
	return mq.ErrUnavailable
}

func (d *Dispatcher) writeOne(ctx context.Context, row claimed, lease time.Time, sendErr error) error {
	now := d.now()
	var logged bool
	var wroteSent bool
	err := dbx.WithTx(ctx, d.db, func(tx *sqlx.Tx) error {
		logged = false
		wroteSent = false
		res, err := execWrite(ctx, tx, row, lease, now, sendErr)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		logged = sendErr != nil
		wroteSent = sendErr == nil
		return nil
	})
	if err != nil {
		return err
	}
	if wroteSent {
		obs.FinishWait(ctx, obs.OutboxWaitKey(row.eventID))
	}
	if logged {
		// 不写 payload，也不写 last_error：错误文本可能带地址。哨兵文本留在列里。
		slog.WarnContext(ctx, "outbox deliver failed",
			"event_id", row.eventID,
			"event_type", row.eventType,
			"topic", row.topic,
		)
	}
	return nil
}

func execWrite(ctx context.Context, tx *sqlx.Tx, row claimed, lease, now time.Time, sendErr error) (sql.Result, error) {
	if sendErr == nil {
		return tx.ExecContext(ctx, sentSQL, now, now, row.id, lease)
	}
	newRetry := row.retryCount + 1
	next := now.Add(backoff(newRetry))
	return tx.ExecContext(ctx, failSQL,
		statusForRetry(newRetry), newRetry, next, truncateBytes(sendErr.Error(), lastErrorMax), now,
		row.id, lease,
	)
}

// backoff 用加 1 之后的 retry_count。第 1 次是 2 秒。2^9 秒已超过 5 分钟，不再移位。
func backoff(newRetry int) time.Duration {
	if newRetry >= backoffCapAt {
		return backoffCap
	}
	if newRetry < 1 {
		newRetry = 1
	}
	return time.Second << newRetry
}

func statusForRetry(newRetry int) string {
	if newRetry >= deadAfter {
		return statusDead
	}
	return statusPending
}

func truncateBytes(s string, max int) string {
	b := []byte(s)
	if len(b) <= max || max < 0 {
		return s
	}
	b = b[:max]
	for len(b) > 0 && !utf8.Valid(b) {
		b = b[:len(b)-1]
	}
	return string(b)
}

var (
	gaugeMu        sync.Mutex
	pendingByTopic = map[string]int64{}
	deadByTopic    = map[string]int64{}
)

// Pending 返回 topic 上的 pay_outbox_pending。没见过的 topic 是 0。
func Pending(topic string) int64 {
	gaugeMu.Lock()
	defer gaugeMu.Unlock()
	return pendingByTopic[topic]
}

// Dead 返回 topic 上的 pay_outbox_dead。没见过的 topic 是 0。
func Dead(topic string) int64 {
	gaugeMu.Lock()
	defer gaugeMu.Unlock()
	return deadByTopic[topic]
}

// PendingTotal 是各 topic 的 pay_outbox_pending 之和。
func PendingTotal() int64 {
	gaugeMu.Lock()
	defer gaugeMu.Unlock()
	return gaugeSum(pendingByTopic)
}

// DeadTotal 是各 topic 的 pay_outbox_dead 之和。
func DeadTotal() int64 {
	gaugeMu.Lock()
	defer gaugeMu.Unlock()
	return gaugeSum(deadByTopic)
}

func gaugeSum(src map[string]int64) int64 {
	var n int64
	for _, v := range src {
		n += v
	}
	return n
}

// RefreshGauges 用当前 COUNT 覆盖进程内 pay_outbox_pending 和 pay_outbox_dead。
// 这次查询没有出现的 topic 记为 0，不留上一次的值。
func (d *Dispatcher) RefreshGauges(ctx context.Context) error {
	if d == nil || d.db == nil {
		return errNilDependency
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	rows, err := d.db.QueryContext(ctx, gaugeSQL)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	nextPending := map[string]int64{}
	nextDead := map[string]int64{}
	gaugeMu.Lock()
	for topic := range pendingByTopic {
		nextPending[topic] = 0
	}
	for topic := range deadByTopic {
		nextDead[topic] = 0
	}
	gaugeMu.Unlock()
	for rows.Next() {
		var topic, status string
		var n int64
		if err = rows.Scan(&topic, &status, &n); err != nil {
			return err
		}
		switch status {
		case statusPending:
			nextPending[topic] = n
		case statusDead:
			nextDead[topic] = n
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	gaugeMu.Lock()
	pendingByTopic = nextPending
	deadByTopic = nextDead
	pendingSnap := copyGauges(nextPending)
	deadSnap := copyGauges(nextDead)
	gaugeMu.Unlock()
	for topic, n := range pendingSnap {
		obs.SetOutboxPending(topic, n)
	}
	for topic, n := range deadSnap {
		obs.SetOutboxDead(topic, n)
	}
	return nil
}

func copyGauges(src map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(src))
	for topic, n := range src {
		out[topic] = n
	}
	return out
}
