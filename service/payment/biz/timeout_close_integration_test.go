//go:build integration

package biz

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/outbox"
	"github.com/yym108/pay-system/service/payment/job"
)

func loadStatusByPayNo(t *testing.T, env payEnv, payNo string) (status string, version uint64, closedAt sql.NullTime) {
	t.Helper()
	err := env.db.QueryRowContext(env.ctx, `SELECT status, version, closed_at FROM payment_order WHERE pay_no = ?`, payNo).
		Scan(&status, &version, &closedAt)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return status, version, closedAt
}

// insertRawPayment 直接插入一行支付单，用于构造大批量或任意状态的过期订单。
func insertRawPayment(t *testing.T, env payEnv, merchantID, payNo, status string, expireAt time.Time) {
	t.Helper()
	created := expireAt.Add(-15 * time.Minute)
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO payment_order (pay_no, merchant_id, out_trade_no, amount, currency, subject, status,
  fee_amount, refunded_amount, notify_url, expire_at, version, created_at, updated_at)
VALUES (?, ?, ?, 10000, 'CNY', 'demo', ?, 60, 0, '', ?, 0, ?, ?)`,
		payNo, merchantID, "OUT-"+payNo, status, expireAt, created, created)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

// PAY-07 验收 1：Fake 时钟推进到过期后，INIT 订单在下一轮变为 CLOSED。
// 循环用 job 包的同一个 Loop，间隔缩短到 10ms；过期前的若干轮不改订单，恰好 expire_at 的那一刻算过期。
func TestTimeoutCloseInitNextRoundAfterExpire(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100070"
	now := feeAt(11, 0, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	order := createCloseOrder(t, env, p, merchantID, "PAY07-INIT", amountNeverFinish)
	clk.Set(now.Add(time.Minute))
	later := createCloseOrder(t, env, p, merchantID, "PAY07-INIT-LATER", amountNeverFinish)

	var rounds atomic.Int64
	loop := job.NewLoop("timeout_close", 10*time.Millisecond, func(ctx context.Context) error {
		defer rounds.Add(1)
		return p.RunTimeoutCloseBatch(ctx)
	})
	served := make(chan error, 1)
	go func() { served <- loop.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := loop.Stop(ctx); err != nil {
			t.Errorf("stop = %v", err)
		}
		if err := <-served; err != nil {
			t.Errorf("serve = %v", err)
		}
	})
	rclk := clock.Real{}

	clk.Set(order.ExpireAt.Add(-time.Millisecond))
	start := rounds.Load()
	if !waitUntil(t, rclk, 5*time.Second, func() bool { return rounds.Load() >= start+3 }) {
		t.Fatalf("rounds = %d", rounds.Load())
	}
	if status, version, _ := loadStatusByPayNo(t, env, order.PayNo); status != payStatusInit || version != 0 {
		t.Fatalf("closed before expiry: %s v%d", status, version)
	}

	clk.Set(order.ExpireAt)
	setAt := rounds.Load()
	if !waitUntil(t, rclk, 5*time.Second, func() bool {
		status, _, _ := loadStatusByPayNo(t, env, order.PayNo)
		return status == payStatusClosed
	}) {
		t.Fatal("expired INIT order not closed")
	}
	// 设时钟时可能有一轮正在跑且已读过旧时间，所以允许“这一轮 + 下一轮”。
	if got := rounds.Load(); got > setAt+2 {
		t.Fatalf("closed after %d rounds", got-setAt)
	}
	status, version, closedAt := loadStatusByPayNo(t, env, order.PayNo)
	if status != payStatusClosed || version != 1 || !closedAt.Valid || !closedAt.Time.Equal(order.ExpireAt) {
		t.Fatalf("order = %s v%d closed_at=%v", status, version, closedAt)
	}
	if status, version, _ := loadStatusByPayNo(t, env, later.PayNo); status != payStatusInit || version != 0 {
		t.Fatalf("unexpired order changed: %s v%d", status, version)
	}
	if row := loadMockTxn(t, env.ctx, mock.db, order.PayNo); row.count != 0 {
		t.Fatalf("INIT close reached channel: %+v", row)
	}
	if count, _ := loadLocalTxnStatus(t, env, order.PayNo); count != 0 {
		t.Fatalf("channel_txn rows = %d", count)
	}
}

// PAY-07 验收 2：过期的 NEVER_FINISH 订单被关闭，渠道侧同为 CLOSED，之后不再有回调。
func TestTimeoutCloseNeverFinishClosesChannel(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100071"
	now := feeAt(11, 30, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	order := createCloseOrder(t, env, p, merchantID, "PAY07-NEVER", amountNeverFinish)
	paid, err := p.Pay(env.ctx, payReq(order))
	if err != nil || paid.Status != payStatusPaying {
		t.Fatalf("pay = %+v err=%v", paid, err)
	}
	// 未过期时不碰 PAYING 订单，也不调用渠道。
	if err = p.RunTimeoutCloseBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	if row := loadMockTxn(t, env.ctx, mock.db, order.PayNo); row.status != txnStatusProcessing {
		t.Fatalf("mock before expiry = %+v", row)
	}
	clk.Set(order.ExpireAt.Add(time.Second))
	if err = p.RunTimeoutCloseBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	stored := loadPayment(t, env, merchantID, "PAY07-NEVER")
	want := order.ExpireAt.Add(time.Second)
	if stored.status != payStatusClosed || !stored.closedAt.Valid || !stored.closedAt.Time.Equal(want) || stored.amount != int64(amountNeverFinish) {
		t.Fatalf("order = %+v", stored)
	}
	row := loadMockTxn(t, env.ctx, mock.db, order.PayNo)
	if row.count != 1 || row.status != payStatusClosed || row.scenario != "NEVER_FINISH" || row.notifySent != 0 || row.amount != int64(amountNeverFinish) {
		t.Fatalf("mock = %+v", row)
	}
	if count, status := loadLocalTxnStatus(t, env, order.PayNo); count != 1 || status != payStatusClosed {
		t.Fatalf("channel_txn count=%d status=%s", count, status)
	}
	if mock.hooks.Load() != 0 || mock.sched.Pending() != 0 {
		t.Fatalf("hooks=%d pending=%d", mock.hooks.Load(), mock.sched.Pending())
	}
	if n := len(loadOutbox(t, env, order.PayNo)); n != 0 {
		t.Fatalf("outbox = %d", n)
	}
	// 再跑一轮：订单已终态，不再被扫描，渠道不再收到关单。
	if err = p.RunTimeoutCloseBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	if again := loadPayment(t, env, merchantID, "PAY07-NEVER"); again.version != stored.version {
		t.Fatalf("version %d -> %d", stored.version, again.version)
	}
}

// PAY-07 规则 2 与 PAY-06 一致：过期订单关单时渠道已成功（CLOSE_RACE），订单推进为 SUCCESS，
// outbox 恰好 1 条 payment.succeeded，之后到达的回调按重复处理。
func TestTimeoutCloseRaceSucceedsOnce(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100072"
	now := feeAt(12, 0, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	order := createCloseOrder(t, env, p, merchantID, "PAY07-RACE", amountCloseRace)
	if paid, err := p.Pay(env.ctx, payReq(order)); err != nil || paid.Status != payStatusPaying {
		t.Fatalf("pay = %+v err=%v", paid, err)
	}
	clk.Set(order.ExpireAt)
	if err := p.RunTimeoutCloseBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	stored := loadPayment(t, env, merchantID, "PAY07-RACE")
	if stored.status != payStatusSuccess || !stored.paidAt.Valid || stored.closedAt.Valid {
		t.Fatalf("order = %+v", stored)
	}
	if row := loadMockTxn(t, env.ctx, mock.db, order.PayNo); row.status != payStatusSuccess || row.scenario != "CLOSE_RACE" {
		t.Fatalf("mock = %+v", row)
	}
	if count, status := loadLocalTxnStatus(t, env, order.PayNo); count != 1 || status != payStatusSuccess {
		t.Fatalf("channel_txn count=%d status=%s", count, status)
	}
	events := loadOutbox(t, env, order.PayNo)
	if len(events) != 1 || events[0].eventType != outbox.EventPaymentSucceeded {
		t.Fatalf("outbox = %+v", events)
	}
	if got := loadNotifyCount(t, env, order.PayNo); got != 0 {
		t.Fatalf("notify_count after close = %d", got)
	}
	if err := p.RunTimeoutCloseBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(loadOutbox(t, env, order.PayNo)); n != 1 {
		t.Fatalf("outbox after rerun = %d", n)
	}
	// 之后到达的渠道回调（金额、流水号、完成时间与本地一致）按重复处理：只累计 notify_count，
	// 订单 version 不变，outbox 仍只有 1 条。
	var (
		txnNo    string
		amount   int64
		finished time.Time
	)
	err := env.db.QueryRowContext(env.ctx, `
SELECT channel_txn_no, amount, channel_finished_at FROM channel_txn WHERE biz_no = ?`, order.PayNo).
		Scan(&txnNo, &amount, &finished)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	amt := money.Amount(amount)
	kind, err := p.ApplyChannelNotify(env.ctx, PayResult{
		BizNo: order.PayNo, Result: payResultSuccess, Source: paySourceNotify,
		ChannelTxnNo: txnNo, Amount: &amt, FinishedAt: finished,
	})
	if err != nil || kind != "DUPLICATE" {
		t.Fatalf("notify = %q err=%v", kind, err)
	}
	if got := loadNotifyCount(t, env, order.PayNo); got != 1 {
		t.Fatalf("notify_count after callback = %d", got)
	}
	if again := loadPayment(t, env, merchantID, "PAY07-RACE"); again.version != stored.version || again.status != payStatusSuccess {
		t.Fatalf("order after callback = %+v (was v%d)", again, stored.version)
	}
	if n := len(loadOutbox(t, env, order.PayNo)); n != 1 {
		t.Fatalf("outbox after callback = %d", n)
	}
}

// loadNotifyCount 只读 notify_count：终态行的 next_query_at 为 NULL，不能用 loadTxn 读。
func loadNotifyCount(t *testing.T, env payEnv, payNo string) int {
	t.Helper()
	var n int
	if err := env.db.QueryRowContext(env.ctx, `SELECT notify_count FROM channel_txn WHERE biz_no = ?`, payNo).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

// PAY-07 规则 1：只扫 INIT / PAYING 且 expire_at <= now，INIT、PAYING 各一条查询各至多 100 条，
// 合计不超过 200 条，都走 idx_status_expire_at 且不做 filesort。
// 201 条过期 INIT 分三批关完（100、200、201）；积压的 INIT 不挡住过期的 PAYING，它在第一批就被关闭。
// 未过期、已终态的行不动。
func TestTimeoutCloseBatchLimitAndFilter(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100073"
	now := feeAt(12, 30, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now.Add(-20 * time.Minute))
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	paying := createCloseOrder(t, env, p, merchantID, "PAY07-BATCH-PAYING", amountNeverFinish)
	if paid, err := p.Pay(env.ctx, payReq(paying)); err != nil || paid.Status != payStatusPaying {
		t.Fatalf("pay = %+v err=%v", paid, err)
	}
	clk.Set(now)
	if !paying.ExpireAt.Before(now) {
		t.Fatalf("paying order not expired: %v", paying.ExpireAt)
	}
	const initRows = timeoutCloseInitLimit*2 + 1
	for i := 0; i < initRows; i++ {
		insertRawPayment(t, env, merchantID, fmt.Sprintf("PA07%017d", i), payStatusInit, now.Add(-time.Duration(i)*time.Millisecond))
	}
	insertRawPayment(t, env, merchantID, "PA07FUTURE", payStatusInit, now.Add(time.Millisecond))
	for _, status := range []string{payStatusSuccess, payStatusFailed} {
		insertRawPayment(t, env, merchantID, "PA07"+status, status, now.Add(-time.Hour))
	}
	for batch, want := range []int{timeoutCloseInitLimit, timeoutCloseInitLimit * 2, initRows} {
		if err := p.RunTimeoutCloseBatch(env.ctx); err != nil {
			t.Fatal(err)
		}
		if got := countClosedRaw(t, env); got != want {
			t.Fatalf("batch %d closed INIT = %d, want %d", batch+1, got, want)
		}
		if batch == 0 {
			if status, _, _ := loadStatusByPayNo(t, env, paying.PayNo); status != payStatusClosed {
				t.Fatalf("expired PAYING after first batch = %s", status)
			}
		}
	}
	if row := loadMockTxn(t, env.ctx, mock.db, paying.PayNo); row.count != 1 || row.status != payStatusClosed {
		t.Fatalf("mock = %+v", row)
	}
	for payNo, want := range map[string]string{"PA07FUTURE": payStatusInit, "PA07SUCCESS": payStatusSuccess, "PA07FAILED": payStatusFailed} {
		if status, version, _ := loadStatusByPayNo(t, env, payNo); status != want || version != 0 {
			t.Fatalf("%s = %s v%d", payNo, status, version)
		}
	}
	for _, status := range []string{payStatusInit, payStatusPaying} {
		assertExpiredScanPlan(t, env, status, now)
	}
}

// countClosedRaw 数 insertRawPayment 插入、已被关闭的行（单号以 PA07 开头）。
func countClosedRaw(t *testing.T, env payEnv) int {
	t.Helper()
	var n int
	err := env.db.QueryRowContext(env.ctx,
		`SELECT COUNT(*) FROM payment_order WHERE status = ? AND pay_no LIKE 'PA07%'`, payStatusClosed).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

// assertExpiredScanPlan 检查单个状态的扫描走 idx_status_expire_at：status 等值后按 expire_at 排序即索引顺序，
// 计划里有 using_filesort 字段且为 false。
func assertExpiredScanPlan(t *testing.T, env payEnv, status string, now time.Time) {
	t.Helper()
	var plan []byte
	err := env.db.QueryRowContext(env.ctx, "EXPLAIN FORMAT=JSON "+expiredPaymentsSQL, status, now, timeoutCloseInitLimit).Scan(&plan)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	var doc struct {
		QueryBlock struct {
			OrderingOperation struct {
				UsingFilesort *bool `json:"using_filesort"`
			} `json:"ordering_operation"`
		} `json:"query_block"`
	}
	if err := json.Unmarshal(plan, &doc); err != nil {
		t.Fatal(err)
	}
	filesort := doc.QueryBlock.OrderingOperation.UsingFilesort
	if !strings.Contains(string(plan), `"key": "idx_status_expire_at"`) || filesort == nil || *filesort {
		t.Fatalf("%s plan = %s", status, plan)
	}
}

// cancelOnNow 在第 n 次读时间时取消批次 ctx，用来让 Stop 落在 INIT 循环中途。
type cancelOnNow struct {
	inner  clock.Clock
	n      int64
	calls  atomic.Int64
	cancel context.CancelFunc
}

func (c *cancelOnNow) Now() time.Time {
	if c.calls.Add(1) == c.n {
		c.cancel()
	}
	return c.inner.Now()
}

// 优雅退出（审查第 2 轮）：批次 ctx 在 INIT 循环中途取消时，RunTimeoutCloseBatch 返回 nil，
// 不把正常停机记成批次失败；已开始的一条照常关闭，后面的 INIT 不再开始，PAYING 不再扫描。
func TestTimeoutCloseCancelMidBatchReturnsNil(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100075"
	now := feeAt(13, 30, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	p := newClosePayments(t, env, merchantID, now, clk)
	// PAYING 行没有渠道流水，若被处理会先查单并在不可达地址上失败（10004 计数 +1）。
	wireCloseChannel(t, p, "http://127.0.0.1:1", clk)
	insertRawPayment(t, env, merchantID, "PA07CANCEL0", payStatusInit, now.Add(-2*time.Second))
	insertRawPayment(t, env, merchantID, "PA07CANCEL1", payStatusInit, now.Add(-time.Second))
	insertRawPayment(t, env, merchantID, "PA07CANCELP", payStatusPaying, now.Add(-time.Second))
	ctx, cancel := context.WithCancel(env.ctx)
	defer cancel()
	// 第 1 次读时间是批次开始，第 2 次是第一条 INIT 的 closed_at。
	p.clk = &cancelOnNow{inner: clk, n: 2, cancel: cancel}
	unavailable := strconv.Itoa(int(errno.ErrUnavailable.Code))
	before := timeoutCloseFailureCount(unavailable)
	if err := p.RunTimeoutCloseBatch(ctx); err != nil {
		t.Fatalf("canceled batch err = %v", err)
	}
	if status, version, _ := loadStatusByPayNo(t, env, "PA07CANCEL0"); status != payStatusClosed || version != 1 {
		t.Fatalf("started row = %s v%d", status, version)
	}
	if status, version, _ := loadStatusByPayNo(t, env, "PA07CANCEL1"); status != payStatusInit || version != 0 {
		t.Fatalf("row after cancel = %s v%d", status, version)
	}
	if got := timeoutCloseFailureCount(unavailable); got != before {
		t.Fatalf("PAYING processed after cancel: 10004 failures %d -> %d", before, got)
	}
	// 开头就已取消的批次仍返回 ctx 的错误，与 PAY-05 一致。
	if err := p.RunTimeoutCloseBatch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled batch err = %v", err)
	}
}

// PAY-07 规则 2：两个实例同时跑同一批过期订单（INIT 与 NEVER_FINISH 的 PAYING 混合），
// 由 CAS 和渠道关单幂等保证每笔恰好关闭一次：订单、channel_txn、渠道侧都为 CLOSED，version 只加一次。
func TestTimeoutCloseTwoInstances(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100074"
	now := feeAt(13, 0, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	a := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, a, mock.url, clk)
	// 第二个实例用不同的 worker id，与生产多实例一致。
	idsB, err := idgen.New(8, clk)
	if err != nil {
		t.Fatal(err)
	}
	b := NewPayments(NewMerchants(env.db, nil, clk, 0), idsB, clk, "merchant-demo", env.db)
	wireCloseChannel(t, b, mock.url, clk)
	var inits, payings []PaymentOrder
	for i := 0; i < 5; i++ {
		inits = append(inits, createCloseOrder(t, env, a, merchantID, fmt.Sprintf("PAY07-2I-%d", i), amountNeverFinish))
		order := createCloseOrder(t, env, a, merchantID, fmt.Sprintf("PAY07-2P-%d", i), amountNeverFinish)
		paid, err := a.Pay(env.ctx, payReq(order))
		if err != nil || paid.Status != payStatusPaying {
			t.Fatalf("pay = %+v err=%v", paid, err)
		}
		payings = append(payings, paid)
	}
	clk.Set(now.Add(time.Hour))
	start := make(chan struct{})
	var g errgroup.Group
	for _, p := range []*Payments{a, b} {
		g.Go(func() error {
			<-start
			return p.RunTimeoutCloseBatch(env.ctx)
		})
	}
	close(start)
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	for _, order := range inits {
		if status, version, _ := loadStatusByPayNo(t, env, order.PayNo); status != payStatusClosed || version != 1 {
			t.Fatalf("init %s = %s v%d", order.PayNo, status, version)
		}
		if row := loadMockTxn(t, env.ctx, mock.db, order.PayNo); row.count != 0 {
			t.Fatalf("init %s reached channel: %+v", order.PayNo, row)
		}
	}
	for _, order := range payings {
		if status, version, _ := loadStatusByPayNo(t, env, order.PayNo); status != payStatusClosed || version != order.Version+1 {
			t.Fatalf("paying %s = %s v%d (was v%d)", order.PayNo, status, version, order.Version)
		}
		if row := loadMockTxn(t, env.ctx, mock.db, order.PayNo); row.count != 1 || row.status != payStatusClosed || row.notifySent != 0 {
			t.Fatalf("mock %s = %+v", order.PayNo, row)
		}
		if count, status := loadLocalTxnStatus(t, env, order.PayNo); count != 1 || status != payStatusClosed {
			t.Fatalf("channel_txn %s count=%d status=%s", order.PayNo, count, status)
		}
		if n := len(loadOutbox(t, env, order.PayNo)); n != 0 {
			t.Fatalf("outbox %s = %d", order.PayNo, n)
		}
	}
}
