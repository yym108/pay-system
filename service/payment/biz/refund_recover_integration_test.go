//go:build integration

package biz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	accountbiz "github.com/yym108/pay-system/service/account/biz"
	accountdal "github.com/yym108/pay-system/service/account/dal"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

// 子进程模式：TestMain 看到 refundCrashChildEnv 时只执行 runRefundCrashChild，不跑测试。
const (
	refundCrashChildEnv = "PAY_REF04_CHILD"
	refundCrashNow      = "PAY_REF04_NOW"
	refundCrashPayNo    = "PA0000000000000000401"
	refundCrashOut      = "R04_CRASH"
	ref04Merchant       = "M100002"
	ref04Amount         = money.Amount(1000)
	childNotKilledExit  = 3
	// ref04Interval 是 job.RefundRecoverInterval 的值（REF-04 规则 1）。biz 测试不引用 job 包。
	ref04Interval = 10 * time.Second
)

// runRefundCrashChild 是“被 kill 的那个 payment 进程”：用真实的 fault.Init 打开 refund.after_freeze，
// 以真实 account biz 冻结后在故障点被 SIGKILL。走到 Point 之后说明故障点没生效，以非 0 退出。
func runRefundCrashChild() int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fault.Init(os.Getenv("FAULT_POINTS")); err != nil {
		fmt.Fprintf(os.Stderr, "fault init: %v\n", err)
		return 1
	}
	now, err := time.Parse(time.RFC3339Nano, os.Getenv(refundCrashNow))
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse now: %v\n", err)
		return 1
	}
	// DSN 带密码，从标准输入读，不放进子进程环境变量：依次是支付库、账户分片 0、账户分片 1，每行一个。
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read stdin: %v\n", err)
		return 1
	}
	dsns := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(dsns) != 3 {
		fmt.Fprintf(os.Stderr, "want 3 dsn lines, got %d\n", len(dsns))
		return 1
	}
	var dbs [3]*sqlx.DB
	for i, dsn := range dsns {
		// 错误可能带地址，只写序号。
		if dbs[i], err = dbx.Open(ctx, dsn, dbx.PoolConfig{}); err != nil {
			fmt.Fprintf(os.Stderr, "open dsn %d failed\n", i)
			return 1
		}
	}
	payDB, acc0, acc1 := dbs[0], dbs[1], dbs[2]
	clk := &clock.Fake{}
	clk.Set(now)
	ids, err := idgen.New(11, clk)
	if err != nil {
		fmt.Fprintf(os.Stderr, "idgen: %v\n", err)
		return 1
	}
	accIDs, err := idgen.New(12, clk)
	if err != nil {
		fmt.Fprintf(os.Stderr, "idgen: %v\n", err)
		return 1
	}
	accounts := accountbiz.New(accountdal.New(acc0, acc1), accIDs, 8)
	p := NewPayments(NewMerchants(payDB, nil, clk, 0), ids, clk, "merchant-demo", payDB)
	p.UseRefund(accountBizFreezer{svc: accounts},
		&fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_R04_CHILD"}}, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(ctx, &CreateRefundInput{
		MerchantID: ref04Merchant, PayNo: refundCrashPayNo, OutRefundNo: refundCrashOut, Amount: ref04Amount,
	})
	fmt.Fprintf(os.Stderr, "not killed: status=%s err=%v\n", got.Status, err)
	return childNotKilledExit
}

// refundAccounts 是两个账户分片的随机库和直接调用的 account biz。商户结算户已开户并有余额。
type refundAccounts struct {
	svc    *accountbiz.Service
	shards [2]*testkit.Schema
}

func openRefundAccounts(t *testing.T, env payEnv, merchantID string, balance int64, now time.Time) refundAccounts {
	t.Helper()
	my, err := testkit.Ready(env.ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	var out refundAccounts
	for i := range out.shards {
		s, openErr := my.OpenSchema(env.ctx, "account")
		if openErr != nil {
			t.Fatal(testkit.Redact(openErr))
		}
		t.Cleanup(func() { closeAccountSchema(t, s) })
		out.shards[i] = s
	}
	clk := &clock.Fake{}
	clk.Set(now)
	ids, err := idgen.New(9, clk)
	if err != nil {
		t.Fatal(err)
	}
	out.svc = accountbiz.New(accountdal.New(out.shards[0].DB, out.shards[1].DB), ids, 8)
	merchant, err := out.svc.OpenAccount(env.ctx, "MERCHANT", merchantID, "MERCHANT_SETTLE", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 余额走正常记账入账（渠道应收借、商户结算贷），CheckBalance 的流水和余额才对得上。
	channel, err := out.svc.OpenAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", merchant.Shard, 0)
	if err != nil {
		t.Fatal(err)
	}
	db := out.shards[merchant.Shard].DB
	channelID := refundAccountID(t, env.ctx, db, channel.AccountNo)
	merchantAccID := refundAccountID(t, env.ctx, db, merchant.AccountNo)
	err = dbx.WithTx(env.ctx, db, func(tx *sqlx.Tx) error {
		_, postErr := out.svc.Post(env.ctx, tx, "PAY", "FUND_R04_"+merchantID, "", []accountbiz.Entry{
			{AccountID: channelID, Direction: "DEBIT", Amount: money.Amount(balance)},
			{AccountID: merchantAccID, Direction: "CREDIT", Amount: money.Amount(balance)},
		})
		return postErr
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return out
}

func refundAccountID(t *testing.T, ctx context.Context, db *sqlx.DB, accountNo string) uint64 {
	t.Helper()
	var id uint64
	if err := db.QueryRowContext(ctx, `SELECT id FROM account WHERE account_no = ?`, accountNo).Scan(&id); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return id
}

// freezes 返回两个分片上该退款单号的冻结单张数，以及其中 FROZEN 的张数。
func (a refundAccounts) freezes(t *testing.T, ctx context.Context, refundNo string) (total, frozen int) {
	t.Helper()
	for _, s := range a.shards {
		var all, fz int
		err := s.DB.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(status = 'FROZEN'), 0) FROM account_freeze WHERE biz_type = 'REFUND' AND biz_no = ?`, refundNo).
			Scan(&all, &fz)
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
		total += all
		frozen += fz
	}
	return total, frozen
}

// checkBalance 先全量核对两个分片（需求 §0.3），再按商户模式核对这一户结算账户。
func (a refundAccounts) checkBalance(t *testing.T, ctx context.Context, merchantID string) {
	t.Helper()
	for _, id := range []string{"", merchantID} {
		check, err := a.svc.CheckBalance(ctx, id, false)
		if err != nil || !check.Passed {
			t.Fatalf("CheckBalance(%q) = %+v err=%v", id, check, err)
		}
	}
}

// switchFreezer 在 down 时模拟 account 超时（ErrUnavailable，与 accountRefundFreezer 的超时口径相同），
// 否则调用真实 account biz。lostReply 时真实冻结成功但响应丢失，同样返回 ErrUnavailable。
// calls 只数 down 期间与恢复后的全部调用。
type switchFreezer struct {
	inner     accountBizFreezer
	down      atomic.Bool
	lostReply atomic.Bool
	calls     atomic.Int32
}

func (s *switchFreezer) FreezeRefund(ctx context.Context, merchantID, refundNo string, amount money.Amount) (RefundFreeze, error) {
	s.calls.Add(1)
	if s.down.Load() {
		return RefundFreeze{}, errno.ErrUnavailable
	}
	got, err := s.inner.FreezeRefund(ctx, merchantID, refundNo, amount)
	if err == nil && s.lostReply.Load() {
		return RefundFreeze{}, errno.ErrUnavailable
	}
	return got, err
}

func paymentsClock(t *testing.T, p *Payments) *clock.Fake {
	t.Helper()
	clk, ok := p.clk.(*clock.Fake)
	if !ok {
		t.Fatal("payments clock is not fake")
	}
	return clk
}

func recoverBatch(t *testing.T, ctx context.Context, p *Payments) {
	t.Helper()
	if err := p.RunRefundRecoverBatch(ctx); err != nil {
		t.Fatalf("RunRefundRecoverBatch: %v", err)
	}
}

func refundByOut(t *testing.T, env payEnv, merchantID, outRefundNo string) (string, refundRow) {
	t.Helper()
	var refundNo string
	err := env.db.QueryRowContext(env.ctx,
		`SELECT refund_no FROM refund_order WHERE merchant_id = ? AND out_refund_no = ?`, merchantID, outRefundNo).Scan(&refundNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return refundNo, loadRefundRow(t, env, refundNo)
}

// reqRefunder 按 req_no 记录渠道退款次数，用来证明多实例下每笔只提交一次。
type reqRefunder struct {
	mu    sync.Mutex
	calls map[string]int
}

func (r *reqRefunder) Refund(_ context.Context, req ChannelRefundRequest) (ChannelAcceptResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls == nil {
		r.calls = map[string]int{}
	}
	r.calls[req.ReqNo]++
	return ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_" + req.ReqNo}, nil
}

func (r *reqRefunder) count(reqNo string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[reqNo]
}

func (r *reqRefunder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.calls {
		n += c
	}
	return n
}

func processingTxns(t *testing.T, env payEnv, refundNo string) int {
	t.Helper()
	var n int
	err := env.db.QueryRowContext(env.ctx, `
SELECT COUNT(*) FROM channel_txn WHERE biz_no = ? AND txn_type = 'REFUND' AND status = 'PROCESSING'`, refundNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

// REF-04 验收 1（C6）：子进程在故障点 refund.after_freeze 被 SIGKILL（冻结已提交，退款单仍是 INIT）。
// 用同一组库新建的 Payments 代表重启后的进程，下一轮扫描把退款推进为 PROCESSING 并提交渠道：
// 渠道受理 1 次，冻结单只有 1 张。再跑一轮不再提交。
func TestRefundRecoverAfterFreezeKill(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, refundCrashPayNo, "OT_R04_CRASH", payStatusSuccess, now)
	accounts := openRefundAccounts(t, env, ref04Merchant, 100000, now)

	cmd := exec.CommandContext(env.ctx, os.Args[0]) //nolint:gosec // 重新执行当前测试二进制，不带额外参数。
	cmd.Env = append(os.Environ(),
		refundCrashChildEnv+"=after_freeze",
		"FAULT_POINTS="+faultRefundAfterFreeze,
		refundCrashNow+"="+now.Format(time.RFC3339Nano),
	)
	cmd.Stdin = strings.NewReader(strings.Join(
		[]string{env.schema.DSN(), accounts.shards[0].DSN(), accounts.shards[1].DSN()}, "\n"))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil || !strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("child err=%v stderr=%s", err, testkit.Redact(errors.New(stderr.String())))
	}

	crashedNo, crashed := refundByOut(t, env, ref04Merchant, refundCrashOut)
	if crashed.status != refundStatusInit || crashed.amount != int64(ref04Amount) {
		t.Fatalf("after kill refund = %+v", crashed)
	}
	if total, frozen := accounts.freezes(t, env.ctx, crashedNo); total != 1 || frozen != 1 {
		t.Fatalf("after kill freezes total=%d frozen=%d", total, frozen)
	}
	if n := countBiz(t, env, "channel_txn", "biz_no", crashedNo); n != 0 {
		t.Fatalf("after kill channel_txn = %d", n)
	}

	// 重启：p 是新的进程状态，没有执行过这笔退款。
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_R04_1"}}
	p.UseRefund(accountBizFreezer{svc: accounts.svc}, channel, nil, testChannelNotifyURL)
	clk := paymentsClock(t, p)
	clk.Set(now.Add(refundRecoverIdle))
	recoverBatch(t, env.ctx, p)

	got := loadRefundRow(t, env, crashedNo)
	if got.status != refundStatusProcessing {
		t.Fatalf("after recover refund = %+v", got)
	}
	if channel.callCount() != 1 || channel.got().ReqNo != crashedNo || channel.got().OrigReqNo != refundCrashPayNo ||
		channel.got().Amount != ref04Amount {
		t.Fatalf("channel calls=%d req=%+v", channel.callCount(), channel.got())
	}
	if n := processingTxns(t, env, crashedNo); n != 1 {
		t.Fatalf("channel_txn = %d", n)
	}
	if total, frozen := accounts.freezes(t, env.ctx, crashedNo); total != 1 || frozen != 1 {
		t.Fatalf("after recover freezes total=%d frozen=%d", total, frozen)
	}

	clk.Advance(ref04Interval)
	recoverBatch(t, env.ctx, p)
	if channel.callCount() != 1 || processingTxns(t, env, crashedNo) != 1 {
		t.Fatalf("second batch channel=%d", channel.callCount())
	}
	if payRefundedAmount(t, env, refundCrashPayNo) != int64(ref04Amount) {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, refundCrashPayNo))
	}
	accounts.checkBalance(t, env.ctx, ref04Merchant)
}

// REF-04 验收 2：account 不可用期间退款保持 INIT（扫描到了、冻结失败，不调用渠道）；
// 恢复后的下一轮扫描把它推进为 PROCESSING 并提交渠道。updated_at 距今不足 30 秒的不扫描。
func TestRefundRecoverAfterAccountRecovers(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_DOWN", payStatusSuccess, now)
	accounts := openRefundAccounts(t, env, ref04Merchant, 100000, now)
	freezer := &switchFreezer{inner: accountBizFreezer{svc: accounts.svc}}
	freezer.down.Store(true)
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_R04_2"}}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)

	created, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: ref04Merchant, PayNo: ref03PayNo, OutRefundNo: "R04_DOWN", Amount: ref04Amount,
	})
	if err != nil || created.Status != refundStatusInit || freezer.calls.Load() != 1 {
		t.Fatalf("created = %+v err=%v freeze=%d", created, err, freezer.calls.Load())
	}

	clk := paymentsClock(t, p)
	clk.Set(created.UpdatedAt.Add(refundRecoverIdle - time.Millisecond))
	recoverBatch(t, env.ctx, p)
	if freezer.calls.Load() != 1 {
		t.Fatalf("scanned before 30s: freeze=%d", freezer.calls.Load())
	}

	clk.Set(created.UpdatedAt.Add(refundRecoverIdle))
	recoverBatch(t, env.ctx, p)
	if freezer.calls.Load() != 2 {
		t.Fatalf("not scanned at 30s: freeze=%d", freezer.calls.Load())
	}
	if row := loadRefundRow(t, env, created.RefundNo); row.status != refundStatusInit {
		t.Fatalf("account down refund = %+v", row)
	}
	if channel.callCount() != 0 || countBiz(t, env, "channel_txn", "biz_no", created.RefundNo) != 0 {
		t.Fatalf("channel=%d while account down", channel.callCount())
	}
	if total, _ := accounts.freezes(t, env.ctx, created.RefundNo); total != 0 {
		t.Fatalf("freezes while down = %d", total)
	}

	freezer.down.Store(false)
	clk.Advance(ref04Interval)
	recoverBatch(t, env.ctx, p)
	if row := loadRefundRow(t, env, created.RefundNo); row.status != refundStatusProcessing {
		t.Fatalf("after recovery refund = %+v", row)
	}
	if channel.callCount() != 1 || channel.got().ReqNo != created.RefundNo || processingTxns(t, env, created.RefundNo) != 1 {
		t.Fatalf("channel=%d txns=%d", channel.callCount(), processingTxns(t, env, created.RefundNo))
	}
	if total, frozen := accounts.freezes(t, env.ctx, created.RefundNo); total != 1 || frozen != 1 {
		t.Fatalf("freezes total=%d frozen=%d", total, frozen)
	}
	accounts.checkBalance(t, env.ctx, ref04Merchant)
}

// REF-04 验收 3：account 持续不可用（Fake 时钟）。创建后恰好 1 小时仍重试冻结；超过 1 小时按 REF-03 第 2 条
// 置 FAILED（REFUND_TIMEOUT），不再冻结，refunded_amount 恢复，outbox 1 条 refund.failed，渠道没有收到请求。
func TestRefundRecoverTimesOutAfterOneHour(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_TIMEOUT", payStatusSuccess, now)
	accounts := openRefundAccounts(t, env, ref04Merchant, 100000, now)
	freezer := &switchFreezer{inner: accountBizFreezer{svc: accounts.svc}}
	freezer.down.Store(true)
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK}}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
	created, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: ref04Merchant, PayNo: ref03PayNo, OutRefundNo: "R04_TIMEOUT", Amount: ref04Amount,
	})
	if err != nil || created.Status != refundStatusInit {
		t.Fatalf("created = %+v err=%v", created, err)
	}
	if payRefundedAmount(t, env, ref03PayNo) != int64(ref04Amount) {
		t.Fatalf("refunded after create = %d", payRefundedAmount(t, env, ref03PayNo))
	}

	clk := paymentsClock(t, p)
	// account 不可用的 1 小时里每 10 秒一轮，每轮都重试冻结，退款一直是 INIT。
	rounds := 0
	for at := refundRecoverIdle; at <= refundRecoverGiveUpAfter; at += ref04Interval {
		clk.Set(created.CreatedAt.Add(at))
		recoverBatch(t, env.ctx, p)
		rounds++
	}
	if row := loadRefundRow(t, env, created.RefundNo); row.status != refundStatusInit {
		t.Fatalf("refund at 1h = %+v", row)
	}
	if got := int(freezer.calls.Load()); got != 1+rounds {
		t.Fatalf("freeze calls = %d, want %d", got, 1+rounds)
	}

	clk.Set(created.CreatedAt.Add(refundRecoverGiveUpAfter + time.Millisecond))
	recoverBatch(t, env.ctx, p)
	row := loadRefundRow(t, env, created.RefundNo)
	if row.status != refundStatusFailed || row.failCode.String != failCodeRefundTimeout {
		t.Fatalf("after 1h refund = %+v", row)
	}
	if got := int(freezer.calls.Load()); got != 1+rounds {
		t.Fatalf("froze on timeout: calls = %d", got)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 0 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
	events := loadOutbox(t, env, created.RefundNo)
	if len(events) != 1 || events[0].eventType != "refund.failed" ||
		decodeRefundEvent(t, events[0].payload).Data.FailCode != failCodeRefundTimeout {
		t.Fatalf("outbox = %+v", events)
	}
	if channel.callCount() != 0 || countBiz(t, env, "channel_txn", "biz_no", created.RefundNo) != 0 {
		t.Fatalf("channel=%d", channel.callCount())
	}

	clk.Advance(ref04Interval)
	recoverBatch(t, env.ctx, p)
	if n := len(loadOutbox(t, env, created.RefundNo)); n != 1 {
		t.Fatalf("outbox after next batch = %d", n)
	}
	if total, _ := accounts.freezes(t, env.ctx, created.RefundNo); total != 0 {
		t.Fatalf("freezes = %d", total)
	}
	accounts.checkBalance(t, env.ctx, ref04Merchant)
}

// REF-04 规则 1：扫描只取 status = INIT 且 updated_at <= 截止时间的行，按 (updated_at, id) 升序，最多 limit 行。
func TestRefundRecoverScanSelectsStuckInit(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_SCAN", payStatusSuccess, now)
	// 先建 PROCESSING 那笔再往后拨时钟建 INIT：idgen 不允许时钟回拨。
	freezer := &fakeFreezer{status: freezeStatusFrozen}
	p.UseRefund(freezer, &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK}}, nil, testChannelNotifyURL)
	clk := paymentsClock(t, p)
	processing, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: ref04Merchant, PayNo: ref03PayNo, OutRefundNo: "R04_SCAN_P", Amount: 100,
	})
	if err != nil || processing.Status != refundStatusProcessing {
		t.Fatalf("processing = %+v err=%v", processing, err)
	}
	freezer.mu.Lock()
	freezer.err = errno.ErrUnavailable
	freezer.mu.Unlock()
	var inits []string
	for i := range 3 {
		clk.Set(now.Add(time.Duration(i) * 10 * time.Second))
		got, createErr := p.CreateRefund(env.ctx, &CreateRefundInput{
			MerchantID: ref04Merchant, PayNo: ref03PayNo, OutRefundNo: fmt.Sprintf("R04_SCAN_%d", i), Amount: 100,
		})
		if createErr != nil || got.Status != refundStatusInit {
			t.Fatalf("init %d = %+v err=%v", i, got, createErr)
		}
		inits = append(inits, got.RefundNo)
	}
	store := sqlOrderStore{db: env.db}
	cases := []struct {
		cutoff time.Time
		limit  int
		want   []string
	}{
		{now.Add(-time.Millisecond), 10, nil},
		{now, 10, inits[:1]},
		{now.Add(10 * time.Second), 10, inits[:2]},
		{now.Add(time.Hour), 10, inits},
		{now.Add(time.Hour), 2, inits[:2]},
	}
	scan := func(cutoff time.Time, after *refundCursor, limit int) ([]string, refundCursor) {
		t.Helper()
		rows, last, scanErr := store.stuckRefunds(env.ctx, cutoff, after, limit)
		if scanErr != nil {
			t.Fatal(testkit.Redact(scanErr))
		}
		var got []string
		for _, r := range rows {
			if r.Status != refundStatusInit || r.MerchantID != ref04Merchant || r.PayNo != ref03PayNo || r.Amount != 100 || r.Channel != channelMockpay {
				t.Fatalf("row = %+v", r)
			}
			got = append(got, r.RefundNo)
		}
		return got, last
	}
	for _, c := range cases {
		if got, _ := scan(c.cutoff, nil, c.limit); strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Fatalf("cutoff %s limit %d got %v want %v", c.cutoff, c.limit, got, c.want)
		}
	}
	// 翻页：游标是上一页最后一行，下一页从它之后开始；截止时间仍然生效；读完后返回零值游标。
	page1, cur := scan(now.Add(time.Hour), nil, 2)
	if strings.Join(page1, ",") != strings.Join(inits[:2], ",") || !cur.updatedAt.Equal(now.Add(10*time.Second)) || cur.id == 0 {
		t.Fatalf("page1 = %v cursor = %+v", page1, cur)
	}
	if page2, cur2 := scan(now.Add(time.Hour), &cur, 2); strings.Join(page2, ",") != strings.Join(inits[2:], ",") || cur2.id <= cur.id {
		t.Fatalf("page2 = %v cursor = %+v", page2, cur2)
	}
	if page2, _ := scan(now.Add(10*time.Second), &cur, 2); len(page2) != 0 {
		t.Fatalf("page2 past cutoff = %v", page2)
	}
	if page3, cur3 := scan(now.Add(time.Hour), &refundCursor{updatedAt: now.Add(20 * time.Second), id: ^uint64(0)}, 2); len(page3) != 0 || cur3 != (refundCursor{}) {
		t.Fatalf("page3 = %v cursor = %+v", page3, cur3)
	}
}

// insertStuckRefunds 直接插入 n 笔 INIT 退款，created_at 与 updated_at 都是 at（同一毫秒，翻页只能靠 id 区分）。
func insertStuckRefunds(t *testing.T, env payEnv, n int, at time.Time) []string {
	t.Helper()
	nos := make([]string, n)
	for i := range nos {
		nos[i] = fmt.Sprintf("RF%019d", 4000+i)
		_, err := env.db.ExecContext(env.ctx, `
INSERT INTO refund_order (refund_no, merchant_id, out_refund_no, pay_no, amount, status, channel, created_at, updated_at)
VALUES (?, ?, ?, ?, 1, 'INIT', 'mockpay', ?, ?)`, nos[i], ref04Merchant, fmt.Sprintf("R04_PAGE_%d", i), ref03PayNo, at, at)
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
	return nos
}

// countingFreezer 按退款单号计数，一律返回不可用（退款保持 INIT）。onCall 不为 nil 时每次调用先执行它。
type countingFreezer struct {
	mu     sync.Mutex
	calls  map[string]int
	onCall func()
}

func (f *countingFreezer) FreezeRefund(_ context.Context, _, refundNo string, _ money.Amount) (RefundFreeze, error) {
	if f.onCall != nil {
		f.onCall()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[refundNo]++
	return RefundFreeze{}, errno.ErrUnavailable
}

func (f *countingFreezer) snapshot() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.calls))
	for k, v := range f.calls {
		out[k] = v
	}
	return out
}

// 审查 L3：一轮按 (updated_at, id) 翻页，排在前面的整页卡单不再挡住后面的行。
// 105 笔同一毫秒的 INIT 退款，一轮里每笔恰好冻结 1 次（第二页靠 id 续上，不重复、不遗漏）。
func TestRefundRecoverPagesPastFullPage(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_PAGE", payStatusSuccess, now)
	nos := insertStuckRefunds(t, env, refundRecoverBatchSize+5, now.Add(-time.Minute))
	freezer := &countingFreezer{}
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK}}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
	recoverBatch(t, env.ctx, p)
	calls := freezer.snapshot()
	if len(calls) != len(nos) {
		t.Fatalf("frozen %d of %d refunds", len(calls), len(nos))
	}
	for _, no := range nos {
		if calls[no] != 1 {
			t.Fatalf("%s freeze calls = %d", no, calls[no])
		}
	}
	if channel.callCount() != 0 {
		t.Fatalf("channel = %d", channel.callCount())
	}
}

// 审查 L3：一轮已用满 8 秒（Fake 时钟）就不再翻下一页，剩下的行留到下一轮。
func TestRefundRecoverStopsPagingAfterBudget(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_BUDGET", payStatusSuccess, now)
	nos := insertStuckRefunds(t, env, refundRecoverBatchSize+5, now.Add(-time.Minute))
	clk := paymentsClock(t, p)
	var once sync.Once
	freezer := &countingFreezer{onCall: func() { once.Do(func() { clk.Advance(refundRecoverRoundBudget) }) }}
	p.UseRefund(freezer, &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK}}, nil, testChannelNotifyURL)
	recoverBatch(t, env.ctx, p)
	calls := freezer.snapshot()
	if len(calls) != refundRecoverBatchSize {
		t.Fatalf("frozen %d refunds, want one page of %d", len(calls), refundRecoverBatchSize)
	}
	for _, no := range nos[refundRecoverBatchSize:] {
		if calls[no] != 0 {
			t.Fatalf("%s frozen after budget", no)
		}
	}
}

// 审查 L5：MySQL 存储上未装配冻结或渠道退款时返回 10003，不扫描、不冻结、不改退款单；
// 批次 ctx 已取消时原样返回取消错误。去掉空值检查后 submitRefund 会把退款原样返回，批次返回 nil，这里就会失败。
func TestRunRefundRecoverBatchNotWiredMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_WIRE", payStatusSuccess, now)
	nos := insertStuckRefunds(t, env, 1, now.Add(-time.Minute))
	freezer := &countingFreezer{}
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK}}
	cases := []struct {
		name    string
		freezer RefundFreezer
		channel ChannelRefunder
	}{
		{"no freezer", nil, channel},
		{"no channel", freezer, nil},
	}
	for _, c := range cases {
		p.UseRefund(c.freezer, c.channel, nil, testChannelNotifyURL)
		if err := p.RunRefundRecoverBatch(env.ctx); !errors.Is(err, errno.ErrInternal) {
			t.Fatalf("%s = %v", c.name, err)
		}
	}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
	canceled, cancel := context.WithCancel(env.ctx)
	cancel()
	if err := p.RunRefundRecoverBatch(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	if calls := freezer.snapshot(); len(calls) != 0 || channel.callCount() != 0 {
		t.Fatalf("freeze=%v channel=%d", calls, channel.callCount())
	}
	if row := loadRefundRow(t, env, nos[0]); row.status != refundStatusInit {
		t.Fatalf("refund = %+v", row)
	}
}

// NFR-REL-02：两个 payment 实例同时扫描同一批卡单，每笔退款仍只冻结 1 张、只提交渠道 1 次、只有 1 条渠道流水。
func TestRefundRecoverTwoInstances(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p1 := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_TWO", payStatusSuccess, now)
	accounts := openRefundAccounts(t, env, ref04Merchant, 100000, now)
	down := &switchFreezer{inner: accountBizFreezer{svc: accounts.svc}}
	down.down.Store(true)
	p1.UseRefund(down, &fakeRefunder{}, nil, testChannelNotifyURL)
	const n = 5
	refunds := make([]string, 0, n)
	for i := range n {
		got, err := p1.CreateRefund(env.ctx, &CreateRefundInput{
			MerchantID: ref04Merchant, PayNo: ref03PayNo, OutRefundNo: fmt.Sprintf("R04_TWO_%d", i), Amount: 1000,
		})
		if err != nil || got.Status != refundStatusInit {
			t.Fatalf("create %d = %+v err=%v", i, got, err)
		}
		refunds = append(refunds, got.RefundNo)
	}
	clk2 := &clock.Fake{}
	ids2, err := idgen.New(8, clk2)
	if err != nil {
		t.Fatal(err)
	}
	p2 := NewPayments(NewMerchants(env.db, nil, clk2, 0), ids2, clk2, "merchant-demo", env.db)
	channel := &reqRefunder{}
	p1.UseRefund(accountBizFreezer{svc: accounts.svc}, channel, nil, testChannelNotifyURL)
	p2.UseRefund(accountBizFreezer{svc: accounts.svc}, channel, nil, testChannelNotifyURL)
	at := now.Add(refundRecoverIdle)
	paymentsClock(t, p1).Set(at)
	clk2.Set(at)

	start := make(chan struct{})
	var g errgroup.Group
	for _, p := range []*Payments{p1, p2} {
		g.Go(func() error {
			<-start
			return p.RunRefundRecoverBatch(env.ctx)
		})
	}
	close(start)
	if err = g.Wait(); err != nil {
		t.Fatal(err)
	}
	if channel.total() != n {
		t.Fatalf("channel calls = %d", channel.total())
	}
	for _, refundNo := range refunds {
		if row := loadRefundRow(t, env, refundNo); row.status != refundStatusProcessing {
			t.Fatalf("%s = %+v", refundNo, row)
		}
		if got := processingTxns(t, env, refundNo); got != 1 || channel.count(refundNo) != 1 {
			t.Fatalf("%s channel_txn = %d channel = %d", refundNo, got, channel.count(refundNo))
		}
		if total, frozen := accounts.freezes(t, env.ctx, refundNo); total != 1 || frozen != 1 {
			t.Fatalf("%s freezes total=%d frozen=%d", refundNo, total, frozen)
		}
	}
	accounts.checkBalance(t, env.ctx, ref04Merchant)
}

// 审查残余风险：冻结在 account 已成功、但 payment 一直没拿到结果（响应丢失）。超过 1 小时后照样置 FAILED
// （REFUND_TIMEOUT），refunded_amount 恢复，outbox 1 条 refund.failed；冻结单仍是 FROZEN，等 MQ-04 消费
// refund.failed 后释放。渠道没有收到请求。
func TestRefundRecoverTimesOutAfterFreeze(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_FROZEN", payStatusSuccess, now)
	accounts := openRefundAccounts(t, env, ref04Merchant, 100000, now)
	freezer := &switchFreezer{inner: accountBizFreezer{svc: accounts.svc}}
	freezer.lostReply.Store(true)
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK}}
	p.UseRefund(freezer, channel, nil, testChannelNotifyURL)
	created, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: ref04Merchant, PayNo: ref03PayNo, OutRefundNo: "R04_FROZEN", Amount: ref04Amount,
	})
	if err != nil || created.Status != refundStatusInit {
		t.Fatalf("created = %+v err=%v", created, err)
	}
	clk := paymentsClock(t, p)
	clk.Set(created.CreatedAt.Add(refundRecoverGiveUpAfter))
	recoverBatch(t, env.ctx, p)
	if total, frozen := accounts.freezes(t, env.ctx, created.RefundNo); total != 1 || frozen != 1 {
		t.Fatalf("before timeout freezes total=%d frozen=%d", total, frozen)
	}
	if row := loadRefundRow(t, env, created.RefundNo); row.status != refundStatusInit {
		t.Fatalf("at 1h refund = %+v", row)
	}

	clk.Set(created.CreatedAt.Add(refundRecoverGiveUpAfter + time.Millisecond))
	recoverBatch(t, env.ctx, p)
	row := loadRefundRow(t, env, created.RefundNo)
	if row.status != refundStatusFailed || row.failCode.String != failCodeRefundTimeout {
		t.Fatalf("after 1h refund = %+v", row)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 0 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
	events := loadOutbox(t, env, created.RefundNo)
	if len(events) != 1 || events[0].eventType != "refund.failed" ||
		decodeRefundEvent(t, events[0].payload).Data.FailCode != failCodeRefundTimeout {
		t.Fatalf("outbox = %+v", events)
	}
	if total, frozen := accounts.freezes(t, env.ctx, created.RefundNo); total != 1 || frozen != 1 {
		t.Fatalf("after timeout freezes total=%d frozen=%d", total, frozen)
	}
	if channel.callCount() != 0 || countBiz(t, env, "channel_txn", "biz_no", created.RefundNo) != 0 {
		t.Fatalf("channel=%d", channel.callCount())
	}
	accounts.checkBalance(t, env.ctx, ref04Merchant)
}

// 审查 L2（MySQL）：判超时时另一路已推进为 PROCESSING，ApplyRefundResult 返回 10003 并记 WARN；
// 恢复任务重读后只记 INFO，没有 ERROR，退款与额度不变。
func TestRefundRecoverTimeoutLostRaceMySQL(t *testing.T) {
	env := openPay(t)
	p, proc := processingRefund(t, env, ref04Merchant, 2000)
	stale := proc
	stale.Status = refundStatusInit
	logs := captureLog(t)
	p.timeoutRefund(env.ctx, stale)
	if errs, _ := countErrorLines(logs, ""); errs != 0 {
		t.Fatalf("error logs = %s", logs.String())
	}
	if _, matched := countLevelLines(logs, "WARN", "refund result not init"); matched != 1 {
		t.Fatalf("warn logs = %s", logs.String())
	}
	if _, matched := countLevelLines(logs, "INFO", "refund recover timeout lost race"); matched != 1 {
		t.Fatalf("info logs = %s", logs.String())
	}
	if row := loadRefundRow(t, env, proc.RefundNo); row.status != refundStatusProcessing {
		t.Fatalf("refund = %+v", row)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 2000 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
}

// 审查 L2（MySQL）：额度已不一致（REF-03 第 5 条，refunded_amount < R）时置超时失败回滚并返回 10003，
// 重读仍是 INIT，恢复任务记 ERROR；退款保持 INIT，没有 refund.failed。
func TestRefundRecoverTimeoutQuotaMismatchMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, ref04Merchant, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, ref04Merchant, ref03PayNo, "OT_R04_QUOTA", payStatusSuccess, now)
	p.UseRefund(&fakeFreezer{err: errno.ErrUnavailable}, &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK}}, nil, testChannelNotifyURL)
	created, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: ref04Merchant, PayNo: ref03PayNo, OutRefundNo: "R04_QUOTA", Amount: ref04Amount,
	})
	if err != nil || created.Status != refundStatusInit {
		t.Fatalf("created = %+v err=%v", created, err)
	}
	if _, err = env.db.ExecContext(env.ctx, `UPDATE payment_order SET refunded_amount = 999 WHERE pay_no = ?`, ref03PayNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	paymentsClock(t, p).Set(created.CreatedAt.Add(2 * time.Hour))
	logs := captureLog(t)
	p.timeoutRefund(env.ctx, created)
	if _, matched := countErrorLines(logs, "refund recover timeout"); matched != 1 {
		t.Fatalf("error logs = %s", logs.String())
	}
	if _, matched := countLevelLines(logs, "INFO", "refund recover timeout lost race"); matched != 0 {
		t.Fatalf("info logs = %s", logs.String())
	}
	if row := loadRefundRow(t, env, created.RefundNo); row.status != refundStatusInit {
		t.Fatalf("refund = %+v", row)
	}
	if n := len(loadOutbox(t, env, created.RefundNo)); n != 0 {
		t.Fatalf("outbox = %d", n)
	}
}
