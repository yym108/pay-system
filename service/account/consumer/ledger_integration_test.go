//go:build integration

package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/account/biz"
	"github.com/yym108/pay-system/service/account/dal"
)

const (
	mq04Child = "MQ04_CHILD"
	subCount  = 8
)

func TestMQ04Accept1DuplicateOnce(t *testing.T) {
	// MQ-04 验收 1：同一条 payment.succeeded 投递 5 次，只有 1 张 PAY 凭证。
	rig := newRig(t)
	const merchantID = "M100002"
	const payNo = "PA-MQ04-1"
	rig.openBooks(merchantID)
	body := payEnvelope("EV-MQ04-1", merchantID, payNo, 10000, 60)
	for i := 0; i < 5; i++ {
		rig.produce(TopicPaymentSucceeded, "EV-MQ04-1", eventPaymentSucceeded, payNo, body)
	}
	c := rig.openConsumer(0)
	before := c.DLQTotal(TopicPaymentSucceeded)
	rig.serve(c)
	rig.waitOffset(5)
	if n := rig.payCount(merchantID, payNo); n != 1 {
		t.Fatalf("vouchers = %d", n)
	}
	if c.DLQTotal(TopicPaymentSucceeded) != before {
		t.Fatal("duplicate went to dlq")
	}
	rig.check()
}

func TestMQ04Accept2KillAfterCommit(t *testing.T) {
	// MQ-04 验收 2：ledger.after_commit 杀掉进程，重启后再消费，凭证仍是 1 张。
	if os.Getenv(mq04Child) != "" {
		t.Skip()
	}
	_ = fault.Init("")
	rig := newRig(t)
	const merchantID = "M100002"
	const payNo = "PA-MQ04-2"
	rig.openBooks(merchantID)
	body := payEnvelope("EV-MQ04-2", merchantID, payNo, 8000, 40)
	rig.produce(TopicPaymentSucceeded, "EV-MQ04-2", eventPaymentSucceeded, payNo, body)

	killCtx, killCancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer killCancel()
	kill := exec.CommandContext(killCtx, os.Args[0], "-test.run", "^TestMQ04Accept2Child$", "-test.count=1", "-test.timeout=60s") //nolint:gosec // G204: 二进制是当前测试，参数固定。
	kill.Env = childEnv(rig, merchantID, payNo)
	out, err := kill.CombinedOutput()
	if err == nil {
		t.Fatalf("child exited 0: %s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.Error() != "signal: killed" {
		t.Fatalf("child err = %v output = %s", err, out)
	}
	if n := rig.payCount(merchantID, payNo); n != 1 {
		t.Fatalf("vouchers after kill = %d", n)
	}
	off, ferr := groupOffset(rig.broker, GroupName, TopicPaymentSucceeded, 0)
	if ferr != nil {
		t.Fatal(ferr)
	}
	if off >= 0 {
		t.Fatalf("offset committed after kill: %d", off)
	}

	c := rig.openConsumer(6 * time.Second)
	rig.serve(c)
	rig.waitOffset(1)
	if n := rig.payCount(merchantID, payNo); n != 1 {
		t.Fatalf("vouchers after replay = %d", n)
	}
	rig.check()
}

func TestMQ04Accept2Child(t *testing.T) {
	if os.Getenv(mq04Child) == "" {
		t.Skip()
	}
	if err := fault.Init(os.Getenv("FAULT_POINTS"), fault.WithKill(func() {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	pools, err := dal.Open(ctx, os.Getenv("MQ04_DSN0"), os.Getenv("MQ04_DSN1"), dbx.PoolConfig{})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = pools.Close() }()
	ids, err := idgen.New(12, clock.Real{})
	if err != nil {
		t.Fatal(err)
	}
	svc := biz.New(pools, ids, subCount)
	c, err := Open(Config{
		Brokers:        []string{os.Getenv("MQ04_BROKER")},
		Ledger:         svc,
		Sleeper:        func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		SessionTimeout: 6 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Serve() }()
	select {
	case err = <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("child was not killed")
	}
}

func TestMQ04Accept3MissingAccountDLQ(t *testing.T) {
	// MQ-04 验收 3：商户账户不存在的消息进 pay.dlq，后面的消息仍入账。
	rig := newRig(t)
	const missing = "M100002"
	const next = "M100004"
	rig.openBooks(next)
	bad := payEnvelope("EV-MISS", missing, "PA-MISS", 1000, 10)
	good := payEnvelope("EV-NEXT", next, "PA-NEXT", 1000, 10)
	rig.produce(TopicPaymentSucceeded, "EV-MISS", eventPaymentSucceeded, "PA-MISS", bad)
	rig.produce(TopicPaymentSucceeded, "EV-NEXT", eventPaymentSucceeded, "PA-NEXT", good)
	c := rig.openConsumer(0)
	before := c.DLQTotal(TopicPaymentSucceeded)
	rig.serve(c)
	rig.waitPay(next, "PA-NEXT", 1)
	rig.waitDLQ("EV-MISS")
	if n := rig.payCount(missing, "PA-MISS"); n != 0 {
		t.Fatalf("missing vouchers = %d", n)
	}
	if c.DLQTotal(TopicPaymentSucceeded) != before+1 {
		t.Fatalf("dlq = %d before %d", c.DLQTotal(TopicPaymentSucceeded), before)
	}
	rig.check()
}

func TestMQ04Accept4InactiveDLQ(t *testing.T) {
	// MQ-04 验收 4：商户结算账户非 ACTIVE，不重试，进 DLQ，pay_mq_dlq_total 加 1，一条 ERROR，没有新凭证，后续消息正常。
	rig := newRig(t)
	const inactive = "M100002"
	const next = "M100004"
	rig.openBooks(inactive)
	rig.openBooks(next)
	if err := rig.setStatus(inactive, "FROZEN"); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	logs := &bytes.Buffer{}
	logx.SetDefault("account", logs)
	t.Cleanup(func() { logx.SetDefault("account", io.Discard) })
	bad := payEnvelope("EV-OFF", inactive, "PA-OFF", 1500, 15)
	good := payEnvelope("EV-ON", next, "PA-ON", 1500, 15)
	rig.produce(TopicPaymentSucceeded, "EV-OFF", eventPaymentSucceeded, "PA-OFF", bad)
	rig.produce(TopicPaymentSucceeded, "EV-ON", eventPaymentSucceeded, "PA-ON", good)
	c := rig.openConsumer(0)
	beforeDLQ := c.DLQTotal(TopicPaymentSucceeded)
	beforeErr := c.ConsumeErrors(TopicPaymentSucceeded)
	rig.serve(c)
	rig.waitPay(next, "PA-ON", 1)
	rig.waitDLQ("EV-OFF")
	if n := rig.payCount(inactive, "PA-OFF"); n != 0 {
		t.Fatalf("inactive vouchers = %d", n)
	}
	if c.DLQTotal(TopicPaymentSucceeded) != beforeDLQ+1 {
		t.Fatalf("dlq = %d before %d", c.DLQTotal(TopicPaymentSucceeded), beforeDLQ)
	}
	if c.ConsumeErrors(TopicPaymentSucceeded) != beforeErr {
		t.Fatalf("retried %d before %d", c.ConsumeErrors(TopicPaymentSucceeded), beforeErr)
	}
	line := errorLine(logs, "ledger account inactive")
	if line == "" || !strings.Contains(line, `"event_id":"EV-OFF"`) ||
		!strings.Contains(line, `"event_type":"payment.succeeded"`) ||
		!strings.Contains(line, `"merchant_id":"M100002"`) ||
		strings.Contains(line, "OT-SECRET-NO") || strings.Contains(line, rig.broker) {
		t.Fatalf("logs = %s", logs.String())
	}
	rig.check()
}

func TestMQ04Accept5ZeroFee(t *testing.T) {
	// MQ-04 验收 5：fee_amount = 0 时凭证只有借渠道应收和贷商户，手续费收入不变。
	rig := newRig(t)
	const merchantID = "M100002"
	rig.openBooks(merchantID)
	feeBefore := rig.sumBalance(merchantID, "PLATFORM", "PLATFORM", "FEE_INCOME")
	body := payEnvelope("EV-ZERO", merchantID, "PA-ZERO", 4200, 0)
	rig.produce(TopicPaymentSucceeded, "EV-ZERO", eventPaymentSucceeded, "PA-ZERO", body)
	rig.serve(rig.openConsumer(0))
	rig.waitPay(merchantID, "PA-ZERO", 1)
	if n := rig.entryCount(merchantID, "PA-ZERO"); n != 2 {
		t.Fatalf("entries = %d", n)
	}
	if got := rig.sumBalance(merchantID, "MERCHANT", merchantID, "MERCHANT_SETTLE"); got != 4200 {
		t.Fatalf("merchant = %d", got)
	}
	if got := rig.sumBalance(merchantID, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE"); got != 4200 {
		t.Fatalf("channel = %d", got)
	}
	if got := rig.sumBalance(merchantID, "PLATFORM", "PLATFORM", "FEE_INCOME"); got != feeBefore {
		t.Fatalf("fee = %d before %d", got, feeBefore)
	}
	rig.check()
}

func TestMQ04FreezeConflictNoRetry(t *testing.T) {
	// MQ-04：40004 不重试，进程内计数加 1，后续消息仍入账。
	rig := newRig(t)
	const merchantID = "M100002"
	rig.openBooks(merchantID)
	logs := &bytes.Buffer{}
	logx.SetDefault("account", logs)
	t.Cleanup(func() { logx.SetDefault("account", io.Discard) })
	before := FreezeConflictTotal()
	bad := refundEnvelope("EV-40004", eventRefundSucceeded, "RF-MISS", "PA-X", merchantID, "R1", "mockpay", 3000, true)
	good := payEnvelope("EV-AFTER-40004", merchantID, "PA-AFTER", 900, 0)
	rig.produce(TopicRefundSucceeded, "EV-40004", eventRefundSucceeded, "RF-MISS", bad)
	rig.produce(TopicPaymentSucceeded, "EV-AFTER-40004", eventPaymentSucceeded, "PA-AFTER", good)
	c := rig.openConsumer(0)
	beforeDLQ := c.DLQTotal(TopicRefundSucceeded)
	beforeErr := c.ConsumeErrors(TopicRefundSucceeded)
	rig.serve(c)
	rig.waitPay(merchantID, "PA-AFTER", 1)
	rig.waitDLQ("EV-40004")
	if FreezeConflictTotal() != before+1 {
		t.Fatalf("counter = %d before %d", FreezeConflictTotal(), before)
	}
	if c.ConsumeErrors(TopicRefundSucceeded) != beforeErr || c.DLQTotal(TopicRefundSucceeded) != beforeDLQ+1 {
		t.Fatalf("errors=%d dlq=%d", c.ConsumeErrors(TopicRefundSucceeded), c.DLQTotal(TopicRefundSucceeded))
	}
	_, matched := countMsg(logs, "ledger freeze conflict")
	if matched != 1 {
		t.Fatalf("logs = %s", logs.String())
	}
	rig.check()
}

func TestMQ04RefundInactiveChannelDLQ(t *testing.T) {
	// MQ-04：渠道应收非 ACTIVE 时 refund.succeeded 进 DLQ，不重试，冻结单仍是 FROZEN，同分区后续消息仍能处理。
	rig := newRig(t)
	const merchantID = "M100002"
	rig.openBooks(merchantID)
	badNo, goodNo, badSub := refundPair(t)
	if _, err := rig.svc.PostPay(rig.ctx, "PA-SEED", merchantID, "mockpay", 5000, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := rig.svc.Freeze(rig.ctx, bizRefund, badNo, merchantID, money.Amount(800)); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := rig.svc.Freeze(rig.ctx, bizRefund, goodNo, merchantID, money.Amount(800)); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	res, err := rig.db(merchantID).ExecContext(rig.ctx, `
UPDATE account SET status = 'CLOSED'
WHERE owner_type = 'PLATFORM' AND owner_id = 'mockpay' AND account_type = 'CHANNEL_RECEIVABLE' AND sub_no = ?`, badSub)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("channel rows = %d err=%v", n, testkit.Redact(err))
	}
	logs := &bytes.Buffer{}
	logx.SetDefault("account", logs)
	t.Cleanup(func() { logx.SetDefault("account", io.Discard) })
	beforeConflict := FreezeConflictTotal()
	bad := refundEnvelope("EV-CH-OFF", eventRefundSucceeded, badNo, "PA-SEED", merchantID, "R-CH-OFF", "mockpay", 800, true)
	good := refundEnvelope("EV-CH-OK", eventRefundSucceeded, goodNo, "PA-SEED", merchantID, "R-CH-OK", "mockpay", 800, true)
	rig.produce(TopicRefundSucceeded, "EV-CH-OFF", eventRefundSucceeded, badNo, bad)
	rig.produce(TopicRefundSucceeded, "EV-CH-OK", eventRefundSucceeded, goodNo, good)
	c := rig.openConsumer(0)
	beforeDLQ := c.DLQTotal(TopicRefundSucceeded)
	beforeErr := c.ConsumeErrors(TopicRefundSucceeded)
	rig.serve(c)
	rig.waitRefund(merchantID, goodNo, 1)
	rig.waitDLQ("EV-CH-OFF")
	if status := rig.freezeStatus(merchantID, badNo); status != "FROZEN" {
		t.Fatalf("freeze = %s", status)
	}
	if n := rig.refundCount(merchantID, badNo); n != 0 {
		t.Fatalf("inactive vouchers = %d", n)
	}
	if FreezeConflictTotal() != beforeConflict {
		t.Fatalf("counter = %d before %d", FreezeConflictTotal(), beforeConflict)
	}
	if c.DLQTotal(TopicRefundSucceeded) != beforeDLQ+1 || c.ConsumeErrors(TopicRefundSucceeded) != beforeErr {
		t.Fatalf("dlq=%d beforeDLQ=%d errors=%d beforeErr=%d",
			c.DLQTotal(TopicRefundSucceeded), beforeDLQ, c.ConsumeErrors(TopicRefundSucceeded), beforeErr)
	}
	line := errorLine(logs, "ledger account inactive")
	if line == "" || !strings.Contains(line, `"event_id":"EV-CH-OFF"`) ||
		!strings.Contains(line, `"event_type":"refund.succeeded"`) ||
		!strings.Contains(line, `"merchant_id":"M100002"`) ||
		strings.Contains(line, badNo) || strings.Contains(line, "pay_mq_dlq_total") {
		t.Fatalf("logs = %s", logs.String())
	}
	rig.check()
}

func refundPair(t *testing.T) (badNo, goodNo string, badSub int) {
	t.Helper()
	badNo = "RF-CH-OFF"
	badSub, err := shard.Sub(badNo, subCount)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		goodNo = fmt.Sprintf("RF-CH-OK-%d", i)
		goodSub, subErr := shard.Sub(goodNo, subCount)
		if subErr != nil {
			t.Fatal(subErr)
		}
		if goodSub != badSub {
			return badNo, goodNo, badSub
		}
	}
	t.Fatal("no distinct channel sub")
	return "", "", 0
}

func TestMQ04ParseFailureDLQ(t *testing.T) {
	// MQ-04：解析失败进 DLQ，不记账，后面的消息仍入账。
	rig := newRig(t)
	const merchantID = "M100002"
	rig.openBooks(merchantID)
	rig.produce(TopicPaymentSucceeded, "EV-BAD", eventPaymentSucceeded, "PA-BAD", []byte("{"))
	good := payEnvelope("EV-GOOD", merchantID, "PA-GOOD", 700, 0)
	rig.produce(TopicPaymentSucceeded, "EV-GOOD", eventPaymentSucceeded, "PA-GOOD", good)
	c := rig.openConsumer(0)
	before := c.DLQTotal(TopicPaymentSucceeded)
	rig.serve(c)
	rig.waitPay(merchantID, "PA-GOOD", 1)
	if n := rig.payCount(merchantID, "PA-BAD"); n != 0 {
		t.Fatalf("bad vouchers = %d", n)
	}
	if c.DLQTotal(TopicPaymentSucceeded) != before+1 {
		t.Fatalf("dlq = %d before %d", c.DLQTotal(TopicPaymentSucceeded), before)
	}
	if total := rig.payTotal(merchantID); total != 1 {
		t.Fatalf("pay vouchers = %d", total)
	}
	rig.check()
}

type rig struct {
	t      *testing.T
	ctx    context.Context
	broker string
	s0     *testkit.Schema
	s1     *testkit.Schema
	svc    *biz.Service
}

func newRig(t *testing.T) *rig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	k, err := testkit.StartKafka(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if err := k.Close(cctx); err != nil {
			t.Errorf("close kafka: %s", testkit.Redact(err))
		}
	})
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	s0, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, s0) })
	s1, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, s1) })
	pools, err := dal.Open(ctx, s0.DSN(), s1.DSN(), dbx.PoolConfig{})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { _ = pools.Close() })
	ids, err := idgen.New(11, clock.Real{})
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, ctx: ctx, broker: k.Broker, s0: s0, s1: s1, svc: biz.New(pools, ids, subCount)}
	r.createTopics()
	return r
}

func closeSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}

func (r *rig) db(merchantID string) *sqlx.DB {
	r.t.Helper()
	n, err := shard.Of(merchantID)
	if err != nil {
		r.t.Fatal(err)
	}
	if n == 0 {
		return r.s0.DB
	}
	return r.s1.DB
}

func (r *rig) openBooks(merchantID string) {
	r.t.Helper()
	n, err := shard.Of(merchantID)
	if err != nil {
		r.t.Fatal(err)
	}
	if _, err = r.svc.OpenAccount(r.ctx, "MERCHANT", merchantID, "MERCHANT_SETTLE", n, 0); err != nil {
		r.t.Fatal(err)
	}
	for sub := 0; sub < subCount; sub++ {
		if _, err = r.svc.OpenAccount(r.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", n, sub); err != nil {
			r.t.Fatal(err)
		}
		if _, err = r.svc.OpenAccount(r.ctx, "PLATFORM", "PLATFORM", "FEE_INCOME", n, sub); err != nil {
			r.t.Fatal(err)
		}
	}
}

func (r *rig) setStatus(merchantID, status string) error {
	res, err := r.db(merchantID).ExecContext(r.ctx, `
UPDATE account SET status = ?
WHERE owner_type = 'MERCHANT' AND owner_id = ? AND account_type = 'MERCHANT_SETTLE'`, status, merchantID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("status rows %d", n)
	}
	return nil
}

func (r *rig) createTopics() {
	r.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.broker))
	if err != nil {
		r.t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(r.ctx, 20*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Version = 7
	req.TimeoutMillis = 15000
	for _, name := range []string{TopicPaymentSucceeded, TopicRefundSucceeded, TopicRefundFailed, mq.DLQTopic} {
		req.Topics = append(req.Topics, kmsg.CreateTopicsRequestTopic{
			Topic: name, NumPartitions: 1, ReplicationFactor: 1,
		})
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, topic := range resp.Topics {
		if topic.ErrorCode == 0 || errors.Is(kerr.ErrorForCode(topic.ErrorCode), kerr.TopicAlreadyExists) {
			continue
		}
		r.t.Fatalf("create topic %s: code %d", topic.Topic, topic.ErrorCode)
	}
}

func (r *rig) produce(topic, eventID, eventType, key string, body []byte) {
	r.t.Helper()
	p, err := mq.OpenProducer([]string{r.broker})
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
	defer cancel()
	err = p.Produce(ctx, mq.Message{
		Topic: topic, Key: []byte(key), Value: body, EventID: eventID, EventType: eventType,
	})
	if err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) openConsumer(session time.Duration) *mq.Consumer {
	r.t.Helper()
	c, err := Open(Config{
		Brokers:        []string{r.broker},
		Ledger:         r.svc,
		Sleeper:        func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		SessionTimeout: session,
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return c
}

func (r *rig) serve(c *mq.Consumer) {
	r.t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- c.Serve() }()
	r.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
		select {
		case <-errCh:
		case <-time.After(15 * time.Second):
			r.t.Errorf("serve did not return")
		}
	})
}

func (r *rig) payCount(merchantID, payNo string) int {
	r.t.Helper()
	var n int
	err := r.db(merchantID).QueryRowContext(r.ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type = 'PAY' AND biz_no = ?`, payNo).Scan(&n)
	if err != nil {
		r.t.Fatal(testkit.Redact(err))
	}
	return n
}

func (r *rig) payTotal(merchantID string) int {
	r.t.Helper()
	var n int
	err := r.db(merchantID).QueryRowContext(r.ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type = 'PAY'`).Scan(&n)
	if err != nil {
		r.t.Fatal(testkit.Redact(err))
	}
	return n
}

func (r *rig) entryCount(merchantID, payNo string) int {
	r.t.Helper()
	var n int
	err := r.db(merchantID).QueryRowContext(r.ctx, `
SELECT COUNT(*) FROM ledger_entry e
JOIN ledger_txn t ON t.txn_no = e.txn_no
WHERE t.biz_type = 'PAY' AND t.biz_no = ?`, payNo).Scan(&n)
	if err != nil {
		r.t.Fatal(testkit.Redact(err))
	}
	return n
}

func (r *rig) sumBalance(merchantID, ownerType, ownerID, accountType string) int64 {
	r.t.Helper()
	var n int64
	err := r.db(merchantID).QueryRowContext(r.ctx, `
SELECT COALESCE(SUM(balance), 0) FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ?`, ownerType, ownerID, accountType).Scan(&n)
	if err != nil {
		r.t.Fatal(testkit.Redact(err))
	}
	return n
}

func (r *rig) check() {
	r.t.Helper()
	got, err := r.svc.CheckBalance(r.ctx, "", true)
	if err != nil || !got.Passed {
		r.t.Fatalf("check = %+v err=%v", got, err)
	}
}

func (r *rig) waitOffset(want int64) {
	r.t.Helper()
	waitUntil(r.t, 30*time.Second, func() (bool, error) {
		off, err := groupOffset(r.broker, GroupName, TopicPaymentSucceeded, 0)
		if err != nil {
			return false, err
		}
		return off == want, nil
	})
}

func (r *rig) waitPay(merchantID, payNo string, want int) {
	r.t.Helper()
	waitUntil(r.t, 30*time.Second, func() (bool, error) {
		return r.payCount(merchantID, payNo) == want, nil
	})
}

func (r *rig) refundCount(merchantID, refundNo string) int {
	r.t.Helper()
	var n int
	err := r.db(merchantID).QueryRowContext(r.ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type = 'REFUND' AND biz_no = ?`, refundNo).Scan(&n)
	if err != nil {
		r.t.Fatal(testkit.Redact(err))
	}
	return n
}

func (r *rig) freezeStatus(merchantID, refundNo string) string {
	r.t.Helper()
	var status string
	err := r.db(merchantID).QueryRowContext(r.ctx, `SELECT status FROM account_freeze WHERE biz_type = 'REFUND' AND biz_no = ?`, refundNo).Scan(&status)
	if err != nil {
		r.t.Fatal(testkit.Redact(err))
	}
	return status
}

func (r *rig) waitRefund(merchantID, refundNo string, want int) {
	r.t.Helper()
	waitUntil(r.t, 30*time.Second, func() (bool, error) {
		return r.refundCount(merchantID, refundNo) == want, nil
	})
}

func (r *rig) waitDLQ(eventID string) {
	r.t.Helper()
	waitUntil(r.t, 30*time.Second, func() (bool, error) {
		return r.dlqHas(eventID), nil
	})
}

func (r *rig) dlqHas(eventID string) bool {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(r.broker),
		kgo.ConsumeTopics(mq.DLQTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return false
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return false
		}
		for _, rec := range fetches.Records() {
			if headerValue(rec, mq.HeaderEventID) == eventID {
				return true
			}
		}
	}
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func waitUntil(t *testing.T, d time.Duration, fn func() (bool, error)) {
	t.Helper()
	// 先取出时间再加减。time.Now().Add 对不上 forbidigo 的排除规则。
	deadline := time.Now()
	deadline = deadline.Add(d)
	var last error
	for {
		now := time.Now()
		if !now.Before(deadline) {
			break
		}
		ok, err := fn()
		if err != nil {
			last = err
		}
		if ok {
			return
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-timer.C:
		case <-t.Context().Done():
			timer.Stop()
			t.Fatal(t.Context().Err())
		}
	}
	t.Fatalf("condition not met: %v", last)
}

func groupOffset(broker, group, topic string, partition int32) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Version = 7
	req.Group = group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{partition}}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, err
	}
	if resp.ErrorCode != 0 {
		return 0, kerr.ErrorForCode(resp.ErrorCode)
	}
	for _, topicResp := range resp.Topics {
		for _, part := range topicResp.Partitions {
			if part.Partition != partition {
				continue
			}
			if part.ErrorCode != 0 {
				return 0, kerr.ErrorForCode(part.ErrorCode)
			}
			return part.Offset, nil
		}
	}
	return -1, nil
}

func childEnv(r *rig, merchantID, payNo string) []string {
	env := make([]string, 0, len(os.Environ())+8)
	for _, item := range os.Environ() {
		if strings.HasPrefix(item, "FAULT_POINTS=") || strings.HasPrefix(item, "MQ04_") {
			continue
		}
		env = append(env, item)
	}
	return append(env,
		mq04Child+"=1",
		"FAULT_POINTS="+faultAfterCommit,
		"MQ04_BROKER="+r.broker,
		"MQ04_DSN0="+r.s0.DSN(),
		"MQ04_DSN1="+r.s1.DSN(),
		"MQ04_MERCHANT="+merchantID,
		"MQ04_PAY="+payNo,
	)
}

func payEnvelope(eventID, merchantID, payNo string, amount, fee int64) []byte {
	body, err := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": eventPaymentSucceeded, "version": 1,
		"occurred_at": "2026-11-10T00:00:00.000+08:00", "trace_context": map[string]any{},
		"data": map[string]any{
			"pay_no": payNo, "merchant_id": merchantID, "out_trade_no": "OT-SECRET-NO",
			"channel": "mockpay", "amount": amount, "fee_amount": fee,
			"paid_at": "2026-11-10T00:00:00.000+08:00",
		},
	})
	if err != nil {
		panic(err)
	}
	return body
}

func refundEnvelope(eventID, eventType, refundNo, payNo, merchantID, outRefundNo, channel string, amount int64, succeeded bool) []byte {
	data := map[string]any{
		"refund_no": refundNo, "pay_no": payNo, "merchant_id": merchantID,
		"out_refund_no": outRefundNo, "amount": amount,
	}
	if succeeded {
		data["channel"] = channel
		data["succeeded_at"] = "2026-11-10T00:00:00.000+08:00"
	} else {
		data["fail_code"] = "CHANNEL_FAILED"
	}
	body, err := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": eventType, "version": 1,
		"occurred_at": "2026-11-10T00:00:00.000+08:00", "trace_context": map[string]any{},
		"data": data,
	})
	if err != nil {
		panic(err)
	}
	return body
}

func countMsg(buf *bytes.Buffer, msg string) (all, matched int) {
	for _, line := range strings.Split(buf.String(), "\n") {
		if !strings.Contains(line, `"level":"ERROR"`) {
			continue
		}
		all++
		if strings.Contains(line, `"msg":"`+msg+`"`) {
			matched++
		}
	}
	return all, matched
}

func errorLine(buf *bytes.Buffer, msg string) string {
	var found string
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, `"level":"ERROR"`) && strings.Contains(line, `"msg":"`+msg+`"`) {
			if found != "" {
				return ""
			}
			found = line
		}
	}
	return found
}
