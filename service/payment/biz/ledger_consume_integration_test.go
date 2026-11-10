//go:build integration

package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/pkg/outbox"
	"github.com/yym108/pay-system/pkg/shard"
	accountbiz "github.com/yym108/pay-system/service/account/biz"
	"github.com/yym108/pay-system/service/account/consumer"
	accountdal "github.com/yym108/pay-system/service/account/dal"
)

// MQ-04 验收 6：CLOSE_RACE 关单返回 30003 后，payment.succeeded 经投递器投出并被 account-ledger 消费，商户入账恰好 1 次。
func TestMQ04Accept6CloseRace(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100061"
	now := feeAt(9, 30, 0, 0)
	clk := &clock.Fake{}
	clk.Set(now)
	mock := startMockHTTP(t, clk)
	p := newClosePayments(t, env, merchantID, now, clk)
	wireCloseChannel(t, p, mock.url, clk)
	order := createCloseOrder(t, env, p, merchantID, "MQ04-RACE", amountCloseRace)
	if _, err := p.Pay(env.ctx, payReq(order)); err != nil {
		t.Fatal(err)
	}
	got, err := p.ClosePayment(env.ctx, closeReq(order))
	if !errors.Is(err, errno.ErrOrderStatusConflict) || got.Status != payStatusSuccess {
		t.Fatalf("close = %+v err=%v", got, err)
	}
	events := loadOutbox(t, env, order.PayNo)
	if len(events) != 1 || events[0].eventType != outbox.EventPaymentSucceeded {
		t.Fatalf("outbox = %+v", events)
	}
	box := newLedgerBox(t, env)
	box.openBooks(merchantID)
	box.dispatch(now)
	if rows := loadOutbox(t, env, order.PayNo); len(rows) != 1 || rows[0].status != "SENT" {
		t.Fatalf("sent = %+v", rows)
	}
	box.serve()
	box.waitPay(merchantID, order.PayNo, 1)
	credit, err := money.Sub(order.Amount, order.FeeAmount)
	if err != nil {
		t.Fatal(err)
	}
	if bal := box.sumBalance(merchantID, "MERCHANT", merchantID, "MERCHANT_SETTLE"); bal != int64(credit) {
		t.Fatalf("merchant = %d want %d", bal, credit)
	}
	box.check()
}

// MQ-04 验收 7：订单 10000 入账后退款 3000 成功，商户余额减少 3000，冻结为 0，渠道应收减少 3000。
func TestMQ04Accept7RefundSucceeded(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(11, 0, 0, 0)
	p, order := payToPaying(t, env, merchantID, "MQ04-PAY", now, "CH-MQ04-PAY")
	paid, err := applySuccess(env.ctx, p, order.PayNo, now.Add(time.Second), paySourceNotify)
	if err != nil || paid.Status != payStatusSuccess || paid.FeeAmount != 60 {
		t.Fatalf("paid = %+v err=%v", paid, err)
	}
	box := newLedgerBox(t, env)
	box.openBooks(merchantID)
	box.dispatch(now)
	box.serve()
	box.waitPay(merchantID, order.PayNo, 1)
	if bal := box.sumBalance(merchantID, "MERCHANT", merchantID, "MERCHANT_SETTLE"); bal != 9940 {
		t.Fatalf("posted merchant = %d", bal)
	}
	p.UseRefund(accountBizFreezer{svc: box.svc}, uniqueRefunder{}, nil, testChannelNotifyURL)
	refund, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R-MQ04-OK", Amount: 3000,
	})
	if err != nil || refund.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", refund, err)
	}
	done, err := p.ApplyRefundResult(env.ctx, refundNotify(refund.RefundNo, payResultSuccess, 3000, now.Add(2*time.Second)))
	if err != nil || done.Status != payResultSuccess {
		t.Fatalf("apply = %+v err=%v", done, err)
	}
	box.dispatch(now)
	box.waitBalance(merchantID, "MERCHANT", merchantID, "MERCHANT_SETTLE", 6940)
	if frozen := box.frozen(merchantID); frozen != 0 {
		t.Fatalf("frozen = %d", frozen)
	}
	if ch := box.sumBalance(merchantID, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE"); ch != 7000 {
		t.Fatalf("channel = %d", ch)
	}
	if fee := box.sumBalance(merchantID, "PLATFORM", "PLATFORM", "FEE_INCOME"); fee != 60 {
		t.Fatalf("fee = %d", fee)
	}
	box.check()
}

// MQ-04 验收 8：消费 refund.failed 后冻结单 RELEASED，frozen_amount 恢复，新的 out_refund_no 可以再次退款。
func TestMQ04Accept8RefundFailed(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(12, 0, 0, 0)
	p, order := payToPaying(t, env, merchantID, "MQ04-FAIL", now, "CH-MQ04-FAIL")
	if _, err := applySuccess(env.ctx, p, order.PayNo, now.Add(time.Second), paySourceNotify); err != nil {
		t.Fatal(err)
	}
	box := newLedgerBox(t, env)
	box.openBooks(merchantID)
	box.dispatch(now)
	box.serve()
	box.waitPay(merchantID, order.PayNo, 1)
	p.UseRefund(accountBizFreezer{svc: box.svc}, uniqueRefunder{}, nil, testChannelNotifyURL)
	first, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R-MQ04-FAIL", Amount: 3000,
	})
	if err != nil || first.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", first, err)
	}
	if box.frozen(merchantID) != 3000 {
		t.Fatalf("frozen before fail = %d", box.frozen(merchantID))
	}
	failed, err := p.ApplyRefundResult(env.ctx, refundNotify(first.RefundNo, payResultFailed, 3000, now.Add(2*time.Second)))
	if err != nil || failed.Status != refundStatusFailed {
		t.Fatalf("apply = %+v err=%v", failed, err)
	}
	box.dispatch(now)
	box.waitFreeze(first.RefundNo, "RELEASED")
	if frozen := box.frozen(merchantID); frozen != 0 {
		t.Fatalf("frozen after release = %d", frozen)
	}
	if bal := box.sumBalance(merchantID, "MERCHANT", merchantID, "MERCHANT_SETTLE"); bal != 9940 {
		t.Fatalf("merchant = %d", bal)
	}
	second, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R-MQ04-AGAIN", Amount: 2000,
	})
	if err != nil || second.Status != refundStatusProcessing || second.RefundNo == first.RefundNo {
		t.Fatalf("again = %+v err=%v", second, err)
	}
	box.check()
}

// uniqueRefunder 按退款单号生成渠道流水号。同一渠道流水号不能给两笔退款用。
type uniqueRefunder struct{}

func (uniqueRefunder) Refund(_ context.Context, req ChannelRefundRequest) (ChannelAcceptResult, error) {
	return ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_" + req.ReqNo}, nil
}

type ledgerBox struct {
	t      *testing.T
	env    payEnv
	ctx    context.Context
	broker string
	s0     *sqlx.DB
	s1     *sqlx.DB
	svc    *accountbiz.Service
	prod   *mq.Producer
}

func newLedgerBox(t *testing.T, env payEnv) *ledgerBox {
	t.Helper()
	ctx, cancel := context.WithTimeout(env.ctx, 3*time.Minute)
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
	t.Cleanup(func() { closeAccountSchema(t, s0) })
	s1, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeAccountSchema(t, s1) })
	ids, err := idgen.New(13, clock.Real{})
	if err != nil {
		t.Fatal(err)
	}
	svc := accountbiz.New(accountdal.New(s0.DB, s1.DB), ids, 8)
	createLedgerTopics(t, ctx, k.Broker)
	prod, err := mq.OpenProducer([]string{k.Broker})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prod.Close() })
	return &ledgerBox{t: t, env: env, ctx: ctx, broker: k.Broker, s0: s0.DB, s1: s1.DB, svc: svc, prod: prod}
}

func (b *ledgerBox) db(merchantID string) *sqlx.DB {
	b.t.Helper()
	n, err := shard.Of(merchantID)
	if err != nil {
		b.t.Fatal(err)
	}
	if n == 0 {
		return b.s0
	}
	return b.s1
}

func (b *ledgerBox) openBooks(merchantID string) {
	b.t.Helper()
	n, err := shard.Of(merchantID)
	if err != nil {
		b.t.Fatal(err)
	}
	if _, err = b.svc.OpenAccount(b.ctx, "MERCHANT", merchantID, "MERCHANT_SETTLE", n, 0); err != nil {
		b.t.Fatal(err)
	}
	for sub := 0; sub < 8; sub++ {
		if _, err = b.svc.OpenAccount(b.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", n, sub); err != nil {
			b.t.Fatal(err)
		}
		if _, err = b.svc.OpenAccount(b.ctx, "PLATFORM", "PLATFORM", "FEE_INCOME", n, sub); err != nil {
			b.t.Fatal(err)
		}
	}
}

func (b *ledgerBox) dispatch(now time.Time) {
	b.t.Helper()
	clk := &clock.Fake{}
	clk.Set(now)
	err := outbox.NewDispatcher(b.env.db, outbox.NewMQSender(b.prod), clk).RunBatch(b.ctx)
	if err != nil {
		b.t.Fatal(err)
	}
}

func (b *ledgerBox) serve() {
	b.t.Helper()
	c, err := consumer.Open(consumer.Config{
		Brokers: []string{b.broker},
		Ledger:  b.svc,
		Sleeper: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	})
	if err != nil {
		b.t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Serve() }()
	b.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
		select {
		case <-errCh:
		case <-time.After(15 * time.Second):
			b.t.Errorf("serve did not return")
		}
	})
}

func (b *ledgerBox) waitPay(merchantID, payNo string, want int) {
	b.t.Helper()
	waitLedger(b.t, 30*time.Second, func() bool {
		var n int
		err := b.db(merchantID).QueryRowContext(b.ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type = 'PAY' AND biz_no = ?`, payNo).Scan(&n)
		return err == nil && n == want
	})
}

func (b *ledgerBox) waitBalance(merchantID, ownerType, ownerID, accountType string, want int64) {
	b.t.Helper()
	waitLedger(b.t, 30*time.Second, func() bool {
		return b.sumBalance(merchantID, ownerType, ownerID, accountType) == want
	})
}

func (b *ledgerBox) waitFreeze(bizNo, status string) {
	b.t.Helper()
	waitLedger(b.t, 30*time.Second, func() bool {
		var got string
		err := b.s0.QueryRowContext(b.ctx, `SELECT status FROM account_freeze WHERE biz_no = ?`, bizNo).Scan(&got)
		return err == nil && got == status
	})
}

func (b *ledgerBox) sumBalance(merchantID, ownerType, ownerID, accountType string) int64 {
	b.t.Helper()
	var n int64
	err := b.db(merchantID).QueryRowContext(b.ctx, `
SELECT COALESCE(SUM(balance), 0) FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ?`, ownerType, ownerID, accountType).Scan(&n)
	if err != nil {
		b.t.Fatal(testkit.Redact(err))
	}
	return n
}

func (b *ledgerBox) frozen(merchantID string) int64 {
	b.t.Helper()
	var n int64
	err := b.db(merchantID).QueryRowContext(b.ctx, `
SELECT frozen_amount FROM account
WHERE owner_type = 'MERCHANT' AND owner_id = ? AND account_type = 'MERCHANT_SETTLE'`, merchantID).Scan(&n)
	if err != nil {
		b.t.Fatal(testkit.Redact(err))
	}
	return n
}

func (b *ledgerBox) check() {
	b.t.Helper()
	got, err := b.svc.CheckBalance(b.ctx, "", true)
	if err != nil || !got.Passed {
		b.t.Fatalf("check = %+v err=%v", got, err)
	}
}

func createLedgerTopics(t *testing.T, ctx context.Context, broker string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	reqCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Version = 7
	req.TimeoutMillis = 15000
	for _, name := range []string{consumer.TopicPaymentSucceeded, consumer.TopicRefundSucceeded, consumer.TopicRefundFailed, mq.DLQTopic} {
		req.Topics = append(req.Topics, kmsg.CreateTopicsRequestTopic{Topic: name, NumPartitions: 1, ReplicationFactor: 1})
	}
	resp, err := req.RequestWith(reqCtx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range resp.Topics {
		if topic.ErrorCode == 0 || errors.Is(kerr.ErrorForCode(topic.ErrorCode), kerr.TopicAlreadyExists) {
			continue
		}
		t.Fatalf("create topic %s: code %d", topic.Topic, topic.ErrorCode)
	}
}

func waitLedger(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		if fn() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(50 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatal("condition not met")
		}
	}
}
