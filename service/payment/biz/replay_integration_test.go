//go:build integration

package biz

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/pkg/outbox"
	"github.com/yym108/pay-system/pkg/shard"
)

// MQ-05 验收 1：删掉已经入账的 PAY 凭证和分录后按 event_id 重放，投递器发出、account-ledger 再记，恰好 1 张 PAY 凭证。
// 对已经入账的同一事件再重放一次，凭证数不变。CheckBalance 通过。
func TestMQ05Accept1ReplayPostsOnce(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100071"
	now := feeAt(9, 0, 0, 0)
	p, order := payToPaying(t, env, merchantID, "MQ05-A1", now, "CH-MQ05-A1")
	paid, err := applySuccess(env.ctx, p, order.PayNo, now.Add(time.Second), paySourceNotify)
	if err != nil || paid.Status != payStatusSuccess {
		t.Fatalf("paid = %+v err=%v", paid, err)
	}
	box := newLedgerBox(t, env)
	box.openBooks(merchantID)
	before := snapAccounts(t, box, merchantID)
	box.dispatch(now)
	box.serve()
	box.waitPay(merchantID, order.PayNo, 1)
	off := waitCommittedReplayOffset(t, box.broker)
	deletePayVoucher(t, box, merchantID, order.PayNo)
	restoreAccounts(t, box, merchantID, before)
	if n := payVoucherCount(t, box, merchantID, order.PayNo); n != 0 {
		t.Fatalf("voucher after delete = %d", n)
	}
	eventID := latestReplay(t, env, order.PayNo, outbox.EventPaymentSucceeded).eventID
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{EventID: eventID})
	if err != nil || got.Rebuilt || got.EventID != eventID {
		t.Fatalf("replay = %+v err=%v", got, err)
	}
	box.dispatch(now)
	waitGroupOffsetAbove(t, box.broker, off)
	box.waitPay(merchantID, order.PayNo, 1)
	if bal := box.sumBalance(merchantID, "MERCHANT", merchantID, "MERCHANT_SETTLE"); bal != 9940 {
		t.Fatalf("merchant = %d", bal)
	}
	off = waitCommittedReplayOffset(t, box.broker)
	again, err := p.ReplayEvent(env.ctx, &ReplayInput{EventID: eventID})
	if err != nil || again.Rebuilt || again.EventID != eventID {
		t.Fatalf("second = %+v err=%v", again, err)
	}
	box.dispatch(now)
	waitGroupOffsetAbove(t, box.broker, off)
	if n := payVoucherCount(t, box, merchantID, order.PayNo); n != 1 {
		t.Fatalf("voucher after second replay = %d", n)
	}
	if bal := box.sumBalance(merchantID, "MERCHANT", merchantID, "MERCHANT_SETTLE"); bal != 9940 {
		t.Fatalf("merchant after second = %d", bal)
	}
	box.check()
}

// MQ-05 验收 2：订单是 PAYING 时按 biz_no + payment.succeeded 重建，返回 30003，outbox 不新增行。
func TestMQ05Accept2PayingRejected(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100072"
	now := feeAt(10, 0, 0, 0)
	p, order := payToPaying(t, env, merchantID, "MQ05-A2", now, "CH-MQ05-A2")
	_, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: order.PayNo, EventType: outbox.EventPaymentSucceeded})
	if !errors.Is(err, errno.ErrOrderStatusConflict) {
		t.Fatalf("err = %v", err)
	}
	if n := countOutboxBiz(t, env, order.PayNo); n != 0 {
		t.Fatalf("outbox = %d", n)
	}
}

// MQ-05 验收 3：商户结算账户不是 ACTIVE 时 payment.succeeded 进入 pay.dlq。
// 账户改回 ACTIVE 后按 event_id 重放，入账恰好 1 次，CheckBalance 通过。
func TestMQ05Accept3InactiveThenReplay(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100073"
	now := feeAt(11, 0, 0, 0)
	p, order := payToPaying(t, env, merchantID, "MQ05-A3", now, "CH-MQ05-A3")
	if _, err := applySuccess(env.ctx, p, order.PayNo, now.Add(time.Second), paySourceNotify); err != nil {
		t.Fatal(err)
	}
	box := newLedgerBox(t, env)
	box.openBooks(merchantID)
	setSettleStatus(t, box, merchantID, "FROZEN")
	box.serve()
	box.dispatch(now)
	stored := latestReplay(t, env, order.PayNo, outbox.EventPaymentSucceeded)
	if stored.status != "SENT" {
		t.Fatalf("outbox = %+v", stored)
	}
	waitReplayDLQ(t, box.ctx, box.broker, stored.eventID)
	if n := payVoucherCount(t, box, merchantID, order.PayNo); n != 0 {
		t.Fatalf("voucher before replay = %d", n)
	}
	setSettleStatus(t, box, merchantID, "ACTIVE")
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{EventID: stored.eventID})
	if err != nil || got.Rebuilt || got.EventID != stored.eventID {
		t.Fatalf("replay = %+v err=%v", got, err)
	}
	box.dispatch(now)
	box.waitPay(merchantID, order.PayNo, 1)
	if bal := box.sumBalance(merchantID, "MERCHANT", merchantID, "MERCHANT_SETTLE"); bal != 9940 {
		t.Fatalf("merchant = %d", bal)
	}
	box.check()
}

func TestReplayPendingUnchanged(t *testing.T) {
	env := openPay(t)
	p, paid := replayPaid(t, env, "M100074", "MQ05-PEND")
	before := latestReplay(t, env, paid.PayNo, outbox.EventPaymentSucceeded)
	if before.status != "PENDING" {
		t.Fatalf("status = %s", before.status)
	}
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{EventID: before.eventID})
	if err != nil || got.Rebuilt || got.EventID != before.eventID {
		t.Fatalf("replay = %+v err=%v", got, err)
	}
	after := latestReplay(t, env, paid.PayNo, outbox.EventPaymentSucceeded)
	if after.status != "PENDING" || after.retry != before.retry || !after.next.Equal(before.next) ||
		!after.updated.Equal(before.updated) || after.sent.Valid != before.sent.Valid || after.last.Valid != before.last.Valid ||
		after.payload != before.payload {
		t.Fatalf("before %+v after %+v", before, after)
	}
}

func TestReplayMissingEventID(t *testing.T) {
	env := openPay(t)
	p, _ := replayPaid(t, env, "M100075", "MQ05-MISS")
	_, err := p.ReplayEvent(env.ctx, &ReplayInput{EventID: "EV_NO_SUCH"})
	if !errors.Is(err, errno.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if n := countOutboxAll(t, env); n != 1 {
		t.Fatalf("outbox = %d", n)
	}
}

func TestReplayBothKeysNoWrite(t *testing.T) {
	env := openPay(t)
	p, paid := replayPaid(t, env, "M100076", "MQ05-BOTH")
	before := latestReplay(t, env, paid.PayNo, outbox.EventPaymentSucceeded)
	_, err := p.ReplayEvent(env.ctx, &ReplayInput{
		EventID: before.eventID, BizNo: paid.PayNo, EventType: outbox.EventPaymentSucceeded,
	})
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("err = %v", err)
	}
	after := latestReplay(t, env, paid.PayNo, outbox.EventPaymentSucceeded)
	if after != before {
		t.Fatalf("before %+v after %+v", before, after)
	}
}

func TestReplaySentAndDeadReset(t *testing.T) {
	env := openPay(t)
	p, paid := replayPaid(t, env, "M100077", "MQ05-RESET")
	clk := payClock(t, p)
	row := latestReplay(t, env, paid.PayNo, outbox.EventPaymentSucceeded)
	sentAt := row.next.Add(-time.Hour)
	markOutbox(t, env, row.eventID, "SENT", 4, row.next.Add(time.Hour), sentAt, "boom")
	later := row.next.Add(2 * time.Hour)
	clk.Set(later)
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{EventID: row.eventID})
	if err != nil || got.Rebuilt || got.EventID != row.eventID {
		t.Fatalf("sent replay = %+v err=%v", got, err)
	}
	reset := loadReplayEvent(t, env, row.eventID)
	want := later.Truncate(time.Millisecond).In(replayShanghai)
	if reset.status != "PENDING" || reset.retry != 0 || !reset.next.Equal(want) || !reset.updated.Equal(want) ||
		!reset.sent.Valid || !reset.sent.Time.Equal(sentAt) || reset.last.String != "boom" {
		t.Fatalf("sent reset = %+v want next %s", reset, want)
	}
	deadAt := later.Add(time.Hour)
	markOutbox(t, env, row.eventID, "DEAD", 9, deadAt, sentAt, "still")
	againAt := deadAt.Add(time.Hour)
	clk.Set(againAt)
	got, err = p.ReplayEvent(env.ctx, &ReplayInput{BizNo: paid.PayNo, EventType: outbox.EventPaymentSucceeded})
	if err != nil || got.Rebuilt || got.EventID != row.eventID {
		t.Fatalf("dead replay = %+v err=%v", got, err)
	}
	dead := loadReplayEvent(t, env, row.eventID)
	want = againAt.Truncate(time.Millisecond).In(replayShanghai)
	if dead.status != "PENDING" || dead.retry != 0 || !dead.next.Equal(want) || !dead.sent.Time.Equal(sentAt) || dead.last.String != "still" {
		t.Fatalf("dead reset = %+v", dead)
	}
	sender := &replaySender{}
	if err = outbox.NewDispatcher(env.db, sender, clk).RunBatch(env.ctx); err != nil {
		t.Fatal(err)
	}
	if sender.n != 1 {
		t.Fatalf("sent %d", sender.n)
	}
	if got := loadReplayEvent(t, env, row.eventID); got.status != "SENT" {
		t.Fatalf("after dispatch = %+v", got)
	}
}

func TestReplayBizUsesMaxID(t *testing.T) {
	env := openPay(t)
	p, paid := replayPaid(t, env, "M100078", "MQ05-MAX")
	first := latestReplay(t, env, paid.PayNo, outbox.EventPaymentSucceeded)
	markOutbox(t, env, first.eventID, "SENT", 2, first.next, first.next, "old")
	const newer = "EV_MAX_NEWER"
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO outbox (
  event_id, event_type, topic, msg_key, biz_no, payload, status, retry_count, next_retry_at, created_at, updated_at
) VALUES (?, 'payment.succeeded', 'pay.payment.succeeded', ?, ?, '{}', 'PENDING', 0, ?, ?, ?)`,
		newer, paid.PayNo, paid.PayNo, first.next, first.next, first.next)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: paid.PayNo, EventType: outbox.EventPaymentSucceeded})
	if err != nil || got.Rebuilt || got.EventID != newer {
		t.Fatalf("replay = %+v err=%v", got, err)
	}
	if old := loadReplayEvent(t, env, first.eventID); old.status != "SENT" || old.retry != 2 {
		t.Fatalf("older row changed: %+v", old)
	}
}

func TestReplayRebuildPayment(t *testing.T) {
	env := openPay(t)
	p, paid := replayPaid(t, env, "M100079", "MQ05-REB")
	before := latestReplay(t, env, paid.PayNo, outbox.EventPaymentSucceeded)
	if _, err := env.db.ExecContext(env.ctx, `DELETE FROM outbox WHERE event_id = ?`, before.eventID); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: paid.PayNo, EventType: outbox.EventPaymentSucceeded})
	if err != nil || !got.Rebuilt || got.EventID == before.eventID || len(got.EventID) < 2 || got.EventID[:2] != "EV" {
		t.Fatalf("replay = %+v err=%v", got, err)
	}
	after := loadReplayEvent(t, env, got.EventID)
	if after.status != "PENDING" || after.retry != 0 || after.eventType != outbox.EventPaymentSucceeded || after.bizNo != paid.PayNo {
		t.Fatalf("row = %+v", after)
	}
	if replayData(t, after.payload) != replayData(t, before.payload) {
		t.Fatalf("data %s want %s", after.payload, before.payload)
	}
}

func TestReplayRebuildSeesInsert(t *testing.T) {
	env := openPay(t)
	p, paid := replayPaid(t, env, "M100080", "MQ05-RACE")
	before := latestReplay(t, env, paid.PayNo, outbox.EventPaymentSucceeded)
	if _, err := env.db.ExecContext(env.ctx, `DELETE FROM outbox WHERE event_id = ?`, before.eventID); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { testReplayAfterMiss = nil }()
	testReplayAfterMiss = func() {
		gen, ok := p.ids.(*idgen.Generator)
		if !ok {
			t.Errorf("ids type %T", p.ids)
			return
		}
		err := dbx.WithTx(env.ctx, env.db, func(tx *sqlx.Tx) error {
			return outbox.Add(env.ctx, tx, outbox.Event{
				Type: outbox.EventPaymentSucceeded, PayNo: paid.PayNo, MerchantID: paid.MerchantID,
				OutTradeNo: paid.OutTradeNo, Channel: paid.Channel, Amount: paid.Amount,
				FeeAmount: paid.FeeAmount, PaidAt: paid.PaidAt,
			}, gen, p.clk)
		})
		if err != nil {
			t.Errorf("hook add: %v", err)
		}
	}
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: paid.PayNo, EventType: outbox.EventPaymentSucceeded})
	if err != nil || got.Rebuilt || got.EventID == "" {
		t.Fatalf("replay = %+v err=%v", got, err)
	}
	if n := countOutboxBiz(t, env, paid.PayNo); n != 1 {
		t.Fatalf("outbox = %d", n)
	}
	if loadReplayEvent(t, env, got.EventID).eventID != got.EventID {
		t.Fatal("missing hooked row")
	}
}

func TestReplayUnknownAndMissingOrder(t *testing.T) {
	env := openPay(t)
	p := openPayments(t, env, "M100081", 60, 60, feeAt(12, 0, 0, 0), "merchant-demo")
	const bizNo = "PA_MYSTERY"
	now := feeAt(12, 0, 0, 0)
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO outbox (
  event_id, event_type, topic, msg_key, biz_no, payload, status, retry_count, next_retry_at, last_error, created_at, updated_at
) VALUES ('EV_MYSTERY_1', 'mystery', 'pay.payment.succeeded', ?, ?, '{}', 'DEAD', 3, ?, 'kept', ?, ?)`,
		bizNo, bizNo, now, now, now)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: bizNo, EventType: "mystery"})
	if err != nil || got.Rebuilt || got.EventID != "EV_MYSTERY_1" {
		t.Fatalf("known row = %+v err=%v", got, err)
	}
	row := loadReplayEvent(t, env, "EV_MYSTERY_1")
	if row.status != "PENDING" || row.retry != 0 || row.last.String != "kept" {
		t.Fatalf("mystery reset = %+v", row)
	}
	if _, err = env.db.ExecContext(env.ctx, `DELETE FROM outbox WHERE event_id = 'EV_MYSTERY_1'`); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err = p.ReplayEvent(env.ctx, &ReplayInput{BizNo: bizNo, EventType: "mystery"}); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("unknown = %v", err)
	}
	if _, err = p.ReplayEvent(env.ctx, &ReplayInput{BizNo: "PA_NONE", EventType: outbox.EventPaymentSucceeded}); !errors.Is(err, errno.ErrNotFound) {
		t.Fatalf("missing order = %v", err)
	}
	if n := countOutboxAll(t, env); n != 0 {
		t.Fatalf("outbox = %d", n)
	}
}

func TestReplayRefundAndTransfer(t *testing.T) {
	env := openPay(t)
	now := feeAt(13, 0, 0, 0)
	p := openPayments(t, env, "M100082", 60, 60, now, "merchant-demo")
	insertReplayRefund(t, env, "RF_MQ05_PAYING", "PROCESSING", "", sql.NullTime{})
	if _, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: "RF_MQ05_PAYING", EventType: outbox.EventRefundSucceeded}); !errors.Is(err, errno.ErrOrderStatusConflict) {
		t.Fatalf("processing = %v", err)
	}
	refundOKAt := now.Add(time.Second)
	insertReplayRefund(t, env, "RF_MQ05_OK", "SUCCESS", "", sql.NullTime{Time: refundOKAt, Valid: true})
	got, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: "RF_MQ05_OK", EventType: outbox.EventRefundSucceeded})
	if err != nil || !got.Rebuilt || got.EventID[:2] != "EV" {
		t.Fatalf("refund rebuild = %+v err=%v", got, err)
	}
	okRow := loadReplayEvent(t, env, got.EventID)
	if okRow.bizNo != "RF_MQ05_OK" || okRow.eventType != outbox.EventRefundSucceeded || okRow.status != "PENDING" || okRow.retry != 0 {
		t.Fatalf("refund row = %+v", okRow)
	}
	assertRefundSucceededData(t, env, okRow.payload, "RF_MQ05_OK")
	if _, err = p.ReplayEvent(env.ctx, &ReplayInput{BizNo: "RF_MQ05_OK", EventType: outbox.EventRefundFailed}); !errors.Is(err, errno.ErrOrderStatusConflict) {
		t.Fatalf("refund failed on success = %v", err)
	}
	if n := countOutboxBiz(t, env, "RF_MQ05_OK"); n != 1 {
		t.Fatalf("success refund outbox = %d", n)
	}
	insertReplayRefund(t, env, "RF_MQ05_FAIL", "FAILED", "CHANNEL_FAILED", sql.NullTime{})
	got, err = p.ReplayEvent(env.ctx, &ReplayInput{BizNo: "RF_MQ05_FAIL", EventType: outbox.EventRefundFailed})
	if err != nil || !got.Rebuilt || len(got.EventID) < 2 || got.EventID[:2] != "EV" {
		t.Fatalf("refund failed rebuild = %+v err=%v", got, err)
	}
	failRow := loadReplayEvent(t, env, got.EventID)
	if failRow.bizNo != "RF_MQ05_FAIL" || failRow.eventType != outbox.EventRefundFailed || failRow.status != "PENDING" || failRow.retry != 0 {
		t.Fatalf("refund failed row = %+v", failRow)
	}
	assertRefundFailedData(t, env, failRow.payload, "RF_MQ05_FAIL")
	insertReplayTransfer(t, env, "TF_MQ05_TRY", "TRYING", "", sql.NullTime{}, sql.NullString{})
	if _, err = p.ReplayEvent(env.ctx, &ReplayInput{BizNo: "TF_MQ05_TRY", EventType: outbox.EventTransferFailed}); !errors.Is(err, errno.ErrOrderStatusConflict) {
		t.Fatalf("trying = %v", err)
	}
	transferOKAt := now.Add(2 * time.Second)
	insertReplayTransfer(t, env, "TF_MQ05_OK", "SUCCESS", "", sql.NullTime{Time: transferOKAt, Valid: true}, sql.NullString{})
	got, err = p.ReplayEvent(env.ctx, &ReplayInput{BizNo: "TF_MQ05_OK", EventType: outbox.EventTransferSucceeded})
	if err != nil || !got.Rebuilt {
		t.Fatalf("transfer rebuild = %+v err=%v", got, err)
	}
	okTransfer := loadReplayEvent(t, env, got.EventID)
	if okTransfer.status != "PENDING" || okTransfer.eventType != outbox.EventTransferSucceeded || okTransfer.bizNo != "TF_MQ05_OK" {
		t.Fatalf("transfer row = %+v", okTransfer)
	}
	assertTransferData(t, env, okTransfer.payload, "TF_MQ05_OK", false)
	transferFailAt := now.Add(3 * time.Second)
	insertReplayTransfer(t, env, "TF_MQ05_FAIL", "FAILED", "INSUFFICIENT_BALANCE", sql.NullTime{Time: transferFailAt, Valid: true}, sql.NullString{})
	got, err = p.ReplayEvent(env.ctx, &ReplayInput{BizNo: "TF_MQ05_FAIL", EventType: outbox.EventTransferFailed})
	if err != nil || !got.Rebuilt || len(got.EventID) < 2 || got.EventID[:2] != "EV" {
		t.Fatalf("transfer failed rebuild = %+v err=%v", got, err)
	}
	failTransfer := loadReplayEvent(t, env, got.EventID)
	if failTransfer.status != "PENDING" || failTransfer.eventType != outbox.EventTransferFailed || failTransfer.bizNo != "TF_MQ05_FAIL" || failTransfer.retry != 0 {
		t.Fatalf("transfer failed row = %+v", failTransfer)
	}
	assertTransferData(t, env, failTransfer.payload, "TF_MQ05_FAIL", true)
	if n := countOutboxBiz(t, env, "RF_MQ05_PAYING") + countOutboxBiz(t, env, "TF_MQ05_TRY"); n != 0 {
		t.Fatalf("rejected inserts = %d", n)
	}
}

func TestReplayRebuildClockAndIDs(t *testing.T) {
	env := openPay(t)
	p, paid := replayPaid(t, env, "M100083", "MQ05-CLK")
	if _, err := env.db.ExecContext(env.ctx, `DELETE FROM outbox WHERE biz_no = ?`, paid.PayNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	clk := payClock(t, p)
	clk.Set(clk.Now().Add(-time.Hour))
	if _, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: paid.PayNo, EventType: outbox.EventPaymentSucceeded}); !errors.Is(err, errno.ErrUnavailable) {
		t.Fatalf("rollback = %v", err)
	}
	if n := countOutboxBiz(t, env, paid.PayNo); n != 0 {
		t.Fatalf("rollback outbox = %d", n)
	}
	clk.Set(clk.Now().Add(2 * time.Hour))
	p.ids = replayStubIDs{}
	if _, err := p.ReplayEvent(env.ctx, &ReplayInput{BizNo: paid.PayNo, EventType: outbox.EventPaymentSucceeded}); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("stub ids = %v", err)
	}
	if n := countOutboxBiz(t, env, paid.PayNo); n != 0 {
		t.Fatalf("stub outbox = %d", n)
	}
}

type replayStubIDs struct{}

func (replayStubIDs) NewNo(context.Context, string) (string, error) {
	return "EV_STUB", nil
}

type replaySender struct {
	n int
}

func (s *replaySender) Send(_ context.Context, batch []outbox.Delivery) []error {
	s.n += len(batch)
	return make([]error, len(batch))
}

type replayAcct struct {
	id              int64
	balance, frozen int64
}

type replayRow struct {
	eventID   string
	eventType string
	bizNo     string
	status    string
	payload   string
	retry     int
	next      time.Time
	updated   time.Time
	sent      sql.NullTime
	last      sql.NullString
}

func replayPaid(t *testing.T, env payEnv, merchantID, outTradeNo string) (*Payments, PaymentOrder) {
	t.Helper()
	now := feeAt(9, 30, 0, 0)
	p, order := payToPaying(t, env, merchantID, outTradeNo, now, "CH-"+outTradeNo)
	paid, err := applySuccess(env.ctx, p, order.PayNo, now.Add(time.Second), paySourceNotify)
	if err != nil || paid.Status != payStatusSuccess {
		t.Fatalf("paid = %+v err=%v", paid, err)
	}
	return p, paid
}

func payClock(t *testing.T, p *Payments) *clock.Fake {
	t.Helper()
	clk, ok := p.clk.(*clock.Fake)
	if !ok || clk == nil {
		t.Fatal("clock")
	}
	return clk
}

func latestReplay(t *testing.T, env payEnv, bizNo, eventType string) replayRow {
	t.Helper()
	var row replayRow
	err := env.db.QueryRowContext(env.ctx, `
SELECT event_id, event_type, biz_no, status, payload, retry_count, next_retry_at, updated_at, sent_at, last_error
FROM outbox
WHERE biz_no = ? AND event_type = ?
ORDER BY id DESC
LIMIT 1`, bizNo, eventType).Scan(
		&row.eventID, &row.eventType, &row.bizNo, &row.status, &row.payload, &row.retry, &row.next, &row.updated, &row.sent, &row.last,
	)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

func loadReplayEvent(t *testing.T, env payEnv, eventID string) replayRow {
	t.Helper()
	var row replayRow
	err := env.db.QueryRowContext(env.ctx, `
SELECT event_id, event_type, biz_no, status, payload, retry_count, next_retry_at, updated_at, sent_at, last_error
FROM outbox WHERE event_id = ?`, eventID).Scan(
		&row.eventID, &row.eventType, &row.bizNo, &row.status, &row.payload, &row.retry, &row.next, &row.updated, &row.sent, &row.last,
	)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

func countOutboxBiz(t *testing.T, env payEnv, bizNo string) int {
	t.Helper()
	var n int
	if err := env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM outbox WHERE biz_no = ?`, bizNo).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func countOutboxAll(t *testing.T, env payEnv) int {
	t.Helper()
	var n int
	if err := env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func markOutbox(t *testing.T, env payEnv, eventID, status string, retry int, next, sent time.Time, last string) {
	t.Helper()
	res, err := env.db.ExecContext(env.ctx, `
UPDATE outbox
SET status = ?, retry_count = ?, next_retry_at = ?, updated_at = ?, sent_at = ?, last_error = ?
WHERE event_id = ?`, status, retry, next, next, sent, last, eventID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("mark rows %d err %v", n, err)
	}
}

func replayData(t *testing.T, payload string) string {
	t.Helper()
	var body struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(payload), &body); err != nil {
		t.Fatal(err)
	}
	return string(body.Data)
}

// replayStamp 与 outbox.formatTime 相同：毫秒、东八区、RFC3339。
func replayStamp(at time.Time) string {
	return at.Truncate(time.Millisecond).In(replayShanghai).Format("2006-01-02T15:04:05.000Z07:00")
}

// payload 列是 JSON，读回会重排键并加空格。按 outbox.Add 写出的字段集合逐项比较，金额必须是不带小数点的整数。
func assertReplayData(t *testing.T, payload string, want map[string]any) {
	t.Helper()
	got := replayDataMap(t, payload)
	if len(got) != len(want) {
		t.Fatalf("data keys got %v want %v", got, want)
	}
	for key, w := range want {
		g, ok := got[key]
		if !ok || !replayValueEqual(g, w) {
			t.Fatalf("data %s = %#v want %#v; full %#v", key, g, w, got)
		}
	}
}

func replayDataMap(t *testing.T, payload string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(replayData(t, payload)))
	dec.UseNumber()
	var got map[string]any
	if err := dec.Decode(&got); err != nil {
		t.Fatal(err)
	}
	if dec.More() {
		t.Fatal("data trailing")
	}
	return got
}

func replayValueEqual(got, want any) bool {
	switch w := want.(type) {
	case string:
		g, ok := got.(string)
		return ok && g == w
	case int64:
		n, ok := got.(json.Number)
		return ok && !strings.Contains(n.String(), ".") && n.String() == strconv.FormatInt(w, 10)
	default:
		return false
	}
}

func assertRefundSucceededData(t *testing.T, env payEnv, payload, refundNo string) {
	t.Helper()
	var (
		payNo, merchant, outNo, channel string
		amount                          int64
		succeeded                       time.Time
	)
	err := env.db.QueryRowContext(env.ctx, `
SELECT pay_no, merchant_id, out_refund_no, channel, amount, succeeded_at
FROM refund_order WHERE refund_no = ?`, refundNo).Scan(
		&payNo, &merchant, &outNo, &channel, &amount, &succeeded)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	assertReplayData(t, payload, map[string]any{
		"refund_no": refundNo, "pay_no": payNo, "merchant_id": merchant, "out_refund_no": outNo,
		"channel": channel, "amount": amount, "succeeded_at": replayStamp(succeeded),
	})
}

func assertRefundFailedData(t *testing.T, env payEnv, payload, refundNo string) {
	t.Helper()
	var (
		payNo, merchant, outNo string
		amount                 int64
		failCode               sql.NullString
	)
	err := env.db.QueryRowContext(env.ctx, `
SELECT pay_no, merchant_id, out_refund_no, amount, fail_code
FROM refund_order WHERE refund_no = ?`, refundNo).Scan(
		&payNo, &merchant, &outNo, &amount, &failCode)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !failCode.Valid || failCode.String == "" {
		t.Fatal("refund fail_code empty")
	}
	assertReplayData(t, payload, map[string]any{
		"refund_no": refundNo, "pay_no": payNo, "merchant_id": merchant, "out_refund_no": outNo,
		"amount": amount, "fail_code": failCode.String,
	})
}

func assertTransferData(t *testing.T, env payEnv, payload, transferNo string, failed bool) {
	t.Helper()
	var (
		outNo, merchant, payee, mode, status string
		amount                               int64
		failCode                             sql.NullString
		finished                             time.Time
	)
	err := env.db.QueryRowContext(env.ctx, `
SELECT out_transfer_no, merchant_id, payee_merchant_id, amount, mode, status, fail_code, finished_at
FROM transfer_order WHERE transfer_no = ?`, transferNo).Scan(
		&outNo, &merchant, &payee, &amount, &mode, &status, &failCode, &finished)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	stamp := replayStamp(finished)
	if failed {
		if !failCode.Valid || failCode.String == "" {
			t.Fatal("transfer fail_code empty")
		}
		assertReplayData(t, payload, map[string]any{
			"transfer_no": transferNo, "out_transfer_no": outNo, "merchant_id": merchant, "payee_merchant_id": payee,
			"amount": amount, "mode": mode, "status": status, "fail_code": failCode.String, "finished_at": stamp,
		})
		return
	}
	if failCode.Valid {
		t.Fatalf("success fail_code = %s", failCode.String)
	}
	assertReplayData(t, payload, map[string]any{
		"transfer_no": transferNo, "out_transfer_no": outNo, "merchant_id": merchant, "payee_merchant_id": payee,
		"amount": amount, "mode": mode, "status": status, "finished_at": stamp,
	})
}

func insertReplayRefund(t *testing.T, env payEnv, refundNo, status, failCode string, succeeded sql.NullTime) {
	t.Helper()
	now := feeAt(13, 0, 0, 0)
	var code any
	if failCode != "" {
		code = failCode
	}
	var at any
	if succeeded.Valid {
		at = succeeded.Time
	}
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO refund_order (
  refund_no, merchant_id, out_refund_no, pay_no, amount, currency, reason,
  status, channel, fail_code, succeeded_at, version, created_at, updated_at
) VALUES (?, 'M100082', ?, 'PA_REPLAY_PAY', 3000, 'CNY', '', ?, 'mockpay', ?, ?, 0, ?, ?)`,
		refundNo, "OR_"+refundNo, status, code, at, now, now)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func insertReplayTransfer(t *testing.T, env payEnv, transferNo, status, failCode string, finished sql.NullTime, txn sql.NullString) {
	t.Helper()
	insertReplayTransferOrder(t, env, transferNo, status, failCode, finished, txn)
}

func insertReplayTransferOrder(t *testing.T, env payEnv, transferNo, status, failCode string, finished sql.NullTime, txn sql.NullString) {
	t.Helper()
	payer, err := shard.Of("M100082")
	if err != nil {
		t.Fatal(err)
	}
	payee, err := shard.Of("M100083")
	if err != nil {
		t.Fatal(err)
	}
	mode := "TCC"
	if payer == payee {
		mode = "LOCAL"
	}
	var code any
	if failCode != "" {
		code = failCode
	}
	var fin any
	if finished.Valid {
		fin = finished.Time
	}
	var txnNo any
	if txn.Valid {
		txnNo = txn.String
	}
	if mode == "LOCAL" && status == transferStatusSuccess && txnNo == nil {
		txnNo = "TX_REPLAY"
	}
	if mode == "TCC" && status == transferStatusSuccess {
		txnNo = nil
	}
	now := feeAt(13, 0, 0, 0)
	_, err = env.db.ExecContext(env.ctx, `
INSERT INTO transfer_order (
  transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, notify_url,
  mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
  version, created_at, updated_at
) VALUES (?, 'M100082', ?, 'M100083', 100, 'CNY', '', ?, ?, ?, ?, ?, ?, 0, ?, ?, 0, ?, ?)`,
		transferNo, "OT_"+transferNo, mode, payer, payee, status, code, txnNo, now, fin, now, now)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func snapAccounts(t *testing.T, box *ledgerBox, merchantID string) []replayAcct {
	t.Helper()
	rows, err := box.db(merchantID).QueryContext(box.ctx, `SELECT id, balance, frozen_amount FROM account ORDER BY id`)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	var got []replayAcct
	for rows.Next() {
		var row replayAcct
		if err = rows.Scan(&row.id, &row.balance, &row.frozen); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		got = append(got, row)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return got
}

func restoreAccounts(t *testing.T, box *ledgerBox, merchantID string, rows []replayAcct) {
	t.Helper()
	for _, row := range rows {
		res, err := box.db(merchantID).ExecContext(box.ctx, `
UPDATE account SET balance = ?, frozen_amount = ?, updated_at = updated_at WHERE id = ?`,
			row.balance, row.frozen, row.id)
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
		n, err := res.RowsAffected()
		if err != nil || n > 1 {
			t.Fatalf("restore %d rows %d err %v", row.id, n, err)
		}
		var balance, frozen int64
		err = box.db(merchantID).QueryRowContext(box.ctx, `SELECT balance, frozen_amount FROM account WHERE id = ?`, row.id).Scan(&balance, &frozen)
		if err != nil || balance != row.balance || frozen != row.frozen {
			t.Fatalf("restore id %d balance %d frozen %d want %d/%d err %v", row.id, balance, frozen, row.balance, row.frozen, err)
		}
	}
}

func deletePayVoucher(t *testing.T, box *ledgerBox, merchantID, payNo string) {
	t.Helper()
	db := box.db(merchantID)
	if _, err := db.ExecContext(box.ctx, `DELETE FROM ledger_entry WHERE biz_type = 'PAY' AND biz_no = ?`, payNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	res, err := db.ExecContext(box.ctx, `DELETE FROM ledger_txn WHERE biz_type = 'PAY' AND biz_no = ?`, payNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("delete txn rows %d err %v", n, err)
	}
}

func payVoucherCount(t *testing.T, box *ledgerBox, merchantID, payNo string) int {
	t.Helper()
	var n int
	err := box.db(merchantID).QueryRowContext(box.ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type = 'PAY' AND biz_no = ?`, payNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func setSettleStatus(t *testing.T, box *ledgerBox, merchantID, status string) {
	t.Helper()
	res, err := box.db(merchantID).ExecContext(box.ctx, `
UPDATE account SET status = ?, updated_at = updated_at
WHERE owner_type = 'MERCHANT' AND owner_id = ? AND account_type = 'MERCHANT_SETTLE'`, status, merchantID)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		t.Fatalf("status rows %d err %v", n, err)
	}
}

func waitReplayDLQ(t *testing.T, ctx context.Context, broker, eventID string) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(mq.DLQTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	waitLedger(t, 30*time.Second, func() bool {
		poll, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		fetches := cl.PollFetches(poll)
		cancel()
		for _, rec := range fetches.Records() {
			for _, h := range rec.Headers {
				if h.Key == mq.HeaderEventID && string(h.Value) == eventID {
					return true
				}
			}
		}
		return false
	})
}

func waitCommittedReplayOffset(t *testing.T, broker string) int64 {
	t.Helper()
	var got int64
	waitLedger(t, 30*time.Second, func() bool {
		off, err := replayGroupOffset(broker)
		if err != nil || off < 0 {
			return false
		}
		got = off
		return true
	})
	return got
}

func waitGroupOffsetAbove(t *testing.T, broker string, before int64) {
	t.Helper()
	waitLedger(t, 30*time.Second, func() bool {
		off, err := replayGroupOffset(broker)
		return err == nil && off > before
	})
}

func replayGroupOffset(broker string) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Version = 7
	req.Group = "account-ledger"
	req.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic: "pay.payment.succeeded", Partitions: []int32{0},
	}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, err
	}
	if resp.ErrorCode != 0 {
		return 0, kerr.ErrorForCode(resp.ErrorCode)
	}
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			if part.Partition != 0 {
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
