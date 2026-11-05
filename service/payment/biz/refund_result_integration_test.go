//go:build integration

package biz

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// ref03PayNo 用生产格式（PA + 19 位数字），日志断言按约定 §7.5 兜底扫描遮盖后的形态比较。
const ref03PayNo = "PA0000000000000000301"

type refundRow struct {
	status, payNo     string
	amount            int64
	failCode, failMsg sql.NullString
	succeededAt       sql.NullTime
	version           uint64
	updated           time.Time
}

func loadRefundRow(t *testing.T, env payEnv, refundNo string) refundRow {
	t.Helper()
	var row refundRow
	err := env.db.QueryRowContext(env.ctx, `
SELECT status, pay_no, amount, fail_code, fail_msg, succeeded_at, version, updated_at
FROM refund_order WHERE refund_no = ?`, refundNo).Scan(
		&row.status, &row.payNo, &row.amount, &row.failCode, &row.failMsg, &row.succeededAt, &row.version, &row.updated,
	)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

// refundEnvelope 只取断言要用的信封与 data 字段。
type refundEnvelope struct {
	EventType string `json:"event_type"`
	Data      struct {
		RefundNo    string `json:"refund_no"`
		PayNo       string `json:"pay_no"`
		MerchantID  string `json:"merchant_id"`
		OutRefundNo string `json:"out_refund_no"`
		Channel     string `json:"channel"`
		Amount      int64  `json:"amount"`
		FailCode    string `json:"fail_code"`
		SucceededAt string `json:"succeeded_at"`
	} `json:"data"`
}

func decodeRefundEvent(t *testing.T, payload string) refundEnvelope {
	t.Helper()
	var env refundEnvelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		t.Fatal(err)
	}
	return env
}

// processingRefund 在 MySQL 上造一笔 PROCESSING 退款：原支付 10000 已成功，冻结成功，渠道受理 OK。
func processingRefund(t *testing.T, env payEnv, merchantID string, amount money.Amount) (*Payments, RefundOrder) {
	t.Helper()
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, ref03PayNo, "OT_REF03_1", payStatusSuccess, now)
	p.UseRefund(&fakeFreezer{status: freezeStatusFrozen},
		&fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_REF03_1"}}, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(env.ctx, &CreateRefundInput{
		MerchantID: merchantID, PayNo: ref03PayNo, OutRefundNo: "R03_1", Amount: amount,
	})
	if err != nil || got.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", got, err)
	}
	return p, got
}

func refundNotify(refundNo, result string, amount int64, finished time.Time) RefundResult {
	return RefundResult{
		RefundNo: refundNo, Result: result, Source: paySourceNotify,
		Amount: fenPtr(amount), ChannelTxnNo: "CH_REF03_1", ChannelStatusRaw: result, FinishedAt: finished,
	}
}

// maskedNo 是两位前缀加 19 位数字的单号经约定 §7.5 兜底扫描后的形态：数字段保留前 6 位、后 4 位，中间 9 位换成 *。
func maskedNo(t *testing.T, no string) string {
	t.Helper()
	if len(no) != 21 {
		t.Fatalf("unexpected no %q", no)
	}
	return no[:8] + "*********" + no[17:]
}

func countLevelLines(buf *bytes.Buffer, level, msg string) (all, matched int) {
	for _, line := range strings.Split(buf.String(), "\n") {
		if !strings.Contains(line, `"level":"`+level+`"`) {
			continue
		}
		all++
		if strings.Contains(line, `"msg":"`+msg+`"`) {
			matched++
		}
	}
	return all, matched
}

func countErrorLines(buf *bytes.Buffer, msg string) (all, matched int) {
	return countLevelLines(buf, "ERROR", msg)
}

// REF-03 验收 2：渠道 FAILED 结果直接调用 ApplyRefundResult。退款 FAILED（CHANNEL_FAILED），refunded_amount 恢复，outbox 恰好 1 条 refund.failed。
func TestApplyRefundChannelFailedMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	p, refund := processingRefund(t, env, merchantID, 1001)
	if payRefundedAmount(t, env, ref03PayNo) != 1001 {
		t.Fatalf("refunded before = %d", payRefundedAmount(t, env, ref03PayNo))
	}
	finished := feeAt(10, 0, 3, 456000000)
	got, err := p.ApplyRefundResult(env.ctx, refundNotify(refund.RefundNo, payResultFailed, 1001, finished))
	if err != nil || got.Status != refundStatusFailed || got.FailCode != failCodeChannelFailed || got.RefundNo != refund.RefundNo {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	row := loadRefundRow(t, env, refund.RefundNo)
	if row.status != refundStatusFailed || row.failCode.String != failCodeChannelFailed || row.succeededAt.Valid || row.version != refund.Version+1 {
		t.Fatalf("refund row = %+v", row)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 0 {
		t.Fatalf("refunded after = %d", payRefundedAmount(t, env, ref03PayNo))
	}
	txn := loadAppliedTxn(t, env, refund.RefundNo)
	if txn.status != payResultFailed || txn.notifyCount != 1 || txn.nextQuery.Valid || !txn.finished.Valid || !txn.finished.Time.Equal(finished) {
		t.Fatalf("txn = %+v", txn)
	}
	rows := loadOutbox(t, env, refund.RefundNo)
	if len(rows) != 1 || rows[0].eventType != "refund.failed" || rows[0].topic != "pay.refund.failed" || rows[0].msgKey != ref03PayNo {
		t.Fatalf("outbox = %+v", rows)
	}
	evt := decodeRefundEvent(t, rows[0].payload)
	if evt.Data.RefundNo != refund.RefundNo || evt.Data.Amount != 1001 || evt.Data.FailCode != failCodeChannelFailed ||
		evt.Data.MerchantID != merchantID || evt.Data.OutRefundNo != "R03_1" || evt.Data.PayNo != ref03PayNo {
		t.Fatalf("event = %+v", evt)
	}
}

// REF-03 验收 5：同一 SUCCESS 回调 10 次（其中 5 次并发），退款只推进一次，outbox 只有 1 条 refund.succeeded，notify_count = 10。
func TestApplyRefundNotifyDuplicateMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	p, refund := processingRefund(t, env, merchantID, 3000)
	finished := feeAt(10, 0, 2, 789000000)
	in := refundNotify(refund.RefundNo, payResultSuccess, 3000, finished)
	kinds := make(chan string, 10)
	apply := func() error {
		kind, err := p.ApplyRefundNotify(env.ctx, in)
		if err != nil {
			return err
		}
		kinds <- kind
		return nil
	}
	var g errgroup.Group
	for i := 0; i < 5; i++ {
		g.Go(apply)
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := apply(); err != nil {
			t.Fatal(err)
		}
	}
	close(kinds)
	count := map[string]int{}
	for k := range kinds {
		count[k]++
	}
	// 并发的 5 次在读“回调前状态”时可能都看到 PROCESSING，所以只断言 APPLIED 至少 1 次、CONFLICT 为 0。
	if count["APPLIED"] < 1 || count["CONFLICT"] != 0 || count["APPLIED"]+count["DUPLICATE"] != 10 {
		t.Fatalf("kinds = %v", count)
	}
	row := loadRefundRow(t, env, refund.RefundNo)
	if row.status != payResultSuccess || !row.succeededAt.Valid || !row.succeededAt.Time.Equal(finished) || row.version != refund.Version+1 {
		t.Fatalf("refund row = %+v", row)
	}
	txn := loadAppliedTxn(t, env, refund.RefundNo)
	if txn.status != payResultSuccess || txn.notifyCount != 10 || txn.version != 1 {
		t.Fatalf("txn = %+v", txn)
	}
	rows := loadOutbox(t, env, refund.RefundNo)
	if len(rows) != 1 || rows[0].eventType != "refund.succeeded" || rows[0].topic != "pay.refund.succeeded" || rows[0].msgKey != ref03PayNo {
		t.Fatalf("outbox = %+v", rows)
	}
	evt := decodeRefundEvent(t, rows[0].payload)
	if evt.Data.RefundNo != refund.RefundNo || evt.Data.Amount != 3000 || evt.Data.Channel != "mockpay" ||
		evt.Data.SucceededAt != "2026-10-17T10:00:02.789+08:00" {
		t.Fatalf("event = %+v", evt)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 3000 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
}

// REF-03 规则 4（同 PAY-04）：先 FAILED 再 SUCCESS，退款保持 FAILED，冲突计数 +1，额度只释放一次。
func TestApplyRefundConflictMySQL(t *testing.T) {
	env := openPay(t)
	p, refund := processingRefund(t, env, "M100002", 2000)
	finished := feeAt(10, 0, 3, 0)
	if _, err := p.ApplyRefundResult(env.ctx, refundNotify(refund.RefundNo, payResultFailed, 2000, finished)); err != nil {
		t.Fatal(err)
	}
	before := callbackConflicts("mockpay")
	kind, err := p.ApplyRefundNotify(env.ctx, refundNotify(refund.RefundNo, payResultSuccess, 2000, finished))
	if err != nil || kind != "CONFLICT" {
		t.Fatalf("kind=%s err=%v", kind, err)
	}
	if callbackConflicts("mockpay") != before+1 {
		t.Fatalf("conflicts = %d before %d", callbackConflicts("mockpay"), before)
	}
	if row := loadRefundRow(t, env, refund.RefundNo); row.status != refundStatusFailed || row.succeededAt.Valid {
		t.Fatalf("refund row = %+v", row)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 0 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
	if rows := loadOutbox(t, env, refund.RefundNo); len(rows) != 1 || rows[0].eventType != "refund.failed" {
		t.Fatalf("outbox = %+v", rows)
	}
	// 冲突不加 notify_count（同 PAY-04）。同一终态的重复 FAILED 只加 notify_count：1（首次）+ 0（冲突）+ 1（重复）。
	kind, err = p.ApplyRefundNotify(env.ctx, refundNotify(refund.RefundNo, payResultFailed, 2000, finished))
	if err != nil || kind != "DUPLICATE" {
		t.Fatalf("dup kind=%s err=%v", kind, err)
	}
	if txn := loadAppliedTxn(t, env, refund.RefundNo); txn.notifyCount != 2 || txn.status != payResultFailed {
		t.Fatalf("txn = %+v", txn)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 0 {
		t.Fatalf("refunded after dup = %d", payRefundedAmount(t, env, ref03PayNo))
	}
}

// REF-03 规则 4（同 PAY-04）：金额不一致 50002、未知单号 30001，退款与流水都不变。
func TestApplyRefundRejectMySQL(t *testing.T) {
	env := openPay(t)
	p, refund := processingRefund(t, env, "M100002", 2000)
	beforeTxn := loadAppliedTxn(t, env, refund.RefundNo)
	finished := feeAt(10, 0, 3, 0)
	_, err := p.ApplyRefundResult(env.ctx, refundNotify(refund.RefundNo, payResultSuccess, 1999, finished))
	assertErrno(t, err, errno.ErrChannelAmountMismatch)
	_, err = p.ApplyRefundResult(env.ctx, refundNotify("RF0000000000000000099", payResultSuccess, 2000, finished))
	assertErrno(t, err, errno.ErrOrderNotFound)
	// PROCESSING 的 PAY 流水不是退款流水：渠道 CAS 带 txn_type = 'REFUND'，不会改到它，再读到 PAY 流水按 30001 处理。
	// 去掉该条件时 CAS 会命中这行，随后退款单 CAS 未命中并回滚，返回 10003，本断言失败。
	const payNo2 = "PA0000000000000000302"
	insertRefundPay(t, env, "M100002", payNo2, "OT_REF03_2", payStatusSuccess, feeAt(10, 0, 0, 0))
	_, err = env.db.ExecContext(env.ctx, `
INSERT INTO channel_txn (channel, txn_type, biz_no, amount, status, version, created_at, updated_at)
VALUES ('mockpay', 'PAY', ?, 10000, 'PROCESSING', 0, ?, ?)`, payNo2, finished, finished)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	beforePay := loadAppliedTxn(t, env, payNo2)
	_, err = p.ApplyRefundResult(env.ctx, refundNotify(payNo2, payResultSuccess, 10000, finished))
	assertErrno(t, err, errno.ErrOrderNotFound)
	if got := loadAppliedTxn(t, env, payNo2); got != beforePay || got.status != txnStatusProcessing || got.notifyCount != 0 || got.version != 0 {
		t.Fatalf("pay txn changed\nbefore %+v\nafter  %+v", beforePay, got)
	}
	if row := loadRefundRow(t, env, refund.RefundNo); row.status != refundStatusProcessing || row.version != refund.Version {
		t.Fatalf("refund row = %+v", row)
	}
	if got := loadAppliedTxn(t, env, refund.RefundNo); got != beforeTxn {
		t.Fatalf("txn changed\nbefore %+v\nafter  %+v", beforeTxn, got)
	}
	if rows := loadOutbox(t, env, refund.RefundNo); len(rows) != 0 {
		t.Fatalf("outbox = %+v", rows)
	}
}

// REF-03 规则 1（同 PAY-04）：退款单 CAS 未命中时整笔回滚，channel_txn 保持 PROCESSING，返回 10003。
func TestApplyRefundOrderCASMissMySQL(t *testing.T) {
	env := openPay(t)
	p, refund := processingRefund(t, env, "M100002", 2000)
	beforeTxn := loadAppliedTxn(t, env, refund.RefundNo)
	if _, err := env.db.ExecContext(env.ctx, `UPDATE refund_order SET status = 'INIT' WHERE refund_no = ?`, refund.RefundNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err := p.ApplyRefundResult(env.ctx, refundNotify(refund.RefundNo, payResultSuccess, 2000, feeAt(10, 0, 3, 0)))
	assertErrno(t, err, errno.ErrInternal)
	if got := loadAppliedTxn(t, env, refund.RefundNo); got != beforeTxn {
		t.Fatalf("txn changed\nbefore %+v\nafter  %+v", beforeTxn, got)
	}
	if rows := loadOutbox(t, env, refund.RefundNo); len(rows) != 0 {
		t.Fatalf("outbox = %+v", rows)
	}
}

// REF-03 规则 2：INIT 失败分支重复调用只释放一次额度、只写 1 条 refund.failed；PROCESSING 退款不接受 INIT 失败原因。
func TestApplyRefundInitFailureMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, ref03PayNo, "OT_REF03_1", payStatusSuccess, now)
	initRow, err := p.CreateRefund(env.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: ref03PayNo, OutRefundNo: "R03_INIT", Amount: 1500})
	if err != nil || initRow.Status != refundStatusInit {
		t.Fatalf("init = %+v err=%v", initRow, err)
	}
	in := RefundResult{RefundNo: initRow.RefundNo, Result: payResultFailed, FailCode: failCodeRefundTimeout, Source: paySourceJob}
	for i := 0; i < 2; i++ {
		got, applyErr := p.ApplyRefundResult(env.ctx, in)
		if applyErr != nil || got.Status != refundStatusFailed || got.FailCode != failCodeRefundTimeout {
			t.Fatalf("apply %d = %+v err=%v", i, got, applyErr)
		}
	}
	if row := loadRefundRow(t, env, initRow.RefundNo); row.status != refundStatusFailed || row.version != 1 || row.failMsg.String != "" {
		t.Fatalf("refund row = %+v", row)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 0 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
	rows := loadOutbox(t, env, initRow.RefundNo)
	if len(rows) != 1 || rows[0].eventType != "refund.failed" || decodeRefundEvent(t, rows[0].payload).Data.FailCode != failCodeRefundTimeout {
		t.Fatalf("outbox = %+v", rows)
	}
	if countBiz(t, env, "channel_txn", "biz_no", initRow.RefundNo) != 0 {
		t.Fatal("init failure wrote channel_txn")
	}

	// 卡单恢复把 INIT 判超时的同时，退款已被推进为 PROCESSING：INIT CAS 未命中，返回 10003，不动渠道流水。
	p.UseRefund(&fakeFreezer{status: freezeStatusFrozen},
		&fakeRefunder{res: ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: "CH_REF03_2"}}, nil, testChannelNotifyURL)
	proc, err := p.CreateRefund(env.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: ref03PayNo, OutRefundNo: "R03_PROC", Amount: 1500})
	if err != nil || proc.Status != refundStatusProcessing {
		t.Fatalf("proc = %+v err=%v", proc, err)
	}
	beforeTxn := loadAppliedTxn(t, env, proc.RefundNo)
	beforeConflicts := callbackConflicts("mockpay")
	logs := captureLog(t)
	_, err = p.ApplyRefundResult(env.ctx, RefundResult{RefundNo: proc.RefundNo, Result: payResultFailed, FailCode: failCodeRefundTimeout, Source: paySourceJob})
	assertErrno(t, err, errno.ErrInternal)
	if warns, matched := countLevelLines(logs, "WARN", "refund result not init"); warns != 1 || matched != 1 || callbackConflicts("mockpay") != beforeConflicts {
		t.Fatalf("logs = %s conflicts %d before %d", logs.String(), callbackConflicts("mockpay"), beforeConflicts)
	}
	if row := loadRefundRow(t, env, proc.RefundNo); row.status != refundStatusProcessing {
		t.Fatalf("proc row = %+v", row)
	}
	if got := loadAppliedTxn(t, env, proc.RefundNo); got != beforeTxn {
		t.Fatalf("txn changed\nbefore %+v\nafter  %+v", beforeTxn, got)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 1500 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
}

// REF-03 规则 2：INIT 失败原因落到已 SUCCESS 的退款上是并发竞争，不是渠道冲突：保持 SUCCESS，返回当前退款单，
// 不计 pay_callback_conflicts_total，只打 1 条 WARN、没有 ERROR，不释放额度、不写 refund.failed；未知退款单号返回 30001。
func TestApplyRefundInitFailureAfterTerminalMySQL(t *testing.T) {
	env := openPay(t)
	p, refund := processingRefund(t, env, "M100002", 2000)
	if _, err := p.ApplyRefundResult(env.ctx, refundNotify(refund.RefundNo, payResultSuccess, 2000, feeAt(10, 0, 3, 0))); err != nil {
		t.Fatal(err)
	}
	before := callbackConflicts("mockpay")
	logs := captureLog(t)
	got, err := p.ApplyRefundResult(env.ctx, RefundResult{RefundNo: refund.RefundNo, Result: payResultFailed, FailCode: failCodeRefundTimeout, Source: paySourceJob})
	if err != nil || got.Status != payResultSuccess || got.RefundNo != refund.RefundNo {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if callbackConflicts("mockpay") != before {
		t.Fatalf("conflicts = %d before %d", callbackConflicts("mockpay"), before)
	}
	if errs, _ := countLevelLines(logs, "ERROR", ""); errs != 0 {
		t.Fatalf("error logs = %s", logs.String())
	}
	if warns, matched := countLevelLines(logs, "WARN", "refund result not init"); warns != 1 || matched != 1 ||
		!strings.Contains(logs.String(), `"status":"SUCCESS"`) {
		t.Fatalf("warn logs = %s", logs.String())
	}
	if payRefundedAmount(t, env, ref03PayNo) != 2000 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
	if rows := loadOutbox(t, env, refund.RefundNo); len(rows) != 1 || rows[0].eventType != "refund.succeeded" {
		t.Fatalf("outbox = %+v", rows)
	}
	_, err = p.ApplyRefundResult(env.ctx, RefundResult{RefundNo: "RF0000000000000000099", Result: payResultFailed, FailCode: failCodeFreezeConflict, Source: paySourceSync})
	assertErrno(t, err, errno.ErrOrderNotFound)
}

// REF-03 验收 4：假渠道注入 REJECTED，接口返回 30006，退款 FAILED（CHANNEL_REJECTED），refunded_amount 恢复，outbox 1 条 refund.failed。
func TestCreateRefundChannelRejectedMySQL(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100002"
	now := feeAt(10, 0, 0, 0)
	p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
	insertRefundPay(t, env, merchantID, ref03PayNo, "OT_REF03_1", payStatusSuccess, now)
	channel := &fakeRefunder{res: ChannelAcceptResult{Outcome: acceptRejected, Reason: "REFUND_EXCEEDED"}}
	p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, channel, nil, testChannelNotifyURL)
	got, err := p.CreateRefund(env.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: ref03PayNo, OutRefundNo: "R03_REJ", Amount: 1000})
	assertErrno(t, err, errno.ErrChannelRejected)
	if got.Status != refundStatusFailed || got.FailCode != failCodeChannelRejected || got.FailMsg != "REFUND_EXCEEDED" {
		t.Fatalf("got = %+v", got)
	}
	row := loadRefundRow(t, env, got.RefundNo)
	if row.status != refundStatusFailed || row.failCode.String != failCodeChannelRejected || row.failMsg.String != "REFUND_EXCEEDED" || row.version != 2 {
		t.Fatalf("refund row = %+v", row)
	}
	if payRefundedAmount(t, env, ref03PayNo) != 0 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
	}
	txn := loadAppliedTxn(t, env, got.RefundNo)
	if txn.status != payResultFailed || txn.notifyCount != 0 || txn.nextQuery.Valid || txn.finished.Valid {
		t.Fatalf("txn = %+v", txn)
	}
	rows := loadOutbox(t, env, got.RefundNo)
	if len(rows) != 1 || rows[0].eventType != "refund.failed" || decodeRefundEvent(t, rows[0].payload).Data.FailCode != failCodeChannelRejected {
		t.Fatalf("outbox = %+v", rows)
	}
	if channel.callCount() != 1 {
		t.Fatalf("channel calls = %d", channel.callCount())
	}
}

// quotaBreakingFreezer 在冻结返回 40002 之前把 refunded_amount 改小，模拟额度数据已不一致。
type quotaBreakingFreezer struct {
	env   payEnv
	payNo string
}

func (q quotaBreakingFreezer) FreezeRefund(ctx context.Context, _, _ string, _ money.Amount) (RefundFreeze, error) {
	if _, err := q.env.db.ExecContext(ctx, `UPDATE payment_order SET refunded_amount = 0 WHERE pay_no = ?`, q.payNo); err != nil {
		return RefundFreeze{}, err
	}
	return RefundFreeze{}, errno.ErrInsufficientBalance
}

// REF-03 验收 6：refunded_amount 被改成小于 R 后，PROCESSING 退款的渠道 FAILED 与 INIT 退款的冻结失败分支都返回 10003。
// 退款单与 channel_txn 保持原状态，refunded_amount 不变，outbox 没有 refund.failed，指标各 +1，各一条 ERROR 日志。
func TestApplyRefundQuotaInconsistentMySQL(t *testing.T) {
	t.Run("PROCESSING", func(t *testing.T) {
		env := openPay(t)
		p, refund := processingRefund(t, env, "M100002", 1000)
		if _, err := env.db.ExecContext(env.ctx, `UPDATE payment_order SET refunded_amount = 999 WHERE pay_no = ?`, ref03PayNo); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		beforeRefund := loadRefundRow(t, env, refund.RefundNo)
		beforeTxn := loadAppliedTxn(t, env, refund.RefundNo)
		beforeCount := refundQuotaInconsistent.Load()
		logs := captureLog(t)
		_, err := p.ApplyRefundResult(env.ctx, refundNotify(refund.RefundNo, payResultFailed, 1000, feeAt(10, 0, 3, 0)))
		assertErrno(t, err, errno.ErrInternal)
		if got := loadRefundRow(t, env, refund.RefundNo); got != beforeRefund {
			t.Fatalf("refund changed\nbefore %+v\nafter  %+v", beforeRefund, got)
		}
		if got := loadAppliedTxn(t, env, refund.RefundNo); got != beforeTxn {
			t.Fatalf("txn changed\nbefore %+v\nafter  %+v", beforeTxn, got)
		}
		if payRefundedAmount(t, env, ref03PayNo) != 999 {
			t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
		}
		if rows := loadOutbox(t, env, refund.RefundNo); len(rows) != 0 {
			t.Fatalf("outbox = %+v", rows)
		}
		if refundQuotaInconsistent.Load() != beforeCount+1 {
			t.Fatalf("counter = %d before %d", refundQuotaInconsistent.Load(), beforeCount)
		}
		all, matched := countErrorLines(logs, "refund quota inconsistent")
		// refund_no、pay_no 都含 19 位连续数字，按约定 §7.5 的兜底扫描被部分遮盖，按遮盖后的形态断言。
		if all != 1 || matched != 1 || !strings.Contains(logs.String(), `"refund_no":"`+maskedNo(t, refund.RefundNo)+`"`) ||
			!strings.Contains(logs.String(), `"pay_no":"`+maskedNo(t, ref03PayNo)+`"`) || !strings.Contains(logs.String(), `"amount":1000`) {
			t.Fatalf("logs = %s", logs.String())
		}
	})
	t.Run("INIT", func(t *testing.T) {
		env := openPay(t)
		const merchantID = "M100002"
		now := feeAt(10, 0, 0, 0)
		p := openPayments(t, env, merchantID, 60, 60, now, "merchant-demo")
		insertRefundPay(t, env, merchantID, ref03PayNo, "OT_REF03_1", payStatusSuccess, now)
		channel := &fakeRefunder{}
		p.UseRefund(quotaBreakingFreezer{env: env, payNo: ref03PayNo}, channel, nil, testChannelNotifyURL)
		beforeCount := refundQuotaInconsistent.Load()
		logs := captureLog(t)
		_, err := p.CreateRefund(env.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: ref03PayNo, OutRefundNo: "R03_QI", Amount: 1000})
		assertErrno(t, err, errno.ErrInternal)
		var refundNo string
		if err = env.db.QueryRowContext(env.ctx, `SELECT refund_no FROM refund_order WHERE out_refund_no = 'R03_QI'`).Scan(&refundNo); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if row := loadRefundRow(t, env, refundNo); row.status != refundStatusInit || row.version != 0 || row.failCode.Valid {
			t.Fatalf("refund row = %+v", row)
		}
		if payRefundedAmount(t, env, ref03PayNo) != 0 {
			t.Fatalf("refunded = %d", payRefundedAmount(t, env, ref03PayNo))
		}
		if rows := loadOutbox(t, env, refundNo); len(rows) != 0 {
			t.Fatalf("outbox = %+v", rows)
		}
		if countBiz(t, env, "channel_txn", "biz_no", refundNo) != 0 || channel.callCount() != 0 {
			t.Fatal("init branch touched channel")
		}
		if refundQuotaInconsistent.Load() != beforeCount+1 {
			t.Fatalf("counter = %d before %d", refundQuotaInconsistent.Load(), beforeCount)
		}
		all, matched := countErrorLines(logs, "refund quota inconsistent")
		if all != 1 || matched != 1 || !strings.Contains(logs.String(), `"refund_no":"`+maskedNo(t, refundNo)+`"`) ||
			!strings.Contains(logs.String(), `"pay_no":"`+maskedNo(t, ref03PayNo)+`"`) || !strings.Contains(logs.String(), `"amount":1000`) {
			t.Fatalf("logs = %s", logs.String())
		}
	})
}

// REF-03 验收 1：正常退款经渠道回调（GW-06 REFUND 分支）推进为 SUCCESS，写 succeeded_at，outbox 恰好 1 条 refund.succeeded。
// 同一用例再走一笔末两位 01 的退款，经真实回调推进为 FAILED（验收 2 的端到端形态）。
func TestRefundCallbackEndToEnd(t *testing.T) {
	env := openPay(t)
	const merchantID = "M100083"
	p, order := gw06Create(t, env, merchantID, "REF03-E2E", 10000)
	channelURL, mockDB, _ := listenChannel(t)
	callbackURL := startGateway(t, p)
	wireCallbackPay(t, p, channelURL, callbackURL)
	if _, err := p.Pay(env.ctx, payReq(order)); err != nil {
		t.Fatal(err)
	}
	clk := clock.Real{}
	if !waitUntil(t, clk, 5*time.Second, func() bool {
		return loadPayment(t, env, merchantID, "REF03-E2E").status == payStatusSuccess
	}) {
		t.Fatalf("payment not SUCCESS: %+v", loadPayment(t, env, merchantID, "REF03-E2E"))
	}
	client, err := NewChannelClient(ChannelClientConfig{BaseURL: channelURL, Key: []byte(testChannelKey), Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, client, nil, callbackURL)

	ok, err := p.CreateRefund(env.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R03_OK", Amount: 3000})
	if err != nil || ok.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", ok, err)
	}
	if !waitUntil(t, clk, 5*time.Second, func() bool {
		return loadRefundRow(t, env, ok.RefundNo).status == payResultSuccess
	}) {
		t.Fatalf("refund not SUCCESS: %+v txn %+v", loadRefundRow(t, env, ok.RefundNo), loadAppliedTxn(t, env, ok.RefundNo))
	}
	row := loadRefundRow(t, env, ok.RefundNo)
	txn := loadAppliedTxn(t, env, ok.RefundNo)
	if !row.succeededAt.Valid || !txn.finished.Valid || !row.succeededAt.Time.Equal(txn.finished.Time) {
		t.Fatalf("succeeded_at = %+v finished = %+v", row.succeededAt, txn.finished)
	}
	if txn.status != payResultSuccess || txn.notifyCount != 1 || !txn.txnNo.Valid {
		t.Fatalf("txn = %+v", txn)
	}
	rows := loadOutbox(t, env, ok.RefundNo)
	if len(rows) != 1 || rows[0].eventType != "refund.succeeded" || rows[0].msgKey != order.PayNo {
		t.Fatalf("outbox = %+v", rows)
	}
	if payRefundedAmount(t, env, order.PayNo) != 3000 {
		t.Fatalf("refunded = %d", payRefundedAmount(t, env, order.PayNo))
	}

	failed, err := p.CreateRefund(env.ctx, &CreateRefundInput{MerchantID: merchantID, PayNo: order.PayNo, OutRefundNo: "R03_FAIL", Amount: 1001})
	if err != nil || failed.Status != refundStatusProcessing {
		t.Fatalf("refund = %+v err=%v", failed, err)
	}
	if !waitUntil(t, clk, 5*time.Second, func() bool {
		return loadRefundRow(t, env, failed.RefundNo).status == refundStatusFailed
	}) {
		t.Fatalf("refund not FAILED: %+v", loadRefundRow(t, env, failed.RefundNo))
	}
	if row = loadRefundRow(t, env, failed.RefundNo); row.failCode.String != failCodeChannelFailed {
		t.Fatalf("failed row = %+v", row)
	}
	if payRefundedAmount(t, env, order.PayNo) != 3000 {
		t.Fatalf("refunded after fail = %d", payRefundedAmount(t, env, order.PayNo))
	}
	if rows = loadOutbox(t, env, failed.RefundNo); len(rows) != 1 || rows[0].eventType != "refund.failed" {
		t.Fatalf("outbox = %+v", rows)
	}
	var mockStatus string
	if err = mockDB.QueryRowContext(env.ctx, `SELECT status FROM mock_txn WHERE txn_type = 'REFUND' AND req_no = ?`, failed.RefundNo).Scan(&mockStatus); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if mockStatus != refundStatusFailed {
		t.Fatalf("mock status = %s", mockStatus)
	}
}
