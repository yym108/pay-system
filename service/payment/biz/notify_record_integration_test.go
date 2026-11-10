//go:build integration

package biz

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/shard"
)

// NTF-01 验收 1：同一事件消费 3 次，只有 1 条通知记录
func TestNTF01Accept1SameEventThreeTimes(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100001", 60, "ACTIVE")
	insertNotifyPayment(t, env, "PA0000000000000000001", "M100001", "https://pay.example/n")
	clk := notifyTestClock()
	ids, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	merchants := &fakeMerchants{cfg: MerchantConfig{MerchantID: "M100001", Status: "ACTIVE", DefaultNotifyURL: "https://default.example/n"}}
	p := NewPayments(merchants, ids, clk, "", env.db)
	in := paymentNotifyInput()
	var first NotifyRecordResult
	for i := 0; i < 3; i++ {
		got, recErr := p.RecordNotify(env.ctx, in)
		if recErr != nil {
			t.Fatalf("call %d err=%v", i, recErr)
		}
		if i == 0 {
			first = got
		}
		if got.NotifyNo != first.NotifyNo || len(got.NotifyNo) < 2 || got.NotifyNo[:2] != notifyNoPrefix {
			t.Fatalf("call %d notify_no=%s first=%s", i, got.NotifyNo, first.NotifyNo)
		}
	}
	if merchants.configCalls != 0 {
		t.Fatalf("order url still read merchant %d times", merchants.configCalls)
	}
	if n := countNotify(t, env, in.EventID); n != 1 {
		t.Fatalf("rows=%d", n)
	}
	row := loadNotify(t, env, in.EventID)
	if row.status != notifyStatusPending || row.attempts != 0 || row.url != "https://pay.example/n" || !row.nulls {
		t.Fatalf("row=%+v", row)
	}
	wantAt := clk.Now().Truncate(time.Millisecond).UnixMilli()
	if row.nextAt.UnixMilli() != wantAt || row.createdAt.UnixMilli() != wantAt || row.updatedAt.UnixMilli() != wantAt {
		t.Fatalf("times next=%s created=%s updated=%s want=%d", row.nextAt, row.createdAt, row.updatedAt, wantAt)
	}
	body := decodeNotifyBody(t, row.payload)
	if body["notify_id"] != first.NotifyNo || body["event_type"] != notifyEventPaymentSucceeded || body["merchant_id"] != "M100001" {
		t.Fatalf("body=%v", body)
	}
	data := bodyData(t, body)
	assertKeys(t, data, "pay_no", "out_trade_no", "amount", "fee_amount", "status", "paid_at")
	if data["status"] != notifyStatusSuccess || data["paid_at"] != in.PaidAt || data["pay_no"] != in.PayNo {
		t.Fatalf("data=%v", data)
	}
	assertNumber(t, data["amount"], 10000)
	assertNumber(t, data["fee_amount"], 60)
	if _, ok := data["channel"]; ok {
		t.Fatalf("payload has channel: %v", data)
	}
}

func TestRecordNotifyEmptyDefaultStillInserts(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100001", 60, "DISABLED")
	insertNotifyPayment(t, env, "PA0000000000000000001", "M100001", "")
	p, merchants := newNotifyPayments(t, env, &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000000000000001"}}, MerchantConfig{
		MerchantID: "M100001", Status: "DISABLED", DefaultNotifyURL: "",
	})
	got, err := p.RecordNotify(env.ctx, paymentNotifyInput())
	if err != nil {
		t.Fatal(err)
	}
	if got.NotifyNo != "NT0000000000000000000000000001" || merchants.configCalls != 1 {
		t.Fatalf("got=%+v calls=%d", got, merchants.configCalls)
	}
	row := loadNotify(t, env, paymentNotifyInput().EventID)
	if row.url != "" || row.status != notifyStatusPending {
		t.Fatalf("row=%+v", row)
	}
	insertNotifyPayment(t, env, "PA0000000000000000002", "M100001", " https://spaced.example/n ")
	spaced := paymentNotifyInput()
	spaced.EventID = "EV00000000000000000000000012"
	spaced.PayNo = "PA0000000000000000002"
	p.ids = &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000000000000009"}}
	if _, err = p.RecordNotify(env.ctx, spaced); err != nil {
		t.Fatal(err)
	}
	if merchants.configCalls != 1 {
		t.Fatalf("spaced url read merchant %d times", merchants.configCalls)
	}
	if gotURL := loadNotify(t, env, spaced.EventID).url; gotURL != " https://spaced.example/n " {
		t.Fatalf("url=%q", gotURL)
	}
}

func TestRecordNotifyRefundUsesPaymentURL(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100001", 60, "ACTIVE")
	insertNotifyPayment(t, env, "PA0000000000000000001", "M100001", "https://pay.example/refund")
	insertNotifyRefund(t, env, "RF0000000000000000001", "M100001", "PA0000000000000000001", "SUCCESS")
	p, merchants := newNotifyPayments(t, env, &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000000000000002"}}, MerchantConfig{
		MerchantID: "M100001", Status: "ACTIVE", DefaultNotifyURL: "https://default.example/n",
	})
	in := refundNotifyInput(false)
	if _, err := p.RecordNotify(env.ctx, in); err != nil {
		t.Fatal(err)
	}
	if merchants.configCalls != 0 {
		t.Fatalf("refund read merchant default %d times", merchants.configCalls)
	}
	row := loadNotify(t, env, in.EventID)
	if row.url != "https://pay.example/refund" || row.bizType != notifyBizRefund || row.bizNo != in.RefundNo {
		t.Fatalf("row=%+v", row)
	}
	data := bodyData(t, decodeNotifyBody(t, row.payload))
	assertKeys(t, data, "refund_no", "out_refund_no", "pay_no", "amount", "status", "succeeded_at")
	if data["status"] != notifyStatusSuccess || data["succeeded_at"] != in.SucceededAt {
		t.Fatalf("data=%v", data)
	}

	in = refundNotifyInput(true)
	in.EventID = "EV0000000000000000000000000F"
	p.ids = &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000000000000003"}}
	if _, err := p.RecordNotify(env.ctx, in); err != nil {
		t.Fatal(err)
	}
	failed := bodyData(t, decodeNotifyBody(t, loadNotify(t, env, in.EventID).payload))
	assertKeys(t, failed, "refund_no", "out_refund_no", "pay_no", "amount", "status", "fail_code")
	if failed["status"] != notifyStatusFailed || failed["fail_code"] != "CHANNEL_REJECTED" {
		t.Fatalf("failed=%v", failed)
	}
}

func TestRecordNotifyTransferPayerOnly(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100001", 60, "ACTIVE")
	insertMerchant(t, env, "M100002", 60, "ACTIVE")
	insertNotifyTransfer(t, env, "TF0000000000000000001", "M100001", "M100002", "https://payer.example/n")
	p, merchants := newNotifyPayments(t, env, &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000000000000004"}}, MerchantConfig{
		MerchantID: "M100001", Status: "ACTIVE", DefaultNotifyURL: "https://default.example/payer",
	})
	in := transferNotifyInput(false)
	if _, err := p.RecordNotify(env.ctx, in); err != nil {
		t.Fatal(err)
	}
	if merchants.configCalls != 0 {
		t.Fatalf("transfer url still read merchant %d", merchants.configCalls)
	}
	row := loadNotify(t, env, in.EventID)
	if row.merchantID != "M100001" || row.bizType != notifyBizTransfer || row.url != "https://payer.example/n" {
		t.Fatalf("row=%+v", row)
	}
	body := decodeNotifyBody(t, row.payload)
	if body["merchant_id"] != "M100001" {
		t.Fatalf("body merchant=%v", body["merchant_id"])
	}
	data := bodyData(t, body)
	assertKeys(t, data, "transfer_no", "out_transfer_no", "payee_merchant_id", "amount", "status", "finished_at")
	if data["payee_merchant_id"] != "M100002" || data["status"] != notifyStatusSuccess {
		t.Fatalf("data=%v", data)
	}
	if _, ok := data["mode"]; ok {
		t.Fatalf("payload has mode: %v", data)
	}

	insertNotifyTransfer(t, env, "TF0000000000000000002", "M100001", "M100002", "")
	in = transferNotifyInput(true)
	in.EventID = "EV00000000000000000000000010"
	in.TransferNo = "TF0000000000000000002"
	p.ids = &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000000000000005"}}
	if _, err := p.RecordNotify(env.ctx, in); err != nil {
		t.Fatal(err)
	}
	if merchants.configCalls != 1 {
		t.Fatalf("empty transfer url calls=%d", merchants.configCalls)
	}
	failed := loadNotify(t, env, in.EventID)
	if failed.url != "https://default.example/payer" || failed.merchantID != "M100001" {
		t.Fatalf("failed row=%+v", failed)
	}
	failedData := bodyData(t, decodeNotifyBody(t, failed.payload))
	assertKeys(t, failedData, "transfer_no", "out_transfer_no", "payee_merchant_id", "amount", "status", "fail_code", "finished_at")
	if failedData["status"] != notifyStatusFailed || failedData["fail_code"] != "INSUFFICIENT_BALANCE" {
		t.Fatalf("failed data=%v", failedData)
	}
}

func TestRecordNotifyMissingOrderAndMerchant(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100001", 60, "ACTIVE")
	p, _ := newNotifyPayments(t, env, &stubIDs{prefix: notifyNoPrefix, nos: []string{"NT0000000000000000000000000006"}}, MerchantConfig{
		MerchantID: "M100001", Status: "ACTIVE", DefaultNotifyURL: "https://default.example/n",
	})
	if _, err := p.RecordNotify(env.ctx, paymentNotifyInput()); !errors.Is(err, errno.ErrNotFound) {
		t.Fatalf("missing order err=%v", err)
	}
	if n := countNotifyAll(t, env); n != 0 {
		t.Fatalf("missing order inserted %d", n)
	}
	insertNotifyPayment(t, env, "PA0000000000000000001", "M100001", "")
	p.merchants = &fakeMerchants{cfg: MerchantConfig{MerchantID: "M199999"}}
	if _, err := p.RecordNotify(env.ctx, paymentNotifyInput()); !errors.Is(err, errno.ErrMerchantInvalid) {
		t.Fatalf("missing merchant err=%v", err)
	}
	if n := countNotifyAll(t, env); n != 0 {
		t.Fatalf("missing merchant inserted %d", n)
	}
}

func TestRecordNotifyClockRollbackAndNotifyNoConflict(t *testing.T) {
	env := openPay(t)
	insertMerchant(t, env, "M100001", 60, "ACTIVE")
	insertNotifyPayment(t, env, "PA0000000000000000001", "M100001", "https://pay.example/n")
	p, _ := newNotifyPayments(t, env, &stubIDs{prefix: notifyNoPrefix, err: idgen.ErrClockRollback}, MerchantConfig{
		MerchantID: "M100001", Status: "ACTIVE",
	})
	if _, err := p.RecordNotify(env.ctx, paymentNotifyInput()); !errors.Is(err, errno.ErrUnavailable) {
		t.Fatalf("rollback err=%v", err)
	}
	if n := countNotifyAll(t, env); n != 0 {
		t.Fatalf("rollback inserted %d", n)
	}

	fixed := "NT0000000000000000000000000007"
	p.ids = &stubIDs{prefix: notifyNoPrefix, nos: []string{fixed}}
	first := paymentNotifyInput()
	if _, err := p.RecordNotify(env.ctx, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.EventID = "EV00000000000000000000000011"
	_, err := p.RecordNotify(env.ctx, second)
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("notify_no conflict err=%v", err)
	}
	if n := countNotifyAll(t, env); n != 1 {
		t.Fatalf("rows=%d", n)
	}
	if loadNotify(t, env, first.EventID).notifyNo != fixed {
		t.Fatal("first row changed")
	}
	var missing int
	err = env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM merchant_notify WHERE event_id = ?`, second.EventID).Scan(&missing)
	if err != nil {
		t.Fatal(err)
	}
	if missing != 0 {
		t.Fatalf("second event rows=%d", missing)
	}
}

type notifyDBRow struct {
	notifyNo   string
	merchantID string
	bizType    string
	bizNo      string
	status     string
	attempts   int
	url        string
	nulls      bool
	nextAt     time.Time
	createdAt  time.Time
	updatedAt  time.Time
	payload    []byte
}

func newNotifyPayments(t *testing.T, env payEnv, ids Numberer, cfg MerchantConfig) (*Payments, *fakeMerchants) {
	t.Helper()
	merchants := &fakeMerchants{cfg: cfg}
	return NewPayments(merchants, ids, notifyTestClock(), "", env.db), merchants
}

func insertNotifyPayment(t *testing.T, env payEnv, payNo, merchantID, notifyURL string) {
	t.Helper()
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO payment_order (
  pay_no, merchant_id, out_trade_no, amount, fee_amount, subject, status, notify_url, expire_at
) VALUES (?, ?, ?, 10000, 60, 'demo', 'SUCCESS', ?, ?)`,
		payNo, merchantID, "out-"+payNo, notifyURL, time.Date(2026, 11, 10, 10, 15, 0, 0, notifyShanghai))
	if err != nil {
		t.Fatal(err)
	}
}

func insertNotifyRefund(t *testing.T, env payEnv, refundNo, merchantID, payNo, status string) {
	t.Helper()
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO refund_order (
  refund_no, merchant_id, out_refund_no, pay_no, amount, channel, status
) VALUES (?, ?, ?, ?, 1000, 'mockpay', ?)`,
		refundNo, merchantID, "out-"+refundNo, payNo, status)
	if err != nil {
		t.Fatal(err)
	}
}

func insertNotifyTransfer(t *testing.T, env payEnv, transferNo, payer, payee, notifyURL string) {
	t.Helper()
	payerShard, err := shard.Of(payer)
	if err != nil {
		t.Fatal(err)
	}
	payeeShard, err := shard.Of(payee)
	if err != nil {
		t.Fatal(err)
	}
	mode := "LOCAL"
	if payerShard != payeeShard {
		mode = "TCC"
	}
	_, err = env.db.ExecContext(env.ctx, `
INSERT INTO transfer_order (
  transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, notify_url,
  mode, payer_shard, payee_shard, status, next_retry_at, finished_at
) VALUES (?, ?, ?, ?, 500, ?, ?, ?, ?, 'SUCCESS', ?, ?)`,
		transferNo, payer, "out-"+transferNo, payee, notifyURL, mode, payerShard, payeeShard,
		time.Date(2026, 11, 10, 10, 0, 0, 0, notifyShanghai),
		time.Date(2026, 11, 10, 10, 0, 2, 0, notifyShanghai))
	if err != nil {
		t.Fatal(err)
	}
}

func countNotify(t *testing.T, env payEnv, eventID string) int {
	t.Helper()
	var n int
	err := env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM merchant_notify WHERE event_id = ?`, eventID).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func countNotifyAll(t *testing.T, env payEnv) int {
	t.Helper()
	var n int
	err := env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM merchant_notify`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func loadNotify(t *testing.T, env payEnv, eventID string) notifyDBRow {
	t.Helper()
	var row notifyDBRow
	var lastHTTP sql.NullInt64
	var lastErr sql.NullString
	var succeeded sql.NullTime
	err := env.db.QueryRowContext(env.ctx, `
SELECT notify_no, merchant_id, biz_type, biz_no, status, attempt_count, notify_url,
       last_http_status, last_error, succeeded_at, next_attempt_at, created_at, updated_at, payload
FROM merchant_notify WHERE event_id = ?`, eventID).Scan(
		&row.notifyNo, &row.merchantID, &row.bizType, &row.bizNo, &row.status, &row.attempts, &row.url,
		&lastHTTP, &lastErr, &succeeded, &row.nextAt, &row.createdAt, &row.updatedAt, &row.payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	row.nulls = !lastHTTP.Valid && !lastErr.Valid && !succeeded.Valid
	return row
}

func decodeNotifyBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body
}

func bodyData(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	data, ok := body["data"].(map[string]any)
	if !ok {
		t.Fatalf("data type %T", body["data"])
	}
	return data
}

func assertKeys(t *testing.T, data map[string]any, keys ...string) {
	t.Helper()
	if len(data) != len(keys) {
		t.Fatalf("keys=%v want=%v", data, keys)
	}
	for _, key := range keys {
		if _, ok := data[key]; !ok {
			t.Fatalf("missing %s in %v", key, data)
		}
	}
}

func assertNumber(t *testing.T, value any, want int64) {
	t.Helper()
	number, ok := value.(json.Number)
	if !ok {
		t.Fatalf("number type %T", value)
	}
	got, err := number.Int64()
	if err != nil || got != want {
		t.Fatalf("number=%s err=%v want=%d", number, err, want)
	}
}
