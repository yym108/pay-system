//go:build integration

package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/dbx"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := testkit.Shutdown(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %s\n", testkit.Redact(err))
	}
	os.Exit(code)
}

func openOutbox(t *testing.T) (context.Context, *sqlx.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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

func countOutbox(t *testing.T, ctx context.Context, db *sqlx.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

type storedOutbox struct {
	eventID, eventType, topic, msgKey, bizNo, status string
	payload                                          []byte
	retryCount                                       int
	nextRetry, created, updated                      time.Time
	lastError                                        sql.NullString
	sentAt                                           sql.NullTime
}

func loadOnlyOutbox(t *testing.T, ctx context.Context, db *sqlx.DB) storedOutbox {
	t.Helper()
	var row storedOutbox
	err := db.QueryRowContext(ctx, `
SELECT event_id, event_type, topic, msg_key, biz_no, payload, status,
       retry_count, next_retry_at, last_error, sent_at, created_at, updated_at
FROM outbox`).Scan(
		&row.eventID, &row.eventType, &row.topic, &row.msgKey, &row.bizNo, &row.payload, &row.status,
		&row.retryCount, &row.nextRetry, &row.lastError, &row.sentAt, &row.created, &row.updated,
	)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row
}

// MQ-01 验收：业务事务回滚时 outbox 无记录；提交时恰好 1 条（提交）。
func TestAddCommitOneRow(t *testing.T) {
	ctx, db := openOutbox(t)
	at, clk := clockAt()
	ids := mustGen(t, clk)
	evt := validPayment()
	span := spanCtx(t, ctx, 1)
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		return Add(span, tx, evt, ids, clk)
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n := countOutbox(t, ctx, db); n != 1 {
		t.Fatalf("rows = %d", n)
	}
	row := loadOnlyOutbox(t, ctx, db)
	if row.status != "PENDING" || row.retryCount != 0 || row.lastError.Valid || row.sentAt.Valid {
		t.Fatalf("status row = %+v", row)
	}
	if row.eventType != "payment.succeeded" || row.topic != "pay.payment.succeeded" ||
		row.msgKey != evt.PayNo || row.bizNo != evt.PayNo {
		t.Fatalf("route = %+v", row)
	}
	if !eventIDRE.MatchString(row.eventID) {
		t.Fatalf("event_id = %s", row.eventID)
	}
	if !row.nextRetry.Equal(at) || !row.created.Equal(at) || !row.updated.Equal(at) {
		t.Fatalf("times next=%s created=%s updated=%s want %s", row.nextRetry, row.created, row.updated, at)
	}
	payload := payloadMap(t, row.payload)
	assertString(t, payload, "event_id", row.eventID)
	assertString(t, payload, "event_type", "payment.succeeded")
	assertString(t, payload, "occurred_at", wantOccurred)
	assertNumber(t, payload, "version", "1")
	traceMap, ok := payload["trace_context"].(map[string]any)
	if !ok || len(traceMap) != 1 {
		t.Fatalf("trace_context = %#v", payload["trace_context"])
	}
	assertString(t, traceMap, "traceparent", "00-"+traceHex+"-"+spanHex+"-01")
	assertData(t, payload, map[string]string{
		"pay_no": evt.PayNo, "merchant_id": evt.MerchantID, "out_trade_no": evt.OutTradeNo,
		"channel": evt.Channel, "paid_at": wantPaidAt,
	}, map[string]string{"amount": "10000", "fee_amount": "60"})
}

// MQ-01 验收：业务事务回滚时 outbox 无记录；提交时恰好 1 条（回滚）。
func TestAddRollbackNoRow(t *testing.T) {
	ctx, db := openOutbox(t)
	_, clk := clockAt()
	ids := mustGen(t, clk)
	sentinel := errors.New("mq-01 rollback")
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO outbox (
    event_id, event_type, topic, msg_key, biz_no, payload, status, retry_count, next_retry_at, created_at, updated_at
) VALUES ('EV0000000000000000001', 'payment.succeeded', 'pay.payment.succeeded', 'PA0000000123456789012', 'PA0000000123456789012', '{}', 'PENDING', 0, ?, ?, ?)`,
			clk.Now(), clk.Now(), clk.Now()); err != nil {
			return err
		}
		if err := Add(ctx, tx, validPayment(), ids, clk); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(testkit.Redact(err))
	}
	if n := countOutbox(t, ctx, db); n != 0 {
		t.Fatalf("rows = %d, want 0", n)
	}
}

// MQ-01：1062 返回错误，不换 event_id 重试，也不当成成功。
func TestInsertDuplicateReturnsError(t *testing.T) {
	ctx, db := openOutbox(t)
	_, clk := clockAt()
	row, err := prepare(ctx, validPayment(), mustGen(t, clk), clk)
	if err != nil {
		t.Fatal(err)
	}
	err = dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		if err := insert(ctx, tx, row); err != nil {
			return err
		}
		dup := insert(ctx, tx, row)
		if !dbx.IsDuplicate(dup) {
			return fmt.Errorf("duplicate err = %w", dup)
		}
		return nil
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n := countOutbox(t, ctx, db); n != 1 {
		t.Fatalf("rows = %d", n)
	}
	got := loadOnlyOutbox(t, ctx, db)
	if got.eventID != row.eventID {
		t.Fatalf("event_id = %s, want %s", got.eventID, row.eventID)
	}
}
