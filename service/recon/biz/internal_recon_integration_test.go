//go:build integration

package biz_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

func openSchemaForTest(t *testing.T, mysql *testkit.MySQL, svc string) *sqlx.DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sc, err := mysql.OpenSchema(ctx, svc)
	if err != nil {
		t.Fatalf("OpenSchema(%s): %v", svc, err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer ccancel()
		_ = sc.Close(cctx)
	})
	return sc.DB
}

func openReconPaymentAndAccountDBs(t *testing.T) (*sqlx.DB, *sqlx.DB, *sqlx.DB, *sqlx.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mysql, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatalf("testkit.Ready: %v", err)
	}
	return openSchemaForTest(t, mysql, "recon"),
		openSchemaForTest(t, mysql, "payment"),
		openSchemaForTest(t, mysql, "account"),
		openSchemaForTest(t, mysql, "account")
}

func insertPaymentOrderFixture(t *testing.T, db *sqlx.DB, payNo, merchantID, outTradeNo string, amount int64, status string, paidAt *time.Time, expireAt time.Time) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `INSERT INTO payment_order
		(pay_no, merchant_id, out_trade_no, amount, currency, subject, status, channel, fee_amount, refunded_amount, notify_url, expire_at, paid_at)
		VALUES (?, ?, ?, ?, 'CNY', 'test', ?, 'mockpay', 0, 0, '', ?, ?)`,
		payNo, merchantID, outTradeNo, amount, status, expireAt, paidAt)
	if err != nil {
		t.Fatalf("insert payment_order %s: %v", payNo, err)
	}
}

func insertRefundOrderFixture(t *testing.T, db *sqlx.DB, refundNo, merchantID, outRefundNo, payNo string, amount int64, status string, succeededAt *time.Time) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `INSERT INTO refund_order
		(refund_no, merchant_id, out_refund_no, pay_no, amount, currency, reason, status, channel, succeeded_at)
		VALUES (?, ?, ?, ?, ?, 'CNY', 'test', ?, 'mockpay', ?)`,
		refundNo, merchantID, outRefundNo, payNo, amount, status, succeededAt)
	if err != nil {
		t.Fatalf("insert refund_order %s: %v", refundNo, err)
	}
}

func insertLedgerTxnFixture(t *testing.T, db *sqlx.DB, txnNo, bizType, bizNo string, amount int64) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `INSERT INTO ledger_txn
		(txn_no, biz_type, biz_no, amount, currency, entry_count, memo)
		VALUES (?, ?, ?, ?, 'CNY', 2, '')`,
		txnNo, bizType, bizNo, amount)
	if err != nil {
		t.Fatalf("insert ledger_txn %s: %v", txnNo, err)
	}
}

func insertTransferOrderFixture(t *testing.T, db *sqlx.DB, transferNo, payer, payee string, amount int64, mode string, payerShard, payeeShard uint8, status string, failCode, txnNo *string, finishedAt time.Time) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `INSERT INTO transfer_order
		(transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, notify_url, mode, payer_shard, payee_shard, status, fail_code, txn_no, next_retry_at, finished_at)
		VALUES (?, ?, ?, ?, ?, 'CNY', '', ?, ?, ?, ?, ?, ?, ?, ?)`,
		transferNo, payer, "OUT_"+transferNo, payee, amount, mode, payerShard, payeeShard, status, failCode, txnNo, finishedAt, finishedAt)
	if err != nil {
		t.Fatalf("insert transfer_order %s: %v", transferNo, err)
	}
}

// REC-05 验收 1：删除某笔支付对应的 outbox 事件并阻止入账后，对账识别为 LEDGER_MISSING（含正常支付/退款对照）。
func TestInternalReconIntegrationPaymentOutboxDropped(t *testing.T) {
	reconDB, paymentDB, acc0, acc1 := openReconPaymentAndAccountDBs(t)
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	billDate := "2026-10-26"
	paidAt := time.Date(2026, 10, 26, 14, 0, 0, 0, shanghai)
	expireAt := paidAt.Add(30 * time.Minute)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids := newTestIDGen(t, clk)

	// 正常支付（M100000 -> shard 0）+ 正常退款（M100001 -> shard 1）
	insertPaymentOrderFixture(t, paymentDB, "PA_INT_OK", "M100000", "OT_OK", 1000, "SUCCESS", &paidAt, expireAt)
	insertLedgerTxnFixture(t, acc0, "TX_INT_PA_OK", "PAY", "PA_INT_OK", 1000)

	insertRefundOrderFixture(t, paymentDB, "RF_INT_OK", "M100001", "OR_OK", "PA_INT_OK", 300, "SUCCESS", &paidAt)
	insertLedgerTxnFixture(t, acc1, "TX_INT_RF_OK", "REFUND", "RF_INT_OK", 300)

	// 模拟删除 outbox 事件且未入账的支付单（M100001 -> shard 1）
	insertPaymentOrderFixture(t, paymentDB, "PA_INT_NO_LEDGER", "M100001", "OT_NO_LEDGER", 2500, "SUCCESS", &paidAt, expireAt)

	dl := &fakeDownloader{
		fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
			return biz.DownloadResult{FileHash: "hash_int_1"}, nil
		},
	}
	extChecker := biz.NewExternalChecker(reconDB, paymentDB, ids, clk)
	intChecker := biz.NewInternalChecker(reconDB, paymentDB, acc0, acc1, ids, clk)
	svc := biz.NewRecon(reconDB, ids, clk, []string{"mockpay"}, dl, biz.NewCompositeChecker(extChecker, intChecker))

	before := reconDiffCounter(t, dal.DiffLedgerMissing)
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	if out.Batch.Status != dal.StatusDone || out.Batch.DiffCount != 1 {
		t.Fatalf("batch = %+v, want DONE with diff_count=1", out.Batch)
	}
	if len(out.Diffs) != 1 || out.Diffs[0].DiffType != dal.DiffLedgerMissing || out.Diffs[0].BizNo != "PA_INT_NO_LEDGER" {
		t.Fatalf("diffs = %+v, want 1 LEDGER_MISSING for PA_INT_NO_LEDGER", out.Diffs)
	}
	if got := reconDiffCounter(t, dal.DiffLedgerMissing) - before; got != 1 {
		t.Fatalf("pay_recon_diffs_total{LEDGER_MISSING} delta = %d, want 1", got)
	}
}

// REC-05 验收 2：删除一张 TRANSFER_IN 凭证后识别为 TRANSFER_MISMATCH；正常转账差错数为 0；重跑同键跳过不报 1062。
func TestInternalReconIntegrationMissingTransferIn(t *testing.T) {
	reconDB, paymentDB, acc0, acc1 := openReconPaymentAndAccountDBs(t)
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	billDate := "2026-10-26"
	finAt := time.Date(2026, 10, 26, 15, 0, 0, 0, shanghai)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, shanghai))
	ids := newTestIDGen(t, clk)

	// 1. 正常同分片 LOCAL 转账（M100000 -> M100002, shard 0）
	txLocal := "TX_TRF_LOCAL_OK"
	insertTransferOrderFixture(t, paymentDB, "TF_INT_LOCAL_OK", "M100000", "M100002", 1000, "LOCAL", 0, 0, "SUCCESS", nil, &txLocal, finAt)
	insertLedgerTxnFixture(t, acc0, txLocal, "TRANSFER", "TF_INT_LOCAL_OK", 1000)

	// 2. 正常跨分片 TCC 转账（M100000 shard 0 -> M100001 shard 1）
	insertTransferOrderFixture(t, paymentDB, "TF_INT_TCC_OK", "M100000", "M100001", 2000, "TCC", 0, 1, "SUCCESS", nil, nil, finAt)
	insertLedgerTxnFixture(t, acc0, "TX_TRF_OUT_OK", "TRANSFER_OUT", "TF_INT_TCC_OK", 2000)
	insertLedgerTxnFixture(t, acc1, "TX_TRF_IN_OK", "TRANSFER_IN", "TF_INT_TCC_OK", 2000)
	if _, err := acc0.ExecContext(context.Background(), `INSERT INTO account_freeze
		(freeze_no, biz_type, biz_no, account_id, amount, status, txn_no)
		VALUES ('FZ_TCC_OK', 'TRANSFER', 'TF_INT_TCC_OK', 1, 2000, 'CONFIRMED', 'TX_TRF_OUT_OK')`); err != nil {
		t.Fatalf("insert account_freeze: %v", err)
	}
	if _, err := acc1.ExecContext(context.Background(), `INSERT INTO transfer_branch
		(transfer_no, role, payee_merchant_id, amount, status, txn_no)
		VALUES ('TF_INT_TCC_OK', 'PAYEE', 'M100001', 2000, 'CONFIRMED', 'TX_TRF_IN_OK')`); err != nil {
		t.Fatalf("insert transfer_branch: %v", err)
	}

	// 3. 异常跨分片 TCC 转账：删除了 TRANSFER_IN 凭证
	insertTransferOrderFixture(t, paymentDB, "TF_INT_MISS_IN", "M100000", "M100001", 3500, "TCC", 0, 1, "SUCCESS", nil, nil, finAt)
	insertLedgerTxnFixture(t, acc0, "TX_TRF_OUT_BAD", "TRANSFER_OUT", "TF_INT_MISS_IN", 3500)
	if _, err := acc0.ExecContext(context.Background(), `INSERT INTO account_freeze
		(freeze_no, biz_type, biz_no, account_id, amount, status, txn_no)
		VALUES ('FZ_TCC_BAD', 'TRANSFER', 'TF_INT_MISS_IN', 1, 3500, 'CONFIRMED', 'TX_TRF_OUT_BAD')`); err != nil {
		t.Fatalf("insert account_freeze bad: %v", err)
	}
	if _, err := acc1.ExecContext(context.Background(), `INSERT INTO transfer_branch
		(transfer_no, role, payee_merchant_id, amount, status, txn_no)
		VALUES ('TF_INT_MISS_IN', 'PAYEE', 'M100001', 3500, 'CONFIRMED', 'TX_MISSING_IN')`); err != nil {
		t.Fatalf("insert transfer_branch bad: %v", err)
	}

	dl := &fakeDownloader{
		fn: func(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (biz.DownloadResult, error) {
			return biz.DownloadResult{FileHash: "hash_int_2"}, nil
		},
	}
	intChecker := biz.NewInternalChecker(reconDB, paymentDB, acc0, acc1, ids, clk)
	svc := biz.NewRecon(reconDB, ids, clk, []string{"mockpay"}, dl, intChecker)

	before := reconDiffCounter(t, dal.DiffTransferMismatch)
	out, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	if out.Batch.Status != dal.StatusDone || out.Batch.DiffCount != 1 {
		t.Fatalf("batch = %+v, want DONE with diff_count=1", out.Batch)
	}
	if len(out.Diffs) != 1 || out.Diffs[0].DiffType != dal.DiffTransferMismatch || out.Diffs[0].BizNo != "TF_INT_MISS_IN" {
		t.Fatalf("diffs = %+v, want 1 TRANSFER_MISMATCH for TF_INT_MISS_IN", out.Diffs)
	}
	if out.Diffs[0].TxnType != "TRANSFER" || out.Diffs[0].HandleNote == nil || *out.Diffs[0].HandleNote == "" {
		t.Fatalf("unexpected TRANSFER_MISMATCH fields: %+v", out.Diffs[0])
	}
	if got := reconDiffCounter(t, dal.DiffTransferMismatch) - before; got != 1 {
		t.Fatalf("pay_recon_diffs_total{TRANSFER_MISMATCH} delta = %d, want 1", got)
	}

	// 将该差错标为 IGNORED 后 force 重跑，验证同键跳过不报 1062 且不重复计数
	if _, err := reconDB.ExecContext(context.Background(),
		`UPDATE recon_diff SET status = 'IGNORED' WHERE batch_no = ? AND biz_no = 'TF_INT_MISS_IN'`,
		out.Batch.BatchNo); err != nil {
		t.Fatalf("mark IGNORED: %v", err)
	}
	out2, err := svc.RunBatch(context.Background(), biz.RunBatchInput{Channel: "mockpay", BillDate: billDate, Force: true})
	if err != nil {
		t.Fatalf("force RunBatch: %v", err)
	}
	if out2.Batch.Status != dal.StatusDone || out2.Batch.DiffCount != 1 {
		t.Fatalf("rerun batch = %+v, want DONE with diff_count=1", out2.Batch)
	}
	if got := reconDiffCounter(t, dal.DiffTransferMismatch) - before; got != 1 {
		t.Fatalf("pay_recon_diffs_total{TRANSFER_MISMATCH} delta after rerun = %d, want 1", got)
	}
}
