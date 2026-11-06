package biz

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/money"
)

// updated_at 显式写入，与 created_at 同一次注入时钟，不用数据库时钟。
// fail_code、txn_no、finished_at 由 CHECK 约束要求 TRYING 时为 NULL。
const insertTransferSQL = `
INSERT INTO transfer_order (
  transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
  mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
  version, created_at, updated_at
) VALUES (
  ?, ?, ?, ?, ?, ?, ?, ?,
  ?, ?, ?, ?, NULL, NULL, 0, ?, NULL,
  0, ?, ?
)`

const transferByMerchantOutSQL = `
SELECT transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
       mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
       version, created_at, updated_at
FROM transfer_order
WHERE merchant_id = ? AND out_transfer_no = ?`

type sqlTransferStore struct {
	db *sqlx.DB
}

func (s sqlTransferStore) insertTransfer(ctx context.Context, row TransferOrder) error {
	// 空备注存 NULL，与未传一致。
	var remark any
	if row.Remark != "" {
		remark = row.Remark
	}
	_, err := s.db.ExecContext(ctx, insertTransferSQL,
		row.TransferNo, row.MerchantID, row.OutTransferNo, row.PayeeMerchantID, int64(row.Amount), row.Currency,
		remark, row.NotifyURL, row.Mode, row.PayerShard, row.PayeeShard, row.Status, row.NextRetryAt,
		row.CreatedAt, row.UpdatedAt,
	)
	return err
}

func (s sqlTransferStore) transferByMerchantOut(ctx context.Context, merchantID, outTransferNo string) (TransferOrder, bool, error) {
	row, err := scanTransfer(s.db.QueryRowContext(ctx, transferByMerchantOutSQL, merchantID, outTransferNo))
	if errors.Is(err, sql.ErrNoRows) {
		return TransferOrder{}, false, nil
	}
	if err != nil {
		return TransferOrder{}, false, err
	}
	// 列排序规则可能命中字节不同的键。只认完全相同的商户号和商户转账单号。
	if row.MerchantID != merchantID || row.OutTransferNo != outTransferNo {
		return TransferOrder{}, false, nil
	}
	return row, true, nil
}

func scanTransfer(s rowScanner) (TransferOrder, error) {
	var (
		row                     TransferOrder
		amount                  int64
		remark, failCode, txnNo sql.NullString
		finishedAt              sql.NullTime
		nextRetryAt             time.Time
		createdAt, updatedAt    time.Time
	)
	err := s.Scan(
		&row.TransferNo, &row.MerchantID, &row.OutTransferNo, &row.PayeeMerchantID, &amount, &row.Currency,
		&remark, &row.NotifyURL, &row.Mode, &row.PayerShard, &row.PayeeShard, &row.Status, &failCode, &txnNo,
		&row.RetryCount, &nextRetryAt, &finishedAt, &row.Version, &createdAt, &updatedAt,
	)
	if err != nil {
		return TransferOrder{}, err
	}
	row.Amount = money.Amount(amount)
	row.Remark = remark.String
	row.FailCode = failCode.String
	row.TxnNo = txnNo.String
	if finishedAt.Valid {
		row.FinishedAt = finishedAt.Time
	}
	row.NextRetryAt = nextRetryAt
	row.CreatedAt = createdAt
	row.UpdatedAt = updatedAt
	return row, nil
}
