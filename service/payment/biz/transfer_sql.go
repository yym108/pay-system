package biz

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/outbox"
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

const casTransferSuccessSQL = `
UPDATE transfer_order
SET status = 'SUCCESS', txn_no = ?, finished_at = ?, version = version + 1, updated_at = ?
WHERE transfer_no = ? AND status = 'TRYING'`

const casTransferFailedSQL = `
UPDATE transfer_order
SET status = 'FAILED', fail_code = ?, finished_at = ?, version = version + 1, updated_at = ?
WHERE transfer_no = ? AND status = 'TRYING'`

const casTransferConfirmingSQL = `
UPDATE transfer_order
SET status = 'CONFIRMING', version = version + 1, updated_at = ?
WHERE transfer_no = ? AND status = 'TRYING'`

const casTransferCancellingSQL = `
UPDATE transfer_order
SET status = 'CANCELLING', fail_code = ?, version = version + 1, updated_at = ?
WHERE transfer_no = ? AND status = 'TRYING'`

const casTCCSuccessSQL = `
UPDATE transfer_order
SET status = 'SUCCESS', finished_at = ?, version = version + 1, updated_at = ?
WHERE transfer_no = ? AND status = 'CONFIRMING'`

const casTCCFailedSQL = `
UPDATE transfer_order
SET status = 'FAILED', finished_at = ?, version = version + 1, updated_at = ?
WHERE transfer_no = ? AND status = 'CANCELLING'`

const transferByNoSQL = `
SELECT transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
       mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
       version, created_at, updated_at
FROM transfer_order
WHERE transfer_no = ?`

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

// finishTransfer 在一个事务里 CAS，并在命中时写一条转账 outbox。账户调用必须在这之前完成。
func (s sqlTransferStore) finishTransfer(ctx context.Context, row TransferOrder, status, failCode, txnNo string, at time.Time, ids *idgen.Generator, clk clock.Clock) (TransferOrder, bool, error) {
	if s.db == nil || ids == nil || clk == nil {
		return TransferOrder{}, false, errno.ErrInternal
	}
	switch status {
	case transferStatusSuccess:
		if txnNo == "" {
			return TransferOrder{}, false, errno.ErrInternal
		}
		failCode = ""
	case transferStatusFailed:
		if failCode == "" {
			return TransferOrder{}, false, errno.ErrInternal
		}
		txnNo = ""
	default:
		return TransferOrder{}, false, errno.ErrInternal
	}
	var current TransferOrder
	var applied bool
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		var (
			res     sql.Result
			execErr error
		)
		if status == transferStatusSuccess {
			res, execErr = tx.ExecContext(ctx, casTransferSuccessSQL, txnNo, at, at, row.TransferNo)
		} else {
			res, execErr = tx.ExecContext(ctx, casTransferFailedSQL, failCode, at, at, row.TransferNo)
		}
		if execErr != nil {
			return execErr
		}
		n, nerr := res.RowsAffected()
		if nerr != nil {
			return nerr
		}
		if n == 0 {
			got, scanErr := scanTransfer(tx.QueryRowContext(ctx, transferByNoSQL, row.TransferNo))
			if scanErr != nil {
				return scanErr
			}
			if got.TransferNo != row.TransferNo {
				return errno.ErrInternal
			}
			current = got
			return nil
		}
		if n != 1 {
			return errno.Wrap(errno.ErrInternal, fmt.Errorf("transfer cas affected %d", n))
		}
		evt := outbox.Event{
			TransferNo: row.TransferNo, OutTransferNo: row.OutTransferNo,
			MerchantID: row.MerchantID, PayeeMerchantID: row.PayeeMerchantID,
			Amount: row.Amount, Mode: row.Mode, Status: status, FinishedAt: at,
		}
		if status == transferStatusSuccess {
			evt.Type = outbox.EventTransferSucceeded
		} else {
			evt.Type = outbox.EventTransferFailed
			evt.FailCode = failCode
		}
		if addErr := outbox.Add(ctx, tx, evt, ids, clk); addErr != nil {
			return addErr
		}
		current = row
		current.Status = status
		current.FailCode = failCode
		current.TxnNo = txnNo
		current.FinishedAt = at
		current.UpdatedAt = at
		current.Version++
		applied = true
		return nil
	})
	if err != nil {
		return TransferOrder{}, false, err
	}
	return current, applied, nil
}

// decideTransfer 在一个事务里写下 CONFIRMING 或 CANCELLING。账户调用必须在这之前完成。
func (s sqlTransferStore) decideTransfer(ctx context.Context, row TransferOrder, status, failCode string, at time.Time) (TransferOrder, bool, error) {
	if s.db == nil {
		return TransferOrder{}, false, errno.ErrInternal
	}
	switch status {
	case transferStatusConfirming:
		if failCode != "" {
			return TransferOrder{}, false, errno.ErrInternal
		}
	case transferStatusCancelling:
		if failCode == "" {
			return TransferOrder{}, false, errno.ErrInternal
		}
	default:
		return TransferOrder{}, false, errno.ErrInternal
	}
	var current TransferOrder
	var applied bool
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		var (
			res     sql.Result
			execErr error
		)
		if status == transferStatusConfirming {
			res, execErr = tx.ExecContext(ctx, casTransferConfirmingSQL, at, row.TransferNo)
		} else {
			res, execErr = tx.ExecContext(ctx, casTransferCancellingSQL, failCode, at, row.TransferNo)
		}
		if execErr != nil {
			return execErr
		}
		n, nerr := res.RowsAffected()
		if nerr != nil {
			return nerr
		}
		if n == 0 {
			got, scanErr := scanTransfer(tx.QueryRowContext(ctx, transferByNoSQL, row.TransferNo))
			if scanErr != nil {
				return scanErr
			}
			if got.TransferNo != row.TransferNo {
				return errno.ErrInternal
			}
			current = got
			return nil
		}
		if n != 1 {
			return errno.Wrap(errno.ErrInternal, fmt.Errorf("transfer decide affected %d", n))
		}
		current = row
		current.Status = status
		current.FailCode = failCode
		current.UpdatedAt = at
		current.Version++
		applied = true
		return nil
	})
	if err != nil {
		return TransferOrder{}, false, err
	}
	return current, applied, nil
}

// finishTCCTransfer 在一个事务里把 TCC 中间态收成终态，并在命中时写一条 outbox。
// SUCCESS 不写 txn_no。FAILED 保留 CANCELLING 时已经写入的 fail_code。
func (s sqlTransferStore) finishTCCTransfer(ctx context.Context, row TransferOrder, fromStatus, status, failCode string, at time.Time, ids *idgen.Generator, clk clock.Clock) (TransferOrder, bool, error) {
	if s.db == nil || ids == nil || clk == nil {
		return TransferOrder{}, false, errno.ErrInternal
	}
	switch {
	case fromStatus == transferStatusConfirming && status == transferStatusSuccess && failCode == "":
	case fromStatus == transferStatusCancelling && status == transferStatusFailed && failCode != "":
	default:
		return TransferOrder{}, false, errno.ErrInternal
	}
	var current TransferOrder
	var applied bool
	err := dbx.WithTx(ctx, s.db, func(tx *sqlx.Tx) error {
		var (
			res     sql.Result
			execErr error
		)
		if status == transferStatusSuccess {
			res, execErr = tx.ExecContext(ctx, casTCCSuccessSQL, at, at, row.TransferNo)
		} else {
			res, execErr = tx.ExecContext(ctx, casTCCFailedSQL, at, at, row.TransferNo)
		}
		if execErr != nil {
			return execErr
		}
		n, nerr := res.RowsAffected()
		if nerr != nil {
			return nerr
		}
		if n == 0 {
			got, scanErr := scanTransfer(tx.QueryRowContext(ctx, transferByNoSQL, row.TransferNo))
			if scanErr != nil {
				return scanErr
			}
			if got.TransferNo != row.TransferNo {
				return errno.ErrInternal
			}
			current = got
			return nil
		}
		if n != 1 {
			return errno.Wrap(errno.ErrInternal, fmt.Errorf("transfer tcc finish affected %d", n))
		}
		evt := outbox.Event{
			TransferNo: row.TransferNo, OutTransferNo: row.OutTransferNo,
			MerchantID: row.MerchantID, PayeeMerchantID: row.PayeeMerchantID,
			Amount: row.Amount, Mode: row.Mode, Status: status, FinishedAt: at,
		}
		if status == transferStatusSuccess {
			evt.Type = outbox.EventTransferSucceeded
		} else {
			evt.Type = outbox.EventTransferFailed
			evt.FailCode = failCode
		}
		if addErr := outbox.Add(ctx, tx, evt, ids, clk); addErr != nil {
			return addErr
		}
		current = row
		current.Status = status
		current.FailCode = failCode
		current.TxnNo = ""
		current.FinishedAt = at
		current.UpdatedAt = at
		current.Version++
		applied = true
		return nil
	})
	if err != nil {
		return TransferOrder{}, false, err
	}
	return current, applied, nil
}

// transferByNo 走 uk_transfer_no。不比较商户号，商户号由调用方比较（TRF-05）。
func (s sqlTransferStore) transferByNo(ctx context.Context, transferNo string) (TransferOrder, bool, error) {
	row, err := scanTransfer(s.db.QueryRowContext(ctx, transferByNoSQL, transferNo))
	if errors.Is(err, sql.ErrNoRows) {
		return TransferOrder{}, false, nil
	}
	if err != nil {
		return TransferOrder{}, false, err
	}
	// 列排序规则可能命中字节不同的单号。只认完全相同的 transfer_no。
	if row.TransferNo != transferNo {
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
