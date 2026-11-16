package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// InternalPaymentOrder 是 pay_payment.payment_order 的只读视图（由 recon 经只读 DSN 查询）。
type InternalPaymentOrder struct {
	PayNo      string `db:"pay_no"`
	MerchantID string `db:"merchant_id"`
	Amount     int64  `db:"amount"`
	Status     string `db:"status"`
}

// InternalRefundOrder 是 pay_payment.refund_order 的只读视图。
type InternalRefundOrder struct {
	RefundNo   string `db:"refund_no"`
	MerchantID string `db:"merchant_id"`
	Amount     int64  `db:"amount"`
	Status     string `db:"status"`
}

// InternalTransferOrder 是 pay_payment.transfer_order 的只读视图。
type InternalTransferOrder struct {
	TransferNo string `db:"transfer_no"`
	Amount     int64  `db:"amount"`
	Mode       string `db:"mode"`
	PayerShard uint8  `db:"payer_shard"`
	PayeeShard uint8  `db:"payee_shard"`
	Status     string `db:"status"`
}

// AccountLedgerTxn 是 pay_account_{0,1}.ledger_txn 的只读视图。
type AccountLedgerTxn struct {
	TxnNo   string `db:"txn_no"`
	BizType string `db:"biz_type"`
	BizNo   string `db:"biz_no"`
	Amount  int64  `db:"amount"`
}

// AccountFreezeRecord 是 pay_account_{0,1}.account_freeze 的只读视图。
type AccountFreezeRecord struct {
	BizType string `db:"biz_type"`
	BizNo   string `db:"biz_no"`
	Amount  int64  `db:"amount"`
	Status  string `db:"status"`
}

// AccountTransferBranch 是 pay_account_{0,1}.transfer_branch 的只读视图。
type AccountTransferBranch struct {
	TransferNo string `db:"transfer_no"`
	Role       string `db:"role"`
	Amount     int64  `db:"amount"`
	Status     string `db:"status"`
}

// ListSuccessPaymentOrdersByDate 只读查询 pay_payment.payment_order 中指定渠道在 [dayStart, dayEnd) 的 SUCCESS 支付单。
func ListSuccessPaymentOrdersByDate(ctx context.Context, paymentExec sqlx.ExtContext, channel string, dayStart, dayEnd time.Time) ([]InternalPaymentOrder, error) {
	var list []InternalPaymentOrder
	err := sqlx.SelectContext(ctx, paymentExec, &list,
		`SELECT pay_no, merchant_id, amount, status FROM payment_order
		WHERE channel = ? AND status = 'SUCCESS' AND paid_at >= ? AND paid_at < ?`,
		channel, dayStart, dayEnd)
	if err != nil {
		return nil, fmt.Errorf("list success payment_order by date: %w", err)
	}
	return list, nil
}

// ListSuccessRefundOrdersByDate 只读查询 pay_payment.refund_order 中指定渠道在 [dayStart, dayEnd) 的 SUCCESS 退款单。
func ListSuccessRefundOrdersByDate(ctx context.Context, paymentExec sqlx.ExtContext, channel string, dayStart, dayEnd time.Time) ([]InternalRefundOrder, error) {
	var list []InternalRefundOrder
	err := sqlx.SelectContext(ctx, paymentExec, &list,
		`SELECT refund_no, merchant_id, amount, status FROM refund_order
		WHERE channel = ? AND status = 'SUCCESS' AND succeeded_at >= ? AND succeeded_at < ?`,
		channel, dayStart, dayEnd)
	if err != nil {
		return nil, fmt.Errorf("list success refund_order by date: %w", err)
	}
	return list, nil
}

// ListFinishedTransferOrdersByDate 只读查询 pay_payment.transfer_order 中在 [dayStart, dayEnd) 进入终态（SUCCESS/FAILED）的转账单。
func ListFinishedTransferOrdersByDate(ctx context.Context, paymentExec sqlx.ExtContext, dayStart, dayEnd time.Time) ([]InternalTransferOrder, error) {
	var list []InternalTransferOrder
	err := sqlx.SelectContext(ctx, paymentExec, &list,
		`SELECT transfer_no, amount, mode, payer_shard, payee_shard, status FROM transfer_order
		WHERE status IN ('SUCCESS', 'FAILED') AND finished_at >= ? AND finished_at < ?`,
		dayStart, dayEnd)
	if err != nil {
		return nil, fmt.Errorf("list finished transfer_order by date: %w", err)
	}
	return list, nil
}

// ListLedgerTxnsByBizNos 只读查询单个账户分片中指定 biz_type 与 biz_no 集合的记账凭证。
func ListLedgerTxnsByBizNos(ctx context.Context, accExec sqlx.ExtContext, bizTypes, bizNos []string) ([]AccountLedgerTxn, error) {
	if len(bizTypes) == 0 || len(bizNos) == 0 {
		return nil, nil
	}
	var out []AccountLedgerTxn
	for start := 0; start < len(bizNos); start += queryChunkSize {
		end := start + queryChunkSize
		if end > len(bizNos) {
			end = len(bizNos)
		}
		chunk := bizNos[start:end]
		query, args, err := sqlx.In(
			`SELECT txn_no, biz_type, biz_no, amount FROM ledger_txn WHERE biz_type IN (?) AND biz_no IN (?)`,
			bizTypes, chunk)
		if err != nil {
			return nil, fmt.Errorf("build ledger_txn IN: %w", err)
		}
		var part []AccountLedgerTxn
		if err := sqlx.SelectContext(ctx, accExec, &part, accExec.Rebind(query), args...); err != nil {
			return nil, fmt.Errorf("list ledger_txn by biz_nos: %w", err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// ListAccountFreezesByBizNos 只读查询单个账户分片中指定 biz_type 与 biz_no 集合的冻结单。
func ListAccountFreezesByBizNos(ctx context.Context, accExec sqlx.ExtContext, bizType string, bizNos []string) ([]AccountFreezeRecord, error) {
	if len(bizNos) == 0 {
		return nil, nil
	}
	var out []AccountFreezeRecord
	for start := 0; start < len(bizNos); start += queryChunkSize {
		end := start + queryChunkSize
		if end > len(bizNos) {
			end = len(bizNos)
		}
		chunk := bizNos[start:end]
		query, args, err := sqlx.In(
			`SELECT biz_type, biz_no, amount, status FROM account_freeze WHERE biz_type = ? AND biz_no IN (?)`,
			bizType, chunk)
		if err != nil {
			return nil, fmt.Errorf("build account_freeze IN: %w", err)
		}
		var part []AccountFreezeRecord
		if err := sqlx.SelectContext(ctx, accExec, &part, accExec.Rebind(query), args...); err != nil {
			return nil, fmt.Errorf("list account_freeze by biz_nos: %w", err)
		}
		out = append(out, part...)
	}
	return out, nil
}

// ListTransferBranchesByTransferNos 只读查询单个账户分片中指定 transfer_no 集合的转账分支。
func ListTransferBranchesByTransferNos(ctx context.Context, accExec sqlx.ExtContext, transferNos []string) ([]AccountTransferBranch, error) {
	if len(transferNos) == 0 {
		return nil, nil
	}
	var out []AccountTransferBranch
	for start := 0; start < len(transferNos); start += queryChunkSize {
		end := start + queryChunkSize
		if end > len(transferNos) {
			end = len(transferNos)
		}
		chunk := transferNos[start:end]
		query, args, err := sqlx.In(
			`SELECT transfer_no, role, amount, status FROM transfer_branch WHERE transfer_no IN (?)`,
			chunk)
		if err != nil {
			return nil, fmt.Errorf("build transfer_branch IN: %w", err)
		}
		var part []AccountTransferBranch
		if err := sqlx.SelectContext(ctx, accExec, &part, accExec.Rebind(query), args...); err != nil {
			return nil, fmt.Errorf("list transfer_branch by transfer_nos: %w", err)
		}
		out = append(out, part...)
	}
	return out, nil
}
