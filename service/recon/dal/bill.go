package dal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// 账单明细匹配状态与交易类型常量，与 recon_channel_bill CHECK 约束一致。
const (
	BillMatchUnchecked = "UNCHECKED"
	BillMatchMatched   = "MATCHED"
	BillMatchDiff      = "DIFF"

	BillInsertBatchSize = 1000
)

// ChannelBill 对应 recon_channel_bill 表。
type ChannelBill struct {
	ID                uint64    `db:"id"`
	BatchNo           string    `db:"batch_no"`
	Channel           string    `db:"channel"`
	BillDate          string    `db:"bill_date"`
	TxnType           string    `db:"txn_type"`
	ChannelTxnNo      string    `db:"channel_txn_no"`
	BizNo             string    `db:"biz_no"`
	Amount            int64     `db:"amount"`
	ChannelStatus     string    `db:"channel_status"`
	ChannelFinishedAt time.Time `db:"channel_finished_at"`
	MatchStatus       string    `db:"match_status"`
	CreatedAt         time.Time `db:"created_at"`
}

// InsertChannelBillsBatch 按每批最多 1000 行批量插入 recon_channel_bill。
// 调用方应在事务内执行，并与更新批次状态为 DOWNLOADED 在同一事务提交。
func InsertChannelBillsBatch(ctx context.Context, exec sqlx.ExtContext, rows []ChannelBill) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += BillInsertBatchSize {
		end := start + BillInsertBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := insertChannelBillChunk(ctx, exec, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func insertChannelBillChunk(ctx context.Context, exec sqlx.ExtContext, chunk []ChannelBill) error {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO recon_channel_bill
		(batch_no, channel, bill_date, txn_type, channel_txn_no, biz_no, amount, channel_status, channel_finished_at, match_status)
		VALUES `)
	args := make([]any, 0, len(chunk)*10)
	for i, r := range chunk {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?)")
		matchStatus := r.MatchStatus
		if matchStatus == "" {
			matchStatus = BillMatchUnchecked
		}
		args = append(args,
			r.BatchNo,
			r.Channel,
			r.BillDate,
			r.TxnType,
			r.ChannelTxnNo,
			r.BizNo,
			r.Amount,
			r.ChannelStatus,
			r.ChannelFinishedAt,
			matchStatus,
		)
	}
	if _, err := exec.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("batch insert recon_channel_bill (%d rows): %w", len(chunk), err)
	}
	return nil
}
