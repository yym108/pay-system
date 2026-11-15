package biz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"strconv"
	"time"

	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

const billTimestampLayout = "2006-01-02T15:04:05.000Z07:00"

// BillResult 是账单下载的业务结果（CH-08）。
type BillResult struct {
	Ready   bool
	Content []byte
	Count   int
	SHA256  string
}

// DownloadBill 按东八区日期生成渠道对账单 CSV（CH-08）。
// date 非法返回 ErrInvalidRequest；今天（未开启 MOCK_ALLOW_PARTIAL_BILL）或未来返回 Ready=false。
func (s *Service) DownloadBill(ctx context.Context, date string) (BillResult, error) {
	if !validBillDate(date) {
		return BillResult{}, fmt.Errorf("invalid bill date: %w", ErrInvalidRequest)
	}
	today := s.clk.Now().In(shanghai).Format(billDateLayout)
	if date > today || (date == today && !s.opts.AllowPartialBill) {
		return BillResult{Ready: false}, nil
	}
	rows, err := s.store.ListBillTxns(ctx, date)
	if err != nil {
		return BillResult{}, err
	}
	content, count, err := encodeBillCSV(rows)
	if err != nil {
		return BillResult{}, err
	}
	sum := sha256.Sum256(content)
	return BillResult{
		Ready:   true,
		Content: content,
		Count:   count,
		SHA256:  fmt.Sprintf("%x", sum),
	}, nil
}

func validBillDate(date string) bool {
	if len(date) != len(billDateLayout) {
		return false
	}
	t, err := time.ParseInLocation(billDateLayout, date, shanghai)
	if err != nil {
		return false
	}
	return t.Format(billDateLayout) == date
}

func encodeBillCSV(rows []dal.Txn) ([]byte, int, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{"txn_type", "req_no", "channel_txn_no", "amount", "status", "finished_at"}); err != nil {
		return nil, 0, fmt.Errorf("write bill header: %w", err)
	}
	count := 0
	for _, r := range rows {
		if r.Scenario == string(scenario.BillMissing) {
			continue
		}
		amt := r.Amount
		if r.Scenario == string(scenario.BillAmountDiff) {
			amt++
		}
		finishedAt := r.FinishedAt.In(shanghai).Truncate(time.Millisecond).Format(billTimestampLayout)
		if err := w.Write([]string{
			r.TxnType,
			r.ReqNo,
			r.ChannelTxnNo,
			strconv.FormatInt(int64(amt), 10),
			r.Status,
			finishedAt,
		}); err != nil {
			return nil, 0, fmt.Errorf("write bill row: %w", err)
		}
		count++
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, 0, fmt.Errorf("flush bill csv: %w", err)
	}
	return buf.Bytes(), count, nil
}
