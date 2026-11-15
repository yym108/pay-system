package biz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

const billTimestampLayout = "2006-01-02T15:04:05.000Z07:00"

// ErrConflict 表示账单注入时指定的 (txn_type, req_no) 已存在且内容不一致。
var ErrConflict = errors.New("biz: bill inject conflict")

// BillInjectRequest 是 POST /mockpay/admin/bill-inject 的入参（CH-09）。
type BillInjectRequest struct {
	Date    string
	TxnType string
	Amount  money.Amount
	ReqNo   string
}

// BillInjectResult 是账单注入的响应（CH-09）。
type BillInjectResult struct {
	ReqNo        string
	ChannelTxnNo string
}

// InjectBill 注入一条 SUCCESS 的 mock_txn（CH-09），用于构造渠道侧长款。
// req_no 为空时自动生成平台不存在的单号；同参数重复注入幂等返回已有流水号，参数不一致返回 ErrConflict。
func (s *Service) InjectBill(ctx context.Context, req BillInjectRequest) (BillInjectResult, error) {
	if err := ctx.Err(); err != nil {
		return BillInjectResult{}, err
	}
	if !validBillDate(req.Date) {
		return BillInjectResult{}, fmt.Errorf("%w: date", ErrInvalidRequest)
	}
	if req.TxnType != dal.TxnTypePay && req.TxnType != dal.TxnTypeRefund {
		return BillInjectResult{}, fmt.Errorf("%w: txn_type", ErrInvalidRequest)
	}
	if req.Amount < money.MinPayAmount {
		return BillInjectResult{}, fmt.Errorf("%w: amount", ErrInvalidRequest)
	}
	if req.ReqNo != "" && !validText(req.ReqNo, maxReqNoRunes) {
		return BillInjectResult{}, fmt.Errorf("%w: req_no", ErrInvalidRequest)
	}

	channelTxnNo, err := s.ids.NewNo(ctx, channelTxnPrefix)
	if err != nil {
		return BillInjectResult{}, fmt.Errorf("new channel_txn_no: %w", err)
	}
	reqNo := req.ReqNo
	if reqNo == "" {
		idNo, err := s.ids.NewNo(ctx, channelTxnPrefix)
		if err != nil {
			return BillInjectResult{}, fmt.Errorf("new injected req_no: %w", err)
		}
		reqNo = "INJ" + idNo[len(channelTxnPrefix):]
	}

	finishedAt, err := time.ParseInLocation(billDateLayout, req.Date, shanghai)
	if err != nil {
		return BillInjectResult{}, fmt.Errorf("%w: date", ErrInvalidRequest)
	}

	t := dal.Txn{
		ChannelTxnNo: channelTxnNo,
		TxnType:      req.TxnType,
		ReqNo:        reqNo,
		Amount:       req.Amount,
		Status:       dal.StatusSuccess,
		Scenario:     string(scenario.Normal),
		FinishedAt:   finishedAt,
		BillDate:     req.Date,
		CreatedAt:    s.now(),
	}
	if _, err := s.store.Insert(ctx, t); err != nil {
		if !dbx.IsDuplicate(err) {
			return BillInjectResult{}, err
		}
		existing, ok, gerr := s.store.Get(ctx, req.TxnType, reqNo)
		if gerr != nil {
			return BillInjectResult{}, gerr
		}
		if !ok {
			return BillInjectResult{}, errDuplicateWithoutRow
		}
		if existing.Status == dal.StatusSuccess && existing.Amount == req.Amount && existing.BillDate == req.Date {
			return BillInjectResult{ReqNo: existing.ReqNo, ChannelTxnNo: existing.ChannelTxnNo}, nil
		}
		return BillInjectResult{}, ErrConflict
	}
	return BillInjectResult{ReqNo: reqNo, ChannelTxnNo: channelTxnNo}, nil
}

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
