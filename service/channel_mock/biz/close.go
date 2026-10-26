package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

// ResultAlreadyPaid 是关单时交易已经成功（或 CLOSE_RACE 被这次关单写成成功）的 result。
const ResultAlreadyPaid = "ALREADY_PAID"

// CloseRequest 是 POST /mockpay/v1/close 的请求。只认 req_no；金额、回调地址等不在这里。
type CloseRequest struct {
	ReqNo string
}

// CloseResult 是关单结果。HTTP body 只有这两个字段。
type CloseResult struct {
	Result string
	Status string
}

// closeAttempt 是一次结果不确定的状态写入。普通关单的 finishedAt 是写入前的值（这次不改它）。
type closeAttempt struct {
	status     string
	finishedAt time.Time
	hook       bool
}

// Close 关闭一笔支付（CH-03）。只认 (PAY, req_no)，同号的 REFUND 行不参与。
//
// 没有行就插入 CLOSED 防悬挂记录（amount 为 0）。已有行按状态回答：
// PROCESSING 且不是 CLOSE_RACE 时 CAS 置为 CLOSED，不写 finished_at、不调钩子；
// CLOSE_RACE 时 CAS 置为 SUCCESS 并在提交后调用一次完成钩子；
// SUCCESS / CLOSED / FAILED 不改行。
// 参数非法返回 ErrInvalidRequest。已取消的 ctx 原样返回。其余错误是内部错误。
func (s *Service) Close(ctx context.Context, req CloseRequest) (CloseResult, error) {
	if err := ctx.Err(); err != nil {
		return CloseResult{}, err
	}
	if err := validateClose(req); err != nil {
		return CloseResult{}, err
	}
	row, ok, err := s.store.Get(ctx, dal.TxnTypePay, req.ReqNo)
	if err != nil {
		return CloseResult{}, err
	}
	if !ok {
		return s.insertTombstone(ctx, req.ReqNo)
	}
	return s.closeFound(ctx, row)
}

func validateClose(r CloseRequest) error {
	if !validText(r.ReqNo, maxReqNoRunes) {
		return fmt.Errorf("%w: req_no", ErrInvalidRequest)
	}
	return nil
}

// insertTombstone 为未知 req_no 占住 (PAY, req_no)。不调度，不调用钩子。
// scenario、notify_url 写成列默认值。created_at 用注入时钟，不用列默认值。
func (s *Service) insertTombstone(ctx context.Context, reqNo string) (CloseResult, error) {
	no, err := s.ids.NewNo(ctx, channelTxnPrefix)
	if err != nil {
		return CloseResult{}, fmt.Errorf("new channel_txn_no: %w", err)
	}
	t := dal.Txn{
		ChannelTxnNo: no,
		TxnType:      dal.TxnTypePay,
		ReqNo:        reqNo,
		Amount:       0,
		Status:       dal.StatusClosed,
		Scenario:     scenario.Normal.String(),
		NotifyURL:    "",
		CreatedAt:    s.now(),
	}
	if _, err := s.store.Insert(ctx, t); err != nil {
		if dbx.IsDuplicate(err) {
			return s.closeAfterDuplicate(ctx, reqNo)
		}
		return s.recoverCloseInsert(ctx, t, err)
	}
	return closedResult(), nil
}

// closeAfterDuplicate 处理插入时的 1062：再读当前行，按它现在的状态走规则表。
func (s *Service) closeAfterDuplicate(ctx context.Context, reqNo string) (CloseResult, error) {
	row, ok, err := s.store.Get(ctx, dal.TxnTypePay, reqNo)
	if err != nil {
		return CloseResult{}, err
	}
	if !ok {
		return CloseResult{}, errDuplicateWithoutRow
	}
	return s.closeFound(ctx, row)
}

// recoverCloseInsert 处理插入的非 1062 错误。行可能已经提交。
// 流水号与本次相同就按墓碑已写入返回；读不到、读失败或是别的行，返回原错误，不猜测。
func (s *Service) recoverCloseInsert(ctx context.Context, t dal.Txn, insertErr error) (CloseResult, error) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rereadTimeout)
	defer cancel()
	row, ok, err := s.store.Get(readCtx, dal.TxnTypePay, t.ReqNo)
	if err != nil || !ok || row.ChannelTxnNo != t.ChannelTxnNo {
		return CloseResult{}, insertErr
	}
	return closedResult(), nil
}

func (s *Service) closeFound(ctx context.Context, row dal.Txn) (CloseResult, error) {
	switch row.Status {
	case dal.StatusSuccess:
		return alreadyPaidResult(), nil
	case dal.StatusClosed:
		return closedResult(), nil
	case dal.StatusFailed:
		return failedResult(), nil
	case dal.StatusProcessing:
		if scenario.Scenario(row.Scenario) == scenario.CloseRace {
			return s.closeAsPaid(ctx, row)
		}
		return s.closeAsClosed(ctx, row)
	default:
		return CloseResult{}, fmt.Errorf("close mock_txn: unexpected status")
	}
}

// closeAsClosed 普通关单。不用 Finish：那条 SQL 会写 finished_at。
func (s *Service) closeAsClosed(ctx context.Context, row dal.Txn) (CloseResult, error) {
	attempt := closeAttempt{status: dal.StatusClosed, finishedAt: row.FinishedAt}
	applied, err := s.store.Close(ctx, row.ID)
	if err != nil {
		return s.confirmClose(ctx, row.ID, attempt, err)
	}
	if !applied {
		return s.answerAfterMiss(ctx, row.ID)
	}
	return closedResult(), nil
}

// closeAsPaid 把 CLOSE_RACE 写成与自动完成相同的 SUCCESS，提交后再调钩子。
func (s *Service) closeAsPaid(ctx context.Context, row dal.Txn) (CloseResult, error) {
	f := s.finishFor(row)
	attempt := closeAttempt{status: f.Status, finishedAt: f.FinishedAt, hook: true}
	applied, err := s.store.Finish(ctx, f)
	if err != nil {
		return s.confirmClose(ctx, row.ID, attempt, err)
	}
	if !applied {
		return s.answerAfterMiss(ctx, row.ID)
	}
	done := row
	done.Status = f.Status
	done.FinishedAt = f.FinishedAt
	done.BillDate = f.BillDate
	s.notifyFinished(ctx, done)
	return alreadyPaidResult(), nil
}

// confirmClose 在写入报错、结果不确定时再读一次。
// 与这次尝试一致：普通关单视为已生效且不调钩子；CLOSE_RACE 才调一次钩子。
// 不一致则按读到的终态回答，不覆盖、不调钩子。读回失败返回原错误。
func (s *Service) confirmClose(ctx context.Context, id uint64, attempt closeAttempt, writeErr error) (CloseResult, error) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rereadTimeout)
	defer cancel()
	row, ok, err := s.store.GetByID(readCtx, id)
	if err != nil || !ok {
		return CloseResult{}, writeErr
	}
	if closeMatched(row, attempt) {
		if attempt.hook {
			s.notifyFinished(ctx, row)
		}
		return answerStatus(row.Status)
	}
	res, err := answerStatus(row.Status)
	if err != nil {
		return CloseResult{}, writeErr
	}
	return res, nil
}

// answerAfterMiss 处理 CAS 影响 0 行且这次没有不确定的写入。按新状态回答，不调钩子。
func (s *Service) answerAfterMiss(ctx context.Context, id uint64) (CloseResult, error) {
	row, ok, err := s.store.GetByID(ctx, id)
	if err != nil {
		return CloseResult{}, err
	}
	if !ok {
		return CloseResult{}, fmt.Errorf("close mock_txn: row missing")
	}
	return answerStatus(row.Status)
}

func closeMatched(row dal.Txn, attempt closeAttempt) bool {
	return row.Status == attempt.status && row.FinishedAt.Equal(attempt.finishedAt)
}

func answerStatus(status string) (CloseResult, error) {
	switch status {
	case dal.StatusSuccess:
		return alreadyPaidResult(), nil
	case dal.StatusClosed:
		return closedResult(), nil
	case dal.StatusFailed:
		return failedResult(), nil
	default:
		return CloseResult{}, fmt.Errorf("close mock_txn: unexpected status")
	}
}

// notifyFinished 在 CAS 确认提交之后调用。ctx 不随本次请求取消，钩子自行设超时。
func (s *Service) notifyFinished(ctx context.Context, row dal.Txn) {
	if s.opts.OnFinished == nil {
		return
	}
	s.opts.OnFinished(context.WithoutCancel(ctx), row)
}

func closedResult() CloseResult {
	return CloseResult{Result: ResultOK, Status: dal.StatusClosed}
}

func alreadyPaidResult() CloseResult {
	return CloseResult{Result: ResultAlreadyPaid, Status: dal.StatusSuccess}
}

func failedResult() CloseResult {
	return CloseResult{Result: ResultOK, Status: dal.StatusFailed}
}
