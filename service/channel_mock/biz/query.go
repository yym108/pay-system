package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

// ResultNotFound 是查单没有对应行时的 result。它不是平台错误码 10002。
const ResultNotFound = "NOT_FOUND"

// QueryRequest 是 POST /mockpay/v1/query 的请求。
type QueryRequest struct {
	TxnType string
	ReqNo   string
}

// QueryResult 是查单结果。Found 为 false 时其余字段无意义，调用方只返回 NOT_FOUND。
// FinishedAt 为零值表示 finished_at 列是 NULL，响应里省略该字段。
type QueryResult struct {
	Found        bool
	Status       string
	Amount       money.Amount
	ChannelTxnNo string
	FinishedAt   time.Time
}

// Query 按 (txn_type, req_no) 读取当前交易（CH-02）。
//
// 不存在时 Found 为 false，不插入行。存在时返回该行的 status、amount、channel_txn_no；
// FinishedAt 为零值表示列是 NULL。不更新行，不调度，不调用完成钩子，也不看 scenario。
// 参数非法返回 ErrInvalidRequest。已取消或已超时的 ctx 原样返回。其余错误是内部错误。
func (s *Service) Query(ctx context.Context, req QueryRequest) (QueryResult, error) {
	if err := ctx.Err(); err != nil {
		return QueryResult{}, err
	}
	if err := validateQuery(req); err != nil {
		return QueryResult{}, err
	}
	row, ok, err := s.store.Get(ctx, req.TxnType, req.ReqNo)
	if err != nil {
		return QueryResult{}, err
	}
	if !ok {
		return QueryResult{}, nil
	}
	return QueryResult{
		Found:        true,
		Status:       row.Status,
		Amount:       row.Amount,
		ChannelTxnNo: row.ChannelTxnNo,
		FinishedAt:   row.FinishedAt,
	}, nil
}

// validateQuery 检查查单参数。错误只说明字段名，不带取值。
func validateQuery(r QueryRequest) error {
	switch r.TxnType {
	case dal.TxnTypePay, dal.TxnTypeRefund:
	default:
		return fmt.Errorf("%w: txn_type", ErrInvalidRequest)
	}
	if !validText(r.ReqNo, maxReqNoRunes) {
		return fmt.Errorf("%w: req_no", ErrInvalidRequest)
	}
	return nil
}
