package biz

import (
	"context"
	"errors"
	"fmt"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

// RefundRequest 是 POST /mockpay/v1/refund。
type RefundRequest struct {
	ReqNo     string
	OrigReqNo string
	Amount    money.Amount
	NotifyURL string
}

// Refund 受理一笔退款（CH-04）。
// 已有 (REFUND, req_no) 先 replay，不再做额度校验。
// 新单在同一事务里锁原支付：不是 SUCCESS 则 ORIG_NOT_SUCCESS；非 FAILED 退款合计加本次大于原金额则 REFUND_EXCEEDED。都不插入。
// 新插入成功后在事务外 schedule。退款场景不是 ACCEPT_TIMEOUT，不 hold。
func (s *Service) Refund(ctx context.Context, req RefundRequest) (PayResult, error) {
	if err := ctx.Err(); err != nil {
		return PayResult{}, err
	}
	if err := validateRefund(req); err != nil {
		return PayResult{}, err
	}
	sc := scenario.ForRefund(int64(req.Amount))
	if sc == scenario.Normal && s.fault.shouldAcceptError() {
		return PayResult{}, errors.New("biz: injected refund accept error")
	}
	if existing, ok, err := s.store.Get(ctx, dal.TxnTypeRefund, req.ReqNo); err != nil {
		return PayResult{}, err
	} else if ok {
		return replay(existing, req.Amount), nil
	}
	no, err := s.ids.NewNo(ctx, channelTxnPrefix)
	if err != nil {
		return PayResult{}, fmt.Errorf("new channel_txn_no: %w", err)
	}
	t := dal.Txn{
		ChannelTxnNo: no,
		TxnType:      dal.TxnTypeRefund,
		ReqNo:        req.ReqNo,
		OrigReqNo:    req.OrigReqNo,
		Amount:       req.Amount,
		Status:       dal.StatusProcessing,
		Scenario:     sc.String(),
		NotifyURL:    req.NotifyURL,
		CreatedAt:    s.now(),
	}
	id, reject, err := s.store.InsertRefund(ctx, t, func(pay dal.Txn, found bool, activeSum money.Amount) string {
		if !found || pay.Status != dal.StatusSuccess {
			return ReasonOrigNotSuccess
		}
		if activeSum+req.Amount > pay.Amount {
			return ReasonRefundExceeded
		}
		return ""
	})
	if err != nil {
		if dbx.IsDuplicate(err) {
			return s.recoverInsert(ctx, t, err)
		}
		return s.recoverInsert(ctx, t, err)
	}
	if reject != "" {
		return PayResult{Result: ResultRejected, Reason: reject}, nil
	}
	t.ID = id
	s.schedule(ctx, t)
	return PayResult{Result: ResultOK, ChannelTxnNo: no, Status: dal.StatusProcessing}, nil
}

func validateRefund(r RefundRequest) error {
	switch {
	case !validText(r.ReqNo, maxReqNoRunes):
		return fmt.Errorf("%w: req_no", ErrInvalidRequest)
	case !validText(r.OrigReqNo, maxReqNoRunes):
		return fmt.Errorf("%w: orig_req_no", ErrInvalidRequest)
	case r.Amount < money.MinPayAmount:
		return fmt.Errorf("%w: amount", ErrInvalidRequest)
	case !validNotifyURL(r.NotifyURL):
		return fmt.Errorf("%w: notify_url", ErrInvalidRequest)
	}
	return nil
}
