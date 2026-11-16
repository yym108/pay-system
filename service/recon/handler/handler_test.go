package handler_test

import (
	"context"
	"testing"
	"time"

	"github.com/yym108/pay-system/kitex_gen/recon"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
	"github.com/yym108/pay-system/service/recon/handler"
)

type stubService struct {
	fn func(ctx context.Context, in biz.RunBatchInput) (*biz.RunBatchOutput, error)
}

func (s *stubService) RunBatch(ctx context.Context, in biz.RunBatchInput) (*biz.RunBatchOutput, error) {
	return s.fn(ctx, in)
}

func TestHandlerRunBatchValidationAndConversion(t *testing.T) {
	h := handler.New(&stubService{
		fn: func(_ context.Context, in biz.RunBatchInput) (*biz.RunBatchOutput, error) {
			if in.Channel != "mockpay" {
				return nil, errno.ErrInvalidParam
			}
			now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*3600))
			return &biz.RunBatchOutput{
				Batch: dal.Batch{
					BatchNo:      "RB001",
					Channel:      "mockpay",
					BillDate:     "2026-10-26",
					Status:       dal.StatusDone,
					ChannelCount: 5,
					DiffCount:    1,
					StartedAt:    &now,
					FinishedAt:   &now,
				},
				Diffs: []dal.Diff{
					{
						DiffNo:    "RD001",
						BatchNo:   "RB001",
						Channel:   "mockpay",
						BillDate:  "2026-10-26",
						DiffType:  "LOCAL_MISSING",
						TxnType:   "PAY",
						BizNo:     "P001",
						Status:    "PENDING",
						CreatedAt: now,
					},
				},
				ManualTotal: 2,
				ManualDiffs: []dal.Diff{
					{
						DiffNo:    "RD000",
						BatchNo:   "RB000",
						Channel:   "mockpay",
						BillDate:  "2026-10-18",
						DiffType:  "CHANNEL_MISSING",
						TxnType:   "PAY",
						BizNo:     "P000",
						Status:    "PENDING",
						CreatedAt: now,
					},
				},
			}, nil
		},
	})

	// nil 请求返回 10001
	_, err := h.RunBatch(context.Background(), nil)
	if code := errno.FromBizStatus(err); code == nil || code.Code != errno.ErrInvalidParam.Code {
		t.Fatalf("nil req: err=%v, want 10001", err)
	}

	// 正常请求转换
	resp, err := h.RunBatch(context.Background(), &recon.RunBatchRequest{
		Channel:  "mockpay",
		BillDate: "2026-10-26",
	})
	if err != nil {
		t.Fatalf("RunBatch err: %v", err)
	}
	if resp.Batch.BatchNo != "RB001" || resp.Batch.Status != "DONE" || len(resp.Diffs) != 1 || resp.Diffs[0].DiffNo != "RD001" {
		t.Fatalf("unexpected resp: %+v", resp)
	}
	if resp.ManualTotal == nil || *resp.ManualTotal != 2 {
		t.Fatalf("ManualTotal = %v, want 2", resp.ManualTotal)
	}
	if len(resp.ManualDiffs) != 1 || resp.ManualDiffs[0].DiffNo != "RD000" {
		t.Fatalf("ManualDiffs = %+v, want [RD000]", resp.ManualDiffs)
	}
}

func TestServerTimeoutMatchesGateway(t *testing.T) {
	if handler.ServerTimeout != 120*time.Second {
		t.Fatalf("ServerTimeout = %s, want 120s", handler.ServerTimeout)
	}
}
