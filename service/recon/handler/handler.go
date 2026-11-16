// Package handler 提供 ReconService 的 Kitex RPC 处理实现。
package handler

import (
	"context"

	"github.com/yym108/pay-system/kitex_gen/recon"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/service/recon/biz"
)

// Service 抽象 handler 依赖的对账业务接口。
type Service interface {
	RunBatch(ctx context.Context, in biz.RunBatchInput) (*biz.RunBatchOutput, error)
}

// Handler 实现 Kitex ReconService。
type Handler struct {
	svc Service
}

// New 创建 ReconService Handler。
func New(svc Service) *Handler {
	return &Handler{svc: svc}
}

// RunBatch 触发或查询指定渠道、账期的对账批次。
func (h *Handler) RunBatch(ctx context.Context, req *recon.RunBatchRequest) (*recon.RunBatchResponse, error) {
	if h == nil || h.svc == nil {
		return nil, errno.ToBizStatus(errno.ErrInternal)
	}
	if req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	force := false
	if req.Force != nil {
		force = *req.Force
	}
	out, err := h.svc.RunBatch(ctx, biz.RunBatchInput{
		Channel:  req.Channel,
		BillDate: req.BillDate,
		Force:    force,
	})
	if err != nil {
		return nil, toBizErr(err)
	}
	return &recon.RunBatchResponse{
		Batch: toBatch(out.Batch),
		Diffs: toDiffs(out.Diffs),
	}, nil
}
