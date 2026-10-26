package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

type closeBody struct {
	ReqNo string `json:"req_no"`
}

// closeResponse 只有 result 和 status。不能复用带 omitempty 的受理响应，否则空字段会漏掉或带出流水号。
type closeResponse struct {
	Result string `json:"result"`
	Status string `json:"status"`
}

func (h *Handler) handleClose(ctx context.Context, c *app.RequestContext) {
	start := h.clk.Now()
	status := h.serveClose(ctx, c)
	logx.Access(ctx, ClosePath, status, h.clk.Now().Sub(start), "", "")
}

// serveClose 处理一次关单并返回 HTTP 状态码。验签和参数失败不写库。
func (h *Handler) serveClose(ctx context.Context, c *app.RequestContext) int {
	body := c.Request.Body()
	if status := h.verify(ctx, c, body); status != http.StatusOK {
		return bare(c, status)
	}
	in, ok := decodeClose(body)
	if !ok {
		return bare(c, http.StatusBadRequest)
	}
	callCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	res, err := h.pay.Close(callCtx, biz.CloseRequest{ReqNo: in.ReqNo})
	if errors.Is(err, biz.ErrInvalidRequest) {
		return bare(c, http.StatusBadRequest)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mock close failed", slog.String("error", err.Error()))
		return bare(c, http.StatusInternalServerError)
	}
	return h.writeSigned(ctx, c, closeResponse{Result: res.Result, Status: res.Status})
}

func decodeClose(body []byte) (closeBody, bool) {
	var in closeBody
	ok := decodeObject(body, map[string]any{
		"req_no": &in.ReqNo,
	})
	return in, ok
}
