package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

const billNotReadyResult = "BILL_NOT_READY"

type billNotReadyResponse struct {
	Result string `json:"result"`
}

func (h *Handler) handleBill(ctx context.Context, c *app.RequestContext) {
	start := h.clk.Now()
	status := h.serveBill(ctx, c)
	logx.Access(ctx, BillPath, status, h.clk.Now().Sub(start), "", "")
}

// serveBill 处理一次账单下载（CH-08）。
func (h *Handler) serveBill(ctx context.Context, c *app.RequestContext) int {
	body := c.Request.Body()
	if status := h.verify(ctx, c, body); status != http.StatusOK {
		return bare(c, status)
	}
	var date string
	var dateCount int
	c.QueryArgs().VisitAll(func(key, value []byte) {
		if string(key) == "date" {
			dateCount++
			date = string(value)
		}
	})
	if dateCount != 1 {
		return bare(c, http.StatusBadRequest)
	}

	callCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	res, err := h.pay.DownloadBill(callCtx, date)
	if errors.Is(err, biz.ErrInvalidRequest) {
		return bare(c, http.StatusBadRequest)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mock bill failed", slog.String("error", err.Error()))
		return bare(c, http.StatusInternalServerError)
	}
	if !res.Ready {
		return h.writeSigned(ctx, c, billNotReadyResponse{Result: billNotReadyResult})
	}

	ts := sign.FormatTimestamp(h.clk.Now())
	sig, err := sign.SignChannel(h.key, sign.ChannelMessage{Timestamp: ts, Body: res.Content})
	if err != nil {
		slog.ErrorContext(ctx, "sign mock bill failed")
		return bare(c, http.StatusInternalServerError)
	}
	c.Response.Header.Set(sign.HeaderMockTimestamp, ts)
	c.Response.Header.Set(sign.HeaderMockSignature, sig)
	c.Response.Header.Set("X-Bill-Sha256", res.SHA256)
	c.Response.Header.Set("X-Bill-Count", strconv.Itoa(res.Count))
	c.Data(http.StatusOK, "text/csv", res.Content)
	return http.StatusOK
}
