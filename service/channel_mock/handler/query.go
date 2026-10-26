package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

// rfc3339Milli 是约定 §4 的时间格式：毫秒三位，东八区固定偏移。
const rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"

// shanghai 是查单响应时间的固定东八区，不依赖 tzdata。
var shanghai = time.FixedZone("Asia/Shanghai", 8*60*60)

type queryBody struct {
	TxnType string `json:"txn_type"`
	ReqNo   string `json:"req_no"`
}

// queryFoundResponse 是找到行时的 body。amount 不用 omitempty，0 也要出现。
type queryFoundResponse struct {
	Result       string `json:"result"`
	Status       string `json:"status"`
	Amount       int64  `json:"amount"`
	ChannelTxnNo string `json:"channel_txn_no"`
	FinishedAt   string `json:"finished_at,omitempty"`
}

// queryMissingResponse 只有 result。不能复用 queryFoundResponse，否则 0 金额和空状态会漏出去。
type queryMissingResponse struct {
	Result string `json:"result"`
}

func (h *Handler) handleQuery(ctx context.Context, c *app.RequestContext) {
	start := h.clk.Now()
	status := h.serveQuery(ctx, c)
	logx.Access(ctx, QueryPath, status, h.clk.Now().Sub(start), "", "")
}

// serveQuery 处理一次查单并返回 HTTP 状态码。只读，不写 mock_txn。
func (h *Handler) serveQuery(ctx context.Context, c *app.RequestContext) int {
	body := c.Request.Body()
	if status := h.verify(ctx, c, body); status != http.StatusOK {
		return bare(c, status)
	}
	in, ok := decodeQuery(body)
	if !ok {
		return bare(c, http.StatusBadRequest)
	}
	callCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	res, err := h.pay.Query(callCtx, biz.QueryRequest{TxnType: in.TxnType, ReqNo: in.ReqNo})
	if errors.Is(err, biz.ErrInvalidRequest) {
		return bare(c, http.StatusBadRequest)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mock query failed", slog.String("error", err.Error()))
		return bare(c, http.StatusInternalServerError)
	}
	if !res.Found {
		return h.writeSigned(ctx, c, queryMissingResponse{Result: biz.ResultNotFound})
	}
	return h.writeSigned(ctx, c, queryFoundResponse{
		Result:       biz.ResultOK,
		Status:       res.Status,
		Amount:       int64(res.Amount),
		ChannelTxnNo: res.ChannelTxnNo,
		FinishedAt:   formatFinishedAt(res.FinishedAt),
	})
}

func decodeQuery(body []byte) (queryBody, bool) {
	var in queryBody
	ok := decodeObject(body, map[string]any{
		"txn_type": &in.TxnType,
		"req_no":   &in.ReqNo,
	})
	return in, ok
}

// formatFinishedAt 把完成时间格式化成约定 §4。零值表示列是 NULL，调用方省略该字段。
// 先换到东八区再截到毫秒，保证偏移是 +08:00，并且不把不足 1 毫秒的尾数进位。
func formatFinishedAt(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.In(shanghai).Truncate(time.Millisecond).Format(rfc3339Milli)
}
