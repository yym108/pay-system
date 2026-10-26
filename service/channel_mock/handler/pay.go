// Package handler 是模拟渠道的 HTTP 入口（Hertz），见 docs/05-api.md §9。受理见 CH-01，查单见 CH-02。
//
// 请求与 200 响应都用渠道密钥签名（§3.5）。验签失败（含时间窗外）401，参数非法 400，
// 内部错误 500；非 200 的响应 body 为空、不签名。日志不含密钥、签名值和请求体。
package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

const (
	// PayPath 是支付受理路由。
	PayPath = "/mockpay/v1/pay"
	// QueryPath 是查单路由。
	QueryPath = "/mockpay/v1/query"
)

const (
	// timestampWindowSec 是 X-Mock-Timestamp 与本地时钟的最大偏差（秒，§3.5，含边界）。
	// 本地时间先取整秒（Unix()）再相减，与只有整秒的请求时间戳同一精度比较。
	timestampWindowSec = 300
	// requestTimeout 是单个请求进入业务后的上限。
	requestTimeout = 3 * time.Second
)

// Payer 是渠道 HTTP 的业务入口，由 biz.Service 实现。
// 受理和查单共用这一份依赖，New 不用再增加参数。
type Payer interface {
	Pay(ctx context.Context, req biz.PayRequest) (biz.PayResult, error)
	Query(ctx context.Context, req biz.QueryRequest) (biz.QueryResult, error)
}

// Handler 持有路由依赖。
type Handler struct {
	pay Payer
	key []byte
	clk clock.Clock
}

// New 创建 Handler。key 是 CHANNEL_MOCKPAY_KEY 的明文，为空时所有请求 500。
func New(pay Payer, key []byte, clk clock.Clock) *Handler {
	return &Handler{pay: pay, key: key, clk: clk}
}

// Register 在 srv 上挂 panic 恢复和业务路由。
func (h *Handler) Register(srv *server.Hertz) {
	srv.Use(recoverPanic)
	srv.POST(PayPath, h.handlePay)
	srv.POST(QueryPath, h.handleQuery)
}

func recoverPanic(ctx context.Context, c *app.RequestContext) {
	defer func() {
		if p := recover(); p != nil {
			slog.ErrorContext(ctx, "handler panicked", slog.String("path", string(c.Path())))
			c.Response.Reset()
			c.AbortWithStatus(http.StatusInternalServerError)
			logx.Access(ctx, string(c.Path()), http.StatusInternalServerError, 0, "", "")
		}
	}()
	c.Next(ctx)
}

type payBody struct {
	ReqNo     string `json:"req_no"`
	Amount    int64  `json:"amount"`
	Subject   string `json:"subject"`
	NotifyURL string `json:"notify_url"`
}

type payResponse struct {
	Result       string `json:"result"`
	Reason       string `json:"reason,omitempty"`
	ChannelTxnNo string `json:"channel_txn_no,omitempty"`
	Status       string `json:"status"`
}

func (h *Handler) handlePay(ctx context.Context, c *app.RequestContext) {
	start := h.clk.Now()
	status := h.servePay(ctx, c)
	logx.Access(ctx, PayPath, status, h.clk.Now().Sub(start), "", "")
}

// servePay 处理一次受理并返回 HTTP 状态码。
func (h *Handler) servePay(ctx context.Context, c *app.RequestContext) int {
	body := c.Request.Body()
	if status := h.verify(ctx, c, body); status != http.StatusOK {
		return bare(c, status)
	}
	in, ok := decodePay(body)
	if !ok {
		return bare(c, http.StatusBadRequest)
	}
	callCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	res, err := h.pay.Pay(callCtx, biz.PayRequest{
		ReqNo:     in.ReqNo,
		Amount:    money.Amount(in.Amount),
		Subject:   in.Subject,
		NotifyURL: in.NotifyURL,
	})
	if errors.Is(err, biz.ErrInvalidRequest) {
		return bare(c, http.StatusBadRequest)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mock pay failed", slog.String("error", err.Error()))
		return bare(c, http.StatusInternalServerError)
	}
	return h.writeSigned(ctx, c, payResponse{
		Result:       res.Result,
		Reason:       res.Reason,
		ChannelTxnNo: res.ChannelTxnNo,
		Status:       res.Status,
	})
}

// verify 校验渠道签名与时间窗。通过返回 200；对端问题 401；本端密钥为空 500。
func (h *Handler) verify(ctx context.Context, c *app.RequestContext, body []byte) int {
	ts := string(c.GetHeader(sign.HeaderMockTimestamp))
	sig := string(c.GetHeader(sign.HeaderMockSignature))
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return http.StatusUnauthorized
	}
	err = sign.VerifyChannel(h.key, sign.ChannelMessage{Timestamp: ts, Body: body}, sig)
	if errors.Is(err, sign.ErrEmptyKey) {
		slog.ErrorContext(ctx, "channel key is empty")
		return http.StatusInternalServerError
	}
	if err != nil {
		return http.StatusUnauthorized
	}
	// sec 来自签名验证通过的请求头，范围不受限：先判断再相减，避免极端值溢出。
	now := h.clk.Now().Unix()
	if sec < now-timestampWindowSec || sec > now+timestampWindowSec {
		return http.StatusUnauthorized
	}
	return http.StatusOK
}

// decodePay 严格解析支付受理 JSON（§9.2）。规则见 decodeObject。
func decodePay(body []byte) (payBody, bool) {
	var in payBody
	ok := decodeObject(body, map[string]any{
		"req_no":     &in.ReqNo,
		"amount":     &in.Amount,
		"subject":    &in.Subject,
		"notify_url": &in.NotifyURL,
	})
	return in, ok
}

// decodeObject 严格解析一个 JSON 对象。以下都算非法：
//   - body 不是合法 UTF-8（encoding/json 会把非法字节换成 U+FFFD，必须在解码前拒绝）；
//   - 顶层不是单个对象，或对象之后还有非空白数据；
//   - 同一个键出现两次（按转义还原后的键名比较，未知字段也算）；
//   - 键名与已知字段大小写不同但按 Unicode 折叠相等（如 Amount、REQ_NO），
//     encoding/json 默认会把它们当成同一字段；
//   - 已知字段类型不符（小数、字符串金额、溢出等）。
//
// 其余未知字段忽略（含嵌套），缺字段或 null 留零值交给业务校验。
func decodeObject(body []byte, fields map[string]any) bool {
	if !utf8.Valid(body) {
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return false
		}
		name, ok := tok.(string)
		if !ok || seen[name] {
			return false
		}
		seen[name] = true
		dst, known := fields[name]
		if !known {
			if foldsToField(name, fields) {
				return false
			}
			var skip json.RawMessage
			dst = &skip
		}
		if err = dec.Decode(dst); err != nil {
			return false
		}
	}
	tok, err = dec.Token()
	if err != nil || tok != json.Delim('}') {
		return false
	}
	// 对象之后只允许空白：再读一个 token 必须正好是 EOF。
	_, err = dec.Token()
	return errors.Is(err, io.EOF)
}

// foldsToField 判断 name 是否是某个已知字段的大小写变体（Unicode 简单折叠，与 encoding/json 一致）。
func foldsToField(name string, fields map[string]any) bool {
	for f := range fields {
		if strings.EqualFold(name, f) {
			return true
		}
	}
	return false
}

// writeSigned 写 200 和签名头。v 按 encoding/json 编码，调用方用 omitempty 控制省略字段。
// 签名失败时改为 500。受理响应仍用 payResponse：REJECTED 的空 channel_txn_no 被省略，status 始终在。
func (h *Handler) writeSigned(ctx context.Context, c *app.RequestContext, v any) int {
	out, err := json.Marshal(v)
	if err != nil {
		slog.ErrorContext(ctx, "marshal mock response failed")
		return bare(c, http.StatusInternalServerError)
	}
	ts := sign.FormatTimestamp(h.clk.Now())
	sig, err := sign.SignChannel(h.key, sign.ChannelMessage{Timestamp: ts, Body: out})
	if err != nil {
		slog.ErrorContext(ctx, "sign mock response failed")
		return bare(c, http.StatusInternalServerError)
	}
	c.Response.Header.Set(sign.HeaderMockTimestamp, ts)
	c.Response.Header.Set(sign.HeaderMockSignature, sig)
	c.Data(http.StatusOK, "application/json; charset=utf-8", out)
	return http.StatusOK
}

// bare 写一个空 body、不签名的响应。
func bare(c *app.RequestContext, status int) int {
	c.Status(status)
	return status
}
