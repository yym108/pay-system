package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/sign"
)

type notifyAPI interface {
	HandleChannelNotify(ctx context.Context, req *payment.HandleChannelNotifyRequest, callOptions ...callopt.Option) (*payment.HandleChannelNotifyResponse, error)
}

func WithNotifyAPI(api notifyAPI) Option {
	return func(h *Handler) {
		if h != nil {
			h.notify = api
		}
	}
}

func WithChannelKey(key []byte) Option {
	return func(h *Handler) {
		if h != nil {
			h.channelKey = append([]byte(nil), key...)
		}
	}
}

const (
	callbackChannel = "mockpay"
	callbackSkew    = 300
	callbackOK      = "SUCCESS"
	callbackFail    = "FAIL"
	callbackTime    = "2006-01-02T15:04:05.000Z07:00"
)

var callbackRejected sync.Map

func CallbackRejected(code string) uint64 {
	actual, ok := callbackRejected.Load(code)
	if !ok {
		return 0
	}
	counter, ok := actual.(*atomic.Uint64)
	if !ok {
		return 0
	}
	return counter.Load()
}

func addCallbackRejected(code int32) {
	key := strconv.FormatInt(int64(code), 10)
	actual, _ := callbackRejected.LoadOrStore(key, new(atomic.Uint64))
	counter, ok := actual.(*atomic.Uint64)
	if !ok {
		return
	}
	counter.Add(1)
	obs.AddCallbackRejected(callbackChannel, key)
}

func (h *Handler) registerCallback(r *server.Hertz) {
	if h == nil || r == nil {
		return
	}
	r.POST("/callback/mockpay", h.channelCallback)
}

func (h *Handler) channelCallback(ctx context.Context, c *app.RequestContext) {
	if h == nil {
		writeCallback(c, http.StatusInternalServerError, callbackFail)
		return
	}
	if len(h.channelKey) == 0 {
		h.log().ErrorContext(ctx, "channel callback key empty", "err_code", "10003")
		writeCallback(c, http.StatusInternalServerError, callbackFail)
		return
	}
	ts := string(c.GetHeader(sign.HeaderMockTimestamp))
	signature := string(c.GetHeader(sign.HeaderMockSignature))
	if ts == "" || signature == "" || headerHasCRLF(ts, signature) {
		writeCallback(c, http.StatusUnauthorized, callbackFail)
		return
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || skew(h.clk.Now().Unix(), sec) > callbackSkew {
		writeCallback(c, http.StatusUnauthorized, callbackFail)
		return
	}
	body, err := c.Body()
	if err != nil {
		h.log().ErrorContext(ctx, "channel callback body", "err_code", "10003")
		writeCallback(c, http.StatusInternalServerError, callbackFail)
		return
	}
	err = sign.VerifyChannel(h.channelKey, sign.ChannelMessage{Timestamp: ts, Body: body}, signature)
	if errors.Is(err, sign.ErrEmptyKey) {
		h.log().ErrorContext(ctx, "channel callback key empty", "err_code", "10003")
		writeCallback(c, http.StatusInternalServerError, callbackFail)
		return
	}
	if err != nil {
		writeCallback(c, http.StatusUnauthorized, callbackFail)
		return
	}
	req, ok := parseCallback(body)
	if !ok {
		h.log().ErrorContext(ctx, "channel callback body", "err_code", "10001")
		writeCallback(c, http.StatusInternalServerError, callbackFail)
		return
	}
	setPayNo(ctx, req.ReqNo)
	if h.notify == nil {
		h.log().ErrorContext(ctx, "channel callback client", "err_code", "10003")
		writeCallback(c, http.StatusInternalServerError, callbackFail)
		return
	}
	resp, err := h.notify.HandleChannelNotify(ctx, req, callopt.WithRPCTimeout(RPCTimeout("HandleChannelNotify")))
	status, ack := callbackResult(err, resp)
	if ack && err != nil {
		biz, bizOK := kerrors.FromBizStatusError(err)
		if bizOK && biz != nil {
			code := biz.BizStatusCode()
			addCallbackRejected(code)
			h.log().ErrorContext(ctx, "channel callback rejected",
				"err_code", strconv.FormatInt(int64(code), 10),
				"req_no", req.ReqNo,
			)
		}
	}
	if status == http.StatusOK {
		writeCallback(c, status, callbackOK)
		return
	}
	writeCallback(c, status, callbackFail)
}

func skew(now, ts int64) int64 {
	d := now - ts
	if d < 0 {
		return -d
	}
	return d
}

func parseCallback(body []byte) (*payment.HandleChannelNotifyRequest, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var obj struct {
		NotifyID     string          `json:"notify_id"`
		TxnType      string          `json:"txn_type"`
		ReqNo        string          `json:"req_no"`
		ChannelTxnNo string          `json:"channel_txn_no"`
		Amount       json.RawMessage `json:"amount"`
		Status       string          `json:"status"`
		FinishedAt   string          `json:"finished_at"`
	}
	if err := dec.Decode(&obj); err != nil {
		return nil, false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, false
	}
	if obj.NotifyID == "" || obj.ReqNo == "" || obj.ChannelTxnNo == "" || obj.FinishedAt == "" {
		return nil, false
	}
	if obj.TxnType != "PAY" && obj.TxnType != "REFUND" {
		return nil, false
	}
	if obj.Status != "SUCCESS" && obj.Status != "FAILED" {
		return nil, false
	}
	if _, err := time.Parse(callbackTime, obj.FinishedAt); err != nil {
		return nil, false
	}
	if len(obj.Amount) == 0 || !jsonIntPattern.Match(obj.Amount) {
		return nil, false
	}
	amount, err := strconv.ParseInt(string(obj.Amount), 10, 64)
	if err != nil {
		return nil, false
	}
	return &payment.HandleChannelNotifyRequest{
		Channel:      callbackChannel,
		NotifyId:     obj.NotifyID,
		TxnType:      obj.TxnType,
		ReqNo:        obj.ReqNo,
		ChannelTxnNo: obj.ChannelTxnNo,
		Amount:       amount,
		Status:       obj.Status,
		FinishedAt:   obj.FinishedAt,
	}, true
}

func callbackResult(err error, resp *payment.HandleChannelNotifyResponse) (int, bool) {
	if err == nil {
		if resp == nil || (resp.Result_ != "APPLIED" && resp.Result_ != "DUPLICATE" && resp.Result_ != "CONFLICT") {
			return http.StatusInternalServerError, false
		}
		return http.StatusOK, false
	}
	biz, ok := kerrors.FromBizStatusError(err)
	if !ok || biz == nil {
		return http.StatusInternalServerError, false
	}
	code := biz.BizStatusCode()
	if code == errno.ErrOrderNotFound.Code || code == errno.ErrChannelAmountMismatch.Code {
		return http.StatusOK, true
	}
	return http.StatusInternalServerError, false
}

func writeCallback(c *app.RequestContext, status int, body string) {
	if c == nil {
		return
	}
	raw := []byte(body)
	c.SetStatusCode(status)
	c.Response.Header.Set("Content-Type", "text/plain; charset=utf-8")
	c.Response.Header.SetContentLength(len(raw))
	c.Response.SetBodyRaw(raw)
}
