package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
)

// paymentAPI 只包含 GW-02 要调的四个方法。生产实现是 Kitex 客户端。
type paymentAPI interface {
	CreatePayment(ctx context.Context, req *payment.CreatePaymentRequest, callOptions ...callopt.Option) (*payment.PaymentOrder, error)
	Pay(ctx context.Context, req *payment.PayRequest, callOptions ...callopt.Option) (*payment.PayResponse, error)
	ClosePayment(ctx context.Context, req *payment.ClosePaymentRequest, callOptions ...callopt.Option) (*payment.ClosePaymentResponse, error)
	QueryPayment(ctx context.Context, req *payment.QueryPaymentRequest, callOptions ...callopt.Option) (*payment.PaymentOrder, error)
}

// accountAPI 只包含余额查询。生产实现是 Kitex 客户端。
type accountAPI interface {
	GetAccount(ctx context.Context, req *account.GetAccountRequest, callOptions ...callopt.Option) (*account.Account, error)
}

// WithPaymentAPI 安装支付 RPC。只应装在商户端口。
func WithPaymentAPI(api paymentAPI) Option {
	return func(h *Handler) {
		if h != nil {
			h.payment = api
		}
	}
}

// WithAccountAPI 安装账户 RPC。只应装在商户端口。
func WithAccountAPI(api accountAPI) Option {
	return func(h *Handler) {
		if h != nil {
			h.account = api
		}
	}
}

const (
	ownerMerchant     = "MERCHANT"
	accountSettle     = "MERCHANT_SETTLE"
	currencyCNY       = "CNY"
	maxSubjectRunes   = 128
	maxNotifyURLBytes = 512
	minExpireMinutes  = 1
	maxExpireMinutes  = 120
	minPaymentAmount  = 1
	maxPaymentAmount  = 10_000_000
)

var (
	jsonIntPattern    = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
	outTradeNoPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// balanceView 是商户余额响应。不要把 Account 的其他字段写出去。
type balanceView struct {
	AccountNo       string `json:"account_no"`
	Currency        string `json:"currency"`
	Balance         int64  `json:"balance"`
	FrozenAmount    int64  `json:"frozen_amount"`
	AvailableAmount int64  `json:"available_amount"`
}

func (h *Handler) registerV1(r *server.Hertz) {
	if h == nil || r == nil {
		return
	}
	r.POST("/v1/payments", h.createPayment)
	r.POST("/v1/payments/:pay_no/pay", h.payPayment)
	r.POST("/v1/payments/:pay_no/close", h.closePayment)
	r.GET("/v1/payments", h.queryByOutTradeNo)
	r.GET("/v1/payments/:pay_no", h.queryByPayNo)
	r.GET("/v1/balance", h.getBalance)
}

func (h *Handler) createPayment(ctx context.Context, c *app.RequestContext) {
	mid, ok := MerchantID(c)
	if !ok {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	body, err := c.Body()
	if err != nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	req, bad := parseCreate(body)
	if bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	req.MerchantId = mid
	if h.payment == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	order, err := h.payment.CreatePayment(ctx, req, callopt.WithRPCTimeout(RPCTimeout("CreatePayment")))
	if err != nil {
		h.writeRoute(c, mapRouteErr(err), nil)
		return
	}
	if order == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	WriteOK(c, order)
}

func (h *Handler) payPayment(ctx context.Context, c *app.RequestContext) {
	mid, ok := MerchantID(c)
	if !ok {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	payNo := c.Param("pay_no")
	if payNo == "" {
		h.writeRoute(c, errno.ErrInvalidParam, nil)
		return
	}
	body, err := c.Body()
	if err != nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	channel, bad := parseChannel(body)
	if bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	if h.payment == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	resp, err := h.payment.Pay(ctx, &payment.PayRequest{
		MerchantId: mid,
		PayNo:      payNo,
		Channel:    channel,
	}, callopt.WithRPCTimeout(RPCTimeout("Pay")))
	if err != nil {
		h.writeRoute(c, mapRouteErr(err), nil)
		return
	}
	if resp == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	h.writeBizOrder(c, resp.BizCode, resp.PaymentOrder)
}

func (h *Handler) closePayment(ctx context.Context, c *app.RequestContext) {
	mid, ok := MerchantID(c)
	if !ok {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	payNo := c.Param("pay_no")
	if payNo == "" {
		h.writeRoute(c, errno.ErrInvalidParam, nil)
		return
	}
	if bad := rejectBody(c); bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	if h.payment == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	resp, err := h.payment.ClosePayment(ctx, &payment.ClosePaymentRequest{
		MerchantId: mid,
		PayNo:      payNo,
	}, callopt.WithRPCTimeout(RPCTimeout("ClosePayment")))
	if err != nil {
		h.writeRoute(c, mapRouteErr(err), nil)
		return
	}
	if resp == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	h.writeBizOrder(c, resp.BizCode, resp.PaymentOrder)
}

func (h *Handler) queryByPayNo(ctx context.Context, c *app.RequestContext) {
	payNo := c.Param("pay_no")
	out := queryArg(c, "out_trade_no")
	if payNo == "" || out != "" {
		h.writeRoute(c, errno.ErrInvalidParam, nil)
		return
	}
	h.queryPayment(ctx, c, &payNo, nil)
}

func (h *Handler) queryByOutTradeNo(ctx context.Context, c *app.RequestContext) {
	payNo := queryArg(c, "pay_no")
	out := queryArg(c, "out_trade_no")
	if out == "" || payNo != "" {
		h.writeRoute(c, errno.ErrInvalidParam, nil)
		return
	}
	h.queryPayment(ctx, c, nil, &out)
}

func (h *Handler) queryPayment(ctx context.Context, c *app.RequestContext, payNo, outTradeNo *string) {
	mid, ok := MerchantID(c)
	if !ok {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	if bad := rejectBody(c); bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	if h.payment == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	order, err := h.payment.QueryPayment(ctx, &payment.QueryPaymentRequest{
		MerchantId: mid,
		PayNo:      payNo,
		OutTradeNo: outTradeNo,
	}, callopt.WithRPCTimeout(RPCTimeout("QueryPayment")))
	if err != nil {
		h.writeRoute(c, mapRouteErr(err), nil)
		return
	}
	if order == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	WriteOK(c, order)
}

func (h *Handler) getBalance(ctx context.Context, c *app.RequestContext) {
	mid, ok := MerchantID(c)
	if !ok {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	if bad := rejectBody(c); bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	if h.account == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	got, err := h.account.GetAccount(ctx, &account.GetAccountRequest{
		OwnerType:   ownerMerchant,
		OwnerId:     mid,
		AccountType: accountSettle,
	}, callopt.WithRPCTimeout(RPCTimeout("GetAccount")))
	if err != nil {
		h.writeRoute(c, mapRouteErr(err), nil)
		return
	}
	if got == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	WriteOK(c, balanceView{
		AccountNo:       got.AccountNo,
		Currency:        got.Currency,
		Balance:         got.Balance,
		FrozenAmount:    got.FrozenAmount,
		AvailableAmount: got.AvailableAmount,
	})
}

func (h *Handler) writeRoute(c *app.RequestContext, e *errno.Error, data any) {
	if c == nil {
		return
	}
	if e == nil {
		e = errno.ErrInternal
	}
	if data == nil {
		writeErrno(c, e.HTTPStatus, e, requestID(c))
		return
	}
	raw, err := marshalData(data)
	if err != nil {
		writeErrno(c, errno.ErrInternal.HTTPStatus, errno.ErrInternal, requestID(c))
		return
	}
	writeErrno(c, e.HTTPStatus, e, requestID(c), raw)
}

func (h *Handler) writeBizOrder(c *app.RequestContext, code int32, order *payment.PaymentOrder) {
	if code == errno.OK.Code {
		if order == nil {
			h.writeRoute(c, errno.ErrInternal, nil)
			return
		}
		WriteOK(c, order)
		return
	}
	e, found := errno.ByCode(code)
	if !found || e == nil || order == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	h.writeRoute(c, e, order)
}

func mapRouteErr(err error) *errno.Error {
	biz, ok := kerrors.FromBizStatusError(err)
	if !ok || biz == nil {
		return errno.ErrUnavailable
	}
	e, found := errno.ByCode(biz.BizStatusCode())
	if !found || e == nil || e.Code == errno.OK.Code {
		return errno.ErrInternal
	}
	return e
}

func rejectBody(c *app.RequestContext) *errno.Error {
	body, err := c.Body()
	if err != nil {
		return errno.ErrInternal
	}
	if len(body) > 0 {
		return errno.ErrInvalidParam
	}
	return nil
}

func parseCreate(body []byte) (*payment.CreatePaymentRequest, *errno.Error) {
	obj, bad := decodeObject(body)
	if bad != nil {
		return nil, bad
	}
	out, ok := jsonString(obj["out_trade_no"])
	if !ok || !outTradeNoPattern.MatchString(out) {
		return nil, errno.ErrInvalidParam
	}
	amount, ok := jsonIntIn(obj["amount"], minPaymentAmount, maxPaymentAmount)
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	subject, ok := jsonString(obj["subject"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	runes := utf8.RuneCountInString(subject)
	if runes < 1 || runes > maxSubjectRunes {
		return nil, errno.ErrInvalidParam
	}
	currency, ok := optionalCNY(obj["currency"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	notifyURL, ok := optionalNotify(obj["notify_url"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	expire, ok := optionalExpire(obj["expire_minutes"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	return &payment.CreatePaymentRequest{
		OutTradeNo:    out,
		Amount:        amount,
		Currency:      currency,
		Subject:       subject,
		NotifyUrl:     notifyURL,
		ExpireMinutes: expire,
	}, nil
}

func parseChannel(body []byte) (string, *errno.Error) {
	obj, bad := decodeObject(body)
	if bad != nil {
		return "", bad
	}
	channel, ok := jsonString(obj["channel"])
	if !ok || channel == "" {
		return "", errno.ErrInvalidParam
	}
	return channel, nil
}

func decodeObject(body []byte) (map[string]json.RawMessage, *errno.Error) {
	if len(body) == 0 {
		return nil, errno.ErrInvalidParam
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var obj map[string]json.RawMessage
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, errno.ErrInvalidParam
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, errno.ErrInvalidParam
	}
	return obj, nil
}

func optionalCNY(raw json.RawMessage) (*string, bool) {
	if raw == nil {
		return nil, true
	}
	s, ok := jsonString(raw)
	if !ok || s != currencyCNY {
		return nil, false
	}
	return &s, true
}

func optionalNotify(raw json.RawMessage) (*string, bool) {
	if raw == nil {
		return nil, true
	}
	s, ok := jsonString(raw)
	if !ok || len(s) > maxNotifyURLBytes || !httpURL(s) {
		return nil, false
	}
	return &s, true
}

func optionalExpire(raw json.RawMessage) (*int32, bool) {
	if raw == nil {
		return nil, true
	}
	n, ok := jsonIntIn(raw, minExpireMinutes, maxExpireMinutes)
	if !ok {
		return nil, false
	}
	v := int32(n) //nolint:gosec // G115: n 已限制在 1–120，放得进 int32。
	return &v, true
}

func jsonIntIn(raw json.RawMessage, min, max int64) (int64, bool) {
	if len(raw) == 0 || !jsonIntPattern.Match(raw) {
		return 0, false
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || n < min || n > max {
		return 0, false
	}
	return n, true
}

func jsonString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

func httpURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

func queryArg(c *app.RequestContext, key string) string {
	if c == nil {
		return ""
	}
	u, err := url.ParseRequestURI(string(c.Request.URI().RequestURI()))
	if err != nil {
		return ""
	}
	return u.Query().Get(key)
}
