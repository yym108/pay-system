package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/sign"
)

// errChannelConfig 是地址或客户端配置不合法。文本固定，不含地址和密钥。
var errChannelConfig = errors.New("channel client config")

// errChannelMarshal 是受理请求无法编码。文本固定，不含请求内容。
var errChannelMarshal = errors.New("channel request body")

// ChannelClientConfig 是生产渠道客户端的配置。Key 是 CHANNEL_MOCKPAY_KEY。
type ChannelClientConfig struct {
	BaseURL string
	Key     []byte
	Clock   clock.Clock
	Timeout time.Duration
}

// ChannelClient 按 CH-01 的受理契约调用 mockpay。不重试，不跟随重定向。
type ChannelClient struct {
	url         string
	closeURL    string
	queryURL    string
	refundURL   string
	key         []byte
	clk         clock.Clock
	http        *http.Client
	timeout     time.Duration
	dialTimeout time.Duration
}

type channelPayBody struct {
	ReqNo     string `json:"req_no"`
	Amount    int64  `json:"amount"`
	Subject   string `json:"subject"`
	NotifyURL string `json:"notify_url"`
}

type channelRefundBody struct {
	ReqNo     string `json:"req_no"`
	OrigReqNo string `json:"orig_req_no"`
	Amount    int64  `json:"amount"`
	NotifyURL string `json:"notify_url"`
}

type channelPayResponse struct {
	Result       string `json:"result"`
	Reason       string `json:"reason"`
	ChannelTxnNo string `json:"channel_txn_no"`
	Status       string `json:"status"`
}

// NewChannelClient 校验渠道根地址并复制密钥。失败时错误不含地址和密钥。
func NewChannelClient(cfg ChannelClientConfig) (*ChannelClient, error) {
	payURL, err := channelPayURL(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	closeURL, err := channelEndpoint(cfg.BaseURL, channelClosePath)
	if err != nil {
		return nil, err
	}
	queryURL, err := channelEndpoint(cfg.BaseURL, channelQueryPath)
	if err != nil {
		return nil, err
	}
	refundURL, err := channelEndpoint(cfg.BaseURL, channelRefundPath)
	if err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = PayChannelTimeout
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	dialer := &net.Dialer{Timeout: PayChannelDialTimeout}
	transport := &http.Transport{
		Proxy:       http.ProxyFromEnvironment,
		DialContext: dialer.DialContext,
	}
	return &ChannelClient{
		url:       payURL,
		closeURL:  closeURL,
		queryURL:  queryURL,
		refundURL: refundURL,
		key:       bytes.Clone(cfg.Key),
		clk:       clk,
		http: &http.Client{
			Transport:     transport,
			CheckRedirect: refuseRedirect,
		},
		timeout:     timeout,
		dialTimeout: PayChannelDialTimeout,
	}, nil
}

func channelPayURL(raw string) (string, error) {
	return channelEndpoint(raw, channelPayPath)
}

func channelEndpoint(raw, path string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return "", errChannelConfig
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = strings.TrimRight(u.Path, "/") + path
	return u.String(), nil
}

func refuseRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// Accept 发起一次支付受理。网络错误、非 200 和验签失败都作为未知结果，error 为 nil。
// 空密钥和无法编码的请求在发出之前返回错误，由 Pay 映射为 10003。
func (c *ChannelClient) Accept(ctx context.Context, req ChannelAcceptRequest) (ChannelAcceptResult, error) {
	if c == nil || c.http == nil {
		return ChannelAcceptResult{}, errChannelConfig
	}
	ctx, end := obs.StartClient(ctx, "channel", "pay")
	obs.SetOrder(trace.SpanFromContext(ctx), req.ReqNo, "", "", "", "")
	var callErr error
	defer func() { end(callErr) }()
	body, err := json.Marshal(channelPayBody{
		ReqNo:     req.ReqNo,
		Amount:    int64(req.Amount),
		Subject:   req.Subject,
		NotifyURL: req.NotifyURL,
	})
	if err != nil {
		callErr = err
		return ChannelAcceptResult{}, errChannelMarshal
	}
	limit := c.timeout
	if limit <= 0 {
		limit = PayChannelTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		callErr = err
		return ChannelAcceptResult{}, errChannelConfig
	}
	ts := sign.FormatTimestamp(c.clk.Now())
	sig, err := sign.SignChannel(c.key, sign.ChannelMessage{Timestamp: ts, Body: body})
	if err != nil {
		callErr = err
		return ChannelAcceptResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(sign.HeaderMockTimestamp, ts)
	httpReq.Header.Set(sign.HeaderMockSignature, sig)
	setTraceparent(ctx, httpReq)
	start := c.clk.Now()
	resp, err := c.http.Do(httpReq) //nolint:gosec // 地址在 NewChannelClient 限定为配置的 http/https 主机，不取商户传入的 URL。
	result := "ERROR"
	defer func() { obs.ObserveChannel("pay", result, c.clk.Now().Sub(start)) }()
	if err != nil {
		callErr = err
		return ChannelAcceptResult{}, nil
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxChannelBody+1))
	if err != nil || len(raw) > maxChannelBody || resp.StatusCode != http.StatusOK {
		return ChannelAcceptResult{}, nil
	}
	respTS := resp.Header.Get(sign.HeaderMockTimestamp)
	respSig := resp.Header.Get(sign.HeaderMockSignature)
	if respTS == "" || respSig == "" {
		return ChannelAcceptResult{}, nil
	}
	if err = sign.VerifyChannel(c.key, sign.ChannelMessage{Timestamp: respTS, Body: raw}, respSig); err != nil {
		return ChannelAcceptResult{}, nil
	}
	var out channelPayResponse
	if err = json.Unmarshal(raw, &out); err != nil {
		return ChannelAcceptResult{}, nil
	}
	switch out.Result {
	case acceptResultOK:
		if out.ChannelTxnNo == "" {
			return ChannelAcceptResult{}, nil
		}
		result = "OK"
		return ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: out.ChannelTxnNo}, nil
	case acceptResultRejected:
		result = "REJECTED"
		return ChannelAcceptResult{Outcome: acceptRejected, Reason: out.Reason}, nil
	default:
		return ChannelAcceptResult{}, nil
	}
}

// Refund 发起一次渠道退款。网络错误、非 200 和验签失败都作为未知结果，error 为 nil。
// 空密钥和无法编码的请求在发出之前返回错误，由退款提交映射为 10003。
func (c *ChannelClient) Refund(ctx context.Context, req ChannelRefundRequest) (ChannelAcceptResult, error) {
	limit := PayChannelTimeout
	if c != nil && c.timeout > 0 {
		limit = c.timeout
	}
	body, failed, err := c.exchange(ctx, "refund", c.refundURL, "", req.ReqNo, limit, channelRefundBody{
		ReqNo:     req.ReqNo,
		OrigReqNo: req.OrigReqNo,
		Amount:    int64(req.Amount),
		NotifyURL: req.NotifyURL,
	})
	if err != nil || failed {
		return ChannelAcceptResult{}, err
	}
	var out channelPayResponse
	if err = json.Unmarshal(body, &out); err != nil {
		return ChannelAcceptResult{}, nil
	}
	switch out.Result {
	case acceptResultOK:
		if out.ChannelTxnNo == "" {
			return ChannelAcceptResult{}, nil
		}
		return ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: out.ChannelTxnNo}, nil
	case acceptResultRejected:
		return ChannelAcceptResult{Outcome: acceptRejected, Reason: out.Reason}, nil
	default:
		return ChannelAcceptResult{}, nil
	}
}

var _ ChannelRefunder = (*ChannelClient)(nil)

const (
	acceptResultOK       = "OK"
	acceptResultRejected = "REJECTED"
)

type closeOutcome int

const (
	closeCallFailed closeOutcome = iota
	closeClosed
	closeAlreadyPaid
	closeTxnFailed
)

// channelCloseResult 是关单 HTTP 的分类。零值表示调用失败，订单保持原状。
type channelCloseResult struct {
	Outcome closeOutcome
}

type queryKind int

const (
	// queryCallFailed 是零值：非 200、验签失败、JSON 不对，或 result/status 认不出有没有行。
	queryCallFailed queryKind = iota
	queryNotFound
	queryFound
)

// channelQueryResult 把「没有行」「已有行」「调用失败」分开。
// OK 只在已有行且 status=SUCCESS、带金额和 finished_at 时为 true。
// PROCESSING 没有完成时间，Kind 仍是 queryFound，不能当成调用失败。
// Status 只在 queryFound 时有值。HasAmount 表示响应带了 amount；FinishedAt 为零表示没有或解析不了。
type channelQueryResult struct {
	Kind         queryKind
	OK           bool
	Status       string
	Amount       money.Amount
	HasAmount    bool
	FinishedAt   time.Time
	ChannelTxnNo string
}

func (q channelQueryResult) hasRow() bool {
	return q.OK || q.Kind == queryFound
}

// channelCloser 在事务外调用渠道关单和查单。实现不得把密钥写入日志或错误。
type channelCloser interface {
	ClosePay(ctx context.Context, reqNo string) (channelCloseResult, error)
	QueryPay(ctx context.Context, reqNo string) (channelQueryResult, error)
}

var _ channelCloser = (*ChannelClient)(nil)

type channelCloseBody struct {
	ReqNo string `json:"req_no"`
}

type channelCloseResponse struct {
	Result string `json:"result"`
	Status string `json:"status"`
}

type channelQueryBody struct {
	TxnType string `json:"txn_type"`
	ReqNo   string `json:"req_no"`
}

type channelQueryResponse struct {
	Result       string `json:"result"`
	Status       string `json:"status"`
	Amount       *int64 `json:"amount"`
	ChannelTxnNo string `json:"channel_txn_no"`
	FinishedAt   string `json:"finished_at"`
}

// ClosePay 调用 POST /mockpay/v1/close。body 只有 req_no。
// 网络错误、非 200、验签失败、JSON 不对，以及 result/status 不是三种已知组合，都是调用失败，error 为 nil。
// 空密钥和无法编码的请求在发出之前返回错误。
func (c *ChannelClient) ClosePay(ctx context.Context, reqNo string) (channelCloseResult, error) {
	body, failed, err := c.exchange(ctx, "close", c.closeURL, reqNo, "", CloseChannelTimeout, channelCloseBody{ReqNo: reqNo})
	if err != nil || failed {
		return channelCloseResult{}, err
	}
	var out channelCloseResponse
	if err = json.Unmarshal(body, &out); err != nil {
		return channelCloseResult{}, nil
	}
	switch {
	case out.Result == acceptResultOK && out.Status == payStatusClosed:
		return channelCloseResult{Outcome: closeClosed}, nil
	case out.Result == resultAlreadyPaid && out.Status == payStatusSuccess:
		return channelCloseResult{Outcome: closeAlreadyPaid}, nil
	case out.Result == acceptResultOK && out.Status == payStatusFailed:
		return channelCloseResult{Outcome: closeTxnFailed}, nil
	default:
		return channelCloseResult{}, nil
	}
}

// QueryPay 调用 POST /mockpay/v1/query，txn_type 固定为 PAY。
// result=NOT_FOUND 是 queryNotFound。result=OK 且 status 为 PROCESSING、SUCCESS、FAILED 或 CLOSED 是 queryFound。
// 只有 SUCCESS 同时带金额和可解析的 finished_at 时 OK 为 true。
// 调用失败的 Kind 是零值 queryCallFailed，error 为 nil。发出之前的配置错误仍返回 error。
func (c *ChannelClient) QueryPay(ctx context.Context, reqNo string) (channelQueryResult, error) {
	return c.query(ctx, CloseChannelTimeout, txnTypePay, reqNo)
}

// QueryTxn 是查单补偿（PAY-05）用的查单，txn_type 为 PAY 或 REFUND，总超时 QueryChannelTimeout。
// 分类与 QueryPay 相同。
func (c *ChannelClient) QueryTxn(ctx context.Context, txnType, reqNo string) (channelQueryResult, error) {
	return c.query(ctx, QueryChannelTimeout, txnType, reqNo)
}

var _ channelQuerier = (*ChannelClient)(nil)

func (c *ChannelClient) query(ctx context.Context, limit time.Duration, txnType, reqNo string) (channelQueryResult, error) {
	if c == nil {
		return channelQueryResult{Kind: queryCallFailed}, errChannelConfig
	}
	payNo, refundNo := "", ""
	if txnType == txnTypePay {
		payNo = reqNo
	} else {
		refundNo = reqNo
	}
	body, failed, err := c.exchange(ctx, "query", c.queryURL, payNo, refundNo, limit, channelQueryBody{TxnType: txnType, ReqNo: reqNo})
	if err != nil || failed {
		return channelQueryResult{Kind: queryCallFailed}, err
	}
	var out channelQueryResponse
	if err = json.Unmarshal(body, &out); err != nil {
		return channelQueryResult{Kind: queryCallFailed}, nil
	}
	if out.Result == queryResultNotFound {
		return channelQueryResult{Kind: queryNotFound}, nil
	}
	if out.Result != acceptResultOK || !knownQueryStatus(out.Status) {
		return channelQueryResult{Kind: queryCallFailed}, nil
	}
	res := channelQueryResult{Kind: queryFound, Status: out.Status, ChannelTxnNo: out.ChannelTxnNo}
	if out.Amount != nil {
		res.HasAmount = true
		res.Amount = money.Amount(*out.Amount)
	}
	if out.FinishedAt != "" {
		if finished, perr := time.Parse(channelTimeLayout, out.FinishedAt); perr == nil && !finished.IsZero() {
			res.FinishedAt = finished
		}
	}
	res.OK = out.Status == payStatusSuccess && res.HasAmount && !res.FinishedAt.IsZero()
	return res, nil
}

func knownQueryStatus(status string) bool {
	switch status {
	case txnStatusProcessing, payStatusSuccess, payStatusFailed, payStatusClosed:
		return true
	default:
		return false
	}
}

// exchange 发送一次签名 JSON。limit 是本次总超时上限；父 ctx 更短时先到期。
// callFailed 为 true 表示已经尝试调用但结果不可用。err 只表示还没发出去的配置问题。
func (c *ChannelClient) exchange(ctx context.Context, api, endpoint, payNo, refundNo string, limit time.Duration, payload any) ([]byte, bool, error) {
	if c == nil || c.http == nil || endpoint == "" {
		return nil, false, errChannelConfig
	}
	ctx, end := obs.StartClient(ctx, "channel", api)
	obs.SetOrder(trace.SpanFromContext(ctx), payNo, refundNo, "", "", "")
	var callErr error
	defer func() { end(callErr) }()
	raw, err := json.Marshal(payload)
	if err != nil {
		callErr = err
		return nil, false, errChannelMarshal
	}
	if limit <= 0 {
		limit = CloseChannelTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		callErr = err
		return nil, false, errChannelConfig
	}
	ts := sign.FormatTimestamp(c.clk.Now())
	sig, err := sign.SignChannel(c.key, sign.ChannelMessage{Timestamp: ts, Body: raw})
	if err != nil {
		callErr = err
		return nil, false, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(sign.HeaderMockTimestamp, ts)
	httpReq.Header.Set(sign.HeaderMockSignature, sig)
	setTraceparent(ctx, httpReq)
	start := c.clk.Now()
	resp, err := c.http.Do(httpReq) //nolint:gosec // 地址在 NewChannelClient 限定为配置的 http/https 主机。
	result := "error"
	defer func() { obs.ObserveChannel(api, result, c.clk.Now().Sub(start)) }()
	if err != nil {
		callErr = err
		return nil, true, nil
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxChannelBody+1))
	if err != nil || len(body) > maxChannelBody || resp.StatusCode != http.StatusOK {
		return nil, true, nil
	}
	respTS := resp.Header.Get(sign.HeaderMockTimestamp)
	respSig := resp.Header.Get(sign.HeaderMockSignature)
	if respTS == "" || respSig == "" {
		return nil, true, nil
	}
	if err = sign.VerifyChannel(c.key, sign.ChannelMessage{Timestamp: respTS, Body: body}, respSig); err != nil {
		return nil, true, nil
	}
	result = "ok"
	return body, false, nil
}

func setTraceparent(ctx context.Context, req *http.Request) {
	if tp := obs.InjectMap(ctx)["traceparent"]; tp != "" {
		req.Header.Set("traceparent", tp)
	}
}
