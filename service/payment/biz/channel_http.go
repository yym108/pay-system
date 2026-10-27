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

	"github.com/yym108/pay-system/pkg/clock"
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
		url: payURL,
		key: bytes.Clone(cfg.Key),
		clk: clk,
		http: &http.Client{
			Transport:     transport,
			CheckRedirect: refuseRedirect,
		},
		timeout:     timeout,
		dialTimeout: PayChannelDialTimeout,
	}, nil
}

func channelPayURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return "", errChannelConfig
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = strings.TrimRight(u.Path, "/") + channelPayPath
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
	body, err := json.Marshal(channelPayBody{
		ReqNo:     req.ReqNo,
		Amount:    int64(req.Amount),
		Subject:   req.Subject,
		NotifyURL: req.NotifyURL,
	})
	if err != nil {
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
		return ChannelAcceptResult{}, errChannelConfig
	}
	ts := sign.FormatTimestamp(c.clk.Now())
	sig, err := sign.SignChannel(c.key, sign.ChannelMessage{Timestamp: ts, Body: body})
	if err != nil {
		return ChannelAcceptResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(sign.HeaderMockTimestamp, ts)
	httpReq.Header.Set(sign.HeaderMockSignature, sig)
	resp, err := c.http.Do(httpReq) //nolint:gosec // 地址在 NewChannelClient 限定为配置的 http/https 主机，不取商户传入的 URL。
	if err != nil {
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
		return ChannelAcceptResult{Outcome: acceptOK, ChannelTxnNo: out.ChannelTxnNo}, nil
	case acceptResultRejected:
		return ChannelAcceptResult{Outcome: acceptRejected, Reason: out.Reason}, nil
	default:
		return ChannelAcceptResult{}, nil
	}
}

const (
	acceptResultOK       = "OK"
	acceptResultRejected = "REJECTED"
)
