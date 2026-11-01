package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/sign"
)

func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		// 不跟随重定向，避免把已签名请求或管理 Token 带到 Location。
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (p prepared) execute(cfg config.PayCLI, stdout, stderr io.Writer, clk clock.Clock, nextNonce func() (string, error), newClient func(time.Duration) *http.Client) int {
	if newClient == nil {
		newClient = newHTTPClient
	}
	client := newClient(p.timeout)
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	endpoint, err := requestURL(cfg.GatewayURL, p.path)
	if err != nil {
		writeLine(stderr, "paycli: gateway address is invalid")
		return 1
	}
	var rdr io.Reader
	if p.body != nil {
		rdr = bytes.NewReader(p.body)
	}
	req, err := http.NewRequestWithContext(ctx, p.method, endpoint, rdr)
	if err != nil {
		writeLine(stderr, "paycli: request failed")
		return 1
	}
	if p.body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	if p.admin {
		req.Header.Set(headerAdminToken, cfg.AdminToken)
		if p.operator != "" {
			req.Header.Set(headerAdminOperator, p.operator)
		}
	} else if err = signMerchant(req, cfg, clk, nextNonce, p.body); err != nil {
		writeLine(stderr, "paycli: sign request failed")
		return 1
	}
	resp, err := client.Do(req)
	if err != nil {
		writeLine(stderr, "paycli: request failed")
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		writeLine(stderr, "paycli: request failed")
		return 1
	}
	if !p.admin {
		if err = sign.VerifyPlatform([]byte(cfg.AppSecret), sign.PlatformMessage{
			Timestamp: resp.Header.Get(sign.HeaderTimestamp),
			Nonce:     resp.Header.Get(sign.HeaderNonce),
			Body:      respBody,
		}, resp.Header.Get(sign.HeaderSignature)); err != nil {
			writeLine(stderr, "paycli: response signature invalid")
			return 1
		}
	}
	if _, err = stdout.Write(respBody); err != nil {
		writeLine(stderr, "paycli: write failed")
		return 1
	}
	code, ok := responseCode(respBody)
	if !ok || code != 0 {
		return 1
	}
	return 0
}

func signMerchant(req *http.Request, cfg config.PayCLI, clk clock.Clock, nextNonce func() (string, error), body []byte) error {
	nonce, err := nextNonce()
	if err != nil || !nonceOK(nonce) {
		return errUsage
	}
	ts := sign.FormatTimestamp(clk.Now())
	sig, err := sign.SignRequest([]byte(cfg.AppSecret), sign.Request{
		Method:     req.Method,
		RequestURI: req.URL.RequestURI(),
		Timestamp:  ts,
		Nonce:      nonce,
		Body:       body,
	})
	if err != nil {
		return err
	}
	req.Header.Set(sign.HeaderAppID, cfg.AppID)
	req.Header.Set(sign.HeaderTimestamp, ts)
	req.Header.Set(sign.HeaderNonce, nonce)
	req.Header.Set(sign.HeaderSignature, sig)
	return nil
}

func requestURL(gateway, pathQuery string) (string, error) {
	u, err := url.Parse(strings.TrimRight(gateway, "/") + pathQuery)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", errUsage
	}
	return u.String(), nil
}

func responseCode(body []byte) (int64, bool) {
	var env struct {
		Code *int64 `json:"code"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Code == nil {
		return 0, false
	}
	return *env.Code, true
}
