//go:build integration

package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	adminserver "github.com/yym108/pay-system/pkg/server"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/gateway" //nolint:depguard // OPS-03 验收要打到真实网关的两条路由。
)

const (
	ops03Merchant = "M100091"
	ops03Secret   = "ops03-app-secret" //nolint:gosec // G101: 测试用的假密钥，不是生产密钥。
	ops03App      = "app_ops03"
)

var ops03NonceSeq atomic.Uint64

// OPS-03 验收 1：下单、支付、回调、入账之后，两条路由、两次状态迁移、一次渠道调用和一次 PAY 入账耗时都加 1。
func TestOPS03PayChainMetrics(t *testing.T) {
	metricsURL := startOPS03Admin(t)
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openPayments(t, env, ops03Merchant, 60, 60, now, "merchant-demo")
	channelURL, _, _ := listenChannel(t)
	callbackURL, merchantURL := startOPS03Gateway(t, p)
	wireCallbackPay(t, p, channelURL, callbackURL)

	before := readMetrics(t, metricsURL)
	createBody := []byte(`{"out_trade_no":"OPS03-PAY","amount":10000,"subject":"demo","notify_url":"http://merchant-demo/notify"}`)
	created := ops03Post(t, merchantURL, "/v1/payments", createBody)
	payNo := ops03PayNo(t, created)
	ops03Post(t, merchantURL, "/v1/payments/"+payNo+"/pay", []byte(`{"channel":"mockpay"}`))
	ok := waitUntil(t, clock.Real{}, 5*time.Second, func() bool {
		return loadPayment(t, env, ops03Merchant, "OPS03-PAY").status == payStatusSuccess
	})
	if !ok {
		t.Fatalf("order not SUCCESS: %+v", loadPayment(t, env, ops03Merchant, "OPS03-PAY"))
	}
	box := newLedgerBox(t, env)
	box.openBooks(ops03Merchant)
	box.dispatch(now)
	box.serve()
	box.waitPay(ops03Merchant, payNo, 1)

	var after string
	ok = waitUntil(t, clock.Real{}, 5*time.Second, func() bool {
		after = readMetrics(t, metricsURL)
		return promDelta(before, after, "pay_ledger_post_duration_seconds_count", map[string]string{"biz_type": "PAY"}) == 1
	})
	if !ok {
		t.Fatalf("ledger histogram\n%s", promLines(after, "pay_ledger_post_duration_seconds"))
	}
	assertPromDelta(t, before, after, "pay_http_requests_total", map[string]string{"route": "/v1/payments"}, 1)
	assertPromDelta(t, before, after, "pay_http_requests_total", map[string]string{"route": "/v1/payments/:pay_no/pay"}, 1)
	assertPromDelta(t, before, after, "pay_order_transitions_total", map[string]string{"type": "PAY", "from": "INIT", "to": "PAYING"}, 1)
	assertPromDelta(t, before, after, "pay_order_transitions_total", map[string]string{"type": "PAY", "from": "PAYING", "to": "SUCCESS"}, 1)
	assertPromDelta(t, before, after, "pay_channel_requests_total", nil, 1)
	assertPromDelta(t, before, after, "pay_ledger_post_duration_seconds_count", map[string]string{"biz_type": "PAY"}, 1)
}

func startOPS03Admin(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := adminserver.New(adminserver.Config{
		Listener:     ln,
		DrainTimeout: 50 * time.Millisecond,
		Signals:      make(chan os.Signal, 1),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = srv.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("admin server did not stop")
		}
	})
	return "http://" + ln.Addr().String() + "/metrics"
}

func startOPS03Gateway(t *testing.T, p *Payments) (callbackURL, base string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ln, err := gateway.Listen(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := gateway.NewEngine(ln)
	gateway.NewMerchant(
		gateway.WithClock(clock.Real{}),
		gateway.WithChannelKey([]byte(testChannelKey)),
		gateway.WithNotifyAPI(gwNotify{p: p}),
		gateway.WithMerchantLookup(ops03Lookup{}),
		gateway.WithNonceStore(gateway.NewMemoryNonce()),
		gateway.WithPaymentAPI(ops03PayAPI{p: p}),
	).Register(srv)
	go func() { _ = srv.Run() }()
	waitListen(t, ln.Addr().String())
	t.Cleanup(func() {
		shut, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := srv.Shutdown(shut); err != nil {
			t.Errorf("shutdown gateway: %v", err)
		}
	})
	base = "http://" + ln.Addr().String()
	return base + "/callback/mockpay", base
}

type ops03Lookup struct{}

func (ops03Lookup) Lookup(context.Context, string) (gateway.Credential, error) {
	return gateway.Credential{MerchantID: ops03Merchant, Status: "ACTIVE", Secret: []byte(ops03Secret)}, nil
}

type ops03PayAPI struct {
	p *Payments
}

func (a ops03PayAPI) CreatePayment(ctx context.Context, req *payment.CreatePaymentRequest, _ ...callopt.Option) (*payment.PaymentOrder, error) {
	if a.p == nil || req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, err := a.p.CreatePayment(ctx, &CreatePaymentInput{
		MerchantID:    req.MerchantId,
		OutTradeNo:    req.OutTradeNo,
		Amount:        money.Amount(req.Amount),
		Currency:      req.Currency,
		Subject:       req.Subject,
		NotifyURL:     req.NotifyUrl,
		ExpireMinutes: req.ExpireMinutes,
	})
	if err != nil {
		_, rpcErr := errno.RPCExit(err, false)
		return nil, rpcErr
	}
	return ops03Order(got), nil
}

func (a ops03PayAPI) Pay(ctx context.Context, req *payment.PayRequest, _ ...callopt.Option) (*payment.PayResponse, error) {
	if a.p == nil || req == nil {
		return nil, errno.ToBizStatus(errno.ErrInvalidParam)
	}
	got, err := a.p.Pay(ctx, &PayInput{MerchantID: req.MerchantId, PayNo: req.PayNo, Channel: req.Channel})
	if err != nil {
		code, rpcErr := errno.RPCExit(err, true)
		if rpcErr != nil {
			return nil, rpcErr
		}
		return &payment.PayResponse{BizCode: code, PaymentOrder: ops03Order(got)}, nil
	}
	return &payment.PayResponse{BizCode: 0, PaymentOrder: ops03Order(got)}, nil
}

func (ops03PayAPI) ClosePayment(context.Context, *payment.ClosePaymentRequest, ...callopt.Option) (*payment.ClosePaymentResponse, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (ops03PayAPI) QueryPayment(context.Context, *payment.QueryPaymentRequest, ...callopt.Option) (*payment.PaymentOrder, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (ops03PayAPI) CreateRefund(context.Context, *payment.CreateRefundRequest, ...callopt.Option) (*payment.CreateRefundResponse, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (ops03PayAPI) QueryRefund(context.Context, *payment.QueryRefundRequest, ...callopt.Option) (*payment.RefundOrder, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (ops03PayAPI) CreateTransfer(context.Context, *payment.CreateTransferRequest, ...callopt.Option) (*payment.CreateTransferResponse, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (ops03PayAPI) QueryTransfer(context.Context, *payment.QueryTransferRequest, ...callopt.Option) (*payment.Transfer, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func ops03Order(in PaymentOrder) *payment.PaymentOrder {
	return &payment.PaymentOrder{
		PayNo:      in.PayNo,
		OutTradeNo: in.OutTradeNo,
		Amount:     int64(in.Amount),
		Currency:   in.Currency,
		Subject:    in.Subject,
		Status:     in.Status,
	}
}

func ops03Post(t *testing.T, base, uri string, body []byte) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+uri, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	hdr := ops03Sign(t, http.MethodPost, uri, body)
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status %d body %s", uri, resp.StatusCode, raw)
	}
	var env struct {
		Code int32 `json:"code"`
	}
	if err = json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("json %s: %v", raw, err)
	}
	if env.Code != 0 {
		t.Fatalf("POST %s code %d body %s", uri, env.Code, raw)
	}
	return raw
}

func ops03PayNo(t *testing.T, raw []byte) string {
	t.Helper()
	var env struct {
		Data struct {
			PayNo string `json:"pay_no"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if env.Data.PayNo == "" {
		t.Fatalf("missing pay_no in %s", raw)
	}
	return env.Data.PayNo
}

func ops03Sign(t *testing.T, method, uri string, body []byte) http.Header {
	t.Helper()
	nonce := fmt.Sprintf("ops03n%010d", ops03NonceSeq.Add(1))
	ts := sign.FormatTimestamp(clock.Real{}.Now())
	sig, err := sign.SignRequest([]byte(ops03Secret), sign.Request{
		Method: method, RequestURI: uri, Timestamp: ts, Nonce: nonce, Body: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := make(http.Header)
	h.Set(sign.HeaderAppID, ops03App)
	h.Set(sign.HeaderTimestamp, ts)
	h.Set(sign.HeaderNonce, nonce)
	h.Set(sign.HeaderSignature, sig)
	h.Set("Content-Type", "application/json")
	return h
}

func readMetrics(t *testing.T, url string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var last string
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			raw, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			if rerr == nil && resp.StatusCode == http.StatusOK {
				return string(raw)
			}
			last = string(raw)
		} else {
			last = err.Error()
		}
		if ctx.Err() != nil {
			t.Fatalf("GET /metrics: %s", last)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func assertPromDelta(t *testing.T, before, after, name string, labels map[string]string, want int64) {
	t.Helper()
	got := promDelta(before, after, name, labels)
	if got != want {
		t.Fatalf("%s %v delta = %d want %d\n%s", name, labels, got, want, promLines(after, name))
	}
}

func promDelta(before, after, name string, labels map[string]string) int64 {
	return promSum(after, name, labels) - promSum(before, name, labels)
}

func promSum(body, name string, labels map[string]string) int64 {
	var sum int64
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp <= 0 {
			continue
		}
		left, raw := line[:sp], line[sp+1:]
		metric := left
		labs := ""
		if i := strings.IndexByte(left, '{'); i >= 0 {
			metric = left[:i]
			labs = left[i:]
		}
		if metric != name || !promLabels(labs, labels) {
			continue
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue
		}
		sum += n
	}
	return sum
}

func promLabels(labs string, want map[string]string) bool {
	for k, v := range want {
		if !strings.Contains(labs, k+`="`+v+`"`) {
			return false
		}
	}
	return true
}

func promLines(body, name string) string {
	var b strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, name) {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}
