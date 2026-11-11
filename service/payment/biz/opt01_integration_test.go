//go:build integration

package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/server"
	"github.com/cloudwego/kitex/transport"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/kitex_gen/payment/paymentservice"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/service/gateway" //nolint:depguard // OPT-01 验收要打到真实网关，生产代码仍禁止。
)

// OPT-01 验收 1：一次跨分片转账在 Jaeger 里能看到 transfer_tcc 的各阶段，
// 各阶段 duration_ns 之和与这次接口耗时的差值小于 5%。
// 接口耗时只包住 client.Do：请求已签好、body 已准备好才开始，响应头一到就停，不含签名和读 body。
func TestOPT01JaegerTransferTCC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	env := openPay(t)
	now := feeAt(11, 0, 0, 0)
	p := openTransfers(t, env, now)
	accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
	keepSchemas(t, schema0, schema1)
	openTCCBooks(t, env.ctx, accounts)
	fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 10000, "PAY-OPT01-JAEGER")
	p.UseTCCTransfer(accountBizTCC{svc: accounts}, false)
	_, base := opt01StartGateway(t, p)
	client := &http.Client{Timeout: 30 * time.Second}
	opt01Warm(t, client, base)
	// 环境就绪后再安装导出器，避免 testcontainers 的 Docker 调用占满 Jaeger 的最近 trace。
	queryBase := opt01StartJaeger(ctx, t)

	body := []byte(`{"out_transfer_no":"OPT01-JAEGER","payee_merchant_id":"M100002","amount":500,"notify_url":"http://merchant-demo/notify"}`)
	status, raw, reqNS := opt01TimedDo(t, client, base, "/v1/transfers", body)
	if status != http.StatusOK {
		t.Fatalf("transfer %d %s", status, raw)
	}
	transferNo := opt01TransferNo(t, raw)
	if reqNS <= 0 {
		t.Fatalf("req_ns = %d", reqNS)
	}

	spans, last, err := opt01WaitJaeger(ctx, queryBase, func(spans []opt01Span) bool {
		_, ok := opt01TCCEvents(spans, transferNo)
		return ok
	})
	if errors.Is(err, errOpt01JaegerAPI) {
		t.Skip("jaeger query: " + err.Error() + " body=" + opt01Clip(last))
	}
	if err != nil {
		t.Fatalf("jaeger: %v transfer_no_in_body=%v stage_in_body=%v body=%s", err, strings.Contains(last, transferNo), strings.Contains(last, obs.StageDBDecision), opt01Clip(last))
	}
	evs, ok := opt01TCCEvents(spans, transferNo)
	if !ok {
		t.Fatalf("stages missing transfer_no=%s body=%s", transferNo, opt01Clip(last))
	}
	var sum int64
	var b strings.Builder
	for _, ev := range evs {
		if ev.Chain != obs.ChainTransferTCC || !ev.HasNS || ev.NS < 0 {
			t.Fatalf("event %+v", ev)
		}
		sum += ev.NS
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(ev.Name)
		b.WriteByte('=')
		b.WriteString(strconv.FormatInt(ev.NS, 10))
	}
	traceID := ""
	for _, sp := range spans {
		if sp.Tags["transfer_no"] == transferNo && opt01Hex32(sp.TraceID) {
			traceID = strings.ToLower(sp.TraceID)
			break
		}
	}
	if !opt01Hex32(traceID) {
		t.Fatalf("trace id %q", traceID)
	}
	diff := sum - reqNS
	if diff < 0 {
		diff = -diff
	}
	fmt.Printf("OPT-01 jaeger transfer_no=%s trace=%s req_ns=%d sum_ns=%d diff_ns=%d stages=%s\n", transferNo, traceID, reqNS, sum, diff, b.String())
	if !opt01Within(sum, reqNS) {
		t.Fatalf("sum_ns=%d req_ns=%d diff exceeds 5%% stages=%s", sum, reqNS, b.String())
	}
	requireCheck(t, env.ctx, accounts)
}

// OPT-01 验收 2：6 条链路各执行一次后，规则表里每个 (chain, stage) 的样本数都 >= 1。
func TestOPT01AllChainsMetrics(t *testing.T) {
	metricsURL := startOPS03Admin(t)
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	if _, err := env.svc.AppendFeeRate(env.ctx, "M100001", 60, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	channelURL, _, _ := listenChannel(t)
	_, merchantURL := opt01StartGateway(t, p)
	wireCallbackPay(t, p, channelURL, merchantURL+"/callback/mockpay")

	client := &http.Client{Timeout: 30 * time.Second}
	createBody := []byte(`{"out_trade_no":"OPT01-PAY","amount":10000,"subject":"demo","notify_url":"http://merchant-demo/notify"}`)
	created := opt01MustOK(t, client, merchantURL, "/v1/payments", createBody)
	payNo := opt01PayNo(t, created)
	opt01MustOK(t, client, merchantURL, "/v1/payments/"+payNo+"/pay", []byte(`{"channel":"mockpay"}`))
	if !waitUntil(t, clock.Real{}, 30*time.Second, func() bool {
		return loadPayment(t, env, "M100001", "OPT01-PAY").status == payStatusSuccess
	}) {
		t.Fatalf("order not SUCCESS: %+v", loadPayment(t, env, "M100001", "OPT01-PAY"))
	}

	box := newLedgerBox(t, env)
	box.openBooks("M100001")
	opt01OpenTransferBooks(t, box)
	box.dispatch(now)
	box.serve()
	box.waitPay("M100001", payNo, 1)

	refunder, err := NewChannelClient(ChannelClientConfig{
		BaseURL: channelURL, Key: []byte(testChannelKey), Clock: clock.Real{},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.UseRefund(accountBizFreezer{svc: box.svc}, refunder, nil, "")
	p.UseLocalTransfer(accountBizLocal{svc: box.svc})
	p.UseTCCTransfer(accountBizTCC{svc: box.svc}, false)

	opt01MustOK(t, client, merchantURL, "/v1/refunds", []byte(`{"pay_no":"`+payNo+`","out_refund_no":"OPT01-RF","amount":1000}`))
	opt01MustOK(t, client, merchantURL, "/v1/transfers", []byte(`{"out_transfer_no":"OPT01-LOCAL","payee_merchant_id":"M100003","amount":500,"notify_url":"http://merchant-demo/notify"}`))
	opt01MustOK(t, client, merchantURL, "/v1/transfers", []byte(`{"out_transfer_no":"OPT01-TCC","payee_merchant_id":"M100002","amount":500,"notify_url":"http://merchant-demo/notify"}`))
	status, raw := opt01Exchange(t, client, merchantURL, "/v1/transfers", []byte(`{"out_transfer_no":"OPT01-CANCEL","payee_merchant_id":"M100002","amount":100000,"notify_url":"http://merchant-demo/notify"}`))
	if status != http.StatusConflict || opt01Code(t, raw) != errno.ErrInsufficientBalance.Code {
		t.Fatalf("cancel %d %s", status, raw)
	}
	requireCheck(t, box.ctx, box.svc)

	body := readMetrics(t, metricsURL)
	for _, pair := range opt01StagePairs {
		n := promSum(body, "pay_stage_duration_seconds_count", map[string]string{"chain": pair[0], "stage": pair[1]})
		fmt.Printf("OPT-01 metric %s %s count=%d\n", pair[0], pair[1], n)
		if n < 1 {
			t.Fatalf("%s %s count=%d\n%s", pair[0], pair[1], n, promLines(body, "pay_stage_duration_seconds"))
		}
	}
}

var opt01StagePairs = [][2]string{
	{obs.ChainPayCreate, obs.StageGWAuth},
	{obs.ChainPayCreate, obs.StageGWNonce},
	{obs.ChainPayCreate, obs.StageRPC},
	{obs.ChainPayCreate, obs.StageDBInsert},
	{obs.ChainPaySubmit, obs.StageGWAuth},
	{obs.ChainPaySubmit, obs.StageDBTx1},
	{obs.ChainPaySubmit, obs.StageChannelCall},
	{obs.ChainPaySubmit, obs.StageDBTx2},
	{obs.ChainPaySettle, obs.StageCallbackTx},
	{obs.ChainPaySettle, obs.StageOutboxWait},
	{obs.ChainPaySettle, obs.StageMQWait},
	{obs.ChainPaySettle, obs.StageLedgerTx},
	{obs.ChainRefund, obs.StageGWAuth},
	{obs.ChainRefund, obs.StageDBQuotaTx},
	{obs.ChainRefund, obs.StageRPCFreeze},
	{obs.ChainRefund, obs.StageDBTx},
	{obs.ChainRefund, obs.StageChannelCall},
	{obs.ChainTransferLocal, obs.StageGWAuth},
	{obs.ChainTransferLocal, obs.StageDBInsert},
	{obs.ChainTransferLocal, obs.StageRPCTransferLocal},
	{obs.ChainTransferLocal, obs.StageDBFinishTx},
	{obs.ChainTransferTCC, obs.StageGWAuth},
	{obs.ChainTransferTCC, obs.StageDBInsert},
	{obs.ChainTransferTCC, obs.StageTry},
	{obs.ChainTransferTCC, obs.StageDBDecision},
	{obs.ChainTransferTCC, obs.StageConfirm},
	{obs.ChainTransferTCC, obs.StageCancel},
	{obs.ChainTransferTCC, obs.StageDBFinishTx},
}

func opt01Within(sum, req int64) bool {
	if req <= 0 {
		return false
	}
	diff := sum - req
	if diff < 0 {
		diff = -diff
	}
	return diff*100 < req*5
}

func opt01OpenTransferBooks(t *testing.T, box *ledgerBox) {
	t.Helper()
	for _, shardNo := range []int{0, 1} {
		for sub := 0; sub < 8; sub++ {
			if _, err := box.svc.OpenAccount(box.ctx, "PLATFORM", "PLATFORM", "TRANSIT", shardNo, sub); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := box.svc.OpenAccount(box.ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := box.svc.OpenAccount(box.ctx, "MERCHANT", "M100003", "MERCHANT_SETTLE", 1, 0); err != nil {
		t.Fatal(err)
	}
}

type opt01Lookup struct{}

func (opt01Lookup) Lookup(context.Context, string) (gateway.Credential, error) {
	return gateway.Credential{MerchantID: "M100001", Status: "ACTIVE", Secret: []byte(ops03Secret)}, nil
}

func opt01StartGateway(t *testing.T, p *Payments) (callbackURL, base string) {
	t.Helper()
	pay := opt01DialPayment(t, p)
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
		gateway.WithMerchantLookup(opt01Lookup{}),
		gateway.WithNonceStore(gateway.NewMemoryNonce()),
		gateway.WithPaymentAPI(pay),
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

// opt01DialPayment 用真实 Kitex。服务端中间件只取出 traceparent，不把网关的阶段槽带进 biz。
func opt01DialPayment(t *testing.T, p *Payments) paymentservice.Client {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	exit := make(chan error, 1)
	svr := paymentservice.NewServer(&opt01Kitex{p: p},
		server.WithListener(ln),
		server.WithMetaHandler(transmeta.ServerTTHeaderHandler),
		server.WithMiddleware(opt01TraceMW),
		server.WithExitSignal(func() <-chan error { return exit }),
		server.WithExitWaitTime(time.Second),
	)
	errCh := make(chan error, 1)
	go func() { errCh <- svr.Run() }()
	waitListen(t, ln.Addr().String())
	t.Cleanup(func() {
		select {
		case exit <- errors.New("stop"):
		default:
		}
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case runErr := <-errCh:
			if runErr != nil && !strings.Contains(runErr.Error(), "stop") {
				t.Errorf("payment rpc exit: %v", runErr)
			}
		case <-timer.C:
			t.Error("payment rpc did not exit")
		}
	})
	cli, err := paymentservice.NewClient("PaymentService",
		client.WithHostPorts(ln.Addr().String()),
		client.WithConnectTimeout(time.Second),
		client.WithRPCTimeout(5*time.Second),
		client.WithTransportProtocol(transport.TTHeader),
		client.WithMetaHandler(transmeta.ClientTTHeaderHandler),
		client.WithMiddleware(obs.ClientMiddleware("gateway")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cli
}

func opt01TraceMW(next endpoint.Endpoint) endpoint.Endpoint {
	return func(ctx context.Context, req, resp interface{}) error {
		if tp, ok := metainfo.GetPersistentValue(ctx, "traceparent"); ok && tp != "" {
			ctx = obs.ExtractMap(ctx, map[string]string{"traceparent": tp})
		}
		return next(ctx, req, resp)
	}
}

type opt01Kitex struct {
	p *Payments
}

func (s opt01Kitex) begin(ctx context.Context, method string) (context.Context, func(error)) {
	return obs.HandlerSpan{Service: "payment"}.Start(ctx, method)
}

// opt01HandlerNS 量这个方法自己的耗时。先注册，关槽和 span 结束之后才回传。
// biz 不能调用 time.Now，用 clock.Mono。没装发送桶时 SendHandlerNS 自己忽略。
func opt01HandlerNS(ctx context.Context) func() {
	start := clock.Mono()
	return func() {
		obs.SendHandlerNS(ctx, time.Since(start))
	}
}

func (s opt01Kitex) CreatePayment(ctx context.Context, req *payment.CreatePaymentRequest) (r *payment.PaymentOrder, err error) {
	defer opt01HandlerNS(ctx)()
	ctx, end := s.begin(ctx, "CreatePayment")
	defer func() { end(err) }()
	if s.p == nil || req == nil {
		err = errno.ToBizStatus(errno.ErrInvalidParam)
		return nil, err
	}
	got, callErr := s.p.CreatePayment(ctx, &CreatePaymentInput{
		MerchantID:    req.MerchantId,
		OutTradeNo:    req.OutTradeNo,
		Amount:        money.Amount(req.Amount),
		Currency:      req.Currency,
		Subject:       req.Subject,
		NotifyURL:     req.NotifyUrl,
		ExpireMinutes: req.ExpireMinutes,
	})
	if callErr != nil {
		_, err = errno.RPCExit(callErr, false)
		return nil, err
	}
	return ops03Order(got), nil
}

func (s opt01Kitex) Pay(ctx context.Context, req *payment.PayRequest) (r *payment.PayResponse, err error) {
	defer opt01HandlerNS(ctx)()
	ctx, end := s.begin(ctx, "Pay")
	defer func() { end(err) }()
	if s.p == nil || req == nil {
		err = errno.ToBizStatus(errno.ErrInvalidParam)
		return nil, err
	}
	got, callErr := s.p.Pay(ctx, &PayInput{MerchantID: req.MerchantId, PayNo: req.PayNo, Channel: req.Channel})
	if errors.Is(callErr, errno.ErrChannelRejected) {
		code, rpcErr := errno.RPCExit(callErr, true)
		if rpcErr != nil {
			err = rpcErr
			return nil, err
		}
		return &payment.PayResponse{BizCode: code, PaymentOrder: ops03Order(got)}, nil
	}
	if callErr != nil {
		_, err = errno.RPCExit(callErr, false)
		return nil, err
	}
	return &payment.PayResponse{BizCode: 0, PaymentOrder: ops03Order(got)}, nil
}

func (s opt01Kitex) CreateRefund(ctx context.Context, req *payment.CreateRefundRequest) (r *payment.CreateRefundResponse, err error) {
	defer opt01HandlerNS(ctx)()
	ctx, end := s.begin(ctx, "CreateRefund")
	defer func() { end(err) }()
	if s.p == nil || req == nil {
		err = errno.ToBizStatus(errno.ErrInvalidParam)
		return nil, err
	}
	reason := ""
	if req.Reason != nil {
		reason = *req.Reason
	}
	got, callErr := s.p.CreateRefund(ctx, &CreateRefundInput{
		MerchantID:  req.MerchantId,
		PayNo:       req.GetPayNo(),
		OutTradeNo:  req.GetOutTradeNo(),
		OutRefundNo: req.OutRefundNo,
		Amount:      money.Amount(req.Amount),
		Reason:      reason,
	})
	if errors.Is(callErr, errno.ErrInsufficientBalance) || errors.Is(callErr, errno.ErrChannelRejected) {
		code, rpcErr := errno.RPCExit(callErr, true)
		if rpcErr != nil {
			err = rpcErr
			return nil, err
		}
		return &payment.CreateRefundResponse{BizCode: code, RefundOrder: opt01Refund(got)}, nil
	}
	if callErr != nil {
		_, err = errno.RPCExit(callErr, false)
		return nil, err
	}
	return &payment.CreateRefundResponse{BizCode: 0, RefundOrder: opt01Refund(got)}, nil
}

func (s opt01Kitex) CreateTransfer(ctx context.Context, req *payment.CreateTransferRequest) (r *payment.CreateTransferResponse, err error) {
	defer opt01HandlerNS(ctx)()
	ctx, end := s.begin(ctx, "CreateTransfer")
	defer func() { end(err) }()
	// 测试服务端不走生产中间件。槽和 db_insert 从方法入口开始，响应转换之后再关。
	ctx = obs.WithSlot(obs.WithChain(ctx, ""))
	defer obs.CloseSlot(ctx)
	if obs.CurrentStage(ctx) != obs.StageDBInsert {
		obs.Begin(ctx, "", obs.StageDBInsert)
	}
	if s.p == nil || req == nil {
		err = errno.ToBizStatus(errno.ErrInvalidParam)
		return nil, err
	}
	got, callErr := s.p.CreateTransfer(ctx, &CreateTransferInput{
		MerchantID:      req.MerchantId,
		OutTransferNo:   req.OutTransferNo,
		PayeeMerchantID: req.PayeeMerchantId,
		Amount:          money.Amount(req.Amount),
		Remark:          req.Remark,
		NotifyURL:       req.NotifyUrl,
	})
	if errors.Is(callErr, errno.ErrInsufficientBalance) {
		code, rpcErr := errno.RPCExit(callErr, true)
		if rpcErr != nil {
			err = rpcErr
			return nil, err
		}
		return &payment.CreateTransferResponse{BizCode: code, Transfer: opt01Transfer(got)}, nil
	}
	if callErr != nil {
		_, err = errno.RPCExit(callErr, false)
		return nil, err
	}
	return &payment.CreateTransferResponse{BizCode: 0, Transfer: opt01Transfer(got)}, nil
}

func (opt01Kitex) ClosePayment(context.Context, *payment.ClosePaymentRequest) (*payment.ClosePaymentResponse, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (opt01Kitex) QueryPayment(context.Context, *payment.QueryPaymentRequest) (*payment.PaymentOrder, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (opt01Kitex) QueryRefund(context.Context, *payment.QueryRefundRequest) (*payment.RefundOrder, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (opt01Kitex) QueryTransfer(context.Context, *payment.QueryTransferRequest) (*payment.Transfer, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (opt01Kitex) HandleChannelNotify(context.Context, *payment.HandleChannelNotifyRequest) (*payment.HandleChannelNotifyResponse, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (opt01Kitex) GetMerchantCredential(context.Context, *payment.GetMerchantCredentialRequest) (*payment.GetMerchantCredentialResponse, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (opt01Kitex) SyncChannelTxn(context.Context, *payment.SyncChannelTxnRequest) (*payment.SyncChannelTxnResponse, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func (opt01Kitex) ReplayEvent(context.Context, *payment.ReplayEventRequest) (*payment.ReplayEventResponse, error) {
	return nil, errno.ToBizStatus(errno.ErrInternal)
}

func opt01Refund(in RefundOrder) *payment.RefundOrder {
	return &payment.RefundOrder{
		RefundNo:    in.RefundNo,
		OutRefundNo: in.OutRefundNo,
		PayNo:       in.PayNo,
		Amount:      int64(in.Amount),
		Currency:    in.Currency,
		Status:      in.Status,
	}
}

func opt01Transfer(in TransferOrder) *payment.Transfer {
	return &payment.Transfer{
		TransferNo:      in.TransferNo,
		OutTransferNo:   in.OutTransferNo,
		MerchantId:      in.MerchantID,
		PayeeMerchantId: in.PayeeMerchantID,
		Amount:          int64(in.Amount),
		Currency:        in.Currency,
		Mode:            in.Mode,
		Status:          in.Status,
	}
}

func opt01Warm(t *testing.T, client *http.Client, base string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/payments", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
	_ = resp.Body.Close()
}

func opt01Exchange(t *testing.T, client *http.Client, base, uri string, body []byte) (int, []byte) {
	t.Helper()
	status, raw, _ := opt01TimedDo(t, client, base, uri, body)
	return status, raw
}

// opt01TimedDo 在签名和 body 都准备好之后开始计时，client.Do 一返回就停。
// 响应头到了之后的 ReadAll 留在停表之后，测试进程被抢走的读 body 时间不算进接口耗时。
func opt01TimedDo(t *testing.T, client *http.Client, base, uri string, body []byte) (int, []byte, int64) {
	t.Helper()
	payload := strings.NewReader(string(body))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+uri, payload)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range ops03Sign(t, http.MethodPost, uri, body) {
		req.Header[k] = v
	}
	start := clock.Mono()
	resp, err := client.Do(req)
	reqNS := time.Since(start).Nanoseconds()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw, reqNS
}

func opt01MustOK(t *testing.T, client *http.Client, base, uri string, body []byte) []byte {
	t.Helper()
	status, raw := opt01Exchange(t, client, base, uri, body)
	if status != http.StatusOK || opt01Code(t, raw) != 0 {
		t.Fatalf("POST %s status %d body %s", uri, status, raw)
	}
	return raw
}

func opt01Code(t *testing.T, raw []byte) int32 {
	t.Helper()
	var env struct {
		Code int32 `json:"code"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("json %s: %v", raw, err)
	}
	return env.Code
}

func opt01PayNo(t *testing.T, raw []byte) string {
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

func opt01TransferNo(t *testing.T, raw []byte) string {
	t.Helper()
	var env struct {
		Data struct {
			TransferNo string `json:"transfer_no"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if env.Data.TransferNo == "" {
		t.Fatalf("missing transfer_no in %s", raw)
	}
	return env.Data.TransferNo
}

func opt01StartJaeger(ctx context.Context, t *testing.T) string {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, "jaegertracing/jaeger:2.22.0",
		testcontainers.WithExposedPorts("4317/tcp", "16686/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("16686/tcp").WithStartupTimeout(2*time.Minute)),
	)
	if err != nil {
		t.Skip("jaeger container: " + err.Error())
	}
	t.Cleanup(func() { //nolint:contextcheck // 清理发生在测试 ctx 取消之后。
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer stopCancel()
		_ = ctr.Terminate(stopCtx)
	})
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if host == "localhost" || host == "::1" {
		host = "127.0.0.1"
	}
	otlp, err := ctr.MappedPort(ctx, "4317/tcp")
	if err != nil {
		t.Fatal(err)
	}
	queryPort, err := ctr.MappedPort(ctx, "16686/tcp")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	shutdown, err := obs.Install(ctx, "gateway", net.JoinHostPort(host, otlp.Port()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { //nolint:contextcheck // 清理发生在测试 ctx 取消之后。
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutCancel()
		_ = shutdown(shutCtx)
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return "http://" + net.JoinHostPort(host, queryPort.Port())
}

var errOpt01JaegerAPI = errors.New("jaeger query path unavailable")

var opt01TCCStages = []string{
	obs.StageGWAuth,
	obs.StageDBInsert,
	obs.StageTry,
	obs.StageDBDecision,
	obs.StageConfirm,
	obs.StageDBFinishTx,
}

func opt01TCCEvents(spans []opt01Span, transferNo string) ([]opt01Event, bool) {
	var traceID string
	for _, sp := range spans {
		if sp.Tags["transfer_no"] == transferNo {
			traceID = sp.TraceID
			break
		}
	}
	if traceID == "" {
		return nil, false
	}
	found := map[string]opt01Event{}
	seenSpan := map[string]bool{}
	for _, sp := range spans {
		if sp.TraceID != traceID {
			continue
		}
		if sp.SpanID != "" {
			if seenSpan[sp.SpanID] {
				continue
			}
			seenSpan[sp.SpanID] = true
		}
		for _, ev := range sp.Events {
			if ev.Chain != "" && ev.Chain != obs.ChainTransferTCC {
				continue
			}
			if _, dup := found[ev.Name]; dup {
				return nil, false
			}
			found[ev.Name] = ev
		}
	}
	out := make([]opt01Event, 0, len(opt01TCCStages))
	for _, name := range opt01TCCStages {
		ev, ok := found[name]
		if !ok {
			return nil, false
		}
		out = append(out, ev)
	}
	return out, true
}

func opt01WaitJaeger(ctx context.Context, base string, ready func([]opt01Span) bool) ([]opt01Span, string, error) {
	v3 := url.Values{}
	v3.Set("query.service_name", "gateway")
	v3.Set("query.operation_name", "/v1/transfers")
	v3.Set("query.num_traces", "20")
	v3.Set("query.start_time_min", "2020-01-01T00:00:00Z")
	v3.Set("query.start_time_max", "2030-01-01T00:00:00Z")
	paths := []string{
		"/api/traces?service=gateway&limit=20&lookback=1h",
		"/api/v3/traces?" + v3.Encode(),
	}
	var last string
	miss := map[string]int{}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		if tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
			flushCtx, flushCancel := context.WithTimeout(ctx, time.Second)
			_ = tp.ForceFlush(flushCtx)
			flushCancel()
		}
		progress := false
		for _, path := range paths {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
			if err != nil {
				return nil, last, err
			}
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			last = string(body)
			if resp.StatusCode == http.StatusNotFound {
				if strings.Contains(last, "No traces found") {
					progress = true
					continue
				}
				miss[path]++
				continue
			}
			progress = true
			if resp.StatusCode != http.StatusOK {
				continue
			}
			spans := opt01ParseSpans(body)
			if ready(spans) {
				return spans, last, nil
			}
		}
		if !progress && miss[paths[0]] >= 4 && miss[paths[1]] >= 4 {
			return nil, last, errOpt01JaegerAPI
		}
		select {
		case <-ctx.Done():
			return nil, last, ctx.Err()
		case <-tick.C:
		}
	}
}

type opt01Event struct {
	Name  string
	Chain string
	Stage string
	NS    int64
	HasNS bool
}

type opt01Span struct {
	TraceID string
	SpanID  string
	Name    string
	Tags    map[string]string
	Events  []opt01Event
}

func opt01ParseSpans(body []byte) []opt01Span {
	if spans := opt01ParseInternal(body); len(spans) > 0 {
		return spans
	}
	return opt01ParseV3(body)
}

func opt01ParseInternal(body []byte) []opt01Span {
	var resp struct {
		Data []struct {
			Spans []struct {
				TraceID       string `json:"traceID"`
				SpanID        string `json:"spanID"`
				OperationName string `json:"operationName"`
				Tags          []struct {
					Key   string          `json:"key"`
					Value json.RawMessage `json:"value"`
				} `json:"tags"`
				Logs []struct {
					Fields []struct {
						Key   string          `json:"key"`
						Value json.RawMessage `json:"value"`
					} `json:"fields"`
				} `json:"logs"`
			} `json:"spans"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Data) == 0 {
		return nil
	}
	out := make([]opt01Span, 0)
	for _, tr := range resp.Data {
		for _, sp := range tr.Spans {
			tags := map[string]string{}
			for _, tag := range sp.Tags {
				tags[tag.Key] = opt01RawString(tag.Value)
			}
			var events []opt01Event
			for _, lg := range sp.Logs {
				ev := opt01Event{}
				for _, f := range lg.Fields {
					switch f.Key {
					case "event":
						ev.Name = opt01RawString(f.Value)
					case "chain":
						ev.Chain = opt01RawString(f.Value)
					case "stage":
						ev.Stage = opt01RawString(f.Value)
					case "duration_ns":
						n, ok := opt01RawInt(f.Value)
						ev.NS = n
						ev.HasNS = ok
					}
				}
				if ev.Name != "" {
					events = append(events, ev)
				}
			}
			out = append(out, opt01Span{
				TraceID: sp.TraceID, SpanID: sp.SpanID, Name: sp.OperationName, Tags: tags, Events: events,
			})
		}
	}
	return out
}

type opt01V3Attr struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type opt01V3Span struct {
	TraceID    string        `json:"traceId"`
	SpanID     string        `json:"spanId"`
	Name       string        `json:"name"`
	Attributes []opt01V3Attr `json:"attributes"`
	Events     []struct {
		Name       string        `json:"name"`
		Attributes []opt01V3Attr `json:"attributes"`
	} `json:"events"`
}

type opt01V3Scope struct {
	Spans []opt01V3Span `json:"spans"`
}

type opt01V3Res struct {
	ScopeSpans []opt01V3Scope `json:"scopeSpans"`
}

func opt01ParseV3(body []byte) []opt01Span {
	var env struct {
		Result struct {
			ResourceSpans []opt01V3Res `json:"resourceSpans"`
		} `json:"result"`
		ResourceSpans []opt01V3Res `json:"resourceSpans"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil
	}
	rs := env.Result.ResourceSpans
	if len(rs) == 0 {
		rs = env.ResourceSpans
	}
	out := make([]opt01Span, 0)
	for _, res := range rs {
		for _, scope := range res.ScopeSpans {
			for _, sp := range scope.Spans {
				tags := map[string]string{}
				for _, attr := range sp.Attributes {
					if s, ok := opt01AttrString(attr.Value); ok {
						tags[attr.Key] = s
					}
				}
				events := make([]opt01Event, 0, len(sp.Events))
				for _, ev := range sp.Events {
					item := opt01Event{Name: ev.Name}
					for _, attr := range ev.Attributes {
						switch attr.Key {
						case "chain":
							item.Chain, _ = opt01AttrString(attr.Value)
						case "stage":
							item.Stage, _ = opt01AttrString(attr.Value)
						case "duration_ns":
							n, ok := opt01AttrInt(attr.Value)
							if !ok {
								if s, sok := opt01AttrString(attr.Value); sok {
									n, ok = opt01ParseDigits(s)
								}
							}
							item.NS = n
							item.HasNS = ok
						}
					}
					events = append(events, item)
				}
				out = append(out, opt01Span{
					TraceID: sp.TraceID, SpanID: sp.SpanID, Name: sp.Name, Tags: tags, Events: events,
				})
			}
		}
	}
	return out
}

func opt01AttrString(raw json.RawMessage) (string, bool) {
	var camel struct {
		StringValue string `json:"stringValue"`
	}
	if json.Unmarshal(raw, &camel) == nil && camel.StringValue != "" {
		return camel.StringValue, true
	}
	var snake struct {
		StringValue string `json:"string_value"`
	}
	if json.Unmarshal(raw, &snake) == nil && snake.StringValue != "" {
		return snake.StringValue, true
	}
	return "", false
}

func opt01AttrInt(raw json.RawMessage) (int64, bool) {
	var camel struct {
		IntValue json.RawMessage `json:"intValue"`
	}
	if json.Unmarshal(raw, &camel) == nil {
		if n, ok := opt01RawInt(camel.IntValue); ok {
			return n, true
		}
	}
	var snake struct {
		IntValue json.RawMessage `json:"int_value"`
	}
	if json.Unmarshal(raw, &snake) == nil {
		if n, ok := opt01RawInt(snake.IntValue); ok {
			return n, true
		}
	}
	return 0, false
}

func opt01RawString(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return ""
	}
	if s[0] == '"' {
		var out string
		if json.Unmarshal(raw, &out) == nil {
			return out
		}
	}
	return s
}

func opt01ParseDigits(s string) (int64, bool) {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func opt01RawInt(raw json.RawMessage) (int64, bool) {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return 0, false
	}
	if s[0] == '"' {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return 0, false
		}
		s = text
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func opt01Hex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

func opt01Clip(s string) string {
	if len(s) > 800 {
		return s[:800]
	}
	return s
}
