package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/kitex_gen/payment/paymentservice"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/service/payment/biz"
)

const testSecret = "demo_secret_M100001_0123456789ab" //nolint:gosec // 演示值，见约定 §8。

func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func demoCred() biz.MerchantCredential {
	return biz.MerchantCredential{
		MerchantID: "M100001", Status: "ACTIVE", AppSecret: biz.NewSecret(testSecret),
		SecretVersion: 2, DefaultNotifyURL: "http://merchant-demo/notify",
	}
}

// PAY-01：真实 Kitex 客户端拿到全部字段；DISABLED 原样返回。
func TestKitexGetMerchantCredential(t *testing.T) {
	svc := &scripted{cred: demoCred()}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, time.Second)
	ctx := context.Background()

	got, err := cli.GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{AppId: "app_m100001"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetMerchantId() != "M100001" || got.GetStatus() != "ACTIVE" || got.GetAppSecret() != testSecret ||
		got.GetSecretVersion() != 2 || got.GetDefaultNotifyUrl() != "http://merchant-demo/notify" {
		t.Fatal("credential fields not passed through")
	}
	if svc.lastAppID() != "app_m100001" {
		t.Fatalf("app_id = %q", svc.lastAppID())
	}

	svc.set(func(s *scripted) { s.cred.Status = "DISABLED" })
	got, err = cli.GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{AppId: "app_m100001"})
	if err != nil || got.GetStatus() != "DISABLED" || got.GetMerchantId() != "M100001" {
		t.Fatalf("disabled merchant must be returned, err=%v", err)
	}
}

// PAY-01 验收 2（单元）：经真实 Kitex 客户端，不存在的 app_id 返回 20004；其余码同样透传。
func TestKitexGetMerchantCredentialErrors(t *testing.T) {
	svc := &scripted{}
	access := &accessLog{}
	addr := startServer(t, svc, Config{Access: access.record})
	cli := dialKitex(t, addr, time.Second)
	for _, code := range []*errno.Error{errno.ErrMerchantInvalid, errno.ErrInternal, errno.ErrInvalidParam} {
		svc.set(func(s *scripted) { s.err = code })
		_, err := cli.GetMerchantCredential(context.Background(), &payment.GetMerchantCredentialRequest{AppId: "app_m100099"})
		assertBizCode(t, code.Error(), err, code.Code)
		rec := access.last()
		if rec.path != "GetMerchantCredential" || rec.errCode != itoa(code.Code) {
			t.Fatalf("access = %+v", rec)
		}
	}
	svc.set(func(s *scripted) { s.err = errors.New("dial tcp: password=xxx select * from merchant") })
	_, err := cli.GetMerchantCredential(context.Background(), &payment.GetMerchantCredentialRequest{AppId: "app_m100001"})
	assertBizCode(t, "unknown", err, errno.ErrInternal.Code)
	if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "select") {
		t.Fatalf("response leaked internals: %q", err.Error())
	}
}

// PAY-01 规则 1：明文密钥不进访问日志和 ERROR 日志；10003 的原因只进日志，裸 10003 不再多打一条。
func TestCredentialNotLogged(t *testing.T) {
	buf := &lockedBuffer{}
	logx.SetDefault("payment", buf)
	t.Cleanup(func() { logx.SetDefault("payment", io.Discard) })

	svc := &scripted{cred: demoCred()}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, time.Second)
	req := &payment.GetMerchantCredentialRequest{AppId: "app_m100001"}
	for i := 0; i < 3; i++ {
		if _, err := cli.GetMerchantCredential(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(buf.String(), `"msg":"request handled"`); n != 3 {
		t.Fatalf("access lines = %d: %s", n, buf.String())
	}

	svc.set(func(s *scripted) { s.err = errno.Wrap(errno.ErrInternal, errors.New("merchant lookup failed")) })
	_, err := cli.GetMerchantCredential(context.Background(), req)
	assertBizCode(t, "wrapped", err, errno.ErrInternal.Code)
	if strings.Contains(err.Error(), "merchant lookup failed") {
		t.Fatalf("response leaked cause: %q", err.Error())
	}
	logged := buf.String()
	if strings.Count(logged, `"msg":"map internal error"`) != 1 || !strings.Contains(logged, "merchant lookup failed") ||
		!strings.Contains(logged, `"path":"GetMerchantCredential"`) {
		t.Fatalf("ERROR log missing cause: %s", logged)
	}

	svc.set(func(s *scripted) { s.err = errno.ErrInternal })
	_, err = cli.GetMerchantCredential(context.Background(), req)
	assertBizCode(t, "bare", err, errno.ErrInternal.Code)
	logged = buf.String()
	if strings.Count(logged, `"msg":"map internal error"`) != 1 {
		t.Fatalf("bare 10003 logged a second ERROR: %s", logged)
	}
	if strings.Contains(logged, testSecret) {
		t.Fatal("log contains app_secret")
	}
}

// 未实现的方法返回 10003，不进 biz。各自功能点实现时替换。
func TestUnimplementedMethodsReturnInternal(t *testing.T) {
	svc := &scripted{cred: demoCred()}
	access := &accessLog{}
	addr := startServer(t, svc, Config{Access: access.record})
	cli := dialKitex(t, addr, time.Second)
	for _, c := range unimplementedCalls(cli) {
		err := c.fn()
		assertBizCode(t, c.name, err, errno.ErrInternal.Code)
		if rec := access.last(); rec.path != c.name || rec.errCode != itoa(errno.ErrInternal.Code) {
			t.Fatalf("access %s = %+v", c.name, rec)
		}
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("unimplemented methods called biz %d times", n)
	}
	h := New(svc)
	ctx := context.Background()
	if _, err := h.CreatePayment(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatal("nil CreatePayment request")
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("nil CreatePayment called biz %d times", n)
	}
	if _, err := h.Pay(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatal("nil Pay request")
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("nil Pay called biz %d times", n)
	}
	if _, err := h.ClosePayment(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatal("nil ClosePayment request")
	}
	if _, err := h.ClosePayment(ctx, &payment.ClosePaymentRequest{MerchantId: "M100001"}); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatal("empty ClosePayment pay_no")
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("invalid ClosePayment called biz %d times", n)
	}
	if _, err := h.HandleChannelNotify(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatal("nil HandleChannelNotify")
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("nil HandleChannelNotify called biz %d times", n)
	}
	if _, err := h.CreateRefund(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatal("nil CreateRefund request")
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("nil CreateRefund called biz %d times", n)
	}
	if _, err := h.CreateTransfer(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatal("nil CreateTransfer request")
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("nil CreateTransfer called biz %d times", n)
	}
	for name, fn := range map[string]func() error{
		"QueryTransfer":  func() error { _, err := h.QueryTransfer(ctx, nil); return err },
		"SyncChannelTxn": func() error { _, err := h.SyncChannelTxn(ctx, nil); return err },
		"ReplayEvent":    func() error { _, err := h.ReplayEvent(ctx, nil); return err },
	} {
		if !errors.Is(errno.FromBizStatus(fn()), errno.ErrInternal) {
			t.Fatalf("%s nil request", name)
		}
	}
}

// handler panic 不让进程退出，调用方收到 10003，panic 值不外泄。
func TestHandlerPanicReturns10003AndServerStaysUp(t *testing.T) {
	svc := &scripted{cred: demoCred(), panicking: true}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, time.Second)
	_, err := cli.GetMerchantCredential(context.Background(), &payment.GetMerchantCredentialRequest{AppId: "app_m100001"})
	assertBizCode(t, "panic", err, errno.ErrInternal.Code)
	if strings.Contains(err.Error(), "boom") || strings.Contains(err.Error(), testSecret) {
		t.Fatalf("panic value leaked: %q", err.Error())
	}
	svc.set(func(s *scripted) { s.panicking = false })
	if _, err := cli.GetMerchantCredential(context.Background(), &payment.GetMerchantCredentialRequest{AppId: "app_m100001"}); err != nil {
		t.Fatalf("server not usable after panic: %v", err)
	}
}

// 服务端处理超时 2 秒，调用方收到超时，不是 10003。
func TestServerTimeoutTwoSeconds(t *testing.T) {
	svc := &scripted{block: blockUntilDone}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, 5*time.Second)
	start := time.Now()
	_, err := cli.GetMerchantCredential(context.Background(), &payment.GetMerchantCredentialRequest{AppId: "app_m100001"})
	elapsed := time.Since(start)
	if elapsed < 1500*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("elapsed %s, want about 2s", elapsed)
	}
	if !isCallTimeout(err) {
		t.Fatalf("want timeout, got %v", err)
	}
	if _, ok := kerrors.FromBizStatusError(err); ok {
		t.Fatalf("timeout became biz status: %v", err)
	}
	svc.set(func(s *scripted) { s.block = nil; s.cred = demoCred() })
	if _, err := cli.GetMerchantCredential(context.Background(), &payment.GetMerchantCredentialRequest{AppId: "app_m100001"}); err != nil {
		t.Fatalf("server not usable after timeout: %v", err)
	}
}

func TestServerTimeoutCanBeShortForTests(t *testing.T) {
	svc := &scripted{block: blockUntilDone}
	addr := startServer(t, svc, Config{Timeout: 80 * time.Millisecond})
	cli := dialKitex(t, addr, time.Second)
	start := time.Now()
	_, err := cli.GetMerchantCredential(context.Background(), &payment.GetMerchantCredentialRequest{AppId: "app_m100001"})
	if !isCallTimeout(err) || time.Since(start) > 700*time.Millisecond {
		t.Fatalf("elapsed %s err=%v", time.Since(start), err)
	}
}

func TestHooksAndGuards(t *testing.T) {
	var methods []string
	var mu sync.Mutex
	tr := tracerFunc(func(ctx context.Context, method string) (context.Context, func(error)) {
		mu.Lock()
		methods = append(methods, method)
		mu.Unlock()
		return ctx, func(error) {}
	})
	var observed atomic.Int32
	var lastCode atomic.Int32
	mt := metricsFunc(func(_ string, code int32, _ time.Duration) {
		observed.Add(1)
		lastCode.Store(code)
	})
	svc := &scripted{err: errno.ErrMerchantInvalid}
	addr := startServer(t, svc, Config{Tracer: tr, Metrics: mt})
	cli := dialKitex(t, addr, time.Second)
	_, _ = cli.GetMerchantCredential(context.Background(), &payment.GetMerchantCredentialRequest{AppId: "x"})
	mu.Lock()
	if len(methods) != 1 || methods[0] != "GetMerchantCredential" || observed.Load() != 1 || lastCode.Load() != errno.ErrMerchantInvalid.Code {
		mu.Unlock()
		t.Fatalf("hooks methods=%v observed=%d code=%d", methods, observed.Load(), lastCode.Load())
	}
	mu.Unlock()

	ctx := context.Background()
	if _, err := New(svc).GetMerchantCredential(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatalf("nil request = %v", err)
	}
	if _, err := New(nil).GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{}); !errors.Is(errno.FromBizStatus(err), errno.ErrInternal) {
		t.Fatalf("nil service = %v", err)
	}
	var nilH *Handler
	if _, err := nilH.GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{}); !errors.Is(errno.FromBizStatus(err), errno.ErrInternal) {
		t.Fatalf("nil handler = %v", err)
	}
	if err := Run(nil, Config{}); err == nil {
		t.Fatal("Run(nil) must fail")
	}
	if err := Run(New(svc), Config{Addr: "not-an-addr:xx"}); err == nil {
		t.Fatal("bad addr must fail")
	}
}

func TestQueryPaymentHandler(t *testing.T) {
	created := time.Date(2026, 11, 1, 10, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*60*60))
	order := biz.PaymentOrder{
		PayNo: "PA0000000000000000001", OutTradeNo: "T1", Amount: 10000, FeeAmount: 60,
		Currency: "CNY", Subject: "demo", Status: "INIT",
		ExpireAt: created.Add(15 * time.Minute), CreatedAt: created,
	}
	svc := &scripted{order: order}
	h := New(svc)
	ctx := context.Background()
	if _, err := h.QueryPayment(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatalf("nil request = %v", err)
	}
	emptyPay := ""
	if _, err := h.QueryPayment(ctx, &payment.QueryPaymentRequest{MerchantId: "M100002", PayNo: &emptyPay}); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatalf("empty pay_no = %v", err)
	}
	payNo := order.PayNo
	outNo := order.OutTradeNo
	if _, err := h.QueryPayment(ctx, &payment.QueryPaymentRequest{MerchantId: "M100002", PayNo: &payNo, OutTradeNo: &outNo}); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatalf("both keys = %v", err)
	}
	spacedPay := " "
	spacedOut := " "
	if _, err := h.QueryPayment(ctx, &payment.QueryPaymentRequest{MerchantId: "M100002", PayNo: &spacedPay, OutTradeNo: &spacedOut}); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatalf("untrimmed spaces = %v", err)
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("invalid query called biz %d times", n)
	}
	got, err := h.QueryPayment(ctx, &payment.QueryPaymentRequest{MerchantId: "M100002", PayNo: &payNo})
	if err != nil || got.GetPayNo() != order.PayNo || got.GetAmount() != 10000 || got.GetStatus() != "INIT" || got.GetOutTradeNo() != order.OutTradeNo {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if svc.lastQuery().PayNo != payNo || svc.lastQuery().OutTradeNo != "" || svc.lastQuery().MerchantID != "M100002" {
		t.Fatalf("biz input = %+v", svc.lastQuery())
	}
	rawPay := " " + order.PayNo
	if _, err = h.QueryPayment(ctx, &payment.QueryPaymentRequest{MerchantId: "M100002", PayNo: &rawPay}); err != nil {
		t.Fatal(err)
	}
	if svc.lastQuery().PayNo != rawPay {
		t.Fatalf("pay_no was trimmed: %q", svc.lastQuery().PayNo)
	}
	svc.set(func(s *scripted) { s.err = errno.ErrOrderNotFound })
	resp, err := h.QueryPayment(ctx, &payment.QueryPaymentRequest{MerchantId: "M100001", PayNo: &payNo})
	if resp != nil {
		t.Fatalf("30001 carried a response: %+v", resp)
	}
	assertBizCode(t, "not found", err, errno.ErrOrderNotFound.Code)
	if _, err = New(nil).QueryPayment(ctx, &payment.QueryPaymentRequest{MerchantId: "M100002", PayNo: &payNo}); !errors.Is(errno.FromBizStatus(err), errno.ErrInternal) {
		t.Fatalf("nil service = %v", err)
	}
	var nilH *Handler
	if _, err = nilH.QueryPayment(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInternal) {
		t.Fatalf("nil handler = %v", err)
	}
}

func TestListenAddrAndTimeout(t *testing.T) {
	if ListenAddr != ":8881" {
		t.Fatalf("ListenAddr = %s", ListenAddr)
	}
	if ServerTimeout != 2*time.Second {
		t.Fatalf("ServerTimeout = %s", ServerTimeout)
	}
	if PayServerTimeout != 5*time.Second {
		t.Fatalf("PayServerTimeout = %s", PayServerTimeout)
	}
	if (Config{}).timeoutFor("Pay") != PayServerTimeout ||
		(Config{}).timeoutFor("CreateRefund") != PayServerTimeout ||
		(Config{}).timeoutFor("CreatePayment") != ServerTimeout ||
		(Config{}).timeoutFor("QueryPayment") != ServerTimeout ||
		(Config{}).timeoutFor("QueryRefund") != ServerTimeout ||
		(Config{}).timeoutFor("ClosePayment") != ServerTimeout {
		t.Fatal("method timeout split drifted")
	}
	if (Config{Timeout: 80 * time.Millisecond}).timeoutFor("Pay") != 80*time.Millisecond ||
		(Config{Timeout: 80 * time.Millisecond}).timeoutFor("CreateRefund") != 80*time.Millisecond {
		t.Fatal("Config.Timeout must override Pay and CreateRefund")
	}
}

// Pay 的服务端处理超时是 5 秒，短于调用方时调用方收到超时，不是 10003。
func TestPayServerTimeoutFiveSeconds(t *testing.T) {
	svc := &scripted{block: blockUntilDone}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, 8*time.Second)
	start := time.Now()
	_, err := cli.Pay(context.Background(), &payment.PayRequest{MerchantId: "M100001", PayNo: "PA1", Channel: "mockpay"})
	elapsed := time.Since(start)
	if elapsed < 4*time.Second || elapsed > 7500*time.Millisecond {
		t.Fatalf("elapsed %s, want about 5s", elapsed)
	}
	if !isCallTimeout(err) {
		t.Fatalf("want timeout, got %v", err)
	}
	if _, ok := kerrors.FromBizStatusError(err); ok {
		t.Fatalf("timeout became biz status: %v", err)
	}
}

type call struct {
	name string
	fn   func() error
}

func unimplementedCalls(cli paymentservice.Client) []call {
	ctx := context.Background()
	return []call{
		{"QueryTransfer", func() error {
			_, err := cli.QueryTransfer(ctx, &payment.QueryTransferRequest{MerchantId: "M100001"})
			return err
		}},
		{"SyncChannelTxn", func() error {
			_, err := cli.SyncChannelTxn(ctx, &payment.SyncChannelTxnRequest{BizNo: "PA1"})
			return err
		}},
		{"ReplayEvent", func() error {
			_, err := cli.ReplayEvent(ctx, &payment.ReplayEventRequest{})
			return err
		}},
	}
}

func itoa(code int32) string {
	return strconv.Itoa(int(code))
}

func isCallTimeout(err error) bool {
	if kerrors.IsTimeoutError(err) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timed interface{ Timeout() bool }
	if errors.As(err, &timed) && timed.Timeout() {
		return true
	}
	var trans *remote.TransError
	return errors.As(err, &trans) && trans != nil && trans.Error() == kerrors.ErrRPCTimeout.Error()
}

func assertBizCode(t *testing.T, name string, err error, want int32) {
	t.Helper()
	bizErr, ok := kerrors.FromBizStatusError(err)
	if !ok || bizErr == nil {
		t.Fatalf("%s: error = %v, want biz %d", name, err, want)
	}
	if bizErr.BizStatusCode() != want {
		t.Fatalf("%s: code = %d, want %d", name, bizErr.BizStatusCode(), want)
	}
	restored := errno.FromBizStatus(err)
	if restored.Code != want {
		t.Fatalf("%s: restored = %d", name, restored.Code)
	}
	if bizErr.BizMessage() != restored.Error() {
		t.Fatalf("%s: msg = %q, want %q", name, bizErr.BizMessage(), restored.Error())
	}
}

func startServer(t *testing.T, svc Service, cfg Config) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listener = ln
	exit := make(chan error, 1)
	cfg.Exit = exit
	errCh := make(chan error, 1)
	go func() { errCh <- Run(New(svc), cfg) }()
	waitListen(t, ln.Addr().String())
	t.Cleanup(func() {
		select {
		case exit <- errors.New("stop"):
		default:
		}
		select {
		case runErr := <-errCh:
			if runErr != nil && !strings.Contains(runErr.Error(), "stop") {
				t.Errorf("server exit: %v", runErr)
			}
		case <-time.After(3 * time.Second):
			t.Error("server did not exit")
		}
	})
	return ln.Addr().String()
}

func waitListen(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := net.Dialer{Timeout: 50 * time.Millisecond}
	var last error
	for {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		last = err
		if ctx.Err() != nil {
			t.Fatalf("server not listening: %v", last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func dialKitex(t *testing.T, addr string, rpc time.Duration) paymentservice.Client {
	t.Helper()
	cli, err := paymentservice.NewClient("PaymentService",
		client.WithHostPorts(addr),
		client.WithConnectTimeout(time.Second),
		client.WithRPCTimeout(rpc),
		client.WithTransportProtocol(transport.TTHeader),
		client.WithMetaHandler(transmeta.ClientTTHeaderHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cli
}

type scripted struct {
	mu        sync.Mutex
	cred      biz.MerchantCredential
	order     biz.PaymentOrder
	refund    biz.RefundOrder
	transfer  biz.TransferOrder
	err       error
	panicking bool
	block     func(context.Context) error
	calls     int32
	appID     string
	lastIn    biz.CreatePaymentInput
	queryIn   biz.QueryInput
	// refundQueryIn 只给 REF-05 的 QueryRefund 使用。
	refundQueryIn biz.RefundQueryInput
	// refundIn 与 notifyKind 只给 REF-03 的 REFUND 回调分支使用。
	refundIn   biz.RefundResult
	notifyKind string
	transferIn biz.CreateTransferInput
}

func (s *scripted) set(fn func(*scripted)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

func (s *scripted) callCount() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scripted) lastAppID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appID
}

func (s *scripted) GetMerchantCredential(ctx context.Context, appID string) (biz.MerchantCredential, error) {
	s.mu.Lock()
	s.calls++
	s.appID = appID
	cred, err, panicking, block := s.cred, s.err, s.panicking, s.block
	s.mu.Unlock()
	if panicking {
		panic("boom " + testSecret)
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return biz.MerchantCredential{}, berr
		}
	}
	if err != nil {
		return biz.MerchantCredential{}, err
	}
	return cred, nil
}

func (s *scripted) QueryPayment(ctx context.Context, in *biz.QueryInput) (biz.PaymentOrder, error) {
	s.mu.Lock()
	s.calls++
	order, err, panicking, block := s.order, s.err, s.panicking, s.block
	if in != nil {
		s.queryIn = *in
	}
	s.mu.Unlock()
	if panicking {
		panic("boom " + testSecret)
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return biz.PaymentOrder{}, berr
		}
	}
	if err != nil {
		return biz.PaymentOrder{}, err
	}
	return order, nil
}

func (s *scripted) QueryRefund(ctx context.Context, in *biz.RefundQueryInput) (biz.RefundOrder, error) {
	s.mu.Lock()
	s.calls++
	refund, err, panicking, block := s.refund, s.err, s.panicking, s.block
	if in != nil {
		s.refundQueryIn = *in
	}
	s.mu.Unlock()
	if panicking {
		panic("boom " + testSecret)
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return biz.RefundOrder{}, berr
		}
	}
	if err != nil {
		return biz.RefundOrder{}, err
	}
	return refund, nil
}

func (s *scripted) lastRefundQuery() biz.RefundQueryInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refundQueryIn
}

func (s *scripted) lastQuery() biz.QueryInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queryIn
}

func (s *scripted) ClosePayment(ctx context.Context, _ *biz.CloseInput) (biz.PaymentOrder, error) {
	s.mu.Lock()
	s.calls++
	order, err, panicking, block := s.order, s.err, s.panicking, s.block
	s.mu.Unlock()
	if panicking {
		panic("boom " + testSecret)
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return biz.PaymentOrder{}, berr
		}
	}
	if err != nil {
		if errors.Is(err, errno.ErrOrderStatusConflict) {
			return order, err
		}
		return biz.PaymentOrder{}, err
	}
	return order, nil
}

func (s *scripted) Pay(ctx context.Context, _ *biz.PayInput) (biz.PaymentOrder, error) {
	s.mu.Lock()
	s.calls++
	order, err, panicking, block := s.order, s.err, s.panicking, s.block
	s.mu.Unlock()
	if panicking {
		panic("boom " + testSecret)
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return biz.PaymentOrder{}, berr
		}
	}
	if err != nil {
		if errors.Is(err, errno.ErrChannelRejected) {
			return order, err
		}
		return biz.PaymentOrder{}, err
	}
	return order, nil
}

func (s *scripted) CreateRefund(ctx context.Context, _ *biz.CreateRefundInput) (biz.RefundOrder, error) {
	s.mu.Lock()
	s.calls++
	refund, err, panicking, block := s.refund, s.err, s.panicking, s.block
	s.mu.Unlock()
	if panicking {
		panic("boom " + testSecret)
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return biz.RefundOrder{}, berr
		}
	}
	if err != nil {
		if errors.Is(err, errno.ErrInsufficientBalance) || errors.Is(err, errno.ErrChannelRejected) {
			return refund, err
		}
		return biz.RefundOrder{}, err
	}
	return refund, nil
}

func (s *scripted) CreateTransfer(ctx context.Context, in *biz.CreateTransferInput) (biz.TransferOrder, error) {
	s.mu.Lock()
	s.calls++
	transfer, err, panicking, block := s.transfer, s.err, s.panicking, s.block
	if in != nil {
		s.transferIn = *in
	}
	s.mu.Unlock()
	if panicking {
		panic("boom " + testSecret)
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return biz.TransferOrder{}, berr
		}
	}
	if err != nil {
		if errors.Is(err, errno.ErrInsufficientBalance) {
			return transfer, err
		}
		return biz.TransferOrder{}, err
	}
	return transfer, nil
}

func (s *scripted) lastTransferInput() biz.CreateTransferInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transferIn
}

func (s *scripted) CreatePayment(ctx context.Context, in *biz.CreatePaymentInput) (biz.PaymentOrder, error) {
	s.mu.Lock()
	s.calls++
	order, err, panicking, block := s.order, s.err, s.panicking, s.block
	if in != nil {
		s.lastIn = copyPaymentInput(in)
	}
	s.mu.Unlock()
	if panicking {
		panic("boom " + testSecret)
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return biz.PaymentOrder{}, berr
		}
	}
	if err != nil {
		return biz.PaymentOrder{}, err
	}
	return order, nil
}

func (s *scripted) ApplyChannelNotify(ctx context.Context, in biz.PayResult) (string, error) {
	s.mu.Lock()
	s.calls++
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	return "APPLIED", nil
}

func (s *scripted) ApplyRefundNotify(ctx context.Context, in biz.RefundResult) (string, error) {
	s.mu.Lock()
	s.calls++
	s.refundIn = in
	kind, err := s.notifyKind, s.err
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	if kind == "" {
		kind = "APPLIED"
	}
	return kind, nil
}

func (s *scripted) lastRefundResult() biz.RefundResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refundIn
}

func (s *scripted) lastPaymentInput() biz.CreatePaymentInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastIn
}

func copyPaymentInput(in *biz.CreatePaymentInput) biz.CreatePaymentInput {
	out := *in
	if in.Currency != nil {
		v := *in.Currency
		out.Currency = &v
	}
	if in.NotifyURL != nil {
		v := *in.NotifyURL
		out.NotifyURL = &v
	}
	if in.ExpireMinutes != nil {
		v := *in.ExpireMinutes
		out.ExpireMinutes = &v
	}
	return out
}

func TestCreateRefundHandlerReturnsInit(t *testing.T) {
	refund := biz.RefundOrder{
		RefundNo: "RF0000000000000000001", OutRefundNo: "R1", PayNo: "PA_REF_1",
		Amount: 3000, Currency: "CNY", Status: "INIT",
	}
	svc := &scripted{refund: refund}
	payNo := "PA_REF_1"
	got, err := New(svc).CreateRefund(context.Background(), &payment.CreateRefundRequest{
		MerchantId: "M100002", PayNo: &payNo, OutRefundNo: "R1", Amount: 3000,
	})
	if err != nil || got.GetBizCode() != 0 || got.GetRefundOrder().GetStatus() != "INIT" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestCreateRefundHandlerCarriesBalanceAndRejected(t *testing.T) {
	refund := biz.RefundOrder{
		RefundNo: "RF0000000000000000001", OutRefundNo: "R1", PayNo: "PA_REF_1",
		Amount: 1000, Currency: "CNY", Status: "FAILED", FailCode: "INSUFFICIENT_BALANCE",
	}
	svc := &scripted{refund: refund, err: errno.ErrInsufficientBalance}
	payNo := "PA_REF_1"
	req := &payment.CreateRefundRequest{MerchantId: "M100002", PayNo: &payNo, OutRefundNo: "R1", Amount: 1000}
	got, err := New(svc).CreateRefund(context.Background(), req)
	if err != nil || got.GetBizCode() != errno.ErrInsufficientBalance.Code || got.GetRefundOrder().GetRefundNo() != refund.RefundNo {
		t.Fatalf("balance got=%+v err=%v", got, err)
	}
	svc.set(func(s *scripted) {
		s.err = errno.ErrChannelRejected
		s.refund.FailCode = "CHANNEL_REJECTED"
		s.refund.Status = "FAILED"
	})
	got, err = New(svc).CreateRefund(context.Background(), req)
	if err != nil || got.GetBizCode() != errno.ErrChannelRejected.Code || got.GetRefundOrder().GetRefundNo() != refund.RefundNo {
		t.Fatalf("rejected got=%+v err=%v", got, err)
	}
	svc.set(func(s *scripted) {
		s.err = nil
		s.refund.Status = "PROCESSING"
		s.refund.FailCode = ""
	})
	got, err = New(svc).CreateRefund(context.Background(), req)
	if err != nil || got.GetBizCode() != 0 || got.GetRefundOrder().GetStatus() != "PROCESSING" {
		t.Fatalf("processing got=%+v err=%v", got, err)
	}
}

// REF-03：HandleChannelNotify 的 REFUND 分支把回调字段原样交给 ApplyRefundNotify（source = NOTIFY），结果与错误码照传。
func TestHandleChannelNotifyRefund(t *testing.T) {
	svc := &scripted{notifyKind: "DUPLICATE"}
	h := New(svc)
	req := &payment.HandleChannelNotifyRequest{
		Channel: "mockpay", NotifyId: "CN0000000123456789012", TxnType: "REFUND",
		ReqNo: "RF0000000000000000001", ChannelTxnNo: "MC0000000000000000001",
		Amount: 3000, Status: "SUCCESS", FinishedAt: "2026-11-02T03:04:05.123+08:00",
	}
	got, err := h.HandleChannelNotify(context.Background(), req)
	if err != nil || got.GetResult_() != "DUPLICATE" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	in := svc.lastRefundResult()
	want := time.Date(2026, 11, 2, 3, 4, 5, 123000000, time.FixedZone("", 8*60*60))
	if in.RefundNo != req.ReqNo || in.Result != "SUCCESS" || in.Source != "NOTIFY" || in.ChannelTxnNo != req.ChannelTxnNo ||
		in.Amount == nil || *in.Amount != 3000 || !in.FinishedAt.Equal(want) || in.FailCode != "" {
		t.Fatalf("refund input = %+v", in)
	}

	svc.set(func(s *scripted) { s.err = errno.ErrChannelAmountMismatch })
	_, err = h.HandleChannelNotify(context.Background(), req)
	assertBizCode(t, "mismatch", err, errno.ErrChannelAmountMismatch.Code)
	svc.set(func(s *scripted) { s.err = errno.ErrInternal })
	_, err = h.HandleChannelNotify(context.Background(), req)
	assertBizCode(t, "internal", err, errno.ErrInternal.Code)

	before := svc.callCount()
	for name, bad := range map[string]func(*payment.HandleChannelNotifyRequest){
		"txn_type":    func(r *payment.HandleChannelNotifyRequest) { r.TxnType = "TRANSFER" },
		"status":      func(r *payment.HandleChannelNotifyRequest) { r.Status = "PROCESSING" },
		"req_no":      func(r *payment.HandleChannelNotifyRequest) { r.ReqNo = "" },
		"finished_at": func(r *payment.HandleChannelNotifyRequest) { r.FinishedAt = "2026-11-02 03:04:05" },
	} {
		copied := *req
		bad(&copied)
		_, err = h.HandleChannelNotify(context.Background(), &copied)
		assertBizCode(t, name, err, errno.ErrInvalidParam.Code)
	}
	if svc.callCount() != before {
		t.Fatalf("invalid requests reached biz: %d -> %d", before, svc.callCount())
	}
}

func TestPayHandlerCarriesChannelRejected(t *testing.T) {
	created := time.Date(2026, 10, 28, 10, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*60*60))
	order := biz.PaymentOrder{
		PayNo: "PA0000000000000000001", OutTradeNo: "T1", Amount: 10000, FeeAmount: 60,
		Currency: "CNY", Subject: "demo", Status: "FAILED", FailCode: "CHANNEL_REJECTED",
		ExpireAt: created.Add(15 * time.Minute), CreatedAt: created,
	}
	svc := &scripted{order: order, err: errno.ErrChannelRejected}
	got, err := New(svc).Pay(context.Background(), &payment.PayRequest{
		MerchantId: "M100002", PayNo: order.PayNo, Channel: "mockpay",
	})
	if err != nil || got.GetBizCode() != errno.ErrChannelRejected.Code || got.GetPaymentOrder().GetPayNo() != order.PayNo ||
		got.GetPaymentOrder().GetStatus() != "FAILED" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	svc.set(func(s *scripted) { s.err = errno.ErrOrderNotFound })
	resp, err := New(svc).Pay(context.Background(), &payment.PayRequest{
		MerchantId: "M100002", PayNo: order.PayNo, Channel: "mockpay",
	})
	if resp != nil {
		t.Fatalf("30001 carried a response: %+v", resp)
	}
	assertBizCode(t, "not found", err, errno.ErrOrderNotFound.Code)
}

func TestClosePaymentHandlerCarriesStatusConflict(t *testing.T) {
	created := time.Date(2026, 11, 1, 10, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*60*60))
	order := biz.PaymentOrder{
		PayNo: "PA0000000000000000001", OutTradeNo: "T1", Amount: 10008, FeeAmount: 60,
		Currency: "CNY", Subject: "demo", Status: "SUCCESS",
		ExpireAt: created.Add(15 * time.Minute), CreatedAt: created, PaidAt: created,
	}
	svc := &scripted{order: order, err: errno.ErrOrderStatusConflict}
	got, err := New(svc).ClosePayment(context.Background(), &payment.ClosePaymentRequest{
		MerchantId: "M100002", PayNo: order.PayNo,
	})
	if err != nil || got.GetBizCode() != errno.ErrOrderStatusConflict.Code || got.GetPaymentOrder().GetPayNo() != order.PayNo ||
		got.GetPaymentOrder().GetStatus() != "SUCCESS" || got.GetPaymentOrder().GetAmount() != 10008 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	for _, code := range []*errno.Error{errno.ErrOrderNotFound, errno.ErrUnavailable, errno.ErrChannelAmountMismatch} {
		svc.set(func(s *scripted) { s.err = code })
		resp, callErr := New(svc).ClosePayment(context.Background(), &payment.ClosePaymentRequest{
			MerchantId: "M100002", PayNo: order.PayNo,
		})
		if resp != nil {
			t.Fatalf("%d carried a response: %+v", code.Code, resp)
		}
		assertBizCode(t, code.Error(), callErr, code.Code)
	}
}

func TestCreatePaymentHandlerPassesOptionals(t *testing.T) {
	created := time.Date(2026, 10, 17, 10, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*60*60))
	svc := &scripted{order: biz.PaymentOrder{
		PayNo: "PA0000000000000000001", OutTradeNo: "T1", Amount: 10000, FeeAmount: 60,
		Currency: "CNY", Subject: "demo", Status: "INIT",
		ExpireAt: created.Add(15 * time.Minute), CreatedAt: created,
	}}
	h := New(svc)
	zero := int32(0)
	raw := "http://alice@merchant-demo/default"
	cny := "CNY"
	got, err := h.CreatePayment(context.Background(), &payment.CreatePaymentRequest{
		MerchantId: "M100002", OutTradeNo: "T1", Amount: 10000, Subject: "demo",
		Currency: &cny, NotifyUrl: &raw, ExpireMinutes: &zero,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetPayNo() != "PA0000000000000000001" || got.GetFeeAmount() != 60 || got.GetStatus() != "INIT" ||
		got.GetAmount() != 10000 || got.IsSetChannel() || got.IsSetPaidAt() || got.IsSetFailCode() || got.IsSetFailMsg() {
		t.Fatalf("order = %+v", got)
	}
	if got.GetExpireAt() != "2026-10-17T10:15:00.000+08:00" || got.GetCreatedAt() != "2026-10-17T10:00:00.000+08:00" {
		t.Fatalf("times expire=%s created=%s", got.GetExpireAt(), got.GetCreatedAt())
	}
	last := svc.lastPaymentInput()
	if last.ExpireMinutes == nil || *last.ExpireMinutes != 0 || last.NotifyURL == nil || *last.NotifyURL != raw ||
		last.Currency == nil || *last.Currency != cny {
		t.Fatalf("optional pointers were dropped: %+v", last)
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func (l *lockedBuffer) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.Reset()
}

type accessRecord struct {
	path    string
	errCode string
}

type accessLog struct {
	mu   sync.Mutex
	rows []accessRecord
}

func (a *accessLog) record(_ context.Context, path string, _ int, _ time.Duration, _, errCode string) {
	a.mu.Lock()
	a.rows = append(a.rows, accessRecord{path: path, errCode: errCode})
	a.mu.Unlock()
}

func (a *accessLog) last() accessRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.rows) == 0 {
		return accessRecord{}
	}
	return a.rows[len(a.rows)-1]
}

type tracerFunc func(context.Context, string) (context.Context, func(error))

func (f tracerFunc) Start(ctx context.Context, method string) (context.Context, func(error)) {
	return f(ctx, method)
}

type metricsFunc func(string, int32, time.Duration)

func (f metricsFunc) Observe(method string, bizCode int32, elapsed time.Duration) {
	f(method, bizCode, elapsed)
}

// TRF-01 规则 6：TRYING 与其他非 40002 结果 biz_code 为 0 并携带转账单；字段按 §5.3 转换，可选字段只在有值时设置。
func TestCreateTransferHandler(t *testing.T) {
	created := time.Date(2026, 11, 6, 10, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*60*60))
	svc := &scripted{transfer: biz.TransferOrder{
		TransferNo: "TF0000000000000000001", OutTransferNo: "TR1", MerchantID: "M100001", PayeeMerchantID: "M100002",
		Amount: 500, Currency: "CNY", Mode: "TCC", Status: "TRYING", CreatedAt: created,
	}}
	h := New(svc)
	remark := "货款结算"
	notify := "http://merchant-demo:9100/notify"
	got, err := h.CreateTransfer(context.Background(), &payment.CreateTransferRequest{
		MerchantId: "M100001", OutTransferNo: "TR1", PayeeMerchantId: "M100002", Amount: 500,
		Remark: &remark, NotifyUrl: &notify,
	})
	if err != nil || got.GetBizCode() != 0 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	tr := got.GetTransfer()
	if tr.GetTransferNo() != "TF0000000000000000001" || tr.GetOutTransferNo() != "TR1" || tr.GetMerchantId() != "M100001" ||
		tr.GetPayeeMerchantId() != "M100002" || tr.GetAmount() != 500 || tr.GetCurrency() != "CNY" ||
		tr.GetMode() != "TCC" || tr.GetStatus() != "TRYING" || tr.IsSetFailCode() || tr.IsSetFinishedAt() {
		t.Fatalf("transfer = %+v", tr)
	}
	if tr.GetCreatedAt() != "2026-11-06T10:00:00.000+08:00" {
		t.Fatalf("created_at = %s", tr.GetCreatedAt())
	}
	in := svc.lastTransferInput()
	if in.MerchantID != "M100001" || in.OutTransferNo != "TR1" || in.PayeeMerchantID != "M100002" || in.Amount != 500 ||
		in.Remark == nil || *in.Remark != remark || in.NotifyURL == nil || *in.NotifyURL != notify {
		t.Fatalf("input = %+v", in)
	}
	// 未传的可选字段保持 nil，不变成空串。
	if _, err := h.CreateTransfer(context.Background(), &payment.CreateTransferRequest{
		MerchantId: "M100001", OutTransferNo: "TR2", PayeeMerchantId: "M100002", Amount: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if in := svc.lastTransferInput(); in.Remark != nil || in.NotifyURL != nil {
		t.Fatalf("optionals = %+v", in)
	}
	// 其他 FAILED（TRY_TIMEOUT）也是 biz_code 0，并带 fail_code、finished_at。
	finished := created.Add(42 * time.Millisecond)
	svc.set(func(s *scripted) {
		s.transfer.Status = "FAILED"
		s.transfer.FailCode = "TRY_TIMEOUT"
		s.transfer.FinishedAt = finished
	})
	got, err = h.CreateTransfer(context.Background(), &payment.CreateTransferRequest{
		MerchantId: "M100001", OutTransferNo: "TR1", PayeeMerchantId: "M100002", Amount: 500,
	})
	if err != nil || got.GetBizCode() != 0 || got.GetTransfer().GetFailCode() != "TRY_TIMEOUT" ||
		got.GetTransfer().GetFinishedAt() != "2026-11-06T10:00:00.042+08:00" {
		t.Fatalf("failed got=%+v err=%v", got, err)
	}
}

// TRF-01 规则 6：40002 放在 biz_code，error 为 nil，并携带已失败的转账单；其他业务错误走 BizStatusError，不带转账单。
func TestCreateTransferHandlerErrors(t *testing.T) {
	failed := biz.TransferOrder{
		TransferNo: "TF0000000000000000002", OutTransferNo: "TR2", MerchantID: "M100001", PayeeMerchantID: "M100002",
		Amount: 9000000, Currency: "CNY", Mode: "TCC", Status: "FAILED", FailCode: "INSUFFICIENT_BALANCE",
	}
	svc := &scripted{transfer: failed, err: errno.ErrInsufficientBalance}
	req := &payment.CreateTransferRequest{MerchantId: "M100001", OutTransferNo: "TR2", PayeeMerchantId: "M100002", Amount: 9000000}
	got, err := New(svc).CreateTransfer(context.Background(), req)
	if err != nil || got.GetBizCode() != errno.ErrInsufficientBalance.Code ||
		got.GetTransfer().GetTransferNo() != failed.TransferNo || got.GetTransfer().GetFailCode() != "INSUFFICIENT_BALANCE" {
		t.Fatalf("40002 got=%+v err=%v", got, err)
	}
	for _, want := range []*errno.Error{errno.ErrInvalidParam, errno.ErrIdempotentConflict, errno.ErrPayeeInvalid, errno.ErrInternal} {
		svc.set(func(s *scripted) { s.err = want })
		got, err := New(svc).CreateTransfer(context.Background(), req)
		if got != nil || !errors.Is(errno.FromBizStatus(err), want) {
			t.Fatalf("%d: got=%+v err=%v", want.Code, got, err)
		}
	}
	if _, err := New(nil).CreateTransfer(context.Background(), req); !errors.Is(errno.FromBizStatus(err), errno.ErrInternal) {
		t.Fatalf("nil service = %v", err)
	}
}

// CreateTransfer 经 Kitex：30008 是 BizStatusError；40002 在 biz_code 里并携带转账单，error 为 nil。
func TestKitexCreateTransfer(t *testing.T) {
	svc := &scripted{err: errno.ErrPayeeInvalid}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, time.Second)
	req := &payment.CreateTransferRequest{MerchantId: "M100001", OutTransferNo: "TR1", PayeeMerchantId: "M100001", Amount: 1}
	_, err := cli.CreateTransfer(context.Background(), req)
	assertBizCode(t, "payee invalid", err, errno.ErrPayeeInvalid.Code)
	svc.set(func(s *scripted) {
		s.err = errno.ErrInsufficientBalance
		s.transfer = biz.TransferOrder{TransferNo: "TF1", Status: "FAILED", FailCode: "INSUFFICIENT_BALANCE", Mode: "TCC"}
	})
	got, err := cli.CreateTransfer(context.Background(), req)
	if err != nil || got.GetBizCode() != errno.ErrInsufficientBalance.Code || got.GetTransfer().GetTransferNo() != "TF1" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

// REF-05：QueryRefund 二选一不合法返回 10001，不进 biz；空串算没提供，不 trim。
// 成功时 error 为 nil 并返回 RefundOrder；30001 走 BizStatusError，不带退款单。
func TestQueryRefundHandler(t *testing.T) {
	created := time.Date(2026, 11, 6, 10, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*60*60))
	refund := biz.RefundOrder{
		RefundNo: "RF0000000000000000001", MerchantID: "M100002", OutRefundNo: "R1", PayNo: "PA0000000000000000001",
		Amount: 3000, Currency: "CNY", Reason: "demo", Status: "FAILED",
		FailCode: "CHANNEL_FAILED", FailMsg: "mock fail", CreatedAt: created,
	}
	svc := &scripted{refund: refund}
	h := New(svc)
	ctx := context.Background()
	if _, err := h.QueryRefund(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatalf("nil request = %v", err)
	}
	refundNo := refund.RefundNo
	outNo := refund.OutRefundNo
	empty := ""
	spaced := " "
	for name, req := range map[string]*payment.QueryRefundRequest{
		"none":   {MerchantId: "M100002"},
		"empty":  {MerchantId: "M100002", RefundNo: &empty},
		"both":   {MerchantId: "M100002", RefundNo: &refundNo, OutRefundNo: &outNo},
		"spaces": {MerchantId: "M100002", RefundNo: &spaced, OutRefundNo: &spaced},
	} {
		if _, err := h.QueryRefund(ctx, req); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
			t.Fatalf("%s = %v", name, err)
		}
	}
	if n := svc.callCount(); n != 0 {
		t.Fatalf("invalid query called biz %d times", n)
	}
	got, err := h.QueryRefund(ctx, &payment.QueryRefundRequest{MerchantId: "M100002", RefundNo: &refundNo})
	if err != nil || got.GetRefundNo() != refund.RefundNo || got.GetOutRefundNo() != "R1" || got.GetPayNo() != refund.PayNo ||
		got.GetAmount() != 3000 || got.GetStatus() != "FAILED" || got.GetFailCode() != "CHANNEL_FAILED" ||
		got.GetFailMsg() != "mock fail" || got.IsSetSucceededAt() || got.GetCreatedAt() != "2026-11-06T10:00:00.000+08:00" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if in := svc.lastRefundQuery(); in.MerchantID != "M100002" || in.RefundNo != refundNo || in.OutRefundNo != "" {
		t.Fatalf("biz input = %+v", in)
	}
	rawOut := " " + outNo
	if _, err = h.QueryRefund(ctx, &payment.QueryRefundRequest{MerchantId: "M100002", OutRefundNo: &rawOut}); err != nil {
		t.Fatal(err)
	}
	if in := svc.lastRefundQuery(); in.OutRefundNo != rawOut || in.RefundNo != "" {
		t.Fatalf("out_refund_no was trimmed: %+v", in)
	}
	svc.set(func(s *scripted) { s.err = errno.ErrOrderNotFound })
	resp, err := h.QueryRefund(ctx, &payment.QueryRefundRequest{MerchantId: "M100001", RefundNo: &refundNo})
	if resp != nil {
		t.Fatalf("30001 carried a response: %+v", resp)
	}
	assertBizCode(t, "not found", err, errno.ErrOrderNotFound.Code)
	if _, err = New(nil).QueryRefund(ctx, &payment.QueryRefundRequest{MerchantId: "M100002", RefundNo: &refundNo}); !errors.Is(errno.FromBizStatus(err), errno.ErrInternal) {
		t.Fatalf("nil service = %v", err)
	}
	var nilH *Handler
	if _, err = nilH.QueryRefund(ctx, nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInternal) {
		t.Fatalf("nil handler = %v", err)
	}
}

// REF-05 验收 1（RPC）：经 Kitex 调用，越权查询收到业务码 30001，没有退款单；访问日志记 30001。
func TestQueryRefundRPCOtherMerchant(t *testing.T) {
	svc := &scripted{err: errno.ErrOrderNotFound}
	access := &accessLog{}
	addr := startServer(t, svc, Config{Access: access.record})
	cli := dialKitex(t, addr, time.Second)
	refundNo := "RF0000000000000000001"
	resp, err := cli.QueryRefund(context.Background(), &payment.QueryRefundRequest{MerchantId: "M100001", RefundNo: &refundNo})
	assertBizCode(t, "QueryRefund", err, errno.ErrOrderNotFound.Code)
	if resp != nil && resp.GetRefundNo() != "" {
		t.Fatalf("30001 carried a refund: %+v", resp)
	}
	if rec := access.last(); rec.path != "QueryRefund" || rec.errCode != itoa(errno.ErrOrderNotFound.Code) {
		t.Fatalf("access = %+v", rec)
	}
	if in := svc.lastRefundQuery(); in.MerchantID != "M100001" || in.RefundNo != refundNo {
		t.Fatalf("biz input = %+v", in)
	}
}
