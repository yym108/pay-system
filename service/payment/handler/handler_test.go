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
	for name, fn := range map[string]func() error{
		"CreatePayment":       func() error { _, err := h.CreatePayment(ctx, nil); return err },
		"Pay":                 func() error { _, err := h.Pay(ctx, nil); return err },
		"ClosePayment":        func() error { _, err := h.ClosePayment(ctx, nil); return err },
		"QueryPayment":        func() error { _, err := h.QueryPayment(ctx, nil); return err },
		"CreateRefund":        func() error { _, err := h.CreateRefund(ctx, nil); return err },
		"QueryRefund":         func() error { _, err := h.QueryRefund(ctx, nil); return err },
		"CreateTransfer":      func() error { _, err := h.CreateTransfer(ctx, nil); return err },
		"QueryTransfer":       func() error { _, err := h.QueryTransfer(ctx, nil); return err },
		"HandleChannelNotify": func() error { _, err := h.HandleChannelNotify(ctx, nil); return err },
		"SyncChannelTxn":      func() error { _, err := h.SyncChannelTxn(ctx, nil); return err },
		"ReplayEvent":         func() error { _, err := h.ReplayEvent(ctx, nil); return err },
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

func TestListenAddrAndTimeout(t *testing.T) {
	if ListenAddr != ":8881" {
		t.Fatalf("ListenAddr = %s", ListenAddr)
	}
	if ServerTimeout != 2*time.Second {
		t.Fatalf("ServerTimeout = %s", ServerTimeout)
	}
}

type call struct {
	name string
	fn   func() error
}

func unimplementedCalls(cli paymentservice.Client) []call {
	ctx := context.Background()
	return []call{
		{"CreatePayment", func() error {
			_, err := cli.CreatePayment(ctx, &payment.CreatePaymentRequest{MerchantId: "M100001", OutTradeNo: "o1", Amount: 1, Subject: "s"})
			return err
		}},
		{"Pay", func() error {
			_, err := cli.Pay(ctx, &payment.PayRequest{MerchantId: "M100001", PayNo: "PA1", Channel: "mockpay"})
			return err
		}},
		{"ClosePayment", func() error {
			_, err := cli.ClosePayment(ctx, &payment.ClosePaymentRequest{MerchantId: "M100001", PayNo: "PA1"})
			return err
		}},
		{"QueryPayment", func() error {
			_, err := cli.QueryPayment(ctx, &payment.QueryPaymentRequest{MerchantId: "M100001"})
			return err
		}},
		{"CreateRefund", func() error {
			_, err := cli.CreateRefund(ctx, &payment.CreateRefundRequest{MerchantId: "M100001", OutRefundNo: "r1", Amount: 1})
			return err
		}},
		{"QueryRefund", func() error {
			_, err := cli.QueryRefund(ctx, &payment.QueryRefundRequest{MerchantId: "M100001"})
			return err
		}},
		{"CreateTransfer", func() error {
			_, err := cli.CreateTransfer(ctx, &payment.CreateTransferRequest{MerchantId: "M100001", OutTransferNo: "t1", PayeeMerchantId: "M100002", Amount: 1})
			return err
		}},
		{"QueryTransfer", func() error {
			_, err := cli.QueryTransfer(ctx, &payment.QueryTransferRequest{MerchantId: "M100001"})
			return err
		}},
		{"HandleChannelNotify", func() error {
			_, err := cli.HandleChannelNotify(ctx, &payment.HandleChannelNotifyRequest{Channel: "mockpay", NotifyId: "n1", TxnType: "PAY", ReqNo: "PA1", Status: "SUCCESS"})
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
	err       error
	panicking bool
	block     func(context.Context) error
	calls     int32
	appID     string
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
