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

	"github.com/yym108/pay-system/pkg/logx"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/kitex_gen/account/accountservice"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/biz"
)

// blockUntilDone 等到 ctx 结束，并把原因交回 handler。真实 biz 也是这样返回取消的。
func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// ACC-11 验收 1：真实 Kitex 客户端调用全部方法，错误码透传。
func TestKitexErrorPassthrough(t *testing.T) {
	svc := &scripted{}
	access := &accessLog{}
	addr := startServer(t, svc, Config{Access: access.record})
	cli := dialKitex(t, addr, time.Second)

	codes := []*errno.Error{
		errno.ErrInvalidParam,
		errno.ErrInternal,
		errno.ErrUnavailable,
		errno.ErrIdempotentConflict,
		errno.ErrAccountNotFound,
		errno.ErrInsufficientBalance,
		errno.ErrFreezeStatusConflict,
		errno.ErrAccountInactive,
	}
	for _, code := range codes {
		svc.mu.Lock()
		svc.err = code
		svc.mu.Unlock()
		for _, call := range allCalls(cli) {
			err := call.fn()
			assertBizCode(t, call.name+" "+code.Error(), err, code.Code)
			if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "select ") {
				t.Fatalf("%s leaked internals: %q", call.name, err.Error())
			}
			rec := access.last()
			wantCode := strconv.Itoa(int(code.Code))
			if rec.path != call.name || rec.errCode != wantCode {
				t.Fatalf("access %s = %+v, want path %s err_code %s", call.name, rec, call.name, wantCode)
			}
		}
	}

	svc.mu.Lock()
	svc.err = errors.New("password=secret select * from account")
	svc.mu.Unlock()
	for _, call := range allCalls(cli) {
		err := call.fn()
		assertBizCode(t, call.name+" unknown", err, errno.ErrInternal.Code)
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "select") {
			t.Fatalf("%s leaked unknown error: %q", call.name, err.Error())
		}
	}

	// Reverse 不看 biz，任何调用都是 10003，且不进入 svc。
	svc.mu.Lock()
	svc.calls = 0
	svc.mu.Unlock()
	_, err := cli.Reverse(context.Background(), &account.ReverseRequest{Shard: 0, TxnNo: "TX1", Reason: "x"})
	assertBizCode(t, "Reverse", err, errno.ErrInternal.Code)
	if n := svc.callCount(); n != 0 {
		t.Fatalf("Reverse called biz %d times", n)
	}
	if access.count() == 0 {
		t.Fatal("access log was not written")
	}
}

// ACC-11：10003 的原因进 ERROR 日志，不进响应。登记过的业务错误不打 ERROR。
func TestInternalCauseLoggedNotReturned(t *testing.T) {
	const cause = "shard lookup failed"
	buf := &lockedBuffer{}
	logx.SetDefault("account", buf)
	t.Cleanup(func() { logx.SetDefault("account", io.Discard) })

	svc := &scripted{}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, time.Second)
	svc.mu.Lock()
	svc.err = errors.New(cause)
	svc.mu.Unlock()

	_, err := cli.GetAccount(context.Background(), &account.GetAccountRequest{
		OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE",
	})
	assertBizCode(t, "unknown", err, errno.ErrInternal.Code)
	if strings.Contains(err.Error(), cause) {
		t.Fatalf("response leaked cause: %q", err.Error())
	}
	logged := buf.String()
	if !strings.Contains(logged, `"level":"ERROR"`) || !strings.Contains(logged, cause) || !strings.Contains(logged, `"path":"GetAccount"`) {
		t.Fatalf("ERROR log missing cause: %s", logged)
	}
	if strings.Contains(err.Error(), "select") {
		t.Fatalf("response msg = %q", err.Error())
	}

	buf.Reset()
	svc.mu.Lock()
	svc.err = errno.ErrAccountNotFound
	svc.mu.Unlock()
	_, err = cli.GetAccount(context.Background(), &account.GetAccountRequest{
		OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE",
	})
	assertBizCode(t, "registered", err, errno.ErrAccountNotFound.Code)
	if strings.Contains(buf.String(), `"level":"ERROR"`) {
		t.Fatalf("registered business error logged ERROR: %s", buf.String())
	}
}

// ACC-11 验收 1：成功响应字段透传。余额不足和核对失败是响应对象，不是 BizStatusError，也不是 biz_code。
func TestKitexSuccessAndObjectFields(t *testing.T) {
	shardNo := 0
	svc := &scripted{
		account: biz.Account{
			AccountNo: "AC0000000000000000001", OwnerType: "MERCHANT", OwnerID: "M100002",
			AccountType: "MERCHANT_SETTLE", Shard: 0, SubNo: 0, Currency: "CNY",
			BalanceDirection: "CREDIT", Balance: 100, FrozenAmount: 40, AvailableAmount: 60,
			Status: "ACTIVE", SubCount: 1,
		},
		freeze:  biz.FreezeResult{FreezeNo: "FZ1", Status: "FROZEN", Duplicated: true},
		confirm: biz.ConfirmResult{FreezeNo: "FZ1", TxnNo: "TX1", Duplicated: true},
		local:   biz.TransferLocalResult{Status: "FAILED", FailCode: "INSUFFICIENT_BALANCE"},
		in:      biz.TransferInResult{Status: "TRIED"},
		check: biz.CheckBalanceResult{Passed: false, Failures: []biz.BalanceFailure{{
			Check: "TRANSIT_ZERO", Expected: 0, Actual: 3,
		}}},
	}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, time.Second)
	ctx := context.Background()

	opened, err := cli.OpenAccount(ctx, &account.OpenAccountRequest{
		OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE",
	})
	if err != nil {
		t.Fatal(err)
	}
	if opened.GetAccountNo() != svc.account.AccountNo || !opened.IsSetShard() || opened.GetShard() != 0 || !opened.IsSetSubNo() || opened.GetSubNo() != 0 {
		t.Fatalf("open account shard/sub not preserved: %+v", opened)
	}

	svc.mu.Lock()
	svc.account.AccountNo = ""
	svc.account.Shard = 0
	svc.account.SubNo = 0
	svc.account.SubCount = 4
	svc.mu.Unlock()
	sum, err := cli.GetAccount(ctx, &account.GetAccountRequest{OwnerType: "PLATFORM", OwnerId: "PLATFORM", AccountType: "TRANSIT"})
	if err != nil {
		t.Fatal(err)
	}
	if sum.GetAccountNo() != "" || sum.IsSetShard() || sum.IsSetSubNo() || sum.GetSubCount() != 4 {
		t.Fatalf("platform summary must omit shard and sub_no: %+v", sum)
	}

	fr, err := cli.Freeze(ctx, &account.FreezeRequest{BizType: "REFUND", BizNo: "RF1", MerchantId: "M100002", Amount: 10})
	if err != nil || fr.GetFreezeNo() != "FZ1" || fr.GetStatus() != "FROZEN" {
		t.Fatalf("freeze = %+v err=%v", fr, err)
	}
	rel, err := cli.ReleaseFreeze(ctx, &account.ReleaseFreezeRequest{BizType: "REFUND", BizNo: "RF1", MerchantId: "M100002", Amount: 10})
	if err != nil || rel.GetStatus() != "FROZEN" {
		t.Fatalf("release = %+v err=%v", rel, err)
	}
	cf, err := cli.ConfirmFreeze(ctx, &account.ConfirmFreezeRequest{BizType: "TRANSFER", BizNo: "TF1", MerchantId: "M100002"})
	if err != nil || cf.GetTxnNo() != "TX1" {
		t.Fatalf("confirm = %+v err=%v", cf, err)
	}

	local, err := cli.TransferLocal(ctx, &account.TransferLocalRequest{
		TransferNo: "TF1", PayerMerchantId: "M100002", PayeeMerchantId: "M100004", Amount: 10,
	})
	if err != nil {
		t.Fatalf("insufficient balance must not be an RPC error: %v", err)
	}
	if local.GetStatus() != "FAILED" || local.GetFailCode() != "INSUFFICIENT_BALANCE" || local.IsSetTxnNo() {
		t.Fatalf("local = %+v", local)
	}

	tried, err := cli.TransferInTry(ctx, &account.TransferInTryRequest{TransferNo: "TF2", PayeeMerchantId: "M100004", Amount: 10})
	if err != nil || tried.GetStatus() != "TRIED" {
		t.Fatalf("try = %+v err=%v", tried, err)
	}
	svc.mu.Lock()
	svc.in.Status = "CONFIRMED"
	svc.in.TxnNo = "TX9"
	svc.mu.Unlock()
	confirmed, err := cli.TransferInConfirm(ctx, &account.TransferInConfirmRequest{TransferNo: "TF2", PayeeMerchantId: "M100004"})
	if err != nil || confirmed.GetStatus() != "CONFIRMED" || confirmed.GetTxnNo() != "TX9" {
		t.Fatalf("confirm in = %+v err=%v", confirmed, err)
	}
	svc.mu.Lock()
	svc.in.Status = "CANCELLED"
	svc.in.TxnNo = ""
	svc.mu.Unlock()
	cancelled, err := cli.TransferInCancel(ctx, &account.TransferInCancelRequest{TransferNo: "TF3", PayeeMerchantId: "M100004", Amount: 10})
	if err != nil || cancelled.GetStatus() != "CANCELLED" {
		t.Fatalf("cancel = %+v err=%v", cancelled, err)
	}

	checked, err := cli.CheckBalance(ctx, &account.CheckBalanceRequest{})
	if err != nil {
		t.Fatalf("passed=false must not be an RPC error: %v", err)
	}
	if checked.GetPassed() || len(checked.GetFailures()) != 1 || checked.GetFailures()[0].IsSetShard() || checked.GetFailures()[0].IsSetAccountNo() {
		t.Fatalf("check = %+v", checked)
	}
	if checked.GetFailures()[0].GetCheck() != "TRANSIT_ZERO" || checked.GetFailures()[0].GetActual() != 3 {
		t.Fatalf("failure = %+v", checked.GetFailures()[0])
	}

	svc.mu.Lock()
	svc.check.Failures = []biz.BalanceFailure{{
		Shard: &shardNo, AccountNo: "AC1", Check: "BALANCE_SUM", Expected: 1, Actual: 2,
	}}
	svc.mu.Unlock()
	checked, err = cli.CheckBalance(ctx, &account.CheckBalanceRequest{})
	if err != nil || !checked.GetFailures()[0].IsSetShard() || checked.GetFailures()[0].GetShard() != 0 || checked.GetFailures()[0].GetAccountNo() != "AC1" {
		t.Fatalf("shard 0 must be present when set: %+v err=%v", checked, err)
	}
}

// ACC-11 验收 2：handler panic 不导致进程退出，调用方收到 10003，之后的调用仍然成功。
func TestHandlerPanicReturns10003AndServerStaysUp(t *testing.T) {
	svc := &scripted{panicMethod: "GetAccount", account: biz.Account{
		AccountNo: "AC1", OwnerType: "MERCHANT", OwnerID: "M100002", AccountType: "MERCHANT_SETTLE",
		Shard: 1, SubNo: 0, Status: "ACTIVE", SubCount: 1,
	}}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, time.Second)
	_, err := cli.GetAccount(context.Background(), &account.GetAccountRequest{OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE"})
	assertBizCode(t, "panic", err, errno.ErrInternal.Code)
	if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "boom") {
		t.Fatalf("panic value leaked: %q", err.Error())
	}
	svc.mu.Lock()
	svc.panicMethod = ""
	svc.mu.Unlock()
	got, err := cli.GetAccount(context.Background(), &account.GetAccountRequest{OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE"})
	if err != nil || got.GetAccountNo() != "AC1" || !got.IsSetShard() || got.GetShard() != 1 {
		t.Fatalf("server not usable after panic: %+v err=%v", got, err)
	}
}

// ACC-11：服务端处理超时 2 秒。调用方收到超时，不是 10003。
func TestServerTimeoutTwoSeconds(t *testing.T) {
	svc := &scripted{block: blockUntilDone}
	addr := startServer(t, svc, Config{})
	cli := dialKitex(t, addr, 5*time.Second)
	start := time.Now()
	_, err := cli.GetAccount(context.Background(), &account.GetAccountRequest{OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE"})
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
	svc.mu.Lock()
	svc.block = nil
	svc.account = biz.Account{AccountNo: "AC1", OwnerType: "MERCHANT", OwnerID: "M100002", AccountType: "MERCHANT_SETTLE", Status: "ACTIVE", SubCount: 1}
	svc.mu.Unlock()
	if _, err = cli.GetAccount(context.Background(), &account.GetAccountRequest{OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE"}); err != nil {
		t.Fatalf("server not usable after timeout: %v", err)
	}
}

func TestServerTimeoutCanBeShortForTests(t *testing.T) {
	svc := &scripted{block: blockUntilDone}
	addr := startServer(t, svc, Config{Timeout: 80 * time.Millisecond})
	cli := dialKitex(t, addr, time.Second)
	start := time.Now()
	_, err := cli.Freeze(context.Background(), &account.FreezeRequest{BizType: "REFUND", BizNo: "RF", MerchantId: "M100002", Amount: 1})
	elapsed := time.Since(start)
	if !isCallTimeout(err) || elapsed > 700*time.Millisecond {
		t.Fatalf("elapsed %s err=%v", elapsed, err)
	}
}

func TestHooksAndNilRequest(t *testing.T) {
	var methods []string
	var mu sync.Mutex
	tr := tracerFunc(func(ctx context.Context, method string) (context.Context, func(error)) {
		mu.Lock()
		methods = append(methods, method)
		mu.Unlock()
		return ctx, func(error) {}
	})
	var observed atomic.Int32
	mt := metricsFunc(func(string, int32, time.Duration) { observed.Add(1) })
	svc := &scripted{err: errno.ErrAccountNotFound}
	addr := startServer(t, svc, Config{Tracer: tr, Metrics: mt})
	cli := dialKitex(t, addr, time.Second)
	_, _ = cli.GetAccount(context.Background(), &account.GetAccountRequest{OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE"})
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 1 || methods[0] != "GetAccount" || observed.Load() != 1 {
		t.Fatalf("hooks methods=%v observed=%d", methods, observed.Load())
	}

	h := New(svc)
	if _, err := h.OpenAccount(context.Background(), nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInvalidParam) {
		t.Fatalf("nil request = %v", err)
	}
	if _, err := h.Reverse(context.Background(), nil); !errors.Is(errno.FromBizStatus(err), errno.ErrInternal) {
		t.Fatalf("nil reverse = %v", err)
	}
}

func TestServerTimeoutConstant(t *testing.T) {
	if ServerTimeout != 2*time.Second {
		t.Fatalf("ServerTimeout = %s", ServerTimeout)
	}
	if ListenAddr != ":8882" {
		t.Fatalf("ListenAddr = %s", ListenAddr)
	}
}

type call struct {
	name string
	fn   func() error
}

func allCalls(cli accountservice.Client) []call {
	ctx := context.Background()
	return []call{
		{"OpenAccount", func() error {
			_, err := cli.OpenAccount(ctx, &account.OpenAccountRequest{OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE"})
			return err
		}},
		{"GetAccount", func() error {
			_, err := cli.GetAccount(ctx, &account.GetAccountRequest{OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE"})
			return err
		}},
		{"Freeze", func() error {
			_, err := cli.Freeze(ctx, &account.FreezeRequest{BizType: "REFUND", BizNo: "RF", MerchantId: "M100002", Amount: 1})
			return err
		}},
		{"ReleaseFreeze", func() error {
			_, err := cli.ReleaseFreeze(ctx, &account.ReleaseFreezeRequest{BizType: "REFUND", BizNo: "RF", MerchantId: "M100002", Amount: 1})
			return err
		}},
		{"ConfirmFreeze", func() error {
			_, err := cli.ConfirmFreeze(ctx, &account.ConfirmFreezeRequest{BizType: "TRANSFER", BizNo: "TF", MerchantId: "M100002"})
			return err
		}},
		{"TransferLocal", func() error {
			_, err := cli.TransferLocal(ctx, &account.TransferLocalRequest{TransferNo: "TF", PayerMerchantId: "M100002", PayeeMerchantId: "M100004", Amount: 1})
			return err
		}},
		{"TransferInTry", func() error {
			_, err := cli.TransferInTry(ctx, &account.TransferInTryRequest{TransferNo: "TF", PayeeMerchantId: "M100004", Amount: 1})
			return err
		}},
		{"TransferInConfirm", func() error {
			_, err := cli.TransferInConfirm(ctx, &account.TransferInConfirmRequest{TransferNo: "TF", PayeeMerchantId: "M100004"})
			return err
		}},
		{"TransferInCancel", func() error {
			_, err := cli.TransferInCancel(ctx, &account.TransferInCancelRequest{TransferNo: "TF", PayeeMerchantId: "M100004", Amount: 1})
			return err
		}},
		{"CheckBalance", func() error {
			_, err := cli.CheckBalance(ctx, &account.CheckBalanceRequest{})
			return err
		}},
	}
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

func dialKitex(t *testing.T, addr string, rpc time.Duration) accountservice.Client {
	t.Helper()
	cli, err := accountservice.NewClient("AccountService",
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
	mu          sync.Mutex
	err         error
	panicMethod string
	block       func(context.Context) error
	account     biz.Account
	freeze      biz.FreezeResult
	confirm     biz.ConfirmResult
	local       biz.TransferLocalResult
	in          biz.TransferInResult
	check       biz.CheckBalanceResult
	calls       int32
}

type scriptSnap struct {
	err     error
	account biz.Account
	freeze  biz.FreezeResult
	confirm biz.ConfirmResult
	local   biz.TransferLocalResult
	in      biz.TransferInResult
	check   biz.CheckBalanceResult
}

func (s *scripted) snapshot() scriptSnap {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return scriptSnap{
		err: s.err, account: s.account, freeze: s.freeze, confirm: s.confirm,
		local: s.local, in: s.in, check: s.check,
	}
}

func (s *scripted) callCount() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scripted) OpenAccount(ctx context.Context, _, _, _ string, _, _ int) (biz.Account, error) {
	snap := s.snapshot()
	return snap.account, s.finish(ctx, "OpenAccount", snap.err)
}
func (s *scripted) GetAccount(ctx context.Context, _, _, _ string) (biz.Account, error) {
	snap := s.snapshot()
	return snap.account, s.finish(ctx, "GetAccount", snap.err)
}
func (s *scripted) Freeze(ctx context.Context, _, _, _ string, _ money.Amount) (biz.FreezeResult, error) {
	snap := s.snapshot()
	return snap.freeze, s.finish(ctx, "Freeze", snap.err)
}
func (s *scripted) ReleaseFreeze(ctx context.Context, _, _, _ string, _ money.Amount) (biz.FreezeResult, error) {
	snap := s.snapshot()
	return snap.freeze, s.finish(ctx, "ReleaseFreeze", snap.err)
}
func (s *scripted) ConfirmFreeze(ctx context.Context, _, _, _, _ string) (biz.ConfirmResult, error) {
	snap := s.snapshot()
	return snap.confirm, s.finish(ctx, "ConfirmFreeze", snap.err)
}
func (s *scripted) TransferLocal(ctx context.Context, _, _, _ string, _ money.Amount) (biz.TransferLocalResult, error) {
	snap := s.snapshot()
	return snap.local, s.finish(ctx, "TransferLocal", snap.err)
}
func (s *scripted) TransferInTry(ctx context.Context, _, _ string, _ money.Amount) (biz.TransferInResult, error) {
	snap := s.snapshot()
	return snap.in, s.finish(ctx, "TransferInTry", snap.err)
}
func (s *scripted) TransferInConfirm(ctx context.Context, _, _ string) (biz.TransferInResult, error) {
	snap := s.snapshot()
	return snap.in, s.finish(ctx, "TransferInConfirm", snap.err)
}
func (s *scripted) TransferInCancel(ctx context.Context, _, _ string, _ money.Amount) (biz.TransferInResult, error) {
	snap := s.snapshot()
	return snap.in, s.finish(ctx, "TransferInCancel", snap.err)
}
func (s *scripted) CheckBalance(ctx context.Context, _ string, _ bool) (biz.CheckBalanceResult, error) {
	snap := s.snapshot()
	return snap.check, s.finish(ctx, "CheckBalance", snap.err)
}

// finish 在 snapshot 已经加过计数之后处理 panic 和阻塞。不再次读共享字段。
func (s *scripted) finish(ctx context.Context, method string, err error) error {
	s.mu.Lock()
	panicMethod := s.panicMethod
	block := s.block
	s.mu.Unlock()
	if panicMethod == method {
		panic("boom password=should-not-leak")
	}
	if block != nil {
		if berr := block(ctx); berr != nil {
			return berr
		}
	}
	return err
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

func (a *accessLog) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.rows)
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
