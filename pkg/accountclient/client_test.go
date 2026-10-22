package accountclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/server"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/kitex_gen/account/accountservice"
	"github.com/yym108/pay-system/pkg/errno"
)

// ACC-11 第 4 条：客户端只对幂等方法在超时时重试 1 次。假 handler 计数。
func TestRetryOnlyIdempotentMethods(t *testing.T) {
	h := &countingHandler{delay: 200 * time.Millisecond}
	addr := startFake(t, h)
	cli, err := Dial(addr, WithTimeouts(time.Second, 40*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	calls := []struct {
		name string
		fn   func() error
	}{
		{"OpenAccount", func() error { _, err := cli.OpenAccount(ctx, &account.OpenAccountRequest{}); return err }},
		{"GetAccount", func() error { _, err := cli.GetAccount(ctx, &account.GetAccountRequest{}); return err }},
		{"Freeze", func() error { _, err := cli.Freeze(ctx, &account.FreezeRequest{}); return err }},
		{"ReleaseFreeze", func() error { _, err := cli.ReleaseFreeze(ctx, &account.ReleaseFreezeRequest{}); return err }},
		{"ConfirmFreeze", func() error { _, err := cli.ConfirmFreeze(ctx, &account.ConfirmFreezeRequest{}); return err }},
		{"TransferLocal", func() error { _, err := cli.TransferLocal(ctx, &account.TransferLocalRequest{}); return err }},
		{"TransferInTry", func() error { _, err := cli.TransferInTry(ctx, &account.TransferInTryRequest{}); return err }},
		{"TransferInConfirm", func() error {
			_, err := cli.TransferInConfirm(ctx, &account.TransferInConfirmRequest{})
			return err
		}},
		{"TransferInCancel", func() error { _, err := cli.TransferInCancel(ctx, &account.TransferInCancelRequest{}); return err }},
		{"CheckBalance", func() error { _, err := cli.CheckBalance(ctx, &account.CheckBalanceRequest{}); return err }},
		{"Reverse", func() error { _, err := cli.Reverse(ctx, &account.ReverseRequest{}); return err }},
	}
	for _, call := range calls {
		if err := call.fn(); !kerrors.IsTimeoutError(err) {
			t.Fatalf("%s err = %v, want timeout", call.name, err)
		}
		want := 1
		if Idempotent(call.name) {
			want = 2
		}
		if got := h.count(call.name); got != want {
			t.Fatalf("%s calls = %d, want %d", call.name, got, want)
		}
	}
}

func TestNoRetryOnBizErrorOrCallerCancel(t *testing.T) {
	h := &countingHandler{biz: errno.ToBizStatus(errno.ErrAccountNotFound)}
	addr := startFake(t, h)
	cli, err := Dial(addr, WithTimeouts(time.Second, 40*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.Freeze(context.Background(), &account.FreezeRequest{BizType: "REFUND", BizNo: "RF", MerchantId: "M100002", Amount: 1})
	if got := errno.FromBizStatus(err); got.Code != errno.ErrAccountNotFound.Code {
		t.Fatalf("code = %d err=%v", got.Code, err)
	}
	if n := h.count("Freeze"); n != 1 {
		t.Fatalf("biz error retried, calls = %d", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = cli.GetAccount(ctx, &account.GetAccountRequest{})
	if n := h.count("GetAccount"); n > 1 {
		t.Fatalf("cancelled caller retried, calls = %d", n)
	}
}

func TestRetryPredicateAndDefaults(t *testing.T) {
	if ConnectTimeout != 100*time.Millisecond || RPCTimeout != 500*time.Millisecond {
		t.Fatalf("defaults connect=%s rpc=%s", ConnectTimeout, RPCTimeout)
	}
	ctx := context.Background()
	if !shouldRetry(ctx, kerrors.ErrRPCTimeout) {
		t.Fatal("rpc timeout should retry")
	}
	serverTimeout := kerrors.ErrRemoteOrNetwork.WithCause(remote.NewTransErrorWithMsg(remote.InternalError, kerrors.ErrRPCTimeout.Error()))
	if !shouldRetry(ctx, serverTimeout) {
		t.Fatal("server processing timeout should retry")
	}
	if !shouldRetry(ctx, netTimeout{timeout: true}) {
		t.Fatal("connect timeout should retry")
	}
	if shouldRetry(ctx, netTimeout{timeout: false}) {
		t.Fatal("connection refused must not retry")
	}
	if shouldRetry(ctx, errno.ToBizStatus(errno.ErrInternal)) {
		t.Fatal("10003 must not retry")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if shouldRetry(cancelled, kerrors.ErrRPCTimeout) {
		t.Fatal("caller cancel must not retry")
	}
	for _, method := range []string{"Freeze", "ReleaseFreeze", "ConfirmFreeze", "TransferLocal", "TransferInTry", "TransferInConfirm", "TransferInCancel", "GetAccount"} {
		if !Idempotent(method) {
			t.Fatalf("%s should be idempotent", method)
		}
	}
	for _, method := range []string{"OpenAccount", "CheckBalance", "Reverse", "Post"} {
		if Idempotent(method) {
			t.Fatalf("%s must not retry", method)
		}
	}
	if _, err := Dial(""); err == nil {
		t.Fatal("empty address accepted")
	}
}

type netTimeout struct{ timeout bool }

func (e netTimeout) Error() string   { return "net" }
func (e netTimeout) Timeout() bool   { return e.timeout }
func (e netTimeout) Temporary() bool { return e.timeout }

type countingHandler struct {
	mu    sync.Mutex
	n     map[string]int
	delay time.Duration
	biz   error
}

func (h *countingHandler) hit(method string) {
	h.mu.Lock()
	if h.n == nil {
		h.n = map[string]int{}
	}
	h.n[method]++
	h.mu.Unlock()
	if h.biz != nil {
		return
	}
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
}

func (h *countingHandler) count(method string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n[method]
}

func (h *countingHandler) OpenAccount(context.Context, *account.OpenAccountRequest) (*account.Account, error) {
	h.hit("OpenAccount")
	return nil, h.err()
}
func (h *countingHandler) GetAccount(context.Context, *account.GetAccountRequest) (*account.Account, error) {
	h.hit("GetAccount")
	return nil, h.err()
}
func (h *countingHandler) Freeze(context.Context, *account.FreezeRequest) (*account.FreezeResponse, error) {
	h.hit("Freeze")
	return nil, h.err()
}
func (h *countingHandler) ReleaseFreeze(context.Context, *account.ReleaseFreezeRequest) (*account.ReleaseFreezeResponse, error) {
	h.hit("ReleaseFreeze")
	return nil, h.err()
}
func (h *countingHandler) ConfirmFreeze(context.Context, *account.ConfirmFreezeRequest) (*account.ConfirmFreezeResponse, error) {
	h.hit("ConfirmFreeze")
	return nil, h.err()
}
func (h *countingHandler) TransferLocal(context.Context, *account.TransferLocalRequest) (*account.TransferLocalResponse, error) {
	h.hit("TransferLocal")
	return nil, h.err()
}
func (h *countingHandler) TransferInTry(context.Context, *account.TransferInTryRequest) (*account.TransferInTryResponse, error) {
	h.hit("TransferInTry")
	return nil, h.err()
}
func (h *countingHandler) TransferInConfirm(context.Context, *account.TransferInConfirmRequest) (*account.TransferInConfirmResponse, error) {
	h.hit("TransferInConfirm")
	return nil, h.err()
}
func (h *countingHandler) TransferInCancel(context.Context, *account.TransferInCancelRequest) (*account.TransferInCancelResponse, error) {
	h.hit("TransferInCancel")
	return nil, h.err()
}
func (h *countingHandler) CheckBalance(context.Context, *account.CheckBalanceRequest) (*account.CheckBalanceResponse, error) {
	h.hit("CheckBalance")
	return nil, h.err()
}
func (h *countingHandler) Reverse(context.Context, *account.ReverseRequest) (*account.ReverseResponse, error) {
	h.hit("Reverse")
	return nil, h.err()
}

func (h *countingHandler) err() error {
	if h.biz != nil {
		return h.biz
	}
	return nil
}

func startFake(t *testing.T, h account.AccountService) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	exit := make(chan error, 1)
	svr := accountservice.NewServer(h,
		server.WithListener(ln),
		server.WithMetaHandler(transmeta.ServerTTHeaderHandler),
		server.WithExitSignal(func() <-chan error { return exit }),
	)
	errCh := make(chan error, 1)
	go func() { errCh <- svr.Run() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := net.Dialer{Timeout: 50 * time.Millisecond}
	for {
		conn, dialErr := dialer.DialContext(ctx, "tcp", ln.Addr().String())
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if ctx.Err() != nil {
			t.Fatal(dialErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() {
		exit <- errors.New("stop")
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("fake server did not exit")
		}
	})
	return ln.Addr().String()
}
