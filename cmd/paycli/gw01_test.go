package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/gateway"
)

// GW-01 验收 3：成功响应可用 paycli 验证签名。
// GW-03 起 /v1 先验签。fake lookup 的 secret 与 testSecret 相同，handler 只 WriteOK。
func TestGW01PayCLIVerifiesPlatformSignature(t *testing.T) {
	clk := fixedClock()
	h := gateway.NewMerchant(gateway.WithClock(clk), gateway.WithMerchantLookup(gw03PaycliLookup{}), gateway.WithAccountAPI(gw02PaycliAccount{}))
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := gateway.Listen(ctx, ":0")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	srv := gateway.NewEngine(ln)
	h.Register(srv)
	errCh := make(chan error, 1)
	go func() { errCh <- gateway.Spin(ctx, srv) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("gateway did not exit")
		}
	})
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	hostport := net.JoinHostPort("127.0.0.1", port)
	waitDial(t, hostport)

	var stdout, stderr bytes.Buffer
	hdr := make(http.Header)
	code := runClient(
		[]string{"--gateway", "http://" + hostport, "--app-id", testAppID, "--app-secret", testSecret, "balance"},
		nil,
		&stdout,
		&stderr,
		clk,
		oneNonce(vectorNonce),
		func(timeout time.Duration) *http.Client {
			return &http.Client{
				Timeout: timeout,
				CheckRedirect: func(*http.Request, []*http.Request) error {
					return http.ErrUseLastResponse
				},
				Transport: headerCapture{
					base: &http.Transport{DisableKeepAlives: true},
					dst:  hdr,
				},
			}
		},
	)
	if code != 0 {
		t.Fatalf("exit %d stderr %s stdout %s", code, stderr.String(), stdout.String())
	}
	msg := sign.PlatformMessage{
		Timestamp: hdr.Get(sign.HeaderTimestamp),
		Nonce:     hdr.Get(sign.HeaderNonce),
		Body:      stdout.Bytes(),
	}
	if err = sign.VerifyPlatform([]byte(testSecret), msg, hdr.Get(sign.HeaderSignature)); err != nil {
		t.Fatal(err)
	}
	if msg.Timestamp != sign.FormatTimestamp(clk.Now()) {
		t.Fatalf("timestamp %s", msg.Timestamp)
	}
}

type gw02PaycliAccount struct{}

func (gw02PaycliAccount) GetAccount(context.Context, *account.GetAccountRequest, ...callopt.Option) (*account.Account, error) {
	return &account.Account{AccountNo: "AC0000000000000000001", Currency: "CNY", AvailableAmount: 0}, nil
}

type gw03PaycliLookup struct{}

func (gw03PaycliLookup) Lookup(context.Context, string) (gateway.Credential, error) {
	return gateway.Credential{
		MerchantID: "M100001",
		Status:     "ACTIVE",
		Secret:     []byte(testSecret),
	}, nil
}

type headerCapture struct {
	base http.RoundTripper
	dst  http.Header
}

func (h headerCapture) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := h.base.RoundTrip(req)
	if resp != nil {
		for k, v := range resp.Header {
			h.dst[k] = append([]string(nil), v...)
		}
	}
	return resp, err
}

func waitDial(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now()
	deadline = deadline.Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	dialer := &net.Dialer{Timeout: 50 * time.Millisecond}
	var last error
	for {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		last = err
		now := time.Now()
		if now.After(deadline) {
			t.Fatal(last)
		}
	}
}
