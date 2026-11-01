package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/sign"
)

func gw02Clock() *clock.Fake {
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	return clk
}

func gw02Lookup() *fakeLookup {
	return &fakeLookup{fn: func(string) (Credential, error) {
		return Credential{MerchantID: gw03MID, Status: "ACTIVE", Secret: []byte(gw03Secret)}, nil
	}}
}

func gw02Sign(t *testing.T, method, uri string, body []byte) http.Header {
	t.Helper()
	return signedHeader(t, []byte(gw03Secret), method, uri, gw03AppID, "1791500000", gw03Nonce, body)
}

func gw02Start(t *testing.T, pay paymentAPI, acct accountAPI) string {
	t.Helper()
	h := NewMerchant(
		WithClock(gw02Clock()),
		WithMerchantLookup(gw02Lookup()),
		WithPaymentAPI(pay),
		WithAccountAPI(acct),
	)
	return startGW(t, h, nil)
}

func assertSignedErrno(t *testing.T, got gwResp, want *errno.Error) {
	t.Helper()
	env := decodeEnv(t, got.body)
	if got.status != want.HTTPStatus || env.Code != want.Code || env.Msg != want.Msg {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if err := sign.VerifyPlatform([]byte(gw03Secret), sign.PlatformMessage{
		Timestamp: got.header.Get(sign.HeaderTimestamp),
		Nonce:     got.header.Get(sign.HeaderNonce),
		Body:      got.body,
	}, got.header.Get(sign.HeaderSignature)); err != nil {
		t.Fatal(err)
	}
}

type gw02Payment struct {
	mu         sync.Mutex
	createN    int
	payN       int
	closeN     int
	queryN     int
	lastCreate *payment.CreatePaymentRequest
	lastPay    *payment.PayRequest
	lastQuery  *payment.QueryPaymentRequest
	createFn   func(*payment.CreatePaymentRequest) (*payment.PaymentOrder, error)
	payFn      func(*payment.PayRequest) (*payment.PayResponse, error)
	closeFn    func(*payment.ClosePaymentRequest) (*payment.ClosePaymentResponse, error)
	queryFn    func(*payment.QueryPaymentRequest) (*payment.PaymentOrder, error)
}

func (f *gw02Payment) CreatePayment(_ context.Context, req *payment.CreatePaymentRequest, _ ...callopt.Option) (*payment.PaymentOrder, error) {
	f.mu.Lock()
	f.createN++
	f.lastCreate = req
	fn := f.createFn
	f.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &payment.PaymentOrder{PayNo: "PA0000000000000000001", OutTradeNo: req.OutTradeNo, Amount: req.Amount, Subject: req.Subject, Currency: "CNY", Status: "INIT"}, nil
}

func (f *gw02Payment) Pay(_ context.Context, req *payment.PayRequest, _ ...callopt.Option) (*payment.PayResponse, error) {
	f.mu.Lock()
	f.payN++
	f.lastPay = req
	fn := f.payFn
	f.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &payment.PayResponse{BizCode: 0, PaymentOrder: &payment.PaymentOrder{PayNo: req.PayNo, Status: "PAYING"}}, nil
}

func (f *gw02Payment) ClosePayment(_ context.Context, req *payment.ClosePaymentRequest, _ ...callopt.Option) (*payment.ClosePaymentResponse, error) {
	f.mu.Lock()
	f.closeN++
	fn := f.closeFn
	f.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &payment.ClosePaymentResponse{BizCode: 0, PaymentOrder: &payment.PaymentOrder{PayNo: req.PayNo, Status: "CLOSED"}}, nil
}

func (f *gw02Payment) QueryPayment(_ context.Context, req *payment.QueryPaymentRequest, _ ...callopt.Option) (*payment.PaymentOrder, error) {
	f.mu.Lock()
	f.queryN++
	f.lastQuery = req
	fn := f.queryFn
	f.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &payment.PaymentOrder{PayNo: "PA0000000000000000001", Status: "INIT"}, nil
}

func (f *gw02Payment) counts() (createN, payN, closeN, queryN int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createN, f.payN, f.closeN, f.queryN
}

type gw02Account struct {
	mu   sync.Mutex
	n    int
	last *account.GetAccountRequest
	fn   func(*account.GetAccountRequest) (*account.Account, error)
}

func (a *gw02Account) GetAccount(_ context.Context, req *account.GetAccountRequest, _ ...callopt.Option) (*account.Account, error) {
	if a == nil {
		return nil, kerrors.ErrRPCTimeout
	}
	a.mu.Lock()
	a.n++
	a.last = req
	fn := a.fn
	a.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &account.Account{AccountNo: "AC0000000000000000001", Currency: "CNY"}, nil
}

func (a *gw02Account) calls() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}

func gw02Post(t *testing.T, addr, path string, body []byte) gwResp {
	t.Helper()
	return do(t, http.MethodPost, "http://"+addr+path, body, gw02Sign(t, http.MethodPost, path, body))
}

// GW-02 验收 1：金额不是 JSON 整数或超出 1–10,000,000 时返回 10001，且不调用 CreatePayment。
func TestGW02AmountRejected(t *testing.T) {
	pay := &gw02Payment{}
	addr := gw02Start(t, pay, nil)
	bodies := []string{
		`{"out_trade_no":"T1","amount":100.5,"subject":"demo"}`,
		`{"out_trade_no":"T1","amount":"100","subject":"demo"}`,
		`{"out_trade_no":"T1","amount":0,"subject":"demo"}`,
		`{"out_trade_no":"T1","amount":10000001,"subject":"demo"}`,
	}
	for _, body := range bodies {
		got := gw02Post(t, addr, "/v1/payments", []byte(body))
		assertSignedErrno(t, got, errno.ErrInvalidParam)
	}
	if createN, _, _, _ := pay.counts(); createN != 0 {
		t.Fatalf("create calls %d", createN)
	}
}

// GW-02 验收 2：请求体里的 merchant_id 不决定订单归属。
func TestGW02ForgedMerchantIgnored(t *testing.T) {
	pay := &gw02Payment{}
	addr := gw02Start(t, pay, nil)
	body := []byte(`{"merchant_id":"M999999","out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`)
	got := gw02Post(t, addr, "/v1/payments", body)
	assertSignedErrno(t, got, errno.OK)
	pay.mu.Lock()
	req := pay.lastCreate
	pay.mu.Unlock()
	if req == nil || req.MerchantId != gw03MID || req.Currency != nil || req.NotifyUrl != nil || req.ExpireMinutes != nil {
		t.Fatalf("req %+v", req)
	}
}

// GW-02 验收 3：余额只属于验签商户，查询串里的 merchant_id 被忽略。
func TestGW02BalanceIgnoresQueryMerchant(t *testing.T) {
	acct := &gw02Account{fn: func(req *account.GetAccountRequest) (*account.Account, error) {
		return &account.Account{
			AccountNo: "AC0000000000000000001", OwnerType: "MERCHANT", OwnerId: req.OwnerId,
			AccountType: "MERCHANT_SETTLE", Currency: "CNY", Balance: 100, FrozenAmount: 40,
			AvailableAmount: 7, Status: "ACTIVE",
		}, nil
	}}
	addr := gw02Start(t, nil, acct)
	path := "/v1/balance?merchant_id=M999999"
	got := do(t, http.MethodGet, "http://"+addr+path, nil, gw02Sign(t, http.MethodGet, path, nil))
	assertSignedErrno(t, got, errno.OK)
	acct.mu.Lock()
	req := acct.last
	acct.mu.Unlock()
	if req == nil || req.OwnerType != "MERCHANT" || req.OwnerId != gw03MID || req.AccountType != "MERCHANT_SETTLE" {
		t.Fatalf("req %+v", req)
	}
	var keys map[string]json.RawMessage
	env := decodeEnv(t, got.body)
	if err := json.Unmarshal(env.Data, &keys); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 5 {
		t.Fatalf("keys %d %s", len(keys), env.Data)
	}
	for _, key := range []string{"account_no", "currency", "balance", "frozen_amount", "available_amount"} {
		if _, ok := keys[key]; !ok {
			t.Fatalf("missing %s in %s", key, env.Data)
		}
	}
	var view balanceView
	if err := json.Unmarshal(env.Data, &view); err != nil {
		t.Fatal(err)
	}
	if view.AvailableAmount != 7 || view.Balance != 100 || view.FrozenAmount != 40 {
		t.Fatalf("view %+v", view)
	}
}

func TestGW02CreateFieldLimits(t *testing.T) {
	pay := &gw02Payment{}
	addr := gw02Start(t, pay, nil)
	bodies := []string{
		`{"out_trade_no":"T1","amount":100,"subject":"` + strings.Repeat("a", 129) + `"}`,
		`{"out_trade_no":"T1","amount":100,"subject":"demo","notify_url":"ftp://example.com/a"}`,
		`{"out_trade_no":"T1","amount":100,"subject":"demo","currency":"USD"}`,
		`{"out_trade_no":"T1","amount":100,"subject":"demo","expire_minutes":121}`,
	}
	for _, body := range bodies {
		got := gw02Post(t, addr, "/v1/payments", []byte(body))
		assertSignedErrno(t, got, errno.ErrInvalidParam)
	}
	if createN, _, _, _ := pay.counts(); createN != 0 {
		t.Fatalf("create calls %d", createN)
	}
}

func TestGW02CloseRejectsBody(t *testing.T) {
	pay := &gw02Payment{}
	addr := gw02Start(t, pay, nil)
	path := "/v1/payments/PA0000000000000000001/close"
	got := gw02Post(t, addr, path, []byte(`{}`))
	assertSignedErrno(t, got, errno.ErrInvalidParam)
	if _, _, closeN, _ := pay.counts(); closeN != 0 {
		t.Fatalf("close calls %d", closeN)
	}
}

func TestGW02QueryRejectsBothKeys(t *testing.T) {
	pay := &gw02Payment{}
	addr := gw02Start(t, pay, nil)
	path := "/v1/payments/PA0000000000000000001?out_trade_no=T1"
	got := do(t, http.MethodGet, "http://"+addr+path, nil, gw02Sign(t, http.MethodGet, path, nil))
	assertSignedErrno(t, got, errno.ErrInvalidParam)
	if _, _, _, queryN := pay.counts(); queryN != 0 {
		t.Fatalf("query calls %d", queryN)
	}
}

func TestGW02PayChannelRejected(t *testing.T) {
	pay := &gw02Payment{payFn: func(req *payment.PayRequest) (*payment.PayResponse, error) {
		return &payment.PayResponse{BizCode: errno.ErrChannelRejected.Code, PaymentOrder: &payment.PaymentOrder{PayNo: req.PayNo, Status: "FAILED"}}, nil
	}}
	addr := gw02Start(t, pay, nil)
	path := "/v1/payments/PA0000000000000000001/pay"
	got := gw02Post(t, addr, path, []byte(`{"channel":"mockpay","merchant_id":"M999999","pay_no":"OTHER"}`))
	assertSignedErrno(t, got, errno.ErrChannelRejected)
	env := decodeEnv(t, got.body)
	var data struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil || data.Status != "FAILED" {
		t.Fatalf("data %s", env.Data)
	}
	pay.mu.Lock()
	req := pay.lastPay
	pay.mu.Unlock()
	if req == nil || req.MerchantId != gw03MID || req.PayNo != "PA0000000000000000001" || req.Channel != "mockpay" {
		t.Fatalf("pay %+v", req)
	}
}

func TestGW02CloseStatusConflict(t *testing.T) {
	pay := &gw02Payment{closeFn: func(req *payment.ClosePaymentRequest) (*payment.ClosePaymentResponse, error) {
		return &payment.ClosePaymentResponse{BizCode: errno.ErrOrderStatusConflict.Code, PaymentOrder: &payment.PaymentOrder{PayNo: req.PayNo, Status: "SUCCESS"}}, nil
	}}
	addr := gw02Start(t, pay, nil)
	path := "/v1/payments/PA0000000000000000001/close"
	got := gw02Post(t, addr, path, nil)
	assertSignedErrno(t, got, errno.ErrOrderStatusConflict)
	env := decodeEnv(t, got.body)
	var data struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil || data.Status != "SUCCESS" {
		t.Fatalf("data %s", env.Data)
	}
}

func TestGW02TransportOnce(t *testing.T) {
	pay := &gw02Payment{createFn: func(*payment.CreatePaymentRequest) (*payment.PaymentOrder, error) {
		return nil, kerrors.ErrRPCTimeout
	}}
	addr := gw02Start(t, pay, nil)
	body := []byte(`{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`)
	got := gw02Post(t, addr, "/v1/payments", body)
	assertSignedErrno(t, got, errno.ErrUnavailable)
	if createN, _, _, _ := pay.counts(); createN != 1 {
		t.Fatalf("create calls %d", createN)
	}
}

func TestGW02NilClient(t *testing.T) {
	addr := gw02Start(t, nil, nil)
	body := []byte(`{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`)
	got := gw02Post(t, addr, "/v1/payments", body)
	assertSignedErrno(t, got, errno.ErrInternal)
	if string(decodeEnv(t, got.body).Data) != "{}" {
		t.Fatalf("data %s", got.body)
	}
}

func TestGW02QueryOneKey(t *testing.T) {
	pay := &gw02Payment{}
	addr := gw02Start(t, pay, nil)
	byNo := "/v1/payments/PA0000000000000000001"
	got := do(t, http.MethodGet, "http://"+addr+byNo, nil, gw02Sign(t, http.MethodGet, byNo, nil))
	assertSignedErrno(t, got, errno.OK)
	byOut := "/v1/payments?out_trade_no=T20261009001"
	got = do(t, http.MethodGet, "http://"+addr+byOut, nil, gw02Sign(t, http.MethodGet, byOut, nil))
	assertSignedErrno(t, got, errno.OK)
	pay.mu.Lock()
	q := pay.lastQuery
	n := pay.queryN
	pay.mu.Unlock()
	if n != 2 || q == nil || q.PayNo != nil || q.OutTradeNo == nil || *q.OutTradeNo != "T20261009001" || q.MerchantId != gw03MID {
		t.Fatalf("query n=%d %+v", n, q)
	}
}
