package gateway

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/sign"
)

const gw06Body = `{"notify_id":"CN0000000123456789012","txn_type":"PAY","req_no":"PA0000000123456789012","channel_txn_no":"MC0000000123456789012","amount":10000,"status":"SUCCESS","finished_at":"2026-10-26T11:22:33.123+08:00"}`

type gw06Notify struct {
	mu     sync.Mutex
	calls  int
	last   *payment.HandleChannelNotifyRequest
	err    error
	result string
}

func (f *gw06Notify) HandleChannelNotify(_ context.Context, req *payment.HandleChannelNotifyRequest, _ ...callopt.Option) (*payment.HandleChannelNotifyResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.last = req
	if f.err != nil {
		return nil, f.err
	}
	res := f.result
	if res == "" {
		res = "APPLIED"
	}
	return &payment.HandleChannelNotifyResponse{Result_: res}, nil
}

func (f *gw06Notify) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *gw06Notify) snapshot() *payment.HandleChannelNotifyRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

func gw06Post(t *testing.T, addr string, key []byte, ts string, body []byte) gwResp {
	t.Helper()
	sig, err := sign.SignChannel(key, sign.ChannelMessage{Timestamp: ts, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	hdr := http.Header{}
	hdr.Set(sign.HeaderMockTimestamp, ts)
	hdr.Set(sign.HeaderMockSignature, sig)
	return do(t, http.MethodPost, "http://"+addr+"/callback/mockpay", body, hdr)
}

func TestGW06BadSignature(t *testing.T) {
	fake := &gw06Notify{}
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	key := []byte("mockpay-plain-key")
	addr := startGW(t, NewMerchant(WithClock(clk), WithChannelKey(key), WithNotifyAPI(fake)), nil)
	ts := strconv.FormatInt(clk.Now().Unix(), 10)
	sig, err := sign.SignChannel(key, sign.ChannelMessage{Timestamp: ts, Body: []byte(gw06Body)})
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(sig)
	raw[len(raw)-1] ^= 1
	hdr := http.Header{}
	hdr.Set(sign.HeaderMockTimestamp, ts)
	hdr.Set(sign.HeaderMockSignature, string(raw))
	got := do(t, http.MethodPost, "http://"+addr+"/callback/mockpay", []byte(gw06Body), hdr)
	if got.status != http.StatusUnauthorized || string(got.body) != "FAIL" {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if n := fake.count(); n != 0 {
		t.Fatalf("calls %d", n)
	}
}

func TestGW06TimestampSkew(t *testing.T) {
	fake := &gw06Notify{}
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	key := []byte("mockpay-plain-key")
	addr := startGW(t, NewMerchant(WithClock(clk), WithChannelKey(key), WithNotifyAPI(fake)), nil)
	ts := strconv.FormatInt(clk.Now().Unix()+301, 10)
	got := gw06Post(t, addr, key, ts, []byte(gw06Body))
	if got.status != http.StatusUnauthorized || string(got.body) != "FAIL" {
		t.Fatalf("skew status %d body %s", got.status, got.body)
	}
	if n := fake.count(); n != 0 {
		t.Fatalf("skew calls %d", n)
	}
	ts = strconv.FormatInt(clk.Now().Unix()+300, 10)
	got = gw06Post(t, addr, key, ts, []byte(gw06Body))
	if got.status != http.StatusOK || string(got.body) != "SUCCESS" {
		t.Fatalf("ok status %d body %s", got.status, got.body)
	}
	if n := fake.count(); n != 1 {
		t.Fatalf("calls %d", n)
	}
	last := fake.snapshot()
	if last == nil || last.Channel != "mockpay" || last.Amount != 10000 || last.TxnType != "PAY" {
		t.Fatalf("last %+v", last)
	}
}

func TestGW06EmptyKey(t *testing.T) {
	fake := &gw06Notify{}
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	key := []byte("mockpay-plain-key")
	addr := startGW(t, NewMerchant(WithClock(clk), WithNotifyAPI(fake)), nil)
	ts := strconv.FormatInt(clk.Now().Unix(), 10)
	got := gw06Post(t, addr, key, ts, []byte(gw06Body))
	if got.status != http.StatusInternalServerError || string(got.body) != "FAIL" {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if n := fake.count(); n != 0 {
		t.Fatalf("calls %d", n)
	}
}

func TestGW06RejectedCodes(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	key := []byte("mockpay-plain-key")
	ts := strconv.FormatInt(clk.Now().Unix(), 10)

	fake := &gw06Notify{err: kerrors.NewBizStatusError(errno.ErrOrderNotFound.Code, "remote")}
	addr := startGW(t, NewMerchant(WithClock(clk), WithChannelKey(key), WithNotifyAPI(fake)), nil)
	before := CallbackRejected("30001")
	got := gw06Post(t, addr, key, ts, []byte(gw06Body))
	if got.status != http.StatusOK || string(got.body) != "SUCCESS" {
		t.Fatalf("30001 status %d body %s", got.status, got.body)
	}
	if CallbackRejected("30001") != before+1 {
		t.Fatalf("30001 count %d", CallbackRejected("30001"))
	}
	if n := fake.count(); n != 1 {
		t.Fatalf("30001 calls %d", n)
	}

	fake = &gw06Notify{err: kerrors.NewBizStatusError(errno.ErrChannelAmountMismatch.Code, "remote")}
	addr = startGW(t, NewMerchant(WithClock(clk), WithChannelKey(key), WithNotifyAPI(fake)), nil)
	before = CallbackRejected("50002")
	got = gw06Post(t, addr, key, ts, []byte(gw06Body))
	if got.status != http.StatusOK || string(got.body) != "SUCCESS" {
		t.Fatalf("50002 status %d body %s", got.status, got.body)
	}
	if CallbackRejected("50002") != before+1 {
		t.Fatalf("50002 count %d", CallbackRejected("50002"))
	}
	if n := fake.count(); n != 1 {
		t.Fatalf("50002 calls %d", n)
	}
}

func TestGW06TransportOnce(t *testing.T) {
	fake := &gw06Notify{err: kerrors.ErrRPCTimeout}
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	key := []byte("mockpay-plain-key")
	addr := startGW(t, NewMerchant(WithClock(clk), WithChannelKey(key), WithNotifyAPI(fake)), nil)
	before := CallbackRejected("10004")
	ts := strconv.FormatInt(clk.Now().Unix(), 10)
	got := gw06Post(t, addr, key, ts, []byte(gw06Body))
	if got.status != http.StatusInternalServerError || string(got.body) != "FAIL" {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if n := fake.count(); n != 1 {
		t.Fatalf("calls %d", n)
	}
	if CallbackRejected("10004") != before {
		t.Fatalf("10004 count %d want %d", CallbackRejected("10004"), before)
	}
}

func TestGW06AmountNotInteger(t *testing.T) {
	fake := &gw06Notify{}
	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	key := []byte("mockpay-plain-key")
	addr := startGW(t, NewMerchant(WithClock(clk), WithChannelKey(key), WithNotifyAPI(fake)), nil)
	body := strings.Replace(gw06Body, `amount":10000`, `amount":100.5`, 1)
	ts := strconv.FormatInt(clk.Now().Unix(), 10)
	got := gw06Post(t, addr, key, ts, []byte(body))
	if got.status != http.StatusInternalServerError || string(got.body) != "FAIL" {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if n := fake.count(); n != 0 {
		t.Fatalf("calls %d", n)
	}
}
