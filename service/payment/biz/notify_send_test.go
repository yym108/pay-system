package biz

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/sign"
)

func notifyUnit(t *testing.T, hosts string) (*Payments, *clock.Fake) {
	t.Helper()
	p, merchants, _, _, clk := newPayTest(t, hosts)
	merchants.cfg.MerchantID = "M100001"
	merchants.secret = NewSecret(demoSecret)
	p.UseNotify(30*time.Second, 5)
	return p, clk
}

func notifyTaskFor(url string, attempt int) notifyTask {
	return notifyTask{
		ID:         1,
		NotifyNo:   "NT0000000000000000001",
		MerchantID: "M100001",
		BizType:    "PAY",
		BizNo:      "PA0000000000000000001",
		NotifyURL:  url,
		Payload:    []byte(`{"notify_id":"NT0000000000000000001","event_type":"payment.succeeded","merchant_id":"M100001","data":{"pay_no":"PA0000000000000000001","out_trade_no":"T1","amount":100,"fee_amount":1,"status":"SUCCESS","paid_at":"2026-11-11T10:00:00.000+08:00"}}`),
		Attempt:    attempt,
	}
}

type rejectTransport struct {
	t    *testing.T
	hits atomic.Int32
}

func (r *rejectTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.hits.Add(1)
	r.t.Error("notify request was sent")
	return nil, io.EOF
}

func swapNotifyLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

// NTF-02 规则 3：HTTP 200 且去掉首尾空白后是 SUCCESS。
func TestNotifySuccessTrimsBody(t *testing.T) {
	var gotBody []byte
	var gotSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		gotBody = body
		gotSig = r.Header.Get(sign.HeaderSignature)
		if err = sign.VerifyPlatform([]byte(demoSecret), sign.PlatformMessage{
			Timestamp: r.Header.Get(sign.HeaderTimestamp),
			Nonce:     r.Header.Get(sign.HeaderNonce),
			Body:      body,
		}, gotSig); err != nil {
			t.Errorf("verify: %v", err)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type %s", r.Header.Get("Content-Type"))
		}
		_, _ = io.WriteString(w, " \nSUCCESS\t")
	}))
	t.Cleanup(srv.Close)
	p, _ := notifyUnit(t, "127.0.0.1")
	out := p.performNotify(context.Background(), notifyTaskFor(srv.URL, 1))
	if out.status != notifyStatusSuccess || out.httpStatus == nil || *out.httpStatus != http.StatusOK || out.lastError != "" {
		t.Fatalf("outcome = %+v", out)
	}
	if !bytes.Equal(gotBody, notifyTaskFor(srv.URL, 1).Payload) || gotSig == "" {
		t.Fatal("body or signature missing")
	}
}

// NTF-02 规则 3：200 以外，或正文不是 SUCCESS，都是失败。未满 5 次保持 PENDING。
func TestNotifyRejectsNonSuccess(t *testing.T) {
	p, _ := notifyUnit(t, "127.0.0.1")
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "fail", status: http.StatusInternalServerError, body: "FAIL"},
		{name: "lowercase", status: http.StatusOK, body: "success"},
		{name: "created", status: http.StatusCreated, body: "SUCCESS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(srv.Close)
			out := p.performNotify(context.Background(), notifyTaskFor(srv.URL, 1))
			if out.status != notifyStatusPending || out.lastError != notifyErrFailed || out.httpStatus == nil || *out.httpStatus != tc.status {
				t.Fatalf("outcome = %+v", out)
			}
		})
	}
}

// NTF-02 规则 2：不跟随重定向。302 的 Location 指向会返回 SUCCESS 的地址，也不算成功。
func TestNotifyDoesNotFollowRedirect(t *testing.T) {
	var followed atomic.Int32
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed.Add(1)
		_, _ = io.WriteString(w, "SUCCESS")
	}))
	t.Cleanup(next.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, next.URL, http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	p, _ := notifyUnit(t, "127.0.0.1")
	out := p.performNotify(context.Background(), notifyTaskFor(srv.URL, 1))
	if followed.Load() != 0 || out.status != notifyStatusPending || out.httpStatus == nil || *out.httpStatus != http.StatusFound {
		t.Fatalf("followed=%d outcome=%+v", followed.Load(), out)
	}
}

// NTF-02 规则 2：响应体只读 1KB。前 1KB 去掉空白后是 SUCCESS 则成功；第 1025 个字节不参与判断。
func TestNotifyReadsAtMostOneKB(t *testing.T) {
	p, _ := notifyUnit(t, "127.0.0.1")
	okBody := append([]byte("SUCCESS"), bytes.Repeat([]byte(" "), notifyMaxBody-len("SUCCESS"))...)
	okBody = append(okBody, 'X')
	badBody := append(bytes.Repeat([]byte("A"), notifyMaxBody), []byte("SUCCESS")...)
	for _, tc := range []struct {
		name string
		body []byte
		want string
	}{
		{name: "trim within limit", body: okBody, want: notifyStatusSuccess},
		{name: "junk fills the limit", body: badBody, want: notifyStatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(tc.body)
			}))
			t.Cleanup(srv.Close)
			out := p.performNotify(context.Background(), notifyTaskFor(srv.URL, 1))
			if out.status != tc.want {
				t.Fatalf("status %s", out.status)
			}
		})
	}
}

// NTF-02 规则 2：密钥为空按 10003 处理，不发送，last_error 只有 internal error，日志不含密钥。
func TestNotifyEmptySecretDoesNotSend(t *testing.T) {
	logBuf := swapNotifyLog(t)
	rt := &rejectTransport{t: t}
	p, merchants, _, _, _ := newPayTest(t, "127.0.0.1")
	merchants.cfg.MerchantID = "M100001"
	merchants.secret = NewSecret("")
	p.UseNotify(30*time.Second, 5)
	p.notifyHTTP = &http.Client{Transport: rt}
	out := p.performNotify(context.Background(), notifyTaskFor("http://127.0.0.1/notify", 1))
	if rt.hits.Load() != 0 || out.status != notifyStatusPending || out.lastError != notifyErrInternal || !out.signFailed || out.httpStatus != nil {
		t.Fatalf("hits=%d outcome=%+v", rt.hits.Load(), out)
	}
	if !strings.Contains(logBuf.String(), "merchant notify sign failed") || !strings.Contains(logBuf.String(), "10003") {
		t.Fatalf("log %s", logBuf.String())
	}
	dead := p.performNotify(context.Background(), notifyTaskFor("http://127.0.0.1/notify", 5))
	if dead.status != notifyDeadStatus || dead.lastError != notifyErrInternal {
		t.Fatalf("fifth = %+v", dead)
	}
}

// NTF-02 规则 2：签名失败的日志不写密钥。nonce 含换行，签名函数拒绝，请求不发出。
func TestNotifySignFailureHidesSecret(t *testing.T) {
	logBuf := swapNotifyLog(t)
	rt := &rejectTransport{t: t}
	p, _ := notifyUnit(t, "127.0.0.1")
	p.notifyHTTP = &http.Client{Transport: rt}
	p.notifyNonce = func() (string, error) { return "bad\nnonce", nil }
	out := p.performNotify(context.Background(), notifyTaskFor("http://127.0.0.1/notify", 2))
	if rt.hits.Load() != 0 || out.lastError != notifyErrInternal || !out.signFailed {
		t.Fatalf("hits=%d outcome=%+v", rt.hits.Load(), out)
	}
	if strings.Contains(logBuf.String(), demoSecret) {
		t.Fatal("log contains app secret")
	}
}

// NTF-02 规则 5：主机不在白名单时不发送，直接 DEAD，last_error 记下原因。
func TestNotifyHostRejectedDoesNotSend(t *testing.T) {
	rt := &rejectTransport{t: t}
	p, _ := notifyUnit(t, "merchant-demo")
	p.notifyHTTP = &http.Client{Transport: rt}
	out := p.performNotify(context.Background(), notifyTaskFor("http://evil.example/notify", 1))
	if rt.hits.Load() != 0 || out.status != notifyDeadStatus || out.lastError != notifyErrHost || out.httpStatus != nil {
		t.Fatalf("hits=%d outcome=%+v", rt.hits.Load(), out)
	}
	bad := p.performNotify(context.Background(), notifyTaskFor("ftp://merchant-demo/notify", 1))
	if bad.status != notifyDeadStatus || bad.lastError != notifyErrURL {
		t.Fatalf("scheme = %+v", bad)
	}
}

// NTF-02 验收 4 的签名一半：商户用另一把密钥验签，结果不是成功。
func TestNotifyTamperedKeyIsRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		err := sign.VerifyPlatform([]byte("other-merchant-key"), sign.PlatformMessage{
			Timestamp: r.Header.Get(sign.HeaderTimestamp),
			Nonce:     r.Header.Get(sign.HeaderNonce),
			Body:      body,
		}, r.Header.Get(sign.HeaderSignature))
		if err == nil {
			t.Error("tampered key accepted the signature")
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	p, _ := notifyUnit(t, "127.0.0.1")
	out := p.performNotify(context.Background(), notifyTaskFor(srv.URL, 1))
	if out.status != notifyStatusPending || out.httpStatus == nil || *out.httpStatus != http.StatusUnauthorized {
		t.Fatalf("outcome = %+v", out)
	}
}

// NTF-02 规则 1：事务外最多 20 个并发。
func TestNotifyDispatchCapsConcurrency(t *testing.T) {
	var active, peak, calls atomic.Int32
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := active.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		calls.Add(1)
		<-hold
		active.Add(-1)
		_, _ = io.WriteString(w, "SUCCESS")
	}))
	t.Cleanup(srv.Close)
	p, _ := notifyUnit(t, "127.0.0.1")
	tasks := make([]notifyTask, 25)
	for i := range tasks {
		tasks[i] = notifyTaskFor(srv.URL, 1)
		tasks[i].ID = int64(i + 1)
	}
	done := make(chan struct{})
	go func() {
		p.dispatchNotify(context.Background(), tasks)
		close(done)
	}()
	if !pollFor(5*time.Second, func() bool {
		return peak.Load() == notifyConcurrency && active.Load() == notifyConcurrency
	}) {
		t.Fatalf("peak=%d active=%d calls=%d", peak.Load(), active.Load(), calls.Load())
	}
	if calls.Load() != notifyConcurrency {
		t.Fatalf("entered = %d", calls.Load())
	}
	close(hold)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not finish")
	}
	if int(calls.Load()) != len(tasks) || peak.Load() != notifyConcurrency {
		t.Fatalf("peak=%d calls=%d", peak.Load(), calls.Load())
	}
}

// 批次 ctx 取消后不再启动新的发送。
func TestNotifyDispatchStopsWhenCanceled(t *testing.T) {
	rt := &rejectTransport{t: t}
	p, _ := notifyUnit(t, "127.0.0.1")
	p.notifyHTTP = &http.Client{Transport: rt}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.dispatchNotify(ctx, []notifyTask{notifyTaskFor("http://127.0.0.1/notify", 1)})
	if rt.hits.Load() != 0 {
		t.Fatalf("hits = %d", rt.hits.Load())
	}
}

type panicMerchants struct{}

func (panicMerchants) MerchantConfig(context.Context, string) (MerchantConfig, error) {
	panic("boom-secret")
}

func (panicMerchants) MerchantSecret(context.Context, string) (Secret, error) {
	panic("boom-secret")
}

func (panicMerchants) FeeRateAt(context.Context, string, time.Time) (FeeRate, bool, error) {
	return FeeRate{}, false, nil
}

// 单个任务 panic 被恢复，日志不写 panic 值。
func TestNotifyTaskPanicIsCounted(t *testing.T) {
	logBuf := swapNotifyLog(t)
	before := notifyTaskPanics.Load()
	p, _ := notifyUnit(t, "127.0.0.1")
	p.merchants = panicMerchants{}
	p.runNotifyTask(context.Background(), notifyTaskFor("http://127.0.0.1/notify", 1))
	if notifyTaskPanics.Load() != before+1 {
		t.Fatalf("panics = %d", notifyTaskPanics.Load())
	}
	if strings.Contains(logBuf.String(), "boom-secret") {
		t.Fatal("log contains panic value")
	}
}

// 没装发送参数、没有库、ctx 已取消，都不抢占。
func TestNotifyBatchNotReady(t *testing.T) {
	p, _, _, _, _ := newPayTest(t, "merchant-demo")
	if err := p.RunNotifyBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("unwired = %v", err)
	}
	p.UseNotify(time.Second, 5)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.RunNotifyBatch(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	if (*Payments)(nil).RunNotifyBatch(context.Background()) == nil {
		t.Fatal("nil payments")
	}
}
