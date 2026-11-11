package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/sign"
)

const gw04Now int64 = 1_791_500_000

func gw04Clock() *clock.Fake {
	clk := &clock.Fake{}
	clk.Set(time.Unix(gw04Now, 0))
	return clk
}

func gw04Nonce() string {
	return fmt.Sprintf("n%015d", gw04Seq.Add(1))
}

var gw04Seq atomic.Uint64

type callNonce struct {
	mu    sync.Mutex
	n     int
	err   error
	inner NonceStore
}

func (s *callNonce) Reserve(ctx context.Context, appID, nonce string) (bool, error) {
	s.mu.Lock()
	s.n++
	err := s.err
	inner := s.inner
	s.mu.Unlock()
	if err != nil {
		return false, err
	}
	if inner == nil {
		return false, errNonceUnavailable
	}
	return inner.Reserve(ctx, appID, nonce)
}

func (s *callNonce) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

func gw04Start(t *testing.T, store NonceStore, look *fakeLookup, logs *logBuf) (string, *hitBox) {
	t.Helper()
	opts := []Option{WithClock(gw04Clock()), WithMerchantLookup(look)}
	if store != nil {
		opts = append(opts, WithNonceStore(store))
	}
	if logs != nil {
		opts = append(opts, WithLogger(logx.New("gateway", logs)))
	}
	var hit hitBox
	addr := startGW(t, NewMerchant(opts...), func(r *server.Hertz) {
		r.POST("/v1/gw04", func(_ context.Context, c *app.RequestContext) {
			id, ok := MerchantID(c)
			hit.mark(id, ok)
			WriteOK(c, map[string]int{"n": 1})
		})
	})
	return addr, &hit
}

// GW-04 验收 1：超前或落后 301 秒返回 20002；299 秒和恰好 ±300 秒进入 handler。
// 非整数时间戳、15 位、33 位、含符号的 nonce 返回 20001，不查商户，不访问存储。
func TestGW04Window(t *testing.T) {
	look := gw02Lookup()
	store := &callNonce{inner: NewMemoryNonce()}
	addr, hit := gw04Start(t, store, look, nil)
	body := []byte(`{"n":1}`)
	const path = "/v1/gw04"
	before := nonceUnavailable.Load()

	reject := func(t *testing.T, ts, nonce string, want *errno.Error) {
		t.Helper()
		lookups := len(look.calls())
		stores := store.calls()
		hits, _, _ := hit.snapshot()
		hdr := http.Header{}
		hdr.Set(sign.HeaderAppID, gw03AppID)
		hdr.Set(sign.HeaderTimestamp, ts)
		hdr.Set(sign.HeaderNonce, nonce)
		hdr.Set(sign.HeaderSignature, "x")
		got := do(t, http.MethodPost, "http://"+addr+path, body, hdr)
		assertErrno(t, got, want)
		if len(look.calls()) != lookups || store.calls() != stores {
			t.Fatalf("lookup %d store %d", len(look.calls()), store.calls())
		}
		if n, _, _ := hit.snapshot(); n != hits {
			t.Fatal("handler called")
		}
	}
	pass := func(t *testing.T, delta int64) {
		t.Helper()
		ts := strconv.FormatInt(gw04Now+delta, 10)
		nonce := gw04Nonce()
		hits, _, _ := hit.snapshot()
		hdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, gw03AppID, ts, nonce, body)
		got := do(t, http.MethodPost, "http://"+addr+path, body, hdr)
		if got.status != http.StatusOK {
			t.Fatalf("delta %d status %d body %s", delta, got.status, got.body)
		}
		n, id, ok := hit.snapshot()
		if n != hits+1 || !ok || id != gw03MID {
			t.Fatalf("delta %d hit n=%d id=%q ok=%v", delta, n, id, ok)
		}
	}

	reject(t, strconv.FormatInt(gw04Now+301, 10), gw03Nonce, errno.ErrTimestampExpired)
	reject(t, strconv.FormatInt(gw04Now-301, 10), gw03Nonce, errno.ErrTimestampExpired)
	pass(t, 299)
	pass(t, -299)
	pass(t, 300)
	pass(t, -300)
	reject(t, "1791500000.0", gw03Nonce, errno.ErrSignInvalid)
	reject(t, strconv.FormatInt(gw04Now, 10), strings.Repeat("b", 15), errno.ErrSignInvalid)
	reject(t, strconv.FormatInt(gw04Now, 10), strings.Repeat("c", 33), errno.ErrSignInvalid)
	reject(t, strconv.FormatInt(gw04Now, 10), "abcdefghijklmnop!", errno.ErrSignInvalid)
	if nonceUnavailable.Load() != before {
		t.Fatalf("counter %d want %d", nonceUnavailable.Load(), before)
	}
}

// GW-04 验收 2：同一 app_id 与 nonce 第二次返回 20003；签名错误不占用；另一个 app_id 仍可使用这个 nonce。
func TestGW04NonceReplay(t *testing.T) {
	look := gw02Lookup()
	store := &callNonce{inner: NewMemoryNonce()}
	addr, hit := gw04Start(t, store, look, nil)
	body := []byte(`{"n":1}`)
	const path = "/v1/gw04"
	const nonce = "ReplayOk0123456789"
	ts := strconv.FormatInt(gw04Now, 10)
	before := nonceUnavailable.Load()

	bad := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, gw03AppID, ts, nonce, body)
	bad.Set(sign.HeaderSignature, "not-a-real-signature")
	got := do(t, http.MethodPost, "http://"+addr+path, body, bad)
	assertErrno(t, got, errno.ErrSignInvalid)
	if store.calls() != 0 {
		t.Fatalf("bad signature occupied nonce, store %d", store.calls())
	}
	if n, _, _ := hit.snapshot(); n != 0 {
		t.Fatal("handler called")
	}
	if nonceUnavailable.Load() != before {
		t.Fatal("verify failure incremented the counter")
	}

	okHdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, gw03AppID, ts, nonce, body)
	got = do(t, http.MethodPost, "http://"+addr+path, body, okHdr)
	if got.status != http.StatusOK {
		t.Fatalf("first status %d body %s", got.status, got.body)
	}
	if n, _, _ := hit.snapshot(); n != 1 || store.calls() != 1 {
		t.Fatalf("hit/store after first")
	}

	got = do(t, http.MethodPost, "http://"+addr+path, body, okHdr)
	assertSignedErrno(t, got, errno.ErrNonceReplayed)
	if n, _, _ := hit.snapshot(); n != 1 {
		t.Fatal("handler called on replay")
	}
	if nonceUnavailable.Load() != before {
		t.Fatal("replay incremented the counter")
	}

	const other = "app_other_100002"
	otherHdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, other, ts, nonce, body)
	got = do(t, http.MethodPost, "http://"+addr+path, body, otherHdr)
	if got.status != http.StatusOK {
		t.Fatalf("other app status %d body %s", got.status, got.body)
	}
	if n, _, _ := hit.snapshot(); n != 2 {
		t.Fatal("other app did not reach the handler")
	}
}

// GW-04 验收 3：存储返回错误或未安装时，商户 /v1 返回 503 / 10004，计数加 1，响应带平台签名，handler 不执行。
func TestGW04NonceUnavailable(t *testing.T) {
	body := []byte(`{"n":1}`)
	const path = "/v1/gw04"
	ts := strconv.FormatInt(gw04Now, 10)

	t.Run("store error", func(t *testing.T) {
		nonce := gw04Nonce()
		logs := newLogBuf()
		store := &callNonce{err: errors.New("down " + nonce)}
		addr, hit := gw04Start(t, store, gw02Lookup(), logs)
		before := nonceUnavailable.Load()
		hdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, gw03AppID, ts, nonce, body)
		got := do(t, http.MethodPost, "http://"+addr+path, body, hdr)
		assertSignedErrno(t, got, errno.ErrUnavailable)
		if nonceUnavailable.Load() != before+1 {
			t.Fatalf("counter %d want %d", nonceUnavailable.Load(), before+1)
		}
		if n, _, _ := hit.snapshot(); n != 0 || store.calls() != 1 {
			t.Fatal("handler called or store skipped")
		}
		text := waitLog(t, logs)
		if strings.Contains(text, nonce) || strings.Contains(text, gw03Secret) {
			t.Fatalf("log leaked: %s", text)
		}
	})

	t.Run("nil store", func(t *testing.T) {
		addr, hit := gw04Start(t, nil, gw02Lookup(), nil)
		before := nonceUnavailable.Load()
		nonce := gw04Nonce()
		hdr := signedHeader(t, []byte(gw03Secret), http.MethodPost, path, gw03AppID, ts, nonce, body)
		got := do(t, http.MethodPost, "http://"+addr+path, body, hdr)
		assertSignedErrno(t, got, errno.ErrUnavailable)
		if nonceUnavailable.Load() != before+1 {
			t.Fatalf("counter %d want %d", nonceUnavailable.Load(), before+1)
		}
		if n, _, _ := hit.snapshot(); n != 0 {
			t.Fatal("handler called")
		}
	})
}

// GW-04 验收 4：nonce 存储一直返回错误时，渠道回调仍调用 payment notify，且存储调用次数为 0。
func TestGW04CallbackWithoutNonce(t *testing.T) {
	store := &callNonce{err: errors.New("redis down")}
	fake := &gw06Notify{}
	clk := gw04Clock()
	key := []byte("mockpay-plain-key")
	before := nonceUnavailable.Load()
	addr := startGW(t, NewMerchant(
		WithClock(clk),
		WithChannelKey(key),
		WithNotifyAPI(fake),
		WithNonceStore(store),
	), nil)
	ts := strconv.FormatInt(clk.Now().Unix(), 10)
	got := gw06Post(t, addr, key, ts, []byte(gw06Body))
	if got.status != http.StatusOK || string(got.body) != "SUCCESS" {
		t.Fatalf("status %d body %s", got.status, got.body)
	}
	if fake.count() != 1 {
		t.Fatalf("notify calls %d", fake.count())
	}
	last := fake.snapshot()
	if last == nil || last.Channel != "mockpay" || last.TxnType != "PAY" {
		t.Fatalf("notify %+v", last)
	}
	if store.calls() != 0 {
		t.Fatalf("nonce store calls %d", store.calls())
	}
	if nonceUnavailable.Load() != before {
		t.Fatal("callback incremented the nonce counter")
	}
}

// GW-04：内存存储并发下，同一个 app_id 与 nonce 只有一次占用成功。
func TestGW04MemoryNonce(t *testing.T) {
	store := NewMemoryNonce()
	const appID = "app_mem"
	const nonce = "MemoryNonce01234567"
	var wg sync.WaitGroup
	var wins atomic.Int32
	var bad atomic.Int32
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := store.Reserve(context.Background(), appID, nonce)
			if err != nil {
				bad.Add(1)
				return
			}
			if ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if bad.Load() != 0 || wins.Load() != 1 {
		t.Fatalf("wins %d bad %d", wins.Load(), bad.Load())
	}
	ok, err := store.Reserve(context.Background(), "app_other", nonce)
	if err != nil || !ok {
		t.Fatalf("other app claimed %v err %v", ok, err)
	}
}
