//go:build integration

package biz

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
)

func notifyIntegration(t *testing.T, hosts string, interval time.Duration, max int) (payEnv, *Payments, *clock.Fake, *fakeMerchants) {
	t.Helper()
	env := openPay(t)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 11, 11, 10, 0, 0, 0, notifyShanghai))
	merchants := &fakeMerchants{
		cfg:    MerchantConfig{MerchantID: "M100001", Status: "ACTIVE"},
		secret: NewSecret(demoSecret),
	}
	p := NewPayments(merchants, &stubIDs{}, clk, hosts, env.db)
	p.UseNotify(interval, max)
	return env, p, clk, merchants
}

func payNotifyBody(payNo string) []byte {
	return []byte(`{"notify_id":"NT` + payNo + `","event_type":"payment.succeeded","merchant_id":"M100001","data":{"pay_no":"` + payNo + `","out_trade_no":"T1","amount":100,"fee_amount":1,"status":"SUCCESS","paid_at":"2026-11-11T10:00:00.000+08:00"}}`)
}

func insertDueNotify(t *testing.T, env payEnv, suffix, payNo, rawURL string, payload []byte, next time.Time) int64 {
	t.Helper()
	res, err := env.db.ExecContext(env.ctx, `
INSERT INTO merchant_notify (
  notify_no, event_id, merchant_id, biz_type, biz_no, notify_url, payload,
  status, attempt_count, next_attempt_at, created_at, updated_at
) VALUES (?, ?, 'M100001', 'PAY', ?, ?, ?, 'PENDING', 0, ?, ?, ?)`,
		"NT"+suffix, "EV"+suffix, payNo, rawURL, payload, next, next, next)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type notifySnap struct {
	status    string
	attempt   int
	next      time.Time
	http      sql.NullInt64
	last      sql.NullString
	succeeded sql.NullTime
}

func loadNotifyID(t *testing.T, env payEnv, id int64) notifySnap {
	t.Helper()
	var snap notifySnap
	err := env.db.QueryRowContext(env.ctx, `
SELECT status, attempt_count, next_attempt_at, last_http_status, last_error, succeeded_at
FROM merchant_notify WHERE id = ?`, id).Scan(&snap.status, &snap.attempt, &snap.next, &snap.http, &snap.last, &snap.succeeded)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return snap
}

// NTF-02 验收 1、4：merchant-demo 前 3 次失败，第 4 次成功，attempt_count = 4，并验过平台签名。
// 篡改密钥后 demo 返回 401，该单号不计数。
func TestNTF02DemoFailThenSuccessAndTamperedKey(t *testing.T) {
	stop := startMerchantDemo(t, "3")
	defer stop()
	env, p, clk, merchants := notifyIntegration(t, "127.0.0.1", 30*time.Second, 5)
	payNo := "PA0000000000000000101"
	now := notifyNow(clk)
	id := insertDueNotify(t, env, "0101", payNo, "http://127.0.0.1:9100/notify", payNotifyBody(payNo), now)
	before := notifyDeadAttempts.Load()
	for i := 1; i <= 4; i++ {
		if err := p.RunNotifyBatch(env.ctx); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		snap := loadNotifyID(t, env, id)
		if i < 4 {
			if snap.status != notifyStatusPending || snap.attempt != i || !snap.http.Valid || snap.http.Int64 != http.StatusInternalServerError || snap.last.String != notifyErrFailed {
				t.Fatalf("attempt %d = %+v", i, snap)
			}
			clk.Advance(30 * time.Second)
			continue
		}
		if snap.status != notifyStatusSuccess || snap.attempt != 4 || !snap.http.Valid || snap.http.Int64 != http.StatusOK || snap.last.Valid || !snap.succeeded.Valid {
			t.Fatalf("success = %+v", snap)
		}
	}
	if notifyDeadAttempts.Load() != before {
		t.Fatalf("dead counter moved")
	}
	if count := demoStatsCount(t, payNo); count != 4 {
		t.Fatalf("demo count = %d", count)
	}

	merchants.secret = NewSecret("tampered-merchant-key")
	other := "PA0000000000000000102"
	otherID := insertDueNotify(t, env, "0102", other, "http://127.0.0.1:9100/notify", payNotifyBody(other), notifyNow(clk))
	if err := p.RunNotifyBatch(env.ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	snap := loadNotifyID(t, env, otherID)
	if snap.status != notifyStatusPending || snap.attempt != 1 || !snap.http.Valid || snap.http.Int64 != http.StatusUnauthorized || snap.last.String != notifyErrFailed {
		t.Fatalf("tampered = %+v", snap)
	}
	if count := demoStatsCount(t, other); count != 0 {
		t.Fatalf("tampered count = %d", count)
	}
}

// NTF-02 验收 2：发送期间另一实例已写成 SUCCESS，失败回写影响 0 行，last_error 保持不变。
func TestNTF02WritebackZeroRows(t *testing.T) {
	env, p, clk, _ := notifyIntegration(t, "127.0.0.1", 30*time.Second, 5)
	var id int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := notifyNow(clk)
		res, err := env.db.ExecContext(r.Context(), `
UPDATE merchant_notify
   SET status = 'SUCCESS', last_error = 'kept', updated_at = ?
 WHERE id = ? AND status = 'PENDING'`, now, id)
		if err != nil {
			t.Error(testkit.Redact(err))
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			t.Errorf("competing rows = %d", n)
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "FAIL")
	}))
	t.Cleanup(srv.Close)
	now := notifyNow(clk)
	id = insertDueNotify(t, env, "0201", "PA0000000000000000201", srv.URL, payNotifyBody("PA0000000000000000201"), now)
	if err := p.RunNotifyBatch(env.ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	snap := loadNotifyID(t, env, id)
	if snap.status != notifyStatusSuccess || snap.attempt != 1 || snap.last.String != "kept" || snap.http.Valid {
		t.Fatalf("kept = %+v", snap)
	}
}

// NTF-02 验收 3：一直失败时，Fake 时钟推进后共 5 次，相邻间隔 30 秒，状态 DEAD。
func TestNTF02FiveAttemptsDead(t *testing.T) {
	env, p, clk, _ := notifyIntegration(t, "127.0.0.1", 30*time.Second, 5)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "FAIL")
	}))
	t.Cleanup(srv.Close)
	now := notifyNow(clk)
	id := insertDueNotify(t, env, "0301", "PA0000000000000000301", srv.URL, payNotifyBody("PA0000000000000000301"), now)
	before := notifyDeadAttempts.Load()
	var prev time.Time
	for i := 1; i <= 5; i++ {
		if err := p.RunNotifyBatch(env.ctx); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		snap := loadNotifyID(t, env, id)
		if i == 1 {
			clk.Advance(29 * time.Second)
			if err := p.RunNotifyBatch(env.ctx); err != nil {
				t.Fatal(testkit.Redact(err))
			}
			early := loadNotifyID(t, env, id)
			if early.attempt != 1 || early.status != notifyStatusPending {
				t.Fatalf("early = %+v", early)
			}
			clk.Advance(time.Second)
		}
		if i < 5 {
			if snap.status != notifyStatusPending || snap.attempt != i {
				t.Fatalf("attempt %d = %+v", i, snap)
			}
			if i > 1 && snap.next.Sub(prev) != 30*time.Second {
				t.Fatalf("gap %s", snap.next.Sub(prev))
			}
			prev = snap.next
			if i > 1 {
				clk.Advance(30 * time.Second)
			}
			continue
		}
		if snap.status != notifyDeadStatus || snap.attempt != 5 || snap.last.String != notifyErrFailed {
			t.Fatalf("dead = %+v", snap)
		}
		if snap.next.Sub(prev) != 30*time.Second {
			t.Fatalf("last gap %s", snap.next.Sub(prev))
		}
	}
	if notifyDeadAttempts.Load() != before+1 {
		t.Fatalf("dead counter = %d", notifyDeadAttempts.Load()-before)
	}
}

// NTF-02 规则 5：不在白名单时不发送，CAS 成 DEAD，计数加 1。
func TestNTF02HostNotAllowedIsDead(t *testing.T) {
	env, p, clk, _ := notifyIntegration(t, "merchant-demo", 30*time.Second, 5)
	now := notifyNow(clk)
	id := insertDueNotify(t, env, "0401", "PA0000000000000000401", "http://evil.example/notify", payNotifyBody("PA0000000000000000401"), now)
	before := notifyDeadAttempts.Load()
	if err := p.RunNotifyBatch(env.ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	snap := loadNotifyID(t, env, id)
	if snap.status != notifyDeadStatus || snap.attempt != 1 || snap.last.String != notifyErrHost || snap.http.Valid {
		t.Fatalf("host = %+v", snap)
	}
	if notifyDeadAttempts.Load() != before+1 {
		t.Fatalf("dead counter = %d", notifyDeadAttempts.Load()-before)
	}
}

// 密钥为空时 5 次都不发送。第 5 次 DEAD，last_error 只有 internal error。
func TestNTF02EmptySecretBecomesDead(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits++
	}))
	t.Cleanup(srv.Close)
	env, p, clk, merchants := notifyIntegration(t, "127.0.0.1", 30*time.Second, 5)
	merchants.secret = NewSecret("")
	now := notifyNow(clk)
	id := insertDueNotify(t, env, "0501", "PA0000000000000000501", srv.URL, payNotifyBody("PA0000000000000000501"), now)
	before := notifyDeadAttempts.Load()
	for i := 1; i <= 5; i++ {
		if err := p.RunNotifyBatch(env.ctx); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if i < 5 {
			clk.Advance(30 * time.Second)
		}
	}
	snap := loadNotifyID(t, env, id)
	if hits != 0 || snap.status != notifyDeadStatus || snap.attempt != 5 || snap.last.String != notifyErrInternal || snap.http.Valid {
		t.Fatalf("hits=%d snap=%+v", hits, snap)
	}
	if notifyDeadAttempts.Load() != before+1 {
		t.Fatalf("dead counter = %d", notifyDeadAttempts.Load()-before)
	}
}

// 未装配时不抢占。还没到期的行也不抢。
func TestNTF02SkipsUnwiredAndFuture(t *testing.T) {
	env := openPay(t)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 11, 11, 12, 0, 0, 0, notifyShanghai))
	now := notifyNow(clk)
	id := insertDueNotify(t, env, "0601", "PA0000000000000000601", "http://127.0.0.1/notify", payNotifyBody("PA0000000000000000601"), now)
	p := NewPayments(&fakeMerchants{
		cfg:    MerchantConfig{MerchantID: "M100001", Status: "ACTIVE"},
		secret: NewSecret(demoSecret),
	}, &stubIDs{}, clk, "127.0.0.1", env.db)
	if err := p.RunNotifyBatch(env.ctx); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("unwired = %v", err)
	}
	if loadNotifyID(t, env, id).attempt != 0 {
		t.Fatal("unwired claimed")
	}
	p.UseNotify(30*time.Second, 5)
	future := insertDueNotify(t, env, "0602", "PA0000000000000000602", "http://127.0.0.1/notify", payNotifyBody("PA0000000000000000602"), now.Add(time.Second))
	if err := p.RunNotifyBatch(env.ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got := loadNotifyID(t, env, future); got.attempt != 0 || got.status != notifyStatusPending {
		t.Fatalf("future = %+v", got)
	}
	if loadNotifyID(t, env, id).attempt != 1 {
		t.Fatal("due row was not claimed")
	}
}

// NTF-02 规则 1：别的事务锁住一行时，SKIP LOCKED 拿走另一行。
func TestNTF02SkipLocked(t *testing.T) {
	env, p, clk, _ := notifyIntegration(t, "127.0.0.1", 30*time.Second, 5)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "SUCCESS")
	}))
	t.Cleanup(srv.Close)
	now := notifyNow(clk)
	held := insertDueNotify(t, env, "0701", "PA0000000000000000701", srv.URL, payNotifyBody("PA0000000000000000701"), now)
	free := insertDueNotify(t, env, "0702", "PA0000000000000000702", srv.URL, payNotifyBody("PA0000000000000000702"), now)
	tx, err := env.db.BeginTxx(env.ctx, nil)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	var locked int64
	if err = tx.QueryRowContext(env.ctx, `SELECT id FROM merchant_notify WHERE id = ? FOR UPDATE`, held).Scan(&locked); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err = p.RunNotifyBatch(env.ctx); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if loadNotifyID(t, env, held).attempt != 0 || loadNotifyID(t, env, free).status != notifyStatusSuccess {
		t.Fatalf("held=%+v free=%+v", loadNotifyID(t, env, held), loadNotifyID(t, env, free))
	}
}

func startMerchantDemo(t *testing.T, failFirstN string) func() {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/merchant-demo")
	cmd.Dir = root
	// go run 的子进程会继承标准输出管道。只杀掉 go 本身时，子进程还占着管道，Wait 会一直等。
	// 独立进程组让取消时能一起杀掉 go 和真正监听的 demo。
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 3 * time.Second
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.Env = replaceEnv(os.Environ(),
		"WORKER_ID=1",
		"FAIL_FIRST_N="+failFirstN,
		"FAULT_POINTS=",
		"DEMO_APP_SECRET_FORMAT=demo_secret_%s_0123456789ab",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	stop := func() {
		cancel()
		_ = cmd.Wait()
	}
	if !pollFor(60*time.Second, func() bool {
		dialer := net.Dialer{}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		conn, dialErr := dialer.DialContext(ctx, "tcp", "127.0.0.1:9100")
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}) {
		stop()
		t.Fatalf("merchant-demo did not listen: %s", redactDemo(stderr.String()+"\n"+stdout.String()))
	}
	return stop
}

func demoStatsCount(t *testing.T, payNo string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:9100/stats?biz_no="+payNo, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	var view struct {
		Count json.Number `json:"count"`
	}
	if err = dec.Decode(&view); err != nil {
		t.Fatal(err)
	}
	n, err := view.Count.Int64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func replaceEnv(base []string, kv ...string) []string {
	drop := map[string]struct{}{}
	for _, item := range kv {
		key, _, _ := strings.Cut(item, "=")
		drop[key] = struct{}{}
	}
	out := make([]string, 0, len(base)+len(kv))
	for _, item := range base {
		key, _, _ := strings.Cut(item, "=")
		if _, ok := drop[key]; ok {
			continue
		}
		out = append(out, item)
	}
	return append(out, kv...)
}

func redactDemo(s string) string {
	return strings.ReplaceAll(s, "demo_secret_", "redacted_")
}
