package biz

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/sign"
)

const (
	// notifyBatchSize 是一轮抢占的行数上限。剩下的下一秒再抢。
	notifyBatchSize = 100
	// notifyConcurrency 是事务外并发发送的上限（NTF-02 第 1 条）。一个失败不取消其他任务。
	notifyConcurrency = 20
	// notifyHTTPTimeout 是单次 POST 的上限，也是这一轮任务的上限。不随批次 ctx 取消。
	notifyHTTPTimeout = 5 * time.Second
	// notifyMaxBody 是响应体最多读取的字节。再多的部分不参与成功判定。
	notifyMaxBody = 1024
	// notifyClaimTimeout 只包住抢占事务。与 dbx 的 3 秒预算一致。
	notifyClaimTimeout = 3 * time.Second
	// notifyNonceLen 与网关响应 nonce 一样，16 位 [A-Za-z0-9]。商户通知不做 nonce 去重。
	notifyNonceLen = 16

	notifyDeadStatus = "DEAD"

	// notifyErrInternal 是签名或取密钥失败时写入 last_error 的唯一文本。不含原因细节。
	notifyErrInternal = "internal error"
	// notifyErrFailed 是发出了请求但没有收到 SUCCESS 时的 last_error。不写入响应体。
	notifyErrFailed = "notify failed"
	// notifyErrHost 是主机不在白名单时的 last_error。不写入完整 URL。
	notifyErrHost = "host not allowed"
	// notifyErrURL 是地址无法解析、没有主机或不是 http/https 时的 last_error。
	notifyErrURL = "notify url invalid"

	notifyBodySuccess = "SUCCESS"

	notifyNonceAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	// 62*4 = 248。大于等于 248 的随机字节丢弃，避免取模偏向。
	notifyNonceUnbiasedMax = 248
)

// notifyDeadAttempts 是 pay_notify_attempts_total{result="dead"}。
// 只有 CAS 真正把一行写成 DEAD 才加 1。进程内计数，没有 Prometheus。
var notifyDeadAttempts atomic.Uint64

// notifyTaskPanics 是单个发送任务 panic 被恢复的次数。不记录 panic 值。
var notifyTaskPanics atomic.Uint64

// notifyHTTPClient 不跟随重定向。超时与单次任务相同。
var notifyHTTPClient = &http.Client{
	Timeout: notifyHTTPTimeout,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// notifyTask 是一条已经抢占的 PENDING 通知。Attempt 是加 1 之后的次数。
// NextAttemptAt 是抢占时推后的时刻。本轮发送不再改这两列。
type notifyTask struct {
	ID            int64
	NotifyNo      string
	MerchantID    string
	BizType       string
	BizNo         string
	NotifyURL     string
	Payload       []byte
	Attempt       int
	NextAttemptAt time.Time
}

// notifyOutcome 是一次发送的回写内容。HTTP 在事务外完成，回写另走 CAS。
type notifyOutcome struct {
	status     string
	httpStatus *int
	lastError  string
	// signFailed 为真时已经打过不含密钥的 ERROR。回写 0 行也不再补日志。
	signFailed bool
}

// UseNotify 装上固定间隔和次数上限。maxAttempts 小于 1 时不装，RunNotifyBatch 返回 10003。
// interval 来自 NOTIFY_RETRY_INTERVAL，可以是 0：推后的 next_attempt_at 就是现在，下一轮扫描会再抢。
func (p *Payments) UseNotify(interval time.Duration, maxAttempts int) {
	if p == nil || maxAttempts < 1 {
		return
	}
	p.notifyInterval = interval
	p.notifyMaxAttempts = maxAttempts
	p.notifyOn = true
	if p.notifyHTTP == nil {
		p.notifyHTTP = notifyHTTPClient
	}
}

// RunNotifyBatch 抢占一批到期的 PENDING 通知，提交后在事务外最多 20 并发发送，再按结果 CAS 回写。
// 未装配、没有时钟或不是 MySQL 存储时返回 10003，不抢占。单个任务的失败只记日志。
// ctx 取消后不再启动新任务。已开始的任务用自己的 5 秒超时，不随批次取消。
func (p *Payments) RunNotifyBatch(ctx context.Context) error {
	if p == nil || !p.notifyOn || p.clk == nil || p.notifyMaxAttempts < 1 {
		return errno.ErrInternal
	}
	if ctx == nil {
		return errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db, ok := p.paymentDB()
	if !ok {
		return errno.ErrInternal
	}
	now := notifyNow(p.clk)
	claimCtx, cancel := context.WithTimeout(ctx, notifyClaimTimeout)
	tasks, err := (sqlOrderStore{db: db}).claimNotifyTasks(claimCtx, now, p.notifyInterval)
	cancel()
	if err != nil {
		return callErr(err)
	}
	p.dispatchNotify(ctx, tasks)
	return nil
}

// dispatchNotify 以最多 notifyConcurrency 个并发发送。等槽时 ctx 取消就不再启动后面的行。
func (p *Payments) dispatchNotify(ctx context.Context, tasks []notifyTask) {
	slots := make(chan struct{}, notifyConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-slots
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			p.runNotifyTask(ctx, task)
		}()
	}
}

// runNotifyTask 用自己的超时发送一条，不随批次 ctx 取消。panic 被恢复并计数，不记录 panic 值。
func (p *Payments) runNotifyTask(ctx context.Context, task notifyTask) {
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), notifyHTTPTimeout)
	defer cancel()
	defer func() {
		if recover() != nil {
			notifyTaskPanics.Add(1)
			slog.ErrorContext(taskCtx, "merchant notify task panicked",
				"merchant_id", task.MerchantID,
				"biz_no", task.BizNo,
			)
		}
	}()
	out := p.performNotify(taskCtx, task)
	p.applyNotify(taskCtx, task, out)
}

// performNotify 在事务外决定这一次的结果。白名单失败和签名失败都不发 HTTP。
func (p *Payments) performNotify(ctx context.Context, task notifyTask) notifyOutcome {
	if reason, ok := p.notifyHost(task.NotifyURL); !ok {
		return notifyOutcome{status: notifyDeadStatus, lastError: reason}
	}
	secret, err := p.notifySecret(ctx, task.MerchantID)
	if err != nil {
		p.logNotifyInternal(ctx, task)
		return p.notifyAttemptFailed(task, nil, notifyErrInternal, true)
	}
	nonce, err := p.nextNotifyNonce()
	if err != nil {
		p.logNotifyInternal(ctx, task)
		return p.notifyAttemptFailed(task, nil, notifyErrInternal, true)
	}
	ts := sign.FormatTimestamp(p.clk.Now())
	signature, err := sign.SignPlatform(secret, sign.PlatformMessage{
		Timestamp: ts,
		Nonce:     nonce,
		Body:      task.Payload,
	})
	if err != nil {
		p.logNotifyInternal(ctx, task)
		return p.notifyAttemptFailed(task, nil, notifyErrInternal, true)
	}
	status, ok := p.postNotify(ctx, task, ts, nonce, signature)
	if ok {
		code := status
		return notifyOutcome{status: notifyStatusSuccess, httpStatus: &code}
	}
	var code *int
	if status > 0 {
		copied := status
		code = &copied
	}
	return p.notifyAttemptFailed(task, code, notifyErrFailed, false)
}

// notifyHost 再查一次主机。白名单不会因重试改变。不解析 DNS。
func (p *Payments) notifyHost(raw string) (string, bool) {
	if raw == "" || !utf8.ValidString(raw) {
		return notifyErrURL, false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return notifyErrURL, false
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return notifyErrURL, false
	}
	host := parsed.Hostname()
	if host == "" || !p.hostAllowed(host) {
		if host == "" {
			return notifyErrURL, false
		}
		return notifyErrHost, false
	}
	return "", true
}

// notifyKeySource 按 merchant_id 取商户密钥。通知行没有 app_id，也不能走会缓存的 MerchantConfig。
type notifyKeySource interface {
	MerchantSecret(ctx context.Context, merchantID string) (Secret, error)
}

func (p *Payments) notifySecret(ctx context.Context, merchantID string) ([]byte, error) {
	src, ok := p.merchants.(notifyKeySource)
	if !ok || src == nil {
		return nil, errno.ErrInternal
	}
	secret, err := src.MerchantSecret(ctx, merchantID)
	if err != nil {
		return nil, callErr(err)
	}
	plain := secret.Reveal()
	if plain == "" {
		return nil, errno.ErrInternal
	}
	return []byte(plain), nil
}

func (p *Payments) nextNotifyNonce() (string, error) {
	if p.notifyNonce != nil {
		return p.notifyNonce()
	}
	return randomNotifyNonce()
}

func (p *Payments) postNotify(ctx context.Context, task notifyTask, ts, nonce, signature string) (int, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, task.NotifyURL, bytes.NewReader(task.Payload))
	if err != nil {
		return 0, false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(sign.HeaderTimestamp, ts)
	req.Header.Set(sign.HeaderNonce, nonce)
	req.Header.Set(sign.HeaderSignature, signature)
	client := p.notifyHTTP
	if client == nil {
		client = notifyHTTPClient
	}
	// 主机已在 notifyHost 对照 NOTIFY_URL_ALLOW_HOSTS 放过，这里不再解析调用方传入的其它地址。
	resp, err := client.Do(req) //nolint:gosec // G704：URL 来自已抢占的通知行，主机白名单在发送前再查过。
	if err != nil {
		return 0, false
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, notifyMaxBody))
	if err != nil {
		return resp.StatusCode, false
	}
	if resp.StatusCode == http.StatusOK && strings.TrimSpace(string(body)) == notifyBodySuccess {
		return resp.StatusCode, true
	}
	return resp.StatusCode, false
}

// notifyAttemptFailed 把这一次算作发送失败。次数已经在抢占时加过，达到上限就 DEAD。
func (p *Payments) notifyAttemptFailed(task notifyTask, httpStatus *int, lastError string, signFailed bool) notifyOutcome {
	status := notifyStatusPending
	if task.Attempt >= p.notifyMaxAttempts {
		status = notifyDeadStatus
	}
	return notifyOutcome{
		status:     status,
		httpStatus: httpStatus,
		lastError:  lastError,
		signFailed: signFailed,
	}
}

func (p *Payments) logNotifyInternal(ctx context.Context, task notifyTask) {
	slog.ErrorContext(ctx, "merchant notify sign failed",
		"merchant_id", task.MerchantID,
		"biz_no", task.BizNo,
		"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
	)
}

// applyNotify 用 CAS 回写。影响 0 行就丢掉这次结果，不加 dead 计数，也不再更新。
func (p *Payments) applyNotify(ctx context.Context, task notifyTask, out notifyOutcome) {
	db, ok := p.paymentDB()
	if !ok {
		return
	}
	now := notifyNow(p.clk)
	var succeeded *time.Time
	if out.status == notifyStatusSuccess {
		succeeded = &now
	}
	wrote, err := (sqlOrderStore{db: db}).finishNotify(ctx, task.ID, out, succeeded, now)
	if err != nil {
		slog.ErrorContext(ctx, "merchant notify writeback failed",
			"merchant_id", task.MerchantID,
			"biz_no", task.BizNo,
			"err_code", strconv.Itoa(int(errno.From(callErr(err)).Code)),
		)
		return
	}
	if !wrote {
		return
	}
	switch out.status {
	case notifyStatusSuccess:
		obs.AddNotifyAttempt("success")
	case notifyDeadStatus:
		notifyDeadAttempts.Add(1)
		obs.AddNotifyAttempt("dead")
		slog.ErrorContext(ctx, "merchant notify dead",
			"merchant_id", task.MerchantID,
			"biz_no", task.BizNo,
		)
	default:
		obs.AddNotifyAttempt("failed")
	}
}

func randomNotifyNonce() (string, error) {
	out := make([]byte, notifyNonceLen)
	buf := make([]byte, notifyNonceLen)
	filled := 0
	for filled < notifyNonceLen {
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		for _, b := range buf {
			if b >= notifyNonceUnbiasedMax {
				continue
			}
			out[filled] = notifyNonceAlphabet[int(b)%len(notifyNonceAlphabet)]
			filled++
			if filled == notifyNonceLen {
				break
			}
		}
	}
	return string(out), nil
}
