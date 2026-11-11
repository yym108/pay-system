package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/gopkg/cloud/metainfo"
	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/sign"
)

const (
	nameRecovery   = "recovery"
	nameRequestID  = "request_id"
	nameTrace      = "trace"
	nameAccessLog  = "access_log"
	nameTimeWindow = "time_window"
	nameVerifySign = "verify_sign"
	nameRateLimit  = "rate_limit"
	nameNonce      = "nonce"

	// maxBodyBytes 是请求体上限。再多 1 字节就拒绝，恰好等于上限的请求通过。
	maxBodyBytes = 64 * 1024
)

// bodyLimit 是第一条中间件。流式模式下 Hertz 不会自己写纯文本 413。
func (h *Handler) bodyLimit(ctx context.Context, c *app.RequestContext) {
	buf, tooBig, err := readCapped(c.RequestBodyStream(), maxBodyBytes)
	if tooBig {
		// 已经读到 max+1。不要再读剩余 body。
		h.rejectBody(ctx, c)
		return
	}
	if err != nil {
		id := h.ensureRequestID(ctx, c)
		h.log().ErrorContext(logx.WithRequestID(ctx, id), "read body failed")
		writeErrno(c, http.StatusInternalServerError, errno.ErrInternal, id)
		c.SetConnectionClose()
		c.Abort()
		return
	}
	c.Request.SetBody(buf)
	c.Next(ctx)
}

func (h *Handler) rejectBody(ctx context.Context, c *app.RequestContext) {
	id := h.ensureRequestID(ctx, c)
	// 413 用 10001 的对外文案，但 HTTP 状态不是该码默认的 400。不签名。
	writeErrno(c, http.StatusRequestEntityTooLarge, errno.ErrInvalidParam, id)
	c.SetConnectionClose()
	c.Abort()
}

func (h *Handler) ensureRequestID(ctx context.Context, c *app.RequestContext) string {
	if id := requestID(c); id != "" {
		return id
	}
	id, err := newRequestID()
	if err != nil {
		h.log().ErrorContext(ctx, "request id failed")
		return ""
	}
	c.Set(requestIDKey, id)
	return id
}

func (h *Handler) recovery(ctx context.Context, c *app.RequestContext) {
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		// 不把 panic 值写进响应或日志，避免内部文本和密钥出去。
		id := requestID(c)
		h.log().ErrorContext(logx.WithRequestID(ctx, id), "panic recovered")
		writeErrno(c, http.StatusInternalServerError, errno.ErrInternal, id)
		c.Abort()
	}()
	appendName(c, nameRecovery)
	c.Next(ctx)
}

func (h *Handler) requestID(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameRequestID)
	id, err := newRequestID()
	if err != nil {
		h.log().ErrorContext(ctx, "request id failed")
		writeErrno(c, http.StatusInternalServerError, errno.ErrInternal, "")
		c.Abort()
		return
	}
	// 忽略客户端同名头，始终换成服务端生成的值。
	c.Set(requestIDKey, id)
	ctx = logx.WithRequestID(ctx, id)
	c.Next(ctx)
	if len(c.Response.Header.Peek(headerRequestID)) == 0 {
		c.Response.Header.Set(headerRequestID, id)
	}
}

// trace 先提取入站 traceparent，再开一个 span。导出器由进程启动时的 obs.Install 安装，这里不安装。
func (h *Handler) trace(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameTrace)
	path := string(c.Request.URI().Path())
	chain, timed := merchantStage(string(c.Method()), path)
	var watch *obs.AuthWatch
	if timed {
		// 商户写路由从这里开始记 gw_auth。RPC 等待由路由里 Pause，响应写完再 Observe。
		watch = obs.StartAuth()
		c.Set(gwAuthKey, watch)
	}
	header := map[string]string{}
	if v := c.Request.Header.Peek("traceparent"); len(v) > 0 {
		header["traceparent"] = string(v)
	}
	if v := c.Request.Header.Peek("tracestate"); len(v) > 0 {
		header["tracestate"] = string(v)
	}
	ctx = obs.ExtractMap(ctx, header)
	ctx, span := otel.Tracer("gateway").Start(ctx, path)
	defer span.End()
	c.Next(ctx)
	if watch == nil {
		return
	}
	if chain != "" {
		watch.Observe(ctx, chain)
		return
	}
	mode, _ := c.Get(gwTransferModeKey)
	text, _ := mode.(string)
	watch.ObserveTransfer(ctx, text)
}

// merchantStage 按商户写路由选择链。转账在验签时还不知道分片，chain 留空。
func merchantStage(method, path string) (string, bool) {
	if method != http.MethodPost {
		return "", false
	}
	switch path {
	case "/v1/payments":
		return obs.ChainPayCreate, true
	case "/v1/refunds":
		return obs.ChainRefund, true
	case "/v1/transfers":
		return "", true
	}
	const prefix = "/v1/payments/"
	const suffix = "/pay"
	if strings.HasPrefix(path, prefix) && strings.HasSuffix(path, suffix) {
		payNo := path[len(prefix) : len(path)-len(suffix)]
		if payNo != "" && !strings.Contains(payNo, "/") {
			return obs.ChainPaySubmit, true
		}
	}
	return "", false
}

func setPayNo(ctx context.Context, payNo string) {
	obs.SetOrder(trace.SpanFromContext(ctx), payNo, "", "", "", "")
}

// accessLog 只记路径、状态、耗时、空商户号和业务码。不记 body、签名、Token、密钥。
func (h *Handler) accessLog(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameAccessLog)
	start := h.clk.Now()
	c.Next(ctx)
	code := ""
	if v, ok := c.Get(bizCodeKey); ok {
		if n, ok := v.(int32); ok {
			code = strconv.FormatInt(int64(n), 10)
		}
	}
	elapsed := h.clk.Now().Sub(start)
	if code == "" {
		code = strconv.Itoa(c.Response.StatusCode())
	}
	h.log().InfoContext(ctx, "request handled",
		"path", string(c.Request.URI().Path()),
		"status", c.Response.StatusCode(),
		"elapsed_ms", elapsed.Milliseconds(),
		"merchant_id", "",
		"err_code", code,
	)
	// 路由模板不含 pay_no。未匹配时 FullPath 为空，由 obs 记成 unmatched。
	obs.ObserveHTTP(string(c.FullPath()), code, elapsed)
}

const merchantStatusActive = "ACTIVE"

// timeWindow 只处理商户 /v1，且四个头都非空。缺任一头不在这里拒绝，交给验签返回 20001。
func (h *Handler) timeWindow(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameTimeWindow)
	if h.admin || !v1Path(string(c.Request.URI().Path())) {
		c.Next(ctx)
		return
	}
	appID := string(c.GetHeader(sign.HeaderAppID))
	tsText := string(c.GetHeader(sign.HeaderTimestamp))
	nonce := string(c.GetHeader(sign.HeaderNonce))
	signature := string(c.GetHeader(sign.HeaderSignature))
	if appID == "" || tsText == "" || nonce == "" || signature == "" {
		c.Next(ctx)
		return
	}
	if !noncePattern.MatchString(nonce) {
		h.failWindow(ctx, c, appID, errno.ErrSignInvalid)
		return
	}
	ts, err := strconv.ParseInt(tsText, 10, 64)
	if err != nil {
		h.failWindow(ctx, c, appID, errno.ErrSignInvalid)
		return
	}
	if timestampOutside(h.clk.Now().Unix(), ts) {
		h.failWindow(ctx, c, appID, errno.ErrTimestampExpired)
		return
	}
	c.Next(ctx)
}

func (h *Handler) failWindow(ctx context.Context, c *app.RequestContext, appID string, e *errno.Error) {
	if e == nil {
		e = errno.ErrSignInvalid
	}
	h.logGate(ctx, c, appID, e, "timestamp window rejected")
	writeErrno(c, e.HTTPStatus, e, requestID(c))
	c.Abort()
}

func (h *Handler) verifySign(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameVerifySign)
	if h.admin || !v1Path(string(c.Request.URI().Path())) {
		c.Next(ctx)
		return
	}
	appID := string(c.GetHeader(sign.HeaderAppID))
	ts := string(c.GetHeader(sign.HeaderTimestamp))
	nonce := string(c.GetHeader(sign.HeaderNonce))
	signature := string(c.GetHeader(sign.HeaderSignature))
	if appID == "" || ts == "" || nonce == "" || signature == "" {
		h.failVerify(ctx, c, appID, errno.ErrSignInvalid)
		return
	}
	if headerHasCRLF(appID, ts, nonce, signature) {
		h.failVerify(ctx, c, appID, errno.ErrSignInvalid)
		return
	}
	if h.lookup == nil {
		h.failVerify(ctx, c, appID, errno.ErrInternal)
		return
	}
	cred, err := h.lookup.Lookup(ctx, appID)
	if err != nil {
		h.failVerify(ctx, c, appID, lookupErrno(err))
		return
	}
	if cred.Status != merchantStatusActive {
		h.failVerify(ctx, c, appID, errno.ErrMerchantInvalid)
		return
	}
	if cred.MerchantID == "" {
		h.failVerify(ctx, c, appID, errno.ErrInternal)
		return
	}
	body, err := c.Body()
	if err != nil {
		h.failVerify(ctx, c, appID, errno.ErrInternal)
		return
	}
	err = sign.VerifyRequest(cred.Secret, sign.Request{
		Method:     string(c.Method()),
		RequestURI: string(c.Request.URI().RequestURI()),
		Timestamp:  ts,
		Nonce:      nonce,
		Body:       body,
	}, signature)
	if err != nil {
		h.failVerify(ctx, c, appID, verifyErrno(err))
		return
	}
	c.Set(merchantIDKey, cred.MerchantID)
	c.Set(signSecretKey, append([]byte(nil), cred.Secret...))
	MarkVerified(c)
	h.logVerify(ctx, c, appID, errno.OK)
	c.Next(ctx)
}

func authWatch(c *app.RequestContext) *obs.AuthWatch {
	if c == nil {
		return nil
	}
	v, _ := c.Get(gwAuthKey)
	w, _ := v.(*obs.AuthWatch)
	return w
}

func pauseAuth(c *app.RequestContext) {
	authWatch(c).Pause()
}

func resumeAuth(c *app.RequestContext) {
	authWatch(c).Resume()
}

// pauseRPC 在真实 payment 调用前停表，并装上 BackwardValues，否则服务端写不回 pay_handler_ns。
// 结束函数仍在停表时把墙钟减去处理方法耗时的正差额加进 gw_auth，然后继续计时。
// 键不存在或解析失败不加。假客户端不写这个键，停表区间与原来相同。
func pauseRPC(c *app.RequestContext, ctx context.Context) (context.Context, func()) {
	ctx = metainfo.WithBackwardValues(ctx)
	pauseAuth(c)
	start := time.Now()
	return ctx, func() {
		addCodecGap(c, ctx, time.Since(start))
		resumeAuth(c)
	}
}

func addCodecGap(c *app.RequestContext, ctx context.Context, wall time.Duration) {
	if ctx == nil {
		return
	}
	raw, ok := metainfo.RecvBackwardValue(ctx, obs.HeaderPayHandlerNS)
	if !ok {
		return
	}
	ns, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ns <= 0 {
		return
	}
	gap := wall - time.Duration(ns)
	if gap <= 0 {
		return
	}
	authWatch(c).Add(gap)
}

func v1Path(path string) bool {
	return path == "/v1" || strings.HasPrefix(path, "/v1/")
}

func headerHasCRLF(vals ...string) bool {
	for _, v := range vals {
		if strings.Contains(v, "\r") || strings.Contains(v, "\n") {
			return true
		}
	}
	return false
}

func loggableAppID(appID string) (string, bool) {
	if strings.Contains(appID, "\r") || strings.Contains(appID, "\n") {
		return "", false
	}
	return appID, true
}

func lookupErrno(err error) *errno.Error {
	var known *errno.Error
	if errors.As(err, &known) && known != nil {
		switch known.Code {
		case errno.ErrMerchantInvalid.Code:
			return errno.ErrMerchantInvalid
		case errno.ErrUnavailable.Code:
			return errno.ErrUnavailable
		default:
			return errno.ErrInternal
		}
	}
	return errno.ErrInternal
}

func verifyErrno(err error) *errno.Error {
	if errors.Is(err, sign.ErrSignatureMismatch) || errors.Is(err, sign.ErrMalformedSignature) || errors.Is(err, sign.ErrInvalidField) {
		return errno.ErrSignInvalid
	}
	return errno.ErrInternal
}

func (h *Handler) failVerify(ctx context.Context, c *app.RequestContext, appID string, e *errno.Error) {
	if e == nil {
		e = errno.ErrInternal
	}
	h.logVerify(ctx, c, appID, e)
	writeErrno(c, e.HTTPStatus, e, requestID(c))
	c.Abort()
}

func (h *Handler) logVerify(ctx context.Context, c *app.RequestContext, appID string, e *errno.Error) {
	if e == nil {
		e = errno.ErrInternal
	}
	ctx = logx.WithRequestID(ctx, requestID(c))
	code := strconv.FormatInt(int64(e.Code), 10)
	attrs := []any{"err_code", code}
	if id, ok := loggableAppID(appID); ok {
		attrs = []any{"app_id", id, "err_code", code}
	}
	if e.Code == errno.ErrInternal.Code {
		h.log().ErrorContext(ctx, "merchant credential unusable", attrs...)
		return
	}
	h.log().InfoContext(ctx, "merchant verify finished", attrs...)
}

func (h *Handler) rateLimit(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameRateLimit)
	// 只限已验签的商户 /v1。管理端口、回调和未验签请求不建桶，避免伪造 app_id 消耗别人的配额。
	if h.admin || !v1Path(string(c.Request.URI().Path())) || !isVerified(c) {
		c.Next(ctx)
		return
	}
	appID := string(c.GetHeader(sign.HeaderAppID))
	now := h.clk.Now()
	if h.rateAllow != nil {
		ok, ra := h.rateAllow(appID, now)
		if !ok {
			h.rejectLimited(ctx, c, appID, ra)
			return
		}
		c.Next(ctx)
		return
	}
	lim := h.bucket(appID)
	// Allow() 读真实时间。时钟停住时连发多次必须把注入的 now 传进 AllowN。
	if lim.AllowN(now, 1) {
		c.Next(ctx)
		return
	}
	h.rejectLimited(ctx, c, appID, retryAfterSeconds(lim, now))
}

func (h *Handler) rejectLimited(ctx context.Context, c *app.RequestContext, appID, retryAfter string) {
	obs.AddRateLimitRejected()
	if retryAfter == "" {
		retryAfter = "1"
	}
	h.logGate(ctx, c, appID, errno.ErrRateLimited, "rate limited")
	writeErrno(c, errno.ErrRateLimited.HTTPStatus, errno.ErrRateLimited, requestID(c))
	c.Response.Header.Set("Retry-After", retryAfter)
	// 错误 JSON 已经写好。nonce 看到标记就不要占用，platformSign 签当前 body。
	c.Set(skipHandlerKey, true)
	c.Next(ctx)
}

func (h *Handler) nonce(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameNonce)
	// 限流已经写出 429。看到跳过标记就不要 Reserve，同一 nonce 仍可在稍后重试。
	if h.admin || !v1Path(string(c.Request.URI().Path())) || !isVerified(c) || skipHandler(c) {
		c.Next(ctx)
		return
	}
	appID := string(c.GetHeader(sign.HeaderAppID))
	nonce := string(c.GetHeader(sign.HeaderNonce))
	var endNonce func()
	if chain, ok := merchantStage(string(c.Method()), string(c.Request.URI().Path())); ok && chain == obs.ChainPayCreate {
		endNonce = obs.Begin(ctx, obs.ChainPayCreate, obs.StageGWNonce)
	}
	finishNonce := func() {
		if endNonce != nil {
			endNonce()
			endNonce = nil
		}
	}
	if h.nonces == nil {
		finishNonce()
		nonceUnavailable.Add(1)
		obs.AddNonceUnavailable()
		h.failNonce(ctx, c, appID, errno.ErrUnavailable)
		return
	}
	claimed, err := h.nonces.Reserve(ctx, appID, nonce)
	finishNonce()
	if err != nil {
		nonceUnavailable.Add(1)
		obs.AddNonceUnavailable()
		h.failNonce(ctx, c, appID, errno.ErrUnavailable)
		return
	}
	if !claimed {
		h.failNonce(ctx, c, appID, errno.ErrNonceReplayed)
		return
	}
	c.Next(ctx)
}

func (h *Handler) failNonce(ctx context.Context, c *app.RequestContext, appID string, e *errno.Error) {
	if e == nil {
		e = errno.ErrUnavailable
	}
	h.logGate(ctx, c, appID, e, "nonce rejected")
	writeErrno(c, e.HTTPStatus, e, requestID(c))
	// 错误 JSON 已经写好。进入 platformSign 签当前 body，不要在这里 Abort。
	c.Set(skipHandlerKey, true)
	c.Next(ctx)
}

func (h *Handler) logGate(ctx context.Context, c *app.RequestContext, appID string, e *errno.Error, msg string) {
	if e == nil {
		e = errno.ErrInternal
	}
	ctx = logx.WithRequestID(ctx, requestID(c))
	code := strconv.FormatInt(int64(e.Code), 10)
	attrs := []any{"err_code", code}
	if id, ok := loggableAppID(appID); ok {
		attrs = []any{"app_id", id, "err_code", code}
	}
	if e.Code == errno.ErrUnavailable.Code || e.Code == errno.ErrInternal.Code {
		h.log().ErrorContext(ctx, msg, attrs...)
		return
	}
	h.log().InfoContext(ctx, msg, attrs...)
}

// platformSign 在 handler 写出 body 之后签名。panic 被外层 Recovery 接住，不会签到 500 上。
// nonce 失败时 body 已经是错误 JSON：看到跳过标记就签当前 body，然后停住，不再进入路由。
// 验签失败在更前面 Abort，到不了这里，响应仍然不签名。
func (h *Handler) platformSign(ctx context.Context, c *app.RequestContext) {
	if skipHandler(c) {
		h.applyPlatformSign(ctx, c)
		c.Abort()
		return
	}
	c.Next(ctx)
	if h.admin || !isVerified(c) {
		return
	}
	h.applyPlatformSign(ctx, c)
}

func (h *Handler) applyPlatformSign(ctx context.Context, c *app.RequestContext) {
	if h.admin || !isVerified(c) {
		return
	}
	secret := h.secret
	if got, ok := requestSignSecret(c); ok {
		secret = got
	}
	body := append([]byte(nil), c.Response.Body()...)
	if len(secret) == 0 {
		h.failSign(ctx, c)
		return
	}
	nonce, err := h.nextNonce()
	if err != nil {
		h.failSign(ctx, c)
		return
	}
	ts := sign.FormatTimestamp(h.clk.Now())
	sig, err := sign.SignPlatform(secret, sign.PlatformMessage{
		Timestamp: ts,
		Nonce:     nonce,
		Body:      body,
	})
	if err != nil {
		h.failSign(ctx, c)
		return
	}
	c.Response.Header.Set(sign.HeaderTimestamp, ts)
	c.Response.Header.Set(sign.HeaderNonce, nonce)
	c.Response.Header.Set(sign.HeaderSignature, sig)
}

func (h *Handler) failSign(ctx context.Context, c *app.RequestContext) {
	// 固定句子。不把密钥、签名或 canonical 串放进日志。
	h.log().ErrorContext(logx.WithRequestID(ctx, requestID(c)), "platform signature failed")
	writeErrno(c, http.StatusInternalServerError, errno.ErrInternal, requestID(c))
}

func (h *Handler) notFound(ctx context.Context, c *app.RequestContext) {
	writeErrno(c, errno.ErrNotFound.HTTPStatus, errno.ErrNotFound, h.ensureRequestID(ctx, c))
}

func (h *Handler) log() *slog.Logger {
	if h != nil && h.logger != nil {
		return h.logger
	}
	return slog.Default()
}
