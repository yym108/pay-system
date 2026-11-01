package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
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

// trace 只开一个 span，不安装导出器，也不读取 OTEL 端点。
func (h *Handler) trace(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameTrace)
	path := string(c.Request.URI().Path())
	ctx, span := otel.Tracer("gateway").Start(ctx, path)
	defer span.End()
	c.Next(ctx)
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
	h.log().InfoContext(ctx, "request handled",
		"path", string(c.Request.URI().Path()),
		"status", c.Response.StatusCode(),
		"elapsed_ms", h.clk.Now().Sub(start).Milliseconds(),
		"merchant_id", "",
		"err_code", code,
	)
}

const merchantStatusActive = "ACTIVE"

// 时间窗口、限流、nonce 仍只留下顺序。不访问 Redis，不限流。验签在 verifySign。
func (h *Handler) timeWindow(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameTimeWindow)
	c.Next(ctx)
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
	c.Next(ctx)
}

func (h *Handler) nonce(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameNonce)
	c.Next(ctx)
}

// platformSign 在 handler 写出 body 之后签名。panic 被外层 Recovery 接住，不会签到 500 上。
func (h *Handler) platformSign(ctx context.Context, c *app.RequestContext) {
	c.Next(ctx)
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
