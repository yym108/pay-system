package gateway

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/sign"
)

const (
	headerRequestID = "X-Request-Id"

	requestIDKey  = "gw.request_id"
	verifiedKey   = "gw.verified"
	bizCodeKey    = "gw.biz_code"
	chainKey      = "gw.chain"
	merchantIDKey = "gw.merchant_id"
	// signSecretKey 只留给本次响应签名。不导出读取函数。
	signSecretKey = "gw.sign_secret"
	// skipHandlerKey 表示错误 JSON 已写好，platformSign 只签名，不再进入路由。
	skipHandlerKey = "gw.skip_handler"

	nonceLen      = 16
	nonceAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	// 248 = 62 * 4。丢弃更大的字节，避免取模偏向。
	nonceUnbiasedMax = 248
)

// envelope 是商户 API 与管理 API 的统一 JSON。字段顺序即写出顺序。
type envelope struct {
	Code      int32           `json:"code"`
	Msg       string          `json:"msg"`
	RequestID string          `json:"request_id"`
	Data      json.RawMessage `json:"data"`
}

// MerchantID 返回本次验签写入的商户号。未验签或商户号为空时 ok 为 false。
func MerchantID(c *app.RequestContext) (string, bool) {
	if c == nil {
		return "", false
	}
	v, ok := c.Get(merchantIDKey)
	if !ok {
		return "", false
	}
	id, ok := v.(string)
	if !ok || id == "" {
		return "", false
	}
	return id, true
}

func requestSignSecret(c *app.RequestContext) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	v, ok := c.Get(signSecretKey)
	if !ok {
		return nil, false
	}
	secret, ok := v.([]byte)
	if !ok || len(secret) == 0 {
		return nil, false
	}
	return secret, true
}

// MarkVerified 标记本请求已验签。商户端口随后给实际写出的 body 加平台签名。
// 管理端口即使标记了也不签名。
func MarkVerified(c *app.RequestContext) {
	if c == nil {
		return
	}
	c.Set(verifiedKey, true)
}

// WriteOK 写出成功响应。data 为 nil 时 data 字段是 {}。
// 调用方应已跑过 RequestID 中间件。
func WriteOK(c *app.RequestContext, data any) {
	raw, err := marshalData(data)
	if err != nil {
		writeErrno(c, http.StatusInternalServerError, errno.ErrInternal, requestID(c))
		return
	}
	writeErrno(c, http.StatusOK, errno.OK, requestID(c), raw)
}

func writeErrno(c *app.RequestContext, status int, e *errno.Error, id string, data ...json.RawMessage) {
	if c == nil {
		return
	}
	if e == nil {
		e = errno.ErrInternal
		status = http.StatusInternalServerError
	}
	var raw json.RawMessage
	if len(data) > 0 {
		raw = data[0]
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	body, err := encodeEnvelope(envelope{
		Code:      e.Code,
		Msg:       e.Msg,
		RequestID: id,
		Data:      raw,
	})
	if err != nil {
		body = internalBody(id)
		status = http.StatusInternalServerError
		e = errno.ErrInternal
	}
	c.SetStatusCode(status)
	c.Response.Header.Set("Content-Type", "application/json; charset=utf-8")
	c.Response.Header.SetContentLength(len(body))
	c.Response.Header.Set(headerRequestID, id)
	// 错误响应和重写后的 10003 都不带平台签名。
	c.Response.Header.Del(sign.HeaderTimestamp)
	c.Response.Header.Del(sign.HeaderNonce)
	c.Response.Header.Del(sign.HeaderSignature)
	c.Response.SetBodyRaw(body)
	c.Set(bizCodeKey, e.Code)
}

func encodeEnvelope(env envelope) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(env); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func marshalData(v any) (json.RawMessage, error) {
	if v == nil {
		return json.RawMessage(`{}`), nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

func internalBody(id string) []byte {
	body, err := encodeEnvelope(envelope{
		Code:      errno.ErrInternal.Code,
		Msg:       errno.ErrInternal.Msg,
		RequestID: id,
		Data:      json.RawMessage(`{}`),
	})
	if err != nil {
		return fmt.Appendf(nil, `{"code":%d,"msg":%q,"request_id":"","data":{}}`, errno.ErrInternal.Code, errno.ErrInternal.Msg)
	}
	return body
}

func requestID(c *app.RequestContext) string {
	if c == nil {
		return ""
	}
	v, ok := c.Get(requestIDKey)
	if !ok {
		return ""
	}
	id, _ := v.(string)
	return id
}

func isVerified(c *app.RequestContext) bool {
	v, ok := c.Get(verifiedKey)
	if !ok {
		return false
	}
	on, _ := v.(bool)
	return on
}

func skipHandler(c *app.RequestContext) bool {
	if c == nil {
		return false
	}
	v, ok := c.Get(skipHandlerKey)
	if !ok {
		return false
	}
	on, _ := v.(bool)
	return on
}

func appendName(c *app.RequestContext, name string) {
	var prev []string
	if v, ok := c.Get(chainKey); ok {
		prev, _ = v.([]string)
	}
	next := make([]string, len(prev)+1)
	copy(next, prev)
	next[len(prev)] = name
	c.Set(chainKey, next)
}

func chainNames(c *app.RequestContext) []string {
	v, ok := c.Get(chainKey)
	if !ok {
		return nil
	}
	names, _ := v.([]string)
	return names
}

// newRequestID 生成 32 位小写十六进制。不读取客户端的 X-Request-Id。
func newRequestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// randomNonce 生成 16 位 [A-Za-z0-9]。大于等于 248 的随机字节丢弃。
func randomNonce() (string, error) {
	out := make([]byte, nonceLen)
	filled := 0
	var buf [32]byte
	for filled < nonceLen {
		if _, err := rand.Read(buf[:]); err != nil {
			return "", err
		}
		for _, b := range buf {
			if b >= nonceUnbiasedMax {
				continue
			}
			out[filled] = nonceAlphabet[int(b)%len(nonceAlphabet)]
			filled++
			if filled == nonceLen {
				return string(out), nil
			}
		}
	}
	return string(out), nil
}

// readCapped 最多读 max+1 个字节。读满 max+1 时 tooBig 为 true，并且不再读。
// 用 LimitReader 封顶，避免 io.ReadAll 把超限 body 全部拉进内存。
func readCapped(r io.Reader, max int) (buf []byte, tooBig bool, err error) {
	if r == nil || max < 0 {
		return []byte{}, false, nil
	}
	limit := max + 1
	tmp := make([]byte, limit)
	limited := io.LimitReader(r, int64(limit))
	n := 0
	for n < limit {
		var nn int
		nn, err = limited.Read(tmp[n:])
		n += nn
		if n == limit {
			return nil, true, nil
		}
		if err == io.EOF {
			return tmp[:n], false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if nn == 0 {
			return nil, false, io.ErrNoProgress
		}
	}
	return nil, true, nil
}
