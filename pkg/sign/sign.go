// Package sign 实现 docs/05-api.md §3 的三种 HMAC-SHA256 签名：
// 商户请求签名（§3.1）、平台响应与商户通知签名（§3.3、§3.4）、渠道双向签名（§3.5）。
//
// 每种都提供签名与验签两侧，供 gateway、paycli、merchant-demo、channel_mock 共用。
// 输入按收到或发出的原始字节参与计算，本包不改大小写、不重新编码、不调整查询串顺序、不解析 JSON。
// 时间窗口与 nonce 检查不在本包，由 GW-04、GW-06 负责。
//
// 错误都是本包的哨兵，文本固定，不含密钥、签名值和拼接串。映射按哪一侧、哪个错误区分：
// 验签侧的 ErrMalformedSignature、ErrSignatureMismatch、ErrInvalidField 是对端签名问题，
// 网关映射为 20001（GW-03）或 50001（GW-06），不向外区分具体原因；
// ErrEmptyKey 和签名侧的全部错误是本端配置或内部故障，平台服务按 10003 处理并打 ERROR 日志（不含密钥）。
package sign

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
)

// 请求头名称，见 docs/05-api.md §3.1、§3.3–§3.5。
const (
	HeaderAppID         = "X-App-Id"
	HeaderTimestamp     = "X-Timestamp"
	HeaderNonce         = "X-Nonce"
	HeaderSignature     = "X-Signature"
	HeaderMockTimestamp = "X-Mock-Timestamp"
	HeaderMockSignature = "X-Mock-Signature"
)

const (
	// signatureLen 是 32 字节 MAC 的标准 base64（带填充）长度。
	signatureLen = 44
	separator    = "\n"
)

var (
	// ErrEmptyKey 表示密钥为空。签名与验签两侧都拒绝，不用空密钥计算 HMAC。
	ErrEmptyKey = errors.New("sign: empty key")
	// ErrInvalidField 表示以换行分隔的字段（方法、RequestURI、时间戳、nonce）含换行。
	// 拼接串用换行分隔，字段含换行会让两组不同的字段得到同一个拼接串。body 不受此限制。
	ErrInvalidField = errors.New("sign: field contains newline")
	// ErrMalformedSignature 表示签名不是 44 字符的标准 base64（带 = 填充）或解码后不是 32 字节。
	ErrMalformedSignature = errors.New("sign: malformed signature")
	// ErrSignatureMismatch 表示签名格式合法但与计算结果不一致。
	ErrSignatureMismatch = errors.New("sign: signature mismatch")
)

// Request 是商户请求签名（§3.1）的输入。字段取实际发送或收到的原样值。
type Request struct {
	// Method 是请求方法，按原样参与签名，本包不转大写。
	Method string
	// RequestURI 是路径加原始查询串（含 ?），与线上字节一致。
	RequestURI string
	// Timestamp 是 X-Timestamp 头的原始值（Unix 秒）。
	Timestamp string
	// Nonce 是 X-Nonce 头的原始值。
	Nonce string
	// Body 是原始请求体。nil 与空切片等价，都按空串计算。
	Body []byte
}

// PlatformMessage 是平台签名（§3.3 响应、§3.4 商户通知）的输入。密钥是商户的 app_secret。
type PlatformMessage struct {
	// Timestamp 是 X-Timestamp 头的原始值（Unix 秒）。
	Timestamp string
	// Nonce 是 X-Nonce 头的原始值。
	Nonce string
	// Body 是原始响应体或通知请求体。
	Body []byte
}

// ChannelMessage 是渠道签名（§3.5）的输入，平台与模拟渠道之间的请求、响应、回调都用它。
type ChannelMessage struct {
	// Timestamp 是 X-Mock-Timestamp 头的原始值（Unix 秒）。
	Timestamp string
	// Body 是原始 body。GET 请求为空。
	Body []byte
}

// SignRequest 计算商户请求签名，返回 X-Signature 的值。
func SignRequest(secret []byte, r Request) (string, error) {
	return sign(secret, r)
}

// VerifyRequest 用常量时间比较校验商户请求签名。通过返回 nil。
func VerifyRequest(secret []byte, r Request, signature string) error {
	return verify(secret, r, signature)
}

// SignPlatform 计算平台响应或商户通知的签名，返回 X-Signature 的值。
func SignPlatform(secret []byte, m PlatformMessage) (string, error) {
	return sign(secret, m)
}

// VerifyPlatform 用常量时间比较校验平台响应或商户通知的签名。通过返回 nil。
func VerifyPlatform(secret []byte, m PlatformMessage, signature string) error {
	return verify(secret, m, signature)
}

// SignChannel 计算渠道签名，返回 X-Mock-Signature 的值。
func SignChannel(key []byte, m ChannelMessage) (string, error) {
	return sign(key, m)
}

// VerifyChannel 用常量时间比较校验渠道签名。通过返回 nil。
func VerifyChannel(key []byte, m ChannelMessage, signature string) error {
	return verify(key, m, signature)
}

// FormatTimestamp 把 t 格式化为签名头使用的 Unix 秒（十进制，不补零）。与 t 的时区无关。
func FormatTimestamp(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}

// message 是一种签名的拼接规则。
type message interface {
	// validate 检查以换行分隔的字段不含换行。
	validate() error
	// writeCanonical 把拼接串写入 w。w 是 hash.Hash 或测试里的 bytes.Buffer，两者的 Write 都不返回错误。
	writeCanonical(w io.Writer)
}

func (r Request) validate() error {
	return noNewline(r.Method, r.RequestURI, r.Timestamp, r.Nonce)
}

func (r Request) writeCanonical(w io.Writer) {
	writeFields(w, r.Method, r.RequestURI, r.Timestamp, r.Nonce)
	writeBodyHash(w, r.Body)
}

func (m PlatformMessage) validate() error {
	return noNewline(m.Timestamp, m.Nonce)
}

func (m PlatformMessage) writeCanonical(w io.Writer) {
	writeFields(w, m.Timestamp, m.Nonce)
	writeBodyHash(w, m.Body)
}

func (m ChannelMessage) validate() error {
	return noNewline(m.Timestamp)
}

// writeCanonical 直接拼原始 body，不取摘要（§3.5）。
func (m ChannelMessage) writeCanonical(w io.Writer) {
	writeFields(w, m.Timestamp)
	_, _ = w.Write(m.Body)
}

func noNewline(fields ...string) error {
	for _, f := range fields {
		if strings.Contains(f, separator) {
			return ErrInvalidField
		}
	}
	return nil
}

// writeFields 写入每个字段及其后的换行。
func writeFields(w io.Writer, fields ...string) {
	for _, f := range fields {
		_, _ = io.WriteString(w, f)
		_, _ = io.WriteString(w, separator)
	}
}

// writeBodyHash 写入 hex(sha256(body))，小写十六进制。
func writeBodyHash(w io.Writer, body []byte) {
	sum := sha256.Sum256(body)
	var buf [2 * sha256.Size]byte
	hex.Encode(buf[:], sum[:])
	_, _ = w.Write(buf[:])
}

func mac(key []byte, m message) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	h := hmac.New(sha256.New, key)
	m.writeCanonical(h)
	return h.Sum(nil), nil
}

func sign(key []byte, m message) (string, error) {
	sum, err := mac(key, m)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sum), nil
}

func verify(key []byte, m message, signature string) error {
	want, err := mac(key, m)
	if err != nil {
		return err
	}
	got, err := decodeSignature(signature)
	if err != nil {
		return err
	}
	if !hmac.Equal(want, got) {
		return ErrSignatureMismatch
	}
	return nil
}

// decodeSignature 只接受规范的标准 base64：长度 44、带填充、填充位为 0、解码后 32 字节。
// Strict 仍会跳过换行，换行使有效字符不足，解码长度检查会拒绝它。
// 解码错误不向上包装，避免把输入的位置信息带进错误。
func decodeSignature(s string) ([]byte, error) {
	if len(s) != signatureLen {
		return nil, ErrMalformedSignature
	}
	b, err := base64.StdEncoding.Strict().DecodeString(s)
	if err != nil || len(b) != sha256.Size {
		return nil, ErrMalformedSignature
	}
	return b, nil
}
