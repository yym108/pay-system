package sign

import (
	"bytes"
	"encoding/base64"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
)

// 以下常量逐字取自 docs/05-api.md §3.7，不得按实现结果回填。
const (
	vectorMerchantSecret = "demo_secret_M100001_0123456789ab" //nolint:gosec // G101: 文档公开的测试向量密钥，不是生产密钥。
	vectorChannelKey     = "demo_channel_key_mockpay_0123456"

	v1Body = `{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`
	v3Body = `{"code":0,"msg":"ok","data":{"pay_no":"PA0000000000000000001"}}`
	v4Body = `{"txn_type":"PAY","req_no":"PA0000000000000000001","status":"SUCCESS"}`

	v1Signature = "hFYKid0+k7q9vCvqh/i6YnvoaAJYzf2niNuvSkMCwoQ="
	v2Signature = "D1t81263+AFhLO6+FPSoVQqNFANBAKpi0akuyE9gVu4="
	v3Signature = "TuLVqqAxs3SyvTtDXp1cpv+Rpv2D5URAFywIHBNjAh4="
	v4Signature = "d1zUa8yWKf9rXU7JL+9BMbK+Mg5ZuQHLao4iLCVdKKY="
)

// 三种签名的统一入口，便于表驱动地覆盖每一种。
type signCase struct {
	sign   func(key []byte) (string, error)
	verify func(key []byte, signature string) error
	msg    message
}

func requestCase(r Request) signCase {
	return signCase{
		sign:   func(key []byte) (string, error) { return SignRequest(key, r) },
		verify: func(key []byte, s string) error { return VerifyRequest(key, r, s) },
		msg:    r,
	}
}

func platformCase(m PlatformMessage) signCase {
	return signCase{
		sign:   func(key []byte) (string, error) { return SignPlatform(key, m) },
		verify: func(key []byte, s string) error { return VerifyPlatform(key, m, s) },
		msg:    m,
	}
}

func channelCase(m ChannelMessage) signCase {
	return signCase{
		sign:   func(key []byte) (string, error) { return SignChannel(key, m) },
		verify: func(key []byte, s string) error { return VerifyChannel(key, m, s) },
		msg:    m,
	}
}

func v1Request() Request {
	return Request{
		Method:     "POST",
		RequestURI: "/v1/payments",
		Timestamp:  "1791500000",
		Nonce:      "AbCdEf0123456789",
		Body:       []byte(v1Body),
	}
}

func v2Request() Request {
	return Request{
		Method:     "GET",
		RequestURI: "/v1/payments?out_trade_no=T20261009001",
		Timestamp:  "1791500000",
		Nonce:      "AbCdEf0123456789",
	}
}

func v3Platform() PlatformMessage {
	return PlatformMessage{Timestamp: "1791500001", Nonce: "ZyXwVu9876543210", Body: []byte(v3Body)}
}

func v4Channel() ChannelMessage {
	return ChannelMessage{Timestamp: "1791500002", Body: []byte(v4Body)}
}

func canonicalOf(m message) string {
	var b bytes.Buffer
	m.writeCanonical(&b)
	return b.String()
}

// BASE-13 验收 1：docs/05-api.md §3.7 的全部测试向量通过（canonical 与签名都逐字比对）。
func TestVectors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		key           string
		c             signCase
		wantCanonical string
		wantSignature string
	}{
		{
			name:          "V1 merchant request POST",
			key:           vectorMerchantSecret,
			c:             requestCase(v1Request()),
			wantCanonical: "POST\n/v1/payments\n1791500000\nAbCdEf0123456789\nc2e7954ac287ebcaff20ff9cdaf71f72e6cf4def6aec049a8b69a3c468e0a1e1",
			wantSignature: v1Signature,
		},
		{
			name:          "V2 merchant request GET empty body",
			key:           vectorMerchantSecret,
			c:             requestCase(v2Request()),
			wantCanonical: "GET\n/v1/payments?out_trade_no=T20261009001\n1791500000\nAbCdEf0123456789\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			wantSignature: v2Signature,
		},
		{
			name:          "V3 platform response",
			key:           vectorMerchantSecret,
			c:             platformCase(v3Platform()),
			wantCanonical: "1791500001\nZyXwVu9876543210\n8196328a69b72a8da9bd189c88ce3409371ce889ce2b414719d4095d5708571c",
			wantSignature: v3Signature,
		},
		{
			name:          "V4 channel callback",
			key:           vectorChannelKey,
			c:             channelCase(v4Channel()),
			wantCanonical: "1791500002\n" + `{"txn_type":"PAY","req_no":"PA0000000000000000001","status":"SUCCESS"}`,
			wantSignature: v4Signature,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := canonicalOf(tc.c.msg); got != tc.wantCanonical {
				t.Fatalf("canonical = %q, want %q", got, tc.wantCanonical)
			}
			got, err := tc.c.sign([]byte(tc.key))
			if err != nil {
				t.Fatalf("sign error: %v", err)
			}
			if got != tc.wantSignature {
				t.Fatalf("signature = %q, want %q", got, tc.wantSignature)
			}
			if err := tc.c.verify([]byte(tc.key), tc.wantSignature); err != nil {
				t.Fatalf("verify document signature: %v", err)
			}
		})
	}
}

// BASE-13 规则 1：商户通知与平台响应同一算法（§3.4），V2 的空 body 与 nil body 等价。
func TestNotifyAndEmptyBody(t *testing.T) {
	t.Parallel()
	// 通知没有单独的函数：平台签名对响应和通知都适用，V3 的签名同样能验过。
	if err := VerifyPlatform([]byte(vectorMerchantSecret), v3Platform(), v3Signature); err != nil {
		t.Fatalf("notify verify: %v", err)
	}
	r := v2Request()
	r.Body = []byte{}
	if err := VerifyRequest([]byte(vectorMerchantSecret), r, v2Signature); err != nil {
		t.Fatalf("empty non-nil body: %v", err)
	}
	// §3.5：GET 请求 body 为空串。
	empty := ChannelMessage{Timestamp: "1791500002"}
	if got, want := canonicalOf(empty), "1791500002\n"; got != want {
		t.Fatalf("channel canonical with empty body = %q, want %q", got, want)
	}
}

// BASE-13 验收 2：body 改动 1 字节、查询串参数顺序改变、时间戳改变，验签均失败。
func TestVerifyRejectsTampering(t *testing.T) {
	t.Parallel()
	merchant := []byte(vectorMerchantSecret)
	channel := []byte(vectorChannelKey)

	// 两个参数的查询串：先按原顺序签名，再把参数调换后用同一签名验签。
	ordered := Request{
		Method:     "GET",
		RequestURI: "/v1/payments?out_trade_no=T20261009001&pay_no=PA0000000000000000001",
		Timestamp:  "1791500000",
		Nonce:      "AbCdEf0123456789",
	}
	orderedSig, err := SignRequest(merchant, ordered)
	if err != nil {
		t.Fatalf("sign ordered query: %v", err)
	}
	if err := VerifyRequest(merchant, ordered, orderedSig); err != nil {
		t.Fatalf("verify ordered query: %v", err)
	}
	swapped := ordered
	swapped.RequestURI = "/v1/payments?pay_no=PA0000000000000000001&out_trade_no=T20261009001"

	withRequest := func(f func(*Request)) Request {
		r := v1Request()
		r.Body = []byte(v1Body)
		f(&r)
		return r
	}
	cases := []struct {
		name      string
		key       []byte
		signature string
		verify    func(key []byte, s string) error
	}{
		{
			name: "request body one byte changed", key: merchant, signature: v1Signature,
			verify: requestCase(withRequest(func(r *Request) { r.Body = []byte(strings.Replace(v1Body, "10000", "10001", 1)) })).verify,
		},
		{
			name: "request body one byte appended", key: merchant, signature: v1Signature,
			verify: requestCase(withRequest(func(r *Request) { r.Body = append(r.Body, ' ') })).verify,
		},
		{
			name: "query parameter order changed", key: merchant, signature: orderedSig,
			verify: requestCase(swapped).verify,
		},
		{
			name: "request timestamp changed", key: merchant, signature: v1Signature,
			verify: requestCase(withRequest(func(r *Request) { r.Timestamp = "1791500001" })).verify,
		},
		{
			name: "request nonce changed", key: merchant, signature: v1Signature,
			verify: requestCase(withRequest(func(r *Request) { r.Nonce = "AbCdEf0123456788" })).verify,
		},
		{
			name: "request method changed", key: merchant, signature: v1Signature,
			verify: requestCase(withRequest(func(r *Request) { r.Method = "PUT" })).verify,
		},
		{
			name: "request path changed", key: merchant, signature: v2Signature,
			verify: requestCase(func() Request { r := v2Request(); r.RequestURI = "/v1/payments?out_trade_no=T20261009002"; return r }()).verify,
		},
		{
			name: "request wrong secret", key: []byte("demo_secret_M100002_0123456789ab"), signature: v1Signature,
			verify: requestCase(v1Request()).verify,
		},
		{
			name: "platform body one byte changed", key: merchant, signature: v3Signature,
			verify: platformCase(PlatformMessage{Timestamp: "1791500001", Nonce: "ZyXwVu9876543210", Body: []byte(strings.Replace(v3Body, `"code":0`, `"code":1`, 1))}).verify,
		},
		{
			name: "platform timestamp changed", key: merchant, signature: v3Signature,
			verify: platformCase(PlatformMessage{Timestamp: "1791500002", Nonce: "ZyXwVu9876543210", Body: []byte(v3Body)}).verify,
		},
		{
			name: "platform nonce changed", key: merchant, signature: v3Signature,
			verify: platformCase(PlatformMessage{Timestamp: "1791500001", Nonce: "ZyXwVu9876543211", Body: []byte(v3Body)}).verify,
		},
		{
			name: "notify wrong secret", key: []byte("demo_secret_M100002_0123456789ab"), signature: v3Signature,
			verify: platformCase(v3Platform()).verify,
		},
		{
			name: "channel body one byte changed", key: channel, signature: v4Signature,
			verify: channelCase(ChannelMessage{Timestamp: "1791500002", Body: []byte(strings.Replace(v4Body, "SUCCESS", "SUCCESs", 1))}).verify,
		},
		{
			name: "channel timestamp changed", key: channel, signature: v4Signature,
			verify: channelCase(ChannelMessage{Timestamp: "1791500003", Body: []byte(v4Body)}).verify,
		},
		{
			name: "channel wrong key", key: []byte(vectorMerchantSecret), signature: v4Signature,
			verify: channelCase(v4Channel()).verify,
		},
		{
			// 签名不能跨场景复用：V3 的平台签名不能当作 V1 请求的签名。
			name: "platform signature used for request", key: merchant, signature: v3Signature,
			verify: requestCase(v1Request()).verify,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.verify(tc.key, tc.signature)
			if !errors.Is(err, ErrSignatureMismatch) {
				t.Fatalf("verify err = %v, want %v", err, ErrSignatureMismatch)
			}
		})
	}
}

// BASE-13 规则 2：不做规范化。大小写、百分号编码、JSON 空白不同即签名不同。
func TestNoNormalization(t *testing.T) {
	t.Parallel()
	key := []byte(vectorMerchantSecret)
	cases := []struct {
		name string
		f    func(*Request)
	}{
		{name: "lowercase method", f: func(r *Request) { r.Method = "post" }},
		{name: "trailing slash", f: func(r *Request) { r.RequestURI = "/v1/payments/" }},
		{name: "empty query marker", f: func(r *Request) { r.RequestURI = "/v1/payments?" }},
		{name: "percent encoded path", f: func(r *Request) { r.RequestURI = "/v1%2Fpayments" }},
		{name: "json whitespace", f: func(r *Request) {
			r.Body = []byte(`{"out_trade_no": "T20261009001","amount":10000,"subject":"demo"}`)
		}},
		{name: "timestamp leading zero", f: func(r *Request) { r.Timestamp = "01791500000" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := v1Request()
			tc.f(&r)
			got, err := SignRequest(key, r)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			if got == v1Signature {
				t.Fatalf("signature equals V1 after %s; input was normalized", tc.name)
			}
			if err := VerifyRequest(key, r, v1Signature); !errors.Is(err, ErrSignatureMismatch) {
				t.Fatalf("verify V1 signature err = %v, want %v", err, ErrSignatureMismatch)
			}
		})
	}
}

// BASE-13 边界：签名长度不符、非法编码一律验签失败，返回 ErrMalformedSignature。
func TestVerifyMalformedSignature(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		signature string
	}{
		{name: "empty", signature: ""},
		{name: "one char short", signature: v1Signature[:43]},
		{name: "missing padding", signature: strings.TrimSuffix(v1Signature, "=")},
		{name: "extra char", signature: v1Signature + "="},
		{name: "doubled", signature: v1Signature + v1Signature},
		{name: "url safe alphabet", signature: strings.NewReplacer("+", "-", "/", "_").Replace(v1Signature)},
		{name: "nonzero padding bits", signature: strings.TrimSuffix(v1Signature, "Q=") + "R="},
		{name: "embedded newline", signature: v1Signature[:20] + "\n" + v1Signature[21:]},
		{name: "trailing space", signature: v1Signature[:43] + " "},
		{name: "hex encoded", signature: "84560a89dd3e93babdbc2bea87f8ba627be8680258cdfda788dbaf4a4302c284"},
		{name: "non ascii", signature: "签" + v1Signature[3:]},
		{name: "16 byte mac", signature: "hFYKid0+k7q9vCvqh/i6Yg=="},
	}
	key := []byte(vectorMerchantSecret)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			checks := map[string]error{
				"request":  VerifyRequest(key, v1Request(), tc.signature),
				"platform": VerifyPlatform(key, v3Platform(), tc.signature),
				"channel":  VerifyChannel([]byte(vectorChannelKey), v4Channel(), tc.signature),
			}
			for kind, err := range checks {
				if !errors.Is(err, ErrMalformedSignature) {
					t.Errorf("%s verify %q err = %v, want %v", kind, tc.signature, err, ErrMalformedSignature)
				}
			}
		})
	}
	// 长度 44、Strict 解码成功，但解码后不是 32 字节：只能由解码长度检查拒绝。
	// 31 字节编码为 44 字符带 "=="，33 字节编码为 44 字符不带填充，两者填充位都为 0。
	for _, n := range []int{31, 33} {
		s := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xA5}, n))
		if len(s) != signatureLen {
			t.Fatalf("%d-byte encoding has length %d, want %d", n, len(s), signatureLen)
		}
		if _, err := base64.StdEncoding.Strict().DecodeString(s); err != nil {
			t.Fatalf("%d-byte encoding %q is not strict base64: %v", n, s, err)
		}
		checks := map[string]error{
			"request":  VerifyRequest(key, v1Request(), s),
			"platform": VerifyPlatform(key, v3Platform(), s),
			"channel":  VerifyChannel([]byte(vectorChannelKey), v4Channel(), s),
		}
		for kind, err := range checks {
			if !errors.Is(err, ErrMalformedSignature) {
				t.Errorf("%s verify %d-byte signature err = %v, want %v", kind, n, err, ErrMalformedSignature)
			}
		}
	}

	// 只改首字符大小写：编码仍合法，结果是不一致而不是格式错误。
	// 不整串转小写，末字符 Q 变 q 会让填充位非 0，那是格式错误。
	flipped := "H" + v1Signature[1:]
	if err := VerifyRequest(key, v1Request(), flipped); !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("case-flipped signature err = %v, want %v", err, ErrSignatureMismatch)
	}
}

// BASE-13 边界：空密钥在签名与验签两侧都拒绝，不计算 HMAC。
func TestEmptyKey(t *testing.T) {
	t.Parallel()
	cases := map[string]signCase{
		"request":  requestCase(v1Request()),
		"platform": platformCase(v3Platform()),
		"channel":  channelCase(v4Channel()),
	}
	signatures := map[string]string{"request": v1Signature, "platform": v3Signature, "channel": v4Signature}
	for name, c := range cases {
		for _, key := range [][]byte{nil, {}} {
			if got, err := c.sign(key); !errors.Is(err, ErrEmptyKey) || got != "" {
				t.Errorf("%s sign with empty key = %q, %v, want \"\", %v", name, got, err, ErrEmptyKey)
			}
			if err := c.verify(key, signatures[name]); !errors.Is(err, ErrEmptyKey) {
				t.Errorf("%s verify with empty key err = %v, want %v", name, err, ErrEmptyKey)
			}
		}
	}
}

// BASE-13 边界：以换行分隔的字段含换行时拼接串有歧义，签名与验签两侧都拒绝。body 可以含换行。
func TestFieldWithNewline(t *testing.T) {
	t.Parallel()
	key := []byte(vectorMerchantSecret)
	cases := []struct {
		name string
		c    signCase
	}{
		{name: "request method", c: requestCase(Request{Method: "POST\n", RequestURI: "/v1/payments", Timestamp: "1791500000", Nonce: "AbCdEf0123456789"})},
		{name: "request uri", c: requestCase(Request{Method: "POST", RequestURI: "/v1/payments\n1791500000", Timestamp: "1791500000", Nonce: "AbCdEf0123456789"})},
		{name: "request timestamp", c: requestCase(Request{Method: "POST", RequestURI: "/v1/payments", Timestamp: "1791500000\nAbCdEf0123456789", Nonce: "x"})},
		{name: "request nonce", c: requestCase(Request{Method: "POST", RequestURI: "/v1/payments", Timestamp: "1791500000", Nonce: "AbCdEf0123456789\n"})},
		{name: "platform timestamp", c: platformCase(PlatformMessage{Timestamp: "1791500001\n", Nonce: "ZyXwVu9876543210"})},
		{name: "platform nonce", c: platformCase(PlatformMessage{Timestamp: "1791500001", Nonce: "Zy\nXw"})},
		{name: "channel timestamp", c: channelCase(ChannelMessage{Timestamp: "1791500002\n{", Body: []byte(`"x":1}`)})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, err := tc.c.sign(key); !errors.Is(err, ErrInvalidField) || got != "" {
				t.Fatalf("sign = %q, %v, want \"\", %v", got, err, ErrInvalidField)
			}
			if err := tc.c.verify(key, v1Signature); !errors.Is(err, ErrInvalidField) {
				t.Fatalf("verify err = %v, want %v", err, ErrInvalidField)
			}
		})
	}

	multiline := ChannelMessage{Timestamp: "1791500002", Body: []byte("{\n\"a\":1\n}")}
	s, err := SignChannel([]byte(vectorChannelKey), multiline)
	if err != nil {
		t.Fatalf("sign multiline body: %v", err)
	}
	if err := VerifyChannel([]byte(vectorChannelKey), multiline, s); err != nil {
		t.Fatalf("verify multiline body: %v", err)
	}
}

// BASE-13 规则：错误文本不含密钥、签名值和 canonical 串。
func TestErrorsDoNotLeakSecrets(t *testing.T) {
	t.Parallel()
	key := []byte(vectorMerchantSecret)
	errs := []error{
		VerifyRequest(key, v1Request(), v2Signature),
		VerifyRequest(key, v1Request(), "not-a-signature"),
		VerifyRequest(nil, v1Request(), v1Signature),
		VerifyRequest(key, Request{Method: "POST\n"}, v1Signature),
	}
	secrets := []string{vectorMerchantSecret, v1Signature, v2Signature, "not-a-signature", "AbCdEf0123456789", "/v1/payments"}
	for _, err := range errs {
		if err == nil {
			t.Fatal("expected an error")
		}
		for _, s := range secrets {
			if strings.Contains(err.Error(), s) {
				t.Fatalf("error %q contains %q", err.Error(), s)
			}
		}
	}
}

// BASE-13 规则：签名头时间戳为 Unix 秒。§3.7：1791500000 即 2026-10-09T06:53:20+08:00，用 Fake 时钟固定到该时刻。
func TestFormatTimestamp(t *testing.T) {
	t.Parallel()
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	f := &clock.Fake{}
	f.Set(time.Date(2026, 10, 9, 6, 53, 20, 999_000_000, loc))
	if got := FormatTimestamp(f.Now()); got != "1791500000" {
		t.Fatalf("FormatTimestamp = %q, want 1791500000", got)
	}
	if got := FormatTimestamp(f.Now().UTC()); got != "1791500000" {
		t.Fatalf("FormatTimestamp(UTC) = %q, want 1791500000", got)
	}
	f.Advance(2 * time.Second)
	if got := FormatTimestamp(f.Now()); got != "1791500002" {
		t.Fatalf("FormatTimestamp after 2s = %q, want 1791500002", got)
	}

	// 用 Fake 时钟产生 V1 的时间戳，签名仍与文档一致。
	f.Set(time.Date(2026, 10, 9, 6, 53, 20, 0, loc))
	r := v1Request()
	r.Timestamp = FormatTimestamp(f.Now())
	got, err := SignRequest([]byte(vectorMerchantSecret), r)
	if err != nil || got != v1Signature {
		t.Fatalf("SignRequest with clock timestamp = %q, %v, want %q", got, err, v1Signature)
	}
}

// BASE-13 规则：请求头名称与 docs/05-api.md §3.1、§3.3–§3.5 一致。
func TestHeaderNames(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		HeaderAppID:         "X-App-Id",
		HeaderTimestamp:     "X-Timestamp",
		HeaderNonce:         "X-Nonce",
		HeaderSignature:     "X-Signature",
		HeaderMockTimestamp: "X-Mock-Timestamp",
		HeaderMockSignature: "X-Mock-Signature",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("header = %q, want %q", got, want)
		}
	}
}

// BASE-13 规则 3：验签只用 hmac.Equal 常量时间比较。扫描本包非测试源码，
// 确认调用了 hmac.Equal，且没有用 bytes.Equal 等非常量时间比较或 == 比较签名。
func TestVerifyUsesHMACEqual(t *testing.T) {
	t.Parallel()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	names, err := filepath.Glob(filepath.Join(filepath.Dir(filename), "*.go"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	fset := token.NewFileSet()
	var sawHMACEqual bool
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				pkg, ok := node.X.(*ast.Ident)
				if !ok {
					return true
				}
				switch pkg.Name + "." + node.Sel.Name {
				case "hmac.Equal":
					sawHMACEqual = true
				case "bytes.Equal", "bytes.Compare", "strings.EqualFold", "strings.Compare":
					t.Errorf("%s uses %s.%s; signatures must be compared with hmac.Equal", fset.Position(node.Pos()), pkg.Name, node.Sel.Name)
				}
			case *ast.BinaryExpr:
				for _, side := range []ast.Expr{node.X, node.Y} {
					if id, ok := side.(*ast.Ident); ok && (id.Name == "signature" || id.Name == "got" || id.Name == "want") &&
						(node.Op == token.EQL || node.Op == token.NEQ) {
						t.Errorf("%s compares %s with %s", fset.Position(node.Pos()), id.Name, node.Op)
					}
				}
			}
			return true
		})
	}
	if !sawHMACEqual {
		t.Fatal("pkg/sign does not call hmac.Equal")
	}
}

func BenchmarkVerifyRequest(b *testing.B) {
	key := []byte(vectorMerchantSecret)
	r := v1Request()
	r.Body = bytes.Repeat([]byte("a"), 1024)
	s, err := SignRequest(key, r)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := VerifyRequest(key, r, s); err != nil {
			b.Fatal(err)
		}
	}
}
