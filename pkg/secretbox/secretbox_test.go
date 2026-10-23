package secretbox

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
)

// demoPlain 与 docs/05-api.md §3.7 测试向量的商户密钥原文一致。
const demoPlain = "demo_secret_M100001_0123456789ab"

func testKey(b byte) []byte { return bytes.Repeat([]byte{b}, KeySize) }

func mustNew(t *testing.T, key []byte) *Box {
	t.Helper()
	box, err := New(key)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return box
}

// BASE-14 验收 1：加密后解密得到原文；同一明文两次加密结果不同。
func TestSealOpenRoundTrip(t *testing.T) {
	t.Parallel()
	box := mustNew(t, testKey(0x11))
	cases := []struct {
		name  string
		plain []byte
	}{
		{name: "demo secret", plain: []byte(demoPlain)},
		{name: "empty", plain: []byte{}},
		{name: "nil", plain: nil},
		{name: "binary", plain: []byte{0x00, 0xff, 0x01, 0xfe}},
		{name: "227 bytes fills VARBINARY(256)", plain: bytes.Repeat([]byte{'x'}, 256-Overhead)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, err := box.Seal(tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			second, err := box.Seal(tc.plain)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(first, second) {
				t.Fatal("same plaintext encrypted twice must differ")
			}
			if bytes.Equal(first[1:1+NonceSize], second[1:1+NonceSize]) {
				t.Fatal("nonce reused")
			}
			for _, sealed := range [][]byte{first, second} {
				if sealed[0] != Version {
					t.Fatalf("version byte = %d, want %d", sealed[0], Version)
				}
				if len(sealed) != Overhead+len(tc.plain) {
					t.Fatalf("len = %d, want %d", len(sealed), Overhead+len(tc.plain))
				}
				got, err := box.Open(sealed)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, tc.plain) {
					t.Fatal("opened plaintext differs")
				}
			}
		})
	}
}

// BASE-14 规则 2：nonce 每次随机。1000 次加密的 nonce 两两不同。
func TestSealNonceUnique(t *testing.T) {
	t.Parallel()
	box := mustNew(t, testKey(0x22))
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		sealed, err := box.Seal([]byte(demoPlain))
		if err != nil {
			t.Fatal(err)
		}
		nonce := string(sealed[1 : 1+NonceSize])
		if _, dup := seen[nonce]; dup {
			t.Fatalf("nonce repeated at %d", i)
		}
		seen[nonce] = struct{}{}
	}
}

// BASE-14 验收 2：篡改任一字节，解密失败。逐字节翻转每一位所在字节的最低位和最高位。
func TestOpenTamperEveryByte(t *testing.T) {
	t.Parallel()
	box := mustNew(t, testKey(0x33))
	sealed, err := box.Seal([]byte(demoPlain))
	if err != nil {
		t.Fatal(err)
	}
	for i := range sealed {
		for _, mask := range []byte{0x01, 0x80} {
			tampered := append([]byte(nil), sealed...)
			tampered[i] ^= mask
			got, err := box.Open(tampered)
			if err == nil {
				t.Fatalf("byte %d mask %#x: tampered ciphertext opened", i, mask)
			}
			if got != nil {
				t.Fatalf("byte %d: plaintext returned on failure", i)
			}
			want := ErrDecrypt
			if i == 0 {
				want = ErrUnknownVersion
			}
			if !errors.Is(err, want) {
				t.Fatalf("byte %d: err = %v, want %v", i, err, want)
			}
		}
	}
}

// BASE-14 规则 3：截断、未知版本、错误主密钥都返回固定文本的哨兵，不含密文或明文。
func TestOpenFailures(t *testing.T) {
	t.Parallel()
	box := mustNew(t, testKey(0x44))
	sealed, err := box.Seal([]byte(demoPlain))
	if err != nil {
		t.Fatal(err)
	}
	unknown := append([]byte(nil), sealed...)
	unknown[0] = 2
	zeroVersion := append([]byte(nil), sealed...)
	zeroVersion[0] = 0
	cases := []struct {
		name string
		box  *Box
		blob []byte
		want error
	}{
		{name: "nil", box: box, blob: nil, want: ErrMalformed},
		{name: "empty", box: box, blob: []byte{}, want: ErrMalformed},
		{name: "version only", box: box, blob: sealed[:1], want: ErrMalformed},
		{name: "no tag", box: box, blob: sealed[:1+NonceSize], want: ErrMalformed},
		{name: "one byte short of overhead", box: box, blob: sealed[:Overhead-1], want: ErrMalformed},
		{name: "truncated tail", box: box, blob: sealed[:len(sealed)-1], want: ErrDecrypt},
		{name: "extra byte", box: box, blob: append(append([]byte(nil), sealed...), 0x00), want: ErrDecrypt},
		{name: "unknown version 2", box: box, blob: unknown, want: ErrUnknownVersion},
		{name: "unknown version 0", box: box, blob: zeroVersion, want: ErrUnknownVersion},
		{name: "wrong master key", box: mustNew(t, testKey(0x45)), blob: sealed, want: ErrDecrypt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.box.Open(tc.blob)
			if got != nil {
				t.Fatal("plaintext returned on failure")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			assertNoMaterial(t, err.Error(), demoPlain, hex.EncodeToString(sealed), base64.StdEncoding.EncodeToString(sealed))
			if err.Error() != tc.want.Error() {
				t.Fatalf("error text %q is not the fixed sentinel text %q", err.Error(), tc.want.Error())
			}
		})
	}
}

// BASE-14 验收 3：主密钥长度不是 32 字节时初始化失败；base64 非法时初始化失败。
func TestNewRejectsBadKey(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, 1, 16, 24, 31, 33, 64} {
		key := bytes.Repeat([]byte{0x5a}, n)
		box, err := New(key)
		if box != nil || !errors.Is(err, ErrKeySize) {
			t.Fatalf("New(%d bytes) = %v, %v; want nil, ErrKeySize", n, box, err)
		}
		encoded := base64.StdEncoding.EncodeToString(key)
		box, err = NewFromBase64(encoded)
		if box != nil || !errors.Is(err, ErrKeySize) {
			t.Fatalf("NewFromBase64(%d bytes) = %v, %v; want nil, ErrKeySize", n, box, err)
		}
		if n > 0 {
			assertNoMaterial(t, err.Error(), encoded)
		}
	}
	if _, err := New(nil); !errors.Is(err, ErrKeySize) {
		t.Fatalf("New(nil) err = %v", err)
	}
	valid := base64.StdEncoding.EncodeToString(testKey(0x5a))
	for _, bad := range []string{
		"%%%not-a-key%%%",
		strings.TrimRight(valid, "="),                    // 缺填充：与 pkg/config 一样只接受标准 base64
		base64.URLEncoding.EncodeToString(testKey(0xfb)), // URL 字母表含 - 和 _
		valid + "=",
	} {
		box, err := NewFromBase64(bad)
		if box != nil || !errors.Is(err, ErrKeyEncoding) {
			t.Fatalf("NewFromBase64(%q) = %v, %v; want nil, ErrKeyEncoding", bad, box, err)
		}
		assertNoMaterial(t, err.Error(), bad)
	}
}

// BASE-14 验收 3 补充：NewFromBase64 与 New 解出同一把主密钥，可互相解密。
func TestNewFromBase64MatchesNew(t *testing.T) {
	t.Parallel()
	key := testKey(0x66)
	fromB64, err := NewFromBase64(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := fromB64.Seal([]byte(demoPlain))
	if err != nil {
		t.Fatal(err)
	}
	got, err := mustNew(t, key).Open(sealed)
	if err != nil || string(got) != demoPlain {
		t.Fatalf("Open = %q, %v", got, err)
	}
}

// BASE-14 兼容：BASE-03 种子已用标准库写入的密文必须能解开（需求 BASE-03 口径澄清）。
// 向量由 BASE-14 之前的 internal/seed.Seal 生成，主密钥是 deploy/.env.example 的演示值（32 个 0 字节）。
func TestOpenLegacySeedCiphertext(t *testing.T) {
	t.Parallel()
	box, err := NewFromBase64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		cipher string
		plain  string
	}{
		{
			name:   "M100001 demo secret",
			cipher: "01be5f5b4f4b854d72fdbfef63cc4f984cda9cee4d2be9d707076aef54c4517280ab3eec518743e85c7cd2c6584a0e42bed31d3b1c938367827882c173",
			plain:  demoPlain,
		},
		{
			name:   "empty",
			cipher: "01c6a85698a5231bcdb264ffc7d2f35b5cd71332c5fd30cd160c2a5016",
			plain:  "",
		},
	}
	for _, tc := range cases {
		blob, err := hex.DecodeString(tc.cipher)
		if err != nil {
			t.Fatal(err)
		}
		got, err := box.Open(blob)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(got) != tc.plain {
			t.Fatalf("%s: plaintext differs", tc.name)
		}
	}
}

// BASE-14：同一个 Box 可被多个 goroutine 同时使用（payment 进程只建一个）。用 -race 运行。
func TestBoxConcurrentUse(t *testing.T) {
	t.Parallel()
	box := mustNew(t, testKey(0x77))
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				sealed, err := box.Seal([]byte(demoPlain))
				if err != nil {
					errs <- err
					return
				}
				got, err := box.Open(sealed)
				if err != nil {
					errs <- err
					return
				}
				if string(got) != demoPlain {
					errs <- errors.New("plaintext differs")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// BASE-14 规则 3：哨兵文本固定，带包名前缀，互不相同。
func TestSentinelTexts(t *testing.T) {
	t.Parallel()
	all := []error{ErrKeySize, ErrKeyEncoding, ErrMalformed, ErrUnknownVersion, ErrDecrypt}
	seen := map[string]bool{}
	for _, err := range all {
		text := err.Error()
		if !strings.HasPrefix(text, "secretbox: ") {
			t.Fatalf("sentinel %q lacks package prefix", text)
		}
		if seen[text] {
			t.Fatalf("duplicate sentinel text %q", text)
		}
		seen[text] = true
	}
}

func assertNoMaterial(t *testing.T, text string, material ...string) {
	t.Helper()
	for _, m := range material {
		if m != "" && strings.Contains(text, m) {
			t.Fatalf("error text leaks material: %q", text)
		}
	}
}
