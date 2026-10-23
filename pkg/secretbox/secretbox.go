// Package secretbox 用 AES-256-GCM 加密商户签名密钥（docs/02-requirements.md BASE-14）。
//
// 密文格式：version(1B) + nonce(12B) + ciphertext + tag(16B)。version 固定为 1，
// 与 BASE-03 种子已写入 merchant.app_secret_cipher 的密文相同，本包能直接解开。
// 不使用附加认证数据（AAD）：种子密文没有 AAD，加上会让已有密文全部解不开。
//
// 主密钥来自 SECRET_MASTER_KEY，标准 base64（带填充），解码后必须是 32 字节。
// 每次加密从 crypto/rand 取 12 字节随机 nonce，同一明文两次加密结果不同。
//
// 错误都是本包的哨兵，文本固定，不含主密钥、明文和密文。这些都是本端配置或内部数据故障，
// 平台服务按已有的 10003 处理并打 ERROR 日志，不新增错误码。
// Box 不保留调用方传入的 key 切片；内部 AES 轮密钥与主密钥等价，Box 应按密钥材料对待
// （不序列化、不打印、长期持有于进程内即可）。Box 可被多个 goroutine 同时使用。
//
// 版本字节不受 GCM tag 保护，当前靠 Open 显式拒绝非 1 的版本。将来的 v2 应把版本字节（及商户号）纳入 AAD。
package secretbox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

const (
	// Version 是当前密文格式的版本字节。
	Version byte = 1
	// KeySize 是主密钥的字节数（AES-256）。
	KeySize = 32
	// NonceSize 是 GCM nonce 的字节数。
	NonceSize = 12
	// TagSize 是 GCM 认证标签的字节数。
	TagSize = 16
	// Overhead 是密文比明文多出的字节数：版本、nonce、标签。
	// merchant.app_secret_cipher 是 VARBINARY(256)，明文最长 256 - Overhead = 227 字节。
	Overhead = 1 + NonceSize + TagSize
)

var (
	// ErrKeySize 表示主密钥不是 32 字节。
	ErrKeySize = errors.New("secretbox: master key must be 32 bytes")
	// ErrKeyEncoding 表示主密钥不是标准 base64（带填充）。
	ErrKeyEncoding = errors.New("secretbox: master key is not standard base64")
	// ErrMalformed 表示密文短于 Overhead，不是本包的格式。
	ErrMalformed = errors.New("secretbox: malformed ciphertext")
	// ErrUnknownVersion 表示版本字节不是 Version。
	ErrUnknownVersion = errors.New("secretbox: unknown ciphertext version")
	// ErrDecrypt 表示认证失败：密文或 nonce 被改动、被截断，或主密钥不对。
	ErrDecrypt = errors.New("secretbox: decrypt failed")
)

// Box 持有由主密钥派生的 AES-256-GCM 实例。零值不可用，用 New 或 NewFromBase64 创建。
type Box struct {
	aead cipher.AEAD
}

// New 用 32 字节主密钥创建 Box。长度不对返回 ErrKeySize。本函数不保留 key 切片的引用。
func New(key []byte) (*Box, error) {
	if len(key) != KeySize {
		return nil, ErrKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		// 长度已校验，这里只在标准库内部故障时出现。底层错误不含密钥。
		return nil, fmt.Errorf("secretbox: aes: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secretbox: gcm: %w", err)
	}
	return &Box{aead: aead}, nil
}

// NewFromBase64 解码 SECRET_MASTER_KEY 的原始值后创建 Box。
// 不是标准 base64 返回 ErrKeyEncoding，解码后不是 32 字节返回 ErrKeySize。
func NewFromBase64(encoded string) (*Box, error) {
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// 不包装 base64 的错误：它带出错位置，没有必要暴露给日志。
		return nil, ErrKeyEncoding
	}
	return New(key)
}

// Seal 加密 plaintext，返回 version + nonce + ciphertext + tag。返回的切片是新分配的。
func (b *Box) Seal(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("secretbox: read nonce: %w", err)
	}
	out := make([]byte, 1+NonceSize, Overhead+len(plaintext))
	out[0] = Version
	copy(out[1:], nonce)
	return b.aead.Seal(out, nonce, plaintext, nil), nil
}

// Open 解密 Seal 的输出。失败时返回 nil 和固定文本的哨兵：
// 短于 Overhead 为 ErrMalformed，版本不对为 ErrUnknownVersion，认证失败为 ErrDecrypt。
// 是否成功只看 err：空明文解密成功时返回的切片可能是 nil。返回的切片不与 blob 共享内存。
func (b *Box) Open(blob []byte) ([]byte, error) {
	if len(blob) < Overhead {
		return nil, ErrMalformed
	}
	if blob[0] != Version {
		return nil, ErrUnknownVersion
	}
	plain, err := b.aead.Open(nil, blob[1:1+NonceSize], blob[1+NonceSize:], nil)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plain, nil
}
