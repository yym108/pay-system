package seed

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

// CipherVersion 是种子密文的版本字节。BASE-14 实现 pkg/secretbox 时必须沿用同一格式：
// version(1B) + nonce(12B) + ciphertext + tag。本包不替代 BASE-14。
const CipherVersion byte = 1

// Seal 用 AES-256-GCM 加密。每次调用使用随机 nonce，因此同一明文两次结果不同。
func Seal(key, plaintext []byte) ([]byte, error) {
	gcm, err := gcmBox(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("seed: read nonce: %w", err)
	}
	sealed := gcm.Seal(nil, nonce, plaintext, nil)
	out := make([]byte, 1+len(nonce)+len(sealed))
	out[0] = CipherVersion
	copy(out[1:], nonce)
	copy(out[1+len(nonce):], sealed)
	return out, nil
}

// Open 解密 Seal 产生的密文。失败时不返回明文或密文内容。
func Open(key, blob []byte) ([]byte, error) {
	gcm, err := gcmBox(key)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(blob) < 1+nonceSize+gcm.Overhead() || blob[0] != CipherVersion {
		return nil, fmt.Errorf("seed: decrypt failed")
	}
	nonce := blob[1 : 1+nonceSize]
	ciphertext := blob[1+nonceSize:]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("seed: decrypt failed")
	}
	return plain, nil
}

func gcmBox(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("seed: master key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("seed: aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("seed: gcm: %w", err)
	}
	return gcm, nil
}
