package main

import "crypto/rand"

const (
	nonceAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	nonceLen      = 16
	// nonceBiasLimit 是 62*4。大于等于它的随机字节丢弃，避免取模偏置。
	nonceBiasLimit = 248
)

// randNonce 用 crypto/rand 生成 16 位 [A-Za-z0-9] nonce。
type randNonce struct{}

func (randNonce) next() (string, error) {
	var out [nonceLen]byte
	filled := 0
	var buf [32]byte
	for filled < nonceLen {
		if _, err := rand.Read(buf[:]); err != nil {
			return "", err
		}
		for _, b := range buf {
			if b >= nonceBiasLimit {
				continue
			}
			out[filled] = nonceAlphabet[int(b)%len(nonceAlphabet)]
			filled++
			if filled == nonceLen {
				break
			}
		}
	}
	return string(out[:]), nil
}

func nonceOK(s string) bool {
	if len(s) < 16 || len(s) > 32 {
		return false
	}
	for _, c := range s {
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}
