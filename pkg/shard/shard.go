// Package shard 计算账户分片和平台子账户。
//
// Of 和 Sub 不读环境变量，也不返回业务码。
// 分片数固定为 2。子账户个数由调用方传入。
package shard

import (
	"errors"
	"hash/crc32"
	"math"
)

const (
	shardCount     = 2
	merchantDigits = 6
)

var (
	// ErrMerchantID 表示商户号不是 M 加恰好 6 位数字。
	ErrMerchantID = errors.New("shard: merchant id must be M followed by 6 digits")
	// ErrSubCount 表示子账户个数不是正数，或放不进 uint32。
	ErrSubCount = errors.New("shard: sub account count must be positive")
	// ErrBizNo 表示业务单号为空。
	ErrBizNo = errors.New("shard: biz no is empty")
)

// Of 返回商户所在的账户分片。
// 商户号必须是 M 加恰好 6 位数字。分片是数字部分对 2 取模。
// 格式非法时返回 ErrMerchantID，不回显商户号，也不返回业务码。
func Of(merchantID string) (int, error) {
	if len(merchantID) != merchantDigits+1 || merchantID[0] != 'M' {
		return 0, ErrMerchantID
	}
	n := 0
	for i := 1; i < len(merchantID); i++ {
		c := merchantID[i]
		if c < '0' || c > '9' {
			return 0, ErrMerchantID
		}
		n = n*10 + int(c-'0')
	}
	return n % shardCount, nil
}

// Sub 返回平台子账户号：crc32.ChecksumIEEE(bizNo) % uint32(n)。
// n 由调用方传入。n < 1、n 超过 uint32，或 bizNo 为空时返回错误。
// 同一 bizNo 和 n 的结果不变。函数不记录曾经用过的 n。
func Sub(bizNo string, n int) (int, error) {
	// uint32(n) 在 n 超过 MaxUint32 时会截断，截成 0 后取模会 panic。
	if n < 1 || uint64(n) > math.MaxUint32 {
		return 0, ErrSubCount
	}
	if bizNo == "" {
		return 0, ErrBizNo
	}
	sum := crc32.ChecksumIEEE([]byte(bizNo))
	return int(sum % uint32(n)), nil
}
