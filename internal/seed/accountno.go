package seed

import (
	"fmt"
	"strconv"
)

const (
	accountNoPrefix = "AC"
	merchantDigits  = 6
)

var platformTypeCode = map[string]string{
	"CHANNEL_RECEIVABLE": "01",
	"FEE_INCOME":         "02",
	"TRANSIT":            "03",
}

// ShardOf 返回商户所属分片：商户号数字部分 % 2。格式必须是 M 加 6 位数字。
// 路由函数的正式归属是 ACC-13 的 pkg/shard，本函数只供种子使用，规则与需求 §2.5.1 相同。
func ShardOf(merchantID string) (int, error) {
	n, err := merchantNumber(merchantID)
	if err != nil {
		return 0, err
	}
	return int(n % 2), nil
}

// PlatformAccountNo 生成平台子账户号。编码固定，重复执行才会命中同一唯一键。
// 布局：AC + 分片 1 位 + 类型 2 位 + sub_no 4 位 + 12 个 0。类型 01/02/03 对应三个平台账户。
func PlatformAccountNo(shard int, accountType string, subNo int) (string, error) {
	if shard != 0 && shard != 1 {
		return "", fmt.Errorf("seed: shard %d out of range", shard)
	}
	code, ok := platformTypeCode[accountType]
	if !ok {
		return "", fmt.Errorf("seed: unknown platform account type %s", accountType)
	}
	if subNo < 0 || subNo > 9999 {
		return "", fmt.Errorf("seed: sub_no %d out of range", subNo)
	}
	return fmt.Sprintf("%s%d%s%04d%012d", accountNoPrefix, shard, code, subNo, 0), nil
}

// MerchantAccountNo 生成商户结算账户号。首位 9 与平台账户号（首位 0 或 1）错开。
func MerchantAccountNo(merchantID string) (string, error) {
	n, err := merchantNumber(merchantID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s9%018d", accountNoPrefix, n), nil
}

func merchantNumber(merchantID string) (uint64, error) {
	if len(merchantID) != 1+merchantDigits || merchantID[0] != 'M' {
		return 0, fmt.Errorf("seed: merchant id %q must be M plus %d digits", merchantID, merchantDigits)
	}
	for _, c := range merchantID[1:] {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("seed: merchant id %q must be M plus %d digits", merchantID, merchantDigits)
		}
	}
	n, err := strconv.ParseUint(merchantID[1:], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("seed: merchant id %q: %w", merchantID, err)
	}
	return n, nil
}
