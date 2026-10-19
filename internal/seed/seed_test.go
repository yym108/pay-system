package seed

import (
	"bytes"
	"strings"
	"testing"
)

// BASE-03 规则：分片 = 商户号数字部分 % 2。演示商户用来构造同分片和跨分片转账。
func TestShardOf(t *testing.T) {
	t.Parallel()
	cases := []struct {
		id    string
		shard int
		ok    bool
	}{
		{id: "M100001", shard: 1, ok: true},
		{id: "M100002", shard: 0, ok: true},
		{id: "M100003", shard: 1, ok: true},
		{id: "M200000", shard: 0, ok: true},
		{id: "M200099", shard: 1, ok: true},
		{id: "100001", ok: false},
		{id: "M10000", ok: false},
		{id: "M1000001", ok: false},
		{id: "MX00001", ok: false},
	}
	for _, tc := range cases {
		got, err := ShardOf(tc.id)
		if tc.ok {
			if err != nil {
				t.Errorf("ShardOf(%s) error: %v", tc.id, err)
				continue
			}
			if got != tc.shard {
				t.Errorf("ShardOf(%s) = %d, want %d", tc.id, got, tc.shard)
			}
			continue
		}
		if err == nil {
			t.Errorf("ShardOf(%s) = %d, want error", tc.id, got)
		}
	}
}

// BASE-03 口径澄清：账户号确定性，重复执行命中同一行。
func TestAccountNo(t *testing.T) {
	t.Parallel()
	platform, err := PlatformAccountNo(0, "CHANNEL_RECEIVABLE", 0)
	if err != nil {
		t.Fatal(err)
	}
	if platform != "AC0010000000000000000" || len(platform) != 21 {
		t.Fatalf("platform account no = %s", platform)
	}
	sub, err := PlatformAccountNo(1, "TRANSIT", 7)
	if err != nil {
		t.Fatal(err)
	}
	if sub != "AC1030007000000000000" {
		t.Fatalf("transit sub account no = %s", sub)
	}
	merchant, err := MerchantAccountNo("M100001")
	if err != nil {
		t.Fatal(err)
	}
	if merchant != "AC9000000000000100001" || len(merchant) != 21 {
		t.Fatalf("merchant account no = %s", merchant)
	}
	if _, err := PlatformAccountNo(2, "FEE_INCOME", 0); err == nil {
		t.Fatal("shard 2 should be rejected")
	}
	if _, err := MerchantAccountNo("M1"); err == nil {
		t.Fatal("short merchant id should be rejected")
	}
}

// BASE-03 口径澄清：密文格式与 BASE-14 规则一致，同一明文两次加密不同，篡改后无法解密。
func TestSealLayoutAndRoundTrip(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x11}, 32)
	plain := []byte("demo_secret_M100001_0123456789ab")
	first, err := Seal(key, plain)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Seal(key, plain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("same plaintext encrypted twice must differ")
	}
	if first[0] != CipherVersion {
		t.Fatalf("version byte = %d, want %d", first[0], CipherVersion)
	}
	if len(first) != 1+12+len(plain)+16 {
		t.Fatalf("cipher length = %d, want %d", len(first), 1+12+len(plain)+16)
	}
	got, err := Open(key, first)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("opened %q", got)
	}
	tampered := append([]byte(nil), first...)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := Open(key, tampered); err == nil {
		t.Fatal("tampered ciphertext opened")
	}
	if _, err := Seal(key[:31], plain); err == nil {
		t.Fatal("31-byte key should fail")
	}
	if _, err := Open(key, first[:10]); err == nil {
		t.Fatal("short blob should fail")
	}
}

// BASE-03 不变量 3：种子插入不得在冲突时回写余额或冻结金额。
func TestInsertSQLDoesNotRewriteBalance(t *testing.T) {
	t.Parallel()
	for _, stmt := range []string{insertAccountSQL, insertMerchantSQL} {
		upper := strings.ToUpper(stmt)
		if !strings.Contains(upper, "ON DUPLICATE KEY UPDATE") {
			t.Fatalf("statement must be idempotent:\n%s", stmt)
		}
		if strings.Contains(upper, "BALANCE =") || strings.Contains(upper, "FROZEN_AMOUNT =") {
			t.Fatalf("idempotent update must not assign balance columns:\n%s", stmt)
		}
	}
	if !strings.Contains(insertAccountSQL, ", 0, 0,") {
		t.Fatal("account insert must write zero balance and frozen amount")
	}
}

// PAY-10：种子用库里的费率和 created_at 补第一档，重复执行不改费率。
func TestFeeRateSeedDoesNotOverwrite(t *testing.T) {
	t.Parallel()
	upper := strings.ToUpper(insertFeeRateSQL)
	if !strings.Contains(upper, "INSERT INTO MERCHANT_FEE_RATE") {
		t.Fatal("fee rate seed must insert into merchant_fee_rate")
	}
	if !strings.Contains(upper, "SELECT MERCHANT.MERCHANT_ID, MERCHANT.FEE_RATE_BP, MERCHANT.CREATED_AT") ||
		!strings.Contains(upper, "FROM MERCHANT") ||
		!strings.Contains(upper, "WHERE MERCHANT.MERCHANT_ID = ?") {
		t.Fatal("fee rate seed must copy the stored rate and created_at")
	}
	if !strings.Contains(upper, "ON DUPLICATE KEY UPDATE MERCHANT_ID = MERCHANT_FEE_RATE.MERCHANT_ID") {
		t.Fatal("duplicate fee tier must not change the stored rate")
	}
	if strings.Contains(upper, "FEE_RATE_BP =") || strings.Contains(upper, "EFFECTIVE_FROM =") {
		t.Fatalf("idempotent update must not assign rate columns:\n%s", insertFeeRateSQL)
	}
}
