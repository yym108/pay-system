package shard

import (
	"errors"
	"hash/crc32"
	"math"
	"math/rand"
	"strings"
	"testing"
)

// ACC-13 验收：M100001、M100003 路由到 1，M100002 路由到 0；非法商户号返回错误。
func TestMerchantRoute(t *testing.T) {
	t.Setenv("ACCOUNT_SHARD_COUNT", "1")

	valid := []struct {
		id   string
		want int
	}{
		{id: "M100001", want: 1},
		{id: "M100003", want: 1},
		{id: "M100002", want: 0},
		{id: "M000001", want: 1},
	}
	for _, tt := range valid {
		t.Run(tt.id, func(t *testing.T) {
			got, err := Of(tt.id)
			if err != nil {
				t.Fatalf("Of(%s) error = %v", tt.id, err)
			}
			if got != tt.want {
				t.Fatalf("Of(%s) = %d, want %d", tt.id, got, tt.want)
			}
		})
	}

	invalid := []string{
		"",
		"M10000",
		"M10001",
		"M1000011",
		"m100001",
		"M10000A",
		"100001",
	}
	for _, id := range invalid {
		t.Run("bad-"+id, func(t *testing.T) {
			got, err := Of(id)
			if !errors.Is(err, ErrMerchantID) {
				t.Fatalf("Of error = %v, want ErrMerchantID", err)
			}
			if got != 0 {
				t.Fatalf("Of = %d, want 0", got)
			}
			assertPlainError(t, err)
		})
	}
}

// ACC-13 验收：同一 biz_no 多次选择的子账户相同；10 万个随机 biz_no 在 8 个子账户上的分布偏差 < 5%。
func TestSubAccountDistribution(t *testing.T) {
	t.Setenv("PLATFORM_SUB_ACCOUNTS", "1")

	const biz = "PAY20261015001"
	const n = 8
	first, err := Sub(biz, n)
	if err != nil {
		t.Fatalf("Sub error = %v", err)
	}
	for i := 0; i < 5; i++ {
		got, err := Sub(biz, n)
		if err != nil {
			t.Fatalf("Sub error = %v", err)
		}
		if got != first {
			t.Fatalf("Sub = %d, want %d", got, first)
		}
	}
	want := int(crc32.ChecksumIEEE([]byte(biz)) % uint32(n))
	if first != want {
		t.Fatalf("Sub = %d, want crc32 %% %d = %d", first, n, want)
	}

	for _, sample := range []string{"a", "M100001", biz} {
		got, err := Sub(sample, 1)
		if err != nil || got != 0 {
			t.Fatalf("Sub(%q, 1) = %d, %v; want 0, nil", sample, got, err)
		}
	}

	_, err = Sub("", n)
	if !errors.Is(err, ErrBizNo) {
		t.Fatalf("empty biz no error = %v", err)
	}
	assertPlainError(t, err)
	_, err = Sub(biz, 0)
	if !errors.Is(err, ErrSubCount) {
		t.Fatalf("n=0 error = %v", err)
	}
	assertPlainError(t, err)
	_, err = Sub(biz, -1)
	if !errors.Is(err, ErrSubCount) {
		t.Fatalf("n=-1 error = %v", err)
	}
	assertPlainError(t, err)
	huge := int(math.MaxUint32) + 1
	_, err = Sub(biz, huge)
	if !errors.Is(err, ErrSubCount) {
		t.Fatalf("n too large error = %v", err)
	}
	assertPlainError(t, err)

	const total = 100_000
	const expected = total / n
	counts := [n]int{}
	// 固定种子只为复现分布，不用于安全。
	rng := rand.New(rand.NewSource(distSeed)) //nolint:gosec // G404: 分布测试要可复现，不用加密随机。
	sum := 0
	for i := 0; i < total; i++ {
		bizNo := randomBizNo(t, rng)
		got, err := Sub(bizNo, n)
		if err != nil {
			t.Fatalf("Sub error = %v", err)
		}
		if got < 0 || got >= n {
			t.Fatalf("Sub = %d", got)
		}
		counts[got]++
		sum++
	}
	if sum != total {
		t.Fatalf("sum = %d, want %d", sum, total)
	}
	var maxDev float64
	for i, count := range counts {
		dev := absFloat(count-expected) / float64(expected)
		if dev > maxDev {
			maxDev = dev
		}
		if dev >= 0.05 {
			t.Fatalf("bucket %d count %d deviation %g, counts=%v seed=%d", i, count, dev, counts, distSeed)
		}
	}
	t.Logf("ACC-13 distribution seed=%d counts=%v max_deviation=%g", distSeed, counts, maxDev)
}

// distSeed 固定 10 万个 biz_no 的随机序列。某个种子失败就换种子，不放宽 5%。
const distSeed int64 = 20261015

func randomBizNo(t *testing.T, rng *rand.Rand) string {
	t.Helper()
	var raw [16]byte
	if _, err := rng.Read(raw[:]); err != nil {
		t.Fatal(err)
	}
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(raw)*2)
	for i, b := range raw {
		out[i*2] = hexdigits[b>>4]
		out[i*2+1] = hexdigits[b&0x0f]
	}
	return string(out)
}

func absFloat(n int) float64 {
	if n < 0 {
		n = -n
	}
	return float64(n)
}

func assertPlainError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
	text := err.Error()
	if strings.Contains(text, "10001") || strings.Contains(strings.ToLower(text), "dsn") {
		t.Fatalf("error text = %q", text)
	}
}
