package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// DurationList 是逗号分隔的正时长。至少一段。测试可以把查单退避调短。
type DurationList []time.Duration

// UnmarshalText 解析逗号分隔的时长。空段、非正时长都拒绝。
func (d *DurationList) UnmarshalText(text []byte) error {
	s := strings.TrimSpace(string(text))
	if s == "" {
		return errors.New("at least one duration is required")
	}
	parts := strings.Split(s, ",")
	out := make(DurationList, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return errors.New("at least one duration is required")
		}
		v, err := time.ParseDuration(part)
		if err != nil || v <= 0 {
			return errors.New("durations must be positive")
		}
		out = append(out, v)
	}
	*d = out
	return nil
}

func (d DurationList) String() string {
	parts := make([]string, len(d))
	for i, v := range d {
		parts[i] = v.String()
	}
	return strings.Join(parts, ",")
}

// RateLimit 是进程内限流的速率和突发，格式为 rate:burst，两个都必须是正整数。
type RateLimit struct {
	rate  int
	burst int
}

// Rate 返回每秒速率。
func (r RateLimit) Rate() int { return r.rate }

// Burst 返回突发容量。
func (r RateLimit) Burst() int { return r.burst }

func (r RateLimit) String() string {
	return fmt.Sprintf("%d:%d", r.rate, r.burst)
}

// UnmarshalText 解析 rate:burst。
func (r *RateLimit) UnmarshalText(text []byte) error {
	rate, burst, err := parseRateBurst(string(text))
	if err != nil {
		return err
	}
	r.rate = rate
	r.burst = burst
	return nil
}

// RateLimitOverride 是单个商户的限流覆盖，格式为 app_id:rate:burst。
type RateLimitOverride struct {
	AppID string
	Rate  int
	Burst int
}

// RateLimitOverrides 是逗号分隔的商户限流覆盖。空字符串合法。
type RateLimitOverrides []RateLimitOverride

// UnmarshalText 解析覆盖列表。空字符串得到空列表。
func (o *RateLimitOverrides) UnmarshalText(text []byte) error {
	s := strings.TrimSpace(string(text))
	if s == "" {
		*o = nil
		return nil
	}
	parts := strings.Split(s, ",")
	out := make(RateLimitOverrides, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		app, rest, ok := strings.Cut(part, ":")
		if part == "" || !ok || strings.TrimSpace(app) == "" {
			return errors.New("must be app_id:rate:burst")
		}
		rate, burst, err := parseRateBurst(rest)
		if err != nil {
			return errors.New("must be app_id:rate:burst")
		}
		out = append(out, RateLimitOverride{AppID: strings.TrimSpace(app), Rate: rate, Burst: burst})
	}
	*o = out
	return nil
}

func (o RateLimitOverrides) String() string {
	parts := make([]string, len(o))
	for i, item := range o {
		parts[i] = fmt.Sprintf("%s:%d:%d", item.AppID, item.Rate, item.Burst)
	}
	return strings.Join(parts, ",")
}

func parseRateBurst(s string) (int, int, error) {
	left, right, ok := strings.Cut(strings.TrimSpace(s), ":")
	if !ok || strings.Contains(right, ":") {
		return 0, 0, errors.New("must be rate:burst")
	}
	rate, err1 := strconv.Atoi(strings.TrimSpace(left))
	burst, err2 := strconv.Atoi(strings.TrimSpace(right))
	if err1 != nil || err2 != nil || rate <= 0 || burst <= 0 {
		return 0, 0, errors.New("rate and burst must be positive integers")
	}
	return rate, burst, nil
}

func decodeMasterKey(key string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(key)
}

func checkNonNegative(name string, d time.Duration) error {
	if d < 0 {
		return invalid(name, "must be >= 0")
	}
	return nil
}

func checkPort(name string, port int) error {
	if port < 1 || port > 65535 {
		return invalid(name, "must be between 1 and 65535")
	}
	return nil
}

func checkHost(name, host string) error {
	if host == "" {
		return invalid(name, "must not be empty")
	}
	return nil
}

func checkAdminToken(token string) error {
	if utf8.RuneCountInString(token) < 32 {
		return invalid("ADMIN_TOKEN", "must be at least 32 characters")
	}
	return nil
}

func checkReconRunAt(v string) error {
	if len(v) != 5 || v[2] != ':' {
		return invalid("RECON_RUN_AT", "must be HH:MM")
	}
	hour, errH := strconv.Atoi(v[:2])
	minute, errM := strconv.Atoi(v[3:])
	if errH != nil || errM != nil || hour > 23 || minute > 59 {
		return invalid("RECON_RUN_AT", "must be HH:MM")
	}
	return nil
}

func checkSeedLoad(n int) error {
	if n != 0 && n != 1 {
		return invalid("SEED_LOAD", "must be 0 or 1")
	}
	return nil
}

func checkDelays(minMS, maxMS int) error {
	if minMS < 0 {
		return invalid("MOCK_DELAY_MIN_MS", "must be >= 0")
	}
	if maxMS < 0 {
		return invalid("MOCK_DELAY_MAX_MS", "must be >= 0")
	}
	if minMS > maxMS {
		return invalid("MOCK_DELAY_MIN_MS", "must be <= MOCK_DELAY_MAX_MS")
	}
	return nil
}
