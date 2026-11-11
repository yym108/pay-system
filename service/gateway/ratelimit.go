package gateway

import (
	"strconv"
	"time"

	"golang.org/x/time/rate"

	"github.com/yym108/pay-system/pkg/config"
)

const (
	// defaultRatePerSec 与 defaultBurst 对齐 RATE_LIMIT_DEFAULT 的默认值 100:200。
	// 没安装 WithRateLimit，或速率、突发不是正数时用这一对，不把“没设置”当成拒绝。
	defaultRatePerSec = 100
	defaultBurst      = 200
)

type limitSpec struct {
	limit rate.Limit
	burst int
}

func defaultLimitSpec() limitSpec {
	return limitSpec{limit: rate.Limit(defaultRatePerSec), burst: defaultBurst}
}

func limitSpecOrDefault(perSec, burst int) limitSpec {
	if perSec <= 0 || burst <= 0 {
		return defaultLimitSpec()
	}
	return limitSpec{limit: rate.Limit(perSec), burst: burst}
}

// WithRateLimit 安装进程内商户限流的默认配额和按 app_id 的覆盖。
// 速率或突发不是正数时仍用每秒 100、突发 200。
// 覆盖的键是请求头 X-App-Id 的原文，不是 merchant_id。
func WithRateLimit(def config.RateLimit, overrides config.RateLimitOverrides) Option {
	return func(h *Handler) {
		if h == nil {
			return
		}
		h.defLimit = limitSpecOrDefault(def.Rate(), def.Burst())
		h.byApp = limitOverrides(overrides)
	}
}

func limitOverrides(in config.RateLimitOverrides) map[string]limitSpec {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]limitSpec, len(in))
	for _, item := range in {
		if item.AppID == "" || item.Rate <= 0 || item.Burst <= 0 {
			continue
		}
		out[item.AppID] = limitSpec{limit: rate.Limit(item.Rate), burst: item.Burst}
	}
	return out
}

func (h *Handler) bucket(appID string) *rate.Limiter {
	if v, ok := h.limiters.Load(appID); ok {
		if lim, ok := v.(*rate.Limiter); ok {
			return lim
		}
	}
	spec := h.defLimit
	if alt, ok := h.byApp[appID]; ok {
		spec = alt
	}
	if spec.burst <= 0 || spec.limit <= 0 {
		spec = defaultLimitSpec()
	}
	created := rate.NewLimiter(spec.limit, spec.burst)
	actual, _ := h.limiters.LoadOrStore(appID, created)
	if lim, ok := actual.(*rate.Limiter); ok {
		return lim
	}
	return created
}

// retryAfterSeconds 用 ReserveN 在注入时钟上算还要等多久。
// 预约马上取消：被拒绝的请求不占用后面的令牌。不足 1 秒写成 1。
func retryAfterSeconds(lim *rate.Limiter, now time.Time) string {
	if lim == nil {
		return "1"
	}
	res := lim.ReserveN(now, 1)
	delay := res.DelayFrom(now)
	res.CancelAt(now)
	return formatRetryAfter(delay)
}

func formatRetryAfter(delay time.Duration) string {
	if delay <= 0 {
		return "1"
	}
	secs := int64(delay / time.Second)
	if delay%time.Second != 0 {
		secs++
	}
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(secs, 10)
}
