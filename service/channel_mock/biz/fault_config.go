package biz

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
)

// ErrInvalidFaultConfig 表示全局故障配置参数越界或非法。
var ErrInvalidFaultConfig = errors.New("biz: invalid fault config")

// FaultConfig 是模拟渠道的全局故障配置（CH-07），仅在内存中保存，重启后恢复默认值。
// 概率字段和延迟范围仅作用于 NORMAL 场景。
type FaultConfig struct {
	DelayMinMS        int     `json:"delay_min_ms"`
	DelayMaxMS        int     `json:"delay_max_ms"`
	DupNotifyRate     float64 `json:"dup_notify_rate"`     //nolint:forbidigo // 概率配置值，非金额
	DropNotifyRate    float64 `json:"drop_notify_rate"`    //nolint:forbidigo // 概率配置值，非金额
	AcceptErrorRate   float64 `json:"accept_error_rate"`   //nolint:forbidigo // 概率配置值，非金额
	AcceptTimeoutRate float64 `json:"accept_timeout_rate"` //nolint:forbidigo // 概率配置值，非金额
}

// FaultConfigPatch 表示对 FaultConfig 的局部更新，nil 字段保持原值。
type FaultConfigPatch struct {
	DelayMinMS        *int     `json:"delay_min_ms"`
	DelayMaxMS        *int     `json:"delay_max_ms"`
	DupNotifyRate     *float64 `json:"dup_notify_rate"`     //nolint:forbidigo // 概率配置值，非金额
	DropNotifyRate    *float64 `json:"drop_notify_rate"`    //nolint:forbidigo // 概率配置值，非金额
	AcceptErrorRate   *float64 `json:"accept_error_rate"`   //nolint:forbidigo // 概率配置值，非金额
	AcceptTimeoutRate *float64 `json:"accept_timeout_rate"` //nolint:forbidigo // 概率配置值，非金额
}

// FaultConfigStore 并发安全地持有并校验全局故障配置。
type FaultConfigStore struct {
	mu       sync.RWMutex
	cfg      FaultConfig
	randFunc func() float64 //nolint:forbidigo // 概率采样函数，非金额
}

// NewFaultConfigStore 用默认延迟范围创建配置存储。概率默认全部为 0。
func NewFaultConfigStore(defaultDelayMinMS, defaultDelayMaxMS int) *FaultConfigStore {
	if defaultDelayMinMS < 0 {
		defaultDelayMinMS = 0
	}
	if defaultDelayMaxMS < defaultDelayMinMS {
		defaultDelayMaxMS = defaultDelayMinMS
	}
	return &FaultConfigStore{
		cfg: FaultConfig{
			DelayMinMS: defaultDelayMinMS,
			DelayMaxMS: defaultDelayMaxMS,
		},
		randFunc: cryptoRandFloat64,
	}
}

// Get 返回当前全局故障配置的快照。
func (s *FaultConfigStore) Get() FaultConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// Update 校验并应用局部或全量配置更新，返回更新后的完整配置。
func (s *FaultConfigStore) Update(patch FaultConfigPatch) (FaultConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.cfg
	if patch.DelayMinMS != nil {
		next.DelayMinMS = *patch.DelayMinMS
	}
	if patch.DelayMaxMS != nil {
		next.DelayMaxMS = *patch.DelayMaxMS
	}
	if patch.DupNotifyRate != nil {
		next.DupNotifyRate = *patch.DupNotifyRate
	}
	if patch.DropNotifyRate != nil {
		next.DropNotifyRate = *patch.DropNotifyRate
	}
	if patch.AcceptErrorRate != nil {
		next.AcceptErrorRate = *patch.AcceptErrorRate
	}
	if patch.AcceptTimeoutRate != nil {
		next.AcceptTimeoutRate = *patch.AcceptTimeoutRate
	}
	if err := validateFaultConfig(next); err != nil {
		return FaultConfig{}, err
	}
	s.cfg = next
	return next, nil
}

func validateFaultConfig(c FaultConfig) error {
	if c.DelayMinMS < 0 {
		return fmt.Errorf("%w: delay_min_ms must be >= 0", ErrInvalidFaultConfig)
	}
	if c.DelayMaxMS < 0 {
		return fmt.Errorf("%w: delay_max_ms must be >= 0", ErrInvalidFaultConfig)
	}
	if c.DelayMinMS > c.DelayMaxMS {
		return fmt.Errorf("%w: delay_min_ms must be <= delay_max_ms", ErrInvalidFaultConfig)
	}
	rates := []struct {
		name string
		val  float64 //nolint:forbidigo // 概率配置值，非金额
	}{
		{"dup_notify_rate", c.DupNotifyRate},
		{"drop_notify_rate", c.DropNotifyRate},
		{"accept_error_rate", c.AcceptErrorRate},
		{"accept_timeout_rate", c.AcceptTimeoutRate},
	}
	for _, r := range rates {
		if math.IsNaN(r.val) || math.IsInf(r.val, 0) || r.val < 0 || r.val > 1 {
			return fmt.Errorf("%w: %s must be in [0, 1]", ErrInvalidFaultConfig, r.name)
		}
	}
	return nil
}

func cryptoRandFloat64() float64 { //nolint:forbidigo // 概率配置采样，非金额
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0
	}
	v := binary.BigEndian.Uint64(buf[:]) >> 11
	return float64(v) / (1 << 53) //nolint:forbidigo // 概率配置采样，非金额
}

func (s *FaultConfigStore) sample() float64 { //nolint:forbidigo // 概率配置采样，非金额
	if s.randFunc != nil {
		return s.randFunc()
	}
	return cryptoRandFloat64()
}

func (s *FaultConfigStore) hit(rate float64) bool { //nolint:forbidigo // 概率配置值，非金额
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	return s.sample() < rate
}

// shouldAcceptError 判定本次 NORMAL 受理是否命中错误率（返回 500）。
func (s *FaultConfigStore) shouldAcceptError() bool {
	if s == nil {
		return false
	}
	return s.hit(s.Get().AcceptErrorRate)
}

// shouldAcceptTimeout 判定本次 NORMAL 受理是否命中阻塞 5 秒的概率。
func (s *FaultConfigStore) shouldAcceptTimeout() bool {
	if s == nil {
		return false
	}
	return s.hit(s.Get().AcceptTimeoutRate)
}

type notifyMode uint8

const (
	notifyNormal notifyMode = iota
	notifyDrop
	notifyDup
)

// decideNotifyMode 按当前 drop_notify_rate 与 dup_notify_rate 决定 NORMAL 交易的回调方式。
// 单次采样互斥判定：先判定丢回调，再判定重复回调。
func (s *FaultConfigStore) decideNotifyMode() notifyMode {
	if s == nil {
		return notifyNormal
	}
	cfg := s.Get()
	if cfg.DropNotifyRate <= 0 && cfg.DupNotifyRate <= 0 {
		return notifyNormal
	}
	if cfg.DropNotifyRate >= 1 {
		return notifyDrop
	}
	if cfg.DropNotifyRate <= 0 && cfg.DupNotifyRate >= 1 {
		return notifyDup
	}
	r := s.sample()
	if r < cfg.DropNotifyRate {
		return notifyDrop
	}
	if r < cfg.DropNotifyRate+cfg.DupNotifyRate {
		return notifyDup
	}
	return notifyNormal
}
