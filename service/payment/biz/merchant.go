package biz

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/secretbox"
)

// redacted 是 Secret 在任何输出里的替代文本。
const redacted = "***"

// Secret 装着明文密钥，只有 Reveal 取得到明文。
// 明文放在指针后面：fmt 碰到未导出字段时调不了方法、改走反射，
// 而反射对非顶层的指针只打印地址，不解引用，所以明文也出不来。
// 导出路径上 Format、MarshalJSON、LogValue 一律输出 ***。
type Secret struct {
	p *string
}

// NewSecret 包装明文密钥。
func NewSecret(plain string) Secret {
	return Secret{p: &plain}
}

// Reveal 返回明文。只用于把凭证交给 RPC 响应，不要传给日志或错误。
func (s Secret) Reveal() string {
	if s.p == nil {
		return ""
	}
	return *s.p
}

// String 返回 ***，零值也一样，不暴露是否为空。
func (Secret) String() string { return redacted }

// GoString 让直接调用的 %#v 风格输出也是带引号的 ***。
func (Secret) GoString() string { return `"` + redacted + `"` }

// Format 覆盖所有格式动词（%v、%+v、%#v、%s、%q、%x、%d 等），fmt 先于 String 调用它。
func (Secret) Format(f fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = io.WriteString(f, `"`+redacted+`"`)
		return
	}
	_, _ = io.WriteString(f, redacted)
}

// MarshalJSON 让 json.Marshal 与 slog JSON handler 输出 "***"。
func (Secret) MarshalJSON() ([]byte, error) {
	return []byte(`"` + redacted + `"`), nil
}

// LogValue 让 slog 直接记录 Secret 时输出 ***。
func (Secret) LogValue() slog.Value { return slog.StringValue(redacted) }

// MerchantCredential 是网关验签用的商户凭证（PAY-01）。
// AppSecret 是 Secret，fmt、json、slog 都只输出 ***；取明文要显式调用 Reveal。
type MerchantCredential struct {
	MerchantID       string
	Status           string
	AppSecret        Secret
	SecretVersion    int
	DefaultNotifyURL string
}

// String 保持原有的可读格式；%#v 由字段各自格式化，AppSecret 也是 ***。
func (c MerchantCredential) String() string {
	return fmt.Sprintf("{MerchantID:%s Status:%s AppSecret:%s SecretVersion:%d DefaultNotifyURL:%s}",
		c.MerchantID, c.Status, c.AppSecret, c.SecretVersion, c.DefaultNotifyURL)
}

// LogValue 让 slog 只记录非敏感字段，并保持 snake_case 键名。
func (c MerchantCredential) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("merchant_id", c.MerchantID),
		slog.String("status", c.Status),
		slog.Int("secret_version", c.SecretVersion),
	)
}

// MerchantConfig 是按 merchant_id 读出的商户静态配置，进程内缓存。
// 不含费率：费率按时刻生效（PAY-10），缓存当前档会在追加新档后最多一个 TTL 内用错，
// 所以生效费率另由 FeeRateAt 每次读 merchant_fee_rate。
type MerchantConfig struct {
	MerchantID       string
	Status           string
	DefaultNotifyURL string
}

// Merchants 读商户凭证和配置。可被多个 goroutine 同时使用。
// 凭证不缓存（网关侧缓存属于 GW-08）；配置按 merchant_id 缓存 ttl，ttl <= 0 时每次读库。
type Merchants struct {
	store merchantStore
	box   *secretbox.Box
	clk   clock.Clock
	ttl   time.Duration

	mu    sync.RWMutex
	cache map[string]cachedConfig
}

type cachedConfig struct {
	cfg      MerchantConfig
	loadedAt time.Time
}

// NewMerchants 使用调用方传入的库、密钥盒和时钟。db 或 box 为 nil 时，对应方法返回 10003。
func NewMerchants(db *sqlx.DB, box *secretbox.Box, clk clock.Clock, ttl time.Duration) *Merchants {
	var store merchantStore
	if db != nil {
		store = sqlMerchantStore{db: db}
	}
	return newMerchants(store, box, clk, ttl)
}

func newMerchants(store merchantStore, box *secretbox.Box, clk clock.Clock, ttl time.Duration) *Merchants {
	return &Merchants{store: store, box: box, clk: clk, ttl: ttl, cache: map[string]cachedConfig{}}
}

func (m *Merchants) guard(ctx context.Context) error {
	if m == nil || ctx == nil {
		return errno.ErrInternal
	}
	// 已取消的上下文原样返回，由 RPC 层变成超时，不写成 10003。
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.store == nil {
		return errno.ErrInternal
	}
	return nil
}

// GetMerchantCredential 按 app_id 读商户并解密密钥。
// DISABLED 商户照常返回，由网关决定是否拒绝。app_id 为空或不存在返回 20004。
// 解密失败是本端配置或数据故障，返回 10003，并打一条只含 app_id、merchant_id 的 ERROR 日志。
func (m *Merchants) GetMerchantCredential(ctx context.Context, appID string) (MerchantCredential, error) {
	if err := m.guard(ctx); err != nil {
		return MerchantCredential{}, err
	}
	if m.box == nil {
		return MerchantCredential{}, errno.ErrInternal
	}
	if appID == "" {
		return MerchantCredential{}, errno.ErrMerchantInvalid
	}
	row, found, err := m.store.credentialByAppID(ctx, appID)
	if err != nil {
		return MerchantCredential{}, callErr(err)
	}
	if !found {
		return MerchantCredential{}, errno.ErrMerchantInvalid
	}
	plain, err := m.box.Open(row.Cipher)
	if err != nil {
		// 不记 err 和密文：错误是固定哨兵，但密文和明文都不进日志。
		// 返回不带原因的哨兵，handler 不会再打第二条 ERROR。
		slog.ErrorContext(ctx, "decrypt merchant secret failed",
			"app_id", appID,
			"merchant_id", row.MerchantID,
		)
		return MerchantCredential{}, errno.ErrInternal
	}
	return MerchantCredential{
		MerchantID:       row.MerchantID,
		Status:           row.Status,
		AppSecret:        NewSecret(string(plain)),
		SecretVersion:    row.SecretVersion,
		DefaultNotifyURL: row.NotifyURL,
	}, nil
}

// MerchantConfig 按 merchant_id 读商户状态和默认通知地址。
// TTL 内返回缓存；不存在返回 20004，不存在和读库失败都不进缓存。
// 时钟回拨时缓存视为过期。
func (m *Merchants) MerchantConfig(ctx context.Context, merchantID string) (MerchantConfig, error) {
	if err := m.guard(ctx); err != nil {
		return MerchantConfig{}, err
	}
	if m.clk == nil {
		return MerchantConfig{}, errno.ErrInternal
	}
	if merchantID == "" {
		return MerchantConfig{}, errno.ErrMerchantInvalid
	}
	// 读库前取时间：装载时刻宁早不晚，缓存不会比 TTL 活得更久。
	now := m.clk.Now()
	if cfg, ok := m.cached(merchantID, now); ok {
		return cfg, nil
	}
	cfg, found, err := m.store.configByID(ctx, merchantID)
	if err != nil {
		return MerchantConfig{}, callErr(err)
	}
	if !found {
		return MerchantConfig{}, errno.ErrMerchantInvalid
	}
	if m.ttl > 0 {
		m.mu.Lock()
		m.cache[merchantID] = cachedConfig{cfg: cfg, loadedAt: now}
		m.mu.Unlock()
	}
	return cfg, nil
}

func (m *Merchants) cached(merchantID string, now time.Time) (MerchantConfig, bool) {
	if m.ttl <= 0 {
		return MerchantConfig{}, false
	}
	m.mu.RLock()
	entry, ok := m.cache[merchantID]
	m.mu.RUnlock()
	if !ok || now.Before(entry.loadedAt) || now.Sub(entry.loadedAt) >= m.ttl {
		return MerchantConfig{}, false
	}
	return entry.cfg, true
}

// FeeRateAt 返回 at 时刻已生效的费率档：merchant_fee_rate 中 effective_from <= at 的最近一档。
// 与 PAY-10 同口径，不读 merchant.fee_rate_bp，也不缓存。
// 没有已生效的档时 found 为 false、err 为 nil，由调用方（PAY-02）决定如何处理。
// merchantID 为空或 at 为零值返回 10001。
func (m *Merchants) FeeRateAt(ctx context.Context, merchantID string, at time.Time) (rate FeeRate, found bool, err error) {
	if err := m.guard(ctx); err != nil {
		return FeeRate{}, false, err
	}
	if merchantID == "" || at.IsZero() {
		return FeeRate{}, false, errno.ErrInvalidParam
	}
	rate, found, err = m.store.feeRateAt(ctx, merchantID, at)
	if err != nil {
		return FeeRate{}, false, callErr(err)
	}
	return rate, found, nil
}

// credentialRow 是 merchant 表里凭证相关的列。Cipher 是 app_secret_cipher 原值。
type credentialRow struct {
	MerchantID    string
	Status        string
	Cipher        []byte
	SecretVersion int
	NotifyURL     string
}

// merchantStore 是商户读路径。只读，不开事务。假实现只给不连库的单元测试。
type merchantStore interface {
	credentialByAppID(ctx context.Context, appID string) (credentialRow, bool, error)
	configByID(ctx context.Context, merchantID string) (MerchantConfig, bool, error)
	feeRateAt(ctx context.Context, merchantID string, at time.Time) (FeeRate, bool, error)
}

// 列跟随库默认 utf8mb4_bin：区分大小写，但 PAD SPACE 会忽略尾部空格；
// 集成测试临时库是 utf8mb4_0900_ai_ci，不区分大小写和重音。两种规则下 WHERE 都可能命中字节不同的值，
// 所以读回 app_id、merchant_id 在 Go 里逐字节比较，只认完全相同的标识；仍走唯一索引。
//
//nolint:gosec // G101 误报：这是查询语句，列名含 secret，不是硬编码凭证。
const credentialByAppIDSQL = `
SELECT app_id, merchant_id, status, app_secret_cipher, secret_version, notify_url
FROM merchant
WHERE app_id = ?`

const configByIDSQL = `
SELECT merchant_id, status, notify_url
FROM merchant
WHERE merchant_id = ?`

// 走 uk_merchant_effective_from (merchant_id, effective_from)。
const feeRateAtSQL = `
SELECT merchant_id, fee_rate_bp, effective_from
FROM merchant_fee_rate
WHERE merchant_id = ? AND effective_from <= ?
ORDER BY effective_from DESC
LIMIT 1`

type sqlMerchantStore struct {
	db *sqlx.DB
}

func (s sqlMerchantStore) credentialByAppID(ctx context.Context, appID string) (credentialRow, bool, error) {
	var row credentialRow
	var stored string
	err := s.db.QueryRowContext(ctx, credentialByAppIDSQL, appID).
		Scan(&stored, &row.MerchantID, &row.Status, &row.Cipher, &row.SecretVersion, &row.NotifyURL)
	if errors.Is(err, sql.ErrNoRows) {
		return credentialRow{}, false, nil
	}
	if err != nil {
		return credentialRow{}, false, err
	}
	// X-App-Id 不在签名串里。大小写不同也能查到的话，同一请求换个大小写就能绕过按 app_id 记的 nonce。
	if stored != appID {
		return credentialRow{}, false, nil
	}
	return row, true, nil
}

func (s sqlMerchantStore) configByID(ctx context.Context, merchantID string) (MerchantConfig, bool, error) {
	var cfg MerchantConfig
	err := s.db.QueryRowContext(ctx, configByIDSQL, merchantID).
		Scan(&cfg.MerchantID, &cfg.Status, &cfg.DefaultNotifyURL)
	if errors.Is(err, sql.ErrNoRows) {
		return MerchantConfig{}, false, nil
	}
	if err != nil {
		return MerchantConfig{}, false, err
	}
	if cfg.MerchantID != merchantID {
		return MerchantConfig{}, false, nil
	}
	return cfg, true, nil
}

func (s sqlMerchantStore) feeRateAt(ctx context.Context, merchantID string, at time.Time) (FeeRate, bool, error) {
	row, err := scanRate(s.db.QueryRowContext(ctx, feeRateAtSQL, merchantID, at))
	if errors.Is(err, sql.ErrNoRows) {
		return FeeRate{}, false, nil
	}
	if err != nil {
		return FeeRate{}, false, err
	}
	return row, true, nil
}
