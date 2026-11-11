package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	timestampWindowSeconds int64 = 300
	nonceTTLSeconds              = 600
	nonceOpTimeout               = 50 * time.Millisecond
)

// nonceUnavailable 是进程内的 pay_nonce_unavailable_total。只在存储失败时加一。
// OPS-03 再暴露 /metrics，这里不加标签。
var nonceUnavailable atomic.Uint64

var errNonceUnavailable = errors.New("nonce unavailable")

var noncePattern = regexp.MustCompile(`^[A-Za-z0-9]{16,32}$`)

// NonceStore 占用一次 app_id 与 nonce。
// claimed 为 true 表示这次占到。claimed 为 false 且 err 为 nil 表示已经存在。
// err 非 nil 表示存储不可用，这次不能当成占用成功。
type NonceStore interface {
	Reserve(ctx context.Context, appID, nonce string) (claimed bool, err error)
}

type memoryNonce struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewMemoryNonce 返回进程内存储。并发安全。键是 app_id 与 nonce，不是 merchant_id。
func NewMemoryNonce() NonceStore {
	return &memoryNonce{seen: map[string]struct{}{}}
}

func (m *memoryNonce) Reserve(_ context.Context, appID, nonce string) (bool, error) {
	if m == nil {
		return false, errNonceUnavailable
	}
	key := appID + "\x00" + nonce
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = map[string]struct{}{}
	}
	if _, ok := m.seen[key]; ok {
		return false, nil
	}
	m.seen[key] = struct{}{}
	return true, nil
}

type redisNonce struct {
	client *redis.Client
}

// OpenRedisNonce 按 addr 构造客户端。连接是懒的，这里不拨号，地址不可达也不报错。
// 第二项给 /readyz Ping。第三项在进程退出时关闭。
// SET 的 50ms 只包这一次命令的上下文，不写进客户端的全局超时，避免把 /readyz 的 1 秒 Ping 截短。
// MaxRetries 用 -1：0 会被 go-redis 改成默认重试 3 次。需求是超时后不重试。
func OpenRedisNonce(addr string) (NonceStore, func(context.Context) error, io.Closer) {
	client := redis.NewClient(&redis.Options{
		Addr:       addr,
		MaxRetries: -1,
	})
	store := &redisNonce{client: client}
	return store, store.Ping, client
}

func (n *redisNonce) Reserve(ctx context.Context, appID, nonce string) (bool, error) {
	if n == nil || n.client == nil {
		return false, errNonceUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, nonceOpTimeout)
	defer cancel()
	key := fmt.Sprintf("gw:nonce:%s:%s", appID, nonce)
	text, err := n.client.Do(ctx, "SET", key, "1", "NX", "EX", nonceTTLSeconds).Text()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if text != "OK" {
		return false, errNonceUnavailable
	}
	return true, nil
}

func (n *redisNonce) Ping(ctx context.Context) error {
	if n == nil || n.client == nil {
		return errNonceUnavailable
	}
	if err := n.client.Ping(ctx).Err(); err != nil {
		return errNonceUnavailable
	}
	return nil
}

func timestampOutside(nowUnix, ts int64) bool {
	// 差值为负只可能是 int64 相减回绕。回绕当成超出窗口，请求会被拒绝。
	if nowUnix >= ts {
		diff := nowUnix - ts
		return diff > timestampWindowSeconds || diff < 0
	}
	diff := ts - nowUnix
	return diff > timestampWindowSeconds || diff < 0
}
