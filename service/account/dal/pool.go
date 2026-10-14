// Package dal 持有账户服务的两个分片连接池。
//
// 事务通过 dbx.WithTx 打开，回调只能拿到其中一个池的 *sqlx.Tx。
package dal

import (
	"context"
	"errors"
	"sync"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
)

// Pools 是分片 0 和分片 1 的连接池。
// 不要复制。mu 保护 db 和 own，ReadyHandler 与 Close 可以同时调用。
type Pools struct {
	mu  sync.RWMutex
	db  [2]*sqlx.DB
	own bool
}

// Open 用 dbx.Open 打开两个分片池。第二个失败时关闭第一个。
// 成功时调用方拥有这两个池，必须 Close。
// 失败时返回 dbx 的错误，里面可能有地址。调用方不要把它写进日志。
func Open(ctx context.Context, dsn0, dsn1 string, pool dbx.PoolConfig) (*Pools, error) {
	db0, err := dbx.Open(ctx, dsn0, pool)
	if err != nil {
		return nil, err
	}
	db1, err := dbx.Open(ctx, dsn1, pool)
	if err != nil {
		// 调用方拿不到第一个池，这里不关就会泄漏到进程退出。
		_ = db0.Close()
		return nil, err
	}
	return &Pools{db: [2]*sqlx.DB{db0, db1}, own: true}, nil
}

// New 用已经打开的池构造。不取得关闭权，Close 不会关闭它们。
// 测试因此可以复用自己打开的库，并在结束时自己关闭。
func New(db0, db1 *sqlx.DB) *Pools {
	return &Pools{db: [2]*sqlx.DB{db0, db1}}
}

// ShardDB 返回分片 0 或 1 的池。其他分片号返回错误。
// 在读锁内复制出 *sqlx.DB。调用方的 Ping 不持有这把锁。
func (p *Pools) ShardDB(shard int) (*sqlx.DB, error) {
	if p == nil || (shard != 0 && shard != 1) {
		return nil, errors.New("dal: shard must be 0 or 1")
	}
	p.mu.RLock()
	db := p.db[shard]
	p.mu.RUnlock()
	if db == nil {
		return nil, errors.New("dal: shard db is nil")
	}
	return db, nil
}

// Close 关闭 Open 打开的池。New 构造的池保持打开。
// 写锁内只摘下池并清掉 own，DB.Close 在锁外，避免和 Ping 互相等待。
func (p *Pools) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if !p.own {
		p.mu.Unlock()
		return nil
	}
	dbs := p.db
	p.db = [2]*sqlx.DB{}
	p.own = false
	p.mu.Unlock()

	var err error
	for _, db := range dbs {
		if db == nil {
			continue
		}
		if cerr := db.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
