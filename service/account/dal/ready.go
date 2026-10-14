package dal

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
)

const readyPingTimeout = time.Second

// ReadyHandler 处理 GET /readyz。
// 两个池都在 1 秒内 Ping 成功则 200，正文 ok；否则 503，正文 unavailable。
// 不回显 ping 错误，错误里可能有地址。不监听端口。
func ReadyHandler(p *Pools) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeText(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !ready(r.Context(), p) {
			writeText(w, http.StatusServiceUnavailable, "unavailable")
			return
		}
		writeText(w, http.StatusOK, "ok")
	})
}

func ready(ctx context.Context, p *Pools) bool {
	if p == nil {
		return false
	}
	for shard := 0; shard <= 1; shard++ {
		db, err := p.ShardDB(shard)
		if err != nil || ping(ctx, db) != nil {
			return false
		}
	}
	return true
}

func ping(ctx context.Context, db *sqlx.DB) error {
	pingCtx, cancel := context.WithTimeout(ctx, readyPingTimeout)
	defer cancel()
	return db.PingContext(pingCtx)
}

func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
