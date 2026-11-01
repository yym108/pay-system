// Package server 在管理端口提供存活与就绪检查，并按固定顺序排空进程。
// Redis 与 Kafka 的检查由以后的功能点注册。没有注册时，/readyz 不因此失败。
package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	// DefaultDrainTimeout 是等待在途请求、消息处理和任务批次的生产默认上限。
	DefaultDrainTimeout = 10 * time.Second
	// CheckTimeout 是单项依赖检查的上限。account 的两个分片串行 Ping，注册时单独放宽。
	CheckTimeout = time.Second

	// 管理端口与需求 §2.2 一致。进程监听所有接口，宿主机只把这些端口绑到 127.0.0.1。
	// 绑到 127.0.0.1 时，以后容器端口发布连的是容器网卡，探活会失败。
	AddrGateway     = ":6060"
	AddrPayment     = ":6061"
	AddrAccount     = ":6062"
	AddrRecon       = ":6063"
	AddrChannelMock = ":6064"
)

// errUnavailable 是检查失败的固定文本。正文和错误都不带地址、DSN 或口令。
var errUnavailable = errors.New("unavailable")

// Check 报告一项依赖。非 nil 视为失败，错误文本不会写进响应。
type Check func(ctx context.Context) error

// Flusher 在排空之后刷新生产者。本次没有生产者，不注册时刷新为空。
type Flusher interface {
	Flush(ctx context.Context) error
}

// Group 是一组会接收新工作的服务。Stop 应停止接收并尽量在 ctx 到期前返回。
type Group interface {
	Serve() error
	Stop(ctx context.Context) error
}

type funcGroup struct {
	serve func() error
	stop  func(context.Context) error
}

// FuncGroup 用两个函数实现 Group。
func FuncGroup(serve func() error, stop func(context.Context) error) Group {
	return funcGroup{serve: serve, stop: stop}
}

func (g funcGroup) Serve() error                   { return g.serve() }
func (g funcGroup) Stop(ctx context.Context) error { return g.stop(ctx) }

// Config 是一次管理端口监听。DrainTimeout 小于等于 0 时用 DefaultDrainTimeout。
// Signals 非空时由调用方送入信号，测试用它代替进程信号。
// DB 是数据库检查。也可以在 Run 之前调用 RequireDB。两者都没有时 /readyz 为 503。
type Config struct {
	Addr         string
	Listener     net.Listener
	DrainTimeout time.Duration
	Signals      <-chan os.Signal
	DB           Check
}

type checkItem struct {
	fn    Check
	limit time.Duration
}

// Server 持有管理端口和可注册的检查、服务、生产者与关闭动作。
type Server struct {
	cfg Config

	mu       sync.Mutex
	db       *checkItem
	checks   []checkItem
	groups   []Group
	flushers []Flusher
	closers  []io.Closer

	draining atomic.Bool
}

// New 复制配置。DrainTimeout 小于等于 0 时改为默认 10 秒。
func New(cfg Config) *Server {
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = DefaultDrainTimeout
	}
	return &Server{cfg: cfg}
}

// RequireDB 注册必做的数据库检查。limit 小于等于 0 时用 CheckTimeout。
// account 两个分片串行各 1 秒，limit 传 2 秒，避免外层把第二次 Ping 提前取消。
func (s *Server) RequireDB(fn Check, limit time.Duration) {
	if limit <= 0 {
		limit = CheckTimeout
	}
	s.mu.Lock()
	s.db = &checkItem{fn: fn, limit: limit}
	s.mu.Unlock()
}

// RegisterCheck 注册数据库以外的检查，单项上限为 CheckTimeout。
func (s *Server) RegisterCheck(fn Check) {
	s.mu.Lock()
	s.checks = append(s.checks, checkItem{fn: fn, limit: CheckTimeout})
	s.mu.Unlock()
}

// RegisterGroup 注册要随进程一起停止的服务。Run 开始时复制已注册的组。
func (s *Server) RegisterGroup(g Group) {
	s.mu.Lock()
	s.groups = append(s.groups, g)
	s.mu.Unlock()
}

// RegisterFlusher 注册排空之后要刷新的生产者。
func (s *Server) RegisterFlusher(f Flusher) {
	s.mu.Lock()
	s.flushers = append(s.flushers, f)
	s.mu.Unlock()
}

// RegisterCloser 注册刷新之后要关闭的连接。
func (s *Server) RegisterCloser(c io.Closer) {
	s.mu.Lock()
	s.closers = append(s.closers, c)
	s.mu.Unlock()
}

// PingCheck 在 ctx 内 Ping。失败时返回固定错误，不带驱动原文。
func PingCheck(db *sqlx.DB) Check {
	return func(ctx context.Context) error {
		if db == nil {
			return errUnavailable
		}
		if err := db.PingContext(ctx); err != nil {
			return errUnavailable
		}
		return nil
	}
}

type runningGroup struct {
	g       Group
	done    chan struct{}
	started bool
}

// Run 监听管理端口并阻塞到 ctx 取消或收到信号。ctx 必须非 nil。
// 顺序是：/readyz 立即 503，停止各组接收，等待不超过 DrainTimeout，刷新生产者，关闭连接，再关管理端口。
// 监听失败时仍关闭已注册的连接。排空和刷新都从 ctx 派生，避免另起一个与调用方无关的根上下文。
func (s *Server) Run(ctx context.Context) error {
	s.ensureDB()
	groups, flushers, closers := s.snapshot()

	ln, err := s.listen(ctx)
	if err != nil {
		drainCtx, cancel := context.WithTimeout(ctx, s.cfg.DrainTimeout)
		s.stopGroups(groups, drainCtx)
		cancel()
		closeAll(closers)
		return err
	}

	httpSrv := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: time.Second,
		WriteTimeout:      3 * time.Second,
	}
	adminDone := make(chan struct{})
	var adminErr error
	go func() {
		err := httpSrv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			adminErr = err
		}
		close(adminDone)
	}()

	lives := make([]runningGroup, len(groups))
	groupErr := make(chan error, len(groups))
	for i, g := range groups {
		lives[i] = runningGroup{g: g.g, done: make(chan struct{}), started: true}
		go func(i int) {
			err := lives[i].g.Serve()
			if err != nil {
				groupErr <- err
			}
			close(lives[i].done)
		}(i)
	}

	sigs, stopSig := s.signals()
	defer stopSig()

	var runErr error
	select {
	case <-ctx.Done():
	case <-sigs:
	case err := <-groupErr:
		runErr = err
	case <-adminDone:
		runErr = adminErr
	}

	s.draining.Store(true)
	drainCtx, cancel := context.WithTimeout(ctx, s.cfg.DrainTimeout)
	s.stopGroups(lives, drainCtx)
	cancel()

	flushAll(ctx, flushers)
	closeAll(closers)

	shutCtx, shutCancel := context.WithTimeout(ctx, CheckTimeout)
	_ = httpSrv.Shutdown(shutCtx)
	shutCancel()
	<-adminDone
	return runErr
}

func (s *Server) ensureDB() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil && s.cfg.DB != nil {
		s.db = &checkItem{fn: s.cfg.DB, limit: CheckTimeout}
	}
}

func (s *Server) snapshot() ([]runningGroup, []Flusher, []io.Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	groups := make([]runningGroup, len(s.groups))
	for i, g := range s.groups {
		groups[i] = runningGroup{g: g}
	}
	return groups, append([]Flusher(nil), s.flushers...), append([]io.Closer(nil), s.closers...)
}

func (s *Server) listen(ctx context.Context) (net.Listener, error) {
	if s.cfg.Listener != nil {
		return s.cfg.Listener, nil
	}
	if s.cfg.Addr == "" {
		return nil, errors.New("admin listen address is empty")
	}
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", s.cfg.Addr)
}

func (s *Server) signals() (<-chan os.Signal, func()) {
	if s.cfg.Signals != nil {
		return s.cfg.Signals, func() {}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return ch, func() { signal.Stop(ch) }
}

func (s *Server) stopGroups(groups []runningGroup, ctx context.Context) {
	var wg sync.WaitGroup
	for i := range groups {
		if groups[i].g == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stopDone := make(chan struct{})
			go func() {
				_ = groups[i].g.Stop(ctx)
				close(stopDone)
			}()
			select {
			case <-stopDone:
			case <-ctx.Done():
				return
			}
			if !groups[i].started {
				return
			}
			select {
			case <-groups[i].done:
			case <-ctx.Done():
			}
		}(i)
	}
	wg.Wait()
}

func flushAll(ctx context.Context, flushers []Flusher) {
	if len(flushers) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, CheckTimeout)
	defer cancel()
	for _, f := range flushers {
		if f == nil {
			continue
		}
		_ = f.Flush(ctx)
	}
}

func closeAll(closers []io.Closer) {
	for _, c := range closers {
		if c == nil {
			continue
		}
		_ = c.Close()
	}
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	return mux
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeText(w, http.StatusOK, "ok")
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() || !s.dependenciesReady(r.Context()) {
		writeText(w, http.StatusServiceUnavailable, "unavailable")
		return
	}
	writeText(w, http.StatusOK, "ok")
}

func (s *Server) dependenciesReady(ctx context.Context) bool {
	s.mu.Lock()
	var db *checkItem
	if s.db != nil {
		copied := *s.db
		db = &copied
	}
	extras := append([]checkItem(nil), s.checks...)
	s.mu.Unlock()
	if db == nil || db.fn == nil {
		return false
	}

	items := make([]checkItem, 0, 1+len(extras))
	items = append(items, *db)
	items = append(items, extras...)
	var failed atomic.Bool
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go func(item checkItem) {
			defer wg.Done()
			if runCheck(ctx, item) != nil {
				failed.Store(true)
			}
		}(item)
	}
	wg.Wait()
	return !failed.Load()
}

func runCheck(parent context.Context, item checkItem) error {
	if item.fn == nil {
		return errUnavailable
	}
	limit := item.limit
	if limit <= 0 {
		limit = CheckTimeout
	}
	ctx, cancel := context.WithTimeout(parent, limit)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if recover() != nil {
				errCh <- errUnavailable
			}
		}()
		errCh <- item.fn(ctx)
	}()
	select {
	case err := <-errCh:
		if err != nil {
			return errUnavailable
		}
		return nil
	case <-ctx.Done():
		return errUnavailable
	}
}

func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
