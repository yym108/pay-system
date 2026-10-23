package biz

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/yym108/pay-system/service/channel_mock/dal"
)

var errStore = errors.New("store down")

// memStore 是内存版 mock_txn。Insert 遇到 (txn_type, req_no) 已存在时返回 MySQL 1062，
// 与 uk_type_req_no 的行为一致；Finish 只在 PROCESSING 时生效，与 CAS 一致。
type memStore struct {
	mu      sync.Mutex
	nextID  uint64
	rows    map[string]*dal.Txn
	inserts int
	gets    int
	lists   int

	insertErr error
	// insertCommitErr 非 nil 时先写入行再返回它，模拟提交结果不确定（行已落库但调用方拿到错误）。
	insertCommitErr error
	getErr          error
	getByIDErrs     []error
	getByIDs        int
	listErr         error
	// finishErrs 依次作为 Finish 的返回错误（不改行），用完后正常执行。
	finishErrs []error
	// finishCommitErrs 依次在 Finish 生效后返回，模拟提交结果不确定。
	finishCommitErrs []error
	// hideOnGet 为真时 Get 报告不存在，模拟 1062 后读不到行。
	hideOnGet bool
	// afterInsert 在 Insert 写入行之后、返回之前调用（不持锁），用来模拟请求在提交后被取消。
	afterInsert func()
}

func newMemStore() *memStore {
	return &memStore{rows: map[string]*dal.Txn{}}
}

func key(txnType, reqNo string) string { return txnType + "|" + reqNo }

func (m *memStore) Insert(_ context.Context, t dal.Txn) (uint64, error) {
	id, err := m.insert(t)
	if m.afterInsert != nil {
		m.afterInsert()
	}
	if err == nil && m.insertCommitErr != nil {
		return 0, m.insertCommitErr
	}
	return id, err
}

func (m *memStore) insert(t dal.Txn) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inserts++
	if m.insertErr != nil {
		return 0, m.insertErr
	}
	k := key(t.TxnType, t.ReqNo)
	if _, ok := m.rows[k]; ok {
		return 0, fmt.Errorf("insert mock_txn: %w", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"})
	}
	for _, r := range m.rows {
		if r.ChannelTxnNo == t.ChannelTxnNo {
			return 0, fmt.Errorf("insert mock_txn: %w", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"})
		}
	}
	m.nextID++
	t.ID = m.nextID
	row := t
	m.rows[k] = &row
	return t.ID, nil
}

func (m *memStore) Get(ctx context.Context, txnType, reqNo string) (dal.Txn, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	if err := ctx.Err(); err != nil {
		return dal.Txn{}, false, err
	}
	if m.getErr != nil {
		return dal.Txn{}, false, m.getErr
	}
	if m.hideOnGet {
		return dal.Txn{}, false, nil
	}
	r, ok := m.rows[key(txnType, reqNo)]
	if !ok {
		return dal.Txn{}, false, nil
	}
	return *r, true, nil
}

func (m *memStore) Finish(_ context.Context, f dal.Finish) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.finishErrs) > 0 {
		err := m.finishErrs[0]
		m.finishErrs = m.finishErrs[1:]
		return false, err
	}
	for _, r := range m.rows {
		if r.ID != f.ID {
			continue
		}
		if r.Status != dal.StatusProcessing {
			return false, nil
		}
		r.Status = f.Status
		r.FinishedAt = f.FinishedAt
		r.BillDate = f.BillDate
		if len(m.finishCommitErrs) > 0 {
			err := m.finishCommitErrs[0]
			m.finishCommitErrs = m.finishCommitErrs[1:]
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (m *memStore) GetByID(ctx context.Context, id uint64) (dal.Txn, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getByIDs++
	if err := ctx.Err(); err != nil {
		return dal.Txn{}, false, err
	}
	if len(m.getByIDErrs) > 0 {
		err := m.getByIDErrs[0]
		m.getByIDErrs = m.getByIDErrs[1:]
		return dal.Txn{}, false, err
	}
	for _, r := range m.rows {
		if r.ID == id {
			return *r, true, nil
		}
	}
	return dal.Txn{}, false, nil
}

func (m *memStore) ListProcessing(_ context.Context, txnType string, afterID uint64, limit int) ([]dal.Txn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lists++
	if m.listErr != nil {
		return nil, m.listErr
	}
	var out []dal.Txn
	for _, r := range m.rows {
		if r.TxnType == txnType && r.Status == dal.StatusProcessing && r.ID > afterID {
			out = append(out, *r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// row 返回当前行的副本。
func (m *memStore) row(reqNo string) (dal.Txn, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rows[key(dal.TxnTypePay, reqNo)]
	if !ok {
		return dal.Txn{}, false
	}
	return *r, true
}

// put 直接写一行，模拟重启前留下的数据或别的功能点（CH-03 关单）改过的行。
func (m *memStore) put(t dal.Txn) dal.Txn {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	t.ID = m.nextID
	row := t
	m.rows[key(t.TxnType, t.ReqNo)] = &row
	return t
}

func (m *memStore) setStatus(reqNo, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[key(dal.TxnTypePay, reqNo)].Status = status
}

func (m *memStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rows)
}

// seqIDs 产生递增的 MC 流水号。
type seqIDs struct {
	mu  sync.Mutex
	n   int
	err error
}

func (s *seqIDs) NewNo(_ context.Context, prefix string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return "", s.err
	}
	s.n++
	return fmt.Sprintf("%s%020d", prefix, s.n), nil
}

// manualSched 记录任务，RunDue 同步执行到期的任务。任务执行中再调度的任务进入下一轮。
type manualSched struct {
	mu    sync.Mutex
	tasks []schedTask
}

type schedTask struct {
	due  time.Time
	task func()
}

func (m *manualSched) Schedule(due time.Time, task func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks = append(m.tasks, schedTask{due: due, task: task})
}

// RunDue 执行 due 不晚于 now 的任务，返回执行个数。
func (m *manualSched) RunDue(now time.Time) int {
	m.mu.Lock()
	var run []func()
	var keep []schedTask
	for _, t := range m.tasks {
		if !t.due.After(now) {
			run = append(run, t.task)
		} else {
			keep = append(keep, t)
		}
	}
	m.tasks = keep
	m.mu.Unlock()
	for _, f := range run {
		f()
	}
	return len(run)
}

func (m *manualSched) dues() []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]time.Time, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t.due)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

func (m *manualSched) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tasks)
}

// hookRecorder 记录 OnFinished 的调用。
type hookRecorder struct {
	mu    sync.Mutex
	calls []dal.Txn
	ctxs  []context.Context
}

func (h *hookRecorder) fn(ctx context.Context, t dal.Txn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, t)
	h.ctxs = append(h.ctxs, ctx)
}

func (h *hookRecorder) contexts() []context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]context.Context(nil), h.ctxs...)
}

func (h *hookRecorder) list() []dal.Txn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]dal.Txn(nil), h.calls...)
}
