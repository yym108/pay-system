package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

const (
	// notifyIDPrefix 是渠道回调发送号（约定 §2.2）。不落库，只出现在 body。
	notifyIDPrefix = "CN"
	// notifyMaxSends 含第一次，第 5 次失败后不再排。
	notifyMaxSends = 5
	// notifyAck 是平台确认送达的全部字节。大小写、引号、尾部换行都不算。
	notifyAck = "SUCCESS"
	// notifyMaxBody 是响应体读取上限。超出视为未送达。
	notifyMaxBody = 1024
	// notifyHTTPTimeout 是单次回调的上下文超时。
	notifyHTTPTimeout = 5 * time.Second
	// notifyPrepRetry 是发号、签名或计数更新没发出请求时的再试间隔。这次不计入 5 次。
	notifyPrepRetry = time.Second
	// notifyTimeLayout 与查单的 finished_at 相同：先截到毫秒，再标东八区。
	notifyTimeLayout = "2006-01-02T15:04:05.000Z07:00"
)

// notifyBackoff 按下标 0 对应「已经发出 1 次」。第 5 次失败不再取间隔。
var notifyBackoff = [...]time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

// NotifyStore 只服务回调计数。完成和关单的 Store 不包含它，避免那些路径发 HTTP。
type NotifyStore interface {
	// AddNotifySent 在短事务里按期望计数加一。true 表示占到了这次发送，调用方在事务外再 POST。
	AddNotifySent(ctx context.Context, id uint64, expected int) (bool, error)
	// GetByID 在条件更新影响 0 行时确认当前状态和计数。不存在返回 ok = false。
	GetByID(ctx context.Context, id uint64) (dal.Txn, bool, error)
}

var _ NotifyStore = (*dal.DB)(nil)

// notifyPayload 的字段顺序就是 JSON 顺序。不要 omitempty，也不要调整字段。
type notifyPayload struct {
	NotifyID     string `json:"notify_id"`
	TxnType      string `json:"txn_type"`
	ReqNo        string `json:"req_no"`
	ChannelTxnNo string `json:"channel_txn_no"`
	Amount       int64  `json:"amount"`
	Status       string `json:"status"`
	FinishedAt   string `json:"finished_at"`
}

type notifyPrepError struct {
	msg string
	err error
}

func (e *notifyPrepError) Error() string {
	if e == nil || e.err == nil {
		return "channel notify prepare failed"
	}
	return e.err.Error()
}

func (e *notifyPrepError) Unwrap() error { return e.err }

// Notifier 把终态回调排进 Scheduler。Handle 只入队。
// 同一 id 的发送链用进程内标记占住，不落库；进程退出后还没发出的重试丢失，Recover 不扫描终态行。
type Notifier struct {
	store  NotifyStore
	ids    IDGenerator
	clk    clock.Clock
	sched  Scheduler
	key    []byte
	client *http.Client

	mu     sync.Mutex
	chains map[uint64]struct{}
}

// NewNotifier 组装回调发送器。key 会被复制，调用方之后改切片不影响签名。
// 空密钥在每次尝试时拒绝，不在这里自旋。
func NewNotifier(store NotifyStore, ids IDGenerator, clk clock.Clock, sched Scheduler, key []byte) *Notifier {
	return &Notifier{
		store:  store,
		ids:    ids,
		clk:    clk,
		sched:  sched,
		key:    bytes.Clone(key),
		client: &http.Client{Timeout: notifyHTTPTimeout, CheckRedirect: refuseRedirect},
		chains: map[uint64]struct{}{},
	}
}

// refuseRedirect 让 3xx 原样返回。跟随重定向会把下一次响应当成送达。
func refuseRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// Handle 只把第一次发送排到当前时钟，然后返回。发送和重试都在 Scheduler 上执行。
// 同一 id 已经有一条发送链时直接返回，避免完成钩子被调用两次把次数打双。
func (n *Notifier) Handle(ctx context.Context, row dal.Txn) {
	if !shouldNotify(row) {
		return
	}
	n.mu.Lock()
	if _, ok := n.chains[row.ID]; ok {
		n.mu.Unlock()
		return
	}
	n.chains[row.ID] = struct{}{}
	n.mu.Unlock()
	n.again(ctx, row, row.NotifySentCount, 0)
}

// shouldNotify 决定要不要入队。DROP_NOTIFY、DUP_NOTIFY、SLOW 留在 CH-06，这里不发也不加计数。
func shouldNotify(row dal.Txn) bool {
	if row.Status != dal.StatusSuccess && row.Status != dal.StatusFailed {
		return false
	}
	if row.NotifyURL == "" || row.FinishedAt.IsZero() || row.NotifySentCount >= notifyMaxSends {
		return false
	}
	switch scenario.Scenario(row.Scenario) {
	case scenario.DropNotify, scenario.DupNotify, scenario.Slow:
		return false
	default:
		return true
	}
}

// again 按注入时钟排下一次尝试。after 为 0 时就是第一次发送。
func (n *Notifier) again(ctx context.Context, row dal.Txn, expected int, after time.Duration) {
	due := n.clk.Now().Add(after)
	n.sched.Schedule(due, func() { n.attempt(ctx, row, expected) })
}

// attempt 在真正 POST 之前占计数。发号、编码、签名或更新失败都不算发送。
func (n *Notifier) attempt(ctx context.Context, row dal.Txn, expected int) {
	if expected >= notifyMaxSends {
		return
	}
	raw, ts, sig, err := n.prepare(ctx, row)
	if err != nil {
		msg := "channel notify prepare failed"
		var prep *notifyPrepError
		if errors.As(err, &prep) && prep.msg != "" {
			msg = prep.msg
		}
		n.logErr(ctx, msg, row, err)
		n.again(ctx, row, expected, notifyPrepRetry)
		return
	}
	applied, err := n.store.AddNotifySent(ctx, row.ID, expected)
	if err != nil {
		n.logErr(ctx, "channel notify count update failed", row, err)
		n.again(ctx, row, expected, notifyPrepRetry)
		return
	}
	if !applied {
		n.afterMiss(ctx, row, expected)
		return
	}
	sent := expected + 1
	if err = n.post(ctx, row.NotifyURL, ts, sig, raw); err != nil {
		n.logErr(ctx, "channel notify not delivered", row, err)
		if sent >= notifyMaxSends {
			return
		}
		n.again(ctx, row, sent, notifyBackoff[sent-1])
		return
	}
	slog.InfoContext(ctx, "channel notify delivered",
		slog.String("channel_txn_no", row.ChannelTxnNo),
		slog.Int("notify_sent_count", sent))
}

func (n *Notifier) prepare(ctx context.Context, row dal.Txn) (raw []byte, ts, sig string, err error) {
	id, err := n.ids.NewNo(ctx, notifyIDPrefix)
	if err != nil {
		return nil, "", "", &notifyPrepError{msg: "channel notify id failed", err: err}
	}
	raw, err = encodeNotify(row, id)
	if err != nil {
		return nil, "", "", &notifyPrepError{msg: "channel notify encode failed", err: err}
	}
	ts = sign.FormatTimestamp(n.clk.Now())
	sig, err = sign.SignChannel(n.key, sign.ChannelMessage{Timestamp: ts, Body: raw})
	if err != nil {
		return nil, "", "", &notifyPrepError{msg: "channel notify sign failed", err: err}
	}
	return raw, ts, sig, nil
}

// afterMiss 在条件更新影响 0 行时再读。次数用尽或不再是终态就停，不改状态，也不发 HTTP。
func (n *Notifier) afterMiss(ctx context.Context, row dal.Txn, expected int) {
	readCtx, cancel := context.WithTimeout(ctx, rereadTimeout)
	defer cancel()
	fresh, ok, err := n.store.GetByID(readCtx, row.ID)
	if err != nil {
		n.logErr(ctx, "channel notify reread failed", row, err)
		n.again(ctx, row, expected, notifyPrepRetry)
		return
	}
	if !ok || fresh.NotifySentCount >= notifyMaxSends || !terminalStatus(fresh.Status) {
		return
	}
	n.again(ctx, fresh, fresh.NotifySentCount, notifyPrepRetry)
}

func terminalStatus(status string) bool {
	return status == dal.StatusSuccess || status == dal.StatusFailed
}

func encodeNotify(row dal.Txn, notifyID string) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(notifyPayload{
		NotifyID:     notifyID,
		TxnType:      row.TxnType,
		ReqNo:        row.ReqNo,
		ChannelTxnNo: row.ChannelTxnNo,
		Amount:       int64(row.Amount),
		Status:       row.Status,
		FinishedAt:   row.FinishedAt.In(shanghai).Truncate(time.Millisecond).Format(notifyTimeLayout),
	})
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// post 在计数事务提交之后调用。nil 表示送达。错误不含 URL、密钥、签名和响应正文。
func (n *Notifier) post(ctx context.Context, rawURL, ts, sig string, body []byte) error {
	reqCtx, cancel := context.WithTimeout(ctx, notifyHTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return errors.New("channel notify request rejected")
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set(sign.HeaderMockTimestamp, ts)
	req.Header.Set(sign.HeaderMockSignature, sig)
	resp, err := n.client.Do(req)
	if err != nil {
		return safeErr(err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(io.LimitReader(resp.Body, notifyMaxBody+1))
	if err != nil {
		return errors.New("channel notify body read failed")
	}
	if len(got) > notifyMaxBody {
		return errors.New("channel notify body too long")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("channel notify status %d", resp.StatusCode)
	}
	if string(got) != notifyAck {
		return errors.New("channel notify body rejected")
	}
	return nil
}

func (n *Notifier) logErr(ctx context.Context, msg string, row dal.Txn, err error) {
	slog.ErrorContext(ctx, msg,
		slog.String("channel_txn_no", row.ChannelTxnNo),
		slog.String("error", safeErr(err).Error()))
}

// safeErr 去掉 url.Error。它的 Error() 带完整 URL，查询里的口令不能进日志。
func safeErr(err error) error {
	if err == nil {
		return errors.New("channel notify failed")
	}
	var uerr *url.Error
	if errors.As(err, &uerr) {
		if uerr.Err == nil {
			return errors.New("channel notify transport failed")
		}
		return safeErr(uerr.Err)
	}
	return err
}
