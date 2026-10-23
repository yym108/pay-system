// Package biz 是模拟渠道的业务逻辑：支付受理（CH-01）与按场景自动完成。
//
// 幂等靠 uk_type_req_no：先 INSERT，冲突（1062）再读已有行，不做先查后插。
// 状态推进只用 CAS（WHERE id = ? AND status = 'PROCESSING'），事务内不调用外部系统；
// 完成钩子在 CAS 生效、事务提交后才调用。时间一律取注入的 clock.Clock。
package biz

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

// 受理结果，见 docs/05-api.md §9.2。
const (
	ResultOK       = "OK"
	ResultRejected = "REJECTED"

	ReasonParamConflict = "PARAM_CONFLICT"
	ReasonOrderClosed   = "ORDER_CLOSED"
)

const (
	// channelTxnPrefix 是渠道流水号前缀（约定 §2.2）。
	channelTxnPrefix = "MC"
	// maxReqNoRunes 与 mock_txn.req_no VARCHAR(32) 一致。
	maxReqNoRunes = 32
	// maxNotifyURLRunes 与 mock_txn.notify_url VARCHAR(512) 一致。
	maxNotifyURLRunes = 512
	// rereadTimeout 是 INSERT 报非 1062 错误后再读一次的上限。
	rereadTimeout = time.Second
)

// ErrInvalidRequest 表示请求参数非法。handler 映射为 HTTP 400，不写库。
var ErrInvalidRequest = errors.New("biz: invalid pay request")

// errDuplicateWithoutRow 表示 INSERT 报 1062 但按 (PAY, req_no) 读不到行，
// 只会是 channel_txn_no 撞号。按内部错误处理，调用方重试即可。
var errDuplicateWithoutRow = errors.New("biz: duplicate key without matching req_no")

// shanghai 是 bill_date 的时区。用固定偏移，不依赖 tzdata。
var shanghai = time.FixedZone("Asia/Shanghai", 8*60*60)

// Store 是 mock_txn 的存取，由 dal.DB 实现。
type Store interface {
	// Insert 写入新行并返回 id；唯一键冲突返回可被 dbx.IsDuplicate 识别的错误。
	Insert(ctx context.Context, t dal.Txn) (uint64, error)
	// Get 按 (txn_type, req_no) 读一行，不存在返回 ok = false。
	Get(ctx context.Context, txnType, reqNo string) (dal.Txn, bool, error)
	// GetByID 按主键读一行，不存在返回 ok = false。
	GetByID(ctx context.Context, id uint64) (dal.Txn, bool, error)
	// Finish 只在 PROCESSING 时推进终态，返回是否生效（已提交）。
	Finish(ctx context.Context, f dal.Finish) (bool, error)
	// ListProcessing 按 id 升序分页列出 PROCESSING 行。
	ListProcessing(ctx context.Context, txnType string, afterID uint64, limit int) ([]dal.Txn, error)
}

// IDGenerator 发渠道流水号，由 idgen.Generator 实现。
type IDGenerator interface {
	NewNo(ctx context.Context, prefix string) (string, error)
}

// Scheduler 在 due 时刻（按注入时钟）执行 task。due 已过时尽快执行。
type Scheduler interface {
	Schedule(due time.Time, task func())
}

// Options 是自动完成的参数。
type Options struct {
	// DelayMinMS、DelayMaxMS 是 MOCK_DELAY_MIN_MS、MOCK_DELAY_MAX_MS，见 Delay。
	DelayMinMS int
	DelayMaxMS int
	// OnFinished 在 CAS 生效、事务提交后调用一次，参数是完成后的行。供 CH-05 回调使用，可为 nil。
	// 在调度器的 goroutine 里执行，不能长时间阻塞。
	// ctx 只带受理请求的值，不会被取消、也没有截止时间：钩子做 I/O 时自己设超时。
	OnFinished func(ctx context.Context, t dal.Txn)
}

// Service 是模拟渠道的支付受理与自动完成。并发安全。
type Service struct {
	store Store
	ids   IDGenerator
	clk   clock.Clock
	sched Scheduler
	opts  Options
}

// New 组装服务。参数都不能为 nil（OnFinished 除外）。
func New(store Store, ids IDGenerator, clk clock.Clock, sched Scheduler, opts Options) *Service {
	return &Service{store: store, ids: ids, clk: clk, sched: sched, opts: opts}
}

// PayRequest 是 POST /mockpay/v1/pay 的请求。
type PayRequest struct {
	ReqNo     string
	Amount    money.Amount
	Subject   string
	NotifyURL string
}

// PayResult 是受理结果。REJECTED 时 ChannelTxnNo 为空，Status 是已有行的当前状态。
type PayResult struct {
	Result       string
	Reason       string
	ChannelTxnNo string
	Status       string
}

// Pay 受理一笔支付（CH-01）。
//
// 新 req_no：写入 PROCESSING 行并按场景调度自动完成，返回 OK。
// 已存在：CLOSED → REJECTED / ORDER_CLOSED（不论金额）；金额一致 → OK，带已有流水号和当前状态；
// 金额不一致 → REJECTED / PARAM_CONFLICT。已存在时不改行、不重复调度。
// INSERT 报非 1062 错误时行可能已提交，见 recoverInsert。
// 参数非法返回 ErrInvalidRequest；其余错误是内部错误。
func (s *Service) Pay(ctx context.Context, req PayRequest) (PayResult, error) {
	if err := ctx.Err(); err != nil {
		return PayResult{}, err
	}
	if err := validatePay(req); err != nil {
		return PayResult{}, err
	}
	no, err := s.ids.NewNo(ctx, channelTxnPrefix)
	if err != nil {
		return PayResult{}, fmt.Errorf("new channel_txn_no: %w", err)
	}
	t := dal.Txn{
		ChannelTxnNo: no,
		TxnType:      dal.TxnTypePay,
		ReqNo:        req.ReqNo,
		Amount:       req.Amount,
		Status:       dal.StatusProcessing,
		Scenario:     scenario.ForPay(int64(req.Amount)).String(),
		NotifyURL:    req.NotifyURL,
		CreatedAt:    s.now(),
	}
	id, err := s.store.Insert(ctx, t)
	if err == nil {
		t.ID = id
		s.schedule(ctx, t)
		return PayResult{Result: ResultOK, ChannelTxnNo: no, Status: dal.StatusProcessing}, nil
	}
	if !dbx.IsDuplicate(err) {
		return s.recoverInsert(ctx, t, err)
	}
	existing, ok, err := s.store.Get(ctx, dal.TxnTypePay, req.ReqNo)
	if err != nil {
		return PayResult{}, err
	}
	if !ok {
		return PayResult{}, errDuplicateWithoutRow
	}
	return replay(existing, req.Amount), nil
}

// recoverInsert 处理 INSERT 的非 1062 错误（提交结果不确定、请求被取消等）。
// 行可能已经提交：不处理的话它会停在 PROCESSING，直到重启扫描才被调度。
// 用不随请求取消的 ctx 按 (PAY, req_no) 再读一次：读到本次写入的行（channel_txn_no 一致）
// 就按新受理处理：仍是 PROCESSING 则调度，按该行回答（通常是 OK / PROCESSING）。
// 读不到、读失败或是别的请求写入的行，返回原错误（HTTP 500），平台按结果未知处理。
func (s *Service) recoverInsert(ctx context.Context, t dal.Txn, insertErr error) (PayResult, error) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rereadTimeout)
	defer cancel()
	row, ok, err := s.store.Get(readCtx, dal.TxnTypePay, t.ReqNo)
	if err != nil || !ok || row.ChannelTxnNo != t.ChannelTxnNo {
		return PayResult{}, insertErr
	}
	if row.Status == dal.StatusProcessing {
		s.schedule(ctx, row)
	}
	return replay(row, t.Amount), nil
}

// replay 按已有行回答重复受理。CLOSED 优先于金额比较。
func replay(t dal.Txn, amount money.Amount) PayResult {
	switch {
	case t.Status == dal.StatusClosed:
		return PayResult{Result: ResultRejected, Reason: ReasonOrderClosed, Status: t.Status}
	case t.Amount != amount:
		return PayResult{Result: ResultRejected, Reason: ReasonParamConflict, Status: t.Status}
	default:
		return PayResult{Result: ResultOK, ChannelTxnNo: t.ChannelTxnNo, Status: t.Status}
	}
}

// now 返回东八区当前时间并截到毫秒。DATETIME(3) 写入时四舍五入，
// 先截断才能保证库里的 finished_at 与按它算出的 bill_date 在同一天。
func (s *Service) now() time.Time {
	return s.clk.Now().Truncate(time.Millisecond).In(shanghai)
}

// validatePay 检查请求参数。错误只说明字段名，不带取值。
func validatePay(r PayRequest) error {
	switch {
	case !validText(r.ReqNo, maxReqNoRunes):
		return fmt.Errorf("%w: req_no", ErrInvalidRequest)
	case r.Amount < money.MinPayAmount:
		return fmt.Errorf("%w: amount", ErrInvalidRequest)
	case r.Subject == "":
		return fmt.Errorf("%w: subject", ErrInvalidRequest)
	case !validNotifyURL(r.NotifyURL):
		return fmt.Errorf("%w: notify_url", ErrInvalidRequest)
	}
	return nil
}

// validText 要求非空、合法 UTF-8、不含 U+FFFD、字符数不超过 max（与 utf8mb4 VARCHAR 长度一致）。
// encoding/json 把孤立代理（如 \udc00）解码成 U+FFFD，这里一并拒绝，避免替换字符落库。
func validText(s string, maxRunes int) bool {
	return s != "" && utf8.ValidString(s) && !strings.ContainsRune(s, utf8.RuneError) &&
		utf8.RuneCountInString(s) <= maxRunes
}

// validNotifyURL 要求 http 或 https 的绝对地址。回调地址是否可达、是否在白名单由 CH-05 处理。
func validNotifyURL(s string) bool {
	if !validText(s, maxNotifyURLRunes) {
		return false
	}
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
