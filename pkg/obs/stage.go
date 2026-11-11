package obs

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// 阶段名与需求 OPT-01 规则 2 一致，调用方不要改写。
const (
	ChainPayCreate     = "pay_create"
	ChainPaySubmit     = "pay_submit"
	ChainPaySettle     = "pay_settle"
	ChainRefund        = "refund"
	ChainTransferLocal = "transfer_local"
	ChainTransferTCC   = "transfer_tcc"

	StageGWAuth           = "gw_auth"
	StageGWNonce          = "gw_nonce"
	StageRPC              = "rpc"
	StageDBInsert         = "db_insert"
	StageDBTx1            = "db_tx1"
	StageDBTx2            = "db_tx2"
	StageChannelCall      = "channel_call"
	StageCallbackTx       = "callback_tx"
	StageOutboxWait       = "outbox_wait"
	StageMQWait           = "mq_wait"
	StageLedgerTx         = "ledger_tx"
	StageDBQuotaTx        = "db_quota_tx"
	StageRPCFreeze        = "rpc_freeze"
	StageDBTx             = "db_tx"
	StageRPCTransferLocal = "rpc_transfer_local"
	StageDBFinishTx       = "db_finish_tx"
	StageTry              = "try"
	StageDBDecision       = "db_decision"
	StageConfirm          = "confirm"
	StageCancel           = "cancel"
)

// sinceFunc 生产路径就是 time.Since。测试把它换成负值，用来验收负耗时记 0。
var sinceFunc = time.Since

type chainKey struct{}

type chainHint struct {
	mu sync.Mutex
	v  string
}

type slotKey struct{}

// stageSlot 保存当前请求还没结束的那一个阶段。同一时刻只放一个，避免嵌套把总和打超。
type stageSlot struct {
	mu    sync.Mutex
	gen   uint64
	end   func()
	stage string
}

// stageOnBackground 给没有父 ctx 的调用记阶段。nil 没有父 ctx，
// 不能在带 ctx 参数的函数里换成 Background 再传给会检查父 ctx 的调用。
func stageOnBackground(chain, stage string) func() {
	return Stage(context.Background(), chain, stage)
}

// Stage 返回结束函数。调用时给已注册的 pay_stage_duration_seconds 记一次耗时，
// 并在当前 span 上写名为 stage 的事件。没有 span 时直方图仍然记录。
// 空的 chain 或 stage 记为 unknown。chain 参数为空时，结束那一刻再读 WithChain 写进上下文的链名。
// 负耗时记 0。重复调用只记一次。
func Stage(ctx context.Context, chain, stage string) func() {
	if ctx == nil {
		return stageOnBackground(chain, stage) //nolint:contextcheck // nil 没有父 ctx。
	}
	start := time.Now()
	stageName := label(stage)
	var once sync.Once
	return func() {
		once.Do(func() {
			elapsed := sinceFunc(start)
			if elapsed < 0 {
				elapsed = 0
			}
			chainName := chainLabel(ctx, chain)
			stageDuration.WithLabelValues(chainName, stageName).Observe(seconds(elapsed))
			trace.SpanFromContext(ctx).AddEvent(stageName, trace.WithAttributes(
				attribute.String("chain", chainName),
				attribute.String("stage", stageName),
				attribute.Int64("duration_ns", elapsed.Nanoseconds()),
			))
		})
	}
}

func chainLabel(ctx context.Context, chain string) string {
	if chain != "" {
		return label(chain)
	}
	return label(ChainOf(ctx))
}

// WithChain 把链名放进 ctx。盒子已经存在且 chain 非空时就地改写，
// 这样更早开始、参数留空的 Stage 在结束时能看到纠正后的链名。
// chain 为空也会建盒子，转账验签时还不知道同分片还是跨分片。
func WithChain(ctx context.Context, chain string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if box, ok := ctx.Value(chainKey{}).(*chainHint); ok && box != nil {
		if chain != "" {
			box.mu.Lock()
			box.v = chain
			box.mu.Unlock()
		}
		return ctx
	}
	return context.WithValue(ctx, chainKey{}, &chainHint{v: chain})
}

// ChainOf 返回 WithChain 写入的链名。没有盒子时为空串。
func ChainOf(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	box, _ := ctx.Value(chainKey{}).(*chainHint)
	if box == nil {
		return ""
	}
	box.mu.Lock()
	defer box.mu.Unlock()
	return box.v
}

// WithSlot 给这次请求一个阶段槽。已有槽时原样返回。
func WithSlot(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if slot, ok := ctx.Value(slotKey{}).(*stageSlot); ok && slot != nil {
		return ctx
	}
	return context.WithValue(ctx, slotKey{}, &stageSlot{})
}

// HasSlot 报告 ctx 里有没有请求阶段槽。
func HasSlot(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	slot, ok := ctx.Value(slotKey{}).(*stageSlot)
	return ok && slot != nil
}

// Begin 先结束槽里的上一阶段，再开始这一阶段。没有槽时，调用方必须自己调用返回的结束函数。
// 有槽时，返回的函数只在这一阶段仍是当前阶段时才结束它，所以可以放进 defer，重复结束只记一次。
func Begin(ctx context.Context, chain, stage string) func() {
	if ctx == nil {
		return stageOnBackground(chain, stage) //nolint:contextcheck // nil 没有父 ctx。
	}
	slot, _ := ctx.Value(slotKey{}).(*stageSlot)
	if slot == nil {
		return Stage(ctx, chain, stage)
	}
	slot.mu.Lock()
	prev := slot.end
	slot.end = nil
	slot.stage = ""
	slot.mu.Unlock()
	if prev != nil {
		prev()
	}
	end := Stage(ctx, chain, stage)
	slot.mu.Lock()
	slot.gen++
	gen := slot.gen
	slot.end = end
	slot.stage = stage
	slot.mu.Unlock()
	return func() {
		slot.mu.Lock()
		if slot.gen != gen {
			slot.mu.Unlock()
			return
		}
		slot.end = nil
		slot.stage = ""
		slot.mu.Unlock()
		end()
	}
}

// CurrentStage 返回槽里还开着的阶段名。没有槽或已经结束时为空串。
func CurrentStage(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	slot, _ := ctx.Value(slotKey{}).(*stageSlot)
	if slot == nil {
		return ""
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.stage
}

// CloseSlot 结束槽里还开着的阶段。应在 span.End 之前调用，事件才能进这条 span。
func CloseSlot(ctx context.Context) {
	if ctx == nil {
		return
	}
	slot, _ := ctx.Value(slotKey{}).(*stageSlot)
	if slot == nil {
		return
	}
	slot.mu.Lock()
	end := slot.end
	slot.end = nil
	slot.stage = ""
	slot.gen++
	slot.mu.Unlock()
	if end != nil {
		end()
	}
}

// KeepOrEnd 在没有请求槽时立刻记录。有槽时留给 CloseSlot 或下一次 Begin，避免提前结束把响应尾部留成空隙。
func KeepOrEnd(ctx context.Context, end func()) {
	if end == nil || HasSlot(ctx) {
		return
	}
	end()
}

var stageBinds sync.Map

// OutboxWaitKey 是支付成功事件从提交到本次投递回写的等待键。开始和结束都在 payment 进程。
func OutboxWaitKey(eventID string) string {
	return "ob:" + eventID
}

// MQWaitKey 是同一进程里按 event_id 结束等待的键。
// 跨进程的 mq_wait 不走这里，由 Kafka 头 produced_at_ns 在消费进程计算。
func MQWaitKey(eventID string) string {
	return "mq:" + eventID
}

func bindKeyOK(key string) bool {
	return key != "" && !strings.HasSuffix(key, ":")
}

// Bind 记住一个还没结束的等待。同一个键再次绑定会先结束上一次，避免泄漏。
// 结束函数只碰内存里的直方图和 span 事件。
func Bind(key string, end func()) {
	if !bindKeyOK(key) || end == nil {
		return
	}
	if prev, loaded := stageBinds.Swap(key, end); loaded {
		if f, ok := prev.(func()); ok && f != nil {
			f()
		}
	}
}

// Release 结束并去掉这个键上的等待。键不存在时什么都不做。
func Release(key string) {
	if !bindKeyOK(key) {
		return
	}
	v, ok := stageBinds.LoadAndDelete(key)
	if !ok {
		return
	}
	f, _ := v.(func())
	if f != nil {
		f()
	}
}

// AuthWatch 记录网关上、payment 处理方法以外的耗时。Start 开始，Pause 停表，Resume 继续。
// Add 只在已经停表、还没记样本时累加正耗时。Observe 先停表再记一次 gw_auth。
// 转账用 ObserveTransfer 按 Mode 选链。
type AuthWatch struct {
	start    time.Time
	elapsed  time.Duration
	running  bool
	observed bool
}

// StartAuth 从进入商户写路由开始计时。
func StartAuth() *AuthWatch {
	return &AuthWatch{start: time.Now(), running: true}
}

// Pause 把已经过去的时间累加后停表。重复调用不把后面的等待算进去。
func (w *AuthWatch) Pause() {
	if w == nil || !w.running || w.observed {
		return
	}
	w.elapsed += nonNeg(sinceFunc(w.start))
	w.running = false
}

// Resume 从现在继续计时。正在计时或已经记过样本时不做事。
func (w *AuthWatch) Resume() {
	if w == nil || w.running || w.observed {
		return
	}
	w.start = time.Now()
	w.running = true
}

// Stop 停表。保留给已经冻结、不再继续的调用。
func (w *AuthWatch) Stop() {
	w.Pause()
}

// Add 把一段正耗时加进累计。只在已经停表且还没 Observe 时生效。
// 正在计时、已经记过、nil、负数或零都忽略，避免把 RPC 里的业务阶段再记进 gw_auth。
func (w *AuthWatch) Add(d time.Duration) {
	if w == nil || w.running || w.observed || d <= 0 {
		return
	}
	w.elapsed += d
}

// Observe 先停表，再把累计耗时记成 gw_auth。chain 为空或已经记过，都不记样本。
func (w *AuthWatch) Observe(ctx context.Context, chain string) {
	if w == nil || w.observed || chain == "" {
		return
	}
	w.Pause()
	w.observed = true
	if ctx == nil {
		observeAuthBackground(chain, w.elapsed) //nolint:contextcheck // nil 没有父 ctx。
		return
	}
	observeElapsed(ctx, chain, StageGWAuth, w.elapsed)
}

// ObserveTransfer 按 CreateTransfer 响应里的 Mode 选择链。
// LOCAL 记 transfer_local，TCC 记 transfer_tcc。其他值（含空）不记成别的链。
func (w *AuthWatch) ObserveTransfer(ctx context.Context, mode string) {
	switch mode {
	case "LOCAL":
		w.Observe(ctx, ChainTransferLocal)
	case "TCC":
		w.Observe(ctx, ChainTransferTCC)
	}
}

func observeAuthBackground(chain string, elapsed time.Duration) {
	observeElapsed(context.Background(), chain, StageGWAuth, elapsed)
}

type heldWait struct {
	start time.Time
	chain string
	stage string
}

var heldWaits sync.Map

// HoldWait 记住一段等待的起点，不捕获 ctx。同一个键再次记住会先记下上一次的直方图，避免泄漏。
// 跨进程不能拿这个闭包去结束：消费端在另一个进程里对不上。
func HoldWait(key, chain, stage string) {
	if !bindKeyOK(key) {
		return
	}
	h := &heldWait{start: time.Now(), chain: chain, stage: stage}
	if prev, loaded := heldWaits.Swap(key, h); loaded {
		if old, ok := prev.(*heldWait); ok && old != nil {
			recordStageHistogram(old.chain, old.stage, nonNeg(sinceFunc(old.start)))
		}
	}
}

// FinishWait 用调用方的 ctx 结束 HoldWait。span 事件写在这份 ctx 上，不用更早已经结束的 span。
// 键不存在或再次调用都不记样本。
func FinishWait(ctx context.Context, key string) {
	if !bindKeyOK(key) {
		return
	}
	v, ok := heldWaits.LoadAndDelete(key)
	if !ok {
		return
	}
	h, _ := v.(*heldWait)
	if h == nil {
		return
	}
	elapsed := nonNeg(sinceFunc(h.start))
	if ctx == nil {
		finishWaitBackground(h.chain, h.stage, elapsed) //nolint:contextcheck // nil 没有父 ctx。
		return
	}
	observeElapsed(ctx, h.chain, h.stage, elapsed)
}

func finishWaitBackground(chain, stage string, elapsed time.Duration) {
	observeElapsed(context.Background(), chain, stage, elapsed)
}

// ObserveMQWait 用生产时刻的十进制纳秒和现在的差，给 pay_settle/mq_wait 记一次。
// 头缺失或无法解析不记样本。负耗时记 0。调用方要在还活着的消费 span 上调用。
func ObserveMQWait(ctx context.Context, producedAt string) {
	ns, err := strconv.ParseInt(producedAt, 10, 64)
	if producedAt == "" || err != nil {
		return
	}
	elapsed := nonNeg(sinceFunc(time.Unix(0, ns)))
	if ctx == nil {
		observeMQWaitBackground(elapsed) //nolint:contextcheck // nil 没有父 ctx。
		return
	}
	observeElapsed(ctx, ChainPaySettle, StageMQWait, elapsed)
}

func observeMQWaitBackground(elapsed time.Duration) {
	observeElapsed(context.Background(), ChainPaySettle, StageMQWait, elapsed)
}

func nonNeg(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

func recordStageHistogram(chain, stage string, elapsed time.Duration) {
	stageDuration.WithLabelValues(label(chain), label(stage)).Observe(seconds(nonNeg(elapsed)))
}

func observeElapsed(ctx context.Context, chain, stage string, elapsed time.Duration) {
	elapsed = nonNeg(elapsed)
	chainName := label(chain)
	stageName := label(stage)
	recordStageHistogram(chainName, stageName, elapsed)
	trace.SpanFromContext(ctx).AddEvent(stageName, trace.WithAttributes(
		attribute.String("chain", chainName),
		attribute.String("stage", stageName),
		attribute.Int64("duration_ns", elapsed.Nanoseconds()),
	))
}
