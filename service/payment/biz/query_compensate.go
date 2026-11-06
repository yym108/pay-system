package biz

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
)

const (
	// queryBatchSize 是每次抢占的行数上限（PAY-05 第 1 条）。
	queryBatchSize = 100
	// queryConcurrency 是事务外并发查单的上限（PAY-05 第 2 条）。
	queryConcurrency = 10
	// queryTaskTimeout 限制单个任务的全部渠道调用与写库：查单 3 秒，NOT_FOUND 后关单 1.5 秒、再查单 1.5 秒，或重提退款 3 秒。
	queryTaskTimeout = 10 * time.Second
	// queryNotFoundGrace 内的 NOT_FOUND 不处理：受理请求可能还在路上（PAY-05 第 3 条）。
	queryNotFoundGrace = 60 * time.Second
	// queryGiveUpAfter 是从创建起的查单期限。超过后不再查单，交给对账（PAY-05 第 4 条）。
	queryGiveUpAfter = 24 * time.Hour
	// queryDefaultTail 是退避计划用完后的间隔（PAY-05 第 4 条“之后每小时一次”）。
	queryDefaultTail = time.Hour
)

// queryExhausted 是 pay_channel_query_exhausted_total（PAY-05 第 4 条）。进程内计数，由 OPS-03 暴露。
var queryExhausted atomic.Uint64

// queryTaskPanics 是单个查单任务 panic 被恢复的次数。进程内计数，由 OPS-03 暴露。
var queryTaskPanics atomic.Uint64

// queryClaimSkipped 是任务启动时抢占已到期、因此跳过的次数。进程内计数，由 OPS-03 暴露。
var queryClaimSkipped atomic.Uint64

// channelQuerier 按 (txn_type, req_no) 查一次渠道。生产实现是 *ChannelClient.QueryTxn。
// 调用失败的 Kind 是 queryCallFailed，error 为 nil；只有本地配置错误返回 error。
type channelQuerier interface {
	QueryTxn(ctx context.Context, txnType, reqNo string) (channelQueryResult, error)
}

// QueryPolicy 是查单退避计划。
// 第 n 次抢占（query_count 变成 n）后，下次查单在 Backoff[n] 之后；第 0 档对应插入流水时的 10 秒。
// 下标超出或该档不是正数时用 Tail，Tail 小于等于 0 时每小时一次。
type QueryPolicy struct {
	Backoff []time.Duration
	Tail    time.Duration
}

// queryTask 是一条已抢占的 PROCESSING 流水。QueryCount 是抢占之后的新值，
// NextQueryAt 是抢占时写入的 next_query_at：到了这个时间，这一行可以被别的实例再次抢占。
// 本实例的下一批不会：定时循环一次只跑一个批次，RunQueryBatch 等本批任务全部结束（wg.Wait）才返回。
type queryTask struct {
	ID          int64
	BizNo       string
	TxnType     string
	QueryCount  int
	CreatedAt   time.Time
	NextQueryAt time.Time
}

// UseQuery 装上查单补偿用的渠道查单和退避计划。q 为 nil 时 RunQueryBatch 返回 10003。
func (p *Payments) UseQuery(q channelQuerier, policy QueryPolicy) {
	if p == nil {
		return
	}
	p.querier = q
	p.queryPolicy = QueryPolicy{Backoff: append([]time.Duration(nil), policy.Backoff...), Tail: policy.Tail}
}

// queryDelay 返回第 n 次抢占之后到下次查单的间隔。
func queryDelay(policy QueryPolicy, n int) time.Duration {
	if n >= 0 && n < len(policy.Backoff) && policy.Backoff[n] > 0 {
		return policy.Backoff[n]
	}
	if policy.Tail > 0 {
		return policy.Tail
	}
	return queryDefaultTail
}

// RunQueryBatch 执行一次查单补偿（PAY-05）：短事务抢占到期的 PROCESSING 流水并推后 next_query_at，
// 提交后在事务外以最多 10 个并发查单，按结果推进。创建超过 24 小时的行置 next_query_at = NULL，不再查单。
// 未装配查单、没有时钟或不是 MySQL 存储时返回 10003；抢占失败按 callErr 返回。单个任务的失败只记日志。
// ctx 取消后不再启动新任务，已开始的任务按自己的超时跑完。
func (p *Payments) RunQueryBatch(ctx context.Context) error {
	if p == nil || p.clk == nil || p.querier == nil {
		return errno.ErrInternal
	}
	db, ok := p.paymentDB()
	if !ok {
		return errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := p.clk.Now().Truncate(time.Millisecond)
	tasks, stale, err := sqlOrderStore{db: db}.claimQueryTasks(ctx, now, p.queryPolicy)
	if err != nil {
		return callErr(err)
	}
	for _, task := range stale {
		queryExhausted.Add(1)
		slog.WarnContext(ctx, "channel query exhausted",
			"biz_type", task.TxnType,
			"biz_no", task.BizNo,
		)
	}
	p.dispatchQueries(ctx, tasks)
	return nil
}

// dispatchQueries 以最多 queryConcurrency 个并发执行任务，已启动的全部结束才返回。
// 等并发槽时 ctx 被取消就不再启动后面的任务，这些行已推后，等下一档。
func (p *Payments) dispatchQueries(ctx context.Context, tasks []queryTask) {
	slots := make(chan struct{}, queryConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}
		// 两个 case 同时就绪时 select 随机选，再看一次，取消后不启动新任务。
		if ctx.Err() != nil {
			<-slots
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			p.runQueryTask(ctx, task)
		}()
	}
}

// runQueryTask 用自己的超时执行一个任务，不随批次 ctx 取消。panic 被恢复并计数，不记录 panic 值。
// 启动时抢占写入的 next_query_at 已到期则跳过：批次慢于退避间隔时，这一行可能已被别的实例
// 抢占进入下一档，再查就是同一档查两次。跳过不写库，这一档由接手的实例处理。
func (p *Payments) runQueryTask(ctx context.Context, task queryTask) {
	if !p.clk.Now().Before(task.NextQueryAt) {
		queryClaimSkipped.Add(1)
		slog.WarnContext(ctx, "channel query claim expired",
			"biz_type", task.TxnType,
			"biz_no", task.BizNo,
		)
		return
	}
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTaskTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			queryTaskPanics.Add(1)
			slog.ErrorContext(taskCtx, "channel query task panicked",
				"biz_type", task.TxnType,
				"biz_no", task.BizNo,
			)
		}
	}()
	p.compensate(taskCtx, task)
}

// compensate 查一次渠道并按 PAY-05 第 3 条分发。调用失败、PROCESSING、60 秒内的 NOT_FOUND 都等下一档。
func (p *Payments) compensate(ctx context.Context, task queryTask) {
	if task.TxnType != txnTypePay && task.TxnType != txnTypeRefund {
		slog.ErrorContext(ctx, "channel query unknown txn type",
			"biz_type", task.TxnType,
			"biz_no", task.BizNo,
		)
		return
	}
	res, err := p.querier.QueryTxn(ctx, task.TxnType, task.BizNo)
	if err != nil {
		p.logQueryErr(ctx, task, "channel query failed", err)
		return
	}
	switch res.Kind {
	case queryNotFound:
		if p.clk.Now().Sub(task.CreatedAt) <= queryNotFoundGrace {
			return
		}
		if task.TxnType == txnTypePay {
			p.closeNotFound(ctx, task)
		} else {
			p.resubmitRefund(ctx, task)
		}
	case queryFound:
		if task.TxnType == txnTypePay {
			p.applyPayQuery(ctx, task, res)
		} else {
			p.applyRefundQuery(ctx, task, res)
		}
	default:
	}
}

// applyPayQuery 把查得的 PAY 终态交给 ApplyPayResult（source = QUERY）。
// SUCCESS 缺金额或完成时间时等下一档；CLOSED 是渠道防悬挂行，不带金额。
func (p *Payments) applyPayQuery(ctx context.Context, task queryTask, res channelQueryResult) {
	in := PayResult{BizNo: task.BizNo, Source: paySourceQuery, ChannelTxnNo: res.ChannelTxnNo}
	switch res.Status {
	case payStatusSuccess:
		if !res.OK {
			return
		}
		amount := res.Amount
		in.Result = payResultSuccess
		in.Amount = &amount
		in.FinishedAt = res.FinishedAt
	case payStatusFailed:
		in.Result = payResultFailed
		in.FailCode = failCodeChannelFailed
		in.FinishedAt = res.FinishedAt
		if res.HasAmount {
			amount := res.Amount
			in.Amount = &amount
		}
	case payStatusClosed:
		in.Result = payResultClosed
	default:
		return
	}
	if p.applier == nil {
		p.logQueryErr(ctx, task, "channel query apply", errno.ErrInternal)
		return
	}
	if _, err := p.applier.ApplyPayResult(ctx, in); err != nil {
		p.logQueryErr(ctx, task, "channel query apply", err)
	}
}

// applyRefundQuery 把查得的 REFUND 终态交给 ApplyRefundResult（source = QUERY）。
// fail_code 由 ApplyRefundResult 补为 CHANNEL_FAILED。退款不会被渠道关闭，查得 CLOSED 只记日志并等下一档。
func (p *Payments) applyRefundQuery(ctx context.Context, task queryTask, res channelQueryResult) {
	in := RefundResult{RefundNo: task.BizNo, Source: paySourceQuery, ChannelTxnNo: res.ChannelTxnNo}
	switch res.Status {
	case payStatusSuccess:
		if !res.OK {
			return
		}
		amount := res.Amount
		in.Result = payResultSuccess
		in.Amount = &amount
		in.FinishedAt = res.FinishedAt
	case payStatusFailed:
		in.Result = payResultFailed
		in.FinishedAt = res.FinishedAt
		if res.HasAmount {
			amount := res.Amount
			in.Amount = &amount
		}
	case payStatusClosed:
		slog.WarnContext(ctx, "channel query refund closed",
			"biz_type", task.TxnType,
			"biz_no", task.BizNo,
		)
		return
	default:
		return
	}
	if p.refundApplier == nil {
		p.logQueryErr(ctx, task, "channel query apply", errno.ErrInternal)
		return
	}
	if _, err := p.refundApplier.ApplyRefundResult(ctx, in); err != nil {
		p.logQueryErr(ctx, task, "channel query apply", err)
	}
}

// closeNotFound 在渠道查无此单超过 60 秒时走 PAY-06 的关单路径（source = QUERY）。
// ALREADY_PAID 与渠道 FAILED 已在 closeOnChannel 里写入终态，返回 30003 属于预期；调用失败 10004 等下一档。
func (p *Payments) closeNotFound(ctx context.Context, task queryTask) {
	_, err := p.closeOnChannel(ctx, task.BizNo, paySourceQuery)
	if err == nil || errors.Is(err, errno.ErrOrderStatusConflict) || errors.Is(err, errno.ErrUnavailable) {
		return
	}
	p.logQueryErr(ctx, task, "channel query close", err)
}

// resubmitRefund 在渠道没收到退款请求超过 60 秒时，按 REF-02 的同一段代码重新提交（渠道按 req_no 幂等）。
// 退款单已不是 PROCESSING 时不提交。OK 回填流水号；REJECTED 写 FAILED（CHANNEL_REJECTED）；结果未知等下一档。
func (p *Payments) resubmitRefund(ctx context.Context, task queryTask) {
	row, ok, err := p.orders.refundByNo(ctx, task.BizNo)
	if err != nil {
		p.logQueryErr(ctx, task, "channel query refund read", callErr(err))
		return
	}
	if !ok {
		p.logQueryErr(ctx, task, "channel query refund missing", errno.ErrInternal)
		return
	}
	if row.Status != refundStatusProcessing {
		slog.WarnContext(ctx, "channel query refund not processing",
			"biz_type", task.TxnType,
			"biz_no", task.BizNo,
		)
		return
	}
	if _, err = p.callRefundChannel(ctx, row); err != nil && !errors.Is(err, errno.ErrChannelRejected) {
		p.logQueryErr(ctx, task, "channel query refund resubmit", err)
	}
}

// logQueryErr 只记错误码，不记错误文本，避免带出地址或密钥。本地配置错误记 ERROR，其余 WARN。
func (p *Payments) logQueryErr(ctx context.Context, task queryTask, msg string, err error) {
	level := slog.LevelWarn
	code := errno.From(err).Code
	if isLocalChannelErr(err) {
		level = slog.LevelError
		code = errno.ErrInternal.Code
	} else if code == errno.ErrInternal.Code {
		level = slog.LevelError
	}
	slog.Log(ctx, level, msg,
		"biz_type", task.TxnType,
		"biz_no", task.BizNo,
		"err_code", strconv.Itoa(int(code)),
	)
}
