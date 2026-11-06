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
	// refundRecoverIdle：updated_at 距今不少于这个时长的 INIT 退款才扫描（REF-04 第 1 条）。
	// 同步请求的处理超时是 5 秒，30 秒后仍是 INIT 说明那次请求已经结束。
	refundRecoverIdle = 30 * time.Second
	// refundRecoverGiveUpAfter：创建超过这个时长仍是 INIT 的退款置 FAILED（REF-04 第 2 条）。恰好 1 小时仍重试冻结。
	refundRecoverGiveUpAfter = time.Hour
	// refundRecoverBatchSize 是每页扫描的行数上限。
	refundRecoverBatchSize = 100
	// refundRecoverRoundBudget：一轮已用满这个时长就不再翻下一页，剩下的行留到下一轮，一轮不会长期拖过 10 秒的间隔。
	refundRecoverRoundBudget = 8 * time.Second
	// refundRecoverConcurrency 是事务外并发恢复的上限，与查单补偿相同。
	refundRecoverConcurrency = 10
	// refundRecoverTaskTimeout 限制单个任务的冻结、写库和渠道调用，与 CreateRefund 的服务端处理超时相同：
	// 冻结最长约 1 秒（调用 500ms，超时重试 1 次），渠道退款 3 秒，其余是写库。
	refundRecoverTaskTimeout = 5 * time.Second
	// refundRecoverScanTimeout 限制每页扫描的读库。
	refundRecoverScanTimeout = 3 * time.Second
)

// refundRecoverPanics 是单个恢复任务 panic 被恢复的次数。进程内计数，由 OPS-03 暴露。
var refundRecoverPanics atomic.Uint64

// RunRefundRecoverBatch 执行一轮退款卡单恢复（REF-04）：按 (updated_at, id) 翻页读出 updated_at 早于 now − 30s 的
// INIT 退款，每页 100 行，事务外以最多 10 个并发逐笔处理，一页处理完再取下一页；不足一页或本轮已用满 8 秒就结束。
// 翻页让长期卡住的行（例如每轮都返回 10003 的额度不一致）每轮只占一次位置，不挡住后面的行。
// 创建不超过 1 小时的从 REF-02 第 1 步（冻结）重新执行；超过 1 小时的按 REF-03 第 2 条置 FAILED（REFUND_TIMEOUT）。
//
// 不抢占、不加锁：多个实例可能同时处理同一笔，由冻结幂等、INIT → PROCESSING 的 CAS 和
// channel_txn.uk_biz_no 保证只有一个提交渠道；置 FAILED 只 CAS INIT，与推进为 PROCESSING 互斥。
// 未装配冻结或渠道退款、没有时钟或不是 MySQL 存储时返回 10003；扫描失败按 callErr 返回。单笔失败只记日志。
// ctx 取消后不再启动新任务，已开始的任务按自己的超时跑完。
func (p *Payments) RunRefundRecoverBatch(ctx context.Context) error {
	if p == nil || p.clk == nil || p.orders == nil || p.refundFreezer == nil || p.refundChannel == nil || p.refundApplier == nil {
		return errno.ErrInternal
	}
	db, ok := p.paymentDB()
	if !ok {
		return errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	start := p.clk.Now()
	// 截止时间整轮固定：本轮里新卡住的行留到下一轮。处理过的行要么已不是 INIT，要么在游标之前，同一轮不会处理两次。
	cutoff := start.Truncate(time.Millisecond).Add(-refundRecoverIdle)
	store := sqlOrderStore{db: db}
	var after *refundCursor
	for {
		scanCtx, cancel := context.WithTimeout(ctx, refundRecoverScanTimeout)
		rows, last, err := store.stuckRefunds(scanCtx, cutoff, after, refundRecoverBatchSize)
		cancel()
		if err != nil {
			return callErr(err)
		}
		p.dispatchRefundRecover(ctx, rows)
		if len(rows) < refundRecoverBatchSize || ctx.Err() != nil || p.clk.Now().Sub(start) >= refundRecoverRoundBudget {
			return nil
		}
		after = &last
	}
}

// dispatchRefundRecover 以最多 refundRecoverConcurrency 个并发处理，已启动的全部结束才返回。
// 等并发槽时 ctx 被取消就不再启动后面的行，这些行仍是 INIT，下一轮再扫到。
func (p *Payments) dispatchRefundRecover(ctx context.Context, rows []RefundOrder) {
	slots := make(chan struct{}, refundRecoverConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, row := range rows {
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
			p.runRefundRecoverTask(ctx, row)
		}()
	}
}

// runRefundRecoverTask 用自己的超时处理一笔，不随批次 ctx 取消：冻结发出后被打断只会让结果变成未知。
// panic 被恢复并计数，不记录 panic 值。
func (p *Payments) runRefundRecoverTask(ctx context.Context, row RefundOrder) {
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), refundRecoverTaskTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			refundRecoverPanics.Add(1)
			slog.ErrorContext(taskCtx, "refund recover task panicked", "refund_no", row.RefundNo)
		}
	}()
	p.recoverRefund(taskCtx, row)
}

// recoverRefund 按 CF-12 处理一笔 INIT 退款。年龄按 created_at 算：INIT 是初始状态，创建以来一直未能推进。
func (p *Payments) recoverRefund(ctx context.Context, row RefundOrder) {
	if p.clk.Now().Sub(row.CreatedAt) > refundRecoverGiveUpAfter {
		p.timeoutRefund(ctx, row)
		return
	}
	// 与 CreateRefund 同一段代码：冻结（幂等）→ 故障点 → CAS INIT → PROCESSING 并插入流水 → 事务外提交渠道。
	// 冻结仍超时或不可用时返回 INIT 且没有错误，下一轮再试。
	_, err := p.submitRefund(ctx, row)
	if err == nil || errors.Is(err, errno.ErrInsufficientBalance) || errors.Is(err, errno.ErrChannelRejected) {
		// 40002 与渠道拒绝已由 ApplyRefundResult 写成 FAILED，不是这一轮的错误。
		return
	}
	logRecoverErr(ctx, row.RefundNo, "refund recover submit", err)
}

// timeoutRefund 走 REF-03 第 2 条：只 CAS INIT → FAILED，释放 refunded_amount，写 refund.failed，同一事务。
// 此时渠道一定没收到请求：提交渠道之前退款单已经是 PROCESSING。
// 与另一路推进为 PROCESSING 并发时 CAS 未命中，返回 10003，退款按另一路的结果继续；
// 另一路已到终态时 ApplyRefundResult 返回该单且 error 为 nil。
func (p *Payments) timeoutRefund(ctx context.Context, row RefundOrder) {
	got, err := p.refundApplier.ApplyRefundResult(ctx, RefundResult{
		RefundNo: row.RefundNo,
		Result:   payResultFailed,
		FailCode: failCodeRefundTimeout,
		Source:   paySourceJob,
	})
	if err != nil {
		p.logTimeoutErr(ctx, row.RefundNo, err)
		return
	}
	if got.Status == refundStatusFailed && got.FailCode == failCodeRefundTimeout {
		slog.WarnContext(ctx, "refund recover timed out",
			"refund_no", row.RefundNo,
			"status", got.Status,
			"fail_code", got.FailCode,
		)
		return
	}
	// 另一路先到了终态（已 SUCCESS，或因其他原因 FAILED），这一笔没有超时。
	slog.InfoContext(ctx, "refund recover timeout superseded",
		"refund_no", row.RefundNo,
		"status", got.Status,
		"fail_code", got.FailCode,
	)
}

// logTimeoutErr 记录置超时失败的错误。10003 时重读一次：已不是 INIT 说明另一路已推进为 PROCESSING 或到了终态，
// 是正常竞争（ApplyRefundResult 已记 WARN），这里只记 INFO。仍是 INIT（REF-03 第 5 条额度不一致或写库失败）
// 或重读失败才按 logRecoverErr 记 ERROR。
func (p *Payments) logTimeoutErr(ctx context.Context, refundNo string, err error) {
	const msg = "refund recover timeout"
	if errno.From(err).Code == errno.ErrInternal.Code {
		cur, ok, readErr := p.orders.refundByNo(ctx, refundNo)
		if readErr == nil && ok && cur.Status != refundStatusInit {
			slog.InfoContext(ctx, msg+" lost race",
				"refund_no", refundNo,
				"status", cur.Status,
				"err_code", strconv.Itoa(int(errno.ErrInternal.Code)),
			)
			return
		}
	}
	logRecoverErr(ctx, refundNo, msg, err)
}

// logRecoverErr 只记错误码，不记错误文本。10003 记 ERROR，其余 WARN。
func logRecoverErr(ctx context.Context, refundNo, msg string, err error) {
	level := slog.LevelWarn
	code := errno.From(err).Code
	if code == errno.ErrInternal.Code {
		level = slog.LevelError
	}
	slog.Log(ctx, level, msg,
		"refund_no", refundNo,
		"err_code", strconv.Itoa(int(code)),
	)
}
