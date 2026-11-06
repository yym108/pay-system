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
	// timeoutCloseInitLimit、timeoutClosePayingLimit 是每批 INIT、PAYING 各自的扫描配额，
	// 合计 200 条（PAY-07 规则 1）。分开扫描，过期 INIT 积压时 PAYING 仍能被扫到。
	timeoutCloseInitLimit   = 100
	timeoutClosePayingLimit = 100
	// timeoutCloseConcurrency 是事务外并发关闭 PAYING 的上限，与查单补偿相同（PAY-05 第 2 条）。
	timeoutCloseConcurrency = 10
	// timeoutCloseScanTimeout 限制扫描过期订单的一次读库（需求 §5.4.1 MySQL 读写 3 秒）。
	timeoutCloseScanTimeout = 3 * time.Second
	// timeoutCloseTaskTimeout 限制关闭一笔订单的全部渠道调用与写库：
	// 查单 1.5 秒、关单 1.5 秒、ALREADY_PAID 后再查单 1.5 秒，加上 ApplyPayResult 的事务。
	timeoutCloseTaskTimeout = 10 * time.Second
)

// timeoutCloseTaskPanics 是关闭单笔过期订单时 panic 被恢复的次数。进程内计数，由 OPS-03 暴露。
var timeoutCloseTaskPanics atomic.Uint64

// timeoutCloseFailures 是 pay_timeout_close_failures_total{err_code}：单笔关单失败的次数。进程内计数，由 OPS-03 暴露。
var timeoutCloseFailures sync.Map

// RunTimeoutCloseBatch 执行一次超时关单（PAY-07）：先读至多 100 条 expire_at <= now 的 INIT 逐条关闭，
// 再读至多 100 条过期 PAYING，以最多 10 个并发关闭。两类都走商户关单（PAY-06）的同一段代码，来源为 JOB。
// 不加锁：多实例同时执行时由状态 CAS 和渠道关单幂等保证每笔只推进一次。
// 未装配渠道关单、没有时钟或不是 MySQL 存储时返回 10003；扫描失败按 callErr 返回。单笔失败只记日志，下一轮再试。
// ctx 取消后不再开始新的一笔，已开始的按自己的超时跑完后才返回。开始时 ctx 已取消则返回 ctx 的错误（与 PAY-05 一致），
// 批次中途取消返回 nil。
func (p *Payments) RunTimeoutCloseBatch(ctx context.Context) error {
	if p == nil || p.clk == nil || p.closer == nil || p.applier == nil {
		return errno.ErrInternal
	}
	db, ok := p.paymentDB()
	if !ok {
		return errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store := sqlOrderStore{db: db}
	now := p.clk.Now().Truncate(time.Millisecond)
	inits, err := scanExpired(ctx, store, payStatusInit, now, timeoutCloseInitLimit)
	if err != nil {
		return err
	}
	p.closeExpiredOrders(ctx, inits)
	// 中途取消是正常停机，不是批次失败，返回 nil，job 循环不记 "job batch failed"。
	if ctx.Err() != nil {
		return nil
	}
	payings, err := scanExpired(ctx, store, payStatusPaying, now, timeoutClosePayingLimit)
	if err != nil {
		return err
	}
	p.dispatchExpiredPaying(ctx, payings)
	return nil
}

// scanExpired 用自己的 3 秒超时读一类过期订单。
func scanExpired(ctx context.Context, store sqlOrderStore, status string, now time.Time, limit int) ([]PaymentOrder, error) {
	scanCtx, cancel := context.WithTimeout(ctx, timeoutCloseScanTimeout)
	defer cancel()
	rows, err := store.expiredPayments(scanCtx, status, now, limit)
	if err != nil {
		return nil, callErr(err)
	}
	return rows, nil
}

// closeExpiredOrders 逐条关闭 INIT。INIT 只做本地 CAS，每笔毫秒级，串行跑完不占多个连接。
// 批次 ctx 取消后不再开始下一笔，没处理的行下一轮仍会被扫到。
func (p *Payments) closeExpiredOrders(ctx context.Context, rows []PaymentOrder) {
	for _, order := range rows {
		if ctx.Err() != nil {
			return
		}
		p.closeExpiredOrder(ctx, order)
	}
}

// dispatchExpiredPaying 以最多 timeoutCloseConcurrency 个并发关闭 PAYING，已启动的全部结束才返回。
// 写法与 dispatchQueries（PAY-05）相同：批次 ctx 取消后等槽的行不再启动。
func (p *Payments) dispatchExpiredPaying(ctx context.Context, rows []PaymentOrder) {
	slots := make(chan struct{}, timeoutCloseConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, order := range rows {
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
			p.closeExpiredOrder(ctx, order)
		}()
	}
}

// closeExpiredOrder 用自己的超时关闭一笔，不随批次 ctx 取消，避免停机时把渠道关单打断在半途。
// panic 被恢复并计数，不记录 panic 值。
func (p *Payments) closeExpiredOrder(ctx context.Context, order PaymentOrder) {
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeoutCloseTaskTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			timeoutCloseTaskPanics.Add(1)
			slog.ErrorContext(taskCtx, "timeout close task panicked",
				"pay_no", order.PayNo,
				"source", paySourceJob,
			)
		}
	}()
	got, err := p.closeStatus(taskCtx, &CloseInput{MerchantID: order.MerchantID, PayNo: order.PayNo}, order, paySourceJob)
	p.logTimeoutClose(taskCtx, order, got, err)
}

// logTimeoutClose 只记单号、前后状态、来源和错误码，不记错误文本，避免带出地址或密钥。
// 成功日志的 from_status 是扫描时读到的状态，to_status 是关单路径返回时订单的当前状态，
// 不表示本实例写入了这次变化：CAS 未命中后重读到别人写的终态、ApplyPayResult 相同终态幂等返回，
// 都记同一条 "timeout close settled"。谁推进了状态以 outbox 事件和 version 为准。
// 30003 表示关单时渠道已成功或已失败，订单已按 PAY-06 推进为该终态，属于预期结果。
// 失败按错误码计数；10003（内部错误）和 50002（渠道金额与订单不符，需人工处理）记 ERROR，其余记 WARN。
func (p *Payments) logTimeoutClose(ctx context.Context, order, got PaymentOrder, err error) {
	if err == nil || errors.Is(err, errno.ErrOrderStatusConflict) {
		slog.InfoContext(ctx, "timeout close settled",
			"pay_no", order.PayNo,
			"from_status", order.Status,
			"to_status", got.Status,
			"source", paySourceJob,
		)
		return
	}
	code := errno.From(err).Code
	errCode := strconv.Itoa(int(code))
	addTimeoutCloseFailure(errCode)
	level := slog.LevelWarn
	if code == errno.ErrInternal.Code || code == errno.ErrChannelAmountMismatch.Code {
		level = slog.LevelError
	}
	slog.Log(ctx, level, "timeout close failed",
		"pay_no", order.PayNo,
		"from_status", order.Status,
		"source", paySourceJob,
		"err_code", errCode,
	)
}

func addTimeoutCloseFailure(errCode string) {
	actual, _ := timeoutCloseFailures.LoadOrStore(errCode, new(atomic.Uint64))
	counter, ok := actual.(*atomic.Uint64)
	if !ok {
		return
	}
	counter.Add(1)
}

func timeoutCloseFailureCount(errCode string) uint64 {
	actual, ok := timeoutCloseFailures.Load(errCode)
	if !ok {
		return 0
	}
	counter, ok := actual.(*atomic.Uint64)
	if !ok {
		return 0
	}
	return counter.Load()
}
