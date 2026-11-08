package biz

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
)

const (
	// transferRecoverBatchSize 是一轮抢占的行数上限。剩下的下一轮再抢。
	transferRecoverBatchSize = 100
	// transferRecoverConcurrency 是事务外并发处理的上限。一个失败不取消其他任务。
	transferRecoverConcurrency = 20
	// transferRecoverTaskTimeout 与退款恢复的单笔超时相同。账户连接 100ms、RPC 500ms、幂等重试 1 次，
	// 串行两次大约 2.4 秒，再加上本库 CAS。5 秒只包住这一轮，不拉长到 tccPhaseAttempts。
	// 截止只结束这一轮，下一轮按 next_retry_at 再来。不随批次 ctx 取消。
	transferRecoverTaskTimeout = 5 * time.Second
	// transferRecoverClaimTimeout 只包住抢占事务。事务里只有锁和更新，与 dbx 的 3 秒预算一致。
	transferRecoverClaimTimeout = 3 * time.Second
	// transferRecoverStuckAfter：retry_count 达到这个值且仍非终态，计入 pay_transfer_stuck。
	transferRecoverStuckAfter = 10
	// transferRecoverMaxBackoff 是退避上限。2^6 = 64 秒，封顶 60 秒。
	transferRecoverMaxBackoff = 60 * time.Second
)

// transferRecoverPanics 是单个恢复任务 panic 被恢复的次数。不记录 panic 值。
var transferRecoverPanics atomic.Uint64

// pay_transfer_stuck 是本进程见过的、retry_count >= 10 且仍非终态的 transfer_no 去重个数。
// 到达终态就去掉。没有 Prometheus，没有 mode 标签。
var (
	transferStuckMu sync.Mutex
	transferStuck   = map[string]struct{}{}
)

func trackTransferStuck(transferNo string, retry int, terminal bool) {
	transferStuckMu.Lock()
	defer transferStuckMu.Unlock()
	if terminal || retry < transferRecoverStuckAfter {
		delete(transferStuck, transferNo)
		return
	}
	transferStuck[transferNo] = struct{}{}
}

func transferStuckGauge() int {
	transferStuckMu.Lock()
	defer transferStuckMu.Unlock()
	return len(transferStuck)
}

func resetTransferStuck() {
	transferStuckMu.Lock()
	transferStuck = map[string]struct{}{}
	transferStuckMu.Unlock()
}

// transferRecoverBackoff 用加 1 之后的 retry_count。n>=6 一律 60 秒，避免 2^n 用浮点。
func transferRecoverBackoff(n int) time.Duration {
	if n >= 6 {
		return transferRecoverMaxBackoff
	}
	if n < 1 {
		n = 1
	}
	return time.Second << n
}

// nextTransferRetry 返回抢占后的 retry_count 和下一次 next_retry_at。
func nextTransferRetry(now time.Time, count int) (int, time.Time) {
	n := count + 1
	return n, now.Add(transferRecoverBackoff(n))
}

// RunTransferRecoverBatch 抢占一批到期的转账单，提交后在事务外最多 20 并发推进。
// 未装配（nil、无时钟、不是 MySQL、LOCAL 与 TCC 都没装）返回 10003，不抢占。
// 单笔失败只记日志，不取消同批的其他任务。ctx 取消后不再启动新任务。
func (p *Payments) RunTransferRecoverBatch(ctx context.Context) error {
	if p == nil || p.clk == nil || p.transfers == nil {
		return errno.ErrInternal
	}
	db, ok := p.paymentDB()
	if !ok {
		return errno.ErrInternal
	}
	localOK := p.localTransfer != nil
	tccOK := p.tccTransfer != nil
	if !localOK && !tccOK {
		return errno.ErrInternal
	}
	if ctx == nil {
		return errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := p.clk.Now().Truncate(time.Millisecond)
	claimCtx, cancel := context.WithTimeout(ctx, transferRecoverClaimTimeout)
	rows, err := (sqlTransferStore{db: db}).claimTransferRecover(claimCtx, now, localOK, tccOK)
	cancel()
	if err != nil {
		return callErr(err)
	}
	p.dispatchTransferRecover(ctx, rows)
	return nil
}

// dispatchTransferRecover 以最多 transferRecoverConcurrency 个并发处理。等槽时 ctx 取消就不再启动后面的行。
func (p *Payments) dispatchTransferRecover(ctx context.Context, rows []TransferOrder) {
	slots := make(chan struct{}, transferRecoverConcurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, row := range rows {
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-slots
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			p.runTransferRecoverTask(ctx, row)
		}()
	}
}

// runTransferRecoverTask 用自己的超时处理一笔，不随批次 ctx 取消。panic 被恢复并计数，不记录 panic 值。
func (p *Payments) runTransferRecoverTask(ctx context.Context, row TransferOrder) {
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), transferRecoverTaskTimeout)
	defer cancel()
	defer func() {
		if recover() != nil {
			transferRecoverPanics.Add(1)
			slog.ErrorContext(taskCtx, "transfer recover task panicked", "transfer_no", row.TransferNo)
		}
	}()
	p.recoverTransfer(taskCtx, row)
}

// recoverTransfer 推进抢占到的一行。进入终态不是处理失败，不打 transfer recover。
func (p *Payments) recoverTransfer(ctx context.Context, row TransferOrder) {
	got, err := p.advanceRecovered(ctx, row)
	terminal := err == nil && (got.Status == transferStatusSuccess || got.Status == transferStatusFailed)
	if !terminal {
		logTransferRecover(ctx, row.TransferNo, err)
	}
	trackTransferStuck(row.TransferNo, row.RetryCount, terminal)
}

// advanceRecovered 按当前状态调用已有的 LOCAL 或 TCC 函数。CAS 影响 0 行只再处理一次。
func (p *Payments) advanceRecovered(ctx context.Context, row TransferOrder) (TransferOrder, error) {
	gen, ok := p.ids.(*idgen.Generator)
	if !ok || gen == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	switch {
	case row.Mode == transferModeLocal && row.Status == transferStatusTrying:
		if p.localTransfer == nil {
			return TransferOrder{}, errno.ErrInternal
		}
		return p.executeLocal(ctx, row)
	case row.Mode == transferModeTCC && (row.Status == transferStatusTrying ||
		row.Status == transferStatusConfirming || row.Status == transferStatusCancelling):
		if p.tccTransfer == nil {
			return TransferOrder{}, errno.ErrInternal
		}
		return p.advanceTCCRecover(ctx, row, gen)
	default:
		return TransferOrder{}, errno.ErrInternal
	}
}

// advanceTCCRecover 不重跑 Try，也不走 TransferExecutor。TRYING 直接改判取消。
func (p *Payments) advanceTCCRecover(ctx context.Context, row TransferOrder, gen *idgen.Generator) (TransferOrder, error) {
	switch row.Status {
	case transferStatusTrying:
		at := p.clk.Now().Truncate(time.Millisecond)
		got, applied, err := p.transfers.decideTransfer(ctx, row, transferStatusCancelling, transferFailTryTimeout, at)
		if err != nil {
			return TransferOrder{}, tccStoreErr(err)
		}
		if !applied {
			return p.continueDecided(ctx, got, gen)
		}
		return p.cancelTCC(ctx, got, gen)
	case transferStatusConfirming:
		return p.confirmTCC(ctx, row, gen)
	case transferStatusCancelling:
		return p.cancelTCC(ctx, row, gen)
	default:
		return row, errno.ErrInternal
	}
}

// logTransferRecover 只记单号和十进制错误码。非终态且没有错误时用已有的 10003，不改请求路径的返回码。
func logTransferRecover(ctx context.Context, transferNo string, err error) {
	code := errno.ErrInternal.Code
	if err != nil {
		if c := errno.From(err).Code; c != 0 {
			code = c
		}
	}
	slog.ErrorContext(ctx, "transfer recover",
		"transfer_no", transferNo,
		"err_code", strconv.Itoa(int(code)),
	)
}
