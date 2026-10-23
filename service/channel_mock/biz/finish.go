package biz

import (
	"context"
	"hash/crc32"
	"log/slog"
	"time"

	"github.com/yym108/pay-system/service/channel_mock/dal"
	"github.com/yym108/pay-system/service/channel_mock/scenario"
)

const (
	// finishTimeout 是一次自动完成写库的上限，与 dbx.WithTx 的默认总时长一致。
	finishTimeout = 3 * time.Second
	// finishRetryDelay 是写库失败后重新调度的间隔。
	finishRetryDelay = time.Second
	// recoverBatch 是重启扫描每页的行数。
	recoverBatch = 500
	// billDateLayout 是 bill_date 的格式。
	billDateLayout = "2006-01-02"
)

// Delay 返回受理到自动完成的延迟（CH-01 规则 4）：
// MIN + crc32(req_no) % (MAX − MIN) 毫秒。MAX ≤ MIN 时取 MIN，负数按 0。
// 同一 req_no 结果固定，所以重启后能按 created_at + Delay 还原原来的 due。
func Delay(reqNo string, minMS, maxMS int) time.Duration {
	lo := int64(max(minMS, 0))
	hi := int64(maxMS)
	if hi <= lo {
		return time.Duration(lo) * time.Millisecond
	}
	h := int64(crc32.ChecksumIEEE([]byte(reqNo)))
	return time.Duration(lo+h%(hi-lo)) * time.Millisecond
}

// Recover 扫描全部 PAY 的 PROCESSING 行并重建调度（CH-01 规则 5），返回调度的笔数。
// due 按 created_at + Delay 计算，已过期的立即执行；NEVER_FINISH 不调度。
// 在开始接收请求之前调用。与已有调度重叠也安全：CAS 保证每笔只完成一次。
func (s *Service) Recover(ctx context.Context) (int, error) {
	var after uint64
	n := 0
	for {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		rows, err := s.store.ListProcessing(ctx, dal.TxnTypePay, after, recoverBatch)
		if err != nil {
			return n, err
		}
		for _, t := range rows {
			if s.schedule(ctx, t) {
				n++
			}
		}
		if len(rows) < recoverBatch {
			return n, nil
		}
		after = rows[len(rows)-1].ID
	}
}

// schedule 按场景调度自动完成，返回是否调度。
// 完成任务比受理请求活得久：只保留 ctx 的值（请求 ID 等），不继承它的取消和截止时间。
func (s *Service) schedule(ctx context.Context, t dal.Txn) bool {
	if !scenario.Scenario(t.Scenario).AutoFinishes() {
		return false
	}
	base := context.WithoutCancel(ctx)
	due := t.CreatedAt.Add(Delay(t.ReqNo, s.opts.DelayMinMS, s.opts.DelayMaxMS))
	s.sched.Schedule(due, func() { s.finish(base, t, nil) })
	return true
}

// finish 用 CAS 把一笔 PROCESSING 推进到场景决定的终态。只有确认已提交后才调用 OnFinished。
//
// 写库报错时结果不确定（可能已提交）：记下这次尝试，finishRetryDelay 后重试。
// 重试的 CAS 得 0 行时按 id 读回：状态与 finished_at 和某次尝试一致，说明那次已生效，
// 用读回的行调用钩子；不一致（被关单或别处改走）或从未有过不确定的尝试，什么也不做。
// 读回也失败时不猜，继续重试。base 不会被取消，原样传给钩子。
func (s *Service) finish(base context.Context, t dal.Txn, tried []dal.Finish) {
	ctx, cancel := context.WithTimeout(base, finishTimeout)
	defer cancel()
	f := s.finishFor(t)
	applied, err := s.store.Finish(ctx, f)
	if err != nil {
		s.retryFinish(ctx, base, t, append(tried, f), err)
		return
	}
	var done dal.Txn
	switch {
	case applied:
		done = t
		done.Status, done.FinishedAt, done.BillDate = f.Status, f.FinishedAt, f.BillDate
	case len(tried) > 0:
		row, ok, err := s.store.GetByID(ctx, t.ID)
		if err != nil {
			s.retryFinish(ctx, base, t, tried, err)
			return
		}
		if !ok || !matchesAttempt(row, tried) {
			s.skipFinish(ctx, t)
			return
		}
		done = row
	default:
		s.skipFinish(ctx, t)
		return
	}
	slog.InfoContext(ctx, "mock txn finished",
		slog.String("channel_txn_no", done.ChannelTxnNo),
		slog.String("from_status", dal.StatusProcessing),
		slog.String("to_status", done.Status),
		slog.String("scenario", done.Scenario))
	if s.opts.OnFinished != nil {
		s.opts.OnFinished(base, done)
	}
}

// finishFor 按场景和当前时间生成本次要写的终态。
func (s *Service) finishFor(t dal.Txn) dal.Finish {
	sc := scenario.Scenario(t.Scenario)
	now := s.now()
	f := dal.Finish{ID: t.ID, Status: dal.StatusSuccess, FinishedAt: now}
	switch {
	case sc.Fails():
		f.Status = dal.StatusFailed
	case sc.InBill():
		f.BillDate = now.Format(billDateLayout)
	}
	return f
}

// retryFinish 记录失败并在 finishRetryDelay 后重试。失败不丢任务，直到成功或进程退出（重启后由 Recover 接上）。
func (s *Service) retryFinish(ctx, base context.Context, t dal.Txn, tried []dal.Finish, err error) {
	slog.WarnContext(ctx, "mock txn finish failed, retry later",
		slog.String("channel_txn_no", t.ChannelTxnNo),
		slog.String("error", err.Error()))
	s.sched.Schedule(s.clk.Now().Add(finishRetryDelay), func() { s.finish(base, t, tried) })
}

func (s *Service) skipFinish(ctx context.Context, t dal.Txn) {
	slog.InfoContext(ctx, "mock txn no longer processing, skip finish",
		slog.String("channel_txn_no", t.ChannelTxnNo))
}

// matchesAttempt 判断读回的行是否就是某次结果不确定的尝试写下的。
// finished_at 截到毫秒写入，同一笔两次尝试的时间不同，状态与 finished_at 都一致才算。
func matchesAttempt(row dal.Txn, tried []dal.Finish) bool {
	for _, f := range tried {
		if row.Status == f.Status && row.FinishedAt.Equal(f.FinishedAt) {
			return true
		}
	}
	return false
}
