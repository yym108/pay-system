// Package biz 实现对账批次与调度核心业务。
package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/service/recon/dal"
)

const (
	batchNoPrefix        = "RB"
	takeoverTimeout      = 30 * time.Minute
	billNotReadyMaxRetry = 6
	billNotReadyDelay    = 30 * time.Minute
	maxReturnedDiffs     = 100
	dailyLookbackDays    = 7
)

var shanghai = time.FixedZone("Asia/Shanghai", 8*3600)

// ErrBillNotReady 表示渠道返回账单未就绪，触发延迟重试计数。
var ErrBillNotReady = errors.New("BILL_NOT_READY")

// DownloadResult 是账单下载与导入的结果（由 REC-02 实现填充）。
// 若 Transitioned 为 true，说明 Downloader 已在同一事务内完成账单导入与 MarkDownloadedCAS，
// 此时 Won 表示 CAS 是否成功，UpdatedAt 为写入的更新时间。
type DownloadResult struct {
	FileHash     string
	Transitioned bool
	Won          bool
	UpdatedAt    time.Time
}

// Downloader 抽象账单下载与导入步骤（REC-02 实现）。
type Downloader interface {
	DownloadAndImport(ctx context.Context, exec sqlx.ExtContext, batch *dal.Batch) (DownloadResult, error)
}

// CheckResult 是外部比对、内部对账与自动处理后的汇总统计（由 REC-03/05/06 实现）。
type CheckResult struct {
	ChannelCount  int32
	ChannelAmount int64
	LocalCount    int32
	LocalAmount   int64
	MatchedCount  int32
}

// Checker 抽象比对与自动处理步骤（REC-03/05/06 实现）。
type Checker interface {
	Check(ctx context.Context, batch *dal.Batch) (CheckResult, error)
}

// RetryScheduler 抽象 BILL_NOT_READY 的单批次延迟重试调度。
type RetryScheduler interface {
	ScheduleRetry(channel, billDate string, delay time.Duration)
}

// IDGen 抽象单号生成器。
type IDGen interface {
	NewNo(ctx context.Context, prefix string) (string, error)
}

// RunBatchInput 是 RunBatch 的输入参数。
type RunBatchInput struct {
	Channel  string
	BillDate string
	Force    bool
}

// RunBatchOutput 是 RunBatch 的输出结果。
type RunBatchOutput struct {
	Batch dal.Batch
	Diffs []dal.Diff
}

type noopDownloader struct{}

func (noopDownloader) DownloadAndImport(_ context.Context, _ sqlx.ExtContext, _ *dal.Batch) (DownloadResult, error) {
	return DownloadResult{}, nil
}

type noopChecker struct{}

func (noopChecker) Check(_ context.Context, _ *dal.Batch) (CheckResult, error) {
	return CheckResult{}, nil
}

// Recon 提供对账批次管理与每日定时触发能力。
type Recon struct {
	db         *sqlx.DB
	ids        IDGen
	clk        clock.Clock
	channels   map[string]struct{}
	channelOrd []string
	downloader Downloader
	checker    Checker
	retrySched RetryScheduler
}

// NewRecon 创建 Recon 业务实例。channels 为空时默认只包含 mockpay。
func NewRecon(db *sqlx.DB, ids IDGen, clk clock.Clock, channels []string, downloader Downloader, checker Checker) *Recon {
	if clk == nil {
		clk = clock.Real{}
	}
	if len(channels) == 0 {
		channels = []string{"mockpay"}
	}
	chMap := make(map[string]struct{}, len(channels))
	ord := make([]string, 0, len(channels))
	for _, ch := range channels {
		if _, ok := chMap[ch]; !ok && ch != "" {
			chMap[ch] = struct{}{}
			ord = append(ord, ch)
		}
	}
	if downloader == nil {
		downloader = noopDownloader{}
	}
	if checker == nil {
		checker = noopChecker{}
	}
	return &Recon{
		db:         db,
		ids:        ids,
		clk:        clk,
		channels:   chMap,
		channelOrd: ord,
		downloader: downloader,
		checker:    checker,
	}
}

// UseRetryScheduler 注入用于 BILL_NOT_READY 30 分钟单批次延迟重试的调度器。
func (r *Recon) UseRetryScheduler(s RetryScheduler) {
	r.retrySched = s
}

func (r *Recon) now() time.Time {
	return r.clk.Now().In(shanghai).Truncate(time.Millisecond)
}

// RunBatch 创建、重跑或接管指定 (channel, bill_date) 的对账批次。
func (r *Recon) RunBatch(ctx context.Context, in RunBatchInput) (*RunBatchOutput, error) {
	now := r.now()
	if err := r.validateInput(in, now); err != nil {
		return nil, err
	}
	if r.db == nil || r.ids == nil {
		return nil, errno.ErrInternal
	}

	batch, created, err := r.getOrCreateBatch(ctx, in.Channel, in.BillDate, now)
	if err != nil {
		return nil, err
	}

	if created {
		return r.runFromInit(ctx, batch)
	}

	switch batch.Status {
	case dal.StatusDone:
		if !in.Force {
			return r.buildOutput(ctx, batch)
		}
		return r.rerunBatch(ctx, batch)
	case dal.StatusFailed:
		return r.rerunBatch(ctx, batch)
	case dal.StatusInit:
		if now.Sub(batch.UpdatedAt) < takeoverTimeout {
			return r.buildOutput(ctx, batch)
		}
		return r.takeoverInit(ctx, batch, now)
	case dal.StatusDownloaded:
		if now.Sub(batch.UpdatedAt) < takeoverTimeout {
			return r.buildOutput(ctx, batch)
		}
		return r.takeoverDownloaded(ctx, batch, now)
	case dal.StatusChecking:
		if batch.StartedAt == nil || now.Sub(*batch.StartedAt) < takeoverTimeout {
			return r.buildOutput(ctx, batch)
		}
		return r.takeoverChecking(ctx, batch, now)
	default:
		return r.buildOutput(ctx, batch)
	}
}

// RunDaily 由每日定时触发：对 D-1 发起对账，并顺带处理 D-8 到 D-2 中未终态的已有批次。
func (r *Recon) RunDaily(ctx context.Context) error {
	now := r.now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, shanghai)
	dMinus1 := today.AddDate(0, 0, -1).Format("2006-01-02")
	fromDate := today.AddDate(0, 0, -(dailyLookbackDays + 1)).Format("2006-01-02")
	toDate := today.AddDate(0, 0, -2).Format("2006-01-02")

	var firstErr error
	for _, ch := range r.channelOrd {
		if _, err := r.RunBatch(ctx, RunBatchInput{Channel: ch, BillDate: dMinus1, Force: false}); err != nil {
			slog.ErrorContext(ctx, "recon daily RunBatch D-1 failed", "channel", ch, "bill_date", dMinus1, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
		pending, err := dal.ListPendingBatches(ctx, r.db, ch, fromDate, toDate)
		if err != nil {
			slog.ErrorContext(ctx, "recon daily ListPendingBatches failed", "channel", ch, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, b := range pending {
			if _, err := r.RunBatch(ctx, RunBatchInput{Channel: ch, BillDate: b.BillDate, Force: false}); err != nil {
				slog.ErrorContext(ctx, "recon daily pending RunBatch failed", "channel", ch, "bill_date", b.BillDate, "error", err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}
	return firstErr
}

func (r *Recon) validateInput(in RunBatchInput, now time.Time) error {
	if _, ok := r.channels[in.Channel]; !ok {
		return errno.Wrap(errno.ErrInvalidParam, fmt.Errorf("unsupported channel %q", in.Channel))
	}
	d, err := time.ParseInLocation("2006-01-02", in.BillDate, shanghai)
	if err != nil || d.Format("2006-01-02") != in.BillDate {
		return errno.Wrap(errno.ErrInvalidParam, fmt.Errorf("invalid bill_date %q", in.BillDate))
	}
	todayStr := now.In(shanghai).Format("2006-01-02")
	if in.BillDate > todayStr {
		return errno.Wrap(errno.ErrInvalidParam, fmt.Errorf("bill_date %q is in the future", in.BillDate))
	}
	return nil
}

func (r *Recon) getOrCreateBatch(ctx context.Context, channel, billDate string, now time.Time) (dal.Batch, bool, error) {
	b, err := dal.GetBatchByChannelDate(ctx, r.db, channel, billDate)
	if err == nil {
		return b, false, nil
	}
	if !errors.Is(err, dal.ErrNotFound) {
		return dal.Batch{}, false, errno.Wrap(errno.ErrInternal, err)
	}

	batchNo, err := r.ids.NewNo(ctx, batchNoPrefix)
	if err != nil {
		if errors.Is(err, idgen.ErrClockRollback) {
			return dal.Batch{}, false, errno.Wrap(errno.ErrUnavailable, err)
		}
		return dal.Batch{}, false, errno.Wrap(errno.ErrInternal, err)
	}

	id, err := dal.InsertBatch(ctx, r.db, batchNo, channel, billDate, now)
	if err != nil {
		if errors.Is(err, dal.ErrBatchExists) {
			existing, gerr := dal.GetBatchByChannelDate(ctx, r.db, channel, billDate)
			if gerr != nil {
				return dal.Batch{}, false, errno.Wrap(errno.ErrInternal, gerr)
			}
			return existing, false, nil
		}
		return dal.Batch{}, false, errno.Wrap(errno.ErrInternal, err)
	}

	created, err := dal.GetBatchByID(ctx, r.db, id)
	if err != nil {
		return dal.Batch{}, false, errno.Wrap(errno.ErrInternal, err)
	}
	return created, true, nil
}

func (r *Recon) rerunBatch(ctx context.Context, batch dal.Batch) (*RunBatchOutput, error) {
	now := r.now()
	won, err := dal.ResetForRerunTx(ctx, r.db, batch.ID, batch.BatchNo, batch.Status, batch.UpdatedAt, now)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	if !won {
		latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
		if err != nil {
			return nil, errno.Wrap(errno.ErrInternal, err)
		}
		return r.buildOutput(ctx, latest)
	}
	latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	return r.runFromInit(ctx, latest)
}

func (r *Recon) takeoverInit(ctx context.Context, batch dal.Batch, now time.Time) (*RunBatchOutput, error) {
	won, err := dal.TakeoverInitCAS(ctx, r.db, batch.ID, batch.UpdatedAt, now)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	if !won {
		latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
		if err != nil {
			return nil, errno.Wrap(errno.ErrInternal, err)
		}
		return r.buildOutput(ctx, latest)
	}
	latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	return r.runFromInit(ctx, latest)
}

func (r *Recon) takeoverDownloaded(ctx context.Context, batch dal.Batch, now time.Time) (*RunBatchOutput, error) {
	won, err := dal.TakeoverDownloadedToCheckingCAS(ctx, r.db, batch.ID, batch.UpdatedAt, now)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	if !won {
		latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
		if err != nil {
			return nil, errno.Wrap(errno.ErrInternal, err)
		}
		return r.buildOutput(ctx, latest)
	}
	latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	return r.runFromChecking(ctx, latest, now)
}

func (r *Recon) takeoverChecking(ctx context.Context, batch dal.Batch, now time.Time) (*RunBatchOutput, error) {
	won, err := dal.TakeoverCheckingCAS(ctx, r.db, batch.ID, batch.BatchNo, *batch.StartedAt, now)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	if !won {
		latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
		if err != nil {
			return nil, errno.Wrap(errno.ErrInternal, err)
		}
		return r.buildOutput(ctx, latest)
	}
	latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	return r.runFromChecking(ctx, latest, now)
}

func (r *Recon) runFromInit(ctx context.Context, batch dal.Batch) (*RunBatchOutput, error) {
	res, err := r.downloader.DownloadAndImport(ctx, r.db, &batch)
	if err != nil {
		if errors.Is(err, ErrBillNotReady) {
			return r.handleBillNotReady(ctx, batch)
		}
		failNow := r.now()
		_, _ = dal.MarkFailedWithUpdatedAtCAS(ctx, r.db, batch.ID, dal.StatusInit, batch.UpdatedAt, err.Error(), failNow)
		latest, gerr := dal.GetBatchByID(ctx, r.db, batch.ID)
		if gerr != nil {
			return nil, errno.Wrap(errno.ErrInternal, gerr)
		}
		return r.buildOutput(ctx, latest)
	}

	var dlNow time.Time
	var won bool
	if res.Transitioned {
		won = res.Won
		dlNow = res.UpdatedAt
	} else {
		dlNow = r.now()
		won, err = dal.MarkDownloadedCAS(ctx, r.db, batch.ID, res.FileHash, batch.UpdatedAt, dlNow)
		if err != nil {
			return nil, errno.Wrap(errno.ErrInternal, err)
		}
	}
	if !won {
		latest, gerr := dal.GetBatchByID(ctx, r.db, batch.ID)
		if gerr != nil {
			return nil, errno.Wrap(errno.ErrInternal, gerr)
		}
		return r.buildOutput(ctx, latest)
	}

	chkNow := r.now()
	won, err = dal.MarkCheckingCAS(ctx, r.db, batch.ID, dlNow, chkNow)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	if !won {
		latest, gerr := dal.GetBatchByID(ctx, r.db, batch.ID)
		if gerr != nil {
			return nil, errno.Wrap(errno.ErrInternal, gerr)
		}
		return r.buildOutput(ctx, latest)
	}

	latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	return r.runFromChecking(ctx, latest, chkNow)
}

func (r *Recon) handleBillNotReady(ctx context.Context, batch dal.Batch) (*RunBatchOutput, error) {
	now := r.now()
	if batch.RetryCount < billNotReadyMaxRetry {
		won, err := dal.IncrementRetryCountCAS(ctx, r.db, batch.ID, batch.RetryCount, now)
		if err != nil {
			return nil, errno.Wrap(errno.ErrInternal, err)
		}
		if won && r.retrySched != nil {
			r.retrySched.ScheduleRetry(batch.Channel, batch.BillDate, billNotReadyDelay)
		}
	} else {
		_, err := dal.MarkBillNotReadyFailedCAS(ctx, r.db, batch.ID, batch.RetryCount, now)
		if err != nil {
			return nil, errno.Wrap(errno.ErrInternal, err)
		}
	}
	latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	return r.buildOutput(ctx, latest)
}

func (r *Recon) runFromChecking(ctx context.Context, batch dal.Batch, startedAt time.Time) (*RunBatchOutput, error) {
	res, err := r.checker.Check(ctx, &batch)
	if err != nil {
		failNow := r.now()
		won, ferr := dal.MarkFailedFromCheckingCAS(ctx, r.db, batch.ID, startedAt, err.Error(), failNow)
		if ferr != nil {
			return nil, errno.Wrap(errno.ErrInternal, ferr)
		}
		if !won {
			// 已被其他接管者接管，旧运行放弃
			latest, gerr := dal.GetBatchByID(ctx, r.db, batch.ID)
			if gerr != nil {
				return nil, errno.Wrap(errno.ErrInternal, gerr)
			}
			return r.buildOutput(ctx, latest)
		}
		latest, gerr := dal.GetBatchByID(ctx, r.db, batch.ID)
		if gerr != nil {
			return nil, errno.Wrap(errno.ErrInternal, gerr)
		}
		return r.buildOutput(ctx, latest)
	}

	diffCount, err := dal.CountBatchDiffs(ctx, r.db, batch.BatchNo)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}

	doneNow := r.now()
	stats := dal.CheckStats{
		ChannelCount:  res.ChannelCount,
		ChannelAmount: res.ChannelAmount,
		LocalCount:    res.LocalCount,
		LocalAmount:   res.LocalAmount,
		MatchedCount:  res.MatchedCount,
		DiffCount:     diffCount,
	}
	won, err := dal.MarkDoneCAS(ctx, r.db, batch.ID, startedAt, stats, doneNow)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	if !won {
		// 已被接管，旧运行放弃，不覆盖新运行结果
		slog.WarnContext(ctx, "recon batch taken over, old checking run abandoned", "batch_no", batch.BatchNo)
	}

	latest, err := dal.GetBatchByID(ctx, r.db, batch.ID)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	return r.buildOutput(ctx, latest)
}

func (r *Recon) buildOutput(ctx context.Context, batch dal.Batch) (*RunBatchOutput, error) {
	diffs, err := dal.ListBatchDiffs(ctx, r.db, batch.BatchNo, maxReturnedDiffs)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	if diffs == nil {
		diffs = []dal.Diff{}
	}
	return &RunBatchOutput{
		Batch: batch,
		Diffs: diffs,
	}, nil
}
