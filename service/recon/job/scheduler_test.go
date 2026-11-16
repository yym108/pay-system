package job_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/job"
)

type fakeRunner struct {
	dailyCalls atomic.Int32
	retryCalls atomic.Int32
	lastChan   string
	lastDate   string
}

func (f *fakeRunner) RunDaily(_ context.Context) error {
	f.dailyCalls.Add(1)
	return nil
}

func (f *fakeRunner) RunBatch(_ context.Context, in biz.RunBatchInput) (*biz.RunBatchOutput, error) {
	f.retryCalls.Add(1)
	f.lastChan = in.Channel
	f.lastDate = in.BillDate
	return &biz.RunBatchOutput{}, nil
}

func TestSchedulerDailyAndSingleRetry(t *testing.T) {
	clk := &clock.Fake{}
	shanghai := time.FixedZone("Asia/Shanghai", 8*3600)
	clk.Set(time.Date(2026, 10, 27, 9, 59, 0, 0, shanghai))

	r := &fakeRunner{}
	s := job.NewScheduler(r, clk, "10:00", nil)

	// 09:59 不触发每日对账
	if err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.dailyCalls.Load() != 0 {
		t.Fatalf("dailyCalls=%d at 09:59, want 0", r.dailyCalls.Load())
	}

	// 10:00 触发每日对账
	clk.Advance(time.Minute)
	if err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.dailyCalls.Load() != 1 {
		t.Fatalf("dailyCalls=%d at 10:00, want 1", r.dailyCalls.Load())
	}

	// 同一天 10:30 不重复触发每日对账
	clk.Advance(30 * time.Minute)
	if err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.dailyCalls.Load() != 1 {
		t.Fatalf("dailyCalls=%d at 10:30, want 1", r.dailyCalls.Load())
	}

	// 注册 30 分钟后的单批次重试
	s.ScheduleRetry("mockpay", "2026-10-26", 30*time.Minute)
	clk.Advance(29 * time.Minute)
	if err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.retryCalls.Load() != 0 {
		t.Fatalf("retryCalls=%d at 29m, want 0", r.retryCalls.Load())
	}

	clk.Advance(time.Minute)
	if err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.retryCalls.Load() != 1 || r.lastChan != "mockpay" || r.lastDate != "2026-10-26" {
		t.Fatalf("retryCalls=%d chan=%s date=%s", r.retryCalls.Load(), r.lastChan, r.lastDate)
	}

	// 第二天 10:00 再次触发每日对账
	clk.Set(time.Date(2026, 10, 28, 10, 0, 0, 0, shanghai))
	if err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.dailyCalls.Load() != 2 {
		t.Fatalf("dailyCalls=%d on next day, want 2", r.dailyCalls.Load())
	}
}
