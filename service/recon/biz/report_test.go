package biz_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

func gaugeValue(t *testing.T, name string, labels map[string]string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matchLabels(m.GetLabel(), labels) && m.GetGauge() != nil {
				return uint64(m.GetGauge().GetValue())
			}
		}
	}
	return 0
}

func matchLabels(got []*dto.LabelPair, want map[string]string) bool {
	if len(want) == 0 {
		return len(got) == 0
	}
	matched := 0
	for _, lp := range got {
		if v, ok := want[lp.GetName()]; ok && lp.GetValue() == v {
			matched++
		}
	}
	return matched == len(want)
}

func TestManualBeforeDate(t *testing.T) {
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	// 2026-10-27 00:10:00 +08:00 -> D = 2026-10-27, D-8 = 2026-10-19
	now := time.Date(2026, 10, 27, 0, 10, 0, 0, sh)
	if got := biz.ManualBeforeDate(now); got != "2026-10-19" {
		t.Fatalf("ManualBeforeDate = %q, want 2026-10-19", got)
	}
}

func TestSetReconPendingDiffsResetsZero(t *testing.T) {
	obs.SetReconPendingDiffs(map[string]int64{
		dal.DiffLocalMissing:   3,
		dal.DiffChannelMissing: 2,
	})
	if got := gaugeValue(t, "pay_recon_pending_diffs", map[string]string{"diff_type": dal.DiffLocalMissing}); got != 3 {
		t.Fatalf("LOCAL_MISSING gauge = %v, want 3", got)
	}
	if got := gaugeValue(t, "pay_recon_pending_diffs", map[string]string{"diff_type": dal.DiffChannelMissing}); got != 2 {
		t.Fatalf("CHANNEL_MISSING gauge = %v, want 2", got)
	}

	// 解决后再次刷新，未出现的类型应归零
	obs.SetReconPendingDiffs(map[string]int64{
		dal.DiffLocalMissing: 1,
	})
	if got := gaugeValue(t, "pay_recon_pending_diffs", map[string]string{"diff_type": dal.DiffLocalMissing}); got != 1 {
		t.Fatalf("LOCAL_MISSING gauge after update = %v, want 1", got)
	}
	if got := gaugeValue(t, "pay_recon_pending_diffs", map[string]string{"diff_type": dal.DiffChannelMissing}); got != 0 {
		t.Fatalf("CHANNEL_MISSING gauge after reset = %v, want 0", got)
	}
}
