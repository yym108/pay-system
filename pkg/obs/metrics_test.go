package obs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestLatencyBucketsShared(t *testing.T) {
	want := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	if len(LatencyBuckets) != len(want) {
		t.Fatalf("len = %d", len(LatencyBuckets))
	}
	for i := range want {
		if LatencyBuckets[i] != want[i] {
			t.Fatalf("bucket %d = %v", i, LatencyBuckets[i])
		}
	}
	ObserveHTTP("obs-bucket-check", "0", 0)
	mf := family(t, "pay_http_request_duration_seconds")
	var bounds []float64
	for _, m := range mf.GetMetric() {
		if labelValue(m, "route") != "obs-bucket-check" {
			continue
		}
		for _, b := range m.GetHistogram().GetBucket() {
			bounds = append(bounds, b.GetUpperBound())
		}
	}
	// dto 的 bucket 不含 +Inf；+Inf 只出现在文本导出里。
	if len(bounds) != len(want) {
		t.Fatalf("bounds = %v", bounds)
	}
	for i := range want {
		if bounds[i] != want[i] {
			t.Fatalf("le %d = %v", i, bounds[i])
		}
	}
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), `route="obs-bucket-check"`) ||
		!strings.Contains(rec.Body.String(), `le="+Inf"`) {
		t.Fatal("exposition is missing the shared +Inf bucket")
	}
}

func TestHandlerExposesRuntime(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "go_goroutines") || !strings.Contains(body, "process_resident_memory_bytes") {
		t.Fatal("missing runtime series")
	}
	if !strings.Contains(body, "pay_refund_quota_inconsistent_total 0") {
		t.Fatal("unlabeled stay-zero counter is missing")
	}
}

func TestLedgerCheckFailureIncrementsOnce(t *testing.T) {
	before := counterValue(t, "pay_ledger_check_failures_total")
	AddLedgerCheckFailure()
	after := counterValue(t, "pay_ledger_check_failures_total")
	if after != before+1 {
		t.Fatalf("counter %v want %v", after, before+1)
	}
}

func family(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	t.Fatalf("metric %s is not registered", name)
	return nil
}

func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	mf := family(t, name)
	if len(mf.GetMetric()) != 1 || mf.GetMetric()[0].GetCounter() == nil {
		t.Fatalf("%s = %+v", name, mf)
	}
	return mf.GetMetric()[0].GetCounter().GetValue()
}

func labelValue(m *dto.Metric, name string) string {
	for _, pair := range m.GetLabel() {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}
