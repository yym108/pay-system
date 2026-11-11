package obs

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func stageSamples(t *testing.T, chain, stage string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for _, mf := range mfs {
		if mf.GetName() != "pay_stage_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelValue(m, "chain") == chain && labelValue(m, "stage") == stage && m.GetHistogram() != nil {
				n += m.GetHistogram().GetSampleCount()
			}
		}
	}
	return n
}

func TestStageHistogramAndEvent(t *testing.T) {
	rec := useRecorder(t)
	ctx, span := otel.Tracer("obs").Start(context.Background(), "stage")
	end := Stage(ctx, "pay_create", "gw_auth")
	before := stageSamples(t, "pay_create", "gw_auth")
	end()
	end()
	span.End()
	after := stageSamples(t, "pay_create", "gw_auth")
	if after != before+1 {
		t.Fatalf("samples = %d want %d", after, before+1)
	}
	spans := rec.Ended()
	if len(spans) != 1 || len(spans[0].Events()) != 1 {
		t.Fatalf("spans=%d events=%d", len(spans), len(spans[0].Events()))
	}
	ev := spans[0].Events()[0]
	if ev.Name != "gw_auth" {
		t.Fatalf("event = %s", ev.Name)
	}
	var chain, stage string
	var ns int64
	var sawNS bool
	for _, attr := range ev.Attributes {
		switch string(attr.Key) {
		case "chain":
			chain = attr.Value.AsString()
		case "stage":
			stage = attr.Value.AsString()
		case "duration_ns":
			ns = attr.Value.AsInt64()
			sawNS = true
		}
	}
	if chain != "pay_create" || stage != "gw_auth" || !sawNS || ns < 0 {
		t.Fatalf("attrs chain=%s stage=%s ns=%d saw=%v", chain, stage, ns, sawNS)
	}
}

func TestStageEmptyLabelsAndNoSpan(t *testing.T) {
	before := stageSamples(t, "unknown", "unknown")
	end := Stage(context.Background(), "", "")
	end()
	after := stageSamples(t, "unknown", "unknown")
	if after != before+1 {
		t.Fatalf("samples = %d want %d", after, before+1)
	}
}

func TestStageNegativeDuration(t *testing.T) {
	rec := useRecorder(t)
	prev := sinceFunc
	sinceFunc = func(time.Time) time.Duration { return -time.Millisecond }
	t.Cleanup(func() { sinceFunc = prev })
	ctx, span := otel.Tracer("obs").Start(context.Background(), "neg")
	before := stageSamples(t, "refund", "db_tx")
	Stage(ctx, "refund", "db_tx")()
	span.End()
	if stageSamples(t, "refund", "db_tx") != before+1 {
		t.Fatal("negative duration was not recorded")
	}
	ev := rec.Ended()[0].Events()
	if len(ev) != 1 {
		t.Fatalf("events = %d", len(ev))
	}
	var ns int64
	var saw bool
	for _, attr := range ev[0].Attributes {
		if string(attr.Key) == "duration_ns" {
			ns = attr.Value.AsInt64()
			saw = true
		}
	}
	if !saw || ns != 0 {
		t.Fatalf("duration_ns = %d saw=%v", ns, saw)
	}
}

func TestBeginSlotOnce(t *testing.T) {
	ctx := WithSlot(WithChain(context.Background(), ChainTransferTCC))
	ctx, span := otel.Tracer("obs").Start(ctx, "slot")
	defer span.End()
	first := Begin(ctx, "", StageGWAuth)
	second := Begin(ctx, "", StageDBInsert)
	first()
	first()
	CloseSlot(ctx)
	second()
	if stageSamples(t, ChainTransferTCC, StageGWAuth) < 1 || stageSamples(t, ChainTransferTCC, StageDBInsert) < 1 {
		t.Fatal("slot stages missing")
	}
	auth := stageSamples(t, ChainTransferTCC, StageGWAuth)
	insert := stageSamples(t, ChainTransferTCC, StageDBInsert)
	CloseSlot(ctx)
	second()
	if stageSamples(t, ChainTransferTCC, StageGWAuth) != auth || stageSamples(t, ChainTransferTCC, StageDBInsert) != insert {
		t.Fatal("repeated end added a sample")
	}
}

func TestChainHintUpdatesOpenStage(t *testing.T) {
	ctx := WithSlot(WithChain(context.Background(), ""))
	end := Begin(ctx, "", StageGWAuth)
	WithChain(ctx, ChainTransferLocal)
	end()
	if stageSamples(t, ChainTransferLocal, StageGWAuth) < 1 {
		t.Fatal("hint was not applied at end")
	}
	if span := trace.SpanFromContext(context.Background()); span.SpanContext().IsValid() {
		t.Fatal("background span")
	}
}

func TestBindReleaseOnce(t *testing.T) {
	before := stageSamples(t, ChainPaySettle, StageMQWait)
	end := Stage(context.Background(), ChainPaySettle, StageMQWait)
	Bind(MQWaitKey("EV1"), end)
	Release(MQWaitKey("EV1"))
	Release(MQWaitKey("EV1"))
	if stageSamples(t, ChainPaySettle, StageMQWait) != before+1 {
		t.Fatalf("samples = %d want %d", stageSamples(t, ChainPaySettle, StageMQWait), before+1)
	}
	Release("")
	Release(MQWaitKey(""))
}

func TestAuthWatchFreezesBeforeObserve(t *testing.T) {
	rec := useRecorder(t)
	prev := sinceFunc
	sinceFunc = func(time.Time) time.Duration { return 5 * time.Millisecond }
	t.Cleanup(func() { sinceFunc = prev })
	w := StartAuth()
	w.Stop()
	sinceFunc = func(time.Time) time.Duration { return time.Hour }
	ctx, span := otel.Tracer("obs").Start(context.Background(), "auth")
	before := stageSamples(t, ChainPayCreate, StageGWAuth)
	w.Observe(ctx, ChainPayCreate)
	w.Observe(ctx, ChainPayCreate)
	span.End()
	if stageSamples(t, ChainPayCreate, StageGWAuth) != before+1 {
		t.Fatal("gw_auth recorded more than once")
	}
	ev := rec.Ended()[0].Events()
	if len(ev) != 1 || ev[0].Name != StageGWAuth {
		t.Fatalf("events = %+v", ev)
	}
	var ns int64
	for _, attr := range ev[0].Attributes {
		if string(attr.Key) == "duration_ns" {
			ns = attr.Value.AsInt64()
		}
	}
	if ns != (5 * time.Millisecond).Nanoseconds() {
		t.Fatalf("duration_ns = %d", ns)
	}
	miss := StartAuth()
	miss.Stop()
	localBefore := stageSamples(t, ChainTransferLocal, StageGWAuth)
	tccBefore := stageSamples(t, ChainTransferTCC, StageGWAuth)
	miss.ObserveTransfer(context.Background(), "")
	miss.ObserveTransfer(context.Background(), "OTHER")
	if stageSamples(t, ChainTransferLocal, StageGWAuth) != localBefore || stageSamples(t, ChainTransferTCC, StageGWAuth) != tccBefore {
		t.Fatal("missing mode recorded a chain")
	}
	miss.ObserveTransfer(context.Background(), "LOCAL")
	if stageSamples(t, ChainTransferLocal, StageGWAuth) != localBefore+1 {
		t.Fatal("LOCAL was not recorded")
	}
}

func TestAuthWatchAddOnlyWhilePaused(t *testing.T) {
	rec := useRecorder(t)
	prev := sinceFunc
	sinceFunc = func(time.Time) time.Duration { return 0 }
	t.Cleanup(func() { sinceFunc = prev })
	w := StartAuth()
	w.Add(2 * time.Millisecond)
	w.Pause()
	w.Add(-time.Millisecond)
	w.Add(0)
	w.Add(2 * time.Millisecond)
	var nilWatch *AuthWatch
	nilWatch.Add(time.Second)
	w.Resume()
	w.Add(9 * time.Millisecond)
	w.Pause()
	ctx, span := otel.Tracer("obs").Start(context.Background(), "auth-add")
	before := stageSamples(t, ChainPayCreate, StageGWAuth)
	w.Observe(ctx, ChainPayCreate)
	w.Add(time.Second)
	span.End()
	if stageSamples(t, ChainPayCreate, StageGWAuth) != before+1 {
		t.Fatal("gw_auth recorded more than once")
	}
	ev := rec.Ended()[0].Events()
	var ns int64
	for _, attr := range ev[0].Attributes {
		if string(attr.Key) == "duration_ns" {
			ns = attr.Value.AsInt64()
		}
	}
	if ns != (2 * time.Millisecond).Nanoseconds() {
		t.Fatalf("duration_ns = %d", ns)
	}
}

func TestFinishWaitUsesCallerSpan(t *testing.T) {
	rec := useRecorder(t)
	_, dead := otel.Tracer("obs").Start(context.Background(), "callback")
	dead.End()
	before := stageSamples(t, ChainPaySettle, StageOutboxWait)
	HoldWait(OutboxWaitKey("EV-HOLD"), ChainPaySettle, StageOutboxWait)
	HoldWait(OutboxWaitKey("EV-HOLD"), ChainPaySettle, StageOutboxWait)
	if stageSamples(t, ChainPaySettle, StageOutboxWait) != before+1 {
		t.Fatal("rebind did not record the previous wait")
	}
	liveCtx, live := otel.Tracer("obs").Start(context.Background(), "dispatch")
	FinishWait(liveCtx, OutboxWaitKey("EV-HOLD"))
	FinishWait(liveCtx, OutboxWaitKey("EV-HOLD"))
	live.End()
	if stageSamples(t, ChainPaySettle, StageOutboxWait) != before+2 {
		t.Fatal("finish did not record once")
	}
	var callbackN, dispatchN int
	for _, sp := range rec.Ended() {
		switch sp.Name() {
		case "callback":
			callbackN = len(sp.Events())
		case "dispatch":
			dispatchN = len(sp.Events())
		}
	}
	if callbackN != 0 || dispatchN != 1 {
		t.Fatalf("callback events=%d dispatch events=%d", callbackN, dispatchN)
	}
}

func TestObserveMQWaitSkipsBadHeader(t *testing.T) {
	rec := useRecorder(t)
	before := stageSamples(t, ChainPaySettle, StageMQWait)
	ctx, span := otel.Tracer("obs").Start(context.Background(), "consume")
	ObserveMQWait(ctx, "")
	ObserveMQWait(ctx, "nope")
	if stageSamples(t, ChainPaySettle, StageMQWait) != before {
		t.Fatal("bad header recorded a sample")
	}
	ObserveMQWait(ctx, "1")
	span.End()
	if stageSamples(t, ChainPaySettle, StageMQWait) != before+1 {
		t.Fatal("produced_at was not recorded")
	}
	if len(rec.Ended()) != 1 || len(rec.Ended()[0].Events()) != 1 || rec.Ended()[0].Events()[0].Name != StageMQWait {
		t.Fatal("mq_wait event is not on the consume span")
	}
}
