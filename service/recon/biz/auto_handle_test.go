package biz_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/outbox"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/recon/biz"
	"github.com/yym108/pay-system/service/recon/dal"
)

type fakeAutoHandleStore struct {
	mu          sync.Mutex
	diffs       map[uint64]*dal.Diff
	order       []uint64
	resolveHook func(id uint64) // 测试用并发/删除钩子
	pendingHook func(id uint64)
}

func newFakeAutoHandleStore(items ...dal.Diff) *fakeAutoHandleStore {
	s := &fakeAutoHandleStore{
		diffs: make(map[uint64]*dal.Diff, len(items)),
	}
	for _, d := range items {
		cp := d
		s.diffs[d.ID] = &cp
		s.order = append(s.order, d.ID)
	}
	return s
}

func (s *fakeAutoHandleStore) ListPendingUnhandledDiffsByBatch(_ context.Context, batchNo string) ([]dal.Diff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []dal.Diff
	for _, id := range s.order {
		d, ok := s.diffs[id]
		if !ok {
			continue
		}
		if d.BatchNo == batchNo && d.Status == dal.DiffStatusPending && d.HandleAction == nil {
			out = append(out, *d)
		}
	}
	return out, nil
}

func (s *fakeAutoHandleStore) ListRetryableRequeryDiffs(_ context.Context, fromDate, toDate string) ([]dal.Diff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []dal.Diff
	for _, id := range s.order {
		d, ok := s.diffs[id]
		if !ok {
			continue
		}
		if d.Status == dal.DiffStatusPending &&
			d.HandleAction != nil && *d.HandleAction == "REQUERY" &&
			d.BillDate >= fromDate && d.BillDate <= toDate &&
			(d.DiffType != dal.DiffChannelMissing || d.ChannelStatus == nil) {
			out = append(out, *d)
		}
	}
	return out, nil
}

func (s *fakeAutoHandleStore) ListRetryableReplayLedgerDiffs(_ context.Context, fromDate, toDate string) ([]dal.Diff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []dal.Diff
	for _, id := range s.order {
		d, ok := s.diffs[id]
		if !ok {
			continue
		}
		if d.DiffType == dal.DiffLedgerMissing &&
			d.Status == dal.DiffStatusPending &&
			d.HandleAction != nil && *d.HandleAction == "REPLAY" &&
			d.BillDate >= fromDate && d.BillDate <= toDate {
			out = append(out, *d)
		}
	}
	return out, nil
}

func (s *fakeAutoHandleStore) GetDiffByID(_ context.Context, id uint64) (dal.Diff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.diffs[id]
	if !ok {
		return dal.Diff{}, dal.ErrNotFound
	}
	return *d, nil
}

func (s *fakeAutoHandleStore) ResolveDiffCAS(_ context.Context, id uint64, status, handleAction, handler, handleNote string, resolvedAt, now time.Time) (bool, error) {
	if s.resolveHook != nil {
		s.resolveHook(id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.diffs[id]
	if !ok || d.Status != dal.DiffStatusPending {
		return false, nil
	}
	d.Status = status
	act := handleAction
	h := handler
	n := handleNote
	rt := resolvedAt
	d.HandleAction = &act
	d.Handler = &h
	d.HandleNote = &n
	d.ResolvedAt = &rt
	d.UpdatedAt = now
	return true, nil
}

func (s *fakeAutoHandleStore) UpdateDiffPendingCAS(_ context.Context, id uint64, handleAction, handler, handleNote string, channelStatus *string, now time.Time) (bool, error) {
	if s.pendingHook != nil {
		s.pendingHook(id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.diffs[id]
	if !ok || d.Status != dal.DiffStatusPending {
		return false, nil
	}
	act := handleAction
	h := handler
	n := handleNote
	d.HandleAction = &act
	d.Handler = &h
	d.HandleNote = &n
	if channelStatus != nil {
		cs := *channelStatus
		d.ChannelStatus = &cs
	}
	d.UpdatedAt = now
	return true, nil
}

func (s *fakeAutoHandleStore) get(id uint64) (dal.Diff, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.diffs[id]
	if !ok {
		return dal.Diff{}, false
	}
	return *d, true
}

func (s *fakeAutoHandleStore) delete(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.diffs, id)
}

type fakeAutoHandleOrderReader struct {
	payments map[string]dal.InternalPaymentOrder
	refunds  map[string]dal.InternalRefundOrder
}

func (f *fakeAutoHandleOrderReader) ListPaymentOrdersByPayNos(_ context.Context, payNos []string) ([]dal.InternalPaymentOrder, error) {
	var out []dal.InternalPaymentOrder
	for _, no := range payNos {
		if p, ok := f.payments[no]; ok {
			out = append(out, p)
		}
	}
	return out, nil
}

func (f *fakeAutoHandleOrderReader) ListRefundOrdersByRefundNos(_ context.Context, refundNos []string) ([]dal.InternalRefundOrder, error) {
	var out []dal.InternalRefundOrder
	for _, no := range refundNos {
		if r, ok := f.refunds[no]; ok {
			out = append(out, r)
		}
	}
	return out, nil
}

type fakeShardReader struct {
	ledgers []dal.AccountLedgerTxn
}

func (f *fakeShardReader) ListLedgerTxnsByBizNos(_ context.Context, _, bizNos []string) ([]dal.AccountLedgerTxn, error) {
	wanted := make(map[string]struct{}, len(bizNos))
	for _, no := range bizNos {
		wanted[no] = struct{}{}
	}
	var out []dal.AccountLedgerTxn
	for _, lt := range f.ledgers {
		if _, ok := wanted[lt.BizNo]; ok {
			out = append(out, lt)
		}
	}
	return out, nil
}

func (f *fakeShardReader) ListAccountFreezesByBizNos(_ context.Context, _ string, _ []string) ([]dal.AccountFreezeRecord, error) {
	return nil, nil
}

func (f *fakeShardReader) ListTransferBranchesByTransferNos(_ context.Context, _ []string) ([]dal.AccountTransferBranch, error) {
	return nil, nil
}

type fakePaymentRPCClient struct {
	mu          sync.Mutex
	syncFn      func(bizNo string) (biz.SyncChannelTxnResult, error)
	replayFn    func(bizNo, eventType string) (biz.ReplayEventResult, error)
	syncCalls   []string
	replayCalls []string
}

func (f *fakePaymentRPCClient) SyncChannelTxn(_ context.Context, bizNo string) (biz.SyncChannelTxnResult, error) {
	f.mu.Lock()
	f.syncCalls = append(f.syncCalls, bizNo)
	fn := f.syncFn
	f.mu.Unlock()
	if fn == nil {
		return biz.SyncChannelTxnResult{}, nil
	}
	return fn(bizNo)
}

func (f *fakePaymentRPCClient) ReplayEvent(_ context.Context, bizNo, eventType string) (biz.ReplayEventResult, error) {
	f.mu.Lock()
	f.replayCalls = append(f.replayCalls, bizNo+":"+eventType)
	fn := f.replayFn
	f.mu.Unlock()
	if fn == nil {
		return biz.ReplayEventResult{EventID: "EV001"}, nil
	}
	return fn(bizNo, eventType)
}

func strPtr(s string) *string { return &s }
func i64Ptr(v int64) *int64   { return &v }

func TestAutoHandleStatusMismatchProcessingResolved(t *testing.T) {
	// REC-06 验收 1：状态不符（本地 PROCESSING）自动修复后，置 RESOLVED。
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))

	store := newFakeAutoHandleStore(
		dal.Diff{
			ID: 1, DiffNo: "RD001", BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26",
			DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA001",
			LocalStatus: strPtr("PROCESSING"), ChannelStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending,
		},
		dal.Diff{
			ID: 2, DiffNo: "RD002", BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26",
			DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA002",
			LocalStatus: strPtr("FAILED"), ChannelStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending,
		},
	)
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: true}, nil
		},
	}
	ah := biz.NewAutoHandlerWithDeps(store, &fakeAutoHandleOrderReader{}, [2]biz.AccountShardReader{&fakeShardReader{}, &fakeShardReader{}}, payCli, clk)
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check err: %v", err)
	}
	if len(payCli.syncCalls) != 1 || payCli.syncCalls[0] != "PA001" {
		t.Fatalf("syncCalls = %v, want [PA001]", payCli.syncCalls)
	}
	d1, _ := store.get(1)
	if d1.Status != dal.DiffStatusResolved || d1.HandleAction == nil || *d1.HandleAction != "REQUERY" || d1.Handler == nil || *d1.Handler != "system" || d1.ResolvedAt == nil {
		t.Fatalf("unexpected d1: %+v", d1)
	}
	// 本地已终态不自动处理，保持 PENDING 且 handle_action 为空
	d2, _ := store.get(2)
	if d2.Status != dal.DiffStatusPending || d2.HandleAction != nil {
		t.Fatalf("unexpected d2: %+v", d2)
	}
}

func TestAutoHandleFailureWritesActionAndRecovers(t *testing.T) {
	// REC-06 验收 3：自动处理失败也写 handle_action=REQUERY、handler=system，handle_note 含失败原因；下次运行恢复后置 RESOLVED。
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))

	store := newFakeAutoHandleStore(
		dal.Diff{
			ID: 10, DiffNo: "RD010", BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-25",
			DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA010",
			LocalStatus: strPtr("PROCESSING"), ChannelStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending,
		},
	)
	failMode := true
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			if failMode {
				return biz.SyncChannelTxnResult{}, errors.New("channel timeout")
			}
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: true}, nil
		},
	}
	ah := biz.NewAutoHandlerWithDeps(store, &fakeAutoHandleOrderReader{}, [2]biz.AccountShardReader{&fakeShardReader{}, &fakeShardReader{}}, payCli, clk)

	// 第一次（RB001）：本批次新差错，因为先跑 CF-41 再跑 CF-29，不会在同一次运行里被重复调用
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-25"}); err != nil {
		t.Fatalf("Check 1 err: %v", err)
	}
	if len(payCli.syncCalls) != 1 {
		t.Fatalf("syncCalls = %d, want 1", len(payCli.syncCalls))
	}
	d, _ := store.get(10)
	if d.Status != dal.DiffStatusPending || d.HandleAction == nil || *d.HandleAction != "REQUERY" || d.Handler == nil || *d.Handler != "system" || d.HandleNote == nil || !strings.Contains(*d.HandleNote, "channel timeout") {
		t.Fatalf("unexpected diff after failure: %+v", d)
	}

	// 恢复，下一次任意账期批次运行（2026-10-27 跑 RB002），2026-10-25 在 [D-8, D-2] 窗口内
	failMode = false
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB002", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check 2 err: %v", err)
	}
	d, _ = store.get(10)
	if d.Status != dal.DiffStatusResolved || d.ResolvedAt == nil {
		t.Fatalf("expected RESOLVED after retry, got: %+v", d)
	}
}

func TestAutoHandleLedgerMissingReplayFailureAndRetry(t *testing.T) {
	// REC-06 验收 2 & 4：LEDGER_MISSING 重放失败也写 handle_action=REPLAY；下次复核仍缺失则再次重放；入账后下次运行置 RESOLVED。
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))

	merchantID := "M001001"
	sIdx, serr := shard.Of(merchantID)
	if serr != nil {
		t.Fatalf("shard.Of: %v", serr)
	}

	store := newFakeAutoHandleStore(
		dal.Diff{
			ID: 20, DiffNo: "RD020", BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-25",
			DiffType: dal.DiffLedgerMissing, TxnType: "PAY", BizNo: "PA020",
			LocalAmount: i64Ptr(5000), LocalStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending,
		},
	)
	orders := &fakeAutoHandleOrderReader{
		payments: map[string]dal.InternalPaymentOrder{
			"PA020": {PayNo: "PA020", MerchantID: merchantID, Amount: 5000, Status: "SUCCESS"},
		},
	}
	var shards [2]biz.AccountShardReader
	shards[0] = &fakeShardReader{}
	shards[1] = &fakeShardReader{}
	targetShard := shards[sIdx].(*fakeShardReader)

	replayErr := errors.New("mq unavailable")
	payCli := &fakePaymentRPCClient{
		replayFn: func(bizNo, eventType string) (biz.ReplayEventResult, error) {
			if eventType != outbox.EventPaymentSucceeded {
				return biz.ReplayEventResult{}, errors.New("wrong event type")
			}
			return biz.ReplayEventResult{EventID: "EV20"}, replayErr
		},
	}
	ah := biz.NewAutoHandlerWithDeps(store, orders, shards, payCli, clk)

	// Run 1: 本批次处理，ReplayEvent 失败，仍写 handle_action = REPLAY
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-25"}); err != nil {
		t.Fatalf("Check 1 err: %v", err)
	}
	d, _ := store.get(20)
	if d.Status != dal.DiffStatusPending || d.HandleAction == nil || *d.HandleAction != "REPLAY" {
		t.Fatalf("expected PENDING + REPLAY, got: %+v", d)
	}

	// Run 2: 跨批次复核，凭证仍缺失，再次调用 ReplayEvent（这次成功）
	replayErr = nil
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB002", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check 2 err: %v", err)
	}
	if len(payCli.replayCalls) != 2 {
		t.Fatalf("replayCalls = %d, want 2", len(payCli.replayCalls))
	}
	d, _ = store.get(20)
	if d.Status != dal.DiffStatusPending {
		t.Fatalf("expected still PENDING, got: %+v", d)
	}

	// 模拟入账完成
	targetShard.ledgers = append(targetShard.ledgers, dal.AccountLedgerTxn{
		TxnNo: "TX020", BizType: "PAY", BizNo: "PA020", Amount: 5000,
	})

	// Run 3: 跨批次复核，凭证存在且金额一致 → RESOLVED
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB003", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check 3 err: %v", err)
	}
	d, _ = store.get(20)
	if d.Status != dal.DiffStatusResolved || d.ResolvedAt == nil {
		t.Fatalf("expected RESOLVED, got: %+v", d)
	}
}

func TestAutoHandleRetryWindow(t *testing.T) {
	// REC-06 验收 5：自动重试窗口 [D-8, D-2]。D=2026-10-27 时，只处理 2026-10-25 (D-2) 和 2026-10-19 (D-8)；
	// 2026-10-26 (D-1) 与 2026-10-18 (D-9) 不变；D-9 的 LEDGER_MISSING 不再重放。
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))

	store := newFakeAutoHandleStore(
		dal.Diff{ID: 1, DiffNo: "RD1", BatchNo: "RB_D1", Channel: "mockpay", BillDate: "2026-10-26", DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_D1", LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending, HandleAction: strPtr("REQUERY")},
		dal.Diff{ID: 2, DiffNo: "RD2", BatchNo: "RB_D2", Channel: "mockpay", BillDate: "2026-10-25", DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_D2", LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending, HandleAction: strPtr("REQUERY")},
		dal.Diff{ID: 3, DiffNo: "RD3", BatchNo: "RB_D8", Channel: "mockpay", BillDate: "2026-10-19", DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_D8", LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending, HandleAction: strPtr("REQUERY")},
		dal.Diff{ID: 4, DiffNo: "RD4", BatchNo: "RB_D9", Channel: "mockpay", BillDate: "2026-10-18", DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA_D9", LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending, HandleAction: strPtr("REQUERY")},
		dal.Diff{ID: 5, DiffNo: "RD5", BatchNo: "RB_D9", Channel: "mockpay", BillDate: "2026-10-18", DiffType: dal.DiffLedgerMissing, TxnType: "PAY", BizNo: "PA_LM_D9", LocalAmount: i64Ptr(100), Status: dal.DiffStatusPending, HandleAction: strPtr("REPLAY")},
	)
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: true}, nil
		},
	}
	ah := biz.NewAutoHandlerWithDeps(store, &fakeAutoHandleOrderReader{}, [2]biz.AccountShardReader{&fakeShardReader{}, &fakeShardReader{}}, payCli, clk)

	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB_CUR", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check err: %v", err)
	}
	if len(payCli.syncCalls) != 2 || payCli.syncCalls[0] != "PA_D2" || payCli.syncCalls[1] != "PA_D8" {
		t.Fatalf("syncCalls = %v, want [PA_D2 PA_D8]", payCli.syncCalls)
	}
	if len(payCli.replayCalls) != 0 {
		t.Fatalf("replayCalls = %v, want empty", payCli.replayCalls)
	}
	d1, _ := store.get(1)
	d2, _ := store.get(2)
	d3, _ := store.get(3)
	d4, _ := store.get(4)
	d5, _ := store.get(5)
	if d1.Status != dal.DiffStatusPending || d4.Status != dal.DiffStatusPending || d5.Status != dal.DiffStatusPending {
		t.Fatalf("out-of-window diffs should remain PENDING")
	}
	if d2.Status != dal.DiffStatusResolved || d3.Status != dal.DiffStatusResolved {
		t.Fatalf("in-window diffs should be RESOLVED")
	}
}

func TestAutoHandleChannelMissingInconsistentAndError(t *testing.T) {
	// REC-06 验收 6：严重不一致（CHANNEL_MISSING 查单返回 FAILED）写 channel_status=FAILED、ERROR 日志、指标 +1、不再重试；
	// 查单报错时 channel_status 为空，下次运行重试。
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))

	var logBuf bytes.Buffer
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(oldLogger)

	// 注册自定义 registry 或读取 global metric 增量
	beforeMetric := counterValue(t, "pay_recon_channel_inconsistent_total", map[string]string{"channel": "mockpay"})

	store := newFakeAutoHandleStore(
		dal.Diff{
			ID: 31, DiffNo: "RD031", BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-25",
			DiffType: dal.DiffChannelMissing, TxnType: "PAY", BizNo: "PA031",
			LocalStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending,
		},
		dal.Diff{
			ID: 32, DiffNo: "RD032", BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-25",
			DiffType: dal.DiffChannelMissing, TxnType: "PAY", BizNo: "PA032",
			LocalStatus: strPtr("SUCCESS"), Status: dal.DiffStatusPending,
		},
	)
	errPA032 := errors.New("rpc timeout")
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			if bizNo == "PA031" {
				return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "FAILED", Changed: false}, nil
			}
			if errPA032 != nil {
				return biz.SyncChannelTxnResult{}, errPA032
			}
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: false}, nil
		},
	}
	ah := biz.NewAutoHandlerWithDeps(store, &fakeAutoHandleOrderReader{}, [2]biz.AccountShardReader{&fakeShardReader{}, &fakeShardReader{}}, payCli, clk)

	// Run 1: PA031 严重不一致，PA032 查单报错
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-25"}); err != nil {
		t.Fatalf("Check 1 err: %v", err)
	}
	d31, _ := store.get(31)
	if d31.Status != dal.DiffStatusPending || d31.ChannelStatus == nil || *d31.ChannelStatus != "FAILED" || d31.HandleAction == nil || *d31.HandleAction != "REQUERY" {
		t.Fatalf("unexpected d31: %+v", d31)
	}
	d32, _ := store.get(32)
	if d32.Status != dal.DiffStatusPending || d32.ChannelStatus != nil || d32.HandleAction == nil || *d32.HandleAction != "REQUERY" {
		t.Fatalf("unexpected d32: %+v", d32)
	}
	afterMetric1 := counterValue(t, "pay_recon_channel_inconsistent_total", map[string]string{"channel": "mockpay"})
	if afterMetric1-beforeMetric != 1 {
		t.Fatalf("metric delta = %v, want 1", afterMetric1-beforeMetric)
	}
	if !strings.Contains(logBuf.String(), `"level":"ERROR"`) || !strings.Contains(logBuf.String(), "RD031") {
		t.Fatalf("expected ERROR log for RD031, got: %s", logBuf.String())
	}

	// Run 2: 跨批次窗口重试，PA031 不再查单，PA032 重试成功置 IGNORED
	errPA032 = nil
	payCli.syncCalls = nil
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB002", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check 2 err: %v", err)
	}
	if len(payCli.syncCalls) != 1 || payCli.syncCalls[0] != "PA032" {
		t.Fatalf("syncCalls = %v, want [PA032]", payCli.syncCalls)
	}
	d32, _ = store.get(32)
	if d32.Status != dal.DiffStatusIgnored {
		t.Fatalf("expected d32 IGNORED, got: %+v", d32)
	}
	afterMetric2 := counterValue(t, "pay_recon_channel_inconsistent_total", map[string]string{"channel": "mockpay"})
	if afterMetric2 != afterMetric1 {
		t.Fatalf("metric should not increase on run 2")
	}
}

func TestAutoHandleCASRowDeleted(t *testing.T) {
	// REC-06 验收 7：CAS 影响 0 行且重读查不到行时，返回“已不存在”，不报错、不重试、不重新插入，输出 WARN 日志。
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))

	var logBuf bytes.Buffer
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(oldLogger)

	store := newFakeAutoHandleStore(
		dal.Diff{
			ID: 41, DiffNo: "RD041", BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26",
			DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA041",
			LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending,
		},
	)
	store.resolveHook = func(id uint64) {
		store.delete(id)
	}
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: true}, nil
		},
	}
	ah := biz.NewAutoHandlerWithDeps(store, &fakeAutoHandleOrderReader{}, [2]biz.AccountShardReader{&fakeShardReader{}, &fakeShardReader{}}, payCli, clk)
	if _, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26"}); err != nil {
		t.Fatalf("Check err: %v", err)
	}
	if len(payCli.syncCalls) != 1 {
		t.Fatalf("syncCalls = %d, want 1 (no retry)", len(payCli.syncCalls))
	}
	if _, ok := store.get(41); ok {
		t.Fatal("deleted row should not be re-inserted")
	}
	if !strings.Contains(logBuf.String(), `"level":"WARN"`) || !strings.Contains(logBuf.String(), "RD041") {
		t.Fatalf("expected WARN log for deleted RD041, got: %s", logBuf.String())
	}
}

func TestAutoHandleConcurrentCAS(t *testing.T) {
	// REC-06 验收 8：两条运行并发置同一条 PENDING 差错为终态，只有一次影响 1 行，另一次影响 0 行重读返回且不覆盖 handle_note。
	sh := time.FixedZone("Asia/Shanghai", 8*3600)
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 10, 27, 10, 0, 0, 0, sh))

	store := newFakeAutoHandleStore(
		dal.Diff{
			ID: 51, DiffNo: "RD051", BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26",
			DiffType: dal.DiffStatusMismatch, TxnType: "PAY", BizNo: "PA051",
			LocalStatus: strPtr("PROCESSING"), Status: dal.DiffStatusPending,
		},
	)
	payCli := &fakePaymentRPCClient{
		syncFn: func(bizNo string) (biz.SyncChannelTxnResult, error) {
			return biz.SyncChannelTxnResult{LocalStatus: "SUCCESS", ChannelStatus: "SUCCESS", Changed: true}, nil
		},
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ah := biz.NewAutoHandlerWithDeps(store, &fakeAutoHandleOrderReader{}, [2]biz.AccountShardReader{&fakeShardReader{}, &fakeShardReader{}}, payCli, clk)
			_, err := ah.Check(context.Background(), &dal.Batch{BatchNo: "RB001", Channel: "mockpay", BillDate: "2026-10-26"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Check err: %v", err)
		}
	}
	d, ok := store.get(51)
	if !ok || d.Status != dal.DiffStatusResolved {
		t.Fatalf("unexpected diff: %+v", d)
	}
}

func counterValue(t *testing.T, name string, labels map[string]string) int64 {
	t.Helper()
	// 触发一次 obs 包初始化
	_ = obs.AddReconDiff
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := true
			for k, v := range labels {
				found := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == k && lp.GetValue() == v {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched {
				return int64(m.GetCounter().GetValue())
			}
		}
	}
	return 0
}
