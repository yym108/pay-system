package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/kitex_gen/payment/paymentservice"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/outbox"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/recon/dal"
)

const (
	handlerSystem      = "system"
	actionRequery      = "REQUERY"
	actionReplay       = "REPLAY"
	statusProcessing   = "PROCESSING"
	rpcCallTimeout     = 3 * time.Second
	retryWindowMinDays = 8
	retryWindowMaxDays = 2
	maxHandleNoteRunes = 512
)

// SyncChannelTxnResult 是调用 PaymentService.SyncChannelTxn 的结果。
type SyncChannelTxnResult struct {
	LocalStatus   string
	ChannelStatus string
	Changed       bool
}

// ReplayEventResult 是调用 PaymentService.ReplayEvent 的结果。
type ReplayEventResult struct {
	EventID string
	Rebuilt bool
}

// PaymentClient 抽象对 payment 服务的 RPC 调用（禁止直接 import service/payment 包）。
type PaymentClient interface {
	SyncChannelTxn(ctx context.Context, bizNo string) (SyncChannelTxnResult, error)
	ReplayEvent(ctx context.Context, bizNo, eventType string) (ReplayEventResult, error)
}

type kitexPaymentClient struct {
	cli paymentservice.Client
}

// NewPaymentRPCClient 将 Kitex paymentservice.Client 适配为 PaymentClient。
func NewPaymentRPCClient(cli paymentservice.Client) PaymentClient {
	return kitexPaymentClient{cli: cli}
}

func (c kitexPaymentClient) SyncChannelTxn(ctx context.Context, bizNo string) (SyncChannelTxnResult, error) {
	if c.cli == nil {
		return SyncChannelTxnResult{}, errors.New("payment rpc client is nil")
	}
	cctx, cancel := context.WithTimeout(ctx, rpcCallTimeout)
	defer cancel()
	resp, err := c.cli.SyncChannelTxn(cctx, &payment.SyncChannelTxnRequest{BizNo: bizNo})
	if err != nil {
		return SyncChannelTxnResult{}, err
	}
	if resp == nil {
		return SyncChannelTxnResult{}, errors.New("nil SyncChannelTxnResponse")
	}
	return SyncChannelTxnResult{
		LocalStatus:   resp.GetLocalStatus(),
		ChannelStatus: resp.GetChannelStatus(),
		Changed:       resp.GetChanged(),
	}, nil
}

func (c kitexPaymentClient) ReplayEvent(ctx context.Context, bizNo, eventType string) (ReplayEventResult, error) {
	if c.cli == nil {
		return ReplayEventResult{}, errors.New("payment rpc client is nil")
	}
	cctx, cancel := context.WithTimeout(ctx, rpcCallTimeout)
	defer cancel()
	req := &payment.ReplayEventRequest{
		BizNo:     &bizNo,
		EventType: &eventType,
	}
	resp, err := c.cli.ReplayEvent(cctx, req)
	if err != nil {
		return ReplayEventResult{}, err
	}
	if resp == nil {
		return ReplayEventResult{}, errors.New("nil ReplayEventResponse")
	}
	return ReplayEventResult{
		EventID: resp.GetEventId(),
		Rebuilt: resp.GetRebuilt(),
	}, nil
}

// AutoHandleStore 抽象自动处理在 pay_recon 库的读写操作。
type AutoHandleStore interface {
	ListPendingUnhandledDiffsByBatch(ctx context.Context, batchNo string) ([]dal.Diff, error)
	ListRetryableRequeryDiffs(ctx context.Context, fromDate, toDate string) ([]dal.Diff, error)
	ListRetryableReplayLedgerDiffs(ctx context.Context, fromDate, toDate string) ([]dal.Diff, error)
	GetDiffByID(ctx context.Context, id uint64) (dal.Diff, error)
	ResolveDiffCAS(ctx context.Context, id uint64, status, handleAction, handler, handleNote string, resolvedAt, now time.Time) (bool, error)
	UpdateDiffPendingCAS(ctx context.Context, id uint64, handleAction, handler, handleNote string, channelStatus *string, now time.Time) (bool, error)
}

// AutoHandleOrderReader 抽象对 pay_payment 订单表的按单号只读查询（用于 LEDGER_MISSING 复核）。
type AutoHandleOrderReader interface {
	ListPaymentOrdersByPayNos(ctx context.Context, payNos []string) ([]dal.InternalPaymentOrder, error)
	ListRefundOrdersByRefundNos(ctx context.Context, refundNos []string) ([]dal.InternalRefundOrder, error)
}

type sqlAutoHandleStore struct {
	db *sqlx.DB
}

func (s sqlAutoHandleStore) ListPendingUnhandledDiffsByBatch(ctx context.Context, batchNo string) ([]dal.Diff, error) {
	return dal.ListPendingUnhandledDiffsByBatch(ctx, s.db, batchNo)
}

func (s sqlAutoHandleStore) ListRetryableRequeryDiffs(ctx context.Context, fromDate, toDate string) ([]dal.Diff, error) {
	return dal.ListRetryableRequeryDiffs(ctx, s.db, fromDate, toDate)
}

func (s sqlAutoHandleStore) ListRetryableReplayLedgerDiffs(ctx context.Context, fromDate, toDate string) ([]dal.Diff, error) {
	return dal.ListRetryableReplayLedgerDiffs(ctx, s.db, fromDate, toDate)
}

func (s sqlAutoHandleStore) GetDiffByID(ctx context.Context, id uint64) (dal.Diff, error) {
	return dal.GetDiffByID(ctx, s.db, id)
}

func (s sqlAutoHandleStore) ResolveDiffCAS(ctx context.Context, id uint64, status, handleAction, handler, handleNote string, resolvedAt, now time.Time) (bool, error) {
	return dal.ResolveDiffCAS(ctx, s.db, id, status, handleAction, handler, handleNote, resolvedAt, now)
}

func (s sqlAutoHandleStore) UpdateDiffPendingCAS(ctx context.Context, id uint64, handleAction, handler, handleNote string, channelStatus *string, now time.Time) (bool, error) {
	return dal.UpdateDiffPendingCAS(ctx, s.db, id, handleAction, handler, handleNote, channelStatus, now)
}

type sqlAutoHandleOrderReader struct {
	db *sqlx.DB
}

func (r sqlAutoHandleOrderReader) ListPaymentOrdersByPayNos(ctx context.Context, payNos []string) ([]dal.InternalPaymentOrder, error) {
	return dal.ListPaymentOrdersByPayNos(ctx, r.db, payNos)
}

func (r sqlAutoHandleOrderReader) ListRefundOrdersByRefundNos(ctx context.Context, refundNos []string) ([]dal.InternalRefundOrder, error) {
	return dal.ListRefundOrdersByRefundNos(ctx, r.db, refundNos)
}

// AutoHandler 实现 REC-06 差错自动处理与跨批次重试复核（CF-29、CF-41）。
type AutoHandler struct {
	store     AutoHandleStore
	orders    AutoHandleOrderReader
	shards    [2]AccountShardReader
	payClient PaymentClient
	clk       clock.Clock
}

// NewAutoHandler 创建基于 MySQL 与 Kitex 客户端的差错自动处理器。
func NewAutoHandler(reconDB, paymentDB, accountDB0, accountDB1 *sqlx.DB, payClient PaymentClient, clk clock.Clock) *AutoHandler {
	return NewAutoHandlerWithDeps(
		sqlAutoHandleStore{db: reconDB},
		sqlAutoHandleOrderReader{db: paymentDB},
		[2]AccountShardReader{
			sqlAccountShardReader{db: accountDB0},
			sqlAccountShardReader{db: accountDB1},
		},
		payClient,
		clk,
	)
}

// NewAutoHandlerWithDeps 创建可注入依赖的差错自动处理器（便于单元测试）。
func NewAutoHandlerWithDeps(store AutoHandleStore, orders AutoHandleOrderReader, shards [2]AccountShardReader, payClient PaymentClient, clk clock.Clock) *AutoHandler {
	if clk == nil {
		clk = clock.Real{}
	}
	return &AutoHandler{
		store:     store,
		orders:    orders,
		shards:    shards,
		payClient: payClient,
		clk:       clk,
	}
}

// Check 先执行自动重试窗口 [D-8, D-2] 内的跨批次重试与复核（CF-41），
// 再执行本批次新插入差错的自动处理（CF-29）。
func (h *AutoHandler) Check(ctx context.Context, batch *dal.Batch) (CheckResult, error) {
	if batch == nil {
		return CheckResult{}, errors.New("auto handle: nil batch")
	}
	if h.store == nil || h.orders == nil || h.shards[0] == nil || h.shards[1] == nil || h.payClient == nil {
		return CheckResult{}, errors.New("auto handle: uninitialized dependencies")
	}

	if err := h.runCrossBatchRetry(ctx); err != nil {
		return CheckResult{}, err
	}
	if err := h.runBatchNewDiffs(ctx, batch.BatchNo); err != nil {
		return CheckResult{}, err
	}
	return CheckResult{}, nil
}

func (h *AutoHandler) now() time.Time {
	return h.clk.Now().In(shanghai).Truncate(time.Millisecond)
}

func (h *AutoHandler) retryWindow() (string, string) {
	now := h.now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, shanghai)
	fromDate := today.AddDate(0, 0, -retryWindowMinDays).Format("2006-01-02")
	toDate := today.AddDate(0, 0, -retryWindowMaxDays).Format("2006-01-02")
	return fromDate, toDate
}

// runCrossBatchRetry 执行 CF-41：跨批次 REQUERY 重试与 LEDGER_MISSING 复核。
func (h *AutoHandler) runCrossBatchRetry(ctx context.Context) error {
	fromDate, toDate := h.retryWindow()

	requeries, err := h.store.ListRetryableRequeryDiffs(ctx, fromDate, toDate)
	if err != nil {
		return err
	}
	for _, d := range requeries {
		if err := h.handleRequeryDiff(ctx, d); err != nil {
			return err
		}
	}

	replays, err := h.store.ListRetryableReplayLedgerDiffs(ctx, fromDate, toDate)
	if err != nil {
		return err
	}
	if len(replays) == 0 {
		return nil
	}
	return h.recheckLedgerMissingDiffs(ctx, replays)
}

// runBatchNewDiffs 执行 CF-29：处理本批次新插入且 handle_action 为空的差错。
func (h *AutoHandler) runBatchNewDiffs(ctx context.Context, batchNo string) error {
	diffs, err := h.store.ListPendingUnhandledDiffsByBatch(ctx, batchNo)
	if err != nil {
		return err
	}
	for _, d := range diffs {
		switch d.DiffType {
		case dal.DiffStatusMismatch:
			if derefPtr(d.LocalStatus) == statusProcessing {
				if err := h.handleRequeryDiff(ctx, d); err != nil {
					return err
				}
			} else {
				h.logManualDiff(ctx, d)
			}
		case dal.DiffChannelMissing:
			if err := h.handleRequeryDiff(ctx, d); err != nil {
				return err
			}
		case dal.DiffLedgerMissing:
			if err := h.handleNewLedgerMissing(ctx, d); err != nil {
				return err
			}
		default:
			// LOCAL_MISSING、AMOUNT_MISMATCH、TRANSFER_MISMATCH 无自动处理
			h.logManualDiff(ctx, d)
		}
	}
	return nil
}

func (h *AutoHandler) handleRequeryDiff(ctx context.Context, d dal.Diff) error {
	resp, err := h.payClient.SyncChannelTxn(ctx, d.BizNo)
	if err != nil {
		note := truncateNote(fmt.Sprintf("查单同步失败: %v", err))
		_, uerr := h.updatePendingWithCAS(ctx, d, actionRequery, note, nil)
		return uerr
	}

	if d.DiffType == dal.DiffChannelMissing {
		if resp.ChannelStatus == channelStatusSuccess {
			_, rerr := h.resolveWithCAS(ctx, d, dal.DiffStatusIgnored, actionRequery, "渠道账单遗漏，查单确认成功")
			return rerr
		}
		// 严重不一致：写 channel_status，不再自动重试
		note := truncateNote(fmt.Sprintf("严重不一致: 渠道状态为 %s", resp.ChannelStatus))
		cs := resp.ChannelStatus
		won, uerr := h.updatePendingWithCAS(ctx, d, actionRequery, note, &cs)
		if uerr != nil {
			return uerr
		}
		if won {
			slog.ErrorContext(ctx, "recon channel inconsistent",
				"diff_no", d.DiffNo,
				"biz_no", d.BizNo,
				"local_status", derefPtr(d.LocalStatus),
				"channel_status", resp.ChannelStatus,
			)
			obs.AddReconChannelInconsistent(d.Channel)
		}
		return nil
	}

	// STATUS_MISMATCH（本地 PROCESSING）
	if resp.LocalStatus != statusProcessing {
		note := truncateNote(fmt.Sprintf("同步渠道状态成功: local=%s, channel=%s", resp.LocalStatus, resp.ChannelStatus))
		_, rerr := h.resolveWithCAS(ctx, d, dal.DiffStatusResolved, actionRequery, note)
		return rerr
	}
	note := truncateNote(fmt.Sprintf("渠道尚未终态: local=%s, channel=%s", resp.LocalStatus, resp.ChannelStatus))
	_, uerr := h.updatePendingWithCAS(ctx, d, actionRequery, note, nil)
	return uerr
}

func (h *AutoHandler) handleNewLedgerMissing(ctx context.Context, d dal.Diff) error {
	eventType, ok := ledgerEventType(d.TxnType)
	if !ok {
		h.logManualDiff(ctx, d)
		return nil
	}
	_, err := h.payClient.ReplayEvent(ctx, d.BizNo, eventType)
	var note string
	if err != nil {
		note = truncateNote(fmt.Sprintf("事件重放失败(%s): %v", eventType, err))
	} else {
		note = truncateNote(fmt.Sprintf("已触发事件重放(%s)，等待下批次复核", eventType))
	}
	_, uerr := h.updatePendingWithCAS(ctx, d, actionReplay, note, nil)
	return uerr
}

func (h *AutoHandler) recheckLedgerMissingDiffs(ctx context.Context, diffs []dal.Diff) error {
	var payNos, refundNos []string
	for _, d := range diffs {
		switch d.TxnType {
		case txnTypePay:
			payNos = append(payNos, d.BizNo)
		case txnTypeRefund:
			refundNos = append(refundNos, d.BizNo)
		}
	}

	payments, err := h.orders.ListPaymentOrdersByPayNos(ctx, payNos)
	if err != nil {
		return err
	}
	refunds, err := h.orders.ListRefundOrdersByRefundNos(ctx, refundNos)
	if err != nil {
		return err
	}

	type orderTarget struct {
		shard  int
		amount int64
	}
	targets := make(map[ledgerKey]orderTarget, len(payments)+len(refunds))
	var shardBizNos [2][]string
	for _, p := range payments {
		s, serr := shard.Of(p.MerchantID)
		if serr != nil {
			continue
		}
		targets[ledgerKey{bizType: txnTypePay, bizNo: p.PayNo}] = orderTarget{shard: s, amount: p.Amount}
		shardBizNos[s] = append(shardBizNos[s], p.PayNo)
	}
	for _, r := range refunds {
		s, serr := shard.Of(r.MerchantID)
		if serr != nil {
			continue
		}
		targets[ledgerKey{bizType: txnTypeRefund, bizNo: r.RefundNo}] = orderTarget{shard: s, amount: r.Amount}
		shardBizNos[s] = append(shardBizNos[s], r.RefundNo)
	}

	var shardLedgers [2]map[ledgerKey]dal.AccountLedgerTxn
	for s := range 2 {
		shardLedgers[s] = make(map[ledgerKey]dal.AccountLedgerTxn)
		if len(shardBizNos[s]) == 0 {
			continue
		}
		rows, lerr := h.shards[s].ListLedgerTxnsByBizNos(ctx, []string{txnTypePay, txnTypeRefund}, shardBizNos[s])
		if lerr != nil {
			return lerr
		}
		for _, row := range rows {
			shardLedgers[s][ledgerKey{bizType: row.BizType, bizNo: row.BizNo}] = row
		}
	}

	for _, d := range diffs {
		key := ledgerKey{bizType: d.TxnType, bizNo: d.BizNo}
		tgt, ok := targets[key]
		if ok {
			if lt, exists := shardLedgers[tgt.shard][key]; exists && lt.Amount == tgt.amount {
				if _, rerr := h.resolveWithCAS(ctx, d, dal.DiffStatusResolved, actionReplay, "重放后复核通过"); rerr != nil {
					return rerr
				}
				continue
			}
		}

		eventType, valid := ledgerEventType(d.TxnType)
		if !valid {
			continue
		}
		_, rerr := h.payClient.ReplayEvent(ctx, d.BizNo, eventType)
		var note string
		if rerr != nil {
			note = truncateNote(fmt.Sprintf("复核凭证仍缺失，再次重放失败(%s): %v", eventType, rerr))
		} else {
			note = truncateNote(fmt.Sprintf("复核凭证仍缺失，已再次重放(%s)", eventType))
		}
		if _, uerr := h.updatePendingWithCAS(ctx, d, actionReplay, note, nil); uerr != nil {
			return uerr
		}
	}
	return nil
}

func (h *AutoHandler) resolveWithCAS(ctx context.Context, d dal.Diff, status, action, note string) (bool, error) {
	now := h.now()
	won, err := h.store.ResolveDiffCAS(ctx, d.ID, status, action, handlerSystem, note, now, now)
	if err != nil {
		return false, err
	}
	if won {
		return true, nil
	}
	return false, h.handleCASMiss(ctx, d)
}

func (h *AutoHandler) updatePendingWithCAS(ctx context.Context, d dal.Diff, action, note string, channelStatus *string) (bool, error) {
	now := h.now()
	won, err := h.store.UpdateDiffPendingCAS(ctx, d.ID, action, handlerSystem, note, channelStatus, now)
	if err != nil {
		return false, err
	}
	if won {
		return true, nil
	}
	return false, h.handleCASMiss(ctx, d)
}

func (h *AutoHandler) handleCASMiss(ctx context.Context, d dal.Diff) error {
	_, err := h.store.GetDiffByID(ctx, d.ID)
	if err != nil {
		if errors.Is(err, dal.ErrNotFound) {
			slog.WarnContext(ctx, "recon diff no longer exists", "diff_no", d.DiffNo, "batch_no", d.BatchNo)
			return nil
		}
		return err
	}
	// 已被其他运行处理，保持现有状态，不覆盖
	return nil
}

func (h *AutoHandler) logManualDiff(ctx context.Context, d dal.Diff) {
	slog.ErrorContext(ctx, "recon diff requires manual attention",
		"diff_no", d.DiffNo,
		"batch_no", d.BatchNo,
		"diff_type", d.DiffType,
		"txn_type", d.TxnType,
		"biz_no", d.BizNo,
		"local_status", derefPtr(d.LocalStatus),
		"channel_status", derefPtr(d.ChannelStatus),
	)
}

func ledgerEventType(txnType string) (string, bool) {
	switch txnType {
	case txnTypePay:
		return outbox.EventPaymentSucceeded, true
	case txnTypeRefund:
		return outbox.EventRefundSucceeded, true
	default:
		return "", false
	}
}

func derefPtr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func truncateNote(s string) string {
	if utf8.RuneCountInString(s) <= maxHandleNoteRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxHandleNoteRunes])
}
