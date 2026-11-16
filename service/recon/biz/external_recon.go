package biz

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/service/recon/dal"
)

const (
	diffNoPrefix         = "RD"
	channelStatusSuccess = "SUCCESS"
)

// ExternalReconStore 抽象外部对账在 pay_recon 库的读写操作。
type ExternalReconStore interface {
	ListChannelBillsByBatch(ctx context.Context, batchNo string) ([]dal.ChannelBill, error)
	SaveExternalCheckResult(ctx context.Context, matchedBillIDs, diffBillIDs []uint64, diffs []dal.Diff) (map[string]int, error)
}

// PaymentTxnReader 抽象对 pay_payment.channel_txn 的只读查询。
type PaymentTxnReader interface {
	ListSuccessChannelTxnsByDate(ctx context.Context, channel string, dayStart, dayEnd time.Time) ([]dal.ChannelTxn, error)
	ListChannelTxnsByBizNos(ctx context.Context, channel string, bizNos []string) ([]dal.ChannelTxn, error)
}

type sqlExternalStore struct {
	db *sqlx.DB
}

func (s sqlExternalStore) ListChannelBillsByBatch(ctx context.Context, batchNo string) ([]dal.ChannelBill, error) {
	return dal.ListChannelBillsByBatch(ctx, s.db, batchNo)
}

func (s sqlExternalStore) SaveExternalCheckResult(ctx context.Context, matchedBillIDs, diffBillIDs []uint64, diffs []dal.Diff) (map[string]int, error) {
	return dal.SaveExternalCheckResultTx(ctx, s.db, matchedBillIDs, diffBillIDs, diffs)
}

type sqlPaymentTxnReader struct {
	db *sqlx.DB
}

func (r sqlPaymentTxnReader) ListSuccessChannelTxnsByDate(ctx context.Context, channel string, dayStart, dayEnd time.Time) ([]dal.ChannelTxn, error) {
	return dal.ListSuccessChannelTxnsByDate(ctx, r.db, channel, dayStart, dayEnd)
}

func (r sqlPaymentTxnReader) ListChannelTxnsByBizNos(ctx context.Context, channel string, bizNos []string) ([]dal.ChannelTxn, error) {
	return dal.ListChannelTxnsByBizNos(ctx, r.db, channel, bizNos)
}

type txnKey struct {
	txnType string
	bizNo   string
}

// ExternalChecker 实现 REC-03 外部对账比对（按单号）。
type ExternalChecker struct {
	store ExternalReconStore
	txns  PaymentTxnReader
	ids   IDGen
	clk   clock.Clock
}

// NewExternalChecker 创建基于 MySQL 的外部对账比对器。
func NewExternalChecker(reconDB, paymentDB *sqlx.DB, ids IDGen, clk clock.Clock) *ExternalChecker {
	return NewExternalCheckerWithDeps(
		sqlExternalStore{db: reconDB},
		sqlPaymentTxnReader{db: paymentDB},
		ids,
		clk,
	)
}

// NewExternalCheckerWithDeps 创建可注入依赖的外部对账比对器（便于单元测试）。
func NewExternalCheckerWithDeps(store ExternalReconStore, txns PaymentTxnReader, ids IDGen, clk clock.Clock) *ExternalChecker {
	if clk == nil {
		clk = clock.Real{}
	}
	return &ExternalChecker{
		store: store,
		txns:  txns,
		ids:   ids,
		clk:   clk,
	}
}

// Check 执行外部对账比对：对比本批次账单行（B）与本地 channel_txn（L 及按单号查询结果），
// 更新账单 match_status 并按 uk_batch_type_biz_no_diff_type 幂等插入差错。
func (c *ExternalChecker) Check(ctx context.Context, batch *dal.Batch) (CheckResult, error) {
	if batch == nil {
		return CheckResult{}, errors.New("external recon: nil batch")
	}
	if c.store == nil || c.txns == nil || c.ids == nil {
		return CheckResult{}, errors.New("external recon: uninitialized dependencies")
	}

	dayStart, err := time.ParseInLocation("2006-01-02", batch.BillDate, shanghai)
	if err != nil {
		return CheckResult{}, fmt.Errorf("external recon: parse bill_date %q: %w", batch.BillDate, err)
	}
	dayEnd := dayStart.AddDate(0, 0, 1)

	bills, err := c.store.ListChannelBillsByBatch(ctx, batch.BatchNo)
	if err != nil {
		return CheckResult{}, err
	}

	localDayTxns, err := c.txns.ListSuccessChannelTxnsByDate(ctx, batch.Channel, dayStart, dayEnd)
	if err != nil {
		return CheckResult{}, err
	}

	bizNos := collectUniqueBizNos(bills)
	localByBiz, err := c.txns.ListChannelTxnsByBizNos(ctx, batch.Channel, bizNos)
	if err != nil {
		return CheckResult{}, err
	}

	localMap := make(map[txnKey]dal.ChannelTxn, len(localByBiz))
	for _, lt := range localByBiz {
		localMap[txnKey{txnType: lt.TxnType, bizNo: lt.BizNo}] = lt
	}

	now := c.clk.Now().In(shanghai).Truncate(time.Millisecond)
	matchedLocalKeys := make(map[txnKey]struct{}, len(bills))

	var (
		channelCount   int32
		channelAmount  int64
		matchedCount   int32
		matchedBillIDs []uint64
		diffBillIDs    []uint64
		diffs          []dal.Diff
	)

	for i := range bills {
		b := &bills[i]
		channelCount++
		channelAmount += b.Amount

		key := txnKey{txnType: b.TxnType, bizNo: b.BizNo}
		lt, exists := localMap[key]
		if !exists {
			diffBillIDs = append(diffBillIDs, b.ID)
			diffs = append(diffs, newBillSideDiff(batch, b, dal.DiffLocalMissing, nil, now))
			continue
		}

		matchedLocalKeys[key] = struct{}{}
		if lt.Status != channelStatusSuccess {
			diffBillIDs = append(diffBillIDs, b.ID)
			diffs = append(diffs, newBillSideDiff(batch, b, dal.DiffStatusMismatch, &lt, now))
			continue
		}
		if lt.Amount != b.Amount {
			diffBillIDs = append(diffBillIDs, b.ID)
			diffs = append(diffs, newBillSideDiff(batch, b, dal.DiffAmountMismatch, &lt, now))
			continue
		}

		matchedCount++
		matchedBillIDs = append(matchedBillIDs, b.ID)
	}

	var (
		localCount  int32
		localAmount int64
	)
	for i := range localDayTxns {
		lt := &localDayTxns[i]
		localCount++
		localAmount += lt.Amount

		key := txnKey{txnType: lt.TxnType, bizNo: lt.BizNo}
		if _, seen := matchedLocalKeys[key]; !seen {
			diffs = append(diffs, newChannelMissingDiff(batch, lt, now))
		}
	}

	for i := range diffs {
		diffNo, idErr := c.ids.NewNo(ctx, diffNoPrefix)
		if idErr != nil {
			return CheckResult{}, fmt.Errorf("external recon: generate diff_no: %w", idErr)
		}
		diffs[i].DiffNo = diffNo
	}

	insertedByType, err := c.store.SaveExternalCheckResult(ctx, matchedBillIDs, diffBillIDs, diffs)
	if err != nil {
		return CheckResult{}, err
	}
	for diffType, n := range insertedByType {
		for range n {
			obs.AddReconDiff(diffType)
		}
	}

	return CheckResult{
		ChannelCount:  channelCount,
		ChannelAmount: channelAmount,
		LocalCount:    localCount,
		LocalAmount:   localAmount,
		MatchedCount:  matchedCount,
	}, nil
}

func collectUniqueBizNos(bills []dal.ChannelBill) []string {
	if len(bills) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(bills))
	out := make([]string, 0, len(bills))
	for _, b := range bills {
		if _, ok := seen[b.BizNo]; !ok {
			seen[b.BizNo] = struct{}{}
			out = append(out, b.BizNo)
		}
	}
	return out
}

func newBillSideDiff(batch *dal.Batch, b *dal.ChannelBill, diffType string, lt *dal.ChannelTxn, now time.Time) dal.Diff {
	chTxnNo := b.ChannelTxnNo
	chAmt := b.Amount
	chSt := b.ChannelStatus
	d := dal.Diff{
		BatchNo:       batch.BatchNo,
		Channel:       batch.Channel,
		BillDate:      batch.BillDate,
		DiffType:      diffType,
		TxnType:       b.TxnType,
		BizNo:         b.BizNo,
		ChannelTxnNo:  &chTxnNo,
		ChannelAmount: &chAmt,
		ChannelStatus: &chSt,
		Status:        dal.DiffStatusPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if lt != nil {
		localAmt := lt.Amount
		localSt := lt.Status
		d.LocalAmount = &localAmt
		d.LocalStatus = &localSt
	}
	return d
}

func newChannelMissingDiff(batch *dal.Batch, lt *dal.ChannelTxn, now time.Time) dal.Diff {
	localAmt := lt.Amount
	localSt := lt.Status
	return dal.Diff{
		BatchNo:      batch.BatchNo,
		Channel:      batch.Channel,
		BillDate:     batch.BillDate,
		DiffType:     dal.DiffChannelMissing,
		TxnType:      lt.TxnType,
		BizNo:        lt.BizNo,
		ChannelTxnNo: lt.ChannelTxnNo,
		LocalAmount:  &localAmt,
		LocalStatus:  &localSt,
		Status:       dal.DiffStatusPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}
