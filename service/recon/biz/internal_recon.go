package biz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/recon/dal"
)

const (
	transferChannel = "mockpay"
	txnTypePay      = "PAY"
	txnTypeRefund   = "REFUND"
	txnTypeTransfer = "TRANSFER"
	modeLocal       = "LOCAL"
	modeTCC         = "TCC"
	statusSuccess   = "SUCCESS"
	statusFailed    = "FAILED"
	statusConfirmed = "CONFIRMED"
	statusReleased  = "RELEASED"
	statusCancelled = "CANCELLED"
	rolePayee       = "PAYEE"
)

// InternalReconStore 抽象内部对账在 pay_recon 库的写入操作。
type InternalReconStore interface {
	InsertInternalDiffs(ctx context.Context, diffs []dal.Diff) (map[string]int, error)
}

// InternalPaymentReader 抽象对 pay_payment 中订单与转账单的只读查询。
type InternalPaymentReader interface {
	ListSuccessPaymentOrdersByDate(ctx context.Context, channel string, dayStart, dayEnd time.Time) ([]dal.InternalPaymentOrder, error)
	ListSuccessRefundOrdersByDate(ctx context.Context, channel string, dayStart, dayEnd time.Time) ([]dal.InternalRefundOrder, error)
	ListFinishedTransferOrdersByDate(ctx context.Context, dayStart, dayEnd time.Time) ([]dal.InternalTransferOrder, error)
}

// AccountShardReader 抽象对单个账户分片（pay_account_0 / pay_account_1）的只读查询。
type AccountShardReader interface {
	ListLedgerTxnsByBizNos(ctx context.Context, bizTypes, bizNos []string) ([]dal.AccountLedgerTxn, error)
	ListAccountFreezesByBizNos(ctx context.Context, bizType string, bizNos []string) ([]dal.AccountFreezeRecord, error)
	ListTransferBranchesByTransferNos(ctx context.Context, transferNos []string) ([]dal.AccountTransferBranch, error)
}

type sqlInternalStore struct {
	db *sqlx.DB
}

func (s sqlInternalStore) InsertInternalDiffs(ctx context.Context, diffs []dal.Diff) (map[string]int, error) {
	return dal.InsertDiffsSkipDuplicate(ctx, s.db, diffs)
}

type sqlInternalPaymentReader struct {
	db *sqlx.DB
}

func (r sqlInternalPaymentReader) ListSuccessPaymentOrdersByDate(ctx context.Context, channel string, dayStart, dayEnd time.Time) ([]dal.InternalPaymentOrder, error) {
	return dal.ListSuccessPaymentOrdersByDate(ctx, r.db, channel, dayStart, dayEnd)
}

func (r sqlInternalPaymentReader) ListSuccessRefundOrdersByDate(ctx context.Context, channel string, dayStart, dayEnd time.Time) ([]dal.InternalRefundOrder, error) {
	return dal.ListSuccessRefundOrdersByDate(ctx, r.db, channel, dayStart, dayEnd)
}

func (r sqlInternalPaymentReader) ListFinishedTransferOrdersByDate(ctx context.Context, dayStart, dayEnd time.Time) ([]dal.InternalTransferOrder, error) {
	return dal.ListFinishedTransferOrdersByDate(ctx, r.db, dayStart, dayEnd)
}

type sqlAccountShardReader struct {
	db *sqlx.DB
}

func (r sqlAccountShardReader) ListLedgerTxnsByBizNos(ctx context.Context, bizTypes, bizNos []string) ([]dal.AccountLedgerTxn, error) {
	return dal.ListLedgerTxnsByBizNos(ctx, r.db, bizTypes, bizNos)
}

func (r sqlAccountShardReader) ListAccountFreezesByBizNos(ctx context.Context, bizType string, bizNos []string) ([]dal.AccountFreezeRecord, error) {
	return dal.ListAccountFreezesByBizNos(ctx, r.db, bizType, bizNos)
}

func (r sqlAccountShardReader) ListTransferBranchesByTransferNos(ctx context.Context, transferNos []string) ([]dal.AccountTransferBranch, error) {
	return dal.ListTransferBranchesByTransferNos(ctx, r.db, transferNos)
}

// InternalChecker 实现 REC-05 内部对账（支付、退款、转账）。
type InternalChecker struct {
	store  InternalReconStore
	pay    InternalPaymentReader
	shards [2]AccountShardReader
	ids    IDGen
	clk    clock.Clock
}

// NewInternalChecker 创建基于 MySQL 只读 DSN 的内部对账比对器。
func NewInternalChecker(reconDB, paymentDB, accountDB0, accountDB1 *sqlx.DB, ids IDGen, clk clock.Clock) *InternalChecker {
	return NewInternalCheckerWithDeps(
		sqlInternalStore{db: reconDB},
		sqlInternalPaymentReader{db: paymentDB},
		[2]AccountShardReader{
			sqlAccountShardReader{db: accountDB0},
			sqlAccountShardReader{db: accountDB1},
		},
		ids,
		clk,
	)
}

// NewInternalCheckerWithDeps 创建可注入依赖的内部对账比对器（便于单元测试）。
func NewInternalCheckerWithDeps(store InternalReconStore, pay InternalPaymentReader, shards [2]AccountShardReader, ids IDGen, clk clock.Clock) *InternalChecker {
	if clk == nil {
		clk = clock.Real{}
	}
	return &InternalChecker{
		store:  store,
		pay:    pay,
		shards: shards,
		ids:    ids,
		clk:    clk,
	}
}

type ledgerOrderItem struct {
	txnType    string
	bizNo      string
	merchantID string
	amount     int64
	status     string
}

type ledgerKey struct {
	bizType string
	bizNo   string
}

// Check 执行内部对账：分别从 pay_payment 与两个 pay_account 分片读取数据并在内存中比对。
func (c *InternalChecker) Check(ctx context.Context, batch *dal.Batch) (CheckResult, error) {
	if batch == nil {
		return CheckResult{}, errors.New("internal recon: nil batch")
	}
	if c.store == nil || c.pay == nil || c.shards[0] == nil || c.shards[1] == nil || c.ids == nil {
		return CheckResult{}, errors.New("internal recon: uninitialized dependencies")
	}

	dayStart, err := time.ParseInLocation("2006-01-02", batch.BillDate, shanghai)
	if err != nil {
		return CheckResult{}, fmt.Errorf("internal recon: parse bill_date %q: %w", batch.BillDate, err)
	}
	dayEnd := dayStart.AddDate(0, 0, 1)
	now := c.clk.Now().In(shanghai).Truncate(time.Millisecond)

	var diffs []dal.Diff

	payDiffs, err := c.checkPaymentsAndRefunds(ctx, batch, dayStart, dayEnd, now)
	if err != nil {
		return CheckResult{}, err
	}
	diffs = append(diffs, payDiffs...)

	if batch.Channel == transferChannel {
		trfDiffs, err := c.checkTransfers(ctx, batch, dayStart, dayEnd, now)
		if err != nil {
			return CheckResult{}, err
		}
		diffs = append(diffs, trfDiffs...)
	}

	if len(diffs) == 0 {
		return CheckResult{}, nil
	}

	for i := range diffs {
		diffNo, idErr := c.ids.NewNo(ctx, diffNoPrefix)
		if idErr != nil {
			return CheckResult{}, fmt.Errorf("internal recon: generate diff_no: %w", idErr)
		}
		diffs[i].DiffNo = diffNo
	}

	insertedByType, err := c.store.InsertInternalDiffs(ctx, diffs)
	if err != nil {
		return CheckResult{}, err
	}
	for diffType, n := range insertedByType {
		for range n {
			obs.AddReconDiff(diffType)
		}
	}
	return CheckResult{}, nil
}

func (c *InternalChecker) checkPaymentsAndRefunds(ctx context.Context, batch *dal.Batch, dayStart, dayEnd, now time.Time) ([]dal.Diff, error) {
	payments, err := c.pay.ListSuccessPaymentOrdersByDate(ctx, batch.Channel, dayStart, dayEnd)
	if err != nil {
		return nil, err
	}
	refunds, err := c.pay.ListSuccessRefundOrdersByDate(ctx, batch.Channel, dayStart, dayEnd)
	if err != nil {
		return nil, err
	}

	items := make([]ledgerOrderItem, 0, len(payments)+len(refunds))
	var shardBizNos [2][]string
	for _, p := range payments {
		s, serr := shard.Of(p.MerchantID)
		if serr != nil {
			return nil, fmt.Errorf("internal recon: payment %s shard: %w", p.PayNo, serr)
		}
		items = append(items, ledgerOrderItem{
			txnType: txnTypePay, bizNo: p.PayNo, merchantID: p.MerchantID, amount: p.Amount, status: p.Status,
		})
		shardBizNos[s] = append(shardBizNos[s], p.PayNo)
	}
	for _, r := range refunds {
		s, serr := shard.Of(r.MerchantID)
		if serr != nil {
			return nil, fmt.Errorf("internal recon: refund %s shard: %w", r.RefundNo, serr)
		}
		items = append(items, ledgerOrderItem{
			txnType: txnTypeRefund, bizNo: r.RefundNo, merchantID: r.MerchantID, amount: r.Amount, status: r.Status,
		})
		shardBizNos[s] = append(shardBizNos[s], r.RefundNo)
	}
	if len(items) == 0 {
		return nil, nil
	}

	var shardLedgers [2]map[ledgerKey]dal.AccountLedgerTxn
	for s := range 2 {
		shardLedgers[s] = make(map[ledgerKey]dal.AccountLedgerTxn)
		if len(shardBizNos[s]) == 0 {
			continue
		}
		rows, lerr := c.shards[s].ListLedgerTxnsByBizNos(ctx, []string{txnTypePay, txnTypeRefund}, shardBizNos[s])
		if lerr != nil {
			return nil, lerr
		}
		for _, row := range rows {
			shardLedgers[s][ledgerKey{bizType: row.BizType, bizNo: row.BizNo}] = row
		}
	}

	var diffs []dal.Diff
	for _, it := range items {
		s, _ := shard.Of(it.merchantID)
		lt, ok := shardLedgers[s][ledgerKey{bizType: it.txnType, bizNo: it.bizNo}]
		if ok && lt.Amount == it.amount {
			continue
		}
		amt := it.amount
		st := it.status
		d := dal.Diff{
			BatchNo:     batch.BatchNo,
			Channel:     batch.Channel,
			BillDate:    batch.BillDate,
			DiffType:    dal.DiffLedgerMissing,
			TxnType:     it.txnType,
			BizNo:       it.bizNo,
			LocalAmount: &amt,
			LocalStatus: &st,
			Status:      dal.DiffStatusPending,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if ok && lt.Amount != it.amount {
			note := fmt.Sprintf("订单金额=%d, 凭证金额=%d (shard=%d)", it.amount, lt.Amount, s)
			d.HandleNote = &note
		}
		diffs = append(diffs, d)
	}
	return diffs, nil
}

type transferShardSnapshot struct {
	ledgers [2][]dal.AccountLedgerTxn
	freezes [2]*dal.AccountFreezeRecord
	branch  [2]*dal.AccountTransferBranch
}

func (c *InternalChecker) checkTransfers(ctx context.Context, batch *dal.Batch, dayStart, dayEnd, now time.Time) ([]dal.Diff, error) {
	orders, err := c.pay.ListFinishedTransferOrdersByDate(ctx, dayStart, dayEnd)
	if err != nil {
		return nil, err
	}
	if len(orders) == 0 {
		return nil, nil
	}

	transferNos := make([]string, len(orders))
	for i, o := range orders {
		transferNos[i] = o.TransferNo
	}

	snapshots := make(map[string]*transferShardSnapshot, len(orders))
	for _, no := range transferNos {
		snapshots[no] = &transferShardSnapshot{}
	}

	ledgerTypes := []string{txnTypeTransfer, "TRANSFER_OUT", "TRANSFER_IN"}
	for s := range 2 {
		txns, err := c.shards[s].ListLedgerTxnsByBizNos(ctx, ledgerTypes, transferNos)
		if err != nil {
			return nil, err
		}
		for _, tx := range txns {
			if snap := snapshots[tx.BizNo]; snap != nil {
				snap.ledgers[s] = append(snap.ledgers[s], tx)
			}
		}

		freezes, err := c.shards[s].ListAccountFreezesByBizNos(ctx, txnTypeTransfer, transferNos)
		if err != nil {
			return nil, err
		}
		for i := range freezes {
			fr := freezes[i]
			if snap := snapshots[fr.BizNo]; snap != nil {
				snap.freezes[s] = &fr
			}
		}

		branches, err := c.shards[s].ListTransferBranchesByTransferNos(ctx, transferNos)
		if err != nil {
			return nil, err
		}
		for i := range branches {
			br := branches[i]
			if br.Role != rolePayee {
				continue
			}
			if snap := snapshots[br.TransferNo]; snap != nil {
				snap.branch[s] = &br
			}
		}
	}

	var diffs []dal.Diff
	for _, o := range orders {
		snap := snapshots[o.TransferNo]
		if ok, note := validateTransferInvariant(o, snap); !ok {
			amt := o.Amount
			st := o.Status
			diffs = append(diffs, dal.Diff{
				BatchNo:     batch.BatchNo,
				Channel:     batch.Channel,
				BillDate:    batch.BillDate,
				DiffType:    dal.DiffTransferMismatch,
				TxnType:     txnTypeTransfer,
				BizNo:       o.TransferNo,
				LocalAmount: &amt,
				LocalStatus: &st,
				Status:      dal.DiffStatusPending,
				HandleNote:  &note,
				CreatedAt:   now,
				UpdatedAt:   now,
			})
		}
	}
	return diffs, nil
}

func validateTransferInvariant(o dal.InternalTransferOrder, snap *transferShardSnapshot) (bool, string) {
	if o.PayerShard > 1 || o.PayeeShard > 1 {
		return false, fmt.Sprintf("invalid shard: payer=%d payee=%d", o.PayerShard, o.PayeeShard)
	}
	switch {
	case o.Status == statusSuccess && o.Mode == modeLocal:
		payerTxns := snap.ledgers[o.PayerShard]
		otherTxns := snap.ledgers[1-o.PayerShard]
		if len(payerTxns) == 1 && payerTxns[0].BizType == txnTypeTransfer && payerTxns[0].Amount == o.Amount && len(otherTxns) == 0 {
			return true, ""
		}
	case o.Status == statusSuccess && o.Mode == modeTCC:
		payerTxns := snap.ledgers[o.PayerShard]
		payeeTxns := snap.ledgers[o.PayeeShard]
		fr := snap.freezes[o.PayerShard]
		br := snap.branch[o.PayeeShard]
		payerOK := len(payerTxns) == 1 && payerTxns[0].BizType == "TRANSFER_OUT" && payerTxns[0].Amount == o.Amount
		payeeOK := len(payeeTxns) == 1 && payeeTxns[0].BizType == "TRANSFER_IN" && payeeTxns[0].Amount == o.Amount
		freezeOK := fr != nil && fr.Status == statusConfirmed && fr.Amount == o.Amount
		branchOK := br != nil && br.Status == statusConfirmed && br.Amount == o.Amount
		if payerOK && payeeOK && freezeOK && branchOK {
			return true, ""
		}
	case o.Status == statusFailed:
		noLedgers := len(snap.ledgers[0]) == 0 && len(snap.ledgers[1]) == 0
		fr := snap.freezes[o.PayerShard]
		br := snap.branch[o.PayeeShard]
		freezeOK := fr == nil || fr.Status == statusReleased
		branchOK := br == nil || br.Status == statusCancelled
		if noLedgers && freezeOK && branchOK {
			return true, ""
		}
	}
	return false, formatTransferSnapshotNote(o, snap)
}

func formatTransferSnapshotNote(o dal.InternalTransferOrder, snap *transferShardSnapshot) string {
	var b strings.Builder
	fmt.Fprintf(&b, "mode=%s status=%s amount=%d;", o.Mode, o.Status, o.Amount)
	for s := range 2 {
		fmt.Fprintf(&b, " s%d_txns=[", s)
		for i, tx := range snap.ledgers[s] {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%s:%d", tx.BizType, tx.Amount)
		}
		b.WriteByte(']')
	}
	if fr := snap.freezes[o.PayerShard]; fr != nil {
		fmt.Fprintf(&b, " freeze(s%d)=%s:%d", o.PayerShard, fr.Status, fr.Amount)
	} else {
		fmt.Fprintf(&b, " freeze(s%d)=NONE", o.PayerShard)
	}
	if br := snap.branch[o.PayeeShard]; br != nil {
		fmt.Fprintf(&b, " branch(s%d)=%s:%d", o.PayeeShard, br.Status, br.Amount)
	} else {
		fmt.Fprintf(&b, " branch(s%d)=NONE", o.PayeeShard)
	}
	note := b.String()
	if len(note) > 512 {
		return note[:512]
	}
	return note
}

// CompositeChecker 依次执行多个 Checker，保留首个非空 CheckResult（外部对账汇总）。
type CompositeChecker struct {
	checkers []Checker
}

// NewCompositeChecker 组合多个 Checker（如外部比对 + 内部对账）。
func NewCompositeChecker(checkers ...Checker) *CompositeChecker {
	return &CompositeChecker{checkers: checkers}
}

// Check 依次执行所有 Checker；任一出错立即返回。
func (c *CompositeChecker) Check(ctx context.Context, batch *dal.Batch) (CheckResult, error) {
	var out CheckResult
	for i, chk := range c.checkers {
		if chk == nil {
			continue
		}
		res, err := chk.Check(ctx, batch)
		if err != nil {
			return CheckResult{}, err
		}
		if i == 0 {
			out = res
		}
	}
	return out, nil
}
