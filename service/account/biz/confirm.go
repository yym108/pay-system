package biz

import (
	"context"
	"database/sql"
	"errors"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

// ConfirmResult 是确认扣款的结果。
// Duplicated 为 true 表示冻结单已经是 CONFIRMED，本次没有记账。
// 该字段只在进程内，不进入 RPC。
type ConfirmResult struct {
	FreezeNo   string
	TxnNo      string
	Duplicated bool
}

type confirmRequest struct {
	BizType    string
	BizNo      string
	MerchantID string
	Channel    string
}

type confirmRow struct {
	FreezeNo  string
	AccountID uint64
	Amount    money.Amount
	Status    string
	TxnNo     string
}

// confirmTx 是同一个分片事务里的确认读写。
// 实现不得另开连接。假实现只给不连数据库的单元测试。
type confirmTx interface {
	lockConfirm(ctx context.Context, bizType, bizNo string) (confirmRow, bool, error)
	selectMerchant(ctx context.Context, merchantID string) (freezeAccount, bool, error)
	selectOwnerAccount(ctx context.Context, ownerType, ownerID, accountType string, subNo int) (uint64, bool, error)
	markConfirmed(ctx context.Context, bizType, bizNo, txnNo string, amount money.Amount, accountID uint64) (bool, error)
}

// ConfirmFreeze 确认一笔冻结：借商户结算账户并同时减少冻结，贷本分片平台子账户。
// 事务由本方法用 dbx.WithTx 提交。Post 仍不提交。
// 请求没有金额，金额只用冻结单上的 amount。
// 第一次成功 Duplicated 为 false，返回已有 freeze_no。已 CONFIRMED 的重放为 true，不再比较 channel。
// 参数不合法返回 10001，不进事务。
func (s *Service) ConfirmFreeze(ctx context.Context, bizType, bizNo, merchantID, channel string) (ConfirmResult, error) {
	shardNo, err := validateConfirm(bizType, bizNo, merchantID, channel)
	if err != nil {
		return ConfirmResult{}, err
	}
	if s == nil {
		return ConfirmResult{}, errno.ErrInternal
	}
	if ctx == nil {
		return ConfirmResult{}, errno.ErrInternal
	}
	if err = ctx.Err(); err != nil {
		return ConfirmResult{}, err
	}
	if s.ids == nil || s.pools == nil || s.subCount < 1 {
		return ConfirmResult{}, errno.ErrInternal
	}
	db, err := s.pools.ShardDB(shardNo)
	if err != nil {
		return ConfirmResult{}, errno.Wrap(errno.ErrInternal, err)
	}
	req := confirmRequest{
		BizType:    bizType,
		BizNo:      bizNo,
		MerchantID: merchantID,
		Channel:    channel,
	}
	var out ConfirmResult
	err = dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		var applyErr error
		out, applyErr = applyConfirm(ctx, s.ids, s.subCount, sqlFreezeTx{tx: tx}, sqlPostTx{tx: tx}, req)
		return applyErr
	})
	if err != nil {
		return ConfirmResult{}, internalErr(err)
	}
	return out, nil
}

func validateConfirm(bizType, bizNo, merchantID, channel string) (int, error) {
	if bizType != bizRefund && bizType != bizTransfer {
		return 0, errno.ErrInvalidParam
	}
	if n := utf8.RuneCountInString(bizNo); n == 0 || n > maxBizNoRunes {
		return 0, errno.ErrInvalidParam
	}
	shardNo, err := shard.Of(merchantID)
	if err != nil {
		return 0, errno.ErrInvalidParam
	}
	switch bizType {
	case bizRefund:
		if !validChannelOwner(channel) {
			return 0, errno.ErrInvalidParam
		}
	case bizTransfer:
		if channel != "" {
			return 0, errno.ErrInvalidParam
		}
	default:
		return 0, errno.ErrInvalidParam
	}
	return shardNo, nil
}

func applyConfirm(ctx context.Context, ids *idgen.Generator, subCount int, db confirmTx, post postTx, req confirmRequest) (ConfirmResult, error) {
	// 先锁冻结单。账户锁交给 Post，按 id 升序。先改商户再锁更小 id 的平台账户会和普通记账死锁。
	row, found, err := db.lockConfirm(ctx, req.BizType, req.BizNo)
	if err != nil {
		return ConfirmResult{}, internalErr(err)
	}
	if !found || row.Status == statusReleased {
		return ConfirmResult{}, errno.ErrFreezeStatusConflict
	}
	switch row.Status {
	case statusConfirmed:
		if row.TxnNo == "" {
			return ConfirmResult{}, errno.ErrInternal
		}
		return ConfirmResult{FreezeNo: row.FreezeNo, TxnNo: row.TxnNo, Duplicated: true}, nil
	case statusFrozen:
		return confirmFrozen(ctx, ids, subCount, db, post, req, row)
	default:
		return ConfirmResult{}, errno.ErrInternal
	}
}

func confirmFrozen(ctx context.Context, ids *idgen.Generator, subCount int, db confirmTx, post postTx, req confirmRequest, row confirmRow) (ConfirmResult, error) {
	acct, found, err := db.selectMerchant(ctx, req.MerchantID)
	if err != nil {
		return ConfirmResult{}, internalErr(err)
	}
	// 找不到结算账户也是和冻结单对不上。不要写成 40001，空回滚才用 40001。
	if !found || row.AccountID != acct.ID {
		return ConfirmResult{}, errno.ErrIdempotentConflict
	}
	if acct.AllowOverdraft != overdraftNo {
		return ConfirmResult{}, errno.ErrInvalidParam
	}
	subNo, err := shard.Sub(req.BizNo, subCount)
	if err != nil {
		return ConfirmResult{}, errno.Wrap(errno.ErrInternal, err)
	}
	credit, err := creditFor(req.BizType, req.Channel)
	if err != nil {
		return ConfirmResult{}, err
	}
	platformID, found, err := db.selectOwnerAccount(ctx, ownerPlatform, credit.ownerID, credit.accountType, subNo)
	if err != nil {
		return ConfirmResult{}, internalErr(err)
	}
	if !found {
		return ConfirmResult{}, errno.ErrAccountNotFound
	}
	if platformID == acct.ID {
		return ConfirmResult{}, errno.ErrInternal
	}
	posted, err := postEntries(ctx, ids, post, credit.voucherType, req.BizNo, "", []Entry{
		{AccountID: acct.ID, Direction: directionDebit, Amount: row.Amount},
		{AccountID: platformID, Direction: directionCredit, Amount: row.Amount},
	}, acct.ID)
	if err != nil {
		// 已经是业务码或发号映射。不要再包一层，否则 40005 会变成 10003。
		return ConfirmResult{}, err
	}
	if posted.Duplicated {
		// 冻结单仍是 FROZEN，凭证却已存在。不要 CAS，也不要减冻结。
		// 本次事务回滚；已经提交的旧凭证不在这个事务里。
		return ConfirmResult{}, errno.ErrInternal
	}
	ok, err := db.markConfirmed(ctx, req.BizType, req.BizNo, posted.TxnNo, row.Amount, acct.ID)
	if err != nil {
		return ConfirmResult{}, internalErr(err)
	}
	if !ok {
		// 这行已被 FOR UPDATE 锁住。影响 0 行是 WHERE 没对上，不是并发幂等。
		// 当成成功会把刚插入的凭证一起提交。
		return ConfirmResult{}, errno.ErrInternal
	}
	return ConfirmResult{FreezeNo: row.FreezeNo, TxnNo: posted.TxnNo, Duplicated: false}, nil
}

type creditSpec struct {
	ownerID     string
	accountType string
	voucherType string
}

func creditFor(bizType, channel string) (creditSpec, error) {
	switch bizType {
	case bizRefund:
		return creditSpec{ownerID: channel, accountType: typeChannelReceivable, voucherType: bizRefund}, nil
	case bizTransfer:
		return creditSpec{ownerID: platformOwnerID, accountType: typeTransit, voucherType: bizTransferOut}, nil
	default:
		return creditSpec{}, errno.ErrInternal
	}
}

const lockConfirmSQL = `
SELECT freeze_no, account_id, amount, status, txn_no
FROM account_freeze
WHERE biz_type = ? AND biz_no = ?
FOR UPDATE`

const selectOwnerAccountSQL = `
SELECT id
FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND currency = ? AND sub_no = ?`

const markConfirmedSQL = `
UPDATE account_freeze
SET status = 'CONFIRMED', txn_no = ?, version = version + 1
WHERE biz_type = ? AND biz_no = ? AND status = 'FROZEN' AND amount = ? AND account_id = ?`

func (s sqlFreezeTx) lockConfirm(ctx context.Context, bizType, bizNo string) (confirmRow, bool, error) {
	if s.tx == nil {
		return confirmRow{}, false, errNilTx
	}
	var (
		row    confirmRow
		amount int64
		txn    sql.NullString
	)
	err := s.tx.QueryRowContext(ctx, lockConfirmSQL, bizType, bizNo).Scan(
		&row.FreezeNo, &row.AccountID, &amount, &row.Status, &txn,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return confirmRow{}, false, nil
	}
	if err != nil {
		return confirmRow{}, false, err
	}
	row.Amount = money.Amount(amount)
	if txn.Valid {
		row.TxnNo = txn.String
	}
	return row, true, nil
}

func (s sqlFreezeTx) selectOwnerAccount(ctx context.Context, ownerType, ownerID, accountType string, subNo int) (uint64, bool, error) {
	if s.tx == nil {
		return 0, false, errNilTx
	}
	var id uint64
	err := s.tx.QueryRowContext(ctx, selectOwnerAccountSQL, ownerType, ownerID, accountType, currencyCNY, subNo).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (s sqlFreezeTx) markConfirmed(ctx context.Context, bizType, bizNo, txnNo string, amount money.Amount, accountID uint64) (bool, error) {
	if s.tx == nil {
		return false, errNilTx
	}
	res, err := s.tx.ExecContext(ctx, markConfirmedSQL, txnNo, bizType, bizNo, int64(amount), accountID)
	if err != nil {
		return false, err
	}
	return affectedOne(res)
}
