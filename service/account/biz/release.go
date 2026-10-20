package biz

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// ReleaseFreeze 解冻该商户结算账户上的一笔冻结，或在冻结单不存在时写入 RELEASED 墓碑。
// 事务由本方法用 dbx.WithTx 提交。第一次解冻和第一次墓碑的 Duplicated 为 false。
// 已经是 RELEASED 时 Duplicated 为 true，返回已有 freeze_no，不再比较金额。
// 参数不合法返回 10001，不写行。已确认返回 40004。不修改 balance。
// 停用账户上的冻结仍然解开，避免这笔钱一直占着可用余额。
func (s *Service) ReleaseFreeze(ctx context.Context, bizType, bizNo, merchantID string, amount money.Amount) (FreezeResult, error) {
	shardNo, err := validateFreeze(bizType, bizNo, merchantID, amount)
	if err != nil {
		return FreezeResult{}, err
	}
	if s == nil {
		return FreezeResult{}, errno.ErrInternal
	}
	if ctx == nil {
		return FreezeResult{}, errno.ErrInternal
	}
	if err = ctx.Err(); err != nil {
		return FreezeResult{}, err
	}
	if s.ids == nil {
		return FreezeResult{}, errno.ErrInternal
	}
	db, err := s.pools.ShardDB(shardNo)
	if err != nil {
		return FreezeResult{}, errno.Wrap(errno.ErrInternal, err)
	}
	// 发号可能要等下一毫秒。放在事务外，只给可能的墓碑用。1213/1205 重试沿用这个号。
	freezeNo, err := s.ids.NewNo(ctx, freezeNoPrefix)
	if err != nil {
		return FreezeResult{}, mapIDError(err)
	}
	req := freezeRequest{
		FreezeNo:   freezeNo,
		BizType:    bizType,
		BizNo:      bizNo,
		MerchantID: merchantID,
		Amount:     amount,
	}
	var out FreezeResult
	err = dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		var applyErr error
		out, applyErr = applyRelease(ctx, sqlFreezeTx{tx: tx}, req)
		return applyErr
	})
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	return out, nil
}

// releaseTx 是同一个分片事务里的解冻读写。
// 实现不得另开连接。假实现只给不连数据库的单元测试。
type releaseTx interface {
	lockFreeze(ctx context.Context, bizType, bizNo string) (freezeRow, bool, error)
	selectMerchant(ctx context.Context, merchantID string) (freezeAccount, bool, error)
	insertFreeze(ctx context.Context, row freezeRow) error
	markReleased(ctx context.Context, bizType, bizNo string, amount money.Amount, accountID uint64) (bool, error)
	subFrozen(ctx context.Context, accountID uint64, amount money.Amount) (bool, error)
	reloadAccount(ctx context.Context, accountID uint64) (freezeAccount, bool, error)
}

func applyRelease(ctx context.Context, db releaseTx, req freezeRequest) (FreezeResult, error) {
	// 先锁冻结单再改账户。Freeze 是先插入冻结单再改账户，两边都不会先锁账户。
	row, found, err := db.lockFreeze(ctx, req.BizType, req.BizNo)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !found {
		return insertReleaseTombstone(ctx, db, req)
	}
	return releaseExisting(ctx, db, req, row)
}

func insertReleaseTombstone(ctx context.Context, db releaseTx, req freezeRequest) (FreezeResult, error) {
	acct, found, err := db.selectMerchant(ctx, req.MerchantID)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !found {
		return FreezeResult{}, errno.ErrAccountNotFound
	}
	// 透支账户不允许占墓碑。MERCHANT_SETTLE 的检查约束已经把这一列固定为 0，这里仍要拦住假数据。
	if acct.AllowOverdraft != overdraftNo {
		return FreezeResult{}, errno.ErrInvalidParam
	}
	err = db.insertFreeze(ctx, freezeRow{
		FreezeNo:  req.FreezeNo,
		BizType:   req.BizType,
		BizNo:     req.BizNo,
		AccountID: acct.ID,
		Amount:    req.Amount,
		Status:    statusReleased,
	})
	if dbx.IsDuplicate(err) {
		// 1062 只让这一条 INSERT 失败，InnoDB 事务仍然开着。同一事务里读已有行。
		return releaseAfterDuplicate(ctx, db, req, err)
	}
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	return FreezeResult{FreezeNo: req.FreezeNo, Status: statusReleased, Duplicated: false}, nil
}

func releaseAfterDuplicate(ctx context.Context, db releaseTx, req freezeRequest, dup error) (FreezeResult, error) {
	row, found, err := db.lockFreeze(ctx, req.BizType, req.BizNo)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !found {
		return FreezeResult{}, errno.Wrap(errno.ErrInternal, dup)
	}
	return releaseExisting(ctx, db, req, row)
}

func releaseExisting(ctx context.Context, db releaseTx, req freezeRequest, row freezeRow) (FreezeResult, error) {
	switch row.Status {
	case statusReleased:
		// 墓碑或已解冻。不再比较金额和账户，否则重试会变成参数冲突。
		return FreezeResult{FreezeNo: row.FreezeNo, Status: statusReleased, Duplicated: true}, nil
	case statusConfirmed:
		return FreezeResult{}, errno.ErrFreezeStatusConflict
	case statusFrozen:
		return releaseFrozen(ctx, db, req, row)
	default:
		return FreezeResult{}, errno.ErrInternal
	}
}

func releaseFrozen(ctx context.Context, db releaseTx, req freezeRequest, row freezeRow) (FreezeResult, error) {
	acct, found, err := db.selectMerchant(ctx, req.MerchantID)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	// 没有结算账户就对不上冻结单上的 account_id。这是和已有行不一致，不是空回滚。
	if !found || row.Amount != req.Amount || row.AccountID != acct.ID {
		return FreezeResult{}, errno.ErrIdempotentConflict
	}
	ok, err := db.markReleased(ctx, req.BizType, req.BizNo, req.Amount, acct.ID)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !ok {
		return rereadReleased(ctx, db, req)
	}
	ok, err = db.subFrozen(ctx, acct.ID, req.Amount)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !ok {
		return FreezeResult{}, rereadFrozenAccount(ctx, db, acct.ID, req.Amount)
	}
	// 用已有单号。事务外生成的号只给墓碑，这次没有插入。
	return FreezeResult{FreezeNo: row.FreezeNo, Status: statusReleased, Duplicated: false}, nil
}

func rereadReleased(ctx context.Context, db releaseTx, req freezeRequest) (FreezeResult, error) {
	row, found, err := db.lockFreeze(ctx, req.BizType, req.BizNo)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !found {
		return FreezeResult{}, errno.ErrInternal
	}
	if row.Status == statusReleased {
		return FreezeResult{FreezeNo: row.FreezeNo, Status: statusReleased, Duplicated: true}, nil
	}
	if row.Status == statusConfirmed {
		return FreezeResult{}, errno.ErrFreezeStatusConflict
	}
	// 仍是 FROZEN 或其他状态。不要在事务里空转。
	return FreezeResult{}, errno.ErrInternal
}

func rereadFrozenAccount(ctx context.Context, db releaseTx, accountID uint64, amount money.Amount) error {
	row, found, err := db.reloadAccount(ctx, accountID)
	if err != nil {
		return internalErr(err)
	}
	if !found || row.FrozenAmount < amount {
		return errno.ErrInternal
	}
	// 条件更新没写上，但重读冻结仍然够减。不要空转，也不要写成余额不足。
	return errno.ErrInternal
}

const lockFreezeSQL = `
SELECT freeze_no, account_id, amount, status
FROM account_freeze
WHERE biz_type = ? AND biz_no = ?
FOR UPDATE`

const releaseFrozenSQL = `
UPDATE account_freeze
SET status = 'RELEASED', version = version + 1
WHERE biz_type = ? AND biz_no = ? AND status = 'FROZEN' AND amount = ? AND account_id = ?`

const subFrozenSQL = `
UPDATE account
SET frozen_amount = frozen_amount - ?, version = version + 1
WHERE id = ? AND frozen_amount >= ?`

func (s sqlFreezeTx) lockFreeze(ctx context.Context, bizType, bizNo string) (freezeRow, bool, error) {
	if s.tx == nil {
		return freezeRow{}, false, errNilTx
	}
	row, err := scanFreezeRow(s.tx.QueryRowContext(ctx, lockFreezeSQL, bizType, bizNo))
	if errors.Is(err, sql.ErrNoRows) {
		return freezeRow{}, false, nil
	}
	if err != nil {
		return freezeRow{}, false, err
	}
	return row, true, nil
}

func (s sqlFreezeTx) markReleased(ctx context.Context, bizType, bizNo string, amount money.Amount, accountID uint64) (bool, error) {
	if s.tx == nil {
		return false, errNilTx
	}
	res, err := s.tx.ExecContext(ctx, releaseFrozenSQL, bizType, bizNo, int64(amount), accountID)
	if err != nil {
		return false, err
	}
	return affectedOne(res)
}

func (s sqlFreezeTx) subFrozen(ctx context.Context, accountID uint64, amount money.Amount) (bool, error) {
	if s.tx == nil {
		return false, errNilTx
	}
	// 不看 status。停用账户上的冻结也要减掉，否则这笔钱会一直占着。
	res, err := s.tx.ExecContext(ctx, subFrozenSQL, int64(amount), accountID, int64(amount))
	if err != nil {
		return false, err
	}
	return affectedOne(res)
}

func affectedOne(res sql.Result) (bool, error) {
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	if n != 1 {
		return false, fmt.Errorf("biz: release update affected %d", n)
	}
	return true, nil
}
