package biz

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

const (
	statusFrozen    = "FROZEN"
	statusConfirmed = "CONFIRMED"
	statusReleased  = "RELEASED"
	freezeNoPrefix  = "FZ"
)

// FreezeResult 是冻结或解冻的结果。
// Duplicated 为 true 表示本次没有新写入：Freeze 没有增加 frozen_amount；
// ReleaseFreeze 返回的是已经存在的 RELEASED 行。第一次成功写入时为 false。
type FreezeResult struct {
	FreezeNo   string
	Status     string
	Duplicated bool
}

type freezeRequest struct {
	FreezeNo   string
	BizType    string
	BizNo      string
	MerchantID string
	Amount     money.Amount
}

type freezeAccount struct {
	ID             uint64
	Balance        money.Amount
	FrozenAmount   money.Amount
	AllowOverdraft int
	Status         string
}

type freezeRow struct {
	FreezeNo  string
	BizType   string
	BizNo     string
	AccountID uint64
	Amount    money.Amount
	Status    string
}

// freezeTx 是同一个分片事务里的冻结读写。
// 实现不得另开连接。假实现只给不连数据库的单元测试。
type freezeTx interface {
	selectMerchant(ctx context.Context, merchantID string) (freezeAccount, bool, error)
	insertFreeze(ctx context.Context, row freezeRow) error
	selectFreeze(ctx context.Context, bizType, bizNo string) (freezeRow, bool, error)
	addFrozen(ctx context.Context, accountID uint64, amount money.Amount) (bool, error)
	reloadAccount(ctx context.Context, accountID uint64) (freezeAccount, bool, error)
}

// Freeze 冻结该商户在所属分片上的结算账户可用余额。
// 事务由本方法用 dbx.WithTx 提交。成功返回 FROZEN；幂等命中返回已有状态。
// 参数不合法返回 10001，不写行。余额不足或非 ACTIVE 时回滚，不留冻结单。
// 不修改 balance。可用余额是 balance 减 frozen_amount。
func (s *Service) Freeze(ctx context.Context, bizType, bizNo, merchantID string, amount money.Amount) (FreezeResult, error) {
	if s == nil {
		return FreezeResult{}, errno.ErrInternal
	}
	shardNo, err := validateFreeze(bizType, bizNo, merchantID, amount)
	if err != nil {
		return FreezeResult{}, err
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
	// 发号可能要等下一毫秒。放在事务外，避免占着账户行，也不把发号算进事务。
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
		out, applyErr = applyFreeze(ctx, sqlFreezeTx{tx: tx}, req)
		return applyErr
	})
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	return out, nil
}

func validateFreeze(bizType, bizNo, merchantID string, amount money.Amount) (int, error) {
	if bizType != bizRefund && bizType != bizTransfer {
		return 0, errno.ErrInvalidParam
	}
	if n := utf8.RuneCountInString(bizNo); n == 0 || n > maxBizNoRunes {
		return 0, errno.ErrInvalidParam
	}
	if amount <= 0 {
		return 0, errno.ErrInvalidParam
	}
	shardNo, err := shard.Of(merchantID)
	if err != nil {
		return 0, errno.ErrInvalidParam
	}
	return shardNo, nil
}

func applyFreeze(ctx context.Context, db freezeTx, req freezeRequest) (FreezeResult, error) {
	acct, found, err := db.selectMerchant(ctx, req.MerchantID)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !found {
		return FreezeResult{}, errno.ErrAccountNotFound
	}
	// 透支账户不允许冻结。MERCHANT_SETTLE 的检查约束已经把这一列固定为 0，这里仍要拦住假数据。
	if acct.AllowOverdraft != overdraftNo {
		return FreezeResult{}, errno.ErrInvalidParam
	}
	err = db.insertFreeze(ctx, freezeRow{
		FreezeNo:  req.FreezeNo,
		BizType:   req.BizType,
		BizNo:     req.BizNo,
		AccountID: acct.ID,
		Amount:    req.Amount,
		Status:    statusFrozen,
	})
	if dbx.IsDuplicate(err) {
		// 1062 只让这一条 INSERT 失败，InnoDB 事务仍然开着。同一事务里读已有行。
		return readFreezeDuplicate(ctx, db, req, acct.ID, err)
	}
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	ok, err := db.addFrozen(ctx, acct.ID, req.Amount)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !ok {
		return FreezeResult{}, classifyFreezeMiss(ctx, db, acct.ID, req.Amount)
	}
	return FreezeResult{FreezeNo: req.FreezeNo, Status: statusFrozen, Duplicated: false}, nil
}

func readFreezeDuplicate(ctx context.Context, db freezeTx, req freezeRequest, accountID uint64, dup error) (FreezeResult, error) {
	row, found, err := db.selectFreeze(ctx, req.BizType, req.BizNo)
	if err != nil {
		return FreezeResult{}, internalErr(err)
	}
	if !found {
		return FreezeResult{}, errno.Wrap(errno.ErrInternal, dup)
	}
	// RELEASED 是墓碑或已解冻。一律 40004，不再比较金额，否则迟到的冻结会改成参数冲突。
	if row.Status == statusReleased {
		return FreezeResult{}, errno.ErrFreezeStatusConflict
	}
	if row.Status == statusFrozen || row.Status == statusConfirmed {
		if row.Amount == req.Amount && row.AccountID == accountID {
			return FreezeResult{FreezeNo: row.FreezeNo, Status: row.Status, Duplicated: true}, nil
		}
		return FreezeResult{}, errno.ErrIdempotentConflict
	}
	return FreezeResult{}, errno.ErrInternal
}

func classifyFreezeMiss(ctx context.Context, db freezeTx, accountID uint64, amount money.Amount) error {
	row, found, err := db.reloadAccount(ctx, accountID)
	if err != nil {
		return internalErr(err)
	}
	if !found {
		return errno.ErrInternal
	}
	// 停用优先于余额不足。调用方看到 40002 会当成还能重试，非 ACTIVE 重试也不会成功。
	if row.Status != statusActive {
		return errno.ErrAccountInactive
	}
	avail, subErr := money.Sub(row.Balance, row.FrozenAmount)
	if subErr != nil {
		return errno.Wrap(errno.ErrInternal, subErr)
	}
	if avail < amount {
		return errno.ErrInsufficientBalance
	}
	// 条件更新没写上，但重读仍然够冻结。不要在事务里空转，否则会占着行锁反复抢。
	return errno.ErrInternal
}

const selectMerchantSQL = `
SELECT id, balance, frozen_amount, allow_overdraft, status
FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND currency = ? AND sub_no = 0`

const insertFreezeSQL = `
INSERT INTO account_freeze (
  freeze_no, biz_type, biz_no, account_id, amount, status, version
) VALUES (?, ?, ?, ?, ?, ?, ?)`

const selectFreezeSQL = `
SELECT freeze_no, account_id, amount, status
FROM account_freeze
WHERE biz_type = ? AND biz_no = ?`

const reloadFreezeAccountSQL = `
SELECT id, balance, frozen_amount, allow_overdraft, status
FROM account
WHERE id = ?`

const addFrozenSQL = `
UPDATE account
SET frozen_amount = frozen_amount + ?, version = version + 1
WHERE id = ? AND status = 'ACTIVE' AND balance - frozen_amount >= ?`

type sqlFreezeTx struct {
	tx *sqlx.Tx
}

func (s sqlFreezeTx) selectMerchant(ctx context.Context, merchantID string) (freezeAccount, bool, error) {
	return s.queryAccount(ctx, selectMerchantSQL, ownerMerchant, merchantID, typeMerchantSettle, currencyCNY)
}

func (s sqlFreezeTx) insertFreeze(ctx context.Context, row freezeRow) error {
	if s.tx == nil {
		return errNilTx
	}
	_, err := s.tx.ExecContext(ctx, insertFreezeSQL,
		row.FreezeNo,
		row.BizType,
		row.BizNo,
		row.AccountID,
		int64(row.Amount),
		row.Status,
		int64(0),
	)
	return err
}

func (s sqlFreezeTx) selectFreeze(ctx context.Context, bizType, bizNo string) (freezeRow, bool, error) {
	if s.tx == nil {
		return freezeRow{}, false, errNilTx
	}
	row, err := scanFreezeRow(s.tx.QueryRowContext(ctx, selectFreezeSQL, bizType, bizNo))
	if errors.Is(err, sql.ErrNoRows) {
		return freezeRow{}, false, nil
	}
	if err != nil {
		return freezeRow{}, false, err
	}
	return row, true, nil
}

func (s sqlFreezeTx) addFrozen(ctx context.Context, accountID uint64, amount money.Amount) (bool, error) {
	if s.tx == nil {
		return false, errNilTx
	}
	res, err := s.tx.ExecContext(ctx, addFrozenSQL, int64(amount), accountID, int64(amount))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	if n != 1 {
		return false, fmt.Errorf("biz: freeze update affected %d", n)
	}
	return true, nil
}

func (s sqlFreezeTx) reloadAccount(ctx context.Context, accountID uint64) (freezeAccount, bool, error) {
	return s.queryAccount(ctx, reloadFreezeAccountSQL, accountID)
}

func (s sqlFreezeTx) queryAccount(ctx context.Context, query string, args ...any) (freezeAccount, bool, error) {
	if s.tx == nil {
		return freezeAccount{}, false, errNilTx
	}
	row, err := scanFreezeAccount(s.tx.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return freezeAccount{}, false, nil
	}
	if err != nil {
		return freezeAccount{}, false, err
	}
	return row, true, nil
}

func scanFreezeAccount(sc rowScanner) (freezeAccount, error) {
	var (
		row       freezeAccount
		balance   int64
		frozen    int64
		overdraft int
	)
	err := sc.Scan(&row.ID, &balance, &frozen, &overdraft, &row.Status)
	if err != nil {
		return freezeAccount{}, err
	}
	row.Balance = money.Amount(balance)
	row.FrozenAmount = money.Amount(frozen)
	row.AllowOverdraft = overdraft
	return row, nil
}

func scanFreezeRow(sc rowScanner) (freezeRow, error) {
	var (
		row    freezeRow
		amount int64
	)
	err := sc.Scan(&row.FreezeNo, &row.AccountID, &amount, &row.Status)
	if err != nil {
		return freezeRow{}, err
	}
	row.Amount = money.Amount(amount)
	return row, nil
}
