// Package biz 实现账户开户、账户查询、通用记账、冻结和解冻。
//
// OpenAccount 供种子和测试调用。GetAccount 只读。Post 在调用方事务里记账。
// Freeze 和 ReleaseFreeze 自己提交分片事务。这些方法都不监听 RPC。
package biz

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/account/dal"
)

const (
	currencyCNY           = "CNY"
	ownerMerchant         = "MERCHANT"
	ownerPlatform         = "PLATFORM"
	platformOwnerID       = "PLATFORM"
	typeMerchantSettle    = "MERCHANT_SETTLE"
	typeChannelReceivable = "CHANNEL_RECEIVABLE"
	typeFeeIncome         = "FEE_INCOME"
	typeTransit           = "TRANSIT"
	directionCredit       = "CREDIT"
	directionDebit        = "DEBIT"
	statusActive          = "ACTIVE"
	accountNoPrefix       = "AC"
	maxOwnerIDRunes       = 32
	overdraftNo           = 0
	overdraftYes          = 1
)

// errDifferentOwner 表示 1062 撞上的不是这一组 owner。
// 文本里没有 SQL，只放在 10003 的 cause 里。
var errDifferentOwner = errors.New("account number conflicts with a different owner")

var errNilTx = errors.New("biz: nil tx")

// Account 是开户或查询结果。金额单位是分。
// Shard 来自开户参数或商户路由，account 表没有这一列。
// AvailableAmount 是 Balance 减 FrozenAmount，不是单独存的列。
// SubCount 是参与本次结果的行数。开户返回 0。
type Account struct {
	AccountNo        string
	OwnerType        string
	OwnerID          string
	AccountType      string
	Shard            int
	SubNo            int
	Currency         string
	BalanceDirection string
	Balance          money.Amount
	FrozenAmount     money.Amount
	AvailableAmount  money.Amount
	Status           string
	SubCount         int
}

// Service 开户、查询、记账、冻结和解冻。pools、发号器和子账户个数都由调用方注入，本类型不读环境变量。
type Service struct {
	pools    *dal.Pools
	ids      *idgen.Generator
	subCount int
	// query 非空时替换分片读取，只给同包测试使用。生产路径保持 nil。
	query accountLister
}

// New 构造账户服务。subCount 是平台子账户个数，调用方传入。查询不使用它做截断。
func New(pools *dal.Pools, ids *idgen.Generator, subCount int) *Service {
	return &Service{pools: pools, ids: ids, subCount: subCount}
}

// OpenAccount 按账户类型开户。
// 已存在则返回已有账户，不修改余额和冻结金额。
// 组合、分片或子账户号非法时返回 10001，不写行。
// 账户号在事务外生成。与另一组 owner 冲突时返回 10003，Error() 不包含 SQL。
func (s *Service) OpenAccount(ctx context.Context, ownerType, ownerID, accountType string, shardNo, subNo int) (Account, error) {
	if s == nil {
		return Account{}, errno.ErrInternal
	}
	if ctx == nil {
		return Account{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return Account{}, err
	}
	spec, err := specFor(ownerType, ownerID, accountType, shardNo, subNo, s.subCount)
	if err != nil {
		return Account{}, err
	}
	if s.pools == nil || s.ids == nil {
		return Account{}, errno.ErrInternal
	}
	// 发号可能等待下一毫秒或追平回拨，放在事务外，避免占着行锁。
	accountNo, err := s.ids.NewNo(ctx, accountNoPrefix)
	if err != nil {
		return Account{}, mapIDError(err)
	}
	db, err := s.pools.ShardDB(spec.Shard)
	if err != nil {
		return Account{}, errno.Wrap(errno.ErrInternal, err)
	}
	var out Account
	err = dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		loaded, loadErr := applyOpen(ctx, sqlAccountTx{tx: tx}, spec, accountNo)
		if loadErr != nil {
			return loadErr
		}
		out = loaded
		return nil
	})
	if err != nil {
		return Account{}, internalErr(err)
	}
	logCtx, logger := shard.WithShard(ctx, nil, out.Shard)
	logger.InfoContext(logCtx, "account opened",
		slog.String("account_no", out.AccountNo),
		slog.String("owner_type", out.OwnerType),
		slog.String("owner_id", out.OwnerID),
		slog.String("account_type", out.AccountType),
		slog.Int("sub_no", out.SubNo),
	)
	return out, nil
}

type accountSpec struct {
	OwnerType        string
	OwnerID          string
	AccountType      string
	Shard            int
	SubNo            int
	Currency         string
	BalanceDirection string
	AllowOverdraft   int
}

func specFor(ownerType, ownerID, accountType string, shardNo, subNo, subCount int) (accountSpec, error) {
	spec := accountSpec{
		OwnerType:   ownerType,
		OwnerID:     ownerID,
		AccountType: accountType,
		Shard:       shardNo,
		SubNo:       subNo,
		Currency:    currencyCNY,
	}
	switch accountType {
	case typeMerchantSettle:
		if ownerType != ownerMerchant {
			return accountSpec{}, errno.ErrInvalidParam
		}
		want, err := shard.Of(ownerID)
		if err != nil || shardNo != want || subNo != 0 {
			return accountSpec{}, errno.ErrInvalidParam
		}
		spec.BalanceDirection = directionCredit
		spec.AllowOverdraft = overdraftNo
	case typeChannelReceivable:
		if ownerType != ownerPlatform || !validPlatformShard(shardNo) || !validSubNo(subNo, subCount) || !validChannelOwner(ownerID) {
			return accountSpec{}, errno.ErrInvalidParam
		}
		spec.BalanceDirection = directionDebit
		spec.AllowOverdraft = overdraftYes
	case typeFeeIncome, typeTransit:
		if ownerType != ownerPlatform || ownerID != platformOwnerID || !validPlatformShard(shardNo) || !validSubNo(subNo, subCount) {
			return accountSpec{}, errno.ErrInvalidParam
		}
		spec.BalanceDirection = directionCredit
		spec.AllowOverdraft = overdraftYes
	default:
		return accountSpec{}, errno.ErrInvalidParam
	}
	return spec, nil
}

func validPlatformShard(shardNo int) bool {
	return shardNo == 0 || shardNo == 1
}

func validSubNo(subNo, subCount int) bool {
	return subNo >= 0 && subNo < subCount
}

func validChannelOwner(ownerID string) bool {
	n := utf8.RuneCountInString(ownerID)
	return n > 0 && n <= maxOwnerIDRunes
}

// accountTx 是同一个分片事务里的插入和按幂等键读取。
// 实现不得另开连接。假实现只用于不连数据库的单元测试。
type accountTx interface {
	insertAccount(ctx context.Context, spec accountSpec, accountNo string) error
	selectAccount(ctx context.Context, spec accountSpec) (Account, bool, error)
}

func applyOpen(ctx context.Context, db accountTx, spec accountSpec, accountNo string) (Account, error) {
	err := db.insertAccount(ctx, spec, accountNo)
	if err == nil {
		return newAccount(spec, accountNo), nil
	}
	if !dbx.IsDuplicate(err) {
		return Account{}, internalErr(err)
	}
	// 不解析 1062 的文本来区分哪个唯一索引。先按 owner 五元组读：
	// 读到同一组才是幂等成功，且不改余额；读不到说明账户号属于另一行。
	row, found, selErr := db.selectAccount(ctx, spec)
	if selErr != nil {
		return Account{}, internalErr(selErr)
	}
	if !found || !sameOwner(row, spec) {
		return Account{}, errno.Wrap(errno.ErrInternal, errors.Join(errDifferentOwner, err))
	}
	avail, subErr := money.Sub(row.Balance, row.FrozenAmount)
	if subErr != nil {
		return Account{}, errno.Wrap(errno.ErrInternal, subErr)
	}
	row.Shard = spec.Shard
	row.AvailableAmount = avail
	return row, nil
}

func newAccount(spec accountSpec, accountNo string) Account {
	return Account{
		AccountNo:        accountNo,
		OwnerType:        spec.OwnerType,
		OwnerID:          spec.OwnerID,
		AccountType:      spec.AccountType,
		Shard:            spec.Shard,
		SubNo:            spec.SubNo,
		Currency:         spec.Currency,
		BalanceDirection: spec.BalanceDirection,
		Status:           statusActive,
	}
}

func sameOwner(row Account, spec accountSpec) bool {
	return row.OwnerType == spec.OwnerType &&
		row.OwnerID == spec.OwnerID &&
		row.AccountType == spec.AccountType &&
		row.Currency == spec.Currency &&
		row.SubNo == spec.SubNo
}

func mapIDError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, idgen.ErrClockRollback) {
		return errno.Wrap(errno.ErrUnavailable, err)
	}
	return errno.Wrap(errno.ErrInternal, err)
}

func internalErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var known *errno.Error
	if errors.As(err, &known) {
		return err
	}
	return errno.Wrap(errno.ErrInternal, err)
}

const insertAccountSQL = `
INSERT INTO account (
  account_no, owner_type, owner_id, account_type, currency, sub_no,
  balance_direction, balance, frozen_amount, allow_overdraft, status, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const selectAccountSQL = `
SELECT account_no, owner_type, owner_id, account_type, sub_no, currency,
       balance_direction, balance, frozen_amount, status
FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND currency = ? AND sub_no = ?`

type sqlAccountTx struct {
	tx *sqlx.Tx
}

func (s sqlAccountTx) insertAccount(ctx context.Context, spec accountSpec, accountNo string) error {
	if s.tx == nil {
		return errNilTx
	}
	_, err := s.tx.ExecContext(ctx, insertAccountSQL,
		accountNo,
		spec.OwnerType,
		spec.OwnerID,
		spec.AccountType,
		spec.Currency,
		spec.SubNo,
		spec.BalanceDirection,
		int64(0),
		int64(0),
		spec.AllowOverdraft,
		statusActive,
		int64(0),
	)
	return err
}

func (s sqlAccountTx) selectAccount(ctx context.Context, spec accountSpec) (Account, bool, error) {
	if s.tx == nil {
		return Account{}, false, errNilTx
	}
	var (
		row     Account
		balance int64
		frozen  int64
	)
	err := s.tx.QueryRowContext(ctx, selectAccountSQL,
		spec.OwnerType, spec.OwnerID, spec.AccountType, spec.Currency, spec.SubNo,
	).Scan(
		&row.AccountNo,
		&row.OwnerType,
		&row.OwnerID,
		&row.AccountType,
		&row.SubNo,
		&row.Currency,
		&row.BalanceDirection,
		&balance,
		&frozen,
		&row.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, false, nil
	}
	if err != nil {
		return Account{}, false, err
	}
	row.Balance = money.Amount(balance)
	row.FrozenAmount = money.Amount(frozen)
	return row, true, nil
}
