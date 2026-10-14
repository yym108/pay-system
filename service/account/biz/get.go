package biz

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/account/dal"
)

// accountReadTimeout 是单次查询的上限，与 MySQL 读写超时一致。
// 两个分片各算各的，不把两次查询放进同一个事务。
const accountReadTimeout = 3 * time.Second

var errNilDB = errors.New("biz: nil db")

type queryKind int

const (
	kindMerchant queryKind = iota + 1
	kindPlatform
)

// accountLister 读取一个分片上的账户行。
// subNo 非 nil 时只读该子账户。实现不得再打开别的分片。
type accountLister interface {
	listAccounts(ctx context.Context, shardNo int, ownerType, ownerID, accountType string, subNo *int) ([]Account, error)
}

// GetAccount 按所有者查询账户。只读，不加锁，也不为了查询去开户。
// MERCHANT 只读所属分片里 sub_no 为 0 的一行，可用金额是余额减冻结，SubCount 为 1。
// PLATFORM 分别读取两个分片的全部子账户后相加，不放进跨库事务。
// 账户号为空。Shard 和 SubNo 为 0，表示省略，不是分片 0。
// 类型与所有者不匹配时返回 10001，不访问数据库。
// 不存在返回 40001。查询失败返回 10003，Error() 不含 SQL。
func (s *Service) GetAccount(ctx context.Context, ownerType, ownerID, accountType string) (Account, error) {
	if s == nil || ctx == nil {
		return Account{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return Account{}, err
	}
	kind, shardNo, err := classifyQuery(ownerType, ownerID, accountType)
	if err != nil {
		return Account{}, err
	}
	q := s.query
	if q == nil {
		if s.pools == nil {
			return Account{}, errno.ErrInternal
		}
		q = poolLister{pools: s.pools}
	}
	return getAccount(ctx, q, kind, shardNo, ownerType, ownerID, accountType)
}

func classifyQuery(ownerType, ownerID, accountType string) (queryKind, int, error) {
	switch accountType {
	case typeMerchantSettle:
		if ownerType != ownerMerchant {
			return 0, 0, errno.ErrInvalidParam
		}
		shardNo, err := shard.Of(ownerID)
		if err != nil {
			return 0, 0, errno.ErrInvalidParam
		}
		return kindMerchant, shardNo, nil
	case typeChannelReceivable:
		if ownerType != ownerPlatform || !validChannelOwner(ownerID) {
			return 0, 0, errno.ErrInvalidParam
		}
		// 平台账户没有单一分片。返回的 0 不得写入汇总结果。
		return kindPlatform, 0, nil
	case typeFeeIncome, typeTransit:
		if ownerType != ownerPlatform || ownerID != platformOwnerID {
			return 0, 0, errno.ErrInvalidParam
		}
		// 平台账户没有单一分片。返回的 0 不得写入汇总结果。
		return kindPlatform, 0, nil
	default:
		return 0, 0, errno.ErrInvalidParam
	}
}

func getAccount(ctx context.Context, q accountLister, kind queryKind, shardNo int, ownerType, ownerID, accountType string) (Account, error) {
	switch kind {
	case kindMerchant:
		sub := 0
		rows, err := q.listAccounts(ctx, shardNo, ownerType, ownerID, accountType, &sub)
		if err != nil {
			return Account{}, internalErr(err)
		}
		return oneMerchant(rows, shardNo)
	case kindPlatform:
		// 两次查询各走各的连接。不放进跨库事务，也不按当前子账户个数截断。
		// 配置调小后，高位子账户不再被选中，余额仍要计入汇总。
		var all []Account
		for shardNo := 0; shardNo <= 1; shardNo++ {
			rows, err := q.listAccounts(ctx, shardNo, ownerType, ownerID, accountType, nil)
			if err != nil {
				return Account{}, internalErr(err)
			}
			all = append(all, rows...)
		}
		return sumPlatform(all, ownerType, ownerID, accountType)
	default:
		return Account{}, errno.ErrInvalidParam
	}
}

func oneMerchant(rows []Account, shardNo int) (Account, error) {
	if len(rows) == 0 {
		return Account{}, errno.ErrAccountNotFound
	}
	if len(rows) != 1 || rows[0].SubNo != 0 {
		return Account{}, errno.ErrInternal
	}
	row := rows[0]
	avail, err := money.Sub(row.Balance, row.FrozenAmount)
	if err != nil {
		return Account{}, errno.Wrap(errno.ErrInternal, err)
	}
	row.AvailableAmount = avail
	row.Shard = shardNo
	row.SubCount = 1
	return row, nil
}

func sumPlatform(rows []Account, ownerType, ownerID, accountType string) (Account, error) {
	if len(rows) == 0 {
		return Account{}, errno.ErrAccountNotFound
	}
	var (
		balance   money.Amount
		frozen    money.Amount
		direction string
		status    string
	)
	for i, row := range rows {
		if row.OwnerType != ownerType || row.OwnerID != ownerID || row.AccountType != accountType || row.Currency != currencyCNY {
			return Account{}, errno.ErrInternal
		}
		if i == 0 {
			direction = row.BalanceDirection
			status = row.Status
			continue
		}
		if row.BalanceDirection != direction || row.Status != status {
			return Account{}, errno.ErrInternal
		}
	}
	for _, row := range rows {
		var err error
		balance, err = money.Add(balance, row.Balance)
		if err != nil {
			return Account{}, errno.Wrap(errno.ErrInternal, err)
		}
		frozen, err = money.Add(frozen, row.FrozenAmount)
		if err != nil {
			return Account{}, errno.Wrap(errno.ErrInternal, err)
		}
	}
	// 可用金额用合计后再减，不把各行的可用金额相加。
	avail, err := money.Sub(balance, frozen)
	if err != nil {
		return Account{}, errno.Wrap(errno.ErrInternal, err)
	}
	return Account{
		AccountNo:        "",
		OwnerType:        ownerType,
		OwnerID:          ownerID,
		AccountType:      accountType,
		Currency:         currencyCNY,
		BalanceDirection: direction,
		Balance:          balance,
		FrozenAmount:     frozen,
		AvailableAmount:  avail,
		Status:           status,
		SubCount:         len(rows),
		// Shard 和 SubNo 置 0 表示省略，不是分片 0、子账户 0。
		// 汇总结果没有单一的分片号或子账户号。
		Shard: 0,
		SubNo: 0,
	}, nil
}

type poolLister struct {
	pools *dal.Pools
}

func (p poolLister) listAccounts(ctx context.Context, shardNo int, ownerType, ownerID, accountType string, subNo *int) ([]Account, error) {
	if p.pools == nil {
		return nil, errors.New("biz: nil pools")
	}
	db, err := p.pools.ShardDB(shardNo)
	if err != nil {
		return nil, err
	}
	return listAccounts(ctx, db, ownerType, ownerID, accountType, subNo)
}

const selectPlatformAccountsSQL = `
SELECT account_no, owner_type, owner_id, account_type, sub_no, currency,
       balance_direction, balance, frozen_amount, status
FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND currency = ?`

// listAccounts 只读一个连接上的账户行，不加锁，不用 FOR UPDATE。
// subNo 非 nil 时使用开户的五元组查询；为 nil 时读该所有者的全部子账户。
func listAccounts(ctx context.Context, db *sqlx.DB, ownerType, ownerID, accountType string, subNo *int) (out []Account, err error) {
	if db == nil {
		return nil, errNilDB
	}
	qctx, cancel := context.WithTimeout(ctx, accountReadTimeout)
	defer cancel()
	if subNo != nil {
		row, scanErr := scanAccount(db.QueryRowContext(qctx, selectAccountSQL,
			ownerType, ownerID, accountType, currencyCNY, *subNo))
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil, nil
		}
		if scanErr != nil {
			return nil, scanErr
		}
		return []Account{row}, nil
	}
	rows, err := db.QueryContext(qctx, selectPlatformAccountsSQL,
		ownerType, ownerID, accountType, currencyCNY)
	if err != nil {
		return nil, err
	}
	defer func() {
		cerr := rows.Close()
		if err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		row, scanErr := scanAccount(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanAccount(sc rowScanner) (Account, error) {
	var (
		row     Account
		balance int64
		frozen  int64
	)
	err := sc.Scan(
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
	if err != nil {
		return Account{}, err
	}
	row.Balance = money.Amount(balance)
	row.FrozenAmount = money.Amount(frozen)
	return row, nil
}
