package biz

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

const (
	bizPay         = "PAY"
	bizRefund      = "REFUND"
	bizTransfer    = "TRANSFER"
	bizTransferOut = "TRANSFER_OUT"
	bizTransferIn  = "TRANSFER_IN"
	txnNoPrefix    = "TX"
	minPostEntries = 2
	maxPostEntries = 10
	maxBizNoRunes  = 64
	maxMemoRunes   = 255
)

// Entry 是一条分录。Direction 是这一笔的借贷方向，不是账户的余额方向。
type Entry struct {
	AccountID uint64
	Direction string
	Amount    money.Amount
}

// PostResult 是记账结果。Duplicated 为 true 时 TxnNo 属于已有凭证，本次没有改余额。
type PostResult struct {
	TxnNo      string
	Duplicated bool
}

type postRequest struct {
	TxnNo   string
	BizType string
	BizNo   string
	Memo    string
	Entries []Entry
	Debit   money.Amount
}

type ledgerTxnInsert struct {
	TxnNo      string
	BizType    string
	BizNo      string
	Amount     money.Amount
	Currency   string
	EntryCount int
	Memo       string
}

type lockedAccount struct {
	ID               uint64
	BalanceDirection string
	Balance          money.Amount
	FrozenAmount     money.Amount
	AllowOverdraft   int
	Status           string
	Currency         string
}

type ledgerEntryRow struct {
	TxnNo        string
	AccountID    uint64
	Direction    string
	Amount       money.Amount
	BalanceAfter money.Amount
	BizType      string
	BizNo        string
}

type preparedEntry struct {
	entry    Entry
	delta    money.Amount
	decrease bool
	guard    *money.Amount
}

// postTx 是调用方已经打开的同一个分片事务。
// 实现不得再 BEGIN，也不得换连接。假实现只给不连数据库的单元测试。
type postTx interface {
	insertTxn(ctx context.Context, row ledgerTxnInsert) error
	selectTxnNo(ctx context.Context, bizType, bizNo string) (string, bool, error)
	lockAccount(ctx context.Context, accountID uint64) (lockedAccount, bool, error)
	updateBalance(ctx context.Context, accountID uint64, delta money.Amount, guard *money.Amount) (bool, error)
	selectBalance(ctx context.Context, accountID uint64) (money.Amount, bool, error)
	reloadAccount(ctx context.Context, accountID uint64) (lockedAccount, bool, error)
	insertEntries(ctx context.Context, rows []ledgerEntryRow) error
}

// Post 在调用方已经打开的分片事务里记一笔复式账。
// 不调用 BEGIN、COMMIT、ROLLBACK，也不调用 dbx.WithTx。
// 校验失败返回 40003，不发号、不写行。其它失败把错误交给调用方回滚。
// 1062 不回滚调用方事务，同一事务里返回已有 txn_no。
// 本连接上看不到的 account_id 返回 40003，不是 40001。
func (s *Service) Post(ctx context.Context, tx *sqlx.Tx, bizType, bizNo, memo string, entries []Entry) (PostResult, error) {
	if s == nil {
		return PostResult{}, errno.ErrInternal
	}
	debit, err := validatePost(bizType, bizNo, memo, entries)
	if err != nil {
		return PostResult{}, err
	}
	if ctx == nil {
		return PostResult{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return PostResult{}, err
	}
	if tx == nil || s.ids == nil {
		return PostResult{}, errno.ErrInternal
	}
	// 发号可能要等下一毫秒。放在行锁之前，避免占着账户行。
	txnNo, err := s.ids.NewNo(ctx, txnNoPrefix)
	if err != nil {
		return PostResult{}, mapIDError(err)
	}
	return applyPost(ctx, sqlPostTx{tx: tx}, postRequest{
		TxnNo:   txnNo,
		BizType: bizType,
		BizNo:   bizNo,
		Memo:    memo,
		Entries: entries,
		Debit:   debit,
	})
}

func validatePost(bizType, bizNo, memo string, entries []Entry) (money.Amount, error) {
	if !allowedBizType(bizType) {
		return 0, errno.ErrUnbalancedEntries
	}
	if n := utf8.RuneCountInString(bizNo); n == 0 || n > maxBizNoRunes {
		return 0, errno.ErrUnbalancedEntries
	}
	if utf8.RuneCountInString(memo) > maxMemoRunes {
		return 0, errno.ErrUnbalancedEntries
	}
	if len(entries) < minPostEntries || len(entries) > maxPostEntries {
		return 0, errno.ErrUnbalancedEntries
	}
	seen := make(map[uint64]struct{}, len(entries))
	var debit, credit money.Amount
	for _, entry := range entries {
		if entry.Amount <= 0 || (entry.Direction != directionDebit && entry.Direction != directionCredit) {
			return 0, errno.ErrUnbalancedEntries
		}
		if _, ok := seen[entry.AccountID]; ok {
			return 0, errno.ErrUnbalancedEntries
		}
		seen[entry.AccountID] = struct{}{}
		var err error
		if entry.Direction == directionDebit {
			debit, err = money.Add(debit, entry.Amount)
		} else {
			credit, err = money.Add(credit, entry.Amount)
		}
		if err != nil {
			// 合计溢出按分录非法处理，不回绕，也不换成别的码。
			return 0, errno.ErrUnbalancedEntries
		}
	}
	if debit != credit {
		return 0, errno.ErrUnbalancedEntries
	}
	return debit, nil
}

func allowedBizType(bizType string) bool {
	switch bizType {
	case bizPay, bizRefund, bizTransfer, bizTransferOut, bizTransferIn:
		return true
	default:
		// REVERSAL 必须带 reversal_of，那是 ACC-08。这里没有该参数，写进去会撞 CHECK。
		return false
	}
}

func applyPost(ctx context.Context, db postTx, req postRequest) (PostResult, error) {
	err := db.insertTxn(ctx, ledgerTxnInsert{
		TxnNo:      req.TxnNo,
		BizType:    req.BizType,
		BizNo:      req.BizNo,
		Amount:     req.Debit,
		Currency:   currencyCNY,
		EntryCount: len(req.Entries),
		Memo:       req.Memo,
	})
	if dbx.IsDuplicate(err) {
		// 1062 只让这一条 INSERT 失败，InnoDB 事务仍然开着。不在这里回滚。
		return readDuplicate(ctx, db, req.BizType, req.BizNo, err)
	}
	if err != nil {
		return PostResult{}, internalErr(err)
	}
	ordered := sortEntries(req.Entries)
	locked := make([]lockedAccount, 0, len(ordered))
	for _, entry := range ordered {
		// 按主键升序加锁，两笔凭证交叉更新时不会死锁。
		row, found, lockErr := db.lockAccount(ctx, entry.AccountID)
		if lockErr != nil {
			return PostResult{}, internalErr(lockErr)
		}
		if !found {
			// 另一分片的账户在本连接上同样没有行，调用方分不清这两种情况。
			return PostResult{}, errno.ErrUnbalancedEntries
		}
		locked = append(locked, row)
	}
	planned, err := planBalances(ordered, locked)
	if err != nil {
		return PostResult{}, err
	}
	after := make([]money.Amount, len(planned))
	for i, item := range planned {
		ok, updateErr := db.updateBalance(ctx, item.entry.AccountID, item.delta, item.guard)
		if updateErr != nil {
			return PostResult{}, internalErr(updateErr)
		}
		if !ok {
			return PostResult{}, classifyNoUpdate(ctx, db, item)
		}
		balance, found, readErr := db.selectBalance(ctx, item.entry.AccountID)
		if readErr != nil {
			return PostResult{}, internalErr(readErr)
		}
		if !found {
			return PostResult{}, errno.ErrInternal
		}
		after[i] = balance
	}
	rows := make([]ledgerEntryRow, len(planned))
	for i, item := range planned {
		rows[i] = ledgerEntryRow{
			TxnNo:        req.TxnNo,
			AccountID:    item.entry.AccountID,
			Direction:    item.entry.Direction,
			Amount:       item.entry.Amount,
			BalanceAfter: after[i],
			BizType:      req.BizType,
			BizNo:        req.BizNo,
		}
	}
	if err := db.insertEntries(ctx, rows); err != nil {
		return PostResult{}, internalErr(err)
	}
	return PostResult{TxnNo: req.TxnNo, Duplicated: false}, nil
}

func readDuplicate(ctx context.Context, db postTx, bizType, bizNo string, dup error) (PostResult, error) {
	txnNo, found, err := db.selectTxnNo(ctx, bizType, bizNo)
	if err != nil {
		return PostResult{}, internalErr(err)
	}
	if !found {
		return PostResult{}, errno.Wrap(errno.ErrInternal, dup)
	}
	return PostResult{TxnNo: txnNo, Duplicated: true}, nil
}

func sortEntries(entries []Entry) []Entry {
	ordered := make([]Entry, len(entries))
	copy(ordered, entries)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].AccountID < ordered[j].AccountID
	})
	return ordered
}

func planBalances(entries []Entry, rows []lockedAccount) ([]preparedEntry, error) {
	if len(entries) != len(rows) {
		return nil, errno.ErrInternal
	}
	// 行锁已经按 id 升序拿齐。先看完全部账户的币种和状态，较小 id 的 40002 不能挡住较大 id 的 40005。
	for i, entry := range entries {
		row := rows[i]
		if row.ID != entry.AccountID {
			return nil, errno.ErrInternal
		}
		if row.Currency != currencyCNY {
			return nil, errno.ErrUnbalancedEntries
		}
		if row.Status != statusActive {
			return nil, errno.ErrAccountInactive
		}
	}
	out := make([]preparedEntry, len(entries))
	for i, entry := range entries {
		row := rows[i]
		decrease, err := entryDecreases(row.BalanceDirection, entry.Direction)
		if err != nil {
			return nil, err
		}
		delta := entry.Amount
		if decrease {
			delta = -entry.Amount
		}
		// 先在内存里加，溢出就不要把会回绕的增量交给 SQL。
		if _, err := money.Add(row.Balance, delta); err != nil {
			return nil, errno.Wrap(errno.ErrInternal, err)
		}
		var guard *money.Amount
		if decrease && row.AllowOverdraft == overdraftNo {
			avail, subErr := money.Sub(row.Balance, row.FrozenAmount)
			if subErr != nil {
				return nil, errno.Wrap(errno.ErrInternal, subErr)
			}
			if avail < entry.Amount {
				return nil, errno.ErrInsufficientBalance
			}
			amount := entry.Amount
			guard = &amount
		}
		out[i] = preparedEntry{entry: entry, delta: delta, decrease: decrease, guard: guard}
	}
	return out, nil
}

func entryDecreases(balanceDirection, entryDirection string) (bool, error) {
	switch balanceDirection {
	case directionCredit:
		return entryDirection == directionDebit, nil
	case directionDebit:
		return entryDirection == directionCredit, nil
	default:
		return false, errno.ErrInternal
	}
}

func classifyNoUpdate(ctx context.Context, db postTx, item preparedEntry) error {
	row, found, err := db.reloadAccount(ctx, item.entry.AccountID)
	if err != nil {
		return internalErr(err)
	}
	if !found {
		return errno.ErrUnbalancedEntries
	}
	if row.Status != statusActive {
		return errno.ErrAccountInactive
	}
	if item.decrease && row.AllowOverdraft == overdraftNo {
		avail, subErr := money.Sub(row.Balance, row.FrozenAmount)
		if subErr != nil {
			return errno.Wrap(errno.ErrInternal, subErr)
		}
		if avail < item.entry.Amount {
			return errno.ErrInsufficientBalance
		}
	}
	return errno.ErrInternal
}

const insertTxnSQL = `
INSERT INTO ledger_txn (
  txn_no, biz_type, biz_no, amount, currency, entry_count, memo
) VALUES (?, ?, ?, ?, ?, ?, ?)`

const selectTxnNoSQL = `
SELECT txn_no FROM ledger_txn WHERE biz_type = ? AND biz_no = ?`

const lockAccountSQL = `
SELECT id, balance_direction, balance, frozen_amount, allow_overdraft, status, currency
FROM account
WHERE id = ?
FOR UPDATE`

const reloadAccountSQL = `
SELECT id, balance_direction, balance, frozen_amount, allow_overdraft, status, currency
FROM account
WHERE id = ?`

const updateBalanceSQL = `
UPDATE account
SET balance = balance + ?, version = version + 1
WHERE id = ? AND status = 'ACTIVE'`

const updateBalanceGuardSQL = `
UPDATE account
SET balance = balance + ?, version = version + 1
WHERE id = ? AND status = 'ACTIVE' AND balance - frozen_amount >= ?`

const selectBalanceSQL = `
SELECT balance FROM account WHERE id = ?`

type sqlPostTx struct {
	tx *sqlx.Tx
}

func (s sqlPostTx) insertTxn(ctx context.Context, row ledgerTxnInsert) error {
	if s.tx == nil {
		return errNilTx
	}
	_, err := s.tx.ExecContext(ctx, insertTxnSQL,
		row.TxnNo,
		row.BizType,
		row.BizNo,
		int64(row.Amount),
		row.Currency,
		row.EntryCount,
		row.Memo,
	)
	return err
}

func (s sqlPostTx) selectTxnNo(ctx context.Context, bizType, bizNo string) (string, bool, error) {
	if s.tx == nil {
		return "", false, errNilTx
	}
	var txnNo string
	err := s.tx.QueryRowContext(ctx, selectTxnNoSQL, bizType, bizNo).Scan(&txnNo)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return txnNo, true, nil
}

func (s sqlPostTx) lockAccount(ctx context.Context, accountID uint64) (lockedAccount, bool, error) {
	return s.queryAccount(ctx, lockAccountSQL, accountID)
}

func (s sqlPostTx) reloadAccount(ctx context.Context, accountID uint64) (lockedAccount, bool, error) {
	return s.queryAccount(ctx, reloadAccountSQL, accountID)
}

func (s sqlPostTx) queryAccount(ctx context.Context, query string, accountID uint64) (lockedAccount, bool, error) {
	if s.tx == nil {
		return lockedAccount{}, false, errNilTx
	}
	row, err := scanLocked(s.tx.QueryRowContext(ctx, query, accountID))
	if errors.Is(err, sql.ErrNoRows) {
		return lockedAccount{}, false, nil
	}
	if err != nil {
		return lockedAccount{}, false, err
	}
	return row, true, nil
}

func (s sqlPostTx) updateBalance(ctx context.Context, accountID uint64, delta money.Amount, guard *money.Amount) (bool, error) {
	if s.tx == nil {
		return false, errNilTx
	}
	var (
		res sql.Result
		err error
	)
	if guard == nil {
		res, err = s.tx.ExecContext(ctx, updateBalanceSQL, int64(delta), accountID)
	} else {
		res, err = s.tx.ExecContext(ctx, updateBalanceGuardSQL, int64(delta), accountID, int64(*guard))
	}
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
		return false, fmt.Errorf("biz: balance update affected %d", n)
	}
	return true, nil
}

func (s sqlPostTx) selectBalance(ctx context.Context, accountID uint64) (money.Amount, bool, error) {
	if s.tx == nil {
		return 0, false, errNilTx
	}
	var balance int64
	err := s.tx.QueryRowContext(ctx, selectBalanceSQL, accountID).Scan(&balance)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return money.Amount(balance), true, nil
}

func (s sqlPostTx) insertEntries(ctx context.Context, rows []ledgerEntryRow) error {
	if s.tx == nil {
		return errNilTx
	}
	query, args := buildEntryInsert(rows)
	if query == "" {
		return errors.New("biz: no entries")
	}
	_, err := s.tx.ExecContext(ctx, query, args...)
	return err
}

func buildEntryInsert(rows []ledgerEntryRow) (string, []any) {
	if len(rows) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO ledger_entry (txn_no, account_id, direction, amount, balance_after, biz_type, biz_no) VALUES ")
	args := make([]any, 0, len(rows)*7)
	for i, row := range rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?, ?, ?, ?, ?, ?, ?)")
		args = append(args,
			row.TxnNo,
			row.AccountID,
			row.Direction,
			int64(row.Amount),
			int64(row.BalanceAfter),
			row.BizType,
			row.BizNo,
		)
	}
	return b.String(), args
}

func scanLocked(sc rowScanner) (lockedAccount, error) {
	var (
		row       lockedAccount
		balance   int64
		frozen    int64
		overdraft int
	)
	err := sc.Scan(
		&row.ID,
		&row.BalanceDirection,
		&balance,
		&frozen,
		&overdraft,
		&row.Status,
		&row.Currency,
	)
	if err != nil {
		return lockedAccount{}, err
	}
	row.Balance = money.Amount(balance)
	row.FrozenAmount = money.Amount(frozen)
	row.AllowOverdraft = overdraft
	return row, nil
}
