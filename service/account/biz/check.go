package biz

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
	"github.com/yym108/pay-system/service/account/dal"
)

const (
	checkBalanceSum           = "BALANCE_SUM"
	checkLastBalanceAfter     = "LAST_BALANCE_AFTER"
	checkFrozenSum            = "FROZEN_SUM"
	checkAvailableNonNegative = "AVAILABLE_NON_NEGATIVE"
	checkTxnBalanced          = "TXN_BALANCED"
	checkGlobalBalanced       = "GLOBAL_BALANCED"
	checkTransitExpected      = "TRANSIT_EXPECTED"
	checkTransitZero          = "TRANSIT_ZERO"

	// snapshotEndTimeout 只用于结束快照。调用方 ctx 取消时仍要 ROLLBACK，避免连接带着未结束事务回池。
	snapshotEndTimeout = 3 * time.Second
)

// ledgerCheckFailures 是 pay_ledger_check_failures_total。
// 一次核对失败加 1，没有标签。HTTP 暴露留给 OPS-01。
var ledgerCheckFailures atomic.Uint64

const (
	setRepeatableReadSQL       = `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
	startConsistentSnapshotSQL = `START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT`
	rollbackSnapshotSQL        = `ROLLBACK`

	selectCheckAccountsSQL = `
SELECT id, account_no, balance_direction, balance, frozen_amount, allow_overdraft
FROM account
ORDER BY id`

	selectCheckMerchantSQL = `
SELECT id, account_no, balance_direction, balance, frozen_amount, allow_overdraft
FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND currency = ? AND sub_no = 0
ORDER BY id`

	selectEntriesFullSQL = `
SELECT account_id, direction, amount
FROM ledger_entry
ORDER BY account_id, id`

	selectEntriesMerchantSQL = `
SELECT account_id, direction, amount
FROM ledger_entry
WHERE account_id = ?
ORDER BY id`

	selectLastFullSQL = `
SELECT e.account_id, e.balance_after
FROM ledger_entry e
INNER JOIN (
  SELECT account_id, MAX(id) AS id
  FROM ledger_entry
  GROUP BY account_id
) last_entry ON last_entry.id = e.id`

	selectLastMerchantSQL = `
SELECT account_id, balance_after
FROM ledger_entry
WHERE id = (SELECT MAX(id) FROM ledger_entry WHERE account_id = ?)`

	selectFrozenFullSQL = `
SELECT account_id, amount
FROM account_freeze
WHERE status = 'FROZEN'
ORDER BY id`

	selectFrozenMerchantSQL = `
SELECT account_id, amount
FROM account_freeze
WHERE account_id = ? AND status = 'FROZEN'
ORDER BY id`

	selectTxnBalanceSQL = `
SELECT t.id, t.entry_count, e.id, e.direction, e.amount,
       CASE WHEN e.id IS NULL THEN 1 WHEN a.id IS NULL THEN 0 ELSE 1 END
FROM ledger_txn t
LEFT JOIN ledger_entry e ON e.txn_no = t.txn_no
LEFT JOIN account a ON a.id = e.account_id
ORDER BY t.id, e.id`

	selectConfirmedTransferFreezeSQL = `
SELECT amount
FROM account_freeze
WHERE biz_type = 'TRANSFER' AND status = 'CONFIRMED'`

	selectConfirmedPayeeSQL = `
SELECT amount
FROM transfer_branch
WHERE role = 'PAYEE' AND status = 'CONFIRMED'`

	selectTransitBalanceSQL = `
SELECT balance
FROM account
WHERE owner_type = 'PLATFORM' AND owner_id = 'PLATFORM' AND account_type = 'TRANSIT' AND currency = 'CNY'`
)

// CheckBalanceResult 是一次余额核对的结果。
// Passed 为 false 时 Failures 非空，且 error 为 nil。
type CheckBalanceResult struct {
	Passed   bool
	Failures []BalanceFailure
}

// BalanceFailure 是一条核对失败。
// Shard 为 nil、AccountNo 为空表示调用方省略该字段。金额单位是分。
type BalanceFailure struct {
	Shard     *int
	AccountNo string
	Check     string
	Expected  money.Amount
	Actual    money.Amount
}

type checkAccount struct {
	ID               uint64
	AccountNo        string
	BalanceDirection string
	Balance          money.Amount
	FrozenAmount     money.Amount
	AllowOverdraft   int
}

type checkLeg struct {
	AccountID uint64
	Direction string
	Amount    money.Amount
}

type checkLast struct {
	AccountID    uint64
	BalanceAfter money.Amount
}

type checkFreeze struct {
	AccountID uint64
	Amount    money.Amount
}

type checkTxnEntry struct {
	Direction     string
	Amount        money.Amount
	AccountExists bool
}

type checkTxn struct {
	ID         uint64
	EntryCount int
	Entries    []checkTxnEntry
}

type shardBook struct {
	Accounts                 []checkAccount
	Entries                  []checkLeg
	Lasts                    []checkLast
	Frozens                  []checkFreeze
	Txns                     []checkTxn
	ConfirmedTransferFreezes []money.Amount
	ConfirmedPayeeAmounts    []money.Amount
	TransitBalances          []money.Amount
}

// balanceSource 读取一个分片的只读快照。生产路径为 nil，走 sqlBalance。
type balanceSource interface {
	loadShard(ctx context.Context, shard int, merchantID string) (shardBook, error)
}

type snapshotQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type sqlBalance struct {
	pools *dal.Pools
}

// CheckBalance 只读核对余额。merchantID 为空时核对两个分片；非空时只核对这一户结算账户。
// 不写分录和账户，不调用外部系统。核对失败时 error 为 nil，Failures 列出全部差异，进程内计数加 1。
// 商户号不合法返回 10001。商户结算账户不存在返回 40001。依赖或数据损坏返回 10003。
func (s *Service) CheckBalance(ctx context.Context, merchantID string, expectQuiescent bool) (CheckBalanceResult, error) {
	full := merchantID == ""
	var shardNo int
	if !full {
		var err error
		shardNo, err = shard.Of(merchantID)
		if err != nil {
			return CheckBalanceResult{}, errno.ErrInvalidParam
		}
	}
	if s == nil || ctx == nil {
		return CheckBalanceResult{}, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return CheckBalanceResult{}, err
	}
	src := s.balance
	if src == nil {
		if s.pools == nil {
			return CheckBalanceResult{}, errno.ErrInternal
		}
		src = sqlBalance{pools: s.pools}
	}
	if full {
		return checkFull(ctx, src, expectQuiescent)
	}
	// 商户模式不看 expectQuiescent，也不核对凭证和在途。
	return checkMerchant(ctx, src, shardNo, merchantID)
}

func checkFull(ctx context.Context, src balanceSource, expectQuiescent bool) (CheckBalanceResult, error) {
	var failures []BalanceFailure
	var transit [2]money.Amount
	for shard := 0; shard < 2; shard++ {
		book, err := src.loadShard(ctx, shard, "")
		if err != nil {
			return CheckBalanceResult{}, internalErr(err)
		}
		part, sum, err := judgeShard(shard, book, true)
		if err != nil {
			return CheckBalanceResult{}, internalErr(err)
		}
		failures = append(failures, part...)
		transit[shard] = sum
	}
	if expectQuiescent {
		// 两个分片是两次独立快照。只有调用方声明已经静止，才把在途余额加总后和 0 比较。
		total, err := money.Add(transit[0], transit[1])
		if err != nil {
			return CheckBalanceResult{}, internalErr(err)
		}
		if total != 0 {
			failures = append(failures, BalanceFailure{
				Check:    checkTransitZero,
				Expected: 0,
				Actual:   total,
			})
		}
	}
	return finishBalance(ctx, failures, "")
}

func checkMerchant(ctx context.Context, src balanceSource, shardNo int, merchantID string) (CheckBalanceResult, error) {
	book, err := src.loadShard(ctx, shardNo, merchantID)
	if err != nil {
		return CheckBalanceResult{}, internalErr(err)
	}
	if len(book.Accounts) == 0 {
		return CheckBalanceResult{}, errno.ErrAccountNotFound
	}
	if len(book.Accounts) != 1 {
		return CheckBalanceResult{}, internalErr(errors.New("biz: merchant settle account is not unique"))
	}
	failures, _, err := judgeShard(shardNo, book, false)
	if err != nil {
		return CheckBalanceResult{}, internalErr(err)
	}
	return finishBalance(ctx, failures, merchantID)
}

func finishBalance(ctx context.Context, failures []BalanceFailure, merchantID string) (CheckBalanceResult, error) {
	if len(failures) == 0 {
		return CheckBalanceResult{Passed: true}, nil
	}
	ledgerCheckFailures.Add(1)
	if merchantID != "" {
		slog.ErrorContext(ctx, "balance check failed", slog.String("merchant_id", merchantID))
	} else {
		slog.ErrorContext(ctx, "balance check failed")
	}
	return CheckBalanceResult{Passed: false, Failures: failures}, nil
}

type legSum struct {
	debit  money.Amount
	credit money.Amount
}

func judgeShard(shard int, book shardBook, full bool) ([]BalanceFailure, money.Amount, error) {
	accounts := append([]checkAccount(nil), book.Accounts...)
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].ID < accounts[j].ID })
	for i := 1; i < len(accounts); i++ {
		if accounts[i].ID == accounts[i-1].ID {
			return nil, 0, errors.New("biz: duplicate account id")
		}
	}

	byAccount := make(map[uint64]*legSum, len(book.Entries))
	var debitTotal, creditTotal money.Amount
	for _, leg := range book.Entries {
		nextDebit, nextCredit, err := addLeg(debitTotal, creditTotal, leg.Direction, leg.Amount)
		if err != nil {
			return nil, 0, err
		}
		debitTotal, creditTotal = nextDebit, nextCredit
		sum := byAccount[leg.AccountID]
		if sum == nil {
			sum = &legSum{}
			byAccount[leg.AccountID] = sum
		}
		sum.debit, sum.credit, err = addLeg(sum.debit, sum.credit, leg.Direction, leg.Amount)
		if err != nil {
			return nil, 0, err
		}
	}

	lasts := make(map[uint64]money.Amount, len(book.Lasts))
	for _, last := range book.Lasts {
		if _, ok := lasts[last.AccountID]; ok {
			return nil, 0, errors.New("biz: duplicate last entry")
		}
		lasts[last.AccountID] = last.BalanceAfter
	}
	frozen := make(map[uint64]money.Amount, len(book.Frozens))
	for _, row := range book.Frozens {
		sum, err := money.Add(frozen[row.AccountID], row.Amount)
		if err != nil {
			return nil, 0, err
		}
		frozen[row.AccountID] = sum
	}

	var failures []BalanceFailure
	for _, acc := range accounts {
		part, err := judgeAccount(shard, acc, byAccount[acc.ID], lasts, frozen)
		if err != nil {
			return nil, 0, err
		}
		failures = append(failures, part...)
	}
	if !full {
		return failures, 0, nil
	}

	txnFailures, err := judgeTxns(shard, book.Txns)
	if err != nil {
		return nil, 0, err
	}
	failures = append(failures, txnFailures...)
	if debitTotal != creditTotal {
		failures = append(failures, balanceFailure(shard, "", checkGlobalBalanced, debitTotal, creditTotal))
	}
	expected, actual, err := transitPair(book)
	if err != nil {
		return nil, 0, err
	}
	if expected != actual {
		failures = append(failures, balanceFailure(shard, "", checkTransitExpected, expected, actual))
	}
	return failures, actual, nil
}

func addLeg(debit, credit money.Amount, direction string, amount money.Amount) (money.Amount, money.Amount, error) {
	switch direction {
	case directionDebit:
		next, err := money.Add(debit, amount)
		return next, credit, err
	case directionCredit:
		next, err := money.Add(credit, amount)
		return debit, next, err
	default:
		return 0, 0, errors.New("biz: unknown entry direction")
	}
}

func judgeAccount(shard int, acc checkAccount, sum *legSum, lasts map[uint64]money.Amount, frozen map[uint64]money.Amount) ([]BalanceFailure, error) {
	var debit, credit money.Amount
	if sum != nil {
		debit, credit = sum.debit, sum.credit
	}
	var expected money.Amount
	var err error
	switch acc.BalanceDirection {
	case directionCredit:
		expected, err = money.Sub(credit, debit)
	case directionDebit:
		expected, err = money.Sub(debit, credit)
	default:
		return nil, errors.New("biz: unknown balance direction")
	}
	if err != nil {
		return nil, err
	}
	var failures []BalanceFailure
	if expected != acc.Balance {
		failures = append(failures, balanceFailure(shard, acc.AccountNo, checkBalanceSum, expected, acc.Balance))
	}
	if after, ok := lasts[acc.ID]; ok {
		if after != acc.Balance {
			failures = append(failures, balanceFailure(shard, acc.AccountNo, checkLastBalanceAfter, after, acc.Balance))
		}
	} else if acc.Balance != 0 {
		failures = append(failures, balanceFailure(shard, acc.AccountNo, checkLastBalanceAfter, 0, acc.Balance))
	}
	frozenSum := frozen[acc.ID]
	if frozenSum != acc.FrozenAmount {
		failures = append(failures, balanceFailure(shard, acc.AccountNo, checkFrozenSum, frozenSum, acc.FrozenAmount))
	}
	if acc.AllowOverdraft != overdraftNo {
		return failures, nil
	}
	available, err := money.Sub(acc.Balance, acc.FrozenAmount)
	if err != nil {
		return nil, err
	}
	if available < 0 {
		failures = append(failures, balanceFailure(shard, acc.AccountNo, checkAvailableNonNegative, 0, available))
	}
	return failures, nil
}

func judgeTxns(shard int, txns []checkTxn) ([]BalanceFailure, error) {
	ordered := append([]checkTxn(nil), txns...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	var failures []BalanceFailure
	for i, txn := range ordered {
		if i > 0 && txn.ID == ordered[i-1].ID {
			return nil, errors.New("biz: duplicate txn id")
		}
		var debit, credit money.Amount
		missing := 0
		for _, entry := range txn.Entries {
			var err error
			debit, credit, err = addLeg(debit, credit, entry.Direction, entry.Amount)
			if err != nil {
				return nil, err
			}
			if !entry.AccountExists {
				missing++
			}
		}
		if debit != credit {
			failures = append(failures, balanceFailure(shard, "", checkTxnBalanced, debit, credit))
		}
		if txn.EntryCount != len(txn.Entries) {
			failures = append(failures, balanceFailure(shard, "", checkTxnBalanced, money.Amount(txn.EntryCount), money.Amount(len(txn.Entries))))
		}
		if missing > 0 {
			failures = append(failures, balanceFailure(shard, "", checkTxnBalanced, 0, money.Amount(missing)))
		}
	}
	return failures, nil
}

func transitPair(book shardBook) (money.Amount, money.Amount, error) {
	freezeSum, err := sumAmounts(book.ConfirmedTransferFreezes)
	if err != nil {
		return 0, 0, err
	}
	payeeSum, err := sumAmounts(book.ConfirmedPayeeAmounts)
	if err != nil {
		return 0, 0, err
	}
	actual, err := sumAmounts(book.TransitBalances)
	if err != nil {
		return 0, 0, err
	}
	expected, err := money.Sub(freezeSum, payeeSum)
	if err != nil {
		return 0, 0, err
	}
	return expected, actual, nil
}

func sumAmounts(amounts []money.Amount) (money.Amount, error) {
	var total money.Amount
	for _, amount := range amounts {
		next, err := money.Add(total, amount)
		if err != nil {
			return 0, err
		}
		total = next
	}
	return total, nil
}

func balanceFailure(shard int, accountNo, check string, expected, actual money.Amount) BalanceFailure {
	shardNo := shard
	return BalanceFailure{
		Shard:     &shardNo,
		AccountNo: accountNo,
		Check:     check,
		Expected:  expected,
		Actual:    actual,
	}
}

func (s sqlBalance) loadShard(ctx context.Context, shard int, merchantID string) (book shardBook, err error) {
	if s.pools == nil {
		return shardBook{}, errors.New("biz: nil pools")
	}
	db, err := s.pools.ShardDB(shard)
	if err != nil {
		return shardBook{}, err
	}
	// 不用 dbx.WithTx：它是 READ COMMITTED，而且 BeginTx 发不出 WITH CONSISTENT SNAPSHOT。
	conn, err := db.Connx(ctx)
	if err != nil {
		return shardBook{}, err
	}
	defer func() {
		cerr := conn.Close()
		if err == nil && cerr != nil {
			err = cerr
		}
	}()
	if _, err = conn.ExecContext(ctx, setRepeatableReadSQL); err != nil {
		return shardBook{}, err
	}
	if _, err = conn.ExecContext(ctx, startConsistentSnapshotSQL); err != nil {
		return shardBook{}, err
	}
	defer func() {
		rbErr := rollbackSnapshot(ctx, conn)
		if err == nil && rbErr != nil {
			err = rbErr
		}
	}()
	book, err = querySnapshot(ctx, conn, merchantID)
	return book, err
}

func rollbackSnapshot(ctx context.Context, conn *sqlx.Conn) error {
	rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotEndTimeout)
	defer cancel()
	if _, err := conn.ExecContext(rbCtx, rollbackSnapshotSQL); err != nil {
		discardConn(conn)
		return err
	}
	return nil
}

func discardConn(conn *sqlx.Conn) {
	_ = conn.Raw(func(dc any) error {
		closer, ok := dc.(interface{ Close() error })
		if !ok {
			return nil
		}
		return closer.Close()
	})
}

func querySnapshot(ctx context.Context, q snapshotQuerier, merchantID string) (shardBook, error) {
	// 不读 @@transaction_isolation。一次性 SET TRANSACTION 不改会话变量，会话仍是 DSN 的 READ-COMMITTED。
	var book shardBook
	var err error
	if merchantID == "" {
		book.Accounts, err = queryAccounts(ctx, q, selectCheckAccountsSQL)
	} else {
		book.Accounts, err = queryAccounts(ctx, q, selectCheckMerchantSQL,
			ownerMerchant, merchantID, typeMerchantSettle, currencyCNY)
	}
	if err != nil {
		return shardBook{}, err
	}
	if merchantID != "" {
		if len(book.Accounts) != 1 {
			return book, nil
		}
		id := book.Accounts[0].ID
		if book.Entries, err = queryLegs(ctx, q, selectEntriesMerchantSQL, id); err != nil {
			return shardBook{}, err
		}
		if book.Lasts, err = queryLasts(ctx, q, selectLastMerchantSQL, id); err != nil {
			return shardBook{}, err
		}
		book.Frozens, err = queryFrozens(ctx, q, selectFrozenMerchantSQL, id)
		return book, err
	}
	if book.Entries, err = queryLegs(ctx, q, selectEntriesFullSQL); err != nil {
		return shardBook{}, err
	}
	if book.Lasts, err = queryLasts(ctx, q, selectLastFullSQL); err != nil {
		return shardBook{}, err
	}
	if book.Frozens, err = queryFrozens(ctx, q, selectFrozenFullSQL); err != nil {
		return shardBook{}, err
	}
	if book.Txns, err = queryTxns(ctx, q); err != nil {
		return shardBook{}, err
	}
	if book.ConfirmedTransferFreezes, err = queryAmounts(ctx, q, selectConfirmedTransferFreezeSQL); err != nil {
		return shardBook{}, err
	}
	if book.ConfirmedPayeeAmounts, err = queryAmounts(ctx, q, selectConfirmedPayeeSQL); err != nil {
		return shardBook{}, err
	}
	book.TransitBalances, err = queryAmounts(ctx, q, selectTransitBalanceSQL)
	return book, err
}

func queryAccounts(ctx context.Context, q snapshotQuerier, query string, args ...any) ([]checkAccount, error) {
	var out []checkAccount
	err := queryRows(ctx, q, query, args, func(rows *sql.Rows) error {
		var (
			row       checkAccount
			balance   int64
			frozen    int64
			overdraft int
		)
		if err := rows.Scan(&row.ID, &row.AccountNo, &row.BalanceDirection, &balance, &frozen, &overdraft); err != nil {
			return err
		}
		row.Balance = money.Amount(balance)
		row.FrozenAmount = money.Amount(frozen)
		row.AllowOverdraft = overdraft
		out = append(out, row)
		return nil
	})
	return out, err
}

func queryLegs(ctx context.Context, q snapshotQuerier, query string, args ...any) ([]checkLeg, error) {
	var out []checkLeg
	err := queryRows(ctx, q, query, args, func(rows *sql.Rows) error {
		var (
			leg    checkLeg
			amount int64
		)
		if err := rows.Scan(&leg.AccountID, &leg.Direction, &amount); err != nil {
			return err
		}
		leg.Amount = money.Amount(amount)
		out = append(out, leg)
		return nil
	})
	return out, err
}

func queryLasts(ctx context.Context, q snapshotQuerier, query string, args ...any) ([]checkLast, error) {
	var out []checkLast
	err := queryRows(ctx, q, query, args, func(rows *sql.Rows) error {
		var (
			last   checkLast
			amount int64
		)
		if err := rows.Scan(&last.AccountID, &amount); err != nil {
			return err
		}
		last.BalanceAfter = money.Amount(amount)
		out = append(out, last)
		return nil
	})
	return out, err
}

func queryFrozens(ctx context.Context, q snapshotQuerier, query string, args ...any) ([]checkFreeze, error) {
	var out []checkFreeze
	err := queryRows(ctx, q, query, args, func(rows *sql.Rows) error {
		var (
			row    checkFreeze
			amount int64
		)
		if err := rows.Scan(&row.AccountID, &amount); err != nil {
			return err
		}
		row.Amount = money.Amount(amount)
		out = append(out, row)
		return nil
	})
	return out, err
}

func queryAmounts(ctx context.Context, q snapshotQuerier, query string, args ...any) ([]money.Amount, error) {
	var out []money.Amount
	err := queryRows(ctx, q, query, args, func(rows *sql.Rows) error {
		var amount int64
		if err := rows.Scan(&amount); err != nil {
			return err
		}
		out = append(out, money.Amount(amount))
		return nil
	})
	return out, err
}

func queryTxns(ctx context.Context, q snapshotQuerier) ([]checkTxn, error) {
	var book shardBook
	err := queryRows(ctx, q, selectTxnBalanceSQL, nil, func(rows *sql.Rows) error {
		var (
			txnID      uint64
			entryCount int
			entryID    sql.NullInt64
			direction  sql.NullString
			amount     sql.NullInt64
			accountOK  int
		)
		if err := rows.Scan(&txnID, &entryCount, &entryID, &direction, &amount, &accountOK); err != nil {
			return err
		}
		return appendTxnScan(&book, txnID, entryCount, entryID, direction, amount, accountOK)
	})
	if err != nil {
		return nil, err
	}
	return book.Txns, nil
}

func appendTxnScan(book *shardBook, txnID uint64, entryCount int, entryID sql.NullInt64, direction sql.NullString, amount sql.NullInt64, accountOK int) error {
	n := len(book.Txns)
	if n == 0 || book.Txns[n-1].ID != txnID {
		if n > 0 && txnID <= book.Txns[n-1].ID {
			return errors.New("biz: ledger txn id is not increasing")
		}
		book.Txns = append(book.Txns, checkTxn{ID: txnID, EntryCount: entryCount})
	} else if book.Txns[n-1].EntryCount != entryCount {
		return errors.New("biz: ledger txn entry_count conflict")
	}
	if !entryID.Valid {
		return nil
	}
	if !direction.Valid || !amount.Valid || (accountOK != 0 && accountOK != 1) {
		return errors.New("biz: ledger entry is incomplete")
	}
	txn := &book.Txns[len(book.Txns)-1]
	txn.Entries = append(txn.Entries, checkTxnEntry{
		Direction:     direction.String,
		Amount:        money.Amount(amount.Int64),
		AccountExists: accountOK == 1,
	})
	return nil
}

func queryRows(ctx context.Context, q snapshotQuerier, query string, args []any, scan func(*sql.Rows) error) (err error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() {
		cerr := rows.Close()
		if err == nil && cerr != nil {
			err = cerr
		}
	}()
	for rows.Next() {
		if err = scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}
