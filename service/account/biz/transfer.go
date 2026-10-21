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
	statusSuccess           = "SUCCESS"
	statusFailed            = "FAILED"
	roleLocal               = "LOCAL"
	failInsufficientBalance = "INSUFFICIENT_BALANCE"
	maxTransferNoRunes      = 32
)

// errBranchDuplicate 让 WithTx 回滚。
// 1062 不会中止 InnoDB 事务，不能在未回滚时继续记账。文本里没有 SQL。
var errBranchDuplicate = errors.New("transfer branch already exists")

// TransferLocalResult 是同分片转账的结果。没有进程内 duplicated 字段。
// 成功时 Status 为 SUCCESS，TxnNo 非空，FailCode 为空，error 为 nil。
// 余额不足的确定失败 Status 为 FAILED，FailCode 为 INSUFFICIENT_BALANCE，TxnNo 为空，error 为 nil。
type TransferLocalResult struct {
	Status   string
	TxnNo    string
	FailCode string
}

type localInsert struct {
	TransferNo string
	Payer      string
	Payee      string
	Amount     money.Amount
}

type localBranch struct {
	Payer    string
	Payee    string
	Amount   money.Amount
	Status   string
	FailCode string
	TxnNo    string
}

type settleAccount struct {
	ID     uint64
	Status string
}

// localSession 是一段分片事务里的分支写入和记账。
// 实现不得另开连接。假实现只给不连数据库的单元测试。
type localSession interface {
	insertSuccess(ctx context.Context, row localInsert) error
	insertFailed(ctx context.Context, row localInsert) error
	writeTxnNo(ctx context.Context, row localInsert, txnNo string) (bool, error)
	post(ctx context.Context, payerID, payeeID uint64, amount money.Amount, transferNo string) (PostResult, error)
}

// localReader 读取已提交的 LOCAL 分支。1062 回滚之后才用它。
type localReader interface {
	selectLocal(ctx context.Context, transferNo string) (localBranch, bool, error)
}

// localRun 提交或回滚一段事务。fn 返回错误时不得提交。
type localRun func(ctx context.Context, fn func(localSession) error) error

// TransferLocal 在双方共同分片把付款商户的可用余额转给收款商户。
// 事务由本方法用 dbx.WithTx 提交。Post 仍不提交。
// 余额不足是确定失败，不把 40002 返回给调用方。同一 transfer_no 之后只返回第一次的结果。
// 参数不合法返回 10001，不写行。
func (s *Service) TransferLocal(ctx context.Context, transferNo, payerMerchantID, payeeMerchantID string, amount money.Amount) (TransferLocalResult, error) {
	shardNo, err := validateTransfer(transferNo, payerMerchantID, payeeMerchantID, amount)
	if err != nil {
		return TransferLocalResult{}, err
	}
	if s == nil {
		return TransferLocalResult{}, errno.ErrInternal
	}
	if ctx == nil {
		return TransferLocalResult{}, errno.ErrInternal
	}
	if err = ctx.Err(); err != nil {
		return TransferLocalResult{}, err
	}
	// 不选平台子账户。subCount 为 0 也不是失败。
	if s.ids == nil || s.pools == nil {
		return TransferLocalResult{}, errno.ErrInternal
	}
	db, err := s.pools.ShardDB(shardNo)
	if err != nil {
		return TransferLocalResult{}, errno.Wrap(errno.ErrInternal, err)
	}
	payer, payerFound, err := selectSettle(ctx, db, payerMerchantID)
	if err != nil {
		return TransferLocalResult{}, internalErr(err)
	}
	payee, payeeFound, err := selectSettle(ctx, db, payeeMerchantID)
	if err != nil {
		return TransferLocalResult{}, internalErr(err)
	}
	if err = classifySettle(payerFound, payeeFound, payer, payee); err != nil {
		return TransferLocalResult{}, err
	}
	req := localInsert{
		TransferNo: transferNo,
		Payer:      payerMerchantID,
		Payee:      payeeMerchantID,
		Amount:     amount,
	}
	return executeLocal(ctx, s.runLocal(db), sqlLocalReader{db: db}, req, payer.ID, payee.ID)
}

func validateTransfer(transferNo, payerMerchantID, payeeMerchantID string, amount money.Amount) (int, error) {
	if n := utf8.RuneCountInString(transferNo); n == 0 || n > maxTransferNoRunes {
		return 0, errno.ErrInvalidParam
	}
	if amount <= 0 {
		return 0, errno.ErrInvalidParam
	}
	payerShard, err := shard.Of(payerMerchantID)
	if err != nil {
		return 0, errno.ErrInvalidParam
	}
	payeeShard, err := shard.Of(payeeMerchantID)
	if err != nil {
		return 0, errno.ErrInvalidParam
	}
	if payerShard != payeeShard || payerMerchantID == payeeMerchantID {
		return 0, errno.ErrInvalidParam
	}
	return payerShard, nil
}

func classifySettle(payerFound, payeeFound bool, payer, payee settleAccount) error {
	if !payerFound || !payeeFound {
		return errno.ErrAccountNotFound
	}
	if payer.ID == 0 || payee.ID == 0 || payer.ID == payee.ID {
		return errno.ErrInternal
	}
	if payer.Status != statusActive || payee.Status != statusActive {
		return errno.ErrAccountInactive
	}
	return nil
}

func (s *Service) runLocal(db *sqlx.DB) localRun {
	return func(ctx context.Context, fn func(localSession) error) error {
		return dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
			return fn(sqlLocalSession{tx: tx, svc: s})
		})
	}
}

func executeLocal(ctx context.Context, run localRun, reader localReader, req localInsert, payerID, payeeID uint64) (TransferLocalResult, error) {
	var out TransferLocalResult
	err := run(ctx, func(sess localSession) error {
		var stepErr error
		out, stepErr = applyLocalSuccess(ctx, sess, req, payerID, payeeID)
		return stepErr
	})
	if errors.Is(err, errBranchDuplicate) {
		return readExistingLocal(ctx, reader, req)
	}
	if errors.Is(err, errno.ErrInsufficientBalance) {
		return writeInsufficientTombstone(ctx, run, reader, req)
	}
	if err != nil {
		return TransferLocalResult{}, internalErr(err)
	}
	return out, nil
}

func applyLocalSuccess(ctx context.Context, sess localSession, req localInsert, payerID, payeeID uint64) (TransferLocalResult, error) {
	err := sess.insertSuccess(ctx, req)
	if dbx.IsDuplicate(err) {
		// 1062 不中止 InnoDB 事务。返回错误让 WithTx 回滚，再在事务外读。
		return TransferLocalResult{}, errBranchDuplicate
	}
	if err != nil {
		return TransferLocalResult{}, err
	}
	// 公开 Post 的 FrozenAccountID 是 0，借方按可用余额（balance - frozen_amount）。
	// 不要在 Post 之前锁账户，锁顺序由 Post 按 id 升序处理。
	posted, err := sess.post(ctx, payerID, payeeID, req.Amount, req.TransferNo)
	if err != nil {
		return TransferLocalResult{}, err
	}
	if posted.TxnNo == "" {
		return TransferLocalResult{}, errno.ErrInternal
	}
	// Duplicated 为 true 时凭证已在，这次没有改余额。分支仍提交 SUCCESS 并回写已有 txn_no。
	// 这和确认扣款“凭证已存在则 10003”不同。
	ok, err := sess.writeTxnNo(ctx, req, posted.TxnNo)
	if err != nil {
		return TransferLocalResult{}, err
	}
	if !ok {
		// 影响 0 行就不能提交，否则会留下没有 txn_no 的 SUCCESS。
		return TransferLocalResult{}, errno.ErrInternal
	}
	return TransferLocalResult{Status: statusSuccess, TxnNo: posted.TxnNo}, nil
}

func writeInsufficientTombstone(ctx context.Context, run localRun, reader localReader, req localInsert) (TransferLocalResult, error) {
	err := run(ctx, func(sess localSession) error {
		return applyLocalTombstone(ctx, sess, req)
	})
	if errors.Is(err, errBranchDuplicate) {
		return readExistingLocal(ctx, reader, req)
	}
	if err != nil {
		return TransferLocalResult{}, internalErr(err)
	}
	return TransferLocalResult{Status: statusFailed, FailCode: failInsufficientBalance}, nil
}

func applyLocalTombstone(ctx context.Context, sess localSession, req localInsert) error {
	err := sess.insertFailed(ctx, req)
	if dbx.IsDuplicate(err) {
		return errBranchDuplicate
	}
	return err
}

func readExistingLocal(ctx context.Context, reader localReader, req localInsert) (TransferLocalResult, error) {
	row, found, err := reader.selectLocal(ctx, req.TransferNo)
	if err != nil {
		return TransferLocalResult{}, internalErr(err)
	}
	return decideExisting(row, found, req)
}

func decideExisting(row localBranch, found bool, req localInsert) (TransferLocalResult, error) {
	if !found {
		// 1062 之后读不到行。不空转重试。
		return TransferLocalResult{}, errno.ErrInternal
	}
	if row.Amount != req.Amount || row.Payer != req.Payer || row.Payee != req.Payee {
		return TransferLocalResult{}, errno.ErrIdempotentConflict
	}
	switch row.Status {
	case statusSuccess:
		if row.TxnNo == "" {
			return TransferLocalResult{}, errno.ErrInternal
		}
		return TransferLocalResult{Status: statusSuccess, TxnNo: row.TxnNo}, nil
	case statusFailed:
		if row.FailCode == "" {
			return TransferLocalResult{}, errno.ErrInternal
		}
		return TransferLocalResult{Status: statusFailed, FailCode: row.FailCode}, nil
	default:
		return TransferLocalResult{}, errno.ErrInternal
	}
}

const selectSettleSQL = `
SELECT id, status
FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND currency = ? AND sub_no = 0`

const insertLocalSuccessSQL = `
INSERT INTO transfer_branch (
  transfer_no, role, payer_merchant_id, payee_merchant_id, amount, status, fail_code, txn_no, version
) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, 0)`

const insertLocalFailedSQL = `
INSERT INTO transfer_branch (
  transfer_no, role, payer_merchant_id, payee_merchant_id, amount, status, fail_code, txn_no, version
) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 0)`

const writeLocalTxnSQL = `
UPDATE transfer_branch
SET txn_no = ?, version = version + 1
WHERE transfer_no = ? AND role = ? AND status = ? AND txn_no IS NULL
  AND amount = ? AND payer_merchant_id = ? AND payee_merchant_id = ?`

const selectLocalSQL = `
SELECT payer_merchant_id, payee_merchant_id, amount, status, fail_code, txn_no
FROM transfer_branch
WHERE transfer_no = ? AND role = ?`

func selectSettle(ctx context.Context, db *sqlx.DB, merchantID string) (settleAccount, bool, error) {
	if db == nil {
		return settleAccount{}, false, errNilTx
	}
	// 不加行锁，也不读余额。账户锁留给 Post，按 id 升序。
	row, err := scanSettle(db.QueryRowContext(ctx, selectSettleSQL, ownerMerchant, merchantID, typeMerchantSettle, currencyCNY))
	if errors.Is(err, sql.ErrNoRows) {
		return settleAccount{}, false, nil
	}
	if err != nil {
		return settleAccount{}, false, err
	}
	return row, true, nil
}

func scanSettle(sc rowScanner) (settleAccount, error) {
	var row settleAccount
	err := sc.Scan(&row.ID, &row.Status)
	if err != nil {
		return settleAccount{}, err
	}
	return row, nil
}

type sqlLocalSession struct {
	tx  *sqlx.Tx
	svc *Service
}

func (q sqlLocalSession) insertSuccess(ctx context.Context, row localInsert) error {
	if q.tx == nil {
		return errNilTx
	}
	_, err := q.tx.ExecContext(ctx, insertLocalSuccessSQL,
		row.TransferNo, roleLocal, row.Payer, row.Payee, int64(row.Amount), statusSuccess,
	)
	return err
}

func (q sqlLocalSession) insertFailed(ctx context.Context, row localInsert) error {
	if q.tx == nil {
		return errNilTx
	}
	_, err := q.tx.ExecContext(ctx, insertLocalFailedSQL,
		row.TransferNo, roleLocal, row.Payer, row.Payee, int64(row.Amount), statusFailed, failInsufficientBalance,
	)
	return err
}

func (q sqlLocalSession) writeTxnNo(ctx context.Context, row localInsert, txnNo string) (bool, error) {
	if q.tx == nil {
		return false, errNilTx
	}
	res, err := q.tx.ExecContext(ctx, writeLocalTxnSQL,
		txnNo, row.TransferNo, roleLocal, statusSuccess, int64(row.Amount), row.Payer, row.Payee,
	)
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
		return false, fmt.Errorf("biz: transfer branch update affected %d", n)
	}
	return true, nil
}

func (q sqlLocalSession) post(ctx context.Context, payerID, payeeID uint64, amount money.Amount, transferNo string) (PostResult, error) {
	if q.svc == nil {
		return PostResult{}, errNilTx
	}
	return q.svc.Post(ctx, q.tx, bizTransfer, transferNo, "", []Entry{
		{AccountID: payerID, Direction: directionDebit, Amount: amount},
		{AccountID: payeeID, Direction: directionCredit, Amount: amount},
	})
}

type sqlLocalReader struct {
	db *sqlx.DB
}

func (q sqlLocalReader) selectLocal(ctx context.Context, transferNo string) (localBranch, bool, error) {
	if q.db == nil {
		return localBranch{}, false, errNilTx
	}
	row, err := scanLocalBranch(q.db.QueryRowContext(ctx, selectLocalSQL, transferNo, roleLocal))
	if errors.Is(err, sql.ErrNoRows) {
		return localBranch{}, false, nil
	}
	if err != nil {
		return localBranch{}, false, err
	}
	return row, true, nil
}

func scanLocalBranch(sc rowScanner) (localBranch, error) {
	var (
		payer, payee, status, failCode, txnNo sql.NullString
		amount                                int64
	)
	err := sc.Scan(&payer, &payee, &amount, &status, &failCode, &txnNo)
	if err != nil {
		return localBranch{}, err
	}
	return localBranch{
		Payer:    payer.String,
		Payee:    payee.String,
		Amount:   money.Amount(amount),
		Status:   status.String,
		FailCode: failCode.String,
		TxnNo:    txnNo.String,
	}, nil
}
