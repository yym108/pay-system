package biz

import (
	"context"
	"database/sql"
	"errors"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

const (
	statusTried     = "TRIED"
	statusCancelled = "CANCELLED"
	rolePayee       = "PAYEE"
)

// TransferInResult 是收款方转账分支的结果。没有进程内 duplicated 字段。
// error 为 nil 才表示业务成功。Try 和 Cancel 成功时 TxnNo 为空。
// Confirm 成功时 Status 为 CONFIRMED，TxnNo 非空。
// 返回错误时 Status 和 TxnNo 都是空字符串。
type TransferInResult struct {
	Status string
	TxnNo  string
}

type payeeInsert struct {
	TransferNo string
	Payee      string
	Amount     money.Amount
}

type payeeBranch struct {
	Payee  string
	Amount money.Amount
	Status string
	TxnNo  string
}

// payeeSession 是收款商户所在分片上的一段事务。
// 实现不得另开连接。假实现只给不连数据库的单元测试。
type payeeSession interface {
	lockPayee(ctx context.Context, transferNo string) (payeeBranch, bool, error)
	insertTried(ctx context.Context, req payeeInsert) error
	insertCancelled(ctx context.Context, req payeeInsert) error
	selectMerchant(ctx context.Context, merchantID string) (settleAccount, bool, error)
	selectTransit(ctx context.Context, subNo int) (uint64, bool, error)
	postIn(ctx context.Context, transitID, merchantID uint64, amount money.Amount, transferNo string) (PostResult, error)
	markConfirmed(ctx context.Context, transferNo, txnNo string, row payeeBranch) (bool, error)
	markCancelled(ctx context.Context, transferNo string, row payeeBranch) (bool, error)
}

// payeeReader 读取已提交的 PAYEE 分支。1062 回滚之后才用它。
type payeeReader interface {
	selectPayee(ctx context.Context, transferNo string) (payeeBranch, bool, error)
}

// payeeRun 提交或回滚一段事务。fn 返回错误时不得提交。
type payeeRun func(ctx context.Context, fn func(payeeSession) error) error

// TransferInTry 在收款商户分片插入 TRIED 分支。不改余额，不调用 Post。
// 事务由本方法用 dbx.WithTx 提交。参数不合法返回 10001，不写行。
// 已 CANCELLED 返回 40004。已 TRIED 或 CONFIRMED 且要素一致时返回库里的状态，TxnNo 仍为空。
func (s *Service) TransferInTry(ctx context.Context, transferNo, payeeMerchantID string, amount money.Amount) (TransferInResult, error) {
	shardNo, err := validatePayeeAmount(transferNo, payeeMerchantID, amount)
	if err != nil {
		return TransferInResult{}, err
	}
	db, err := payeeDB(ctx, s, shardNo)
	if err != nil {
		return TransferInResult{}, err
	}
	// 不加行锁，也不读余额。不存在含非 CNY 选不中，都是 40001。
	acct, found, err := selectSettle(ctx, db, payeeMerchantID)
	if err != nil {
		return TransferInResult{}, internalErr(err)
	}
	if !found {
		return TransferInResult{}, errno.ErrAccountNotFound
	}
	if acct.Status != statusActive {
		return TransferInResult{}, errno.ErrAccountInactive
	}
	req := payeeInsert{TransferNo: transferNo, Payee: payeeMerchantID, Amount: amount}
	return executeTry(ctx, s.runPayee(db), sqlPayeeReader{db: db}, req)
}

// TransferInConfirm 把 TRIED 分支确认成 CONFIRMED，并记一张 TRANSFER_IN 凭证。
// 借本分片在途子账户，贷收款商户结算账户。金额只用分支上的 amount。
// 事务由本方法用 dbx.WithTx 提交。Post 仍不提交。
func (s *Service) TransferInConfirm(ctx context.Context, transferNo, payeeMerchantID string) (TransferInResult, error) {
	shardNo, err := validatePayee(transferNo, payeeMerchantID)
	if err != nil {
		return TransferInResult{}, err
	}
	if s == nil || ctx == nil {
		return TransferInResult{}, errno.ErrInternal
	}
	if err = ctx.Err(); err != nil {
		return TransferInResult{}, err
	}
	// Post 要发 TX 号。子账户号用 shard.Sub，不开事务。
	if s.pools == nil || s.ids == nil || s.subCount < 1 {
		return TransferInResult{}, errno.ErrInternal
	}
	subNo, err := shard.Sub(transferNo, s.subCount)
	if err != nil {
		return TransferInResult{}, errno.Wrap(errno.ErrInternal, err)
	}
	db, err := s.pools.ShardDB(shardNo)
	if err != nil {
		return TransferInResult{}, errno.Wrap(errno.ErrInternal, err)
	}
	req := payeeInsert{TransferNo: transferNo, Payee: payeeMerchantID}
	return executeConfirm(ctx, s.runPayee(db), subNo, req)
}

// TransferInCancel 把 TRIED 分支取消，或在没有行时插入 CANCELLED 墓碑。
// 不查账户，不改余额，不调用 Post。事务由本方法用 dbx.WithTx 提交。
func (s *Service) TransferInCancel(ctx context.Context, transferNo, payeeMerchantID string, amount money.Amount) (TransferInResult, error) {
	shardNo, err := validatePayeeAmount(transferNo, payeeMerchantID, amount)
	if err != nil {
		return TransferInResult{}, err
	}
	db, err := payeeDB(ctx, s, shardNo)
	if err != nil {
		return TransferInResult{}, err
	}
	req := payeeInsert{TransferNo: transferNo, Payee: payeeMerchantID, Amount: amount}
	return executeCancel(ctx, s.runPayee(db), sqlPayeeReader{db: db}, req)
}

func validatePayee(transferNo, payeeMerchantID string) (int, error) {
	if n := utf8.RuneCountInString(transferNo); n == 0 || n > maxTransferNoRunes {
		return 0, errno.ErrInvalidParam
	}
	shardNo, err := shard.Of(payeeMerchantID)
	if err != nil {
		return 0, errno.ErrInvalidParam
	}
	return shardNo, nil
}

func validatePayeeAmount(transferNo, payeeMerchantID string, amount money.Amount) (int, error) {
	shardNo, err := validatePayee(transferNo, payeeMerchantID)
	if err != nil {
		return 0, err
	}
	if amount <= 0 {
		return 0, errno.ErrInvalidParam
	}
	return shardNo, nil
}

func payeeDB(ctx context.Context, s *Service, shardNo int) (*sqlx.DB, error) {
	if s == nil || ctx == nil {
		return nil, errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Try 和 Cancel 不选平台子账户。subCount 和发号器都不在这里检查。
	if s.pools == nil {
		return nil, errno.ErrInternal
	}
	db, err := s.pools.ShardDB(shardNo)
	if err != nil {
		return nil, errno.Wrap(errno.ErrInternal, err)
	}
	return db, nil
}

func (s *Service) runPayee(db *sqlx.DB) payeeRun {
	return func(ctx context.Context, fn func(payeeSession) error) error {
		return dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
			return fn(sqlPayeeSession{tx: tx, svc: s})
		})
	}
}

func executeTry(ctx context.Context, run payeeRun, reader payeeReader, req payeeInsert) (TransferInResult, error) {
	var out TransferInResult
	err := run(ctx, func(sess payeeSession) error {
		insErr := sess.insertTried(ctx, req)
		if dbx.IsDuplicate(insErr) {
			// 1062 不中止 InnoDB。返回错误让 WithTx 回滚，再在事务外读。
			return errBranchDuplicate
		}
		if insErr != nil {
			return insErr
		}
		out = TransferInResult{Status: statusTried}
		return nil
	})
	if errors.Is(err, errBranchDuplicate) {
		return readExistingPayee(ctx, reader, req)
	}
	if err != nil {
		return TransferInResult{}, internalErr(err)
	}
	return out, nil
}

func readExistingPayee(ctx context.Context, reader payeeReader, req payeeInsert) (TransferInResult, error) {
	row, found, err := reader.selectPayee(ctx, req.TransferNo)
	if err != nil {
		return TransferInResult{}, internalErr(err)
	}
	if !found {
		return TransferInResult{}, errno.ErrInternal
	}
	switch row.Status {
	case statusCancelled:
		// 空回滚已经占住单号。不再比较金额和收款商户，也不插入。
		return TransferInResult{}, errno.ErrFreezeStatusConflict
	case statusTried, statusConfirmed:
		if row.Amount != req.Amount || row.Payee != req.Payee {
			return TransferInResult{}, errno.ErrIdempotentConflict
		}
		// 已确认的 Try 只返回状态，不把 txn_no 交出去。
		return TransferInResult{Status: row.Status}, nil
	default:
		return TransferInResult{}, errno.ErrInternal
	}
}

func executeConfirm(ctx context.Context, run payeeRun, subNo int, req payeeInsert) (TransferInResult, error) {
	var out TransferInResult
	err := run(ctx, func(sess payeeSession) error {
		var stepErr error
		out, stepErr = applyConfirmIn(ctx, sess, subNo, req)
		return stepErr
	})
	if err != nil {
		return TransferInResult{}, internalErr(err)
	}
	return out, nil
}

func applyConfirmIn(ctx context.Context, sess payeeSession, subNo int, req payeeInsert) (TransferInResult, error) {
	// 先锁分支。账户锁交给 Post，按 id 升序。先锁账户会和普通记账死锁。
	row, found, err := sess.lockPayee(ctx, req.TransferNo)
	if err != nil {
		return TransferInResult{}, err
	}
	if !found || row.Status == statusCancelled {
		return TransferInResult{}, errno.ErrFreezeStatusConflict
	}
	switch row.Status {
	case statusConfirmed:
		if row.TxnNo == "" {
			return TransferInResult{}, errno.ErrInternal
		}
		// 已确认不再比较收款商户，也不再记账。
		return TransferInResult{Status: statusConfirmed, TxnNo: row.TxnNo}, nil
	case statusTried:
		if row.Payee != req.Payee {
			return TransferInResult{}, errno.ErrIdempotentConflict
		}
		return confirmTried(ctx, sess, subNo, req.TransferNo, row)
	default:
		return TransferInResult{}, errno.ErrInternal
	}
}

func confirmTried(ctx context.Context, sess payeeSession, subNo int, transferNo string, row payeeBranch) (TransferInResult, error) {
	merchant, found, err := sess.selectMerchant(ctx, row.Payee)
	if err != nil {
		return TransferInResult{}, err
	}
	if !found {
		// 商户行此时没了。不调用 Post，事务回滚后分支仍是 TRIED。
		return TransferInResult{}, errno.ErrAccountNotFound
	}
	transitID, found, err := sess.selectTransit(ctx, subNo)
	if err != nil {
		return TransferInResult{}, err
	}
	if !found {
		return TransferInResult{}, errno.ErrAccountNotFound
	}
	// id 为 0 或两个 id 相同是数据损坏，不能交给 Post。
	if merchant.ID == 0 || transitID == 0 || merchant.ID == transitID {
		return TransferInResult{}, errno.ErrInternal
	}
	posted, err := sess.postIn(ctx, transitID, merchant.ID, row.Amount, transferNo)
	if err != nil {
		// 40002、40005 原样返回。调用方回滚，分支保持 TRIED，不写墓碑。
		return TransferInResult{}, err
	}
	if posted.TxnNo == "" {
		return TransferInResult{}, errno.ErrInternal
	}
	// Duplicated 为 true 时凭证已在，这次没有改余额。分支仍提交为 CONFIRMED。
	// 这和确认扣款“凭证已存在则 10003”不同。
	ok, err := sess.markConfirmed(ctx, transferNo, posted.TxnNo, row)
	if err != nil {
		return TransferInResult{}, err
	}
	if !ok {
		// 行上有 FOR UPDATE。影响 0 行不是并发幂等。
		return TransferInResult{}, errno.ErrInternal
	}
	return TransferInResult{Status: statusConfirmed, TxnNo: posted.TxnNo}, nil
}

func executeCancel(ctx context.Context, run payeeRun, reader payeeReader, req payeeInsert) (TransferInResult, error) {
	var out TransferInResult
	err := run(ctx, func(sess payeeSession) error {
		var stepErr error
		out, stepErr = applyCancel(ctx, sess, req)
		return stepErr
	})
	if errors.Is(err, errBranchDuplicate) {
		return cancelAfterDuplicate(ctx, run, reader, req)
	}
	if err != nil {
		return TransferInResult{}, internalErr(err)
	}
	return out, nil
}

func applyCancel(ctx context.Context, sess payeeSession, req payeeInsert) (TransferInResult, error) {
	row, found, err := sess.lockPayee(ctx, req.TransferNo)
	if err != nil {
		return TransferInResult{}, err
	}
	if !found {
		// 空回滚不查账户。账户不存在也写 CANCELLED 墓碑。
		insErr := sess.insertCancelled(ctx, req)
		if dbx.IsDuplicate(insErr) {
			return TransferInResult{}, errBranchDuplicate
		}
		if insErr != nil {
			return TransferInResult{}, insErr
		}
		return TransferInResult{Status: statusCancelled}, nil
	}
	return cancelLocked(ctx, sess, req, row)
}

func cancelLocked(ctx context.Context, sess payeeSession, req payeeInsert, row payeeBranch) (TransferInResult, error) {
	switch row.Status {
	case statusCancelled:
		return TransferInResult{Status: statusCancelled}, nil
	case statusConfirmed:
		return TransferInResult{}, errno.ErrFreezeStatusConflict
	case statusTried:
		if row.Amount != req.Amount || row.Payee != req.Payee {
			return TransferInResult{}, errno.ErrIdempotentConflict
		}
		ok, err := sess.markCancelled(ctx, req.TransferNo, row)
		if err != nil {
			return TransferInResult{}, err
		}
		if !ok {
			// 持有 FOR UPDATE 时影响 0 行不是幂等成功。
			return TransferInResult{}, errno.ErrInternal
		}
		return TransferInResult{Status: statusCancelled}, nil
	default:
		return TransferInResult{}, errno.ErrInternal
	}
}

func cancelAfterDuplicate(ctx context.Context, run payeeRun, reader payeeReader, req payeeInsert) (TransferInResult, error) {
	row, found, err := reader.selectPayee(ctx, req.TransferNo)
	if err != nil {
		return TransferInResult{}, internalErr(err)
	}
	if !found {
		return TransferInResult{}, errno.ErrInternal
	}
	switch row.Status {
	case statusCancelled:
		return TransferInResult{Status: statusCancelled}, nil
	case statusConfirmed:
		return TransferInResult{}, errno.ErrFreezeStatusConflict
	case statusTried:
		if row.Amount != req.Amount || row.Payee != req.Payee {
			return TransferInResult{}, errno.ErrIdempotentConflict
		}
		return cancelTriedAgain(ctx, run, req)
	default:
		return TransferInResult{}, errno.ErrInternal
	}
}

func cancelTriedAgain(ctx context.Context, run payeeRun, req payeeInsert) (TransferInResult, error) {
	var out TransferInResult
	err := run(ctx, func(sess payeeSession) error {
		row, found, lockErr := sess.lockPayee(ctx, req.TransferNo)
		if lockErr != nil {
			return lockErr
		}
		if !found {
			insErr := sess.insertCancelled(ctx, req)
			if dbx.IsDuplicate(insErr) {
				return errBranchDuplicate
			}
			if insErr != nil {
				return insErr
			}
			out = TransferInResult{Status: statusCancelled}
			return nil
		}
		var stepErr error
		out, stepErr = cancelLocked(ctx, sess, req, row)
		return stepErr
	})
	if errors.Is(err, errBranchDuplicate) {
		// 第二次仍冲突就不再读。避免空转。
		return TransferInResult{}, errno.ErrInternal
	}
	if err != nil {
		return TransferInResult{}, internalErr(err)
	}
	return out, nil
}

const insertPayeeSQL = `
INSERT INTO transfer_branch (
  transfer_no, role, payer_merchant_id, payee_merchant_id, amount, status, fail_code, txn_no, version
) VALUES (?, ?, NULL, ?, ?, ?, NULL, NULL, 0)`

const lockPayeeSQL = `
SELECT payee_merchant_id, amount, status, txn_no
FROM transfer_branch
WHERE transfer_no = ? AND role = ?
FOR UPDATE`

const selectPayeeSQL = `
SELECT payee_merchant_id, amount, status, txn_no
FROM transfer_branch
WHERE transfer_no = ? AND role = ?`

const markPayeeConfirmedSQL = `
UPDATE transfer_branch
SET status = 'CONFIRMED', txn_no = ?, version = version + 1
WHERE transfer_no = ? AND role = ? AND status = 'TRIED' AND txn_no IS NULL
  AND amount = ? AND payee_merchant_id = ?`

const markPayeeCancelledSQL = `
UPDATE transfer_branch
SET status = 'CANCELLED', version = version + 1
WHERE transfer_no = ? AND role = ? AND status = 'TRIED'
  AND amount = ? AND payee_merchant_id = ?`

type sqlPayeeSession struct {
	tx  *sqlx.Tx
	svc *Service
}

func (q sqlPayeeSession) insertTried(ctx context.Context, req payeeInsert) error {
	return q.insertPayee(ctx, req, statusTried)
}

func (q sqlPayeeSession) insertCancelled(ctx context.Context, req payeeInsert) error {
	return q.insertPayee(ctx, req, statusCancelled)
}

func (q sqlPayeeSession) insertPayee(ctx context.Context, req payeeInsert, status string) error {
	if q.tx == nil {
		return errNilTx
	}
	_, err := q.tx.ExecContext(ctx, insertPayeeSQL,
		req.TransferNo, rolePayee, req.Payee, int64(req.Amount), status,
	)
	return err
}

func (q sqlPayeeSession) lockPayee(ctx context.Context, transferNo string) (payeeBranch, bool, error) {
	if q.tx == nil {
		return payeeBranch{}, false, errNilTx
	}
	return queryPayee(q.tx.QueryRowContext(ctx, lockPayeeSQL, transferNo, rolePayee))
}

func (q sqlPayeeSession) selectMerchant(ctx context.Context, merchantID string) (settleAccount, bool, error) {
	if q.tx == nil {
		return settleAccount{}, false, errNilTx
	}
	// 不加行锁。账户锁留给 Post，按 id 升序。
	row, err := scanSettle(q.tx.QueryRowContext(ctx, selectSettleSQL, ownerMerchant, merchantID, typeMerchantSettle, currencyCNY))
	if errors.Is(err, sql.ErrNoRows) {
		return settleAccount{}, false, nil
	}
	if err != nil {
		return settleAccount{}, false, err
	}
	return row, true, nil
}

func (q sqlPayeeSession) selectTransit(ctx context.Context, subNo int) (uint64, bool, error) {
	if q.tx == nil {
		return 0, false, errNilTx
	}
	var id uint64
	err := q.tx.QueryRowContext(ctx, selectOwnerAccountSQL,
		ownerPlatform, platformOwnerID, typeTransit, currencyCNY, subNo,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (q sqlPayeeSession) postIn(ctx context.Context, transitID, merchantID uint64, amount money.Amount, transferNo string) (PostResult, error) {
	if q.tx == nil || q.svc == nil {
		return PostResult{}, errNilTx
	}
	return q.svc.Post(ctx, q.tx, bizTransferIn, transferNo, "", []Entry{
		{AccountID: transitID, Direction: directionDebit, Amount: amount},
		{AccountID: merchantID, Direction: directionCredit, Amount: amount},
	})
}

func (q sqlPayeeSession) markConfirmed(ctx context.Context, transferNo, txnNo string, row payeeBranch) (bool, error) {
	if q.tx == nil {
		return false, errNilTx
	}
	res, err := q.tx.ExecContext(ctx, markPayeeConfirmedSQL,
		txnNo, transferNo, rolePayee, int64(row.Amount), row.Payee,
	)
	if err != nil {
		return false, err
	}
	return affectedOne(res)
}

func (q sqlPayeeSession) markCancelled(ctx context.Context, transferNo string, row payeeBranch) (bool, error) {
	if q.tx == nil {
		return false, errNilTx
	}
	res, err := q.tx.ExecContext(ctx, markPayeeCancelledSQL,
		transferNo, rolePayee, int64(row.Amount), row.Payee,
	)
	if err != nil {
		return false, err
	}
	return affectedOne(res)
}

type sqlPayeeReader struct {
	db *sqlx.DB
}

func (q sqlPayeeReader) selectPayee(ctx context.Context, transferNo string) (payeeBranch, bool, error) {
	if q.db == nil {
		return payeeBranch{}, false, errNilTx
	}
	return queryPayee(q.db.QueryRowContext(ctx, selectPayeeSQL, transferNo, rolePayee))
}

func queryPayee(sc rowScanner) (payeeBranch, bool, error) {
	row, err := scanPayee(sc)
	if errors.Is(err, sql.ErrNoRows) {
		return payeeBranch{}, false, nil
	}
	if err != nil {
		return payeeBranch{}, false, err
	}
	return row, true, nil
}

func scanPayee(sc rowScanner) (payeeBranch, error) {
	var (
		payee, status, txn sql.NullString
		amount             int64
	)
	err := sc.Scan(&payee, &amount, &status, &txn)
	if err != nil {
		return payeeBranch{}, err
	}
	row := payeeBranch{Amount: money.Amount(amount)}
	if payee.Valid {
		row.Payee = payee.String
	}
	if status.Valid {
		row.Status = status.String
	}
	if txn.Valid {
		row.TxnNo = txn.String
	}
	return row, nil
}
