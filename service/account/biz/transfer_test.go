package biz

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/dal"
)

// ACC-07：空单号、33 个码点、金额 0、负数、非法商户号都是 10001。service 为 nil 时仍是 10001。
func TestTransferLocalRejectsParams(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	ctx := context.Background()
	const payer = "M100001"
	const payee = "M100003"
	cases := []struct {
		name   string
		no     string
		payer  string
		payee  string
		amount money.Amount
	}{
		{name: "empty no", payer: payer, payee: payee, amount: 1},
		{name: "33 runes", no: strings.Repeat("单", 33), payer: payer, payee: payee, amount: 1},
		{name: "amount zero", no: "T1", payer: payer, payee: payee, amount: 0},
		{name: "amount negative", no: "T1", payer: payer, payee: payee, amount: -1},
		{name: "bad payer", no: "T1", payer: "M10001", payee: payee, amount: 1},
		{name: "bad payee", no: "T1", payer: payer, payee: "NOPE", amount: 1},
		{name: "cross shard", no: "T1", payer: payer, payee: "M100002", amount: 1},
		{name: "self", no: "T1", payer: payer, payee: payer, amount: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.TransferLocal(ctx, tc.no, tc.payer, tc.payee, tc.amount)
			assertErrno(t, err, errno.ErrInvalidParam)
			_, err = (*Service)(nil).TransferLocal(ctx, tc.no, tc.payer, tc.payee, tc.amount)
			assertErrno(t, err, errno.ErrInvalidParam)
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.TransferLocal(canceled, "", payer, payee, 1)
	assertErrno(t, err, errno.ErrInvalidParam)
	_, err = (*Service)(nil).TransferLocal(canceled, "", payer, payee, 1)
	assertErrno(t, err, errno.ErrInvalidParam)
	if clk.i != 0 {
		t.Fatalf("invalid params issued an id, calls = %d", clk.i)
	}

	// 32 个码点只过校验。nil db 说明已经过了校验，仍然不发号。
	_, err = svc.TransferLocal(ctx, strings.Repeat("单", 32), payer, payee, 1)
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil db issued an id")
	}
}

// ACC-07：合法参数下 nil service、nil ctx 为 10003。已取消或已过截止时间的 ctx 原样返回，不调用发号器。
func TestTransferLocalNilAndCanceled(t *testing.T) {
	ctx := context.Background()
	_, err := (*Service)(nil).TransferLocal(ctx, "T1", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrInternal)

	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	// SA1012 不允许把 nil context 当参数。这里正是要证明 nil ctx 返回 10003。
	_, err = svc.TransferLocal(nil, "T1", "M100001", "M100003", 1) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)

	_, err = New(nil, newIDs(t, clk), testSubCount).TransferLocal(ctx, "T1", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrInternal)
	_, err = New(dal.New(nil, nil), nil, testSubCount).TransferLocal(ctx, "T1", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrInternal)
	// 不选平台子账户。subCount 为 0 或负数不是 10001，nil db 仍是 10003。
	_, err = New(dal.New(nil, nil), newIDs(t, clk), 0).TransferLocal(ctx, "T1", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrInternal)
	if errors.Is(err, errno.ErrInvalidParam) {
		t.Fatal("subCount 0 became 10001")
	}
	_, err = New(dal.New(nil, nil), newIDs(t, clk), -1).TransferLocal(ctx, "T1", "M100001", "M100003", 1)
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil dependency issued an id")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.TransferLocal(canceled, "T1", "M100001", "M100003", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
	if errors.Is(err, errno.ErrInternal) {
		t.Fatal("canceled context became 10003")
	}
	deadline, stop := context.WithDeadline(context.Background(), openTestTime)
	defer stop()
	_, err = svc.TransferLocal(deadline, "T1", "M100001", "M100003", 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline", err)
	}
	if clk.i != 0 {
		t.Fatal("canceled request issued an id")
	}
}

func TestClassifySettle(t *testing.T) {
	active := settleAccount{ID: 2, Status: statusActive}
	other := settleAccount{ID: 9, Status: statusActive}
	inactive := settleAccount{ID: 4, Status: "FROZEN"}
	if err := classifySettle(false, true, settleAccount{}, inactive); !errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatalf("missing payer = %v", err)
	}
	if err := classifySettle(true, false, inactive, settleAccount{}); !errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatalf("missing payee = %v", err)
	}
	if err := classifySettle(true, true, active, inactive); !errors.Is(err, errno.ErrAccountInactive) {
		t.Fatalf("inactive = %v", err)
	}
	if err := classifySettle(true, true, inactive, other); !errors.Is(err, errno.ErrAccountInactive) {
		t.Fatalf("payer inactive = %v", err)
	}
	if err := classifySettle(true, true, active, active); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("same id = %v", err)
	}
	if err := classifySettle(true, true, settleAccount{ID: 0, Status: statusActive}, other); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("zero id = %v", err)
	}
	if err := classifySettle(true, true, active, other); err != nil {
		t.Fatal(err)
	}
}

// ACC-07：事务 1 的 1062 回滚后，按已有 SUCCESS 或 FAILED 返回，不再记账。
func TestExecuteLocalDuplicateBranch(t *testing.T) {
	success := &localFake{committed: &localBranch{
		Payer: "M100001", Payee: "M100003", Amount: 1, Status: statusSuccess, TxnNo: "TX0000000000000000009",
	}}
	got, err := executeLocal(context.Background(), success.run, success, sampleLocal("T1"), 9, 2)
	if err != nil || got.Status != statusSuccess || got.TxnNo != "TX0000000000000000009" || got.FailCode != "" {
		t.Fatalf("success replay = %+v err=%v", got, err)
	}
	if success.posts != 0 || success.tombstones != 0 || success.commits != 0 || success.rollbacks != 1 {
		t.Fatalf("success replay wrote again: %+v", success)
	}

	failed := &localFake{committed: &localBranch{
		Payer: "M100001", Payee: "M100003", Amount: 1, Status: statusFailed, FailCode: failInsufficientBalance,
	}}
	got, err = executeLocal(context.Background(), failed.run, failed, sampleLocal("T1"), 9, 2)
	if err != nil || got.Status != statusFailed || got.FailCode != failInsufficientBalance || got.TxnNo != "" {
		t.Fatalf("failed replay = %+v err=%v", got, err)
	}
	if failed.posts != 0 || failed.tombstones != 0 || failed.commits != 0 {
		t.Fatalf("failed replay wrote again: %+v", failed)
	}

	mismatch := &localFake{committed: &localBranch{
		Payer: "M100005", Payee: "M100003", Amount: 1, Status: statusSuccess, TxnNo: "TX0000000000000000008",
	}}
	_, err = executeLocal(context.Background(), mismatch.run, mismatch, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if mismatch.posts != 0 || mismatch.committed.Payer != "M100005" || mismatch.commits != 0 {
		t.Fatalf("mismatch wrote: %+v row=%+v", mismatch, mismatch.committed)
	}
}

// ACC-07：回写 txn_no 影响 0 行得到 10003，事务 1 不提交，不写墓碑。
func TestExecuteLocalWriteTxnMiss(t *testing.T) {
	fake := &localFake{writeZero: true}
	_, err := executeLocal(context.Background(), fake.run, fake, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrInternal)
	if fake.commits != 0 || fake.committed != nil || fake.tombstones != 0 || fake.posts != 1 || fake.rollbacks != 1 {
		t.Fatalf("zero writeback committed: %+v row=%+v", fake, fake.committed)
	}
}

// ACC-07：已有 SUCCESS 但 txn_no 为空得到 10003，不写。
func TestExecuteLocalSuccessWithoutTxnNo(t *testing.T) {
	fake := &localFake{committed: &localBranch{
		Payer: "M100001", Payee: "M100003", Amount: 1, Status: statusSuccess,
	}}
	_, err := executeLocal(context.Background(), fake.run, fake, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrInternal)
	if fake.posts != 0 || fake.tombstones != 0 || fake.commits != 0 || fake.committed.TxnNo != "" {
		t.Fatalf("empty txn_no wrote: %+v", fake)
	}

	unknown := &localFake{committed: &localBranch{
		Payer: "M100001", Payee: "M100003", Amount: 1, Status: "TRIED", TxnNo: "TX1",
	}}
	_, err = executeLocal(context.Background(), unknown.run, unknown, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrInternal)

	noFail := &localFake{committed: &localBranch{
		Payer: "M100001", Payee: "M100003", Amount: 1, Status: statusFailed,
	}}
	_, err = executeLocal(context.Background(), noFail.run, noFail, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrInternal)

	missing := &localFake{forceDup: true}
	_, err = executeLocal(context.Background(), missing.run, missing, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrInternal)
	if missing.posts != 0 || missing.tombstones != 0 || missing.committed != nil {
		t.Fatalf("missing row wrote: %+v", missing)
	}
}

// ACC-07：Post 返回 40005 或 40003 时只回滚事务 1，不写失败墓碑。
func TestExecuteLocalPostErrorNoTombstone(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want *errno.Error
	}{
		{name: "inactive", err: errno.ErrAccountInactive, want: errno.ErrAccountInactive},
		{name: "unbalanced", err: errno.ErrUnbalancedEntries, want: errno.ErrUnbalancedEntries},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &localFake{postErr: tc.err}
			_, err := executeLocal(context.Background(), fake.run, fake, sampleLocal("T1"), 9, 2)
			assertErrno(t, err, tc.want)
			if fake.tombstones != 0 || fake.commits != 0 || fake.committed != nil || fake.posts != 1 {
				t.Fatalf("tombstone written: %+v", fake)
			}
		})
	}
	fake := &localFake{postErr: context.Canceled}
	_, err := executeLocal(context.Background(), fake.run, fake, sampleLocal("T1"), 9, 2)
	if !errors.Is(err, context.Canceled) || errors.Is(err, errno.ErrInternal) {
		t.Fatalf("error = %v", err)
	}
	if fake.tombstones != 0 || fake.committed != nil {
		t.Fatal("canceled post wrote a tombstone")
	}
}

// ACC-07：Post 返回 40002 时回滚事务 1，事务 2 写入 INSUFFICIENT_BALANCE 墓碑。
func TestExecuteLocalInsufficientWritesTombstone(t *testing.T) {
	fake := &localFake{postErr: errno.ErrInsufficientBalance}
	got, err := executeLocal(context.Background(), fake.run, fake, sampleLocal("T1"), 9, 2)
	if err != nil || got.Status != statusFailed || got.FailCode != failInsufficientBalance || got.TxnNo != "" {
		t.Fatalf("result = %+v err=%v", got, err)
	}
	if fake.posts != 1 || fake.tombstones != 1 || fake.commits != 1 || fake.rollbacks != 1 {
		t.Fatalf("counts = %+v", fake)
	}
	if fake.committed == nil || fake.committed.Status != statusFailed || fake.committed.FailCode != failInsufficientBalance || fake.committed.TxnNo != "" {
		t.Fatalf("tombstone = %+v", fake.committed)
	}
	if fake.lastPayer != 9 || fake.lastPayee != 2 || fake.lastAmount != 1 || fake.lastNo != "T1" {
		t.Fatalf("post args payer=%d payee=%d amount=%d no=%s", fake.lastPayer, fake.lastPayee, fake.lastAmount, fake.lastNo)
	}
}

// ACC-07：余额不足写墓碑时遇到 1062，回滚后返回已有 SUCCESS。
func TestExecuteLocalTombstoneDuplicateReturnsSuccess(t *testing.T) {
	seed := &localBranch{
		Payer: "M100001", Payee: "M100003", Amount: 1, Status: statusSuccess, TxnNo: "TX0000000000000000007",
	}
	fake := &localFake{postErr: errno.ErrInsufficientBalance, seedAfterInsufficient: seed}
	got, err := executeLocal(context.Background(), fake.run, fake, sampleLocal("T1"), 9, 2)
	if err != nil || got.Status != statusSuccess || got.TxnNo != seed.TxnNo || got.FailCode != "" {
		t.Fatalf("result = %+v err=%v", got, err)
	}
	if fake.tombstones != 1 || fake.commits != 0 || fake.committed.Status != statusSuccess || fake.committed.TxnNo != seed.TxnNo {
		t.Fatalf("stored = %+v counts=%+v", fake.committed, fake)
	}
}

func TestExecuteLocalCommitsPostedTxn(t *testing.T) {
	fake := &localFake{postTxn: "TX0000000000000000004", postDup: true}
	got, err := executeLocal(context.Background(), fake.run, fake, sampleLocal("T1"), 9, 2)
	if err != nil || got.Status != statusSuccess || got.TxnNo != "TX0000000000000000004" || got.FailCode != "" {
		t.Fatalf("duplicated post = %+v err=%v", got, err)
	}
	if fake.commits != 1 || fake.tombstones != 0 || fake.committed.TxnNo != got.TxnNo || fake.committed.Status != statusSuccess {
		t.Fatalf("commit = %+v", fake.committed)
	}

	empty := &localFake{emptyTxn: true}
	_, err = executeLocal(context.Background(), empty.run, empty, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrInternal)
	if empty.commits != 0 || empty.committed != nil || empty.tombstones != 0 {
		t.Fatal("empty post txn committed")
	}
}

func TestExecuteLocalInsertErrors(t *testing.T) {
	text := &localFake{
		insertErr: errors.New("Error 1062: Duplicate entry"),
		committed: &localBranch{Payer: "M100001", Payee: "M100003", Amount: 1, Status: statusSuccess, TxnNo: "TX1"},
	}
	_, err := executeLocal(context.Background(), text.run, text, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrInternal)
	if text.posts != 0 || text.committed.TxnNo != "TX1" {
		t.Fatal("text 1062 took the duplicate path")
	}

	check := &localFake{insertErr: &mysql.MySQLError{Number: 3819, Message: "Check constraint 'ck_branch_amount' is violated"}}
	_, err = executeLocal(context.Background(), check.run, check, sampleLocal("T1"), 9, 2)
	assertErrno(t, err, errno.ErrInternal)
	if check.tombstones != 0 || check.posts != 0 || check.committed != nil {
		t.Fatal("check violation wrote a tombstone")
	}
	if strings.Contains(err.Error(), "ck_branch") || strings.Contains(err.Error(), "SELECT") {
		t.Fatalf("Error() leaked: %s", err.Error())
	}
}

func TestTransferSQLHelpers(t *testing.T) {
	ctx := context.Background()
	var sess sqlLocalSession
	if err := sess.insertSuccess(ctx, sampleLocal("T1")); !errors.Is(err, errNilTx) {
		t.Fatalf("insertSuccess = %v", err)
	}
	if err := sess.insertFailed(ctx, sampleLocal("T1")); !errors.Is(err, errNilTx) {
		t.Fatalf("insertFailed = %v", err)
	}
	if _, err := sess.writeTxnNo(ctx, sampleLocal("T1"), "TX1"); !errors.Is(err, errNilTx) {
		t.Fatalf("writeTxnNo = %v", err)
	}
	var reader sqlLocalReader
	if _, _, err := reader.selectLocal(ctx, "T1"); !errors.Is(err, errNilTx) {
		t.Fatalf("selectLocal = %v", err)
	}
	if _, _, err := selectSettle(ctx, nil, "M100001"); !errors.Is(err, errNilTx) {
		t.Fatalf("selectSettle = %v", err)
	}

	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), 0)
	_, err := (sqlLocalSession{svc: svc}).post(ctx, 9, 2, 1, "T1")
	assertErrno(t, err, errno.ErrInternal)
	if clk.i != 0 {
		t.Fatal("nil tx issued an id")
	}

	got, err := scanSettle(seqScan{vals: []any{uint64(7), statusActive}})
	if err != nil || got.ID != 7 || got.Status != statusActive {
		t.Fatalf("settle = %+v err=%v", got, err)
	}
	row, err := scanLocalBranch(seqScan{vals: []any{
		sql.NullString{String: "M100001", Valid: true},
		sql.NullString{String: "M100003", Valid: true},
		int64(4),
		sql.NullString{String: statusFailed, Valid: true},
		sql.NullString{String: failInsufficientBalance, Valid: true},
		sql.NullString{},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if row.Payer != "M100001" || row.Payee != "M100003" || row.Amount != 4 || row.Status != statusFailed || row.FailCode != failInsufficientBalance || row.TxnNo != "" {
		t.Fatalf("branch = %+v", row)
	}
	if _, err := scanLocalBranch(seqScan{err: errors.New("SELECT boom")}); err == nil {
		t.Fatal("scan error lost")
	}
}

func sampleLocal(no string) localInsert {
	return localInsert{TransferNo: no, Payer: "M100001", Payee: "M100003", Amount: 1}
}

type localFake struct {
	committed             *localBranch
	insertErr             error
	postErr               error
	postTxn               string
	postDup               bool
	emptyTxn              bool
	writeZero             bool
	forceDup              bool
	seedAfterInsufficient *localBranch
	posts                 int
	tombstones            int
	commits               int
	rollbacks             int
	lastPayer             uint64
	lastPayee             uint64
	lastAmount            money.Amount
	lastNo                string
}

func (f *localFake) run(_ context.Context, fn func(localSession) error) error {
	sess := &localFakeSession{db: f}
	err := fn(sess)
	if err != nil {
		f.rollbacks++
		if errors.Is(err, errno.ErrInsufficientBalance) && f.seedAfterInsufficient != nil {
			cp := *f.seedAfterInsufficient
			f.committed = &cp
		}
		return err
	}
	f.commits++
	if sess.staged != nil {
		cp := *sess.staged
		f.committed = &cp
	}
	return nil
}

func (f *localFake) selectLocal(context.Context, string) (localBranch, bool, error) {
	if f.committed == nil {
		return localBranch{}, false, nil
	}
	return *f.committed, true, nil
}

type localFakeSession struct {
	db     *localFake
	staged *localBranch
}

func (s *localFakeSession) insertSuccess(_ context.Context, req localInsert) error {
	if s.db.insertErr != nil {
		return s.db.insertErr
	}
	if s.db.forceDup || s.db.committed != nil {
		return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'T1' for key 'uk_transfer_role'"}
	}
	s.staged = &localBranch{Payer: req.Payer, Payee: req.Payee, Amount: req.Amount, Status: statusSuccess}
	return nil
}

func (s *localFakeSession) insertFailed(_ context.Context, req localInsert) error {
	s.db.tombstones++
	if s.db.forceDup || s.db.committed != nil {
		return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'T1' for key 'uk_transfer_role'"}
	}
	s.staged = &localBranch{
		Payer: req.Payer, Payee: req.Payee, Amount: req.Amount,
		Status: statusFailed, FailCode: failInsufficientBalance,
	}
	return nil
}

func (s *localFakeSession) writeTxnNo(_ context.Context, req localInsert, txnNo string) (bool, error) {
	if s.db.writeZero || s.staged == nil || s.staged.Status != statusSuccess || s.staged.TxnNo != "" {
		return false, nil
	}
	if s.staged.Amount != req.Amount || s.staged.Payer != req.Payer || s.staged.Payee != req.Payee {
		return false, nil
	}
	s.staged.TxnNo = txnNo
	return true, nil
}

func (s *localFakeSession) post(_ context.Context, payerID, payeeID uint64, amount money.Amount, transferNo string) (PostResult, error) {
	s.db.posts++
	s.db.lastPayer = payerID
	s.db.lastPayee = payeeID
	s.db.lastAmount = amount
	s.db.lastNo = transferNo
	if s.db.postErr != nil {
		return PostResult{}, s.db.postErr
	}
	if s.db.emptyTxn {
		return PostResult{Duplicated: s.db.postDup}, nil
	}
	txn := s.db.postTxn
	if txn == "" {
		txn = "TX0000000000000000001"
	}
	return PostResult{TxnNo: txn, Duplicated: s.db.postDup}, nil
}

type seqScan struct {
	vals []any
	err  error
}

func (s seqScan) Scan(dest ...any) error {
	if s.err != nil {
		return s.err
	}
	if len(dest) != len(s.vals) {
		return errors.New("scan argc")
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *sql.NullString:
			v, ok := s.vals[i].(sql.NullString)
			if !ok {
				return errors.New("want null string")
			}
			*p = v
		case *int64:
			v, ok := s.vals[i].(int64)
			if !ok {
				return errors.New("want int64")
			}
			*p = v
		case *uint64:
			v, ok := s.vals[i].(uint64)
			if !ok {
				return errors.New("want uint64")
			}
			*p = v
		case *string:
			v, ok := s.vals[i].(string)
			if !ok {
				return errors.New("want string")
			}
			*p = v
		default:
			return errors.New("dest type")
		}
	}
	return nil
}
