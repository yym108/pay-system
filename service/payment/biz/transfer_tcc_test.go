package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

type scriptedTCC struct {
	mu       sync.Mutex
	seq      []string
	freeze   error
	tryPayee error
	confirmP error
	confirmE error
	cancelP  error
	cancelE  error
	tryWait  func(ctx context.Context)
	onPayer  func()
}

func (s *scriptedTCC) note(name string) {
	s.mu.Lock()
	s.seq = append(s.seq, name)
	s.mu.Unlock()
}

func (s *scriptedTCC) names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.seq))
	copy(out, s.seq)
	return out
}

func (s *scriptedTCC) Freeze(ctx context.Context, _, _ string, _ money.Amount) error {
	s.note("freeze")
	return s.freeze
}

func (s *scriptedTCC) TryPayee(ctx context.Context, _, _ string, _ money.Amount) error {
	s.note("try")
	if s.tryWait != nil {
		s.tryWait(ctx)
	}
	return s.tryPayee
}

func (s *scriptedTCC) ConfirmPayer(_ context.Context, _, _ string) error {
	s.note("confirm-payer")
	if s.onPayer != nil {
		s.onPayer()
	}
	return s.confirmP
}

func (s *scriptedTCC) ConfirmPayee(_ context.Context, _, _ string) error {
	s.note("confirm-payee")
	return s.confirmE
}

func (s *scriptedTCC) CancelPayer(_ context.Context, _, _ string, _ money.Amount) error {
	s.note("cancel-payer")
	return s.cancelP
}

func (s *scriptedTCC) CancelPayee(_ context.Context, _, _ string, _ money.Amount) error {
	s.note("cancel-payee")
	return s.cancelE
}

func newTCCTest(t *testing.T) transferTest {
	t.Helper()
	tt := newTransferTest(t)
	ids, err := idgen.New(4, tt.clk)
	if err != nil {
		t.Fatal(err)
	}
	tt.p.ids = ids
	return tt
}

func tccInput(out string, amount money.Amount) *CreateTransferInput {
	return transferInput(out, "M100002", amount)
}

func containsName(names []string, want string) bool {
	return countName(names, want) > 0
}

func countName(names []string, want string) int {
	n := 0
	for _, name := range names {
		if name == want {
			n++
		}
	}
	return n
}

// TRF-03：未装协调器时 TCC 仍返回刚插入的 TRYING。
func TestTCCNotInstalledStaysTrying(t *testing.T) {
	tt := newTCCTest(t)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-NONE", 500))
	if err != nil || got.Mode != transferModeTCC || got.Status != transferStatusTrying {
		t.Fatalf("got = %+v err=%v", got, err)
	}
}

// TRF-03 验收 1：两个 Try 都成功后转账单 SUCCESS，不写 txn_no。
func TestTCCSuccess(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{}
	tt.p.UseTCCTransfer(branch, true)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-OK", 500))
	if err != nil || got.Status != transferStatusSuccess || got.FailCode != "" || got.TxnNo != "" || got.Version != 2 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	names := branch.names()
	if !containsName(names, "freeze") || !containsName(names, "try") || !containsName(names, "confirm-payer") || !containsName(names, "confirm-payee") {
		t.Fatalf("calls = %v", names)
	}
	if containsName(names, "cancel-payer") {
		t.Fatalf("cancel on success: %v", names)
	}
}

// TRF-03 验收 2：付款方余额不足时取消两边，本次返回 40002。
func TestTCCInsufficientCancels(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{freeze: errno.ErrInsufficientBalance}
	tt.p.UseTCCTransfer(branch, false)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-POOR", 500))
	assertErrno(t, err, errno.ErrInsufficientBalance)
	if got.Status != transferStatusFailed || got.FailCode != transferFailInsufficient || got.TxnNo != "" || got.Version != 2 {
		t.Fatalf("got = %+v", got)
	}
	names := branch.names()
	if names[0] != "freeze" || names[1] != "try" || names[2] != "cancel-payer" || names[3] != "cancel-payee" {
		t.Fatalf("calls = %v", names)
	}
}

// TRF-03 验收 3：收款方 Try 超时，fail_code 为 TRY_TIMEOUT，接口 error 为 nil。
func TestTCCPayeeTimeoutCancels(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{tryPayee: errno.ErrUnavailable}
	tt.p.UseTCCTransfer(branch, true)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-TIME", 500))
	if err != nil || got.Status != transferStatusFailed || got.FailCode != transferFailTryTimeout {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	names := branch.names()
	if countName(names, "try") != 1 || !containsName(names, "cancel-payer") || !containsName(names, "cancel-payee") {
		t.Fatalf("calls = %v", names)
	}
}

// TRF-03：TryPayee 的 DeadlineExceeded 再调一次，第二次成功则 SUCCESS。
func TestTCCTryPayeeDeadlineRetriesOnce(t *testing.T) {
	tt := newTCCTest(t)
	inner := &scriptedTCC{}
	tt.p.UseTCCTransfer(&deadlineOncePayee{scriptedTCC: inner}, false)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-TRYDDL", 20))
	if err != nil || got.Status != transferStatusSuccess || got.TxnNo != "" || got.Version != 2 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if countName(inner.names(), "try") != 2 {
		t.Fatalf("calls = %v", inner.names())
	}
}

type deadlineOncePayee struct {
	*scriptedTCC
	calls int
}

func (d *deadlineOncePayee) TryPayee(context.Context, string, string, money.Amount) error {
	d.calls++
	d.note("try")
	if d.calls == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

// TRF-03：两个 Try 都失败时取付款方原因。
func TestTCCBothFailedUsesPayer(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{freeze: errno.ErrInsufficientBalance, tryPayee: errno.ErrUnavailable}
	tt.p.UseTCCTransfer(branch, true)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-BOTH", 500))
	assertErrno(t, err, errno.ErrInsufficientBalance)
	if got.FailCode != transferFailInsufficient {
		t.Fatalf("fail_code = %s", got.FailCode)
	}
}

// TRF-03：10001 和 10006 记成 TRY_TIMEOUT 并取消，不把转账单留在 TRYING。
func TestTCCUnexpectedTryCodesCancel(t *testing.T) {
	for _, branchErr := range []error{errno.ErrInvalidParam, errno.ErrIdempotentConflict} {
		tt := newTCCTest(t)
		branch := &scriptedTCC{tryPayee: branchErr}
		tt.p.UseTCCTransfer(branch, true)
		got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-BAD", 500))
		if err != nil || got.Status != transferStatusFailed || got.FailCode != transferFailTryTimeout {
			t.Fatalf("%v got = %+v err=%v", branchErr, got, err)
		}
	}
}

// TRF-03 验收 4：付款方 Confirm 失败一次，接口返回 CONFIRMING，不取消。
func TestTCCConfirmFailureStays(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{confirmP: errno.ErrInternal}
	tt.p.UseTCCTransfer(branch, true)
	before := transferInconsistent.Load()
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-CFM", 500))
	if err != nil || got.Status != transferStatusConfirming || got.FailCode != "" || got.Version != 1 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	names := branch.names()
	if countName(names, "confirm-payer") != 1 || countName(names, "confirm-payee") != 1 {
		t.Fatalf("calls = %v", names)
	}
	if containsName(names, "cancel-payer") {
		t.Fatal("confirm failure cancelled")
	}
	if transferInconsistent.Load() != before {
		t.Fatal("confirm failure counted as inconsistent")
	}
}

// TRF-03：Confirm 的 DeadlineExceeded 再调一次，第二次成功则 SUCCESS。
func TestTCCConfirmDeadlineRetriesOnce(t *testing.T) {
	tt := newTCCTest(t)
	inner := &scriptedTCC{}
	tt.p.UseTCCTransfer(&deadlineOncePayer{scriptedTCC: inner}, false)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-DDL", 20))
	if err != nil || got.Status != transferStatusSuccess || got.TxnNo != "" || got.Version != 2 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	names := inner.names()
	if len(names) != 6 || names[0] != "freeze" || names[1] != "try" ||
		names[2] != "confirm-payer" || names[3] != "confirm-payee" ||
		names[4] != "confirm-payer" || names[5] != "confirm-payee" {
		t.Fatalf("calls = %v", names)
	}
}

// TRF-03：Confirm 的 sql: Rows are closed 再调一次，第二次成功则 SUCCESS。
func TestTCCConfirmRowsClosedRetriesOnce(t *testing.T) {
	tt := newTCCTest(t)
	inner := &scriptedTCC{}
	tt.p.UseTCCTransfer(&rowsClosedOncePayer{scriptedTCC: inner}, false)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-ROWS", 20))
	if err != nil || got.Status != transferStatusSuccess || got.TxnNo != "" || got.Version != 2 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	names := inner.names()
	if len(names) != 6 || names[0] != "freeze" || names[1] != "try" ||
		names[2] != "confirm-payer" || names[3] != "confirm-payee" ||
		names[4] != "confirm-payer" || names[5] != "confirm-payee" {
		t.Fatalf("calls = %v", names)
	}
}

type rowsClosedOncePayer struct {
	*scriptedTCC
	calls int
}

func (d *rowsClosedOncePayer) ConfirmPayer(context.Context, string, string) error {
	d.calls++
	d.note("confirm-payer")
	if d.calls == 1 {
		return errno.Wrap(errno.ErrInternal, errors.New("sql: Rows are closed"))
	}
	return nil
}

type deadlineOncePayer struct {
	*scriptedTCC
	calls int
}

func (d *deadlineOncePayer) ConfirmPayer(context.Context, string, string) error {
	d.calls++
	d.note("confirm-payer")
	if d.calls == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

// TRF-03：Confirm 返回 40004 时计数加 1，停在 CONFIRMING。
func TestTCCConfirmConflict(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{confirmE: errno.ErrFreezeStatusConflict}
	tt.p.UseTCCTransfer(branch, false)
	before := transferInconsistent.Load()
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-C40004", 500))
	if err != nil || got.Status != transferStatusConfirming {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if transferInconsistent.Load() != before+1 {
		t.Fatalf("counter = %d before %d", transferInconsistent.Load(), before)
	}
	names := branch.names()
	if len(names) < 4 || names[2] != "confirm-payer" || names[3] != "confirm-payee" {
		t.Fatalf("calls = %v", names)
	}
}

// TRF-03：Cancel 返回 40004 时计数加 1，停在 CANCELLING。
func TestTCCCancelConflict(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{freeze: errno.ErrUnavailable, cancelP: errno.ErrFreezeStatusConflict}
	tt.p.UseTCCTransfer(branch, true)
	before := transferInconsistent.Load()
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-X40004", 500))
	if err != nil || got.Status != transferStatusCancelling || got.FailCode != transferFailTryTimeout || got.Version != 1 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if transferInconsistent.Load() != before+1 {
		t.Fatalf("counter = %d before %d", transferInconsistent.Load(), before)
	}
}

// TRF-03：行已插入、调用任何账户方法之前父 ctx 已取消，不调用分支，行保持 TRYING。
// 取消不能早于插入：TRF-01 在建单前发现 ctx 已结束时不写行，发号器也会直接返回 ctx 错误。
func TestTCCCanceledBeforeCall(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{}
	tt.p.UseTCCTransfer(branch, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tt.store.beforeInsert = cancel
	_, err := tt.p.CreateTransfer(ctx, tccInput("TRF03-CXL", 500))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if len(branch.names()) != 0 {
		t.Fatalf("calls = %v", branch.names())
	}
	if rows := tt.store.snapshot(); len(rows) != 1 || rows[0].Status != transferStatusTrying {
		t.Fatalf("stored = %+v", rows)
	}
}

// TRF-03：stub 发号器在调用账户之前返回 10003。
func TestTCCRejectsStubIDsBeforeAccount(t *testing.T) {
	tt := newTransferTest(t)
	branch := &scriptedTCC{}
	tt.p.UseTCCTransfer(branch, true)
	_, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-STUB", 500))
	assertErrno(t, err, errno.ErrInternal)
	if len(branch.names()) != 0 {
		t.Fatalf("calls = %v", branch.names())
	}
}

// TRF-03：决策 CAS 影响 0 行且已经 SUCCESS 时不再 Confirm。
func TestTCCDecideAlreadyTerminal(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{}
	tt.store.hijackDecide = func(row *TransferOrder) {
		row.Status = transferStatusSuccess
	}
	tt.p.UseTCCTransfer(branch, true)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-RACE", 500))
	if err != nil || got.Status != transferStatusSuccess {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if containsName(branch.names(), "confirm-payer") || containsName(branch.names(), "cancel-payer") {
		t.Fatalf("calls = %v", branch.names())
	}
}

// TRF-03 验收 6：并行与串行结果一致。串行顺序是 Freeze、TryPayee、ConfirmPayer、然后 after_payer_confirm、然后 ConfirmPayee。一个 Try 失败不取消另一个的 ctx。
func TestTCCSerialOrderAndParallelDoesNotCancel(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{confirmP: errno.ErrInternal}
	tt.p.UseTCCTransfer(branch, false)
	if _, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-SERIAL", 20)); err != nil {
		t.Fatal(err)
	}
	names := branch.names()
	if len(names) != 4 || names[0] != "freeze" || names[1] != "try" || names[2] != "confirm-payer" || names[3] != "confirm-payee" {
		t.Fatalf("serial = %v", names)
	}

	tt = newTCCTest(t)
	freezeDone := make(chan struct{})
	var once sync.Once
	var siblingErr error
	branch = &scriptedTCC{
		freeze: errno.ErrInsufficientBalance,
		tryWait: func(ctx context.Context) {
			<-freezeDone
			siblingErr = ctx.Err()
		},
	}
	// Freeze 返回错误后才放开 sibling Try。ctx 仍有效说明用的是 errgroup.Group，不是 WithContext。
	tt.p.UseTCCTransfer(&releaseAfterFreeze{scriptedTCC: branch, done: freezeDone, once: &once}, true)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-PAR", 20))
	assertErrno(t, err, errno.ErrInsufficientBalance)
	if got.Status != transferStatusFailed {
		t.Fatalf("got = %+v", got)
	}
	if siblingErr != nil {
		t.Fatalf("sibling ctx = %v", siblingErr)
	}
	if !containsName(branch.names(), "try") {
		t.Fatalf("calls = %v", branch.names())
	}
}

type releaseAfterFreeze struct {
	*scriptedTCC
	done chan struct{}
	once *sync.Once
}

func (r *releaseAfterFreeze) Freeze(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	err := r.scriptedTCC.Freeze(ctx, transferNo, payer, amount)
	r.once.Do(func() { close(r.done) })
	return err
}

// TRF-03：LOCAL 不走 TCC。注入的执行器优先于 TCC。
func TestTCCSkipsLocalAndExecutorWins(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{}
	local := &scriptedLocal{res: LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX-LOCAL"}}
	tt.p.UseLocalTransfer(local)
	tt.p.UseTCCTransfer(branch, true)
	got, err := tt.p.CreateTransfer(context.Background(), transferInput("TRF03-LOCAL", "M100003", 20))
	if err != nil || got.Mode != transferModeLocal || got.Status != transferStatusSuccess || local.callCount() != 1 {
		t.Fatalf("local got = %+v err=%v calls=%d", got, err, local.callCount())
	}
	if len(branch.names()) != 0 {
		t.Fatalf("tcc calls = %v", branch.names())
	}

	exec := &fakeExecutor{result: func(row TransferOrder) (TransferOrder, error) {
		row.Status = transferStatusTrying
		return row, nil
	}}
	tt.p.UseTransfer(10*time.Second, exec)
	got, err = tt.p.CreateTransfer(context.Background(), tccInput("TRF03-EXEC", 20))
	if err != nil || got.Status != transferStatusTrying || len(exec.calls()) != 1 || len(branch.names()) != 0 {
		t.Fatalf("exec got = %+v err=%v", got, err)
	}
}

// TRF-03：终态发号回拨时行停在 CONFIRMING，本次返回 10004。
func TestTCCClockRollback(t *testing.T) {
	tt := newTCCTest(t)
	branch := &scriptedTCC{onPayer: func() {
		tt.clk.Set(transferAt().Add(-10 * time.Millisecond))
	}}
	tt.p.UseTCCTransfer(branch, false)
	_, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF03-CLOCK", 500))
	assertErrno(t, err, errno.ErrUnavailable)
	rows := tt.store.snapshot()
	if len(rows) != 1 || rows[0].Status != transferStatusConfirming || rows[0].Version != 1 {
		t.Fatalf("stored = %+v", rows)
	}
}

func TestTCCErrFromRPC(t *testing.T) {
	ctx := context.Background()
	if err := tccErrFromRPC(ctx, nil); err != nil {
		t.Fatal(err)
	}
	assertErrno(t, tccErrFromRPC(ctx, kerrors.ErrRPCTimeout), errno.ErrUnavailable)
	assertErrno(t, tccErrFromRPC(ctx, errno.ErrInsufficientBalance), errno.ErrInsufficientBalance)
	if err := NewAccountTCCTransfer(nil).Freeze(ctx, "TF1", "M100001", 20); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil client = %v", err)
	}
}
