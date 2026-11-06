package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/pkg/accountclient"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

type scriptedLocal struct {
	mu    sync.Mutex
	calls int
	res   LocalTransferResult
	err   error
	fn    func(transferNo string) (LocalTransferResult, error)
}

func (s *scriptedLocal) TransferLocal(_ context.Context, transferNo, _, _ string, _ money.Amount) (LocalTransferResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.fn != nil {
		return s.fn(transferNo)
	}
	return s.res, s.err
}

func (s *scriptedLocal) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newLocalTransferTest(t *testing.T) transferTest {
	t.Helper()
	tt := newTransferTest(t)
	ids, err := idgen.New(3, tt.clk)
	if err != nil {
		t.Fatal(err)
	}
	tt.p.ids = ids
	return tt
}

func localInput() *CreateTransferInput {
	return transferInput("TRF02-1", "M100003", 20)
}

// TRF-02：未装 LocalTransferrer 时 LOCAL 仍返回刚插入的 TRYING。
func TestLocalTransferNotInstalledStaysTrying(t *testing.T) {
	tt := newTransferTest(t)
	got, err := tt.p.CreateTransfer(context.Background(), localInput())
	if err != nil || got.Status != transferStatusTrying || got.Mode != transferModeLocal {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	if tt.store.finishCount() != 0 {
		t.Fatalf("finishes = %d", tt.store.finishCount())
	}
}

// TRF-02：发号器不是 *idgen.Generator 时不调用账户，行保持 TRYING，返回 10003。
func TestLocalTransferRejectsStubIDsBeforeAccount(t *testing.T) {
	tt := newTransferTest(t)
	local := &scriptedLocal{res: LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX1"}}
	tt.p.UseLocalTransfer(local)
	_, err := tt.p.CreateTransfer(context.Background(), localInput())
	assertErrno(t, err, errno.ErrInternal)
	if local.callCount() != 0 || tt.store.finishCount() != 0 {
		t.Fatalf("calls=%d finishes=%d", local.callCount(), tt.store.finishCount())
	}
	if rows := tt.store.snapshot(); len(rows) != 1 || rows[0].Status != transferStatusTrying {
		t.Fatalf("rows = %+v", rows)
	}
}

// TRF-02：SUCCESS 写入 txn_no、finished_at，version 从 0 变为 1。
func TestLocalTransferSuccess(t *testing.T) {
	tt := newLocalTransferTest(t)
	local := &scriptedLocal{res: LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX100"}}
	tt.p.UseLocalTransfer(local)
	got, err := tt.p.CreateTransfer(context.Background(), localInput())
	if err != nil {
		t.Fatal(err)
	}
	wantAt := transferAt().Truncate(time.Millisecond)
	if got.Status != transferStatusSuccess || got.TxnNo != "TX100" || got.FailCode != "" ||
		!got.FinishedAt.Equal(wantAt) || got.Version != 1 || local.callCount() != 1 {
		t.Fatalf("got = %+v calls=%d", got, local.callCount())
	}
}

// TRF-02：本次执行到余额不足返回 40002；相同参数再请求不再执行，error 为 nil。
func TestLocalTransferInsufficientThenReplay(t *testing.T) {
	tt := newLocalTransferTest(t)
	local := &scriptedLocal{res: LocalTransferResult{Status: transferStatusFailed, FailCode: transferFailInsufficient}}
	tt.p.UseLocalTransfer(local)
	got, err := tt.p.CreateTransfer(context.Background(), localInput())
	assertErrno(t, err, errno.ErrInsufficientBalance)
	if got.Status != transferStatusFailed || got.FailCode != transferFailInsufficient || got.TxnNo != "" {
		t.Fatalf("got = %+v", got)
	}
	again, err := tt.p.CreateTransfer(context.Background(), localInput())
	if err != nil || again.TransferNo != got.TransferNo || again.Status != transferStatusFailed || local.callCount() != 1 {
		t.Fatalf("again = %+v err=%v calls=%d", again, err, local.callCount())
	}
}

// TRF-02：超时、10003、10004 保持 TRYING，不 CAS。
func TestLocalTransferUnknownStaysTrying(t *testing.T) {
	for _, err := range []error{errno.ErrUnavailable, errno.ErrInternal, kerrors.ErrRPCTimeout} {
		tt := newLocalTransferTest(t)
		local := &scriptedLocal{err: err}
		tt.p.UseLocalTransfer(local)
		got, callErr := tt.p.CreateTransfer(context.Background(), localInput())
		if callErr != nil || got.Status != transferStatusTrying || tt.store.finishCount() != 0 {
			t.Fatalf("err=%v got=%+v finishes=%d", callErr, got, tt.store.finishCount())
		}
	}
}

// TRF-02：10001、10006 保持 TRYING，不置 FAILED。
func TestLocalTransferInconsistentStaysTrying(t *testing.T) {
	for _, err := range []error{errno.ErrInvalidParam, errno.ErrIdempotentConflict} {
		tt := newLocalTransferTest(t)
		local := &scriptedLocal{err: err}
		tt.p.UseLocalTransfer(local)
		got, callErr := tt.p.CreateTransfer(context.Background(), localInput())
		if callErr != nil || got.Status != transferStatusTrying || got.FailCode != "" || tt.store.finishCount() != 0 {
			t.Fatalf("err=%v got=%+v finishes=%d", callErr, got, tt.store.finishCount())
		}
	}
}

// TRF-02：40001、40005 写成 FAILED / ACCOUNT_INVALID，接口 error 为 nil。
func TestLocalTransferAccountInvalid(t *testing.T) {
	for _, err := range []error{errno.ErrAccountNotFound, errno.ErrAccountInactive} {
		tt := newLocalTransferTest(t)
		local := &scriptedLocal{err: err}
		tt.p.UseLocalTransfer(local)
		got, callErr := tt.p.CreateTransfer(context.Background(), localInput())
		if callErr != nil || got.Status != transferStatusFailed || got.FailCode != transferFailAccountInvalid || got.TxnNo != "" {
			t.Fatalf("err=%v got=%+v", callErr, got)
		}
	}
}

// TRF-02：SUCCESS 缺少 txn_no，或 FAILED 缺少 fail_code，保持 TRYING。
func TestLocalTransferRejectsEmptyTerminalFields(t *testing.T) {
	cases := []LocalTransferResult{
		{Status: transferStatusSuccess},
		{Status: transferStatusFailed},
		{Status: "CONFIRMING", TxnNo: "TX1"},
	}
	for _, res := range cases {
		tt := newLocalTransferTest(t)
		local := &scriptedLocal{res: res}
		tt.p.UseLocalTransfer(local)
		got, err := tt.p.CreateTransfer(context.Background(), localInput())
		if err != nil || got.Status != transferStatusTrying || tt.store.finishCount() != 0 {
			t.Fatalf("res=%+v err=%v got=%+v finishes=%d", res, err, got, tt.store.finishCount())
		}
	}
}

// TRF-02：TCC 不调用 LocalTransferrer。
func TestLocalTransferSkipsTCC(t *testing.T) {
	tt := newLocalTransferTest(t)
	local := &scriptedLocal{fn: func(string) (LocalTransferResult, error) {
		t.Fatal("TCC called TransferLocal")
		return LocalTransferResult{}, nil
	}}
	tt.p.UseLocalTransfer(local)
	got, err := tt.p.CreateTransfer(context.Background(), transferInput("TRF02-TCC", "M100002", 20))
	if err != nil || got.Mode != transferModeTCC || got.Status != transferStatusTrying || local.callCount() != 0 {
		t.Fatalf("got = %+v err=%v calls=%d", got, err, local.callCount())
	}
}

// TRF-02：已注入的执行器优先，不调用 local。
func TestLocalTransferExecutorWins(t *testing.T) {
	tt := newLocalTransferTest(t)
	local := &scriptedLocal{res: LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX1"}}
	exec := &fakeExecutor{result: func(row TransferOrder) (TransferOrder, error) {
		row.Status = transferStatusTrying
		return row, nil
	}}
	tt.p.UseLocalTransfer(local)
	tt.p.UseTransfer(10*time.Second, exec)
	got, err := tt.p.CreateTransfer(context.Background(), localInput())
	if err != nil || got.Status != transferStatusTrying || local.callCount() != 0 || len(exec.calls()) != 1 {
		t.Fatalf("got = %+v err=%v local=%d exec=%d", got, err, local.callCount(), len(exec.calls()))
	}
}

// TRF-02：CAS 影响 0 行时返回再读到的行，不改 version。
func TestLocalTransferCASMissRereads(t *testing.T) {
	tt := newLocalTransferTest(t)
	local := &scriptedLocal{fn: func(string) (LocalTransferResult, error) {
		tt.store.mu.Lock()
		tt.store.rows[0].Status = transferStatusSuccess
		tt.store.rows[0].TxnNo = "TX_OTHER"
		tt.store.rows[0].Version = 3
		tt.store.mu.Unlock()
		return LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX_NEW"}, nil
	}}
	tt.p.UseLocalTransfer(local)
	got, err := tt.p.CreateTransfer(context.Background(), localInput())
	if err != nil || got.Status != transferStatusSuccess || got.TxnNo != "TX_OTHER" || got.Version != 3 {
		t.Fatalf("got = %+v err=%v", got, err)
	}
}

// TRF-02：本库写 outbox 时时钟回拨返回 10004，行保持 TRYING。
func TestLocalTransferClockRollback(t *testing.T) {
	tt := newLocalTransferTest(t)
	tt.store.finishErr = idgen.ErrClockRollback
	local := &scriptedLocal{res: LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX1"}}
	tt.p.UseLocalTransfer(local)
	_, err := tt.p.CreateTransfer(context.Background(), localInput())
	assertErrno(t, err, errno.ErrUnavailable)
	rows := tt.store.snapshot()
	if len(rows) != 1 || rows[0].Status != transferStatusTrying || rows[0].TxnNo != "" {
		t.Fatalf("rows = %+v", rows)
	}
}

// TRF-02：调用前 ctx 已取消则不调用账户。
func TestLocalTransferCanceledBeforeCall(t *testing.T) {
	tt := newLocalTransferTest(t)
	local := &scriptedLocal{res: LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX1"}}
	tt.p.UseLocalTransfer(local)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := tt.p.CreateTransfer(ctx, localInput())
	if !errors.Is(err, context.Canceled) || local.callCount() != 0 {
		t.Fatalf("err=%v calls=%d", err, local.callCount())
	}
}

func TestLocalResultFromRPC(t *testing.T) {
	txn := "TX9"
	fail := transferFailInsufficient
	ctx := context.Background()
	got, err := localResultFromRPC(ctx, &account.TransferLocalResponse{Status: transferStatusSuccess, TxnNo: &txn}, nil)
	if err != nil || got.TxnNo != txn || got.Status != transferStatusSuccess {
		t.Fatalf("success = %+v err=%v", got, err)
	}
	got, err = localResultFromRPC(ctx, &account.TransferLocalResponse{Status: transferStatusFailed, FailCode: &fail}, nil)
	if err != nil || got.FailCode != fail {
		t.Fatalf("failed = %+v err=%v", got, err)
	}
	_, err = localResultFromRPC(ctx, nil, nil)
	assertErrno(t, err, errno.ErrInternal)
	_, err = localResultFromRPC(ctx, nil, kerrors.ErrRPCTimeout)
	assertErrno(t, err, errno.ErrUnavailable)
	_, err = localResultFromRPC(ctx, nil, errno.ErrAccountNotFound)
	assertErrno(t, err, errno.ErrAccountNotFound)
	_, err = localResultFromRPC(ctx, nil, errno.ErrAccountInactive)
	assertErrno(t, err, errno.ErrAccountInactive)
	if _, err = NewAccountLocalTransfer(nil).TransferLocal(ctx, "TF1", "M100001", "M100003", 20); err == nil {
		t.Fatal("nil client")
	}
	assertErrno(t, err, errno.ErrInternal)
}

func TestAccountLocalTransferConnectionRefused(t *testing.T) {
	cli, err := accountclient.Dial("127.0.0.1:1", accountclient.WithTimeouts(50*time.Millisecond, 50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = NewAccountLocalTransfer(cli).TransferLocal(ctx, "TF1", "M100001", "M100003", 20)
	assertErrno(t, err, errno.ErrInternal)
}
