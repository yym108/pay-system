package biz

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/dal"
)

// ACC-14：空单号、33 个码点、金额 0、负数、非法商户号都是 10001。service 为 nil 时仍是 10001。
func TestTransferInRejectsParams(t *testing.T) {
	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	ctx := context.Background()
	const payee = "M100002"
	tryCases := []struct {
		name   string
		no     string
		payee  string
		amount money.Amount
	}{
		{name: "empty no", payee: payee, amount: 1},
		{name: "33 runes", no: strings.Repeat("单", 33), payee: payee, amount: 1},
		{name: "amount zero", no: "T1", payee: payee, amount: 0},
		{name: "amount negative", no: "T1", payee: payee, amount: -1},
		{name: "bad payee", no: "T1", payee: "NOPE", amount: 1},
	}
	for _, tc := range tryCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := svc.TransferInTry(ctx, tc.no, tc.payee, tc.amount)
			assertErrno(t, err, errno.ErrInvalidParam)
			assertEmptyIn(t, got)
			got, err = (*Service)(nil).TransferInTry(ctx, tc.no, tc.payee, tc.amount)
			assertErrno(t, err, errno.ErrInvalidParam)
			assertEmptyIn(t, got)
			got, err = svc.TransferInCancel(ctx, tc.no, tc.payee, tc.amount)
			assertErrno(t, err, errno.ErrInvalidParam)
			assertEmptyIn(t, got)
			got, err = (*Service)(nil).TransferInCancel(ctx, tc.no, tc.payee, tc.amount)
			assertErrno(t, err, errno.ErrInvalidParam)
			assertEmptyIn(t, got)
		})
	}
	confirmCases := []struct {
		name  string
		no    string
		payee string
	}{
		{name: "confirm empty", payee: payee},
		{name: "confirm 33", no: strings.Repeat("单", 33), payee: payee},
		{name: "confirm bad payee", no: "T1", payee: "M1"},
	}
	for _, tc := range confirmCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := svc.TransferInConfirm(ctx, tc.no, tc.payee)
			assertErrno(t, err, errno.ErrInvalidParam)
			assertEmptyIn(t, got)
			got, err = (*Service)(nil).TransferInConfirm(ctx, tc.no, tc.payee)
			assertErrno(t, err, errno.ErrInvalidParam)
			assertEmptyIn(t, got)
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := svc.TransferInTry(canceled, "", payee, 1)
	assertErrno(t, err, errno.ErrInvalidParam)
	assertEmptyIn(t, got)
	got, err = (*Service)(nil).TransferInConfirm(canceled, "", payee)
	assertErrno(t, err, errno.ErrInvalidParam)
	assertEmptyIn(t, got)
	if clk.i != 0 {
		t.Fatalf("invalid params issued an id, calls = %d", clk.i)
	}

	// 32 个码点只过校验。nil db 说明已经过了校验，Confirm 也不发号。
	got, err = svc.TransferInTry(ctx, strings.Repeat("单", 32), payee, 1)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = svc.TransferInConfirm(ctx, strings.Repeat("单", 32), payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if clk.i != 0 {
		t.Fatal("nil db issued an id")
	}
}

// ACC-14：合法参数下 nil service 为 10003。已取消的 ctx 原样返回，Confirm 不调用发号器。
func TestTransferInNilAndCanceled(t *testing.T) {
	ctx := context.Background()
	const payee = "M100002"
	got, err := (*Service)(nil).TransferInTry(ctx, "T1", payee, 1)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = (*Service)(nil).TransferInConfirm(ctx, "T1", payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = (*Service)(nil).TransferInCancel(ctx, "T1", payee, 1)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)

	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	// SA1012 不允许把 nil context 当参数。这里正是要证明 nil ctx 返回 10003。
	got, err = svc.TransferInTry(nil, "T1", payee, 1) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = svc.TransferInConfirm(nil, "T1", payee) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = svc.TransferInCancel(nil, "T1", payee, 1) //nolint:staticcheck
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)

	got, err = New(nil, newIDs(t, clk), testSubCount).TransferInTry(ctx, "T1", payee, 1)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = New(nil, newIDs(t, clk), testSubCount).TransferInCancel(ctx, "T1", payee, 1)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = New(nil, newIDs(t, clk), testSubCount).TransferInConfirm(ctx, "T1", payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)

	// Try 和 Cancel 不选平台子账户。subCount 为 0、ids 为空仍不是 10001。
	got, err = New(dal.New(nil, nil), nil, 0).TransferInTry(ctx, "T1", payee, 1)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if errors.Is(err, errno.ErrInvalidParam) {
		t.Fatal("try subCount 0 became 10001")
	}
	got, err = New(dal.New(nil, nil), nil, -1).TransferInCancel(ctx, "T1", payee, 1)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)

	// Confirm 在开事务前要求 ids 非空且 subCount >= 1。shard.Sub 失败也是 10003，不发号。
	got, err = New(dal.New(nil, nil), nil, testSubCount).TransferInConfirm(ctx, "T1", payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = New(dal.New(nil, nil), newIDs(t, clk), 0).TransferInConfirm(ctx, "T1", payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	got, err = New(dal.New(nil, nil), newIDs(t, clk), int(math.MaxUint32)+1).TransferInConfirm(ctx, "T1", payee)
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if clk.i != 0 {
		t.Fatal("confirm dependency failure issued an id")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	got, err = svc.TransferInTry(canceled, "T1", payee, 1)
	if !errors.Is(err, context.Canceled) || errors.Is(err, errno.ErrInternal) {
		t.Fatalf("try error = %v, want canceled", err)
	}
	assertEmptyIn(t, got)
	got, err = svc.TransferInConfirm(canceled, "T1", payee)
	if !errors.Is(err, context.Canceled) || errors.Is(err, errno.ErrInternal) {
		t.Fatalf("confirm error = %v, want canceled", err)
	}
	assertEmptyIn(t, got)
	got, err = svc.TransferInCancel(canceled, "T1", payee, 1)
	if !errors.Is(err, context.Canceled) || errors.Is(err, errno.ErrInternal) {
		t.Fatalf("cancel error = %v, want canceled", err)
	}
	assertEmptyIn(t, got)
	deadline, stop := context.WithDeadline(context.Background(), openTestTime)
	defer stop()
	_, err = svc.TransferInConfirm(deadline, "T1", payee)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline", err)
	}
	if clk.i != 0 {
		t.Fatal("canceled confirm issued an id")
	}
}

// ACC-14：Try 的 1062 回滚后，按已有 TRIED、CONFIRMED、CANCELLED 返回，不再记账。
func TestExecuteTryDuplicateBranch(t *testing.T) {
	tried := &payeeFake{committed: &payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried}}
	got, err := executeTry(context.Background(), tried.run, tried, samplePayee("T1"))
	if err != nil || got.Status != statusTried || got.TxnNo != "" {
		t.Fatalf("tried replay = %+v err=%v", got, err)
	}
	if tried.posts != 0 || tried.commits != 0 || tried.rollbacks != 1 || tried.settleLookups != 0 {
		t.Fatalf("tried replay wrote: %+v", tried)
	}

	confirmed := &payeeFake{committed: &payeeBranch{
		Payee: "M100002", Amount: 40, Status: statusConfirmed, TxnNo: "TX0000000000000000009",
	}}
	got, err = executeTry(context.Background(), confirmed.run, confirmed, samplePayee("T1"))
	if err != nil || got.Status != statusConfirmed || got.TxnNo != "" {
		t.Fatalf("confirmed try = %+v err=%v", got, err)
	}
	if confirmed.posts != 0 || confirmed.commits != 0 || confirmed.committed.TxnNo != "TX0000000000000000009" {
		t.Fatalf("confirmed try returned txn or wrote: %+v", confirmed)
	}

	cancelled := &payeeFake{committed: &payeeBranch{Payee: "M100004", Amount: 1, Status: statusCancelled}}
	got, err = executeTry(context.Background(), cancelled.run, cancelled, samplePayee("T1"))
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	assertEmptyIn(t, got)
	if cancelled.posts != 0 || cancelled.commits != 0 || cancelled.committed.Amount != 1 || cancelled.committed.Payee != "M100004" {
		t.Fatalf("cancelled try wrote: %+v", cancelled.committed)
	}

	mismatch := &payeeFake{committed: &payeeBranch{Payee: "M100004", Amount: 40, Status: statusTried}}
	got, err = executeTry(context.Background(), mismatch.run, mismatch, samplePayee("T1"))
	assertErrno(t, err, errno.ErrIdempotentConflict)
	assertEmptyIn(t, got)
	if mismatch.committed.Payee != "M100004" || mismatch.posts != 0 || mismatch.commits != 0 {
		t.Fatal("mismatch wrote")
	}

	amountMismatch := &payeeFake{committed: &payeeBranch{Payee: "M100002", Amount: 7, Status: statusConfirmed, TxnNo: "TX1"}}
	got, err = executeTry(context.Background(), amountMismatch.run, amountMismatch, samplePayee("T1"))
	assertErrno(t, err, errno.ErrIdempotentConflict)
	assertEmptyIn(t, got)

	unknown := &payeeFake{committed: &payeeBranch{Payee: "M100002", Amount: 40, Status: statusSuccess, TxnNo: "TX1"}}
	got, err = executeTry(context.Background(), unknown.run, unknown, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)

	missing := &payeeFake{forceDup: true}
	got, err = executeTry(context.Background(), missing.run, missing, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if missing.committed != nil || missing.posts != 0 {
		t.Fatal("missing row wrote")
	}
}

// ACC-14：Confirm 回写 0 行得到 10003，不提交。CONFIRMED 且 txn_no 为空也是 10003。
func TestExecuteConfirmWriteMissAndEmptyTxn(t *testing.T) {
	zero := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried})
	zero.writeZero = true
	got, err := executeConfirm(context.Background(), zero.run, 3, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if zero.commits != 0 || zero.posts != 1 || zero.committed.Status != statusTried || zero.committed.TxnNo != "" {
		t.Fatalf("zero writeback committed: %+v row=%+v", zero, zero.committed)
	}
	if zero.lastSub != 3 || zero.lastAmount != 40 || zero.lastTransit != 9 || zero.lastMerchant != 4 {
		t.Fatalf("post args sub=%d amount=%d transit=%d merchant=%d", zero.lastSub, zero.lastAmount, zero.lastTransit, zero.lastMerchant)
	}

	emptyTxn := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried})
	emptyTxn.emptyTxn = true
	got, err = executeConfirm(context.Background(), emptyTxn.run, 3, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if emptyTxn.commits != 0 || emptyTxn.committed.Status != statusTried {
		t.Fatal("empty txn committed")
	}

	emptyConfirmed := &payeeFake{committed: &payeeBranch{Payee: "M100004", Amount: 40, Status: statusConfirmed}}
	got, err = executeConfirm(context.Background(), emptyConfirmed.run, 3, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if emptyConfirmed.posts != 0 || emptyConfirmed.commits != 0 || emptyConfirmed.settleLookups != 0 || emptyConfirmed.committed.TxnNo != "" {
		t.Fatalf("empty confirmed wrote: %+v", emptyConfirmed)
	}
}

// ACC-14：Post Duplicated=true 仍提交 CONFIRMED，并回写已有 txn_no。
func TestExecuteConfirmDuplicatedPostStillCommits(t *testing.T) {
	fake := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried})
	fake.postDup = true
	fake.postTxn = "TX0000000000000000099"
	got, err := executeConfirm(context.Background(), fake.run, 2, samplePayee("T1"))
	if err != nil || got.Status != statusConfirmed || got.TxnNo != fake.postTxn {
		t.Fatalf("duplicated = %+v err=%v", got, err)
	}
	if fake.commits != 1 || fake.posts != 1 || !fake.sawDup || fake.committed.Status != statusConfirmed || fake.committed.TxnNo != fake.postTxn {
		t.Fatalf("commit = %+v dup=%v", fake.committed, fake.sawDup)
	}
	want := []string{"lock", "merchant", "transit", "post", "mark"}
	if strings.Join(fake.events, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v", fake.events)
	}

	replay := &payeeFake{committed: &payeeBranch{
		Payee: "M100002", Amount: 9, Status: statusConfirmed, TxnNo: "TX0000000000000000008",
	}}
	got, err = executeConfirm(context.Background(), replay.run, 2, samplePayee("OTHER"))
	if err != nil || got.Status != statusConfirmed || got.TxnNo != "TX0000000000000000008" {
		t.Fatalf("replay = %+v err=%v", got, err)
	}
	if replay.posts != 0 || replay.settleLookups != 0 || replay.committed.Payee != "M100002" || replay.committed.Amount != 9 {
		t.Fatal("confirmed replay posted or compared payee")
	}
}

// ACC-14：Post 40005 不改状态。40002 原样返回，也不改状态。
func TestExecuteConfirmPostErrorKeepsTried(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want *errno.Error
	}{
		{name: "inactive", err: errno.ErrAccountInactive, want: errno.ErrAccountInactive},
		{name: "insufficient", err: errno.ErrInsufficientBalance, want: errno.ErrInsufficientBalance},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried})
			fake.postErr = tc.err
			got, err := executeConfirm(context.Background(), fake.run, 1, samplePayee("T1"))
			assertErrno(t, err, tc.want)
			assertEmptyIn(t, got)
			if fake.commits != 0 || fake.posts != 1 || fake.committed.Status != statusTried || fake.committed.TxnNo != "" {
				t.Fatalf("status changed: %+v row=%+v", fake, fake.committed)
			}
			if strings.Contains(strings.Join(fake.events, ","), "mark") {
				t.Fatal("post error still marked confirmed")
			}
		})
	}

	missingMerchant := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried})
	missingMerchant.settleFound = false
	got, err := executeConfirm(context.Background(), missingMerchant.run, 1, samplePayee("T1"))
	assertErrno(t, err, errno.ErrAccountNotFound)
	assertEmptyIn(t, got)
	if missingMerchant.posts != 0 || missingMerchant.transitLookups != 0 || missingMerchant.commits != 0 || missingMerchant.committed.Status != statusTried {
		t.Fatal("missing merchant posted")
	}

	missingTransit := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried})
	missingTransit.transitFound = false
	got, err = executeConfirm(context.Background(), missingTransit.run, 1, samplePayee("T1"))
	assertErrno(t, err, errno.ErrAccountNotFound)
	assertEmptyIn(t, got)
	if missingTransit.posts != 0 || missingTransit.transitLookups != 1 || missingTransit.commits != 0 {
		t.Fatal("missing transit posted")
	}

	same := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried})
	same.settle.ID = 7
	same.transitID = 7
	got, err = executeConfirm(context.Background(), same.run, 1, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if same.posts != 0 || same.commits != 0 {
		t.Fatal("same id posted")
	}

	mismatch := readyConfirmFake(&payeeBranch{Payee: "M100004", Amount: 40, Status: statusTried})
	got, err = executeConfirm(context.Background(), mismatch.run, 1, samplePayee("T1"))
	assertErrno(t, err, errno.ErrIdempotentConflict)
	assertEmptyIn(t, got)
	if mismatch.posts != 0 || mismatch.settleLookups != 0 || mismatch.committed.Payee != "M100004" || mismatch.commits != 0 {
		t.Fatal("payee mismatch posted")
	}

	for _, status := range []string{statusCancelled, ""} {
		fake := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: status})
		if status == "" {
			fake.committed = nil
		}
		got, err = executeConfirm(context.Background(), fake.run, 1, samplePayee("T1"))
		assertErrno(t, err, errno.ErrFreezeStatusConflict)
		assertEmptyIn(t, got)
		if fake.posts != 0 || fake.commits != 0 {
			t.Fatalf("status %q posted", status)
		}
	}

	unknown := readyConfirmFake(&payeeBranch{Payee: "M100002", Amount: 40, Status: "SUCCESS"})
	got, err = executeConfirm(context.Background(), unknown.run, 1, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if unknown.posts != 0 {
		t.Fatal("unknown status posted")
	}
}

// ACC-14：Cancel 没有行时插入墓碑，不查账户。
func TestExecuteCancelInsertsTombstoneWithoutAccount(t *testing.T) {
	fake := &payeeFake{}
	got, err := executeCancel(context.Background(), fake.run, fake, samplePayee("T1"))
	if err != nil || got.Status != statusCancelled || got.TxnNo != "" {
		t.Fatalf("tombstone = %+v err=%v", got, err)
	}
	if fake.settleLookups != 0 || fake.transitLookups != 0 || fake.posts != 0 || fake.commits != 1 || fake.cancelInserts != 1 {
		t.Fatalf("tombstone looked up account: %+v", fake)
	}
	if fake.committed == nil || fake.committed.Status != statusCancelled || fake.committed.Payee != "M100002" || fake.committed.Amount != 40 || fake.committed.TxnNo != "" {
		t.Fatalf("stored = %+v", fake.committed)
	}
}

// ACC-14：Cancel 的 1062 读到 TRIED 后取消，读到 CONFIRMED 得 40004。
func TestExecuteCancelDuplicate(t *testing.T) {
	tried := &payeeFake{
		missOnce:  true,
		committed: &payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried},
	}
	got, err := executeCancel(context.Background(), tried.run, tried, samplePayee("T1"))
	if err != nil || got.Status != statusCancelled || got.TxnNo != "" {
		t.Fatalf("cancel after dup = %+v err=%v", got, err)
	}
	if tried.settleLookups != 0 || tried.posts != 0 || tried.commits != 1 || tried.rollbacks != 1 || tried.committed.Status != statusCancelled {
		t.Fatalf("dup tried = %+v row=%+v", tried, tried.committed)
	}

	confirmed := &payeeFake{
		missOnce: true,
		committed: &payeeBranch{
			Payee: "M100004", Amount: 3, Status: statusConfirmed, TxnNo: "TX0000000000000000004",
		},
	}
	got, err = executeCancel(context.Background(), confirmed.run, confirmed, samplePayee("T1"))
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	assertEmptyIn(t, got)
	if confirmed.commits != 0 || confirmed.posts != 0 || confirmed.settleLookups != 0 || confirmed.committed.Status != statusConfirmed || confirmed.committed.Amount != 3 {
		t.Fatalf("dup confirmed wrote: %+v", confirmed.committed)
	}

	cancelled := &payeeFake{
		missOnce:  true,
		committed: &payeeBranch{Payee: "M100009", Amount: 2, Status: statusCancelled},
	}
	got, err = executeCancel(context.Background(), cancelled.run, cancelled, samplePayee("T1"))
	if err != nil || got.Status != statusCancelled || got.TxnNo != "" {
		t.Fatalf("dup cancelled = %+v err=%v", got, err)
	}
	if cancelled.commits != 0 || cancelled.committed.Amount != 2 || cancelled.committed.Payee != "M100009" || cancelled.settleLookups != 0 {
		t.Fatal("dup cancelled rewrote the tombstone")
	}

	mismatch := &payeeFake{
		missOnce:  true,
		committed: &payeeBranch{Payee: "M100002", Amount: 8, Status: statusTried},
	}
	got, err = executeCancel(context.Background(), mismatch.run, mismatch, samplePayee("T1"))
	assertErrno(t, err, errno.ErrIdempotentConflict)
	assertEmptyIn(t, got)
	if mismatch.commits != 0 || mismatch.committed.Amount != 8 || mismatch.committed.Status != statusTried {
		t.Fatal("mismatch cancel wrote")
	}
}

func TestExecuteCancelLocked(t *testing.T) {
	tried := &payeeFake{committed: &payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried}}
	got, err := executeCancel(context.Background(), tried.run, tried, samplePayee("T1"))
	if err != nil || got.Status != statusCancelled || got.TxnNo != "" || tried.settleLookups != 0 || tried.posts != 0 || tried.committed.Status != statusCancelled {
		t.Fatalf("cancel tried = %+v err=%v row=%+v", got, err, tried.committed)
	}

	mismatch := &payeeFake{committed: &payeeBranch{Payee: "M100004", Amount: 40, Status: statusTried}}
	got, err = executeCancel(context.Background(), mismatch.run, mismatch, samplePayee("T1"))
	assertErrno(t, err, errno.ErrIdempotentConflict)
	assertEmptyIn(t, got)
	if mismatch.commits != 0 || mismatch.committed.Status != statusTried || mismatch.settleLookups != 0 {
		t.Fatal("locked mismatch wrote")
	}

	confirmed := &payeeFake{committed: &payeeBranch{Payee: "M100008", Amount: 1, Status: statusConfirmed, TxnNo: "TX1"}}
	got, err = executeCancel(context.Background(), confirmed.run, confirmed, samplePayee("T1"))
	assertErrno(t, err, errno.ErrFreezeStatusConflict)
	assertEmptyIn(t, got)
	if confirmed.committed.Status != statusConfirmed || confirmed.committed.Amount != 1 || confirmed.posts != 0 {
		t.Fatal("confirmed cancel compared or wrote")
	}

	again := &payeeFake{committed: &payeeBranch{Payee: "M100001", Amount: 5, Status: statusCancelled}}
	got, err = executeCancel(context.Background(), again.run, again, samplePayee("T1"))
	if err != nil || got.Status != statusCancelled || again.committed.Amount != 5 || again.cancelInserts != 0 || again.settleLookups != 0 {
		t.Fatalf("idempotent cancel = %+v row=%+v", got, again.committed)
	}

	zero := &payeeFake{committed: &payeeBranch{Payee: "M100002", Amount: 40, Status: statusTried}, writeZero: true}
	got, err = executeCancel(context.Background(), zero.run, zero, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if zero.commits != 0 || zero.committed.Status != statusTried {
		t.Fatal("zero cancel committed")
	}

	unknown := &payeeFake{committed: &payeeBranch{Payee: "M100002", Amount: 40, Status: statusFailed}}
	got, err = executeCancel(context.Background(), unknown.run, unknown, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
}

func TestExecutePayeeInsertErrors(t *testing.T) {
	text := &payeeFake{insertErr: errors.New("Error 1062: Duplicate entry")}
	got, err := executeTry(context.Background(), text.run, text, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if text.committed != nil || text.posts != 0 {
		t.Fatal("text 1062 took the duplicate path")
	}

	check := &payeeFake{insertErr: &mysql.MySQLError{Number: 3819, Message: "Check constraint 'ck_branch_payer' is violated"}}
	got, err = executeCancel(context.Background(), check.run, check, samplePayee("T1"))
	assertErrno(t, err, errno.ErrInternal)
	assertEmptyIn(t, got)
	if check.committed != nil || strings.Contains(err.Error(), "ck_branch") || strings.Contains(err.Error(), "INSERT") {
		t.Fatalf("check leaked or wrote: %v row=%+v", err, check.committed)
	}
}

func TestTransferInSQLHelpers(t *testing.T) {
	ctx := context.Background()
	var sess sqlPayeeSession
	req := samplePayee("T1")
	if err := sess.insertTried(ctx, req); !errors.Is(err, errNilTx) {
		t.Fatalf("insertTried = %v", err)
	}
	if err := sess.insertCancelled(ctx, req); !errors.Is(err, errNilTx) {
		t.Fatalf("insertCancelled = %v", err)
	}
	if _, _, err := sess.lockPayee(ctx, "T1"); !errors.Is(err, errNilTx) {
		t.Fatalf("lockPayee = %v", err)
	}
	if _, _, err := sess.selectMerchant(ctx, "M100002"); !errors.Is(err, errNilTx) {
		t.Fatalf("selectMerchant = %v", err)
	}
	if _, _, err := sess.selectTransit(ctx, 1); !errors.Is(err, errNilTx) {
		t.Fatalf("selectTransit = %v", err)
	}
	if _, err := sess.markConfirmed(ctx, "T1", "TX1", payeeBranch{Payee: "M100002", Amount: 1}); !errors.Is(err, errNilTx) {
		t.Fatalf("markConfirmed = %v", err)
	}
	if _, err := sess.markCancelled(ctx, "T1", payeeBranch{Payee: "M100002", Amount: 1}); !errors.Is(err, errNilTx) {
		t.Fatalf("markCancelled = %v", err)
	}
	var reader sqlPayeeReader
	if _, _, err := reader.selectPayee(ctx, "T1"); !errors.Is(err, errNilTx) {
		t.Fatalf("selectPayee = %v", err)
	}
	clk := &stepClock{times: []time.Time{openTestTime}}
	svc := New(dal.New(nil, nil), newIDs(t, clk), testSubCount)
	if _, err := (sqlPayeeSession{svc: svc}).postIn(ctx, 9, 4, 1, "T1"); !errors.Is(err, errNilTx) {
		t.Fatal(err)
	}
	if clk.i != 0 {
		t.Fatal("nil tx issued an id")
	}
	row, err := scanPayee(seqScan{vals: []any{
		sql.NullString{String: "M100002", Valid: true},
		int64(40),
		sql.NullString{String: statusConfirmed, Valid: true},
		sql.NullString{String: "TX1", Valid: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if row.Payee != "M100002" || row.Amount != 40 || row.Status != statusConfirmed || row.TxnNo != "TX1" {
		t.Fatalf("payee = %+v", row)
	}
	empty, err := scanPayee(seqScan{vals: []any{
		sql.NullString{String: "M100002", Valid: true},
		int64(1),
		sql.NullString{String: statusTried, Valid: true},
		sql.NullString{},
	}})
	if err != nil || empty.TxnNo != "" || empty.Status != statusTried {
		t.Fatalf("empty txn = %+v err=%v", empty, err)
	}
	if _, err := scanPayee(seqScan{err: errors.New("SELECT boom")}); err == nil {
		t.Fatal("scan error lost")
	}
}

func assertEmptyIn(t *testing.T, got TransferInResult) {
	t.Helper()
	if got.Status != "" || got.TxnNo != "" {
		t.Fatalf("error result = %+v", got)
	}
}

func samplePayee(no string) payeeInsert {
	return payeeInsert{TransferNo: no, Payee: "M100002", Amount: 40}
}

func readyConfirmFake(row *payeeBranch) *payeeFake {
	cp := *row
	return &payeeFake{
		committed:    &cp,
		settleFound:  true,
		settle:       settleAccount{ID: 4, Status: statusActive},
		transitFound: true,
		transitID:    9,
		postTxn:      "TX0000000000000000006",
	}
}

type payeeFake struct {
	committed      *payeeBranch
	insertErr      error
	postErr        error
	postTxn        string
	postDup        bool
	emptyTxn       bool
	writeZero      bool
	forceDup       bool
	missOnce       bool
	missed         bool
	settleFound    bool
	settle         settleAccount
	transitFound   bool
	transitID      uint64
	sawDup         bool
	posts          int
	settleLookups  int
	transitLookups int
	cancelInserts  int
	commits        int
	rollbacks      int
	lastSub        int
	lastTransit    uint64
	lastMerchant   uint64
	lastAmount     money.Amount
	events         []string
}

func (f *payeeFake) run(_ context.Context, fn func(payeeSession) error) error {
	sess := &payeeFakeSession{db: f}
	err := fn(sess)
	if err != nil {
		f.rollbacks++
		return err
	}
	f.commits++
	if sess.staged != nil {
		cp := *sess.staged
		f.committed = &cp
	}
	return nil
}

func (f *payeeFake) selectPayee(context.Context, string) (payeeBranch, bool, error) {
	if f.committed == nil {
		return payeeBranch{}, false, nil
	}
	return *f.committed, true, nil
}

type payeeFakeSession struct {
	db     *payeeFake
	locked *payeeBranch
	staged *payeeBranch
}

func (s *payeeFakeSession) lockPayee(context.Context, string) (payeeBranch, bool, error) {
	s.db.events = append(s.db.events, "lock")
	if s.db.missOnce && !s.db.missed {
		s.db.missed = true
		return payeeBranch{}, false, nil
	}
	if s.db.committed == nil {
		return payeeBranch{}, false, nil
	}
	cp := *s.db.committed
	s.locked = &cp
	return cp, true, nil
}

func (s *payeeFakeSession) insertTried(_ context.Context, req payeeInsert) error {
	s.db.events = append(s.db.events, "insert")
	return s.insert(req, statusTried)
}

func (s *payeeFakeSession) insertCancelled(_ context.Context, req payeeInsert) error {
	s.db.events = append(s.db.events, "insert")
	s.db.cancelInserts++
	return s.insert(req, statusCancelled)
}

func (s *payeeFakeSession) insert(req payeeInsert, status string) error {
	if s.db.insertErr != nil {
		return s.db.insertErr
	}
	if s.db.forceDup || s.db.committed != nil {
		return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'T1' for key 'uk_transfer_role'"}
	}
	s.staged = &payeeBranch{Payee: req.Payee, Amount: req.Amount, Status: status}
	return nil
}

func (s *payeeFakeSession) markConfirmed(_ context.Context, _ string, txnNo string, row payeeBranch) (bool, error) {
	s.db.events = append(s.db.events, "mark")
	if s.db.writeZero || s.locked == nil || txnNo == "" {
		return false, nil
	}
	if s.locked.Status != statusTried || s.locked.TxnNo != "" || s.locked.Amount != row.Amount || s.locked.Payee != row.Payee {
		return false, nil
	}
	next := *s.locked
	next.Status = statusConfirmed
	next.TxnNo = txnNo
	s.staged = &next
	return true, nil
}

func (s *payeeFakeSession) markCancelled(_ context.Context, _ string, row payeeBranch) (bool, error) {
	s.db.events = append(s.db.events, "mark")
	if s.db.writeZero || s.locked == nil {
		return false, nil
	}
	if s.locked.Status != statusTried || s.locked.Amount != row.Amount || s.locked.Payee != row.Payee {
		return false, nil
	}
	next := *s.locked
	next.Status = statusCancelled
	next.TxnNo = ""
	s.staged = &next
	return true, nil
}

func (s *payeeFakeSession) selectMerchant(context.Context, string) (settleAccount, bool, error) {
	s.db.events = append(s.db.events, "merchant")
	s.db.settleLookups++
	if !s.db.settleFound {
		return settleAccount{}, false, nil
	}
	return s.db.settle, true, nil
}

func (s *payeeFakeSession) selectTransit(_ context.Context, subNo int) (uint64, bool, error) {
	s.db.events = append(s.db.events, "transit")
	s.db.transitLookups++
	s.db.lastSub = subNo
	if !s.db.transitFound {
		return 0, false, nil
	}
	return s.db.transitID, true, nil
}

func (s *payeeFakeSession) postIn(_ context.Context, transitID, merchantID uint64, amount money.Amount, _ string) (PostResult, error) {
	s.db.events = append(s.db.events, "post")
	s.db.posts++
	s.db.lastTransit = transitID
	s.db.lastMerchant = merchantID
	s.db.lastAmount = amount
	s.db.sawDup = s.db.postDup
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
