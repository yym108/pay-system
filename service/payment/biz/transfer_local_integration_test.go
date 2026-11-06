//go:build integration

package biz

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	accountbiz "github.com/yym108/pay-system/service/account/biz"
	accountdal "github.com/yym108/pay-system/service/account/dal"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

// accountBizLocal 在测试里直接调用 account 的 TransferLocal，不起 Kitex。
type accountBizLocal struct {
	svc *accountbiz.Service
}

func (a accountBizLocal) TransferLocal(ctx context.Context, transferNo, payer, payee string, amount money.Amount) (LocalTransferResult, error) {
	res, err := a.svc.TransferLocal(ctx, transferNo, payer, payee, amount)
	if err != nil {
		return LocalTransferResult{}, err
	}
	return LocalTransferResult{Status: res.Status, TxnNo: res.TxnNo, FailCode: res.FailCode}, nil
}

func openAccountPair(t *testing.T, ctx context.Context) (*accountbiz.Service, *sqlx.DB, *testkit.Schema, *testkit.Schema) {
	t.Helper()
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	s0, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	s1, err := my.OpenSchema(ctx, "account")
	if err != nil {
		// 测试函数还没登记 Cleanup。这里 ctx 仍然有效，先关掉已经打开的分片。
		_ = s0.Close(ctx)
		t.Fatal(testkit.Redact(err))
	}
	clk := &clock.Fake{}
	clk.Set(feeAt(11, 0, 0, 0))
	ids, err := idgen.New(11, clk)
	if err != nil {
		t.Fatal(err)
	}
	return accountbiz.New(accountdal.New(s0.DB, s1.DB), ids, 8), s1.DB, s0, s1
}

// keepSchemas 在测试结束时关闭账户库。不能写在带 context 参数的函数里，否则 contextcheck 要求把那个 context 传进关闭函数；关闭发生在测试 ctx 取消之后，必须另开超时。
func keepSchemas(t *testing.T, schemas ...*testkit.Schema) {
	t.Helper()
	for _, schema := range schemas {
		schema := schema
		t.Cleanup(func() { closeAccountSchema(t, schema) })
	}
}

func accountIDByOwner(t *testing.T, ctx context.Context, db *sqlx.DB, ownerType, ownerID, accountType string) uint64 {
	t.Helper()
	var id uint64
	err := db.QueryRowContext(ctx, `
SELECT id FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND sub_no = 0`,
		ownerType, ownerID, accountType).Scan(&id)
	if err != nil || id == 0 {
		t.Fatalf("account id %s/%s: %v", ownerType, ownerID, err)
	}
	return id
}

func postBalanced(t *testing.T, ctx context.Context, db *sqlx.DB, svc *accountbiz.Service, bizNo string, entries []accountbiz.Entry) {
	t.Helper()
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		_, postErr := svc.Post(ctx, tx, "PAY", bizNo, "", entries)
		return postErr
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func countTransferVouchers(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM ledger_txn WHERE biz_type = 'TRANSFER' AND biz_no = ?`, transferNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func countFailedLocalBranch(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM transfer_branch
WHERE transfer_no = ? AND role = 'LOCAL' AND status = 'FAILED' AND fail_code = 'INSUFFICIENT_BALANCE'`,
		transferNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

// TRF-02 验收 1：A 余额 1000，100 个并发请求各转 20 给同分片的 C，恰好 50 个 SUCCESS、50 个 40002，A+C 不变，CheckBalance 通过。
func TestTransferLocalConcurrentBalanceMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(11, 0, 0, 0)
	p := openTransfers(t, env, now)
	accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
	keepSchemas(t, schema0, schema1)
	const payer, payee = "M100001", "M100003"
	if _, err := accounts.OpenAccount(env.ctx, "MERCHANT", payer, "MERCHANT_SETTLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := accounts.OpenAccount(env.ctx, "MERCHANT", payee, "MERCHANT_SETTLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := accounts.OpenAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	payerID := accountIDByOwner(t, env.ctx, s1, "MERCHANT", payer, "MERCHANT_SETTLE")
	payeeID := accountIDByOwner(t, env.ctx, s1, "MERCHANT", payee, "MERCHANT_SETTLE")
	channelID := accountIDByOwner(t, env.ctx, s1, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	postBalanced(t, env.ctx, s1, accounts, "PAY-TRF02-SEED", []accountbiz.Entry{
		{AccountID: channelID, Direction: "DEBIT", Amount: 1000},
		{AccountID: payerID, Direction: "CREDIT", Amount: 1000},
	})
	p.UseLocalTransfer(accountBizLocal{svc: accounts})

	const n = 100
	type outcome struct {
		order TransferOrder
		err   error
	}
	results := make([]outcome, n)
	var mu sync.Mutex
	var g errgroup.Group
	for i := 0; i < n; i++ {
		i := i
		g.Go(func() error {
			got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
				MerchantID: payer, OutTransferNo: fmt.Sprintf("TRF02C%03d", i),
				PayeeMerchantID: payee, Amount: 20,
			})
			mu.Lock()
			results[i] = outcome{order: got, err: err}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	var success, failed int
	for _, item := range results {
		switch {
		case item.err == nil && item.order.Status == transferStatusSuccess && item.order.TxnNo != "":
			success++
			row := loadTransferRow(t, env, item.order.TransferNo)
			if row.status != transferStatusSuccess || !row.txnNo.Valid || row.failCode.Valid || !row.finishedAt.Valid || row.version != 1 {
				t.Fatalf("success row = %+v", row)
			}
			if countTransferVouchers(t, env.ctx, s1, item.order.TransferNo) != 1 {
				t.Fatalf("voucher %s", item.order.TransferNo)
			}
			box := loadOutbox(t, env, item.order.TransferNo)
			if len(box) != 1 || box[0].eventType != "transfer.succeeded" || box[0].msgKey != item.order.TransferNo {
				t.Fatalf("outbox = %+v", box)
			}
		case item.err != nil && errors.Is(item.err, errno.ErrInsufficientBalance) &&
			item.order.Status == transferStatusFailed && item.order.FailCode == transferFailInsufficient:
			failed++
			row := loadTransferRow(t, env, item.order.TransferNo)
			if row.status != transferStatusFailed || row.txnNo.Valid || !row.failCode.Valid || row.failCode.String != transferFailInsufficient || !row.finishedAt.Valid || row.version != 1 {
				t.Fatalf("failed row = %+v", row)
			}
			if countTransferVouchers(t, env.ctx, s1, item.order.TransferNo) != 0 ||
				countFailedLocalBranch(t, env.ctx, s1, item.order.TransferNo) != 1 {
				t.Fatalf("failed voucher/branch %s", item.order.TransferNo)
			}
			box := loadOutbox(t, env, item.order.TransferNo)
			if len(box) != 1 || box[0].eventType != "transfer.failed" {
				t.Fatalf("outbox = %+v", box)
			}
		default:
			t.Fatalf("unexpected %+v err=%v", item.order, item.err)
		}
	}
	if success != 50 || failed != 50 {
		t.Fatalf("success=%d failed=%d", success, failed)
	}
	var payerBal, payeeBal int64
	if err := s1.QueryRowContext(env.ctx, `SELECT balance FROM account WHERE id = ?`, payerID).Scan(&payerBal); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if err := s1.QueryRowContext(env.ctx, `SELECT balance FROM account WHERE id = ?`, payeeID).Scan(&payeeBal); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if payerBal != 0 || payeeBal != 1000 || payerBal+payeeBal != 1000 {
		t.Fatalf("balances payer=%d payee=%d", payerBal, payeeBal)
	}
	check, err := accounts.CheckBalance(env.ctx, "", true)
	if err != nil || !check.Passed {
		t.Fatalf("check = %+v err=%v", check, err)
	}
	for _, merchant := range []string{payer, payee} {
		one, oneErr := accounts.CheckBalance(env.ctx, merchant, false)
		if oneErr != nil || !one.Passed {
			t.Fatalf("merchant %s check = %+v err=%v", merchant, one, oneErr)
		}
	}
}

// TRF-02 验收 2：account 不可用时返回 TRYING，转账单保持 TRYING，没有转账凭证。
func TestTransferLocalAccountUnavailableMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(11, 0, 0, 0)
	p := openTransfers(t, env, now)
	_, s1, schema0, schema1 := openAccountPair(t, env.ctx)
	keepSchemas(t, schema0, schema1)
	local := &scriptedLocal{err: errno.ErrUnavailable}
	p.UseLocalTransfer(local)
	got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF02-DOWN", PayeeMerchantID: "M100003", Amount: 20,
	})
	if err != nil || got.Status != transferStatusTrying || got.Mode != transferModeLocal {
		t.Fatalf("got = %+v err=%v", got, err)
	}
	row := loadTransferRow(t, env, got.TransferNo)
	if row.status != transferStatusTrying || row.failCode.Valid || row.txnNo.Valid || row.finishedAt.Valid || row.version != 0 {
		t.Fatalf("row = %+v", row)
	}
	if box := loadOutbox(t, env, got.TransferNo); len(box) != 0 {
		t.Fatalf("outbox = %+v", box)
	}
	if countTransferVouchers(t, env.ctx, s1, got.TransferNo) != 0 {
		t.Fatal("unexpected transfer voucher")
	}
}

// TRF-02：装上 local 之后，TCC 仍返回 TRYING，不调用 TransferLocal。
func TestTransferLocalTCCDoesNotCallAccountMySQL(t *testing.T) {
	env := openPay(t)
	p := openTransfers(t, env, feeAt(11, 0, 0, 0))
	local := &scriptedLocal{fn: func(string) (LocalTransferResult, error) {
		t.Fatal("TCC called TransferLocal")
		return LocalTransferResult{}, nil
	}}
	p.UseLocalTransfer(local)
	got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF02-TCC", PayeeMerchantID: "M100002", Amount: 20,
	})
	if err != nil || got.Mode != transferModeTCC || got.Status != transferStatusTrying || local.callCount() != 0 {
		t.Fatalf("got = %+v err=%v calls=%d", got, err, local.callCount())
	}
}
