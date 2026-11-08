//go:build integration

package biz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	accountbiz "github.com/yym108/pay-system/service/account/biz"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
)

// accountBizTCC 在测试里直接调用 account 的分支方法，不起 Kitex。
type accountBizTCC struct {
	svc *accountbiz.Service
}

func (a accountBizTCC) Freeze(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	_, err := a.svc.Freeze(ctx, transferBizType, transferNo, payer, amount)
	return err
}

func (a accountBizTCC) TryPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	_, err := a.svc.TransferInTry(ctx, transferNo, payee, amount)
	return err
}

func (a accountBizTCC) ConfirmPayer(ctx context.Context, transferNo, payer string) error {
	_, err := a.svc.ConfirmFreeze(ctx, transferBizType, transferNo, payer, "")
	return err
}

func (a accountBizTCC) ConfirmPayee(ctx context.Context, transferNo, payee string) error {
	_, err := a.svc.TransferInConfirm(ctx, transferNo, payee)
	return err
}

func (a accountBizTCC) CancelPayer(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	_, err := a.svc.ReleaseFreeze(ctx, transferBizType, transferNo, payer, amount)
	return err
}

func (a accountBizTCC) CancelPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	_, err := a.svc.TransferInCancel(ctx, transferNo, payee, amount)
	return err
}

// errLogTCC 只保留最近的分支错误，失败信息里能看到是哪一步超时。
type errLogTCC struct {
	next TCCBrancher
	mu   sync.Mutex
	log  []string
}

func (e *errLogTCC) note(transferNo, step string, err error) error {
	if err == nil {
		return nil
	}
	e.mu.Lock()
	e.log = append(e.log, transferNo+" "+step+": "+errChain(err))
	if len(e.log) > 6 {
		e.log = e.log[len(e.log)-6:]
	}
	e.mu.Unlock()
	return err
}

// errChain 把 Unwrap 链上每一层的 Error() 拼出来。errno 的文本不含 cause。
func errChain(err error) string {
	var b strings.Builder
	for err != nil {
		if b.Len() > 0 {
			b.WriteString(": ")
		}
		b.WriteString(err.Error())
		err = errors.Unwrap(err)
	}
	return b.String()
}

func (e *errLogTCC) recent() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return strings.Join(e.log, "; ")
}

func (e *errLogTCC) Freeze(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	return e.note(transferNo, "freeze", e.next.Freeze(ctx, transferNo, payer, amount))
}

func (e *errLogTCC) TryPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	return e.note(transferNo, "try", e.next.TryPayee(ctx, transferNo, payee, amount))
}

func (e *errLogTCC) ConfirmPayer(ctx context.Context, transferNo, payer string) error {
	return e.note(transferNo, "confirm-payer", e.next.ConfirmPayer(ctx, transferNo, payer))
}

func (e *errLogTCC) ConfirmPayee(ctx context.Context, transferNo, payee string) error {
	return e.note(transferNo, "confirm-payee", e.next.ConfirmPayee(ctx, transferNo, payee))
}

func (e *errLogTCC) CancelPayer(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	return e.note(transferNo, "cancel-payer", e.next.CancelPayer(ctx, transferNo, payer, amount))
}

func (e *errLogTCC) CancelPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	return e.note(transferNo, "cancel-payee", e.next.CancelPayee(ctx, transferNo, payee, amount))
}

type hookedTCC struct {
	next         TCCBrancher
	tryPayee     func(ctx context.Context, transferNo, payee string, amount money.Amount) error
	confirmPayer func(ctx context.Context, transferNo, payer string) error
}

func (h hookedTCC) Freeze(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	return h.next.Freeze(ctx, transferNo, payer, amount)
}

func (h hookedTCC) TryPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	if h.tryPayee != nil {
		return h.tryPayee(ctx, transferNo, payee, amount)
	}
	return h.next.TryPayee(ctx, transferNo, payee, amount)
}

func (h hookedTCC) ConfirmPayer(ctx context.Context, transferNo, payer string) error {
	if h.confirmPayer != nil {
		return h.confirmPayer(ctx, transferNo, payer)
	}
	return h.next.ConfirmPayer(ctx, transferNo, payer)
}

func (h hookedTCC) ConfirmPayee(ctx context.Context, transferNo, payee string) error {
	return h.next.ConfirmPayee(ctx, transferNo, payee)
}

func (h hookedTCC) CancelPayer(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	return h.next.CancelPayer(ctx, transferNo, payer, amount)
}

func (h hookedTCC) CancelPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	return h.next.CancelPayee(ctx, transferNo, payee, amount)
}

func forTCCModes(t *testing.T, fn func(t *testing.T, parallel bool)) {
	t.Helper()
	for _, parallel := range []bool{true, false} {
		parallel := parallel
		name := "serial"
		if parallel {
			name = "parallel"
		}
		t.Run(name, func(t *testing.T) { fn(t, parallel) })
	}
}

func openTCCBooks(t *testing.T, ctx context.Context, svc *accountbiz.Service) {
	t.Helper()
	for _, shardNo := range []int{0, 1} {
		for sub := 0; sub < 8; sub++ {
			if _, err := svc.OpenAccount(ctx, "PLATFORM", "PLATFORM", "TRANSIT", shardNo, sub); err != nil {
				t.Fatal(testkit.Redact(err))
			}
		}
		if _, err := svc.OpenAccount(ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", shardNo, 0); err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
	if _, err := svc.OpenAccount(ctx, "MERCHANT", "M100001", "MERCHANT_SETTLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := svc.OpenAccount(ctx, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func fundMerchant(t *testing.T, ctx context.Context, db *sqlx.DB, svc *accountbiz.Service, merchant string, shard int, amount money.Amount, bizNo string) {
	t.Helper()
	if amount <= 0 {
		return
	}
	merchantID := accountIDByOwner(t, ctx, db, "MERCHANT", merchant, "MERCHANT_SETTLE")
	channelID := accountIDByOwner(t, ctx, db, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	postBalanced(t, ctx, db, svc, bizNo, []accountbiz.Entry{
		{AccountID: channelID, Direction: "DEBIT", Amount: amount},
		{AccountID: merchantID, Direction: "CREDIT", Amount: amount},
	})
}

func countBizTxn(t *testing.T, ctx context.Context, db *sqlx.DB, bizType, bizNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM ledger_txn WHERE biz_type = ? AND biz_no = ?`, bizType, bizNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func freezeStatus(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) string {
	t.Helper()
	var status string
	err := db.QueryRowContext(ctx, `
SELECT status FROM account_freeze WHERE biz_type = 'TRANSFER' AND biz_no = ?`, transferNo).Scan(&status)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return status
}

func payeeBranchStatus(t *testing.T, ctx context.Context, db *sqlx.DB, transferNo string) string {
	t.Helper()
	var status string
	err := db.QueryRowContext(ctx, `
SELECT status FROM transfer_branch WHERE transfer_no = ? AND role = 'PAYEE'`, transferNo).Scan(&status)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return status
}

func accountBalance(t *testing.T, ctx context.Context, db *sqlx.DB, id uint64) (balance, frozen int64) {
	t.Helper()
	err := db.QueryRowContext(ctx, `SELECT balance, frozen_amount FROM account WHERE id = ?`, id).Scan(&balance, &frozen)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return balance, frozen
}

func transitSum(t *testing.T, ctx context.Context, db *sqlx.DB) int64 {
	t.Helper()
	var sum int64
	err := db.QueryRowContext(ctx, `
SELECT COALESCE(SUM(balance), 0) FROM account
WHERE owner_type = 'PLATFORM' AND owner_id = 'PLATFORM' AND account_type = 'TRANSIT'`).Scan(&sum)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return sum
}

func requireCheck(t *testing.T, ctx context.Context, svc *accountbiz.Service) {
	t.Helper()
	check, err := svc.CheckBalance(ctx, "", true)
	if err != nil || !check.Passed {
		t.Fatalf("check = %+v err=%v", check, err)
	}
}

// TRF-03 验收 1：M100001 → M100002 转 500 成功，两个分片各一张凭证，在途合计为 0。
func TestTCCSuccessMySQL(t *testing.T) {
	forTCCModes(t, func(t *testing.T, parallel bool) {
		env := openPay(t)
		p := openTransfers(t, env, feeAt(11, 0, 0, 0))
		accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
		keepSchemas(t, schema0, schema1)
		openTCCBooks(t, env.ctx, accounts)
		fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 500, "PAY-TRF03-SEED")
		p.UseTCCTransfer(accountBizTCC{svc: accounts}, parallel)
		got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
			MerchantID: "M100001", OutTransferNo: "TRF03-OK", PayeeMerchantID: "M100002", Amount: 500,
		})
		if err != nil || got.Status != transferStatusSuccess || got.TxnNo != "" || got.FailCode != "" || got.Version != 2 {
			t.Fatalf("got = %+v err=%v", got, err)
		}
		row := loadTransferRow(t, env, got.TransferNo)
		if row.status != transferStatusSuccess || row.txnNo.Valid || row.failCode.Valid || !row.finishedAt.Valid || row.version != 2 {
			t.Fatalf("row = %+v", row)
		}
		if countBizTxn(t, env.ctx, s1, "TRANSFER_OUT", got.TransferNo) != 1 {
			t.Fatal("missing TRANSFER_OUT")
		}
		if countBizTxn(t, env.ctx, schema0.DB, "TRANSFER_IN", got.TransferNo) != 1 {
			t.Fatal("missing TRANSFER_IN")
		}
		if transitSum(t, env.ctx, s1)+transitSum(t, env.ctx, schema0.DB) != 0 {
			t.Fatal("transit not zero")
		}
		box := loadOutbox(t, env, got.TransferNo)
		if len(box) != 1 || box[0].eventType != "transfer.succeeded" || box[0].msgKey != got.TransferNo || box[0].bizNo != got.TransferNo {
			t.Fatalf("outbox = %+v", box)
		}
		requireCheck(t, env.ctx, accounts)
	})
}

// TRF-03 验收 2：付款方余额不足，40002，冻结单 RELEASED，收款分支 CANCELLED，没有转账凭证。
func TestTCCInsufficientMySQL(t *testing.T) {
	forTCCModes(t, func(t *testing.T, parallel bool) {
		env := openPay(t)
		p := openTransfers(t, env, feeAt(11, 0, 0, 0))
		accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
		keepSchemas(t, schema0, schema1)
		openTCCBooks(t, env.ctx, accounts)
		fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 100, "PAY-TRF03-POOR")
		p.UseTCCTransfer(accountBizTCC{svc: accounts}, parallel)
		got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
			MerchantID: "M100001", OutTransferNo: "TRF03-POOR", PayeeMerchantID: "M100002", Amount: 500,
		})
		if !errors.Is(err, errno.ErrInsufficientBalance) || got.Status != transferStatusFailed || got.FailCode != transferFailInsufficient {
			t.Fatalf("got = %+v err=%v", got, err)
		}
		if freezeStatus(t, env.ctx, s1, got.TransferNo) != "RELEASED" {
			t.Fatalf("freeze = %s", freezeStatus(t, env.ctx, s1, got.TransferNo))
		}
		if payeeBranchStatus(t, env.ctx, schema0.DB, got.TransferNo) != "CANCELLED" {
			t.Fatalf("payee = %s", payeeBranchStatus(t, env.ctx, schema0.DB, got.TransferNo))
		}
		if countBizTxn(t, env.ctx, s1, "TRANSFER_OUT", got.TransferNo) != 0 || countBizTxn(t, env.ctx, schema0.DB, "TRANSFER_IN", got.TransferNo) != 0 {
			t.Fatal("unexpected voucher")
		}
		box := loadOutbox(t, env, got.TransferNo)
		if len(box) != 1 || box[0].eventType != "transfer.failed" {
			t.Fatalf("outbox = %+v", box)
		}
		requireCheck(t, env.ctx, accounts)
	})
}

// TRF-03 验收 3：收款方 Try 超时后退回付款方冻结，可用余额恢复。
func TestTCCPayeeTimeoutMySQL(t *testing.T) {
	forTCCModes(t, func(t *testing.T, parallel bool) {
		env := openPay(t)
		p := openTransfers(t, env, feeAt(11, 0, 0, 0))
		accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
		keepSchemas(t, schema0, schema1)
		openTCCBooks(t, env.ctx, accounts)
		fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 500, "PAY-TRF03-TIME")
		payerID := accountIDByOwner(t, env.ctx, s1, "MERCHANT", "M100001", "MERCHANT_SETTLE")
		p.UseTCCTransfer(hookedTCC{
			next: accountBizTCC{svc: accounts},
			tryPayee: func(context.Context, string, string, money.Amount) error {
				return errno.ErrUnavailable
			},
		}, parallel)
		got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
			MerchantID: "M100001", OutTransferNo: "TRF03-TIME", PayeeMerchantID: "M100002", Amount: 100,
		})
		if err != nil || got.Status != transferStatusFailed || got.FailCode != transferFailTryTimeout {
			t.Fatalf("got = %+v err=%v", got, err)
		}
		balance, frozen := accountBalance(t, env.ctx, s1, payerID)
		if balance != 500 || frozen != 0 {
			t.Fatalf("balance=%d frozen=%d", balance, frozen)
		}
		if freezeStatus(t, env.ctx, s1, got.TransferNo) != "RELEASED" {
			t.Fatal("freeze not released")
		}
		requireCheck(t, env.ctx, accounts)
	})
}

// TRF-03 验收 4：付款方 Confirm 失败一次，转账单停在 CONFIRMING。
func TestTCCConfirmFailMySQL(t *testing.T) {
	forTCCModes(t, func(t *testing.T, parallel bool) {
		env := openPay(t)
		p := openTransfers(t, env, feeAt(11, 0, 0, 0))
		accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
		keepSchemas(t, schema0, schema1)
		openTCCBooks(t, env.ctx, accounts)
		fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 500, "PAY-TRF03-CFM")
		p.UseTCCTransfer(hookedTCC{
			next: accountBizTCC{svc: accounts},
			confirmPayer: func(context.Context, string, string) error {
				return errno.ErrInternal
			},
		}, parallel)
		got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
			MerchantID: "M100001", OutTransferNo: "TRF03-CFM", PayeeMerchantID: "M100002", Amount: 100,
		})
		if err != nil || got.Status != transferStatusConfirming || got.Version != 1 {
			t.Fatalf("got = %+v err=%v", got, err)
		}
		row := loadTransferRow(t, env, got.TransferNo)
		if row.status != transferStatusConfirming || row.failCode.Valid || row.finishedAt.Valid {
			t.Fatalf("row = %+v", row)
		}
		if box := loadOutbox(t, env, got.TransferNo); len(box) != 0 {
			t.Fatalf("outbox = %+v", box)
		}
		if countBizTxn(t, env.ctx, s1, "TRANSFER_OUT", got.TransferNo) != 0 {
			t.Fatal("unexpected TRANSFER_OUT")
		}
		// 付款方 Confirm 失败后收款方仍入账，在途不为 0。不能声明静止；分录配平仍要核对。
		check, err := accounts.CheckBalance(env.ctx, "", false)
		if err != nil || !check.Passed {
			t.Fatalf("check = %+v err=%v", check, err)
		}
	})
}

// TRF-03 验收 5：两个分片对向各 200 笔后余额之和不变，在途为 0。
func TestTCCConcurrentBothWaysMySQL(t *testing.T) {
	forTCCModes(t, func(t *testing.T, parallel bool) {
		env := openPay(t)
		p := openTransfers(t, env, feeAt(11, 0, 0, 0))
		accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
		keepSchemas(t, schema0, schema1)
		openTCCBooks(t, env.ctx, accounts)
		fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 10000, "PAY-TRF03-A")
		fundMerchant(t, env.ctx, schema0.DB, accounts, "M100002", 0, 10000, "PAY-TRF03-B")
		logged := &errLogTCC{next: accountBizTCC{svc: accounts}}
		p.UseTCCTransfer(logged, parallel)
		const n = 200
		var g errgroup.Group
		for i := 0; i < n; i++ {
			i := i
			g.Go(func() error {
				got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
					MerchantID: "M100001", OutTransferNo: fmt.Sprintf("AB%03d", i),
					PayeeMerchantID: "M100002", Amount: 1,
				})
				if err != nil {
					return fmt.Errorf("A→B %d: status=%s fail=%s: %w; branches=%s", i, got.Status, got.FailCode, err, logged.recent())
				}
				if got.Status != transferStatusSuccess {
					return fmt.Errorf("A→B %d: status=%s fail=%s branches=%s", i, got.Status, got.FailCode, logged.recent())
				}
				return nil
			})
			g.Go(func() error {
				got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
					MerchantID: "M100002", OutTransferNo: fmt.Sprintf("BA%03d", i),
					PayeeMerchantID: "M100001", Amount: 1,
				})
				if err != nil {
					return fmt.Errorf("B→A %d: status=%s fail=%s: %w; branches=%s", i, got.Status, got.FailCode, err, logged.recent())
				}
				if got.Status != transferStatusSuccess {
					return fmt.Errorf("B→A %d: status=%s fail=%s branches=%s", i, got.Status, got.FailCode, logged.recent())
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		aID := accountIDByOwner(t, env.ctx, s1, "MERCHANT", "M100001", "MERCHANT_SETTLE")
		bID := accountIDByOwner(t, env.ctx, schema0.DB, "MERCHANT", "M100002", "MERCHANT_SETTLE")
		aBal, _ := accountBalance(t, env.ctx, s1, aID)
		bBal, _ := accountBalance(t, env.ctx, schema0.DB, bID)
		if aBal+bBal != 20000 {
			t.Fatalf("balances A=%d B=%d", aBal, bBal)
		}
		if transitSum(t, env.ctx, s1)+transitSum(t, env.ctx, schema0.DB) != 0 {
			t.Fatal("transit not zero")
		}
		requireCheck(t, env.ctx, accounts)
	})
}
