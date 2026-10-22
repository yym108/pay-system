//go:build integration

package handler

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/pkg/accountclient"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/account/biz"
	"github.com/yym108/pay-system/service/account/dal"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := testkit.Shutdown(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %s\n", testkit.Redact(err))
	}
	os.Exit(code)
}

// ACC-11 验收 1 的资金路径：真实 Kitex 客户端调用会改余额的方法，结束时全量 CheckBalance。
// 初始余额用进程内 Post 准备，因为记账不是 RPC。之后的转账、冻结和收款方分支都走 RPC。
func TestRPCMovesMoneyThenCheckBalance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	s0, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, s0) })
	s1, err := my.OpenSchema(ctx, "account")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { closeSchema(t, s1) })
	pools := dal.New(s0.DB, s1.DB)
	ids, err := idgen.New(3, fixedClock{at: time.Date(2026, 10, 22, 10, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*60*60))})
	if err != nil {
		t.Fatal(err)
	}
	svc := biz.New(pools, ids, 1)
	addr := startServer(t, svc, Config{})
	cli, err := accountclient.Dial(addr, accountclient.WithTimeouts(time.Second, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	channel := mustOpen(t, cli, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 0, 0)
	payer := mustOpen(t, cli, "MERCHANT", "M100002", "MERCHANT_SETTLE", 0, 0)
	payee := mustOpen(t, cli, "MERCHANT", "M100004", "MERCHANT_SETTLE", 0, 0)
	again, err := cli.OpenAccount(ctx, &account.OpenAccountRequest{
		OwnerType: "MERCHANT", OwnerId: "M100002", AccountType: "MERCHANT_SETTLE", Shard: 0, SubNo: 0,
	})
	if err != nil || again.GetAccountNo() != payer.GetAccountNo() {
		t.Fatalf("idempotent open = %+v err=%v", again, err)
	}
	_ = mustOpen(t, cli, "PLATFORM", "PLATFORM", "TRANSIT", 0, 0)

	_, err = cli.GetAccount(ctx, &account.GetAccountRequest{OwnerType: "MERCHANT", OwnerId: "M100099", AccountType: "MERCHANT_SETTLE"})
	if got := errno.FromBizStatus(err); got.Code != errno.ErrAccountNotFound.Code {
		t.Fatalf("missing account = %d err=%v", got.Code, err)
	}

	fund(t, ctx, svc, s0.DB, channel.GetAccountNo(), payer.GetAccountNo(), 10000)
	local, err := cli.TransferLocal(ctx, &account.TransferLocalRequest{
		TransferNo: "TFACC1101", PayerMerchantId: "M100002", PayeeMerchantId: "M100004", Amount: 1000,
	})
	if err != nil || local.GetStatus() != "SUCCESS" || local.GetTxnNo() == "" {
		t.Fatalf("transfer local = %+v err=%v", local, err)
	}
	frozen, err := cli.Freeze(ctx, &account.FreezeRequest{BizType: "REFUND", BizNo: "RFACC1101", MerchantId: "M100002", Amount: 500})
	if err != nil || frozen.GetStatus() != "FROZEN" {
		t.Fatalf("freeze = %+v err=%v", frozen, err)
	}
	released, err := cli.ReleaseFreeze(ctx, &account.ReleaseFreezeRequest{BizType: "REFUND", BizNo: "RFACC1101", MerchantId: "M100002", Amount: 500})
	if err != nil || released.GetStatus() != "RELEASED" {
		t.Fatalf("release = %+v err=%v", released, err)
	}
	held, err := cli.Freeze(ctx, &account.FreezeRequest{BizType: "TRANSFER", BizNo: "TFACC1102", MerchantId: "M100002", Amount: 2000})
	if err != nil || held.GetStatus() != "FROZEN" {
		t.Fatalf("transfer freeze = %+v err=%v", held, err)
	}
	confirmed, err := cli.ConfirmFreeze(ctx, &account.ConfirmFreezeRequest{BizType: "TRANSFER", BizNo: "TFACC1102", MerchantId: "M100002", Channel: ""})
	if err != nil || confirmed.GetTxnNo() == "" {
		t.Fatalf("confirm = %+v err=%v", confirmed, err)
	}
	tried, err := cli.TransferInTry(ctx, &account.TransferInTryRequest{TransferNo: "TFACC1102", PayeeMerchantId: "M100004", Amount: 2000})
	if err != nil || tried.GetStatus() != "TRIED" {
		t.Fatalf("try = %+v err=%v", tried, err)
	}
	inConfirmed, err := cli.TransferInConfirm(ctx, &account.TransferInConfirmRequest{TransferNo: "TFACC1102", PayeeMerchantId: "M100004"})
	if err != nil || inConfirmed.GetStatus() != "CONFIRMED" || inConfirmed.GetTxnNo() == "" {
		t.Fatalf("transfer in confirm = %+v err=%v", inConfirmed, err)
	}
	cancelTry, err := cli.TransferInTry(ctx, &account.TransferInTryRequest{TransferNo: "TFACC1103", PayeeMerchantId: "M100004", Amount: 100})
	if err != nil || cancelTry.GetStatus() != "TRIED" {
		t.Fatalf("cancel try = %+v err=%v", cancelTry, err)
	}
	cancelled, err := cli.TransferInCancel(ctx, &account.TransferInCancelRequest{TransferNo: "TFACC1103", PayeeMerchantId: "M100004", Amount: 100})
	if err != nil || cancelled.GetStatus() != "CANCELLED" {
		t.Fatalf("cancel = %+v err=%v", cancelled, err)
	}

	sum, err := cli.GetAccount(ctx, &account.GetAccountRequest{OwnerType: "PLATFORM", OwnerId: "PLATFORM", AccountType: "TRANSIT"})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if sum.IsSetShard() || sum.IsSetSubNo() || sum.GetAccountNo() != "" {
		t.Fatalf("platform summary via RPC = %+v", sum)
	}

	quiet := true
	checked, err := cli.CheckBalance(ctx, &account.CheckBalanceRequest{ExpectQuiescent: &quiet})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !checked.GetPassed() || len(checked.GetFailures()) != 0 {
		t.Fatalf("CheckBalance = %+v", checked)
	}
	_ = payee
}

func mustOpen(t *testing.T, cli *accountclient.Client, ownerType, ownerID, accountType string, shardNo, subNo int32) *account.Account {
	t.Helper()
	got, err := cli.OpenAccount(context.Background(), &account.OpenAccountRequest{
		OwnerType: ownerType, OwnerId: ownerID, AccountType: accountType, Shard: shardNo, SubNo: subNo,
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if got.GetAccountNo() == "" {
		t.Fatal("open returned empty account_no")
	}
	return got
}

func fund(t *testing.T, ctx context.Context, svc *biz.Service, db *sqlx.DB, channelNo, merchantNo string, amount money.Amount) {
	t.Helper()
	channelID := accountID(t, ctx, db, channelNo)
	merchantID := accountID(t, ctx, db, merchantNo)
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		_, postErr := svc.Post(ctx, tx, "PAY", "FUNDACC11", "", []biz.Entry{
			{AccountID: channelID, Direction: "DEBIT", Amount: amount},
			{AccountID: merchantID, Direction: "CREDIT", Amount: amount},
		})
		return postErr
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func accountID(t *testing.T, ctx context.Context, db *sqlx.DB, accountNo string) uint64 {
	t.Helper()
	var id uint64
	if err := db.QueryRowContext(ctx, `SELECT id FROM account WHERE account_no = ?`, accountNo).Scan(&id); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if id == 0 {
		t.Fatal("account id is 0")
	}
	return id
}

func closeSchema(t *testing.T, schema *testkit.Schema) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := schema.Close(ctx); err != nil {
		t.Errorf("close schema: %s", testkit.Redact(err))
	}
}

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }
