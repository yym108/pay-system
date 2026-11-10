//go:build integration

package biz

import (
	"errors"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

func TestPostPayEqualFeeInactive(t *testing.T) {
	// MQ-04：fee == amount 且商户为 FROZEN 时返回 40005，凭证张数是 0。
	env := openEnv(t)
	const merchantID = "M100002"
	openPayBooks(t, env, merchantID)
	db := env.s0.DB
	if _, err := resStatus(t, env, db, "FROZEN"); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	_, err := env.svc.PostPay(env.ctx, "PA-EQ-OFF", merchantID, "mockpay", 80, 80)
	if !errors.Is(err, errno.ErrAccountInactive) {
		t.Fatalf("equal fee inactive = %v", err)
	}
	if n := countPayTxn(t, env, db, "PA-EQ-OFF"); n != 0 {
		t.Fatalf("vouchers = %d", n)
	}
	if n := countPayEntries(t, env, db, "PA-EQ-OFF"); n != 0 {
		t.Fatalf("entries = %d", n)
	}
	checked, err := env.svc.CheckBalance(env.ctx, "", true)
	if err != nil || !checked.Passed {
		t.Fatalf("check = %+v err=%v", checked, err)
	}
}

func TestPostPayEntriesAndDuplicate(t *testing.T) {
	env := openEnv(t)
	const merchantID = "M100002"
	openPayBooks(t, env, merchantID)
	db := env.s0.DB

	first, err := env.svc.PostPay(env.ctx, "PA-PAY-1", merchantID, "mockpay", 10000, 60)
	if err != nil || first.Duplicated || first.TxnNo == "" {
		t.Fatalf("post = %+v err=%v", first, err)
	}
	if n := countPayTxn(t, env, db, "PA-PAY-1"); n != 1 {
		t.Fatalf("vouchers = %d", n)
	}
	assertPayLegs(t, env, db, "PA-PAY-1", map[string]legExpect{
		typeChannelReceivable: {directionDebit, 10000},
		typeMerchantSettle:    {directionCredit, 9940},
		typeFeeIncome:         {directionCredit, 60},
	})

	again, err := env.svc.PostPay(env.ctx, "PA-PAY-1", merchantID, "mockpay", 10000, 60)
	if err != nil || !again.Duplicated || again.TxnNo != first.TxnNo {
		t.Fatalf("dup = %+v err=%v", again, err)
	}
	if n := countPayTxn(t, env, db, "PA-PAY-1"); n != 1 {
		t.Fatalf("vouchers after dup = %d", n)
	}

	if _, err = env.svc.PostPay(env.ctx, "PA-PAY-0", merchantID, "mockpay", 5000, 0); err != nil {
		t.Fatal(err)
	}
	assertPayLegs(t, env, db, "PA-PAY-0", map[string]legExpect{
		typeChannelReceivable: {directionDebit, 5000},
		typeMerchantSettle:    {directionCredit, 5000},
	})
	if n := countPayEntries(t, env, db, "PA-PAY-0"); n != 2 {
		t.Fatalf("zero fee entries = %d", n)
	}

	if _, err = env.svc.PostPay(env.ctx, "PA-PAY-EQ", merchantID, "mockpay", 80, 80); err != nil {
		t.Fatal(err)
	}
	assertPayLegs(t, env, db, "PA-PAY-EQ", map[string]legExpect{
		typeChannelReceivable: {directionDebit, 80},
		typeFeeIncome:         {directionCredit, 80},
	})
	if n := countPayEntries(t, env, db, "PA-PAY-EQ"); n != 2 {
		t.Fatalf("equal fee entries = %d", n)
	}

	if _, err = env.svc.PostPay(env.ctx, "PA-NO-MERCHANT", "M100004", "mockpay", 10, 1); !errors.Is(err, errno.ErrAccountNotFound) {
		t.Fatalf("missing merchant = %v", err)
	}
	if _, err = env.svc.PostPay(env.ctx, "PA-NO-SUB", merchantID, "otherch", 10, 1); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("missing sub = %v", err)
	}
	if _, err = resStatus(t, env, db, "FROZEN"); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err = env.svc.PostPay(env.ctx, "PA-OFF", merchantID, "mockpay", 10, 1); !errors.Is(err, errno.ErrAccountInactive) {
		t.Fatalf("inactive = %v", err)
	}
	if n := countPayTxn(t, env, db, "PA-OFF"); n != 0 {
		t.Fatalf("inactive vouchers = %d", n)
	}
	if _, err = env.svc.PostPay(env.ctx, "PA-BAD-FEE", merchantID, "mockpay", 10, 11); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("fee = %v", err)
	}
	checked, err := env.svc.CheckBalance(env.ctx, "", true)
	if err != nil || !checked.Passed {
		t.Fatalf("check = %+v err=%v", checked, err)
	}
}

type legExpect struct {
	direction string
	amount    money.Amount
}

func openPayBooks(t *testing.T, env shardEnv, merchantID string) {
	t.Helper()
	shardNo, err := shard.Of(merchantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = env.svc.OpenAccount(env.ctx, ownerMerchant, merchantID, typeMerchantSettle, shardNo, 0); err != nil {
		t.Fatal(err)
	}
	for sub := 0; sub < testSubCount; sub++ {
		if _, err = env.svc.OpenAccount(env.ctx, ownerPlatform, "mockpay", typeChannelReceivable, shardNo, sub); err != nil {
			t.Fatal(err)
		}
		if _, err = env.svc.OpenAccount(env.ctx, ownerPlatform, platformOwnerID, typeFeeIncome, shardNo, sub); err != nil {
			t.Fatal(err)
		}
	}
}

func countPayTxn(t *testing.T, env shardEnv, db *sqlx.DB, payNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM ledger_txn WHERE biz_type = 'PAY' AND biz_no = ?`, payNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func countPayEntries(t *testing.T, env shardEnv, db *sqlx.DB, payNo string) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(env.ctx, `
SELECT COUNT(*) FROM ledger_entry e
JOIN ledger_txn t ON t.txn_no = e.txn_no
WHERE t.biz_type = 'PAY' AND t.biz_no = ?`, payNo).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func assertPayLegs(t *testing.T, env shardEnv, db *sqlx.DB, payNo string, want map[string]legExpect) {
	t.Helper()
	rows, err := db.QueryContext(env.ctx, `
SELECT a.account_type, e.direction, e.amount
FROM ledger_entry e
JOIN ledger_txn t ON t.txn_no = e.txn_no
JOIN account a ON a.id = e.account_id
WHERE t.biz_type = 'PAY' AND t.biz_no = ?`, payNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	got := map[string]legExpect{}
	for rows.Next() {
		var accountType, direction string
		var amount int64
		if err = rows.Scan(&accountType, &direction, &amount); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if _, ok := got[accountType]; ok {
			t.Fatalf("duplicate leg %s", accountType)
		}
		got[accountType] = legExpect{direction, money.Amount(amount)}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if len(got) != len(want) {
		t.Fatalf("legs = %+v want %+v", got, want)
	}
	for key, exp := range want {
		if got[key] != exp {
			t.Fatalf("%s = %+v want %+v", key, got[key], exp)
		}
	}
}

func resStatus(t *testing.T, env shardEnv, db *sqlx.DB, status string) (int64, error) {
	t.Helper()
	res, err := db.ExecContext(env.ctx, `
UPDATE account SET status = ?
WHERE owner_type = 'MERCHANT' AND owner_id = 'M100002' AND account_type = 'MERCHANT_SETTLE'`, status)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
