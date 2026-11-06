//go:build integration

package biz

import (
	"database/sql"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
)

// openTransfers 插入付款商户 M100001 与收款方 M100002、M100003（ACTIVE）、M100004（DISABLED），
// 返回连真实 MySQL 的 Payments。商户配置不缓存。
func openTransfers(t *testing.T, env payEnv, now time.Time) *Payments {
	t.Helper()
	insertMerchant(t, env, "M100001", 60, "ACTIVE")
	insertMerchant(t, env, "M100002", 60, "ACTIVE")
	insertMerchant(t, env, "M100003", 60, "ACTIVE")
	insertMerchant(t, env, "M100004", 60, "DISABLED")
	clk := &clock.Fake{}
	clk.Set(now)
	ids, err := idgen.New(9, clk)
	if err != nil {
		t.Fatal(err)
	}
	p := NewPayments(NewMerchants(env.db, nil, clk, 0), ids, clk, "merchant-demo", env.db)
	p.UseTransfer(10*time.Second, nil)
	return p
}

type transferDBRow struct {
	transferNo, merchantID, outNo, payee, currency, notify, mode, status string
	amount                                                               int64
	payerShard, payeeShard                                               int
	remark, failCode, txnNo                                              sql.NullString
	retryCount                                                           int
	version                                                              uint64
	nextRetryAt, createdAt                                               time.Time
	finishedAt                                                           sql.NullTime
}

func loadTransferRow(t *testing.T, env payEnv, transferNo string) transferDBRow {
	t.Helper()
	var r transferDBRow
	err := env.db.QueryRowContext(env.ctx, `
SELECT transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
       mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
       version, created_at
FROM transfer_order WHERE transfer_no = ?`, transferNo).Scan(
		&r.transferNo, &r.merchantID, &r.outNo, &r.payee, &r.amount, &r.currency, &r.remark, &r.notify,
		&r.mode, &r.payerShard, &r.payeeShard, &r.status, &r.failCode, &r.txnNo, &r.retryCount, &r.nextRetryAt,
		&r.finishedAt, &r.version, &r.createdAt)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return r
}

func transferCount(t *testing.T, env payEnv) int {
	t.Helper()
	var n int
	if err := env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM transfer_order`).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

// TRF-01 验收 4：MySQL 上 M100001 → M100003 为 LOCAL，M100001 → M100002 为 TCC；
// 行为 TRYING，next_retry_at = created_at + 10 秒，fail_code、txn_no、finished_at 为 NULL。
func TestCreateTransferModeMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(9, 0, 0, 0)
	p := openTransfers(t, env, now)
	remark := "货款结算"
	notify := "http://merchant-demo:9100/notify"
	local, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TR_LOCAL", PayeeMerchantID: "M100003", Amount: 500,
		Remark: &remark, NotifyURL: &notify,
	})
	if err != nil {
		t.Fatal(err)
	}
	tcc, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TR_TCC", PayeeMerchantID: "M100002", Amount: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if local.Mode != transferModeLocal || tcc.Mode != transferModeTCC {
		t.Fatalf("modes local=%s tcc=%s", local.Mode, tcc.Mode)
	}
	cases := []struct {
		no                     string
		mode, payee, notify    string
		payerShard, payeeShard int
		remark                 sql.NullString
	}{
		{local.TransferNo, transferModeLocal, "M100003", notify, 1, 1, sql.NullString{String: remark, Valid: true}},
		{tcc.TransferNo, transferModeTCC, "M100002", "", 1, 0, sql.NullString{}},
	}
	for _, c := range cases {
		r := loadTransferRow(t, env, c.no)
		if r.mode != c.mode || r.payerShard != c.payerShard || r.payeeShard != c.payeeShard {
			t.Fatalf("%s: mode=%s payer=%d payee=%d", c.no, r.mode, r.payerShard, r.payeeShard)
		}
		if r.status != transferStatusTrying || r.merchantID != "M100001" || r.payee != c.payee ||
			r.amount != 500 || r.currency != "CNY" || r.notify != c.notify || r.remark != c.remark {
			t.Fatalf("%s: row = %+v", c.no, r)
		}
		if r.failCode.Valid || r.txnNo.Valid || r.finishedAt.Valid || r.retryCount != 0 || r.version != 0 {
			t.Fatalf("%s: defaults = %+v", c.no, r)
		}
		if !r.createdAt.Equal(now) || !r.nextRetryAt.Equal(now.Add(10*time.Second)) {
			t.Fatalf("%s: created=%s next=%s", c.no, r.createdAt, r.nextRetryAt)
		}
		if len(r.transferNo) != 21 || r.transferNo[:2] != "TF" {
			t.Fatalf("transfer_no = %s", r.transferNo)
		}
	}
}

// TRF-01 验收 1：MySQL 上 100 个并发相同请求只生成 1 张转账单，全部返回同一 transfer_no。
func TestCreateTransferConcurrentMySQL(t *testing.T) {
	env := openPay(t)
	p := openTransfers(t, env, feeAt(9, 10, 0, 0))
	const n = 100
	got := make([]TransferOrder, n)
	errs := make([]error, n)
	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			got[i], errs[i] = p.CreateTransfer(env.ctx, &CreateTransferInput{
				MerchantID: "M100001", OutTransferNo: "TR_CONC", PayeeMerchantID: "M100002", Amount: 700,
			})
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("req %d: %v", i, errs[i])
		}
		if got[i].TransferNo == "" || got[i].TransferNo != got[0].TransferNo {
			t.Fatalf("req %d: %s != %s", i, got[i].TransferNo, got[0].TransferNo)
		}
	}
	if c := transferCount(t, env); c != 1 {
		t.Fatalf("rows = %d", c)
	}
}

// TRF-01 验收 2：MySQL 上相同 out_transfer_no、不同金额返回 10006，原行不变；参数一致返回同一 transfer_no。
func TestCreateTransferIdempotentMySQL(t *testing.T) {
	env := openPay(t)
	p := openTransfers(t, env, feeAt(9, 20, 0, 0))
	in := &CreateTransferInput{MerchantID: "M100001", OutTransferNo: "TR_SAME", PayeeMerchantID: "M100002", Amount: 500}
	first, err := p.CreateTransfer(env.ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := p.CreateTransfer(env.ctx, in)
	if err != nil || again.TransferNo != first.TransferNo {
		t.Fatalf("again = %+v err=%v", again, err)
	}
	_, err = p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TR_SAME", PayeeMerchantID: "M100002", Amount: 501,
	})
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if c := transferCount(t, env); c != 1 {
		t.Fatalf("rows = %d", c)
	}
	if r := loadTransferRow(t, env, first.TransferNo); r.amount != 500 || r.version != 0 {
		t.Fatalf("row changed: %+v", r)
	}
	// 同一 out_transfer_no 在另一个付款商户下是另一张单。
	other, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100003", OutTransferNo: "TR_SAME", PayeeMerchantID: "M100002", Amount: 501,
	})
	if err != nil || other.TransferNo == first.TransferNo {
		t.Fatalf("other merchant = %+v err=%v", other, err)
	}
}

// TRF-01 规则 3 先于规则 2：MySQL 上建单后把收款商户改为 DISABLED，相同参数重试返回已有单，
// 不同金额仍是 10006；新单号转给它是 30008。商户配置不缓存，停用立即可见。
func TestCreateTransferPayeeDisabledAfterCreateMySQL(t *testing.T) {
	env := openPay(t)
	p := openTransfers(t, env, feeAt(9, 40, 0, 0))
	in := &CreateTransferInput{MerchantID: "M100001", OutTransferNo: "TR_LATE", PayeeMerchantID: "M100003", Amount: 800}
	first, err := p.CreateTransfer(env.ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.db.ExecContext(env.ctx, `UPDATE merchant SET status = 'DISABLED' WHERE merchant_id = 'M100003'`); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	again, err := p.CreateTransfer(env.ctx, in)
	if err != nil || again.TransferNo != first.TransferNo {
		t.Fatalf("retry after payee disabled: got=%+v err=%v", again, err)
	}
	_, err = p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TR_LATE", PayeeMerchantID: "M100003", Amount: 801,
	})
	assertErrno(t, err, errno.ErrIdempotentConflict)
	_, err = p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TR_LATE_2", PayeeMerchantID: "M100003", Amount: 800,
	})
	assertErrno(t, err, errno.ErrPayeeInvalid)
	if c := transferCount(t, env); c != 1 {
		t.Fatalf("rows = %d", c)
	}
	if r := loadTransferRow(t, env, first.TransferNo); r.amount != 800 || r.status != "TRYING" || r.version != 0 {
		t.Fatalf("row changed: %+v", r)
	}
}

// TRF-01 验收 3：MySQL 上转给自己、不存在、停用的商户返回 30008，不插入。
func TestCreateTransferPayeeInvalidMySQL(t *testing.T) {
	env := openPay(t)
	p := openTransfers(t, env, feeAt(9, 30, 0, 0))
	for _, payee := range []string{"M100001", "M100099", "M100004"} {
		_, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
			MerchantID: "M100001", OutTransferNo: "TR_BAD_" + payee, PayeeMerchantID: payee, Amount: 100,
		})
		assertErrno(t, err, errno.ErrPayeeInvalid)
	}
	if c := transferCount(t, env); c != 0 {
		t.Fatalf("rows = %d", c)
	}
}
