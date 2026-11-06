//go:build integration

package biz

import (
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
)

// transferVersion 读 status、version、updated_at，用来断言查询不改行。
func transferVersion(t *testing.T, env payEnv, transferNo string) (string, uint64, time.Time) {
	t.Helper()
	var (
		status  string
		version uint64
		updated time.Time
	)
	err := env.db.QueryRowContext(env.ctx,
		`SELECT status, version, updated_at FROM transfer_order WHERE transfer_no = ?`, transferNo).
		Scan(&status, &version, &updated)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return status, version, updated
}

// TRF-05 验收 1：收款商户查询返回 30001。
// M100001 → M100002 的转账，收款商户 M100002 用 transfer_no、out_transfer_no 查询都是 30001，
// 其他商户 M100003、大小写不同的商户号与单号同样是 30001，与不存在的单号是同一个错误，不带转账单；
// 转账单不变，没有写 outbox。
func TestTRF05PayeeCannotReadTransfer(t *testing.T) {
	env := openPay(t)
	p := openTransfers(t, env, feeAt(11, 0, 0, 0))
	created, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF05-A", PayeeMerchantID: "M100002", Amount: 500,
	})
	if err != nil || created.Status != transferStatusTrying {
		t.Fatalf("created = %+v err=%v", created, err)
	}
	stBefore, verBefore, updBefore := transferVersion(t, env, created.TransferNo)
	outboxBefore := countPayTable(t, env, "outbox")

	_, errMissing := p.QueryTransfer(env.ctx, &TransferQueryInput{MerchantID: "M100002", TransferNo: "TF9999999999999999999"})
	assertErrno(t, errMissing, errno.ErrOrderNotFound)
	for _, in := range []*TransferQueryInput{
		{MerchantID: "M100002", TransferNo: created.TransferNo},
		{MerchantID: "M100002", OutTransferNo: "TRF05-A"},
		{MerchantID: "M100003", TransferNo: created.TransferNo},
		{MerchantID: "M100003", OutTransferNo: "TRF05-A"},
		{MerchantID: "", TransferNo: created.TransferNo},
		// 测试库排序规则不区分大小写，SQL 能匹配到付款商户的行；服务端逐字节比较后仍是 30001。
		{MerchantID: "m100001", TransferNo: created.TransferNo},
		{MerchantID: "m100001", OutTransferNo: "TRF05-A"},
		{MerchantID: "M100001", OutTransferNo: "trf05-a"},
		{MerchantID: "M100001", OutTransferNo: "NO-SUCH"},
	} {
		got, qerr := p.QueryTransfer(env.ctx, in)
		assertErrno(t, qerr, errno.ErrOrderNotFound)
		if got.TransferNo != "" || got.Amount != 0 || qerr.Error() != errMissing.Error() {
			t.Fatalf("in=%+v got=%+v err=%v", in, got, qerr)
		}
	}

	stAfter, verAfter, updAfter := transferVersion(t, env, created.TransferNo)
	if stAfter != stBefore || verAfter != verBefore || !updAfter.Equal(updBefore) || stAfter != transferStatusTrying || verAfter != 0 {
		t.Fatalf("row changed: before=%s/%d/%s after=%s/%d/%s", stBefore, verBefore, updBefore, stAfter, verAfter, updAfter)
	}
	if countPayTable(t, env, "outbox") != outboxBefore || transferCount(t, env) != 1 {
		t.Fatal("query wrote outbox or transfer_order")
	}
}

// TRF-05：付款商户按两种单号都查到同一行；SUCCESS 带 finished_at，FAILED 带 fail_code，查询不改行。
func TestTRF05PayerReadsTransferMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(11, 10, 0, 0)
	p := openTransfers(t, env, now)
	remark := "货款结算"
	created, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF05-OWN", PayeeMerchantID: "M100003", Amount: 800, Remark: &remark,
	})
	if err != nil {
		t.Fatal(err)
	}
	byNo, err := p.QueryTransfer(env.ctx, &TransferQueryInput{MerchantID: "M100001", TransferNo: created.TransferNo})
	if err != nil || byNo.TransferNo != created.TransferNo || byNo.OutTransferNo != "TRF05-OWN" ||
		byNo.PayeeMerchantID != "M100003" || byNo.Amount != 800 || byNo.Mode != transferModeLocal ||
		byNo.Status != transferStatusTrying || byNo.Remark != remark || byNo.Version != 0 ||
		!byNo.CreatedAt.Equal(created.CreatedAt) || !byNo.NextRetryAt.Equal(created.NextRetryAt) {
		t.Fatalf("by transfer_no = %+v err=%v", byNo, err)
	}
	byOut, err := p.QueryTransfer(env.ctx, &TransferQueryInput{MerchantID: "M100001", OutTransferNo: "TRF05-OWN"})
	if err != nil || byOut != byNo {
		t.Fatalf("by out_transfer_no = %+v err=%v, want %+v", byOut, err, byNo)
	}

	finished := now.Add(time.Second)
	if _, err = env.db.ExecContext(env.ctx,
		`UPDATE transfer_order SET status = 'SUCCESS', txn_no = 'TX_TRF05', finished_at = ?, version = 1 WHERE transfer_no = ?`,
		finished, created.TransferNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err := p.QueryTransfer(env.ctx, &TransferQueryInput{MerchantID: "M100001", TransferNo: created.TransferNo})
	if err != nil || got.Status != transferStatusSuccess || !got.FinishedAt.Equal(finished) || got.TxnNo != "TX_TRF05" ||
		got.Version != 1 || got.FailCode != "" {
		t.Fatalf("success = %+v err=%v", got, err)
	}
	if _, err = env.db.ExecContext(env.ctx,
		`UPDATE transfer_order SET status = 'FAILED', txn_no = NULL, fail_code = 'INSUFFICIENT_BALANCE', version = 2 WHERE transfer_no = ?`,
		created.TransferNo); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	got, err = p.QueryTransfer(env.ctx, &TransferQueryInput{MerchantID: "M100001", OutTransferNo: "TRF05-OWN"})
	if err != nil || got.Status != transferStatusFailed || got.FailCode != transferFailInsufficient ||
		!got.FinishedAt.Equal(finished) || got.TxnNo != "" || got.Version != 2 {
		t.Fatalf("failed = %+v err=%v", got, err)
	}
	if st, ver, _ := transferVersion(t, env, created.TransferNo); st != transferStatusFailed || ver != 2 {
		t.Fatalf("stored = %s/%d", st, ver)
	}
	if countPayTable(t, env, "outbox") != 0 {
		t.Fatal("read path wrote outbox")
	}
}

// TRF-05：sqlTransferStore.transferByNo 在存储层逐字节比较 transfer_no。
// 测试库排序规则不区分大小写，先确认 SQL 本身能用小写单号匹配到行，再确认存储层把它当作没有行；
// 付款商户用小写单号经 QueryTransfer 查询也是 30001。
func TestTRF05TransferByNoExactBytesMySQL(t *testing.T) {
	env := openPay(t)
	p := openTransfers(t, env, feeAt(11, 20, 0, 0))
	created, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF05-CASE", PayeeMerchantID: "M100002", Amount: 300,
	})
	if err != nil {
		t.Fatal(err)
	}
	lower := "tf" + created.TransferNo[2:]
	var n int
	if err := env.db.QueryRowContext(env.ctx, `SELECT COUNT(*) FROM transfer_order WHERE transfer_no = ?`, lower).Scan(&n); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if n != 1 {
		t.Fatalf("collation precondition: lowercase transfer_no matched %d rows, want 1", n)
	}
	store := sqlTransferStore{db: env.db}
	if row, ok, err := store.transferByNo(env.ctx, lower); err != nil || ok || row.TransferNo != "" {
		t.Fatalf("lowercase transferByNo = %+v ok=%v err=%v", row, ok, err)
	}
	if row, ok, err := store.transferByNo(env.ctx, created.TransferNo); err != nil || !ok || row.TransferNo != created.TransferNo {
		t.Fatalf("exact transferByNo = %+v ok=%v err=%v", row, ok, err)
	}
	_, err = p.QueryTransfer(env.ctx, &TransferQueryInput{MerchantID: "M100001", TransferNo: lower})
	assertErrno(t, err, errno.ErrOrderNotFound)
}
