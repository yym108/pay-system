//go:build integration

package dal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/dbx"
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

var shanghaiTZ = time.FixedZone("Asia/Shanghai", 8*3600)

func openDB(t *testing.T) (context.Context, *sqlx.DB, *DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	schema, err := my.OpenSchema(ctx, "channel_mock")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer closeCancel()
		if err := schema.Close(closeCtx); err != nil {
			t.Errorf("close schema: %s", testkit.Redact(err))
		}
	})
	return ctx, schema.DB, New(schema.DB)
}

func sample(reqNo string) Txn {
	return Txn{
		ChannelTxnNo: "MC" + reqNo,
		TxnType:      TxnTypePay,
		ReqNo:        reqNo,
		Amount:       10006,
		Status:       StatusProcessing,
		Scenario:     "BILL_MISSING",
		NotifyURL:    "http://gateway:8080/callback/mockpay",
		CreatedAt:    time.Date(2020, 1, 2, 3, 4, 5, 678000000, shanghaiTZ),
	}
}

// created_at 由调用方写入（注入时钟），不是列默认值；NULL 列读回零值。
func TestInsertGetRoundTrip(t *testing.T) {
	ctx, _, db := openDB(t)
	in := sample("PA001")
	id, err := db.Insert(ctx, in)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if id == 0 {
		t.Fatal("id = 0")
	}
	got, ok, err := db.Get(ctx, TxnTypePay, "PA001")
	if err != nil || !ok {
		t.Fatalf("get = %v, %v", ok, testkit.Redact(err))
	}
	in.ID = id
	if got.ID != in.ID || got.ChannelTxnNo != in.ChannelTxnNo || got.TxnType != in.TxnType ||
		got.ReqNo != in.ReqNo || got.Amount != in.Amount || got.Status != in.Status ||
		got.Scenario != in.Scenario || got.NotifyURL != in.NotifyURL || got.NotifySentCount != 0 ||
		!got.CreatedAt.Equal(in.CreatedAt) || !got.FinishedAt.IsZero() || got.BillDate != "" {
		t.Fatalf("got %+v\nwant %+v", got, in)
	}
	if got.CreatedAt.Location().String() != "Asia/Shanghai" {
		t.Fatalf("location = %v", got.CreatedAt.Location())
	}
	if _, ok, err := db.Get(ctx, TxnTypePay, "PA404"); err != nil || ok {
		t.Fatalf("missing row: ok = %v, err = %v", ok, err)
	}
	if _, ok, err := db.Get(ctx, "REFUND", "PA001"); err != nil || ok {
		t.Fatalf("other txn_type: ok = %v, err = %v", ok, err)
	}
}

// 唯一键冲突原样返回，调用方用 dbx.IsDuplicate 判断。
func TestInsertDuplicate(t *testing.T) {
	ctx, _, db := openDB(t)
	if _, err := db.Insert(ctx, sample("PA001")); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	sameReq := sample("PA001")
	sameReq.ChannelTxnNo = "MC_OTHER"
	if _, err := db.Insert(ctx, sameReq); !dbx.IsDuplicate(err) {
		t.Fatalf("same req_no: err = %v", err)
	}
	sameNo := sample("PA002")
	sameNo.ChannelTxnNo = "MCPA001"
	if _, err := db.Insert(ctx, sameNo); !dbx.IsDuplicate(err) {
		t.Fatalf("same channel_txn_no: err = %v", err)
	}
}

// Finish 是 CAS：只在 PROCESSING 时生效；BillDate 为空写 NULL。
func TestFinishCAS(t *testing.T) {
	ctx, raw, db := openDB(t)
	id, err := db.Insert(ctx, sample("PA001"))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	finished := time.Date(2026, 10, 23, 23, 59, 59, 999000000, shanghaiTZ)
	ok, err := db.Finish(ctx, Finish{ID: id, Status: StatusSuccess, FinishedAt: finished, BillDate: "2026-10-23"})
	if err != nil || !ok {
		t.Fatalf("finish = %v, %v", ok, err)
	}
	got, _, err := db.Get(ctx, TxnTypePay, "PA001")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusSuccess || !got.FinishedAt.Equal(finished) || got.BillDate != "2026-10-23" {
		t.Fatalf("after finish: %+v", got)
	}
	ok, err = db.Finish(ctx, Finish{ID: id, Status: StatusFailed, FinishedAt: finished.Add(time.Second)})
	if err != nil || ok {
		t.Fatalf("second finish = %v, %v", ok, err)
	}
	got, _, _ = db.Get(ctx, TxnTypePay, "PA001")
	if got.Status != StatusSuccess || !got.FinishedAt.Equal(finished) {
		t.Fatalf("second finish changed row: %+v", got)
	}

	id2, err := db.Insert(ctx, sample("PA002"))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if ok, err := db.Finish(ctx, Finish{ID: id2, Status: StatusFailed, FinishedAt: finished}); err != nil || !ok {
		t.Fatalf("finish failed row = %v, %v", ok, err)
	}
	var billNull bool
	if err := raw.QueryRowContext(ctx, `SELECT bill_date IS NULL FROM mock_txn WHERE id = ?`, id2).Scan(&billNull); err != nil {
		t.Fatal(err)
	}
	if !billNull {
		t.Fatal("empty BillDate was not written as NULL")
	}

	id3, err := db.Insert(ctx, sample("PA003"))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := raw.ExecContext(ctx, `UPDATE mock_txn SET status = 'CLOSED' WHERE id = ?`, id3); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.Finish(ctx, Finish{ID: id3, Status: StatusSuccess, FinishedAt: finished, BillDate: "2026-10-23"}); err != nil || ok {
		t.Fatalf("finish closed row = %v, %v", ok, err)
	}
	var finishedAt sql.NullTime
	if err := raw.QueryRowContext(ctx, `SELECT finished_at FROM mock_txn WHERE id = ?`, id3).Scan(&finishedAt); err != nil {
		t.Fatal(err)
	}
	if finishedAt.Valid {
		t.Fatal("closed row got finished_at")
	}
}

// ListProcessing 按 id 升序分页，只返回该类型的 PROCESSING。
func TestListProcessingKeyset(t *testing.T) {
	ctx, raw, db := openDB(t)
	var ids []uint64
	for i := 1; i <= 6; i++ {
		id, err := db.Insert(ctx, sample(fmt.Sprintf("PA%03d", i)))
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
		ids = append(ids, id)
	}
	if _, err := db.Finish(ctx, Finish{ID: ids[1], Status: StatusSuccess, FinishedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, shanghaiTZ)}); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `UPDATE mock_txn SET txn_type = 'REFUND' WHERE id = ?`, ids[3]); err != nil {
		t.Fatal(err)
	}
	page1, err := db.ListProcessing(ctx, TxnTypePay, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || page1[0].ID != ids[0] || page1[1].ID != ids[2] {
		t.Fatalf("page1 = %+v", page1)
	}
	page2, err := db.ListProcessing(ctx, TxnTypePay, page1[1].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].ID != ids[4] || page2[1].ID != ids[5] {
		t.Fatalf("page2 = %+v", page2)
	}
	if page2[0].Scenario != "BILL_MISSING" || !page2[0].CreatedAt.Equal(sample("x").CreatedAt) {
		t.Fatalf("listed row fields = %+v", page2[0])
	}
	page3, err := db.ListProcessing(ctx, TxnTypePay, page2[1].ID, 2)
	if err != nil || len(page3) != 0 {
		t.Fatalf("page3 = %+v, %v", page3, err)
	}
}

// GetByID 按主键读一行，完成后的 finished_at、bill_date 原样读回；不存在时 ok = false、err = nil。
func TestGetByID(t *testing.T) {
	ctx, _, db := openDB(t)
	id, err := db.Insert(ctx, sample("PA001"))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	finished := time.Date(2026, 10, 23, 23, 59, 59, 999000000, shanghaiTZ)
	if ok, err := db.Finish(ctx, Finish{ID: id, Status: StatusSuccess, FinishedAt: finished, BillDate: "2026-10-23"}); err != nil || !ok {
		t.Fatalf("finish = %v, %v", ok, err)
	}
	got, ok, err := db.GetByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get by id = %v, %v", ok, testkit.Redact(err))
	}
	want, _, err := db.Get(ctx, TxnTypePay, "PA001")
	if err != nil {
		t.Fatal(err)
	}
	if got != want || got.ID != id || got.Status != StatusSuccess || !got.FinishedAt.Equal(finished) ||
		got.BillDate != "2026-10-23" {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if _, ok, err := db.GetByID(ctx, id+1000); err != nil || ok {
		t.Fatalf("missing id: ok = %v, err = %v", ok, err)
	}
}
