//go:build integration

package dal

import (
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/money"
)

// CH-08：只查 bill_date = date 的 SUCCESS 交易（PAY 与 REFUND），按 finished_at、channel_txn_no 排序；
// 不包含 PROCESSING、FAILED、CLOSED、其他日期及 bill_date IS NULL（如 BILL_MISSING）。
func TestListBillTxns(t *testing.T) {
	ctx, raw, db := openDB(t)

	insertFinished := func(channelTxnNo, txnType, reqNo string, amount money.Amount, status, scenario string, finishedAt time.Time, billDate string) {
		t.Helper()
		id, err := db.Insert(ctx, Txn{
			ChannelTxnNo: channelTxnNo,
			TxnType:      txnType,
			ReqNo:        reqNo,
			Amount:       amount,
			Status:       StatusProcessing,
			Scenario:     scenario,
			NotifyURL:    "http://gateway:8080/callback/mockpay",
			CreatedAt:    finishedAt.Add(-time.Second),
		})
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if status == StatusProcessing {
			return
		}
		if status == StatusClosed {
			ok, err := db.Close(ctx, id)
			if err != nil || !ok {
				t.Fatalf("close %s: ok=%v err=%v", reqNo, ok, err)
			}
			return
		}
		ok, err := db.Finish(ctx, Finish{
			ID:         id,
			Status:     status,
			FinishedAt: finishedAt,
			BillDate:   billDate,
		})
		if err != nil || !ok {
			t.Fatalf("finish %s: ok=%v err=%v", reqNo, ok, err)
		}
	}

	t1 := time.Date(2026, 10, 12, 10, 0, 1, 234000000, shanghaiTZ)
	t2 := time.Date(2026, 10, 12, 11, 20, 0, 500000000, shanghaiTZ)
	tOtherDay := time.Date(2026, 10, 13, 9, 0, 0, 0, shanghaiTZ)

	// 插入顺序故意打乱，验证排序：finished_at ASC, channel_txn_no ASC
	insertFinished("MC0003", TxnTypeRefund, "RF003", 3000, StatusSuccess, "NORMAL", t2, "2026-10-12")
	insertFinished("MC0002", TxnTypePay, "PA002", 10007, StatusSuccess, "BILL_AMOUNT_DIFF", t1, "2026-10-12")
	insertFinished("MC0001", TxnTypePay, "PA001", 10000, StatusSuccess, "NORMAL", t1, "2026-10-12")

	// 不应进入 2026-10-12 账单的记录：
	insertFinished("MC0004", TxnTypePay, "PA_MISS", 10006, StatusSuccess, "BILL_MISSING", t1, "")
	insertFinished("MC0005", TxnTypePay, "PA_FAIL", 10001, StatusFailed, "FAIL", t1, "")
	insertFinished("MC0006", TxnTypePay, "PA_PROC", 10000, StatusProcessing, "NORMAL", t1, "")
	insertFinished("MC0007", TxnTypePay, "PA_CLOSED", 10000, StatusClosed, "NORMAL", t1, "")
	insertFinished("MC0008", TxnTypePay, "PA_NEXT_DAY", 10000, StatusSuccess, "NORMAL", tOtherDay, "2026-10-13")

	// 即使有人为脏数据 bill_date 非空但 status != SUCCESS，也不得进入账单。
	if _, err := raw.ExecContext(ctx, `UPDATE mock_txn SET bill_date = '2026-10-12' WHERE req_no = 'PA_FAIL'`); err != nil {
		t.Fatal(err)
	}

	got, err := db.ListBillTxns(ctx, "2026-10-12")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if len(got) != 3 {
		t.Fatalf("len(got) = %d, want 3: %+v", len(got), got)
	}
	wantOrder := []string{"MC0001", "MC0002", "MC0003"}
	for i, w := range wantOrder {
		if got[i].ChannelTxnNo != w {
			t.Fatalf("got[%d].ChannelTxnNo = %s, want %s", i, got[i].ChannelTxnNo, w)
		}
	}

	empty, err := db.ListBillTxns(ctx, "2026-10-11")
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if len(empty) != 0 {
		t.Fatalf("empty day len = %d, want 0", len(empty))
	}
}
