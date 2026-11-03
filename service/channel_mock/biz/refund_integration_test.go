//go:build integration

package biz

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

// CH-04 验收 1：MySQL 上原支付未成功与超额都被拒绝且不插入。
func TestRefundRejectMySQL(t *testing.T) {
	e := openMock(t)
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := New(e.store, newIDs(t, clk), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 200})

	missing, err := svc.Refund(e.ctx, refundReq("RF_MISS", "PA_NONE", 100))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if missing.Result != ResultRejected || missing.Reason != ReasonOrigNotSuccess || countRefunds(t, e, "PA_NONE") != 0 {
		t.Fatalf("missing %+v", missing)
	}

	if _, err := svc.Pay(e.ctx, payReq("PA_PROC", 10000)); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	proc, err := svc.Refund(e.ctx, refundReq("RF_PROC", "PA_PROC", 100))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if proc.Result != ResultRejected || proc.Reason != ReasonOrigNotSuccess || countRefunds(t, e, "PA_PROC") != 0 {
		t.Fatalf("processing %+v", proc)
	}

	if _, err := svc.Pay(e.ctx, payReq("PA_CLOSED", 10000)); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := svc.Close(e.ctx, CloseRequest{ReqNo: "PA_CLOSED"}); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	closed, err := svc.Refund(e.ctx, refundReq("RF_CLOSED", "PA_CLOSED", 100))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if closed.Result != ResultRejected || closed.Reason != ReasonOrigNotSuccess || countRefunds(t, e, "PA_CLOSED") != 0 {
		t.Fatalf("closed %+v", closed)
	}

	if _, err := svc.Pay(e.ctx, payReq("PA_FAIL", 10001)); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	finishMock(t, clk, sched, e.row(t, "PA_FAIL"))
	if got := e.row(t, "PA_FAIL"); got.Status != dal.StatusFailed {
		t.Fatalf("fail pay %+v", got)
	}
	failed, err := svc.Refund(e.ctx, refundReq("RF_FAIL_ORIG", "PA_FAIL", 100))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if failed.Result != ResultRejected || failed.Reason != ReasonOrigNotSuccess || countRefunds(t, e, "PA_FAIL") != 0 {
		t.Fatalf("failed orig %+v", failed)
	}

	paySuccessMySQL(t, e, svc, clk, sched, "PA_OK", 10000)
	first, err := svc.Refund(e.ctx, refundReq("RF_6000", "PA_OK", 6000))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if first.Result != ResultOK {
		t.Fatalf("first %+v", first)
	}
	second, err := svc.Refund(e.ctx, refundReq("RF_5000", "PA_OK", 5000))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if second.Result != ResultRejected || second.Reason != ReasonRefundExceeded || countRefunds(t, e, "PA_OK") != 1 {
		t.Fatalf("exceeded %+v count=%d", second, countRefunds(t, e, "PA_OK"))
	}
	if _, ok := refundByReq(t, e, "RF_5000"); ok {
		t.Fatal("exceeded row inserted")
	}
}

// CH-04 验收 2：全额后再提交返回已有流水号。
func TestRefundIdempotentMySQL(t *testing.T) {
	e := openMock(t)
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := New(e.store, newIDs(t, clk), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 200})
	paySuccessMySQL(t, e, svc, clk, sched, "PA_IDEM", 10000)
	first, err := svc.Refund(e.ctx, refundReq("RF_FULL", "PA_IDEM", 10000))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	again, err := svc.Refund(e.ctx, refundReq("RF_FULL", "PA_IDEM", 10000))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if first.Result != ResultOK || again.Result != ResultOK || again.ChannelTxnNo == "" || again.ChannelTxnNo != first.ChannelTxnNo {
		t.Fatalf("first %+v again %+v", first, again)
	}
	if again.Reason == ReasonRefundExceeded || countRefunds(t, e, "PA_IDEM") != 1 {
		t.Fatalf("again %+v count=%d", again, countRefunds(t, e, "PA_IDEM"))
	}
}

// CH-04 验收 3：NORMAL 自动完成后 txn_type=REFUND、SUCCESS、bill_date 等于 finished_at 的东八区日期。Query(REFUND) 查到，Query(PAY, 同一个 req_no) 不存在。
func TestRefundBillDateMySQL(t *testing.T) {
	e := openMock(t)
	srv, _, _ := scenarioServer(t, nil, http.StatusOK, []byte("SUCCESS"))
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := New(e.store, newIDs(t, clk), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 200})
	svc.SetOnFinished(NewNotifier(e.store, newIDs(t, clk), clk, sched, []byte(notifyKey)).Handle)
	paySuccessMySQL(t, e, svc, clk, sched, "PA_BILL", 10000)
	res, err := svc.Refund(e.ctx, RefundRequest{
		ReqNo: "RF_BILL", OrigReqNo: "PA_BILL", Amount: 10000, NotifyURL: srv.URL,
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if res.Result != ResultOK {
		t.Fatalf("refund %+v", res)
	}
	row, ok := refundByReq(t, e, "RF_BILL")
	if !ok {
		t.Fatal("refund missing")
	}
	finishMock(t, clk, sched, row)
	got, ok := refundByReq(t, e, "RF_BILL")
	if !ok {
		t.Fatal("refund missing after finish")
	}
	wantDate := got.FinishedAt.In(shanghai).Format(billDateLayout)
	if got.TxnType != dal.TxnTypeRefund || got.Status != dal.StatusSuccess || got.BillDate == "" || got.BillDate != wantDate {
		t.Fatalf("row %+v want date %s", got, wantDate)
	}
	q, err := svc.Query(e.ctx, QueryRequest{TxnType: dal.TxnTypeRefund, ReqNo: "RF_BILL"})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !q.Found || q.Status != dal.StatusSuccess || q.ChannelTxnNo != got.ChannelTxnNo {
		t.Fatalf("query refund %+v", q)
	}
	payQ, err := svc.Query(e.ctx, QueryRequest{TxnType: dal.TxnTypePay, ReqNo: "RF_BILL"})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if payQ.Found {
		t.Fatalf("pay query found %+v", payQ)
	}
}

// CH-04：FAIL 完成后 bill_date 为空，回调 body 的 status 是 FAILED，notify_sent_count=1。DROP_NOTIFY 有 bill_date 且 HTTP 0 次。
func TestRefundFailAndDropMySQL(t *testing.T) {
	e := openMock(t)
	srv, cap, hits := scenarioServer(t, nil, http.StatusOK, []byte("SUCCESS"))
	clk := &clock.Fake{}
	clk.Set(baseTime)
	sched := &manualSched{}
	svc := New(e.store, newIDs(t, clk), clk, sched, Options{DelayMinMS: 200, DelayMaxMS: 200})
	paySuccessMySQL(t, e, svc, clk, sched, "PA_FOR_FAIL", 20000)
	paySuccessMySQL(t, e, svc, clk, sched, "PA_FOR_DROP", 20000)
	svc.SetOnFinished(NewNotifier(e.store, newIDs(t, clk), clk, sched, []byte(notifyKey)).Handle)

	failRes, err := svc.Refund(e.ctx, RefundRequest{
		ReqNo: "RF_FAIL", OrigReqNo: "PA_FOR_FAIL", Amount: 10001, NotifyURL: srv.URL,
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if failRes.Result != ResultOK {
		t.Fatalf("fail refund %+v", failRes)
	}
	failRow, ok := refundByReq(t, e, "RF_FAIL")
	if !ok || failRow.Scenario != "FAIL" {
		t.Fatalf("fail row %+v", failRow)
	}
	finishMock(t, clk, sched, failRow)
	if hits.Load() != 0 {
		t.Fatal("http inside finish")
	}
	failed, ok := refundByReq(t, e, "RF_FAIL")
	if !ok || failed.Status != dal.StatusFailed || failed.BillDate != "" || failed.NotifySentCount != 0 {
		t.Fatalf("after finish %+v", failed)
	}
	if ran := sched.RunDue(clk.Now()); ran != 1 {
		t.Fatalf("notify ran %d", ran)
	}
	sent, _ := refundByReq(t, e, "RF_FAIL")
	body := ""
	if list := cap.list(); len(list) > 0 {
		body = string(list[0].body)
	}
	if hits.Load() != 1 || sent.NotifySentCount != 1 || !strings.Contains(body, `"status":"FAILED"`) {
		t.Fatalf("hits=%d count=%d body=%s", hits.Load(), sent.NotifySentCount, body)
	}

	before := hits.Load()
	dropRes, err := svc.Refund(e.ctx, RefundRequest{
		ReqNo: "RF_DROP", OrigReqNo: "PA_FOR_DROP", Amount: 10002, NotifyURL: srv.URL,
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if dropRes.Result != ResultOK {
		t.Fatalf("drop refund %+v", dropRes)
	}
	dropRow, ok := refundByReq(t, e, "RF_DROP")
	if !ok || dropRow.Scenario != "DROP_NOTIFY" {
		t.Fatalf("drop row %+v", dropRow)
	}
	finishMock(t, clk, sched, dropRow)
	dropped, ok := refundByReq(t, e, "RF_DROP")
	wantDate := dropped.FinishedAt.In(shanghai).Format(billDateLayout)
	if !ok || dropped.Status != dal.StatusSuccess || dropped.BillDate == "" || dropped.BillDate != wantDate || dropped.NotifySentCount != 0 {
		t.Fatalf("drop %+v want %s", dropped, wantDate)
	}
	clk.Advance(time.Minute)
	if ran := sched.RunDue(clk.Now()); ran != 0 || hits.Load() != before {
		t.Fatalf("drop http ran=%d hits=%d before=%d", ran, hits.Load(), before)
	}
}

func countRefunds(t *testing.T, e dbEnv, orig string) int {
	t.Helper()
	var n int
	err := e.raw.QueryRowContext(e.ctx, `SELECT COUNT(*) FROM mock_txn WHERE txn_type = ? AND orig_req_no = ?`, dal.TxnTypeRefund, orig).Scan(&n)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return n
}

func refundByReq(t *testing.T, e dbEnv, reqNo string) (dal.Txn, bool) {
	t.Helper()
	row, ok, err := e.store.Get(e.ctx, dal.TxnTypeRefund, reqNo)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return row, ok
}

func paySuccessMySQL(t *testing.T, e dbEnv, svc *Service, clk *clock.Fake, sched *manualSched, reqNo string, amount int64) dal.Txn {
	t.Helper()
	res, err := svc.Pay(e.ctx, payReq(reqNo, amountOf(amount)))
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if res.Result != ResultOK {
		t.Fatalf("pay %s %+v", reqNo, res)
	}
	row := e.row(t, reqNo)
	finishMock(t, clk, sched, row)
	got := e.row(t, reqNo)
	if got.Status != dal.StatusSuccess {
		t.Fatalf("pay %s %+v", reqNo, got)
	}
	return got
}

func finishMock(t *testing.T, clk *clock.Fake, sched *manualSched, row dal.Txn) {
	t.Helper()
	due := row.CreatedAt.Add(Delay(row.ReqNo, 200, 200))
	clk.Set(due)
	if n := sched.RunDue(clk.Now()); n < 1 {
		t.Fatalf("finish %s ran %d", row.ReqNo, n)
	}
}
