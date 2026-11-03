package biz

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

// CH-04 验收 1：原支付不存在、PROCESSING、FAILED、CLOSED 都是 ORIG_NOT_SUCCESS，且没有退款行。
func TestRefundRejectsOrigNotSuccess(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		prep func(t *testing.T, e *env) string
	}{
		{name: "missing", prep: func(t *testing.T, e *env) string { return "pay-missing" }},
		{name: "processing", prep: func(t *testing.T, e *env) string {
			return mustPayProcessing(t, e, "pay-proc", 10000)
		}},
		{name: "failed", prep: func(t *testing.T, e *env) string {
			return mustPayFinished(t, e, "pay-fail", 10001, dal.StatusFailed)
		}},
		{name: "closed", prep: func(t *testing.T, e *env) string {
			reqNo := mustPayProcessing(t, e, "pay-closed", 10000)
			e.store.setStatus(reqNo, dal.StatusClosed)
			return reqNo
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			orig := tc.prep(t, e)
			reqNo := "refund-" + tc.name
			got, err := e.svc.Refund(ctx, refundReq(reqNo, orig, 100))
			if err != nil {
				t.Fatal(err)
			}
			if got.Result != ResultRejected || got.Reason != ReasonOrigNotSuccess {
				t.Fatalf("got %+v", got)
			}
			if _, ok := e.store.rowOf(dal.TxnTypeRefund, reqNo); ok {
				t.Fatal("refund row inserted")
			}
		})
	}
}

// CH-04 验收 1：10000 分成功后先退 6000 再退 5000，第二笔 REFUND_EXCEEDED，库里只有第一笔。再另起一笔原支付，6000 加 4000 都 OK。
func TestRefundQuota(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	orig := mustPaySuccess(t, e, "pay-quota", 10000)

	first, err := e.svc.Refund(ctx, refundReq("refund-6000", orig, 6000))
	if err != nil {
		t.Fatal(err)
	}
	if first.Result != ResultOK || first.Status != dal.StatusProcessing {
		t.Fatalf("first %+v", first)
	}
	second, err := e.svc.Refund(ctx, refundReq("refund-5000", orig, 5000))
	if err != nil {
		t.Fatal(err)
	}
	if second.Result != ResultRejected || second.Reason != ReasonRefundExceeded {
		t.Fatalf("second %+v", second)
	}
	if _, ok := e.store.rowOf(dal.TxnTypeRefund, "refund-5000"); ok {
		t.Fatal("exceeded refund inserted")
	}
	if _, ok := e.store.rowOf(dal.TxnTypeRefund, "refund-6000"); !ok {
		t.Fatal("first refund missing")
	}

	orig2 := mustPaySuccess(t, e, "pay-quota-2", 10000)
	for _, c := range []struct {
		req string
		amt money.Amount
	}{{"refund-b-6000", 6000}, {"refund-b-4000", 4000}} {
		got, err := e.svc.Refund(ctx, refundReq(c.req, orig2, c.amt))
		if err != nil {
			t.Fatal(err)
		}
		if got.Result != ResultOK {
			t.Fatalf("%s %+v", c.req, got)
		}
	}
}

// CH-04 验收 2：全额退款后同一 req_no 再提交，返回同一个 channel_txn_no，不是 REFUND_EXCEEDED。
func TestRefundIdempotentSkipsQuota(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	orig := mustPaySuccess(t, e, "pay-idem", 10000)
	first, err := e.svc.Refund(ctx, refundReq("refund-full", orig, 10000))
	if err != nil {
		t.Fatal(err)
	}
	if first.Result != ResultOK {
		t.Fatalf("first %+v", first)
	}
	again, err := e.svc.Refund(ctx, refundReq("refund-full", orig, 10000))
	if err != nil {
		t.Fatal(err)
	}
	if again.Result != ResultOK || again.ChannelTxnNo != first.ChannelTxnNo {
		t.Fatalf("again %+v want %s", again, first.ChannelTxnNo)
	}
	if again.Reason == ReasonRefundExceeded {
		t.Fatalf("quota checked on replay: %+v", again)
	}
}

// CH-04：已有退款金额不一致时 PARAM_CONFLICT。
func TestRefundAmountConflict(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	orig := mustPaySuccess(t, e, "pay-conflict", 10000)
	if _, err := e.svc.Refund(ctx, refundReq("refund-c", orig, 1000)); err != nil {
		t.Fatal(err)
	}
	got, err := e.svc.Refund(ctx, refundReq("refund-c", orig, 2000))
	if err != nil {
		t.Fatal(err)
	}
	if got.Result != ResultRejected || got.Reason != ReasonParamConflict {
		t.Fatalf("got %+v", got)
	}
}

// CH-04：末两位 01 是 FAIL，02 是 DROP_NOTIFY，04、05、08 都是 NORMAL，08 仍有 1 个调度任务。
func TestRefundScenarioCollapses(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		amt      money.Amount
		scenario string
	}{
		{amt: 10001, scenario: "FAIL"},
		{amt: 10002, scenario: "DROP_NOTIFY"},
		{amt: 10004, scenario: "NORMAL"},
		{amt: 10005, scenario: "NORMAL"},
		{amt: 10008, scenario: "NORMAL"},
	}
	for _, tc := range cases {
		e := newEnv(t)
		orig := mustPaySuccess(t, e, "pay-sc", 20000)
		before := e.sched.len()
		got, err := e.svc.Refund(ctx, refundReq("refund-sc", orig, tc.amt))
		if err != nil {
			t.Fatal(err)
		}
		if got.Result != ResultOK {
			t.Fatalf("amount %d: %+v", tc.amt, got)
		}
		row, ok := e.store.rowOf(dal.TxnTypeRefund, "refund-sc")
		if !ok || row.Scenario != tc.scenario {
			t.Fatalf("amount %d scenario=%s ok=%v", tc.amt, row.Scenario, ok)
		}
		if got := e.sched.len() - before; got != 1 {
			t.Fatalf("amount %d tasks %d", tc.amt, got)
		}
	}
}

// CH-04：两个 goroutine 用不同 req_no 同时全额退，恰好一笔 OK，另一笔 REFUND_EXCEEDED。
func TestRefundConcurrentFullAmount(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	orig := mustPaySuccess(t, e, "pay-race", 10000)
	var wg sync.WaitGroup
	type out struct {
		res PayResult
		err error
	}
	got := make([]out, 2)
	reqs := []string{"refund-race-a", "refund-race-b"}
	wg.Add(2)
	for i, reqNo := range reqs {
		go func(i int, reqNo string) {
			defer wg.Done()
			got[i].res, got[i].err = e.svc.Refund(ctx, refundReq(reqNo, orig, 10000))
		}(i, reqNo)
	}
	wg.Wait()
	okN, exceeded := 0, 0
	for _, g := range got {
		if g.err != nil {
			t.Fatal(g.err)
		}
		switch {
		case g.res.Result == ResultOK:
			okN++
		case g.res.Result == ResultRejected && g.res.Reason == ReasonRefundExceeded:
			exceeded++
		default:
			t.Fatalf("unexpected %+v", g.res)
		}
	}
	if okN != 1 || exceeded != 1 {
		t.Fatalf("ok=%d exceeded=%d", okN, exceeded)
	}
}

func refundReq(reqNo, orig string, amount money.Amount) RefundRequest {
	return RefundRequest{ReqNo: reqNo, OrigReqNo: orig, Amount: amount, NotifyURL: "http://keep"}
}

func mustPayProcessing(t *testing.T, e *env, reqNo string, amount money.Amount) string {
	t.Helper()
	got, err := e.svc.Pay(context.Background(), payReq(reqNo, amount))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != dal.StatusProcessing {
		t.Fatalf("status %s", got.Status)
	}
	return reqNo
}

func mustPaySuccess(t *testing.T, e *env, reqNo string, amount money.Amount) string {
	t.Helper()
	return mustPayFinished(t, e, reqNo, amount, dal.StatusSuccess)
}

func mustPayFinished(t *testing.T, e *env, reqNo string, amount money.Amount, want string) string {
	t.Helper()
	if _, err := e.svc.Pay(context.Background(), payReq(reqNo, amount)); err != nil {
		t.Fatal(err)
	}
	e.clk.Advance(2 * time.Second)
	e.sched.RunDue(e.clk.Now())
	row, ok := e.store.row(reqNo)
	if !ok || row.Status != want {
		t.Fatalf("pay %s status %q want %s", reqNo, row.Status, want)
	}
	return reqNo
}
