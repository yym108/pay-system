//go:build integration

package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/channel_mock/biz"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

// CH-04 验收：签名正确的退款 POST 返回 HTTP 200，result 是 OK。
func TestRefundHTTPMySQL(t *testing.T) {
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
	store := dal.New(schema.DB)
	clk := testClock()
	sched := &dueSched{}
	ids, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	svc := biz.New(store, ids, clk, sched, biz.Options{DelayMinMS: 60000, DelayMaxMS: 60000})
	srv := newTestServerWithClock(t, svc, testKey, clk)

	const payNo = "PA_RF_HTTP"
	payRes, err := svc.Pay(ctx, biz.PayRequest{
		ReqNo: payNo, Amount: 10000, Subject: "demo", NotifyURL: "http://keep",
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if payRes.Result != biz.ResultOK {
		t.Fatalf("pay %+v", payRes)
	}
	payRow, ok, err := store.Get(ctx, dal.TxnTypePay, payNo)
	if err != nil || !ok {
		t.Fatalf("pay row ok=%v err=%v", ok, err)
	}
	clk.Set(payRow.CreatedAt.Add(60 * time.Second))
	if n := sched.RunDue(clk.Now()); n != 1 {
		t.Fatalf("pay finish ran %d", n)
	}
	paid, ok, err := store.Get(ctx, dal.TxnTypePay, payNo)
	if err != nil || !ok || paid.Status != dal.StatusSuccess {
		t.Fatalf("paid ok=%v err=%v row=%+v", ok, err, paid)
	}

	body := []byte(`{"req_no":"RF_HTTP","orig_req_no":"PA_RF_HTTP","amount":10000,"notify_url":"http://keep"}`)
	w := postSigned(t, srv, RefundPath, testKey, sign.FormatTimestamp(clk.Now()), body)
	resp := w.Result()
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("status = %d body = %s", resp.StatusCode(), resp.Body())
	}
	var m map[string]any
	if err := json.Unmarshal(resp.Body(), &m); err != nil {
		t.Fatalf("body = %s: %v", resp.Body(), err)
	}
	if m["result"] != biz.ResultOK {
		t.Fatalf("body = %s", resp.Body())
	}
	got, ok, err := store.Get(ctx, dal.TxnTypeRefund, "RF_HTTP")
	if err != nil || !ok {
		t.Fatalf("refund ok=%v err=%v", ok, err)
	}
	if got.TxnType != dal.TxnTypeRefund || got.Status != dal.StatusProcessing {
		t.Fatalf("refund row %+v", got)
	}
}
