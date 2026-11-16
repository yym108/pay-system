//go:build integration

package biz

import (
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
)

// PAY-09 验收 1、2 与渠道流水立即同步集成测试：真实 MySQL + 真实 channel-mock。
func TestSyncChannelTxnIntegration(t *testing.T) {
	const merchantID = "M100001"
	e := newQueryEnv(t, merchantID, false)

	// 验收 1：本地 PAYING、渠道 SUCCESS，调用后订单 SUCCESS，changed = true。
	t.Run("local paying channel success", func(t *testing.T) {
		order := e.pay(t, merchantID, "SYNC_PAY_OK_01", amountDropNotify)
		e.clk.Advance(mockFinishDelay)
		e.mock.sched.RunDue()

		got, err := e.p.SyncChannelTxn(e.ctx, order.PayNo)
		if err != nil {
			t.Fatalf("SyncChannelTxn: %v", err)
		}
		if got.LocalStatus != payStatusSuccess || got.ChannelStatus != payStatusSuccess || !got.Changed {
			t.Fatalf("got = %+v, want local=SUCCESS channel=SUCCESS changed=true", got)
		}
		stored := loadPayment(t, e.payEnv, merchantID, "SYNC_PAY_OK_01")
		if stored.status != payStatusSuccess {
			t.Fatalf("order status = %s, want SUCCESS", stored.status)
		}
		txn := loadQueryTxn(t, e.payEnv, order.PayNo)
		if txn.status != payStatusSuccess || txn.nextQuery.Valid {
			t.Fatalf("channel_txn = %+v", txn)
		}
	})

	// 验收 2：本地 CLOSED、渠道 SUCCESS，调用后订单仍为 CLOSED，返回双方状态，changed = false。
	t.Run("local closed channel success", func(t *testing.T) {
		order := e.pay(t, merchantID, "SYNC_PAY_CLOSED_01", amountDropNotify)
		e.clk.Advance(mockFinishDelay)
		e.mock.sched.RunDue()

		// 模拟本地已进入 CLOSED（例如关单与渠道完成竞态后本地先落 CLOSED）。
		if _, err := e.p.ApplyPayResult(e.ctx, PayResult{
			BizNo:  order.PayNo,
			Result: payResultClosed,
			Source: paySourceClose,
		}); err != nil {
			t.Fatalf("ApplyPayResult CLOSED: %v", err)
		}

		got, err := e.p.SyncChannelTxn(e.ctx, order.PayNo)
		if err != nil {
			t.Fatalf("SyncChannelTxn: %v", err)
		}
		if got.LocalStatus != payStatusClosed || got.ChannelStatus != payStatusSuccess || got.Changed {
			t.Fatalf("got = %+v, want local=CLOSED channel=SUCCESS changed=false", got)
		}
		stored := loadPayment(t, e.payEnv, merchantID, "SYNC_PAY_CLOSED_01")
		if stored.status != payStatusClosed {
			t.Fatalf("order status = %s, want CLOSED", stored.status)
		}
		txn := loadQueryTxn(t, e.payEnv, order.PayNo)
		if txn.status != payStatusClosed {
			t.Fatalf("channel_txn status = %s, want CLOSED", txn.status)
		}
	})

	// 渠道流水不存在返回 30001。
	t.Run("missing channel_txn returns 30001", func(t *testing.T) {
		if _, err := e.p.SyncChannelTxn(e.ctx, "PA0000000000000999999"); !errors.Is(err, errno.ErrOrderNotFound) {
			t.Fatalf("want 30001, got %v", err)
		}
	})

	// 本地 PAYING、渠道 NOT_FOUND 且超过 60 秒，按 PAY-05 关单并同步为 CLOSED。
	t.Run("local paying not found after 60s closes order", func(t *testing.T) {
		order := createCloseOrder(t, e.payEnv, e.p, merchantID, "SYNC_PAY_NF_01", amountNormal)
		if err := fault.Init(faultPayAfterTx1, fault.WithKill(func() {
			panic("boom")
		})); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fault.Init("") })
		func() {
			defer func() { _ = recover() }()
			_, _ = e.p.Pay(e.ctx, payReq(order))
		}()
		_ = fault.Init("")

		e.clk.Advance(queryNotFoundGrace + time.Second)
		got, err := e.p.SyncChannelTxn(e.ctx, order.PayNo)
		if err != nil {
			t.Fatalf("SyncChannelTxn: %v", err)
		}
		if got.LocalStatus != payStatusClosed || got.ChannelStatus != queryResultNotFound || !got.Changed {
			t.Fatalf("got = %+v, want local=CLOSED channel=NOT_FOUND changed=true", got)
		}
		stored := loadPayment(t, e.payEnv, merchantID, "SYNC_PAY_NF_01")
		if stored.status != payStatusClosed {
			t.Fatalf("order status = %s, want CLOSED", stored.status)
		}
	})

	// 退款 PROCESSING、渠道 SUCCESS，调用后退款单 SUCCESS，changed = true。
	t.Run("refund processing channel success", func(t *testing.T) {
		paid := e.paySucceeded(t, merchantID, "SYNC_REF_PAY_01")
		client, err := NewChannelClient(ChannelClientConfig{
			BaseURL: e.mock.url, Key: []byte(testChannelKey), Clock: e.clk,
		})
		if err != nil {
			t.Fatal(err)
		}
		e.p.UseRefund(&fakeFreezer{status: freezeStatusFrozen}, client, e.p, testChannelNotifyURL)
		refund, err := e.p.CreateRefund(e.ctx, &CreateRefundInput{
			MerchantID:  merchantID,
			PayNo:       paid.PayNo,
			OutRefundNo: "SYNC_REF_01",
			Amount:      refundDropNotify,
		})
		if err != nil || refund.Status != refundStatusProcessing {
			t.Fatalf("CreateRefund = %+v err=%v", refund, err)
		}
		e.clk.Advance(mockFinishDelay)
		e.mock.sched.RunDue()

		got, err := e.p.SyncChannelTxn(e.ctx, refund.RefundNo)
		if err != nil {
			t.Fatalf("SyncChannelTxn: %v", err)
		}
		if got.LocalStatus != payStatusSuccess || got.ChannelStatus != payStatusSuccess || !got.Changed {
			t.Fatalf("got = %+v, want local=SUCCESS channel=SUCCESS changed=true", got)
		}
		var status string
		if err := e.db.QueryRowContext(e.ctx, `SELECT status FROM refund_order WHERE refund_no = ?`, refund.RefundNo).Scan(&status); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		if status != payStatusSuccess {
			t.Fatalf("refund status = %s, want SUCCESS", status)
		}
	})
}
