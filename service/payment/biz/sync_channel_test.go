package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
)

type memSyncPayApplier struct {
	rec    *recordingApplier
	orders *memOrders
}

func (a *memSyncPayApplier) ApplyPayResult(ctx context.Context, in PayResult) (PaymentOrder, error) {
	order, err := a.rec.ApplyPayResult(ctx, in)
	if err != nil {
		return order, err
	}
	a.orders.mu.Lock()
	defer a.orders.mu.Unlock()
	for i := range a.orders.txns {
		if a.orders.txns[i].BizNo == in.BizNo {
			a.orders.txns[i].Status = in.Result
		}
	}
	return order, nil
}

type memSyncRefundApplier struct {
	rec    *refundRecordingApplier
	orders *memOrders
}

func (a *memSyncRefundApplier) ApplyRefundResult(ctx context.Context, in RefundResult) (RefundOrder, error) {
	order, err := a.rec.ApplyRefundResult(ctx, in)
	if err != nil {
		return order, err
	}
	a.orders.mu.Lock()
	defer a.orders.mu.Unlock()
	for i := range a.orders.txns {
		if a.orders.txns[i].BizNo == in.RefundNo {
			a.orders.txns[i].Status = in.Result
		}
	}
	return order, nil
}

// PAY-09 验收 1、2 与 CF-09 各出口：立即同步渠道状态。
func TestSyncChannelTxn(t *testing.T) {
	finished := payAt().Add(2 * time.Minute)
	amount := int64(10000)
	refundAmount := int64(3000)

	t.Run("validation and missing dependencies", func(t *testing.T) {
		var nilPayments *Payments
		if _, err := nilPayments.SyncChannelTxn(context.Background(), queryPayNo); !errors.Is(err, errno.ErrInternal) {
			t.Fatalf("nil payments err = %v", err)
		}
		u := newQueryUnit(t)
		//nolint:staticcheck // SA1012: 专门验证 ctx == nil 的防呆返回 10003。
		if _, err := u.p.SyncChannelTxn(nil, queryPayNo); !errors.Is(err, errno.ErrInternal) {
			t.Fatalf("nil ctx err = %v", err)
		}
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := u.p.SyncChannelTxn(canceledCtx, queryPayNo); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled ctx err = %v", err)
		}
		if _, err := u.p.SyncChannelTxn(context.Background(), ""); !errors.Is(err, errno.ErrInvalidParam) {
			t.Fatalf("empty biz_no err = %v", err)
		}
		uNoQuerier := newPayments(nil, &stubIDs{}, u.clk, "", u.orders)
		if _, err := uNoQuerier.SyncChannelTxn(context.Background(), queryPayNo); !errors.Is(err, errno.ErrInternal) {
			t.Fatalf("nil querier err = %v", err)
		}
	})

	t.Run("channel_txn not found returns 30001", func(t *testing.T) {
		u := newQueryUnit(t)
		_, err := u.p.SyncChannelTxn(context.Background(), "PA_MISSING")
		if !errors.Is(err, errno.ErrOrderNotFound) {
			t.Fatalf("want 30001, got %v", err)
		}
		if calls, _, _ := u.querier.snapshot(); len(calls) != 0 {
			t.Fatalf("querier should not be called, got %v", calls)
		}
	})

	t.Run("channel query failed returns 10004", func(t *testing.T) {
		u := newQueryUnit(t)
		u.orders.txns = append(u.orders.txns, channelTxn{
			Channel: channelMockpay, TxnType: txnTypePay, BizNo: queryPayNo,
			Amount: 10000, Status: txnStatusProcessing, CreatedAt: u.clk.Now().Add(-time.Minute),
		})
		u.querier.res = channelQueryResult{Kind: queryCallFailed}
		_, err := u.p.SyncChannelTxn(context.Background(), queryPayNo)
		if !errors.Is(err, errno.ErrUnavailable) {
			t.Fatalf("want 10004, got %v", err)
		}
	})

	t.Run("channel config error returns 10003", func(t *testing.T) {
		u := newQueryUnit(t)
		u.orders.txns = append(u.orders.txns, channelTxn{
			Channel: channelMockpay, TxnType: txnTypePay, BizNo: queryPayNo,
			Amount: 10000, Status: txnStatusProcessing, CreatedAt: u.clk.Now().Add(-time.Minute),
		})
		u.querier.err = errChannelConfig
		_, err := u.p.SyncChannelTxn(context.Background(), queryPayNo)
		if !errors.Is(err, errno.ErrInternal) {
			t.Fatalf("want 10003, got %v", err)
		}
	})

	// PAY-09 验收 2：本地 CLOSED、渠道 SUCCESS，调用后订单仍为 CLOSED，返回双方状态，changed = false。
	t.Run("local terminal does not modify and returns both statuses", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			localStatus string
			queryRes    channelQueryResult
			wantChannel string
		}{
			{
				name:        "local closed channel success",
				localStatus: payStatusClosed,
				queryRes:    found(payStatusSuccess, amount, true, finished, "MC-1"),
				wantChannel: payStatusSuccess,
			},
			{
				name:        "local success channel not found",
				localStatus: payStatusSuccess,
				queryRes:    channelQueryResult{Kind: queryNotFound},
				wantChannel: queryResultNotFound,
			},
			{
				name:        "local failed channel success",
				localStatus: payStatusFailed,
				queryRes:    found(payStatusSuccess, amount, true, finished, "MC-2"),
				wantChannel: payStatusSuccess,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				u := newQueryUnit(t)
				u.orders.txns = append(u.orders.txns, channelTxn{
					Channel: channelMockpay, TxnType: txnTypePay, BizNo: queryPayNo,
					Amount: 10000, Status: tc.localStatus, CreatedAt: u.clk.Now().Add(-5 * time.Minute),
				})
				u.querier.res = tc.queryRes
				got, err := u.p.SyncChannelTxn(context.Background(), queryPayNo)
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				if got.LocalStatus != tc.localStatus || got.ChannelStatus != tc.wantChannel || got.Changed {
					t.Fatalf("got = %+v, want local=%s channel=%s changed=false", got, tc.localStatus, tc.wantChannel)
				}
				closes, _ := u.closer.calls()
				if u.applier.calls != 0 || closes != 0 {
					t.Fatalf("terminal local txn should not trigger apply=%d or close=%d", u.applier.calls, closes)
				}
			})
		}
	})

	// PAY-09 验收 1（单测）：本地 PROCESSING、渠道终态，按 PAY-05 分发并更新状态。
	t.Run("local processing pay dispatches per PAY-05", func(t *testing.T) {
		cases := []struct {
			name        string
			age         time.Duration
			queryRes    channelQueryResult
			closeRes    channelCloseResult
			closeErr    error
			applyErr    error
			wantLocal   string
			wantChannel string
			wantChanged bool
			wantApply   bool
			wantResult  string
			wantClose   int
			wantErr     error
		}{
			{
				name:        "channel success",
				age:         2 * time.Minute,
				queryRes:    found(payStatusSuccess, amount, true, finished, "MC-OK"),
				wantLocal:   payStatusSuccess,
				wantChannel: payStatusSuccess,
				wantChanged: true,
				wantApply:   true,
				wantResult:  payResultSuccess,
			},
			{
				name:        "channel success missing fields stays processing",
				age:         2 * time.Minute,
				queryRes:    found(payStatusSuccess, amount, false, time.Time{}, "MC-PART"),
				wantLocal:   txnStatusProcessing,
				wantChannel: payStatusSuccess,
				wantChanged: false,
			},
			{
				name:        "channel failed",
				age:         2 * time.Minute,
				queryRes:    found(payStatusFailed, amount, true, finished, "MC-FAIL"),
				wantLocal:   payStatusFailed,
				wantChannel: payStatusFailed,
				wantChanged: true,
				wantApply:   true,
				wantResult:  payResultFailed,
			},
			{
				name:        "channel closed",
				age:         2 * time.Minute,
				queryRes:    found(payStatusClosed, 0, false, time.Time{}, "MC-CL"),
				wantLocal:   payStatusClosed,
				wantChannel: payStatusClosed,
				wantChanged: true,
				wantApply:   true,
				wantResult:  payResultClosed,
			},
			{
				name:        "channel processing",
				age:         2 * time.Minute,
				queryRes:    found(txnStatusProcessing, amount, true, time.Time{}, "MC-P"),
				wantLocal:   txnStatusProcessing,
				wantChannel: txnStatusProcessing,
				wantChanged: false,
			},
			{
				name:        "not found within 60s grace stays processing",
				age:         60 * time.Second,
				queryRes:    channelQueryResult{Kind: queryNotFound},
				wantLocal:   txnStatusProcessing,
				wantChannel: queryResultNotFound,
				wantChanged: false,
			},
			{
				name:        "not found after 60s closes on channel",
				age:         61 * time.Second,
				queryRes:    channelQueryResult{Kind: queryNotFound},
				closeRes:    channelCloseResult{Outcome: closeClosed},
				wantLocal:   payStatusClosed,
				wantChannel: queryResultNotFound,
				wantChanged: true,
				wantClose:   1,
				wantApply:   true,
				wantResult:  payResultClosed,
			},
			{
				name:      "not found after 60s close error returns 10004",
				age:       61 * time.Second,
				queryRes:  channelQueryResult{Kind: queryNotFound},
				closeErr:  errors.New("timeout"),
				wantClose: 1,
				wantErr:   errno.ErrUnavailable,
			},
			{
				name:     "apply amount mismatch returns 50002",
				age:      2 * time.Minute,
				queryRes: found(payStatusSuccess, amount, true, finished, "MC-MM"),
				applyErr: errno.ErrChannelAmountMismatch,
				wantErr:  errno.ErrChannelAmountMismatch,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				u := newQueryUnit(t)
				u.p.applier = &memSyncPayApplier{rec: u.applier, orders: u.orders}
				u.orders.txns = append(u.orders.txns, channelTxn{
					Channel: channelMockpay, TxnType: txnTypePay, BizNo: queryPayNo,
					Amount: 10000, Status: txnStatusProcessing, CreatedAt: u.clk.Now().Add(-tc.age),
				})
				u.querier.res = tc.queryRes
				u.closer.closeOut = tc.closeRes
				u.closer.closeErr = tc.closeErr
				u.applier.err = tc.applyErr
				got, err := u.p.SyncChannelTxn(context.Background(), queryPayNo)
				if tc.wantErr != nil {
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("want err %v, got %v", tc.wantErr, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if got.LocalStatus != tc.wantLocal || got.ChannelStatus != tc.wantChannel || got.Changed != tc.wantChanged {
					t.Fatalf("got = %+v, want local=%s channel=%s changed=%v", got, tc.wantLocal, tc.wantChannel, tc.wantChanged)
				}
				closes, _ := u.closer.calls()
				if closes != tc.wantClose {
					t.Fatalf("closeCalls = %d, want %d", closes, tc.wantClose)
				}
				if tc.wantApply {
					if u.applier.calls != 1 || u.applier.in.Result != tc.wantResult || u.applier.in.Source != paySourceQuery {
						t.Fatalf("apply calls=%d in=%+v, want result=%s source=QUERY", u.applier.calls, u.applier.in, tc.wantResult)
					}
				} else if u.applier.calls != 0 {
					t.Fatalf("apply calls = %d, want 0", u.applier.calls)
				}
			})
		}
	})

	t.Run("local processing refund dispatches per PAY-05", func(t *testing.T) {
		t.Run("channel success", func(t *testing.T) {
			u := newQueryUnit(t)
			u.p.refundApplier = &memSyncRefundApplier{rec: u.rapplier, orders: u.orders}
			u.seedRefund(refundStatusProcessing)
			u.orders.mu.Lock()
			u.orders.txns[0].CreatedAt = u.clk.Now().Add(-2 * time.Minute)
			u.orders.mu.Unlock()
			u.querier.res = found(payStatusSuccess, refundAmount, true, finished, "MC-R1")
			got, err := u.p.SyncChannelTxn(context.Background(), queryRefundNo)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got.LocalStatus != payStatusSuccess || got.ChannelStatus != payStatusSuccess || !got.Changed {
				t.Fatalf("got = %+v", got)
			}
			if u.rapplier.calls != 1 {
				t.Fatalf("refund apply calls = %d", u.rapplier.calls)
			}
		})

		t.Run("channel closed ignored for refund", func(t *testing.T) {
			u := newQueryUnit(t)
			u.p.refundApplier = &memSyncRefundApplier{rec: u.rapplier, orders: u.orders}
			u.seedRefund(refundStatusProcessing)
			u.orders.mu.Lock()
			u.orders.txns[0].CreatedAt = u.clk.Now().Add(-2 * time.Minute)
			u.orders.mu.Unlock()
			u.querier.res = found(payStatusClosed, 0, false, time.Time{}, "MC-RC")
			got, err := u.p.SyncChannelTxn(context.Background(), queryRefundNo)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got.LocalStatus != txnStatusProcessing || got.ChannelStatus != payStatusClosed || got.Changed {
				t.Fatalf("got = %+v", got)
			}
			if u.rapplier.calls != 0 {
				t.Fatalf("refund apply calls = %d, want 0", u.rapplier.calls)
			}
		})

		t.Run("not found after 60s resubmits and rejected moves to failed", func(t *testing.T) {
			u := newQueryUnit(t)
			u.p.refundApplier = &memSyncRefundApplier{rec: u.rapplier, orders: u.orders}
			u.seedRefund(refundStatusProcessing)
			u.orders.mu.Lock()
			u.orders.txns[0].CreatedAt = u.clk.Now().Add(-61 * time.Second)
			u.orders.mu.Unlock()
			u.querier.res = channelQueryResult{Kind: queryNotFound}
			u.refunder.res = ChannelAcceptResult{Outcome: acceptRejected, Reason: "refund rejected"}
			got, err := u.p.SyncChannelTxn(context.Background(), queryRefundNo)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got.LocalStatus != payStatusFailed || got.ChannelStatus != queryResultNotFound || !got.Changed {
				t.Fatalf("got = %+v", got)
			}
			if u.refunder.callCount() != 1 {
				t.Fatalf("refunder calls = %d, want 1", u.refunder.callCount())
			}
		})
	})
}
