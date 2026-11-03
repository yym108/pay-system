package biz

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

func refundSeedPay(payNo, outTradeNo string) PaymentOrder {
	return PaymentOrder{
		MerchantID: "M100002", PayNo: payNo, OutTradeNo: outTradeNo,
		Amount: 10000, RefundedAmount: 0, Channel: channelMockpay,
		Currency: payCurrencyCNY, Status: payStatusSuccess, Version: 1,
	}
}

func newRefundOrders(extra ...PaymentOrder) *memOrders {
	m := &memOrders{}
	m.rows = append(m.rows, refundSeedPay("PA_REF_1", "OT_REF_1"))
	m.rows = append(m.rows, extra...)
	return m
}

func newRefundPayments(orders *memOrders, ids Numberer) *Payments {
	clk := &clock.Fake{}
	clk.Set(feeAt(10, 0, 0, 0))
	if ids == nil {
		ids = &stubIDs{prefix: refundNoPrefix, nos: []string{
			"RF0000000000000000001",
			"RF0000000000000000002",
			"RF0000000000000000003",
			"RF0000000000000000004",
		}}
	}
	return newPayments(nil, ids, clk, "", orders)
}

func refundInput(payNo, outTradeNo, outRefundNo string, amount money.Amount) *CreateRefundInput {
	return &CreateRefundInput{
		MerchantID: "M100002", PayNo: payNo, OutTradeNo: outTradeNo,
		OutRefundNo: outRefundNo, Amount: amount,
	}
}

func payRefunded(m *memOrders, payNo string) (money.Amount, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, row := range m.rows {
		if row.PayNo == payNo {
			return row.RefundedAmount, row.Version
		}
	}
	return -1, 0
}

func copiedRefunds(m *memOrders) []RefundOrder {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RefundOrder, len(m.refunds))
	copy(out, m.refunds)
	return out
}

// REF-01 验收：PAYING、INIT、CLOSED、FAILED 都不能退，不占额度。
func TestCreateRefundRejectsPaying(t *testing.T) {
	for _, status := range []string{payStatusPaying, payStatusInit, payStatusClosed, payStatusFailed} {
		t.Run(status, func(t *testing.T) {
			orders := newRefundOrders()
			orders.rows[0].Status = status
			p := newRefundPayments(orders, nil)
			_, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
			assertErrno(t, err, errno.ErrOrderStatusConflict)
			if n := copiedRefunds(orders); len(n) != 0 {
				t.Fatalf("refunds = %+v", n)
			}
			if got, ver := payRefunded(orders, "PA_REF_1"); got != 0 || ver != 1 {
				t.Fatalf("refunded=%d version=%d", got, ver)
			}
		})
	}
}

// REF-01 验收：同一单 6000 成功后再退 5000 超额；另一张单 6000 加 4000 刚好占满。
func TestCreateRefundQuota(t *testing.T) {
	orders := newRefundOrders(refundSeedPay("PA_REF_2", "OT_REF_2"))
	p := newRefundPayments(orders, nil)
	ctx := context.Background()
	first, err := p.CreateRefund(ctx, refundInput("PA_REF_1", "", "R1", 6000))
	if err != nil || first.Status != refundStatusInit || first.RefundNo != "RF0000000000000000001" || first.Amount != 6000 {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	_, err = p.CreateRefund(ctx, refundInput("PA_REF_1", "", "R2", 5000))
	assertErrno(t, err, errno.ErrRefundExceeded)
	if got, _ := payRefunded(orders, "PA_REF_1"); got != 6000 {
		t.Fatalf("refunded = %d", got)
	}
	if _, err := p.CreateRefund(ctx, refundInput("PA_REF_2", "", "R3", 6000)); err != nil {
		t.Fatal(err)
	}
	second, err := p.CreateRefund(ctx, refundInput("", "OT_REF_2", "R4", 4000))
	if err != nil || second.PayNo != "PA_REF_2" || second.Status != refundStatusInit {
		t.Fatalf("second = %+v err=%v", second, err)
	}
	if got, ver := payRefunded(orders, "PA_REF_2"); got != 10000 || ver != 3 {
		t.Fatalf("second refunded=%d version=%d", got, ver)
	}
	if n := copiedRefunds(orders); len(n) != 3 {
		t.Fatalf("refunds = %d", len(n))
	}
}

// REF-01 验收：10 个并发各退 3000，恰好 3 笔成功，额度停在 9000。
func TestCreateRefundConcurrentQuota(t *testing.T) {
	orders := newRefundOrders()
	clk := &clock.Fake{}
	clk.Set(feeAt(10, 0, 0, 0))
	ids, err := idgen.New(1, clk)
	if err != nil {
		t.Fatal(err)
	}
	p := newPayments(nil, ids, clk, "", orders)
	const n = 10
	errs := make([]error, n)
	got := make([]RefundOrder, n)
	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			got[i], errs[i] = p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", fmt.Sprintf("C%d", i), 3000))
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	ok := 0
	for i, err := range errs {
		if err == nil {
			ok++
			if got[i].Status != refundStatusInit || got[i].Amount != 3000 || got[i].PayNo != "PA_REF_1" {
				t.Fatalf("success %d = %+v", i, got[i])
			}
			continue
		}
		assertErrno(t, err, errno.ErrRefundExceeded)
	}
	if ok != 3 {
		t.Fatalf("successes = %d", ok)
	}
	if amount, _ := payRefunded(orders, "PA_REF_1"); amount != 9000 {
		t.Fatalf("refunded = %d", amount)
	}
	if rows := copiedRefunds(orders); len(rows) != 3 {
		t.Fatalf("refunds = %d", len(rows))
	}
}

// REF-01 验收：相同 out_refund_no 返回原单且不加额度；金额或 pay_no 不同是 10006。重复请求不新增第二行。
func TestCreateRefundIdempotent(t *testing.T) {
	orders := newRefundOrders(refundSeedPay("PA_REF_2", "OT_REF_2"))
	p := newRefundPayments(orders, nil)
	ctx := context.Background()
	in := refundInput("PA_REF_1", "", "SAME", 3000)
	in.Reason = "demo"
	first, err := p.CreateRefund(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := p.CreateRefund(ctx, in)
	if err != nil || again.RefundNo != first.RefundNo || again.Amount != 3000 {
		t.Fatalf("again = %+v err=%v", again, err)
	}
	if rows := copiedRefunds(orders); len(rows) != 1 {
		t.Fatalf("replay rows = %d", len(rows))
	}
	if got, ver := payRefunded(orders, "PA_REF_1"); got != 3000 || ver != 2 {
		t.Fatalf("after replay refunded=%d version=%d", got, ver)
	}
	changed := refundInput("PA_REF_1", "", "SAME", 2000)
	_, err = p.CreateRefund(ctx, changed)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	other := refundInput("PA_REF_2", "", "SAME", 3000)
	_, err = p.CreateRefund(ctx, other)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if got, _ := payRefunded(orders, "PA_REF_1"); got != 3000 {
		t.Fatalf("original refunded = %d", got)
	}
	if got, ver := payRefunded(orders, "PA_REF_2"); got != 0 || ver != 1 {
		t.Fatalf("other refunded=%d version=%d", got, ver)
	}
	if rows := copiedRefunds(orders); len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
}

// REF-01 验收：别的商户、不存在的 pay_no、不存在的 out_trade_no 都是 30001。
func TestCreateRefundOtherMerchant(t *testing.T) {
	orders := newRefundOrders()
	p := newRefundPayments(orders, nil)
	ctx := context.Background()
	cases := []*CreateRefundInput{
		{MerchantID: "M100003", PayNo: "PA_REF_1", OutRefundNo: "R1", Amount: 1000},
		{MerchantID: "M100002", PayNo: "PA_MISSING", OutRefundNo: "R2", Amount: 1000},
		{MerchantID: "M100002", OutTradeNo: "OT_MISSING", OutRefundNo: "R3", Amount: 1000},
	}
	for _, in := range cases {
		_, err := p.CreateRefund(ctx, in)
		assertErrno(t, err, errno.ErrOrderNotFound)
	}
	if rows := copiedRefunds(orders); len(rows) != 0 {
		t.Fatalf("refunds = %+v", rows)
	}
	if got, ver := payRefunded(orders, "PA_REF_1"); got != 0 || ver != 1 {
		t.Fatalf("refunded=%d version=%d", got, ver)
	}
}

// REF-01 验收：两个键都有、都没有、out_refund_no 空、金额 0、reason 超长或非法 UTF-8 都是 10001，不写。
func TestCreateRefundKeyChoice(t *testing.T) {
	orders := newRefundOrders()
	p := newRefundPayments(orders, nil)
	ctx := context.Background()
	cases := []*CreateRefundInput{
		{MerchantID: "M100002", PayNo: "PA_REF_1", OutTradeNo: "OT_REF_1", OutRefundNo: "R1", Amount: 1000},
		{MerchantID: "M100002", OutRefundNo: "R1", Amount: 1000},
		{MerchantID: "M100002", PayNo: "PA_REF_1", Amount: 1000},
		{MerchantID: "M100002", PayNo: "PA_REF_1", OutRefundNo: "R1", Amount: 0},
		{MerchantID: "M100002", PayNo: "PA_REF_1", OutRefundNo: "R1", Amount: 1000, Reason: strings.Repeat("a", maxReasonRunes+1)},
		{MerchantID: "M100002", PayNo: "PA_REF_1", OutRefundNo: "R1", Amount: 1000, Reason: "\xff"},
	}
	for i, in := range cases {
		_, err := p.CreateRefund(ctx, in)
		assertErrno(t, err, errno.ErrInvalidParam)
		if rows := copiedRefunds(orders); len(rows) != 0 {
			t.Fatalf("case %d wrote %+v", i, rows)
		}
	}
	if got, _ := payRefunded(orders, "PA_REF_1"); got != 0 {
		t.Fatalf("refunded = %d", got)
	}
}

// REF-01 验收：空 channel 的 SUCCESS 单是内部错误，不插入。
func TestCreateRefundEmptyChannel(t *testing.T) {
	orders := newRefundOrders()
	orders.rows[0].Channel = ""
	p := newRefundPayments(orders, nil)
	_, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	assertErrno(t, err, errno.ErrInternal)
	if rows := copiedRefunds(orders); len(rows) != 0 {
		t.Fatalf("refunds = %+v", rows)
	}
	if got, ver := payRefunded(orders, "PA_REF_1"); got != 0 || ver != 1 {
		t.Fatalf("refunded=%d version=%d", got, ver)
	}
}

// REF-01 验收：发号时钟回拨映射为 10004，不插入。
func TestCreateRefundClockRollback(t *testing.T) {
	orders := newRefundOrders()
	p := newRefundPayments(orders, &stubIDs{prefix: refundNoPrefix, err: idgen.ErrClockRollback})
	_, err := p.CreateRefund(context.Background(), refundInput("PA_REF_1", "", "R1", 1000))
	assertErrno(t, err, errno.ErrUnavailable)
	if rows := copiedRefunds(orders); len(rows) != 0 {
		t.Fatalf("refunds = %+v", rows)
	}
	if got, _ := payRefunded(orders, "PA_REF_1"); got != 0 {
		t.Fatalf("refunded = %d", got)
	}
}
