package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/outbox"
)

func TestReplayKeys(t *testing.T) {
	ctx := context.Background()
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 11, 10, 9, 0, 0, 0, time.FixedZone("Asia/Shanghai", 8*60*60)))
	p := NewPayments(nil, nil, clk, "", nil)
	cases := []struct {
		name string
		in   *ReplayInput
	}{
		{name: "nil", in: nil},
		{name: "empty", in: &ReplayInput{}},
		{name: "both", in: &ReplayInput{EventID: "EV1", BizNo: "PA1", EventType: outbox.EventPaymentSucceeded}},
		{name: "only biz", in: &ReplayInput{BizNo: "PA1"}},
		{name: "only type", in: &ReplayInput{EventType: outbox.EventPaymentSucceeded}},
	}
	for _, tc := range cases {
		_, err := p.ReplayEvent(ctx, tc.in)
		if !errors.Is(err, errno.ErrInvalidParam) {
			t.Fatalf("%s = %v", tc.name, err)
		}
		var app *App
		if _, err = app.ReplayEvent(ctx, tc.in); !errors.Is(err, errno.ErrInvalidParam) {
			t.Fatalf("nil app %s = %v", tc.name, err)
		}
	}
	var nilPay *Payments
	if _, err := nilPay.ReplayEvent(ctx, &ReplayInput{EventID: "EV1"}); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil receiver valid key = %v", err)
	}
	if _, err := nilPay.ReplayEvent(ctx, nil); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("nil receiver nil input = %v", err)
	}
	if _, err := p.ReplayEvent(ctx, &ReplayInput{EventID: "EV1"}); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("no db = %v", err)
	}
	if _, err := p.ReplayEvent(ctx, &ReplayInput{EventID: " "}); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("whitespace event_id = %v", err)
	}
	if _, err := p.ReplayEvent(ctx, &ReplayInput{EventID: "", BizNo: "PA1", EventType: outbox.EventPaymentSucceeded}); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("empty event_id with biz = %v", err)
	}
	p.clk = nil
	if _, err := p.ReplayEvent(ctx, &ReplayInput{EventID: "EV1"}); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil clock = %v", err)
	}
	if _, err := (&App{}).ReplayEvent(ctx, &ReplayInput{EventID: "EV1"}); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil payments = %v", err)
	}
}
