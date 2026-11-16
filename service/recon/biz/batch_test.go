package biz_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/service/recon/biz"
)

func shanghaiTime(year int, month time.Month, day, hour, min, sec int) time.Time {
	return time.Date(year, month, day, hour, min, sec, 0, time.FixedZone("Asia/Shanghai", 8*3600))
}

func TestRunBatchValidateParams(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(shanghaiTime(2026, 10, 27, 10, 0, 0))

	svc := biz.NewRecon(nil, nil, clk, []string{"mockpay"}, nil, nil)

	cases := []struct {
		name     string
		channel  string
		billDate string
		wantErr  bool
	}{
		{name: "empty channel", channel: "", billDate: "2026-10-26", wantErr: true},
		{name: "unsupported channel", channel: "alipay", billDate: "2026-10-26", wantErr: true},
		{name: "bad date format", channel: "mockpay", billDate: "2026/10/26", wantErr: true},
		{name: "future date", channel: "mockpay", billDate: "2026-10-28", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RunBatch(context.Background(), biz.RunBatchInput{
				Channel:  tc.channel,
				BillDate: tc.billDate,
			})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				var e *errno.Error
				if !errors.As(err, &e) || e.Code != errno.ErrInvalidParam.Code {
					t.Fatalf("expected ErrInvalidParam (10001), got %v", err)
				}
			}
		})
	}
}

func TestNewReconDefaultsAndNilDB(t *testing.T) {
	svc := biz.NewRecon(nil, nil, nil, nil, nil, nil)
	today := clock.Real{}.Now().Format("2006-01-02")
	_, err := svc.RunBatch(context.Background(), biz.RunBatchInput{
		Channel:  "mockpay",
		BillDate: today,
	})
	var e *errno.Error
	if !errors.As(err, &e) || e.Code != errno.ErrInternal.Code {
		t.Fatalf("expected ErrInternal when db is nil, got %v", err)
	}
}
