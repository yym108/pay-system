package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
)

func TestApplyChannelNotifyRejectsEmpty(t *testing.T) {
	_, err := (&Payments{}).ApplyChannelNotify(context.Background(), PayResult{})
	if !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("err = %v", err)
	}
}

func TestApplyChannelNotifyNilStore(t *testing.T) {
	in := PayResult{
		BizNo:      "PA0000000123456789012",
		Result:     payResultSuccess,
		Source:     paySourceNotify,
		FinishedAt: time.Unix(1, 0),
	}
	_, err := (&Payments{}).ApplyChannelNotify(context.Background(), in)
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("err = %v", err)
	}
}
