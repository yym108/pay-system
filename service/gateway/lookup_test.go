package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
)

type fakeCredClient struct {
	n     int
	appID string
	opts  int
	resp  *payment.GetMerchantCredentialResponse
	err   error
}

func (f *fakeCredClient) GetMerchantCredential(_ context.Context, req *payment.GetMerchantCredentialRequest, opts ...callopt.Option) (*payment.GetMerchantCredentialResponse, error) {
	f.n++
	if req != nil {
		f.appID = req.AppId
	}
	f.opts = len(opts)
	return f.resp, f.err
}

func TestPaymentLookupMapsErrors(t *testing.T) {
	if RPCTimeout("GetMerchantCredential") != DefaultRPCTimeout || DefaultRPCTimeout != time.Second {
		t.Fatalf("timeout %s", RPCTimeout("GetMerchantCredential"))
	}
	remote := "remote-secret-must-not-leak"
	cases := []struct {
		name string
		err  error
		resp *payment.GetMerchantCredentialResponse
		want *errno.Error
	}{
		{name: "merchant", err: kerrors.NewBizStatusError(errno.ErrMerchantInvalid.Code, remote), want: errno.ErrMerchantInvalid},
		{name: "invalid param", err: kerrors.NewBizStatusError(errno.ErrInvalidParam.Code, remote), want: errno.ErrInternal},
		{name: "internal", err: kerrors.NewBizStatusError(errno.ErrInternal.Code, remote), want: errno.ErrInternal},
		{name: "unknown", err: kerrors.NewBizStatusError(99999, remote), want: errno.ErrInternal},
		{name: "deadline", err: context.DeadlineExceeded, want: errno.ErrUnavailable},
		{name: "canceled", err: context.Canceled, want: errno.ErrUnavailable},
		{name: "dial", err: errors.New("dial tcp: connection refused"), want: errno.ErrUnavailable},
		{name: "rpc timeout", err: kerrors.ErrRPCTimeout, want: errno.ErrUnavailable},
		{name: "nil resp", want: errno.ErrInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeCredClient{resp: tc.resp, err: tc.err}
			_, err := (&paymentLookup{cli: fake}).Lookup(context.Background(), "AbC ")
			if !errors.Is(err, tc.want) || err.Error() != tc.want.Msg || strings.Contains(err.Error(), remote) {
				t.Fatalf("err %v", err)
			}
		})
	}
}

func TestPaymentLookupCopiesCredential(t *testing.T) {
	const appID = "AbC "
	fake := &fakeCredClient{resp: &payment.GetMerchantCredentialResponse{
		MerchantId: "M100001",
		Status:     "ACTIVE",
		AppSecret:  "sekret",
	}}
	p := &paymentLookup{cli: fake}
	cred, err := p.Lookup(context.Background(), appID)
	if err != nil {
		t.Fatal(err)
	}
	if cred.MerchantID != "M100001" || cred.Status != "ACTIVE" || string(cred.Secret) != "sekret" {
		t.Fatalf("cred %+v", cred)
	}
	cred.Secret[0] = 'X'
	if fake.resp.AppSecret != "sekret" {
		t.Fatal("secret alias")
	}
	if _, err = p.Lookup(context.Background(), appID); err != nil {
		t.Fatal(err)
	}
	if fake.n != 2 || fake.appID != appID || fake.opts != 1 {
		t.Fatalf("n=%d app=%q opts=%d", fake.n, fake.appID, fake.opts)
	}

	var bare paymentLookup
	if _, err = bare.Lookup(context.Background(), appID); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil client %v", err)
	}
	if NewPaymentLookup(nil) == nil {
		t.Fatal("nil lookup")
	}
}
