package gateway

import (
	"context"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/kitex_gen/payment/paymentservice"
	"github.com/yym108/pay-system/pkg/errno"
)

// Credential 是验签用的商户凭证。Secret 是 app_secret 的副本。
type Credential struct {
	MerchantID string
	Status     string
	Secret     []byte
}

// MerchantLookup 按 app_id 的原始字节取凭证。实现不得缓存。
type MerchantLookup interface {
	Lookup(ctx context.Context, appID string) (Credential, error)
}

// WithMerchantLookup 安装商户凭证查询。只应装在商户端口。
// 管理端口即使装了也不调用。
func WithMerchantLookup(lu MerchantLookup) Option {
	return func(h *Handler) {
		if h != nil {
			h.lookup = lu
		}
	}
}

// merchantCredentialClient 只保留验签要调用的那一个方法，测试假客户端不必实现整个 PaymentService。
type merchantCredentialClient interface {
	GetMerchantCredential(ctx context.Context, req *payment.GetMerchantCredentialRequest, callOptions ...callopt.Option) (*payment.GetMerchantCredentialResponse, error)
}

type paymentLookup struct {
	cli merchantCredentialClient
}

// NewPaymentLookup 用 PaymentService 客户端查商户凭证。
// 每次调用 GetMerchantCredential，不缓存，不重试。超时走 RPCTimeout 的默认 1 秒。
func NewPaymentLookup(cli paymentservice.Client) MerchantLookup {
	return &paymentLookup{cli: cli}
}

func (p *paymentLookup) Lookup(ctx context.Context, appID string) (Credential, error) {
	if p == nil || p.cli == nil {
		return Credential{}, errno.ErrInternal
	}
	resp, err := p.cli.GetMerchantCredential(ctx, &payment.GetMerchantCredentialRequest{AppId: appID},
		callopt.WithRPCTimeout(RPCTimeout("GetMerchantCredential")))
	if err != nil {
		return Credential{}, mapCredentialErr(err)
	}
	if resp == nil {
		return Credential{}, errno.ErrInternal
	}
	return Credential{
		MerchantID: resp.MerchantId,
		Status:     resp.Status,
		Secret:     append([]byte(nil), resp.AppSecret...),
	}, nil
}

// mapCredentialErr 只认已有哨兵。传输错误不是 10003，避免把“结果未知”收成内部错误。
// 不用 errno.From，也不采用对端 msg。
func mapCredentialErr(err error) error {
	biz, ok := kerrors.FromBizStatusError(err)
	if !ok || biz == nil {
		return errno.ErrUnavailable
	}
	if biz.BizStatusCode() == errno.ErrMerchantInvalid.Code {
		return errno.ErrMerchantInvalid
	}
	return errno.ErrInternal
}
