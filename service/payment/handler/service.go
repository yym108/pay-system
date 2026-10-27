package handler

import (
	"context"

	"github.com/yym108/pay-system/service/payment/biz"
)

// Service 是 handler 调用的进程内支付服务。
// *biz.App 已实现它。后续功能点在这里加方法。测试用假实现替换。
type Service interface {
	GetMerchantCredential(ctx context.Context, appID string) (biz.MerchantCredential, error)
	CreatePayment(ctx context.Context, in *biz.CreatePaymentInput) (biz.PaymentOrder, error)
	Pay(ctx context.Context, in *biz.PayInput) (biz.PaymentOrder, error)
}

var _ Service = (*biz.App)(nil)
