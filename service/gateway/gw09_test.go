package gateway

import (
	"context"
	"sync"
	"testing"

	"github.com/cloudwego/kitex/client/callopt"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
)

// gw09Payment 是退款和转账路由的假 RPC。
// 紧跟着的四个方法补在 GW-02 的 gw02Payment 上，paymentAPI 扩大后原有测试仍能编译。
type gw09Payment struct {
	mu           sync.Mutex
	refundN      int
	transferN    int
	lastTransfer *payment.CreateTransferRequest
}

func (f *gw02Payment) CreateRefund(context.Context, *payment.CreateRefundRequest, ...callopt.Option) (*payment.CreateRefundResponse, error) {
	return &payment.CreateRefundResponse{BizCode: 0, RefundOrder: &payment.RefundOrder{RefundNo: "RF0000000000000000001", Status: "PROCESSING"}}, nil
}

func (f *gw02Payment) QueryRefund(context.Context, *payment.QueryRefundRequest, ...callopt.Option) (*payment.RefundOrder, error) {
	return &payment.RefundOrder{RefundNo: "RF0000000000000000001", Status: "PROCESSING"}, nil
}

func (f *gw02Payment) CreateTransfer(context.Context, *payment.CreateTransferRequest, ...callopt.Option) (*payment.CreateTransferResponse, error) {
	return &payment.CreateTransferResponse{BizCode: 0, Transfer: &payment.Transfer{TransferNo: "TF0000000000000000001", Status: "SUCCESS"}}, nil
}

func (f *gw02Payment) QueryTransfer(context.Context, *payment.QueryTransferRequest, ...callopt.Option) (*payment.Transfer, error) {
	return &payment.Transfer{TransferNo: "TF0000000000000000001", Status: "SUCCESS"}, nil
}

func (f *gw09Payment) CreatePayment(context.Context, *payment.CreatePaymentRequest, ...callopt.Option) (*payment.PaymentOrder, error) {
	return &payment.PaymentOrder{PayNo: "PA0000000000000000001", Status: "INIT"}, nil
}

func (f *gw09Payment) Pay(context.Context, *payment.PayRequest, ...callopt.Option) (*payment.PayResponse, error) {
	return &payment.PayResponse{BizCode: 0, PaymentOrder: &payment.PaymentOrder{Status: "PAYING"}}, nil
}

func (f *gw09Payment) ClosePayment(context.Context, *payment.ClosePaymentRequest, ...callopt.Option) (*payment.ClosePaymentResponse, error) {
	return &payment.ClosePaymentResponse{BizCode: 0, PaymentOrder: &payment.PaymentOrder{Status: "CLOSED"}}, nil
}

func (f *gw09Payment) QueryPayment(context.Context, *payment.QueryPaymentRequest, ...callopt.Option) (*payment.PaymentOrder, error) {
	return &payment.PaymentOrder{PayNo: "PA0000000000000000001", Status: "INIT"}, nil
}

func (f *gw09Payment) CreateRefund(_ context.Context, req *payment.CreateRefundRequest, _ ...callopt.Option) (*payment.CreateRefundResponse, error) {
	f.mu.Lock()
	f.refundN++
	f.mu.Unlock()
	out := ""
	var amount int64
	if req != nil {
		out = req.OutRefundNo
		amount = req.Amount
	}
	return &payment.CreateRefundResponse{
		BizCode:     0,
		RefundOrder: &payment.RefundOrder{RefundNo: "RF0000000000000000001", OutRefundNo: out, Amount: amount, Status: "PROCESSING"},
	}, nil
}

func (f *gw09Payment) QueryRefund(context.Context, *payment.QueryRefundRequest, ...callopt.Option) (*payment.RefundOrder, error) {
	return &payment.RefundOrder{RefundNo: "RF0000000000000000001", Status: "PROCESSING"}, nil
}

func (f *gw09Payment) CreateTransfer(_ context.Context, req *payment.CreateTransferRequest, _ ...callopt.Option) (*payment.CreateTransferResponse, error) {
	f.mu.Lock()
	f.transferN++
	f.lastTransfer = req
	f.mu.Unlock()
	tr := &payment.Transfer{TransferNo: "TF0000000000000000001", Currency: "CNY", Mode: "LOCAL", Status: "SUCCESS"}
	if req != nil {
		tr.OutTransferNo = req.OutTransferNo
		tr.MerchantId = req.MerchantId
		tr.PayeeMerchantId = req.PayeeMerchantId
		tr.Amount = req.Amount
	}
	return &payment.CreateTransferResponse{BizCode: 0, Transfer: tr}, nil
}

func (f *gw09Payment) QueryTransfer(context.Context, *payment.QueryTransferRequest, ...callopt.Option) (*payment.Transfer, error) {
	return &payment.Transfer{TransferNo: "TF0000000000000000001", MerchantId: gw03MID, Status: "SUCCESS"}, nil
}

// GW-09 验收 1：创建退款和创建转账的四个非法 amount 都是签名过的 10001，且不调用 RPC。收款商户号 M12345、X100002 同样是 10001，不调用 RPC。
func TestGW09AmountAndPayeeRejected(t *testing.T) {
	pay := &gw09Payment{}
	addr := gw02Start(t, pay, nil)
	refunds := []string{
		`{"pay_no":"PA1","out_refund_no":"R1","amount":100.5}`,
		`{"pay_no":"PA1","out_refund_no":"R1","amount":"100"}`,
		`{"pay_no":"PA1","out_refund_no":"R1","amount":0}`,
		`{"pay_no":"PA1","out_refund_no":"R1","amount":10000001}`,
	}
	for _, body := range refunds {
		got := gw02Post(t, addr, "/v1/refunds", []byte(body))
		assertSignedErrno(t, got, errno.ErrInvalidParam)
	}
	transfers := []string{
		`{"out_transfer_no":"TR1","payee_merchant_id":"M100002","amount":100.5}`,
		`{"out_transfer_no":"TR1","payee_merchant_id":"M100002","amount":"100"}`,
		`{"out_transfer_no":"TR1","payee_merchant_id":"M100002","amount":0}`,
		`{"out_transfer_no":"TR1","payee_merchant_id":"M100002","amount":10000001}`,
		`{"out_transfer_no":"TR1","payee_merchant_id":"M12345","amount":100}`,
		`{"out_transfer_no":"TR1","payee_merchant_id":"X100002","amount":100}`,
	}
	for _, body := range transfers {
		got := gw02Post(t, addr, "/v1/transfers", []byte(body))
		assertSignedErrno(t, got, errno.ErrInvalidParam)
	}
	pay.mu.Lock()
	refundN, transferN := pay.refundN, pay.transferN
	pay.mu.Unlock()
	if refundN != 0 || transferN != 0 {
		t.Fatalf("rpc refund %d transfer %d", refundN, transferN)
	}
}

// GW-09 验收 2：转账请求体伪造 merchant_id，假 RPC 收到的 MerchantId 是验签商户。
func TestGW09ForgedPayerIgnored(t *testing.T) {
	pay := &gw09Payment{}
	addr := gw02Start(t, pay, nil)
	body := []byte(`{"merchant_id":"M999999","out_transfer_no":"TR20261012001","payee_merchant_id":"M100002","amount":500}`)
	got := gw02Post(t, addr, "/v1/transfers", body)
	assertSignedErrno(t, got, errno.OK)
	pay.mu.Lock()
	req := pay.lastTransfer
	pay.mu.Unlock()
	if req == nil || req.MerchantId != gw03MID {
		t.Fatalf("req %+v", req)
	}
}
