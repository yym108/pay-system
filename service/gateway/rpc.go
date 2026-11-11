package gateway

import (
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"

	"github.com/yym108/pay-system/kitex_gen/account/accountservice"
	"github.com/yym108/pay-system/kitex_gen/payment/paymentservice"
	"github.com/yym108/pay-system/kitex_gen/recon/reconservice"
	"github.com/yym108/pay-system/pkg/obs"
)

const (
	// AddrPayment 是网关调用 payment 的地址。
	AddrPayment = "payment:8881"
	// AddrAccount 是网关调用 account 的地址。
	AddrAccount = "account:8882"
	// AddrRecon 是网关调用 recon 的地址。
	AddrRecon = "recon:8883"

	// ConnectTimeout 是三个 Kitex 客户端的连接超时。
	ConnectTimeout = 100 * time.Millisecond
	// DefaultRPCTimeout 是未单独列出的方法的调用超时。
	DefaultRPCTimeout = time.Second
)

// Clients 是网关自己的 Kitex 客户端。不使用 pkg/accountclient，也不重试。
// GW-01 没有 HTTP 路由调用它们。对端没启动时 NewClients 也不拨号。
type Clients struct {
	Payment paymentservice.Client
	Account accountservice.Client
	Recon   reconservice.Client
}

// RPCTimeout 按方法返回调用超时。未列出的方法是 1 秒。
func RPCTimeout(method string) time.Duration {
	switch method {
	case "Pay", "ClosePayment", "CreateRefund":
		return 5 * time.Second
	case "CreateTransfer":
		return 3 * time.Second
	case "RunBatch", "CheckBalance":
		return 120 * time.Second
	default:
		return DefaultRPCTimeout
	}
}

// NewClients 建三个客户端。这里不拨号，对端没起来也不会失败。
func NewClients() (*Clients, error) {
	payment, err := paymentservice.NewClient("PaymentService", dialOptions(AddrPayment, DefaultRPCTimeout)...)
	if err != nil {
		return nil, err
	}
	account, err := accountservice.NewClient("AccountService", dialOptions(AddrAccount, DefaultRPCTimeout)...)
	if err != nil {
		return nil, err
	}
	recon, err := reconservice.NewClient("ReconService", dialOptions(AddrRecon, DefaultRPCTimeout)...)
	if err != nil {
		return nil, err
	}
	return &Clients{Payment: payment, Account: account, Recon: recon}, nil
}

// dialOptions 只有连接、调用超时、地址和 TTHeader。没有 retry。
func dialOptions(addr string, rpc time.Duration) []client.Option {
	return []client.Option{
		client.WithHostPorts(addr),
		client.WithConnectTimeout(ConnectTimeout),
		client.WithRPCTimeout(rpc),
		client.WithTransportProtocol(transport.TTHeader),
		client.WithMetaHandler(transmeta.ClientTTHeaderHandler),
		client.WithMiddleware(obs.ClientMiddleware("gateway")),
	}
}
