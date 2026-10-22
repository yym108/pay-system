// Package accountclient 是 payment 调用 account 的 Kitex 客户端。
//
// 连接 100ms，调用 500ms。只对幂等方法在超时时重试 1 次。
// 业务错误不重试。OpenAccount 即使进程内幂等也不重试。
// 本包只依赖 kitex_gen，不依赖 service/account，因此 payment 不必 import account 的内部包。
// gateway 不使用本包：网关不重试，超时见需求 §5.4.1，由 GW-01 自己建客户端。
package accountclient

import (
	"context"
	"errors"
	"net"
	"os"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/transmeta"
	"github.com/cloudwego/kitex/transport"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/kitex_gen/account/accountservice"
	"github.com/yym108/pay-system/pkg/errno"
)

const (
	// ConnectTimeout 是 payment → account 的连接超时。
	ConnectTimeout = 100 * time.Millisecond
	// RPCTimeout 是 payment → account 的调用超时。
	RPCTimeout = 500 * time.Millisecond
)

// Client 是带超时和有限重试的 AccountService 客户端。
type Client struct {
	raw accountservice.Client
}

// Option 覆盖默认超时。测试用短超时，生产不要改这两个值。
type Option func(*dialConfig)

type dialConfig struct {
	connect time.Duration
	rpc     time.Duration
}

// WithTimeouts 覆盖连接和调用超时。任一值小于等于 0 时保留默认。
func WithTimeouts(connect, rpc time.Duration) Option {
	return func(cfg *dialConfig) {
		if connect > 0 {
			cfg.connect = connect
		}
		if rpc > 0 {
			cfg.rpc = rpc
		}
	}
}

// Dial 连到 addr。addr 是 host:port，例如 127.0.0.1:8882。
func Dial(addr string, opts ...Option) (*Client, error) {
	if addr == "" {
		return nil, errors.New("account client: empty address")
	}
	cfg := dialConfig{connect: ConnectTimeout, rpc: RPCTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}
	raw, err := accountservice.NewClient("AccountService",
		client.WithHostPorts(addr),
		client.WithConnectTimeout(cfg.connect),
		client.WithRPCTimeout(cfg.rpc),
		// 业务码在 TTHeader 里。不用这个协议时，BizStatusError 到不了调用方。
		client.WithTransportProtocol(transport.TTHeader),
		client.WithMetaHandler(transmeta.ClientTTHeaderHandler),
	)
	if err != nil {
		return nil, err
	}
	return &Client{raw: raw}, nil
}

// Idempotent 报告该方法是否允许超时重试 1 次。
// OpenAccount、CheckBalance、Reverse 和未列出的方法都不重试。
func Idempotent(method string) bool {
	switch method {
	case "Freeze", "ReleaseFreeze", "ConfirmFreeze", "TransferLocal",
		"TransferInTry", "TransferInConfirm", "TransferInCancel", "GetAccount":
		return true
	default:
		return false
	}
}

func (c *Client) OpenAccount(ctx context.Context, req *account.OpenAccountRequest) (*account.Account, error) {
	return call(c, ctx, false, func(ctx context.Context) (*account.Account, error) {
		return c.raw.OpenAccount(ctx, req)
	})
}

func (c *Client) GetAccount(ctx context.Context, req *account.GetAccountRequest) (*account.Account, error) {
	return call(c, ctx, true, func(ctx context.Context) (*account.Account, error) {
		return c.raw.GetAccount(ctx, req)
	})
}

func (c *Client) Freeze(ctx context.Context, req *account.FreezeRequest) (*account.FreezeResponse, error) {
	return call(c, ctx, true, func(ctx context.Context) (*account.FreezeResponse, error) {
		return c.raw.Freeze(ctx, req)
	})
}

func (c *Client) ReleaseFreeze(ctx context.Context, req *account.ReleaseFreezeRequest) (*account.ReleaseFreezeResponse, error) {
	return call(c, ctx, true, func(ctx context.Context) (*account.ReleaseFreezeResponse, error) {
		return c.raw.ReleaseFreeze(ctx, req)
	})
}

func (c *Client) ConfirmFreeze(ctx context.Context, req *account.ConfirmFreezeRequest) (*account.ConfirmFreezeResponse, error) {
	return call(c, ctx, true, func(ctx context.Context) (*account.ConfirmFreezeResponse, error) {
		return c.raw.ConfirmFreeze(ctx, req)
	})
}

func (c *Client) TransferLocal(ctx context.Context, req *account.TransferLocalRequest) (*account.TransferLocalResponse, error) {
	return call(c, ctx, true, func(ctx context.Context) (*account.TransferLocalResponse, error) {
		return c.raw.TransferLocal(ctx, req)
	})
}

func (c *Client) TransferInTry(ctx context.Context, req *account.TransferInTryRequest) (*account.TransferInTryResponse, error) {
	return call(c, ctx, true, func(ctx context.Context) (*account.TransferInTryResponse, error) {
		return c.raw.TransferInTry(ctx, req)
	})
}

func (c *Client) TransferInConfirm(ctx context.Context, req *account.TransferInConfirmRequest) (*account.TransferInConfirmResponse, error) {
	return call(c, ctx, true, func(ctx context.Context) (*account.TransferInConfirmResponse, error) {
		return c.raw.TransferInConfirm(ctx, req)
	})
}

func (c *Client) TransferInCancel(ctx context.Context, req *account.TransferInCancelRequest) (*account.TransferInCancelResponse, error) {
	return call(c, ctx, true, func(ctx context.Context) (*account.TransferInCancelResponse, error) {
		return c.raw.TransferInCancel(ctx, req)
	})
}

func (c *Client) CheckBalance(ctx context.Context, req *account.CheckBalanceRequest) (*account.CheckBalanceResponse, error) {
	return call(c, ctx, false, func(ctx context.Context) (*account.CheckBalanceResponse, error) {
		return c.raw.CheckBalance(ctx, req)
	})
}

func (c *Client) Reverse(ctx context.Context, req *account.ReverseRequest) (*account.ReverseResponse, error) {
	return call(c, ctx, false, func(ctx context.Context) (*account.ReverseResponse, error) {
		return c.raw.Reverse(ctx, req)
	})
}

func call[T any](c *Client, ctx context.Context, retry bool, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	if c == nil || c.raw == nil {
		return zero, errno.ErrInternal
	}
	if ctx == nil {
		return zero, errno.ErrInternal
	}
	out, err := fn(ctx)
	if err == nil || !retry || !shouldRetry(ctx, err) {
		return out, err
	}
	return fn(ctx)
}

// shouldRetry 只在调用方 ctx 仍有效、且错误是超时时为 true。
// 连接超时和调用超时都算。连接拒绝、业务错误、调用方取消都不重试。
func shouldRetry(ctx context.Context, err error) bool {
	if err == nil || ctx == nil || ctx.Err() != nil {
		return false
	}
	return isTimeout(err)
}

// isTimeout 识别客户端自己的超时，以及服务端把处理超时包成远程错误的情况。
// 服务端 2 秒截止时间返回的不是 BizStatusError。Kitex 可能把它包成 remote error，
// 但内层仍是 rpc timeout。这种结果未知，幂等方法要重试。
func isTimeout(err error) bool {
	if kerrors.IsTimeoutError(err) || errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var timed interface{ Timeout() bool }
	if errors.As(err, &timed) && timed.Timeout() {
		return true
	}
	// 服务端把 ErrRPCTimeout 写成 Thrift 异常后，客户端看到的是 TransError，消息仍是 rpc timeout。
	// 这不是业务码。结果未知，幂等方法要重试。
	var trans *remote.TransError
	return errors.As(err, &trans) && trans != nil && trans.Error() == kerrors.ErrRPCTimeout.Error()
}
