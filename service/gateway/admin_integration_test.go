//go:build integration

package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/transmeta"
	kitexserver "github.com/cloudwego/kitex/server"
	"github.com/cloudwego/kitex/transport"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/kitex_gen/account/accountservice"
	"github.com/yym108/pay-system/kitex_gen/base"
	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/kitex_gen/payment/paymentservice"
	"github.com/yym108/pay-system/kitex_gen/recon"
	"github.com/yym108/pay-system/kitex_gen/recon/reconservice"
	"github.com/yym108/pay-system/pkg/errno"
)

type gw07PaymentRPC struct {
	payment.PaymentService
}

func (s *gw07PaymentRPC) ReplayEvent(_ context.Context, req *payment.ReplayEventRequest) (*payment.ReplayEventResponse, error) {
	if req.GetEventId() == "EV_MISSING" {
		return nil, kerrors.NewBizStatusError(errno.ErrNotFound.Code, errno.ErrNotFound.Msg)
	}
	return &payment.ReplayEventResponse{EventId: req.GetEventId(), Rebuilt: false}, nil
}

type gw07AccountRPC struct {
	account.AccountService
}

func (s *gw07AccountRPC) CheckBalance(_ context.Context, _ *account.CheckBalanceRequest) (*account.CheckBalanceResponse, error) {
	return &account.CheckBalanceResponse{Passed: true, Failures: nil}, nil
}

type gw07ReconRPC struct{}

func (s *gw07ReconRPC) RunBatch(_ context.Context, req *recon.RunBatchRequest) (*recon.RunBatchResponse, error) {
	if req.Channel != "mockpay" {
		return nil, kerrors.NewBizStatusError(errno.ErrInvalidParam.Code, errno.ErrInvalidParam.Msg)
	}
	ts := base.Timestamp("2026-10-27T10:00:00.000+08:00")
	mt := int32(1)
	return &recon.RunBatchResponse{
		Batch: &recon.ReconBatch{
			BatchNo:    "RB2026102701",
			Channel:    req.Channel,
			BillDate:   req.BillDate,
			Status:     "DONE",
			StartedAt:  &ts,
			FinishedAt: &ts,
		},
		Diffs:       []*recon.ReconDiff{},
		ManualTotal: &mt,
		ManualDiffs: []*recon.ReconDiff{{
			DiffNo:    "RD01",
			BatchNo:   "RB2026102701",
			Channel:   "mockpay",
			BillDate:  "2026-10-27",
			DiffType:  "LOCAL_MISSING",
			TxnType:   "PAY",
			BizNo:     "PA01",
			Status:    "PENDING",
			CreatedAt: ts,
		}},
	}, nil
}

func startKitexTestServer(t *testing.T, makeSrv func(net.Addr) kitexserver.Server) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := makeSrv(tcpAddr)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	waitListen(t, addr)
	t.Cleanup(func() {
		_ = srv.Stop()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
		}
	})
	return addr
}

// GW-07 集成测试：管理端口通过真实 Kitex TTHeader RPC 调用三个后端服务，校验 Token、操作人与审计日志。
func TestGW07AdminRPCRoundTripIntegration(t *testing.T) {
	payAddr := startKitexTestServer(t, func(addr net.Addr) kitexserver.Server {
		return paymentservice.NewServer(&gw07PaymentRPC{},
			kitexserver.WithServiceAddr(addr),
			kitexserver.WithMetaHandler(transmeta.ServerTTHeaderHandler),
		)
	})
	accAddr := startKitexTestServer(t, func(addr net.Addr) kitexserver.Server {
		return accountservice.NewServer(&gw07AccountRPC{},
			kitexserver.WithServiceAddr(addr),
			kitexserver.WithMetaHandler(transmeta.ServerTTHeaderHandler),
		)
	})
	recAddr := startKitexTestServer(t, func(addr net.Addr) kitexserver.Server {
		return reconservice.NewServer(&gw07ReconRPC{},
			kitexserver.WithServiceAddr(addr),
			kitexserver.WithMetaHandler(transmeta.ServerTTHeaderHandler),
		)
	})

	payCli, err := paymentservice.NewClient("PaymentService",
		client.WithHostPorts(payAddr),
		client.WithTransportProtocol(transport.TTHeader),
		client.WithMetaHandler(transmeta.ClientTTHeaderHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	accCli, err := accountservice.NewClient("AccountService",
		client.WithHostPorts(accAddr),
		client.WithTransportProtocol(transport.TTHeader),
		client.WithMetaHandler(transmeta.ClientTTHeaderHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	recCli, err := reconservice.NewClient("ReconService",
		client.WithHostPorts(recAddr),
		client.WithTransportProtocol(transport.TTHeader),
		client.WithMetaHandler(transmeta.ClientTTHeaderHandler),
	)
	if err != nil {
		t.Fatal(err)
	}

	logs := newLogBuf()
	admin := startGW(t, NewAdmin(
		WithAdminToken(testAdminToken),
		WithReplayAPI(payCli),
		WithCheckBalanceAPI(accCli),
		WithReconAPI(recCli),
		WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	), nil)

	// 1. 重放成功 + TTHeader BizStatusError 透传
	got := do(t, http.MethodPost, "http://"+admin+"/admin/outbox/replay", []byte(`{"event_id":"EV_OK"}`), adminHeaders(testAdminToken, "ops-it"))
	env := decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 {
		t.Fatalf("replay ok: status %d env %+v", got.status, env)
	}
	got = do(t, http.MethodPost, "http://"+admin+"/admin/outbox/replay", []byte(`{"event_id":"EV_MISSING"}`), adminHeaders(testAdminToken, "ops-it"))
	env = decodeEnv(t, got.body)
	if got.status != http.StatusNotFound || env.Code != errno.ErrNotFound.Code {
		t.Fatalf("replay 10002: status %d env %+v", got.status, env)
	}

	// 2. 余额核对成功
	got = do(t, http.MethodPost, "http://"+admin+"/admin/accounts/check", []byte(`{}`), adminHeaders(testAdminToken, ""))
	env = decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 {
		t.Fatalf("check ok: status %d env %+v", got.status, env)
	}

	// 3. 触发对账成功
	got = do(t, http.MethodPost, "http://"+admin+"/admin/recon/run", []byte(`{"channel":"mockpay","bill_date":"2026-10-27"}`), adminHeaders(testAdminToken, "ops-it"))
	env = decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 {
		t.Fatalf("recon ok: status %d env %+v", got.status, env)
	}
	var reconData struct {
		ManualTotal int32              `json:"manual_total"`
		ManualDiffs []*recon.ReconDiff `json:"manual_diffs"`
	}
	if err := json.Unmarshal(env.Data, &reconData); err != nil {
		t.Fatal(err)
	}
	if reconData.ManualTotal != 1 || len(reconData.ManualDiffs) != 1 {
		t.Fatalf("reconData = %+v", reconData)
	}

	audits := collectAuditLogs(t, logs.String())
	if len(audits) != 3 {
		t.Fatalf("audit count = %d, want 3", len(audits))
	}
}
