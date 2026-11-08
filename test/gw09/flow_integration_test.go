//go:build integration

package gw09

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/kitex/client/callopt"
	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
	accountbiz "github.com/yym108/pay-system/service/account/biz"
	accountdal "github.com/yym108/pay-system/service/account/dal"
	cbiz "github.com/yym108/pay-system/service/channel_mock/biz"
	cdal "github.com/yym108/pay-system/service/channel_mock/dal"
	chandler "github.com/yym108/pay-system/service/channel_mock/handler"
	"github.com/yym108/pay-system/service/gateway"
	paybiz "github.com/yym108/pay-system/service/payment/biz"
	payhandler "github.com/yym108/pay-system/service/payment/handler"
)

const (
	gw09AppID      = "app_m100001"
	gw09Secret     = "gw09-app-secret-0123456789abcdef" //nolint:gosec // 测试密钥，不是生产配置。
	gw09ChannelKey = "channel-key-0123456789abcdef"     //nolint:gosec // 与渠道回调测试同一把测试密钥。
	gw09Payer      = "M100001"
	gw09PayeeLocal = "M100003"
	gw09PayeeCross = "M100002"
	gw09Seed       = money.Amount(10000)
)

// payAPI 把已导出的 payment handler 装进网关。callopt 是网关调用超时，进程内忽略。
type payAPI struct {
	h *payhandler.Handler
}

func (a payAPI) CreatePayment(ctx context.Context, req *payment.CreatePaymentRequest, _ ...callopt.Option) (*payment.PaymentOrder, error) {
	return a.h.CreatePayment(ctx, req)
}

func (a payAPI) Pay(ctx context.Context, req *payment.PayRequest, _ ...callopt.Option) (*payment.PayResponse, error) {
	return a.h.Pay(ctx, req)
}

func (a payAPI) ClosePayment(ctx context.Context, req *payment.ClosePaymentRequest, _ ...callopt.Option) (*payment.ClosePaymentResponse, error) {
	return a.h.ClosePayment(ctx, req)
}

func (a payAPI) QueryPayment(ctx context.Context, req *payment.QueryPaymentRequest, _ ...callopt.Option) (*payment.PaymentOrder, error) {
	return a.h.QueryPayment(ctx, req)
}

func (a payAPI) CreateRefund(ctx context.Context, req *payment.CreateRefundRequest, _ ...callopt.Option) (*payment.CreateRefundResponse, error) {
	return a.h.CreateRefund(ctx, req)
}

func (a payAPI) QueryRefund(ctx context.Context, req *payment.QueryRefundRequest, _ ...callopt.Option) (*payment.RefundOrder, error) {
	return a.h.QueryRefund(ctx, req)
}

func (a payAPI) CreateTransfer(ctx context.Context, req *payment.CreateTransferRequest, _ ...callopt.Option) (*payment.CreateTransferResponse, error) {
	return a.h.CreateTransfer(ctx, req)
}

func (a payAPI) QueryTransfer(ctx context.Context, req *payment.QueryTransferRequest, _ ...callopt.Option) (*payment.Transfer, error) {
	return a.h.QueryTransfer(ctx, req)
}

// notifyAPI 把渠道回调交给同一个 handler。不起 Kitex。
type notifyAPI struct {
	h *payhandler.Handler
}

func (a notifyAPI) HandleChannelNotify(ctx context.Context, req *payment.HandleChannelNotifyRequest, _ ...callopt.Option) (*payment.HandleChannelNotifyResponse, error) {
	return a.h.HandleChannelNotify(ctx, req)
}

type gw09Lookup struct{}

func (gw09Lookup) Lookup(_ context.Context, appID string) (gateway.Credential, error) {
	if appID != gw09AppID {
		return gateway.Credential{}, errno.ErrMerchantInvalid
	}
	return gateway.Credential{MerchantID: gw09Payer, Status: "ACTIVE", Secret: []byte(gw09Secret)}, nil
}

// refundFreezer 直接调 account biz 冻结。M4 没有 MQ-04，支付成功不会给商户入账。
type refundFreezer struct {
	svc *accountbiz.Service
}

func (f refundFreezer) FreezeRefund(ctx context.Context, merchantID, refundNo string, amount money.Amount) (paybiz.RefundFreeze, error) {
	res, err := f.svc.Freeze(ctx, "REFUND", refundNo, merchantID, amount)
	if err != nil {
		return paybiz.RefundFreeze{}, err
	}
	return paybiz.RefundFreeze{Status: res.Status}, nil
}

type localTransfer struct {
	svc *accountbiz.Service
}

func (a localTransfer) TransferLocal(ctx context.Context, transferNo, payer, payee string, amount money.Amount) (paybiz.LocalTransferResult, error) {
	res, err := a.svc.TransferLocal(ctx, transferNo, payer, payee, amount)
	if err != nil {
		return paybiz.LocalTransferResult{}, err
	}
	return paybiz.LocalTransferResult{Status: res.Status, TxnNo: res.TxnNo, FailCode: res.FailCode}, nil
}

type tccTransfer struct {
	svc *accountbiz.Service
}

func (a tccTransfer) Freeze(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	_, err := a.svc.Freeze(ctx, "TRANSFER", transferNo, payer, amount)
	return err
}

func (a tccTransfer) TryPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	_, err := a.svc.TransferInTry(ctx, transferNo, payee, amount)
	return err
}

func (a tccTransfer) ConfirmPayer(ctx context.Context, transferNo, payer string) error {
	_, err := a.svc.ConfirmFreeze(ctx, "TRANSFER", transferNo, payer, "")
	return err
}

func (a tccTransfer) ConfirmPayee(ctx context.Context, transferNo, payee string) error {
	_, err := a.svc.TransferInConfirm(ctx, transferNo, payee)
	return err
}

func (a tccTransfer) CancelPayer(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	_, err := a.svc.ReleaseFreeze(ctx, "TRANSFER", transferNo, payer, amount)
	return err
}

func (a tccTransfer) CancelPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	_, err := a.svc.TransferInCancel(ctx, transferNo, payee, amount)
	return err
}

// GW-09 验收 3：paycli 经网关完成下单、支付、查询、退款、查询退款、同分片转账、跨分片转账、查询转账。每一步退出码 0，响应签名由 paycli 自己校验。
func TestGW09PaycliFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	payDB := openSchema(t, ctx, my, "payment")
	acc0 := openSchema(t, ctx, my, "account")
	acc1 := openSchema(t, ctx, my, "account")
	clk := clock.Real{}
	insertMerchants(t, ctx, payDB.DB)
	fees := paybiz.New(payDB.DB)
	for _, id := range []string{gw09Payer, gw09PayeeLocal, gw09PayeeCross} {
		if _, err = fees.AppendFeeRate(ctx, id, 60, clk.Now().Add(-time.Hour)); err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
	payIDs, err := idgen.New(21, clk)
	if err != nil {
		t.Fatal(err)
	}
	merchants := paybiz.NewMerchants(payDB.DB, nil, clk, 0)
	payments := paybiz.NewPayments(merchants, payIDs, clk, "", payDB.DB)
	accounts := openBooks(t, ctx, clk, acc0.DB, acc1.DB)
	fundPayer(t, ctx, acc1.DB, accounts)
	handler := payhandler.New(paybiz.NewApp(merchants, payments))
	// 先监听再装配，再 Spin。go 语句发生在写入之后，Pay 读到的渠道和转账执行器不再与测试协程抢同一字段。
	base, serve := listenGateway(t, ctx, handler)
	channelURL := startChannel(t, ctx, clk)
	client, err := paybiz.NewChannelClient(paybiz.ChannelClientConfig{
		BaseURL: channelURL, Key: []byte(gw09ChannelKey), Clock: clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	callbackURL := base + "/callback/mockpay"
	payments.UsePay(client, payments, callbackURL)
	payments.UseRefund(refundFreezer{svc: accounts}, client, nil, callbackURL)
	payments.UseLocalTransfer(localTransfer{svc: accounts})
	payments.UseTCCTransfer(tccTransfer{svc: accounts}, false)
	serve()
	bin := buildPaycli(t, ctx)

	created := runPaycli(t, ctx, bin, base, "create", "--out-trade-no", "GW09P1", "--amount", "1000", "--subject", "gw09")
	payNo := strField(t, created, "pay_no")
	runPaycli(t, ctx, bin, base, "pay", "--pay-no", payNo, "--channel", "mockpay")
	waitPaymentSuccess(t, ctx, bin, base, payNo)

	refunded := runPaycli(t, ctx, bin, base, "refund", "--pay-no", payNo, "--out-refund-no", "GW09R1", "--amount", "100")
	refundNo := strField(t, refunded, "refund_no")
	queriedRefund := runPaycli(t, ctx, bin, base, "query-refund", "--refund-no", refundNo)
	if strField(t, queriedRefund, "refund_no") != refundNo || strField(t, queriedRefund, "out_refund_no") != "GW09R1" {
		t.Fatalf("refund query %#v", queriedRefund)
	}

	local := runPaycli(t, ctx, bin, base, "transfer", "--out-transfer-no", "GW09L1", "--payee-merchant-id", gw09PayeeLocal, "--amount", "200")
	localNo := strField(t, local, "transfer_no")
	localQuery := runPaycli(t, ctx, bin, base, "query-transfer", "--transfer-no", localNo)
	if strField(t, localQuery, "mode") != "LOCAL" || strField(t, localQuery, "merchant_id") != gw09Payer || strField(t, localQuery, "status") != "SUCCESS" {
		t.Fatalf("local transfer %#v", localQuery)
	}

	cross := runPaycli(t, ctx, bin, base, "transfer", "--out-transfer-no", "GW09C1", "--payee-merchant-id", gw09PayeeCross, "--amount", "300")
	crossNo := strField(t, cross, "transfer_no")
	crossQuery := runPaycli(t, ctx, bin, base, "query-transfer", "--transfer-no", crossNo)
	if strField(t, crossQuery, "mode") != "TCC" || strField(t, crossQuery, "merchant_id") != gw09Payer || strField(t, crossQuery, "status") != "SUCCESS" {
		t.Fatalf("cross transfer %#v", crossQuery)
	}

	// 退款冻结没有 MQ-04 解冻。两个分片一次全量核对，expect_quiescent 为 false。
	checked, err := accounts.CheckBalance(ctx, "", false)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if !checked.Passed {
		t.Fatalf("CheckBalance %+v", checked.Failures)
	}
}

func openSchema(t *testing.T, ctx context.Context, my *testkit.MySQL, kind string) *testkit.Schema {
	t.Helper()
	schema, err := my.OpenSchema(ctx, kind)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	t.Cleanup(func() { //nolint:contextcheck // 清理发生在测试 ctx 取消之后。
		closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := schema.Close(closeCtx); err != nil {
			t.Errorf("close schema: %s", testkit.Redact(err))
		}
	})
	return schema
}

func insertMerchants(t *testing.T, ctx context.Context, db *sqlx.DB) {
	t.Helper()
	for _, id := range []string{gw09Payer, gw09PayeeLocal, gw09PayeeCross} {
		_, err := db.ExecContext(ctx, `
INSERT INTO merchant (
  merchant_id, merchant_name, app_id, app_secret_cipher, fee_rate_bp, status
) VALUES (?, 'demo', ?, ?, 60, 'ACTIVE')`,
			id, "app_"+strings.ToLower(id), []byte("cipher"))
		if err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
}

func openBooks(t *testing.T, ctx context.Context, clk clock.Real, db0, db1 *sqlx.DB) *accountbiz.Service {
	t.Helper()
	for _, id := range []string{gw09Payer, gw09PayeeLocal, gw09PayeeCross} {
		got, err := shard.Of(id)
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if id != gw09PayeeCross {
			want = 1
		}
		if got != want {
			t.Fatalf("%s shard %d", id, got)
		}
	}
	ids, err := idgen.New(22, clk)
	if err != nil {
		t.Fatal(err)
	}
	svc := accountbiz.New(accountdal.New(db0, db1), ids, 8)
	for _, shardNo := range []int{0, 1} {
		for sub := 0; sub < 8; sub++ {
			if _, err = svc.OpenAccount(ctx, "PLATFORM", "PLATFORM", "TRANSIT", shardNo, sub); err != nil {
				t.Fatal(testkit.Redact(err))
			}
		}
		if _, err = svc.OpenAccount(ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", shardNo, 0); err != nil {
			t.Fatal(testkit.Redact(err))
		}
	}
	if _, err = svc.OpenAccount(ctx, "MERCHANT", gw09Payer, "MERCHANT_SETTLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err = svc.OpenAccount(ctx, "MERCHANT", gw09PayeeLocal, "MERCHANT_SETTLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err = svc.OpenAccount(ctx, "MERCHANT", gw09PayeeCross, "MERCHANT_SETTLE", 0, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return svc
}

func fundPayer(t *testing.T, ctx context.Context, db *sqlx.DB, svc *accountbiz.Service) {
	t.Helper()
	merchantID := accountID(t, ctx, db, "MERCHANT", gw09Payer, "MERCHANT_SETTLE")
	channelID := accountID(t, ctx, db, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE")
	err := dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		_, postErr := svc.Post(ctx, tx, "PAY", "GW09-SEED", "", []accountbiz.Entry{
			{AccountID: channelID, Direction: "DEBIT", Amount: gw09Seed},
			{AccountID: merchantID, Direction: "CREDIT", Amount: gw09Seed},
		})
		return postErr
	})
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

func accountID(t *testing.T, ctx context.Context, db *sqlx.DB, ownerType, ownerID, accountType string) uint64 {
	t.Helper()
	var id uint64
	err := db.QueryRowContext(ctx, `
SELECT id FROM account
WHERE owner_type = ? AND owner_id = ? AND account_type = ? AND sub_no = 0`,
		ownerType, ownerID, accountType).Scan(&id)
	if err != nil || id == 0 {
		t.Fatalf("account id %s/%s: %v", ownerType, ownerID, err)
	}
	return id
}

func listenGateway(t *testing.T, ctx context.Context, h *payhandler.Handler) (base string, serve func()) {
	t.Helper()
	ln, err := gateway.Listen(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := gateway.NewEngine(ln)
	gateway.NewMerchant(
		gateway.WithClock(clock.Real{}),
		gateway.WithMerchantLookup(gw09Lookup{}),
		gateway.WithPaymentAPI(payAPI{h: h}),
		gateway.WithNotifyAPI(notifyAPI{h: h}),
		gateway.WithChannelKey([]byte(gw09ChannelKey)),
	).Register(srv)
	return "http://" + ln.Addr().String(), func() { //nolint:contextcheck // 清理发生在测试 ctx 取消之后。
		runCtx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- gateway.Spin(runCtx, srv) }()
		t.Cleanup(func() {
			cancel()
			// ExitWaitTime 是 10 秒。空闲连接排空可能用满这段时间。
			select {
			case <-errCh:
			case <-time.After(12 * time.Second):
				t.Error("gateway did not exit")
			}
		})
		waitDial(t, ln.Addr().String())
	}
}

func startChannel(t *testing.T, ctx context.Context, clk clock.Real) string {
	t.Helper()
	my, err := testkit.Ready(ctx)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	schema := openSchema(t, ctx, my, "channel_mock")
	ids, err := idgen.New(23, clk)
	if err != nil {
		t.Fatal(err)
	}
	store := cdal.New(schema.DB)
	sched := cbiz.NewTimerScheduler(clk)
	notifier := cbiz.NewNotifier(store, ids, clk, sched, []byte(gw09ChannelKey))
	svc := cbiz.New(store, ids, clk, sched, cbiz.Options{
		DelayMinMS: 50,
		DelayMaxMS: 50,
		OnFinished: notifier.Handle,
	})
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithExitWaitTime(time.Second),
		server.WithDisablePrintRoute(true),
	)
	chandler.New(svc, []byte(gw09ChannelKey), clk).Register(srv)
	go func() { _ = srv.Run() }()
	waitDial(t, ln.Addr().String()) //nolint:contextcheck // 与 waitListen 相同，拨号超时从 Background 派生，不占用测试 ctx。
	t.Cleanup(func() {              //nolint:contextcheck // 清理发生在测试 ctx 取消之后。
		shut, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := srv.Shutdown(shut); err != nil {
			t.Errorf("shutdown channel http: %v", err)
		}
	})
	t.Cleanup(sched.Stop)
	return "http://" + ln.Addr().String()
}

func waitDial(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var last error
	for ctx.Err() == nil {
		dialer := &net.Dialer{Timeout: 50 * time.Millisecond}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		last = err
	}
	t.Fatalf("%s not listening: %v", addr, last)
}

func buildPaycli(t *testing.T, ctx context.Context) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "paycli")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/paycli") //nolint:gosec // bin 在临时目录，参数是本测试的固定命令，不是外部输入。
	cmd.Dir = moduleRoot(t)
	cmd.Env = paycliEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build paycli: %v\n%s", err, out)
	}
	return bin
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func paycliEnv() []string {
	var out []string
	for _, item := range os.Environ() {
		if strings.HasPrefix(item, "PAYCLI_") {
			continue
		}
		out = append(out, item)
	}
	return out
}

func runPaycli(t *testing.T, ctx context.Context, bin, base string, args ...string) map[string]any {
	t.Helper()
	full := append([]string{"--gateway", base, "--app-id", gw09AppID, "--app-secret", gw09Secret}, args...)
	cmd := exec.CommandContext(ctx, bin, full...) //nolint:gosec // bin 是刚编译出的 paycli，参数是本测试的固定命令，不是外部输入。
	cmd.Env = paycliEnv()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("paycli %v: %v\nstderr %s\nstdout %s", args, err, stderr.String(), stdout.String())
	}
	var env struct {
		Code int32           `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout %s: %v", stdout.String(), err)
	}
	if env.Code != 0 {
		t.Fatalf("code %d body %s", env.Code, stdout.String())
	}
	var data map[string]any
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("data %s: %v", stdout.String(), err)
	}
	return data
}

func waitPaymentSuccess(t *testing.T, ctx context.Context, bin, base, payNo string) {
	t.Helper()
	clk := clock.Real{}
	deadline := clk.Now().Add(5 * time.Second)
	var last map[string]any
	for {
		last = runPaycli(t, ctx, bin, base, "query", "--pay-no", payNo)
		status, _ := last["status"].(string)
		if status == "SUCCESS" {
			return
		}
		if status == "FAILED" || status == "CLOSED" {
			t.Fatalf("payment %s: %#v", status, last)
		}
		if !clk.Now().Before(deadline) {
			t.Fatalf("payment not SUCCESS: %#v", last)
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
}

func strField(t *testing.T, data map[string]any, key string) string {
	t.Helper()
	v, ok := data[key].(string)
	if !ok || v == "" {
		t.Fatalf("%s = %#v in %#v", key, data[key], data)
	}
	return v
}
