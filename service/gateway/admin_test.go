package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/kitex_gen/base"
	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/kitex_gen/recon"
	"github.com/yym108/pay-system/pkg/errno"
)

const testAdminToken = "0123456789abcdef0123456789abcdef" //nolint:gosec // G101: 测试用管理令牌。

type checkBalanceFake struct {
	mu    sync.Mutex
	calls int
	req   *account.CheckBalanceRequest
	resp  *account.CheckBalanceResponse
	err   error
}

func (f *checkBalanceFake) CheckBalance(_ context.Context, req *account.CheckBalanceRequest, _ ...callopt.Option) (*account.CheckBalanceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *checkBalanceFake) lastReq() *account.CheckBalanceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.req
}

func (f *checkBalanceFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *checkBalanceFake) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

type reconFake struct {
	mu    sync.Mutex
	calls int
	req   *recon.RunBatchRequest
	resp  *recon.RunBatchResponse
	err   error
}

func (f *reconFake) RunBatch(_ context.Context, req *recon.RunBatchRequest, _ ...callopt.Option) (*recon.RunBatchResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *reconFake) lastReq() *recon.RunBatchRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.req
}

func (f *reconFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *reconFake) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func adminHeaders(token, operator string) http.Header {
	hdr := make(http.Header)
	if token != "" {
		hdr.Set("X-Admin-Token", token)
	}
	if operator != "" {
		hdr.Set("X-Admin-Operator", operator)
	}
	return hdr
}

// GW-07 验收 1：无 Token、错误 Token 返回 401。
func TestGW07AdminTokenAuth(t *testing.T) {
	replay := &replayFake{resp: &payment.ReplayEventResponse{EventId: "EV1", Rebuilt: false}}
	check := &checkBalanceFake{resp: &account.CheckBalanceResponse{Passed: true}}
	rc := &reconFake{resp: &recon.RunBatchResponse{Batch: &recon.ReconBatch{BatchNo: "RB1"}}}

	logs := newLogBuf()
	admin := startGW(t, NewAdmin(
		WithAdminToken(testAdminToken),
		WithReplayAPI(replay),
		WithCheckBalanceAPI(check),
		WithReconAPI(rc),
		WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	), nil)

	endpoints := []struct {
		path string
		body []byte
	}{
		{"/admin/outbox/replay", []byte(`{"event_id":"EV1"}`)},
		{"/admin/accounts/check", []byte(`{}`)},
		{"/admin/recon/run", []byte(`{"channel":"mockpay","bill_date":"2026-10-27"}`)},
	}

	for _, ep := range endpoints {
		// 无 Token
		got := do(t, http.MethodPost, "http://"+admin+ep.path, ep.body, adminHeaders("", "alice"))
		env := decodeEnv(t, got.body)
		if got.status != http.StatusUnauthorized || env.Code != errno.ErrAdminUnauthorized.Code || signed(got.header) {
			t.Fatalf("%s missing token: status %d env %+v signed %v", ep.path, got.status, env, signed(got.header))
		}
		// 错误 Token（同长度和不同长度）
		for _, bad := range []string{"wrong", "0123456789abcdef0123456789abcde0"} {
			got = do(t, http.MethodPost, "http://"+admin+ep.path, ep.body, adminHeaders(bad, "alice"))
			env = decodeEnv(t, got.body)
			if got.status != http.StatusUnauthorized || env.Code != errno.ErrAdminUnauthorized.Code {
				t.Fatalf("%s bad token %q: status %d env %+v", ep.path, bad, got.status, env)
			}
		}
	}

	// 未配置 Token 时也拒绝（fail-closed）。
	noTokenAdmin := startGW(t, NewAdmin(WithCheckBalanceAPI(check)), nil)
	got := do(t, http.MethodPost, "http://"+noTokenAdmin+"/admin/accounts/check", []byte(`{}`), adminHeaders(testAdminToken, ""))
	env := decodeEnv(t, got.body)
	if got.status != http.StatusUnauthorized || env.Code != errno.ErrAdminUnauthorized.Code {
		t.Fatalf("empty server token: status %d env %+v", got.status, env)
	}

	if replay.calls != 0 || check.callCount() != 0 || rc.callCount() != 0 {
		t.Fatalf("unauthorized requests called rpc: replay=%d check=%d recon=%d", replay.calls, check.callCount(), rc.callCount())
	}
	if strings.Contains(logs.String(), testAdminToken) {
		t.Fatal("admin token leaked into logs")
	}
}

// GW-07 验收 2：写操作缺少 X-Admin-Operator 返回 10001；只读核对不需要操作人。
func TestGW07WriteRequiresOperator(t *testing.T) {
	replay := &replayFake{resp: &payment.ReplayEventResponse{EventId: "EV1", Rebuilt: false}}
	check := &checkBalanceFake{resp: &account.CheckBalanceResponse{Passed: true}}
	rc := &reconFake{resp: &recon.RunBatchResponse{Batch: &recon.ReconBatch{BatchNo: "RB1"}}}

	admin := startGW(t, NewAdmin(
		WithAdminToken(testAdminToken),
		WithReplayAPI(replay),
		WithCheckBalanceAPI(check),
		WithReconAPI(rc),
	), nil)

	writeEndpoints := []struct {
		path string
		body []byte
	}{
		{"/admin/outbox/replay", []byte(`{"event_id":"EV1"}`)},
		{"/admin/recon/run", []byte(`{"channel":"mockpay","bill_date":"2026-10-27"}`)},
	}

	tooLong := strings.Repeat("a", 65)
	for _, ep := range writeEndpoints {
		for _, op := range []string{"", "   ", tooLong} {
			got := do(t, http.MethodPost, "http://"+admin+ep.path, ep.body, adminHeaders(testAdminToken, op))
			env := decodeEnv(t, got.body)
			if got.status != http.StatusBadRequest || env.Code != errno.ErrInvalidParam.Code {
				t.Fatalf("%s operator %q: status %d env %+v", ep.path, op, got.status, env)
			}
		}
	}
	if replay.calls != 0 || rc.callCount() != 0 {
		t.Fatalf("missing operator called rpc: replay=%d recon=%d", replay.calls, rc.callCount())
	}

	// 只读接口不传 X-Admin-Operator 正常成功。
	got := do(t, http.MethodPost, "http://"+admin+"/admin/accounts/check", []byte(`{}`), adminHeaders(testAdminToken, ""))
	env := decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 || check.callCount() != 1 {
		t.Fatalf("read-only check without operator: status %d env %+v calls=%d", got.status, env, check.callCount())
	}
}

// GW-07 验收 3：三个接口各调用一次成功；每次写操作产生一条审计日志。
func TestGW07ThreeEndpointsAndAuditLog(t *testing.T) {
	replay := &replayFake{resp: &payment.ReplayEventResponse{EventId: "EV99", Rebuilt: true}}
	check := &checkBalanceFake{resp: &account.CheckBalanceResponse{Passed: true}}
	startedAt := base.Timestamp("2026-10-27T10:00:00.000+08:00")
	finishedAt := base.Timestamp("2026-10-27T10:00:05.000+08:00")
	rc := &reconFake{resp: &recon.RunBatchResponse{
		Batch: &recon.ReconBatch{
			BatchNo:       "RB20261027001",
			Channel:       "mockpay",
			BillDate:      "2026-10-27",
			Status:        "DONE",
			ChannelCount:  2,
			ChannelAmount: 20000,
			LocalCount:    2,
			LocalAmount:   20000,
			MatchedCount:  1,
			DiffCount:     1,
			ErrorMsg:      "",
			StartedAt:     &startedAt,
			FinishedAt:    &finishedAt,
		},
		Diffs: []*recon.ReconDiff{{
			DiffNo:    "RD001",
			BatchNo:   "RB20261027001",
			Channel:   "mockpay",
			BillDate:  "2026-10-27",
			DiffType:  "AMOUNT_MISMATCH",
			TxnType:   "PAY",
			BizNo:     "PA001",
			Status:    "PENDING",
			Handler:   "",
			CreatedAt: finishedAt,
		}},
		// 故意省略 ManualTotal / ManualDiffs，验证网关缺省补 0 和 []。
	}}

	logs := newLogBuf()
	admin := startGW(t, NewAdmin(
		WithAdminToken(testAdminToken),
		WithReplayAPI(replay),
		WithCheckBalanceAPI(check),
		WithReconAPI(rc),
		WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	), nil)
	merchant := startGW(t, NewMerchant(), nil)

	// 1. 重放（写操作）
	got := do(t, http.MethodPost, "http://"+admin+"/admin/outbox/replay", []byte(`{"event_id":"EV99"}`), adminHeaders(testAdminToken, "ops-alice"))
	env := decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 || signed(got.header) {
		t.Fatalf("replay: status %d env %+v", got.status, env)
	}
	var replayData struct {
		EventID string `json:"event_id"`
		Rebuilt bool   `json:"rebuilt"`
	}
	if err := json.Unmarshal(env.Data, &replayData); err != nil || replayData.EventID != "EV99" || !replayData.Rebuilt {
		t.Fatalf("replay data %s err %v", env.Data, err)
	}

	// 2. 余额核对（只读）
	got = do(t, http.MethodPost, "http://"+admin+"/admin/accounts/check", []byte(`{"merchant_id":"M100001","expect_quiescent":true}`), adminHeaders(testAdminToken, ""))
	env = decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 || signed(got.header) {
		t.Fatalf("check: status %d env %+v", got.status, env)
	}
	if checkReq := check.lastReq(); checkReq == nil || checkReq.GetMerchantId() != "M100001" || !checkReq.GetExpectQuiescent() {
		t.Fatalf("check req %+v", checkReq)
	}
	if !strings.Contains(string(env.Data), `"passed":true`) || !strings.Contains(string(env.Data), `"failures":[]`) {
		t.Fatalf("check data %s", env.Data)
	}

	// 3. 触发对账（写操作）
	got = do(t, http.MethodPost, "http://"+admin+"/admin/recon/run", []byte(`{"channel":"mockpay","bill_date":"2026-10-27","force":true}`), adminHeaders(testAdminToken, "ops-bob"))
	env = decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 || signed(got.header) {
		t.Fatalf("recon: status %d env %+v", got.status, env)
	}
	if rcReq := rc.lastReq(); rcReq == nil || rcReq.Channel != "mockpay" || rcReq.BillDate != "2026-10-27" || !rcReq.GetForce() {
		t.Fatalf("recon req %+v", rcReq)
	}
	var reconData struct {
		Batch       *recon.ReconBatch  `json:"batch"`
		Diffs       []*recon.ReconDiff `json:"diffs"`
		ManualTotal *int32             `json:"manual_total"`
		ManualDiffs []*recon.ReconDiff `json:"manual_diffs"`
	}
	if err := json.Unmarshal(env.Data, &reconData); err != nil {
		t.Fatalf("unmarshal recon data: %v (%s)", err, env.Data)
	}
	if reconData.Batch == nil || reconData.Batch.BatchNo != "RB20261027001" || len(reconData.Diffs) != 1 {
		t.Fatalf("recon data %+v", reconData)
	}
	if reconData.ManualTotal == nil || *reconData.ManualTotal != 0 || reconData.ManualDiffs == nil || len(reconData.ManualDiffs) != 0 {
		t.Fatalf("recon manual defaults %+v", reconData)
	}

	// 校验审计日志：两次写操作各产生一条审计日志，只读核对不产生审计日志。
	auditLines := collectAuditLogs(t, logs.String())
	if len(auditLines) != 2 {
		t.Fatalf("audit logs count = %d, want 2; logs:\n%s", len(auditLines), logs.String())
	}
	if auditLines[0]["operator"] != "ops-alice" || auditLines[0]["path"] != "/admin/outbox/replay" || auditLines[0]["result"] != "ok" {
		t.Fatalf("audit[0] = %+v", auditLines[0])
	}
	if auditLines[0]["params"] == "" {
		t.Fatalf("audit[0] params empty: %+v", auditLines[0])
	}
	if auditLines[1]["operator"] != "ops-bob" || auditLines[1]["path"] != "/admin/recon/run" || auditLines[1]["result"] != "ok" {
		t.Fatalf("audit[1] = %+v", auditLines[1])
	}
	if auditLines[1]["params"] == "" {
		t.Fatalf("audit[1] params empty: %+v", auditLines[1])
	}

	// 端口隔离：商户端口不暴露管理接口。
	for _, path := range []string{"/admin/outbox/replay", "/admin/accounts/check", "/admin/recon/run"} {
		mResp := do(t, http.MethodPost, "http://"+merchant+path, []byte(`{}`), adminHeaders(testAdminToken, "ops-alice"))
		if mResp.status != http.StatusNotFound {
			t.Fatalf("merchant %s status %d, want 404", path, mResp.status)
		}
	}
}

func TestGW07CheckAndReconValidationAndErrors(t *testing.T) {
	check := &checkBalanceFake{resp: &account.CheckBalanceResponse{Passed: true}}
	rc := &reconFake{resp: &recon.RunBatchResponse{Batch: &recon.ReconBatch{BatchNo: "RB1"}}}

	admin := startGW(t, NewAdmin(
		WithAdminToken(testAdminToken),
		WithCheckBalanceAPI(check),
		WithReconAPI(rc),
	), nil)

	// check 空 body 允许全量核对
	got := do(t, http.MethodPost, "http://"+admin+"/admin/accounts/check", nil, adminHeaders(testAdminToken, ""))
	env := decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 {
		t.Fatalf("empty body check: status %d env %+v", got.status, env)
	}

	// check 非法 JSON / 类型错误
	for _, body := range []string{`{`, `null`, `{"merchant_id":123}`, `{"expect_quiescent":"true"}`} {
		got = do(t, http.MethodPost, "http://"+admin+"/admin/accounts/check", []byte(body), adminHeaders(testAdminToken, ""))
		env = decodeEnv(t, got.body)
		if got.status != http.StatusBadRequest || env.Code != errno.ErrInvalidParam.Code {
			t.Fatalf("check invalid body %s: status %d env %+v", body, got.status, env)
		}
	}

	// recon 非法 JSON / 缺字段 / 类型错误
	for _, body := range []string{`{`, `{}`, `{"channel":"mockpay"}`, `{"bill_date":"2026-10-27"}`, `{"channel":1,"bill_date":"2026-10-27"}`, `{"channel":"mockpay","bill_date":"2026-10-27","force":"yes"}`} {
		got = do(t, http.MethodPost, "http://"+admin+"/admin/recon/run", []byte(body), adminHeaders(testAdminToken, "ops"))
		env = decodeEnv(t, got.body)
		if got.status != http.StatusBadRequest || env.Code != errno.ErrInvalidParam.Code {
			t.Fatalf("recon invalid body %s: status %d env %+v", body, got.status, env)
		}
	}

	// RPC 业务错误映射
	check.setErr(kerrors.NewBizStatusError(errno.ErrAccountNotFound.Code, errno.ErrAccountNotFound.Msg))
	got = do(t, http.MethodPost, "http://"+admin+"/admin/accounts/check", []byte(`{"merchant_id":"M999999"}`), adminHeaders(testAdminToken, ""))
	env = decodeEnv(t, got.body)
	if got.status != http.StatusNotFound || env.Code != errno.ErrAccountNotFound.Code {
		t.Fatalf("check 40001: status %d env %+v", got.status, env)
	}

	rc.setErr(kerrors.NewBizStatusError(errno.ErrInvalidParam.Code, errno.ErrInvalidParam.Msg))
	got = do(t, http.MethodPost, "http://"+admin+"/admin/recon/run", []byte(`{"channel":"bad","bill_date":"2026-10-27"}`), adminHeaders(testAdminToken, "ops"))
	env = decodeEnv(t, got.body)
	if got.status != http.StatusBadRequest || env.Code != errno.ErrInvalidParam.Code {
		t.Fatalf("recon 10001: status %d env %+v", got.status, env)
	}
}

func collectAuditLogs(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal log line %q: %v", line, err)
		}
		if m["msg"] == "admin write audited" {
			out = append(out, m)
		}
	}
	return out
}
