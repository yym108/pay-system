package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

type fakeBillInjector struct {
	res biz.BillInjectResult
	err error
	got biz.BillInjectRequest
}

func (f *fakeBillInjector) InjectBill(_ context.Context, req biz.BillInjectRequest) (biz.BillInjectResult, error) {
	f.got = req
	return f.res, f.err
}

// CH-09 验收 1：POST /mockpay/admin/bill-inject 返回 req_no 与 channel_txn_no。
func TestAdminBillInjectHandler(t *testing.T) {
	inj := &fakeBillInjector{
		res: biz.BillInjectResult{
			ReqNo:        "INJ0000000000000000001",
			ChannelTxnNo: "MC0000000000000000002",
		},
	}
	h := NewAdminBillInjectHandler(inj)

	body := `{"date":"2026-10-12","txn_type":"PAY","amount":10000}`
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, AdminBillInjectPath, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", rec.Code, rec.Body.String())
	}
	var got struct {
		ReqNo        string `json:"req_no"`
		ChannelTxnNo string `json:"channel_txn_no"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ReqNo != "INJ0000000000000000001" || got.ChannelTxnNo != "MC0000000000000000002" {
		t.Fatalf("got = %+v", got)
	}
	if inj.got.Date != "2026-10-12" || inj.got.TxnType != "PAY" || inj.got.Amount != 10000 || inj.got.ReqNo != "" {
		t.Fatalf("passed req = %+v", inj.got)
	}

	// 可选 req_no 透传
	body2 := `{"date":"2026-10-12","txn_type":"REFUND","amount":500,"req_no":"CUSTOM_001"}`
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequestWithContext(t.Context(), http.MethodPost, AdminBillInjectPath, strings.NewReader(body2)))
	if rec2.Code != http.StatusOK {
		t.Fatalf("status2 = %d, want 200", rec2.Code)
	}
	if inj.got.ReqNo != "CUSTOM_001" || inj.got.TxnType != "REFUND" || inj.got.Amount != 500 {
		t.Fatalf("passed req2 = %+v", inj.got)
	}
}

// CH-09：管理接口参数校验（400）、冲突（409）、内部错误（500）、方法校验（405）与端口隔离（业务端口 404）。
func TestAdminBillInjectValidationAndPortIsolation(t *testing.T) {
	inj := &fakeBillInjector{}
	h := NewAdminBillInjectHandler(inj)

	badPayloads := []string{
		``,
		`[]`,
		`{"date":"2026-10-12","txn_type":"PAY"}`,
		`{"date":"2026-10-12","amount":100}`,
		`{"txn_type":"PAY","amount":100}`,
		`{"date":"2026-10-12","txn_type":"PAY","amount":100,"extra":1}`,
		`{"date":"2026-10-12","txn_type":"PAY","amount":100,"date":"2026-10-13"}`,
		`{"date":null,"txn_type":"PAY","amount":100}`,
		`{"date":"2026-10-12","txn_type":"PAY","amount":100,"req_no":null}`,
		`{"date":"2026-10-12","txn_type":"PAY","amount":"100"}`,
		`{"date":"2026-10-12","txn_type":"PAY","amount":100} trailing`,
	}
	for _, p := range badPayloads {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, AdminBillInjectPath, strings.NewReader(p)))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("payload %q: status=%d, want 400", p, rec.Code)
		}
	}

	// biz 返回 ErrInvalidRequest -> 400
	injErr := NewAdminBillInjectHandler(&fakeBillInjector{err: biz.ErrInvalidRequest})
	rec400 := httptest.NewRecorder()
	injErr.ServeHTTP(rec400, httptest.NewRequestWithContext(t.Context(), http.MethodPost, AdminBillInjectPath,
		strings.NewReader(`{"date":"2026-10-12","txn_type":"PAY","amount":100}`)))
	if rec400.Code != http.StatusBadRequest {
		t.Fatalf("ErrInvalidRequest status=%d, want 400", rec400.Code)
	}

	// biz 返回 ErrConflict -> 409
	injConf := NewAdminBillInjectHandler(&fakeBillInjector{err: biz.ErrConflict})
	rec409 := httptest.NewRecorder()
	injConf.ServeHTTP(rec409, httptest.NewRequestWithContext(t.Context(), http.MethodPost, AdminBillInjectPath,
		strings.NewReader(`{"date":"2026-10-12","txn_type":"PAY","amount":100,"req_no":"DUP"}`)))
	if rec409.Code != http.StatusConflict {
		t.Fatalf("ErrConflict status=%d, want 409", rec409.Code)
	}

	// biz 返回其他错误 -> 500
	inj500 := NewAdminBillInjectHandler(&fakeBillInjector{err: errors.New("db down")})
	rec500 := httptest.NewRecorder()
	inj500.ServeHTTP(rec500, httptest.NewRequestWithContext(t.Context(), http.MethodPost, AdminBillInjectPath,
		strings.NewReader(`{"date":"2026-10-12","txn_type":"PAY","amount":100}`)))
	if rec500.Code != http.StatusInternalServerError {
		t.Fatalf("internal err status=%d, want 500", rec500.Code)
	}

	// 非 POST 方法 -> 405
	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), m, AdminBillInjectPath, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method %s: status=%d, want 405", m, rec.Code)
		}
	}

	// 业务端口不暴露管理路由（返回 404）
	srv := server.New(server.WithDisablePrintRoute(true))
	New(nil, []byte("secret"), clock.Real{}).Register(srv)
	resp := ut.PerformRequest(srv.Engine, http.MethodPost, AdminBillInjectPath, nil).Result()
	if resp.StatusCode() != http.StatusNotFound {
		t.Fatalf("biz port POST %s: status=%d, want 404", AdminBillInjectPath, resp.StatusCode())
	}
}
