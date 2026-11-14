package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

// CH-07：管理接口 GET / PUT /mockpay/admin/config 读写与部分更新。
func TestAdminConfigGetAndPut(t *testing.T) {
	fc := biz.NewFaultConfigStore(200, 2000)
	h := NewAdminConfigHandler(fc)

	// 1. 初始 GET 返回默认值。
	req := httptest.NewRequest(http.MethodGet, AdminConfigPath, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", rec.Code)
	}
	var got biz.FaultConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.DelayMinMS != 200 || got.DelayMaxMS != 2000 ||
		got.DupNotifyRate != 0 || got.DropNotifyRate != 0 ||
		got.AcceptErrorRate != 0 || got.AcceptTimeoutRate != 0 {
		t.Fatalf("initial GET = %+v", got)
	}

	// 2. PUT 部分更新（仅改 delay 和 drop_notify_rate）。
	body := `{"delay_min_ms":50,"delay_max_ms":200,"drop_notify_rate":1}`
	putReq := httptest.NewRequest(http.MethodPut, AdminConfigPath, strings.NewReader(body))
	putRec := httptest.NewRecorder()
	h.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body=%s", putRec.Code, putRec.Body.String())
	}
	if err := json.Unmarshal(putRec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.DelayMinMS != 50 || got.DelayMaxMS != 200 || got.DropNotifyRate != 1 || got.DupNotifyRate != 0 {
		t.Fatalf("after PUT = %+v", got)
	}

	// 3. 再次 GET 读到更新后的值。
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, AdminConfigPath, nil))
	if rec2.Code != http.StatusOK || !bytes.Equal(rec2.Body.Bytes(), putRec.Body.Bytes()) {
		t.Fatalf("GET2 status=%d body=%s", rec2.Code, rec2.Body.String())
	}
}

// CH-07：管理接口参数校验（非法 JSON、未知字段、重复字段、越界值返回 400）与方法校验（405）。
func TestAdminConfigValidation(t *testing.T) {
	fc := biz.NewFaultConfigStore(200, 2000)
	h := NewAdminConfigHandler(fc)

	badPayloads := []string{
		``,
		`[]`,
		`{"unknown_field": 1}`,
		`{"delay_min_ms": -1}`,
		`{"delay_min_ms": 500, "delay_max_ms": 100}`,
		`{"dup_notify_rate": 1.5}`,
		`{"drop_notify_rate": -0.1}`,
		`{"accept_error_rate": "0.5"}`,
		`{"delay_min_ms": 1.5}`,
		`{"delay_min_ms": null}`,
		`{"drop_notify_rate": null}`,
		`{"delay_min_ms": 10, "delay_min_ms": 20}`,
		`{"delay_min_ms": 10} trailing`,
	}
	for _, p := range badPayloads {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, AdminConfigPath, strings.NewReader(p)))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("payload %q: status=%d, want 400", p, rec.Code)
		}
	}

	// 非 GET/PUT 方法返回 405。
	for _, m := range []string{http.MethodPost, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(m, AdminConfigPath, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method %s: status=%d, want 405", m, rec.Code)
		}
	}
}

// CH-07：管理接口仅注册在管理端口，业务端口（Hertz）不暴露该路由（返回 404）。
func TestAdminConfigOnlyOnAdminPort(t *testing.T) {
	srv := server.New()
	New(nil, []byte("secret"), clock.Real{}).Register(srv)

	for _, method := range []string{http.MethodGet, http.MethodPut} {
		resp := ut.PerformRequest(srv.Engine, method, AdminConfigPath, nil).Result()
		if resp.StatusCode() != http.StatusNotFound {
			t.Fatalf("biz port %s %s: status=%d, want 404", method, AdminConfigPath, resp.StatusCode())
		}
	}
}
