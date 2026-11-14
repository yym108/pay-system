package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/yym108/pay-system/service/channel_mock/biz"
)

// AdminConfigPath 是管理端口全局故障配置路由（CH-07），仅挂载在管理端口 :6064。
const AdminConfigPath = "/mockpay/admin/config"

const maxAdminConfigBody = 64 * 1024

// AdminConfigHandler 处理 GET / PUT /mockpay/admin/config。
type AdminConfigHandler struct {
	store *biz.FaultConfigStore
}

// NewAdminConfigHandler 创建模拟渠道管理端口的全局故障配置处理器。
func NewAdminConfigHandler(store *biz.FaultConfigStore) *AdminConfigHandler {
	return &AdminConfigHandler{store: store}
}

// ServeHTTP 实现 http.Handler：GET 返回当前配置，PUT 校验并更新配置，其余方法 405。
func (h *AdminConfigHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.store == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, h.store.Get())
	case http.MethodPut:
		h.handlePut(w, r)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *AdminConfigHandler) handlePut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminConfigBody+1))
	if err != nil || len(body) > maxAdminConfigBody {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	patch, ok := decodeFaultConfigPatch(body)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	updated, err := h.store.Update(patch)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func decodeFaultConfigPatch(body []byte) (biz.FaultConfigPatch, bool) {
	if !utf8.Valid(body) {
		return biz.FaultConfigPatch{}, false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return biz.FaultConfigPatch{}, false
	}
	var patch biz.FaultConfigPatch
	fields := map[string]any{
		"delay_min_ms":        &patch.DelayMinMS,
		"delay_max_ms":        &patch.DelayMaxMS,
		"dup_notify_rate":     &patch.DupNotifyRate,
		"drop_notify_rate":    &patch.DropNotifyRate,
		"accept_error_rate":   &patch.AcceptErrorRate,
		"accept_timeout_rate": &patch.AcceptTimeoutRate,
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return biz.FaultConfigPatch{}, false
		}
		name, ok := tok.(string)
		if !ok || seen[name] {
			return biz.FaultConfigPatch{}, false
		}
		seen[name] = true
		dst, known := fields[name]
		if !known {
			return biz.FaultConfigPatch{}, false
		}
		var raw json.RawMessage
		if err = dec.Decode(&raw); err != nil {
			return biz.FaultConfigPatch{}, false
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return biz.FaultConfigPatch{}, false
		}
		if err = json.Unmarshal(raw, dst); err != nil {
			return biz.FaultConfigPatch{}, false
		}
	}
	tok, err = dec.Token()
	if err != nil || tok != json.Delim('}') {
		return biz.FaultConfigPatch{}, false
	}
	_, err = dec.Token()
	if !errors.Is(err, io.EOF) {
		return biz.FaultConfigPatch{}, false
	}
	return patch, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	out, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(out)
}
