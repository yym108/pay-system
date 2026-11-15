package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"unicode/utf8"

	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/biz"
)

// AdminBillInjectPath 是管理端口账单注入路由（CH-09），仅挂载在管理端口 :6064。
const AdminBillInjectPath = "/mockpay/admin/bill-inject"

// BillInjector 执行账单注入（CH-09）。
type BillInjector interface {
	InjectBill(ctx context.Context, req biz.BillInjectRequest) (biz.BillInjectResult, error)
}

// AdminBillInjectHandler 处理 POST /mockpay/admin/bill-inject。
type AdminBillInjectHandler struct {
	svc BillInjector
}

// NewAdminBillInjectHandler 创建模拟渠道管理端口的账单注入处理器。
func NewAdminBillInjectHandler(svc BillInjector) *AdminBillInjectHandler {
	return &AdminBillInjectHandler{svc: svc}
}

type billInjectResponse struct {
	ReqNo        string `json:"req_no"`
	ChannelTxnNo string `json:"channel_txn_no"`
}

// ServeHTTP 实现 http.Handler：仅允许 POST，其余方法返回 405。
func (h *AdminBillInjectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.svc == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxAdminConfigBody+1))
	if err != nil || len(body) > maxAdminConfigBody {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	req, ok := decodeBillInjectRequest(body)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	callCtx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	res, err := h.svc.InjectBill(callCtx, req)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, billInjectResponse{
			ReqNo:        res.ReqNo,
			ChannelTxnNo: res.ChannelTxnNo,
		})
	case errors.Is(err, biz.ErrInvalidRequest):
		w.WriteHeader(http.StatusBadRequest)
	case errors.Is(err, biz.ErrConflict):
		w.WriteHeader(http.StatusConflict)
	default:
		slog.ErrorContext(r.Context(), "admin bill inject failed", slog.String("error", err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func decodeBillInjectRequest(body []byte) (biz.BillInjectRequest, bool) {
	if !utf8.Valid(body) {
		return biz.BillInjectRequest{}, false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return biz.BillInjectRequest{}, false
	}
	var (
		date    string
		txnType string
		amount  int64
		reqNo   string
	)
	fields := map[string]any{
		"date":     &date,
		"txn_type": &txnType,
		"amount":   &amount,
		"req_no":   &reqNo,
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return biz.BillInjectRequest{}, false
		}
		name, ok := tok.(string)
		if !ok || seen[name] {
			return biz.BillInjectRequest{}, false
		}
		seen[name] = true
		dst, known := fields[name]
		if !known {
			return biz.BillInjectRequest{}, false
		}
		var raw json.RawMessage
		if err = dec.Decode(&raw); err != nil {
			return biz.BillInjectRequest{}, false
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return biz.BillInjectRequest{}, false
		}
		if err = json.Unmarshal(raw, dst); err != nil {
			return biz.BillInjectRequest{}, false
		}
	}
	tok, err = dec.Token()
	if err != nil || tok != json.Delim('}') {
		return biz.BillInjectRequest{}, false
	}
	_, err = dec.Token()
	if !errors.Is(err, io.EOF) {
		return biz.BillInjectRequest{}, false
	}
	if !seen["date"] || !seen["txn_type"] || !seen["amount"] {
		return biz.BillInjectRequest{}, false
	}
	return biz.BillInjectRequest{
		Date:    date,
		TxnType: txnType,
		Amount:  money.Amount(amount),
		ReqNo:   reqNo,
	}, true
}
