package gateway

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/kitex/client/callopt"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/kitex_gen/recon"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/logx"
)

const (
	headerAdminToken    = "X-Admin-Token"
	headerAdminOperator = "X-Admin-Operator"
	nameAdminAuth       = "admin_auth"
	maxOperatorRunes    = 64
)

// checkBalanceAPI 是管理端口余额核对要调的账户方法。生产实现是 Kitex 客户端。
type checkBalanceAPI interface {
	CheckBalance(ctx context.Context, req *account.CheckBalanceRequest, callOptions ...callopt.Option) (*account.CheckBalanceResponse, error)
}

// reconAPI 是管理端口触发对账要调的对账方法。生产实现是 Kitex 客户端。
type reconAPI interface {
	RunBatch(ctx context.Context, req *recon.RunBatchRequest, callOptions ...callopt.Option) (*recon.RunBatchResponse, error)
}

// WithAdminToken 配置管理端口的 ADMIN_TOKEN。只应装在管理端口。
func WithAdminToken(token string) Option {
	return func(h *Handler) {
		if h != nil {
			h.adminToken = []byte(token)
		}
	}
}

// WithCheckBalanceAPI 安装余额核对 RPC。只应装在管理端口。
func WithCheckBalanceAPI(api checkBalanceAPI) Option {
	return func(h *Handler) {
		if h != nil {
			h.checkBalance = api
		}
	}
}

// WithReconAPI 安装触发对账 RPC。只应装在管理端口。
func WithReconAPI(api reconAPI) Option {
	return func(h *Handler) {
		if h != nil {
			h.recon = api
		}
	}
}

func (h *Handler) adminAuth(ctx context.Context, c *app.RequestContext) {
	appendName(c, nameAdminAuth)
	if !h.admin {
		c.Next(ctx)
		return
	}
	got := string(c.GetHeader(headerAdminToken))
	if len(h.adminToken) == 0 || subtle.ConstantTimeCompare([]byte(got), h.adminToken) != 1 {
		h.log().WarnContext(logx.WithRequestID(ctx, requestID(c)), "admin token rejected")
		writeErrno(c, errno.ErrAdminUnauthorized.HTTPStatus, errno.ErrAdminUnauthorized, requestID(c))
		c.Abort()
		return
	}
	c.Next(ctx)
}

func requireAdminOperator(c *app.RequestContext) (string, *errno.Error) {
	if c == nil {
		return "", errno.ErrInvalidParam
	}
	op := string(c.GetHeader(headerAdminOperator))
	if strings.TrimSpace(op) == "" || strings.ContainsAny(op, "\r\n") || utf8.RuneCountInString(op) > maxOperatorRunes {
		return "", errno.ErrInvalidParam
	}
	return op, nil
}

func (h *Handler) writeAdminAudit(ctx context.Context, c *app.RequestContext, operator, path, params string, err *errno.Error) {
	result := "ok"
	errCode := ""
	if err != nil {
		result = "failed"
		errCode = strconv.FormatInt(int64(err.Code), 10)
	}
	h.log().InfoContext(logx.WithRequestID(ctx, requestID(c)), "admin write audited",
		"operator", operator,
		"path", path,
		"params", params,
		"result", result,
		"err_code", errCode,
	)
}

type checkBalanceView struct {
	Passed   bool                      `json:"passed"`
	Failures []*account.BalanceFailure `json:"failures"`
}

func (h *Handler) checkAccounts(ctx context.Context, c *app.RequestContext) {
	if h == nil {
		return
	}
	body, err := c.Body()
	if err != nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	req, bad := parseCheckBalance(body)
	if bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	if h.checkBalance == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	resp, err := h.checkBalance.CheckBalance(ctx, req, callopt.WithRPCTimeout(RPCTimeout("CheckBalance")))
	if err != nil {
		h.writeRoute(c, mapRouteErr(err), nil)
		return
	}
	if resp == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	failures := resp.Failures
	if failures == nil {
		failures = []*account.BalanceFailure{}
	}
	WriteOK(c, checkBalanceView{Passed: resp.Passed, Failures: failures})
}

func parseCheckBalance(body []byte) (*account.CheckBalanceRequest, *errno.Error) {
	if len(body) == 0 {
		return &account.CheckBalanceRequest{}, nil
	}
	obj, bad := decodeObject(body)
	if bad != nil {
		return nil, bad
	}
	hasMerchant, merchantID, ok := optionalID(obj["merchant_id"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	hasQuiet, quiet, ok := optionalBool(obj["expect_quiescent"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	req := &account.CheckBalanceRequest{}
	if hasMerchant {
		req.MerchantId = &merchantID
	}
	if hasQuiet {
		req.ExpectQuiescent = &quiet
	}
	return req, nil
}

type runBatchView struct {
	Batch       *recon.ReconBatch  `json:"batch"`
	Diffs       []*recon.ReconDiff `json:"diffs"`
	ManualTotal int32              `json:"manual_total"`
	ManualDiffs []*recon.ReconDiff `json:"manual_diffs"`
}

func (h *Handler) runRecon(ctx context.Context, c *app.RequestContext) {
	if h == nil {
		return
	}
	operator, bad := requireAdminOperator(c)
	if bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	body, err := c.Body()
	if err != nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	req, bad := parseReconRun(body)
	if bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	if h.recon == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	params := fmt.Sprintf("channel=%s bill_date=%s force=%v", req.Channel, req.BillDate, req.GetForce())
	resp, err := h.recon.RunBatch(ctx, req, callopt.WithRPCTimeout(RPCTimeout("RunBatch")))
	if err != nil {
		mapped := mapRouteErr(err)
		h.writeAdminAudit(ctx, c, operator, "/admin/recon/run", params, mapped)
		h.writeRoute(c, mapped, nil)
		return
	}
	if resp == nil {
		h.writeAdminAudit(ctx, c, operator, "/admin/recon/run", params, errno.ErrInternal)
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	h.writeAdminAudit(ctx, c, operator, "/admin/recon/run", params, nil)
	diffs := resp.Diffs
	if diffs == nil {
		diffs = []*recon.ReconDiff{}
	}
	manualDiffs := resp.ManualDiffs
	if manualDiffs == nil {
		manualDiffs = []*recon.ReconDiff{}
	}
	WriteOK(c, runBatchView{
		Batch:       resp.Batch,
		Diffs:       diffs,
		ManualTotal: resp.GetManualTotal(),
		ManualDiffs: manualDiffs,
	})
}

func parseReconRun(body []byte) (*recon.RunBatchRequest, *errno.Error) {
	obj, bad := decodeObject(body)
	if bad != nil {
		return nil, bad
	}
	hasChannel, channel, ok := optionalID(obj["channel"])
	if !ok || !hasChannel {
		return nil, errno.ErrInvalidParam
	}
	hasDate, billDate, ok := optionalID(obj["bill_date"])
	if !ok || !hasDate {
		return nil, errno.ErrInvalidParam
	}
	hasForce, force, ok := optionalBool(obj["force"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	req := &recon.RunBatchRequest{
		Channel:  channel,
		BillDate: billDate,
	}
	if hasForce {
		req.Force = &force
	}
	return req, nil
}

func optionalBool(raw json.RawMessage) (present bool, value bool, ok bool) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false, false, true
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return false, false, false
	}
	return true, b, true
}
