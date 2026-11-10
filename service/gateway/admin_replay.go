package gateway

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/kitex/client/callopt"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
)

// replayAPI 是管理端口重放要调的支付方法。生产实现是 Kitex 客户端。
// 不放进 paymentAPI，商户端口的假实现不用实现它。
type replayAPI interface {
	ReplayEvent(ctx context.Context, req *payment.ReplayEventRequest, callOptions ...callopt.Option) (*payment.ReplayEventResponse, error)
}

// WithReplayAPI 安装重放 RPC。只应装在管理端口。
func WithReplayAPI(api replayAPI) Option {
	return func(h *Handler) {
		if h != nil {
			h.replay = api
		}
	}
}

func (h *Handler) registerAdmin(r *server.Hertz) {
	if h == nil || r == nil {
		return
	}
	r.POST("/admin/outbox/replay", h.replayOutbox)
}

// replayView 的字段顺序就是 JSON 顺序：event_id，然后 rebuilt。
type replayView struct {
	EventID string `json:"event_id"`
	Rebuilt bool   `json:"rebuilt"`
}

// replayOutbox 只在管理端口。不读 X-Admin-Token 和 X-Admin-Operator，不写审计日志。
// 非法 JSON 或两个键不是二选一返回 10001，不调用 RPC。
func (h *Handler) replayOutbox(ctx context.Context, c *app.RequestContext) {
	if h == nil {
		return
	}
	body, err := c.Body()
	if err != nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	req, bad := parseReplay(body)
	if bad != nil {
		h.writeRoute(c, bad, nil)
		return
	}
	if h.replay == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	resp, err := h.replay.ReplayEvent(ctx, req, callopt.WithRPCTimeout(RPCTimeout("ReplayEvent")))
	if err != nil {
		h.writeRoute(c, mapRouteErr(err), nil)
		return
	}
	if resp == nil {
		h.writeRoute(c, errno.ErrInternal, nil)
		return
	}
	WriteOK(c, replayView{EventID: resp.EventId, Rebuilt: resp.Rebuilt})
}

func parseReplay(body []byte) (*payment.ReplayEventRequest, *errno.Error) {
	obj, bad := decodeObject(body)
	if bad != nil {
		return nil, bad
	}
	eventPresent, eventID, ok := optionalID(obj["event_id"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	bizPresent, bizNo, ok := optionalID(obj["biz_no"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	typePresent, eventType, ok := optionalID(obj["event_type"])
	if !ok {
		return nil, errno.ErrInvalidParam
	}
	if eventPresent {
		if bizPresent || typePresent {
			return nil, errno.ErrInvalidParam
		}
		return &payment.ReplayEventRequest{EventId: &eventID}, nil
	}
	if bizPresent && typePresent {
		return &payment.ReplayEventRequest{BizNo: &bizNo, EventType: &eventType}, nil
	}
	return nil, errno.ErrInvalidParam
}
