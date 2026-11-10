package gateway

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/errno"
)

type replayFake struct {
	calls int
	req   *payment.ReplayEventRequest
	resp  *payment.ReplayEventResponse
	err   error
}

func (f *replayFake) ReplayEvent(_ context.Context, req *payment.ReplayEventRequest, _ ...callopt.Option) (*payment.ReplayEventResponse, error) {
	f.calls++
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func TestAdminReplay(t *testing.T) {
	fake := &replayFake{resp: &payment.ReplayEventResponse{EventId: "EV9", Rebuilt: true}}
	admin := startGW(t, NewAdmin(WithReplayAPI(fake)), nil)
	merchant := startGW(t, NewMerchant(), nil)

	got := do(t, http.MethodPost, "http://"+admin+"/admin/outbox/replay", []byte(`{"event_id":"EV9","note":1}`), nil)
	env := decodeEnv(t, got.body)
	if got.status != http.StatusOK || signed(got.header) || env.Code != 0 || fake.calls != 1 {
		t.Fatalf("status %d signed %v env %+v calls %d body %s", got.status, signed(got.header), env, fake.calls, got.body)
	}
	if !strings.Contains(string(got.body), `"data":{"event_id":"EV9","rebuilt":true}`) {
		t.Fatalf("body %s", got.body)
	}
	if fake.req == nil || fake.req.GetEventId() != "EV9" || fake.req.IsSetBizNo() || fake.req.IsSetEventType() {
		t.Fatalf("req %+v", fake.req)
	}

	before := fake.calls
	for _, body := range []string{
		`{`,
		`{}`,
		`{"event_id":"EV1","biz_no":"PA1","event_type":"payment.succeeded"}`,
		`{"biz_no":"PA1"}`,
		`{"event_type":"payment.succeeded"}`,
		`{"event_id":""}`,
		`{"event_id":1}`,
	} {
		got = do(t, http.MethodPost, "http://"+admin+"/admin/outbox/replay", []byte(body), nil)
		env = decodeEnv(t, got.body)
		if got.status != http.StatusBadRequest || env.Code != errno.ErrInvalidParam.Code || signed(got.header) {
			t.Fatalf("body %s status %d env %+v", body, got.status, env)
		}
	}
	if fake.calls != before {
		t.Fatalf("invalid body called rpc %d", fake.calls-before)
	}

	got = do(t, http.MethodPost, "http://"+admin+"/admin/outbox/replay", []byte(`{"biz_no":"PA1","event_type":"payment.succeeded","event_id":""}`), nil)
	env = decodeEnv(t, got.body)
	if got.status != http.StatusOK || env.Code != 0 || fake.calls != before+1 {
		t.Fatalf("biz replay status %d env %+v calls %d", got.status, env, fake.calls)
	}
	if fake.req.GetBizNo() != "PA1" || fake.req.GetEventType() != "payment.succeeded" || fake.req.IsSetEventId() {
		t.Fatalf("biz req %+v", fake.req)
	}

	fake.err = kerrors.NewBizStatusError(errno.ErrNotFound.Code, errno.ErrNotFound.Msg)
	got = do(t, http.MethodPost, "http://"+admin+"/admin/outbox/replay", []byte(`{"event_id":"EV_NONE"}`), nil)
	env = decodeEnv(t, got.body)
	if got.status != http.StatusNotFound || env.Code != errno.ErrNotFound.Code {
		t.Fatalf("missing status %d env %+v", got.status, env)
	}
	fake.err = kerrors.NewBizStatusError(errno.ErrOrderStatusConflict.Code, errno.ErrOrderStatusConflict.Msg)
	got = do(t, http.MethodPost, "http://"+admin+"/admin/outbox/replay", []byte(`{"event_id":"EV_PAYING"}`), nil)
	env = decodeEnv(t, got.body)
	if got.status != http.StatusConflict || env.Code != errno.ErrOrderStatusConflict.Code {
		t.Fatalf("conflict status %d env %+v", got.status, env)
	}

	none := startGW(t, NewAdmin(), nil)
	got = do(t, http.MethodPost, "http://"+none+"/admin/outbox/replay", []byte(`{"event_id":"EV1"}`), nil)
	env = decodeEnv(t, got.body)
	if got.status != http.StatusInternalServerError || env.Code != errno.ErrInternal.Code {
		t.Fatalf("nil client status %d env %+v", got.status, env)
	}

	got = do(t, http.MethodPost, "http://"+merchant+"/admin/outbox/replay", []byte(`{"event_id":"EV1"}`), nil)
	if got.status != http.StatusNotFound {
		t.Fatalf("merchant status %d body %s", got.status, got.body)
	}
}
