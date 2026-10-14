package errno

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cloudwego/kitex/pkg/kerrors"
)

// defined 是约定 §5.2 的可用错误码。30002 保留，不在此表。
// 数字写在测试里，用来对照约定；业务代码只能引用本包的哨兵。
var defined = []struct {
	err        *Error
	code       int32
	httpStatus int
	msg        string
}{
	{OK, 0, 200, "ok"},
	{ErrInvalidParam, 10001, 400, "invalid parameter"},
	{ErrNotFound, 10002, 404, "not found"},
	{ErrInternal, 10003, 500, "internal error"},
	{ErrUnavailable, 10004, 503, "service unavailable"},
	{ErrRateLimited, 10005, 429, "rate limited"},
	{ErrIdempotentConflict, 10006, 409, "idempotent conflict"},
	{ErrSignInvalid, 20001, 401, "invalid signature"},
	{ErrTimestampExpired, 20002, 401, "timestamp expired"},
	{ErrNonceReplayed, 20003, 401, "nonce replayed"},
	{ErrMerchantInvalid, 20004, 401, "invalid merchant"},
	{ErrAdminUnauthorized, 20005, 401, "admin unauthorized"},
	{ErrOrderNotFound, 30001, 404, "order not found"},
	{ErrOrderStatusConflict, 30003, 409, "order status conflict"},
	{ErrOrderExpired, 30004, 409, "order expired"},
	{ErrRefundExceeded, 30005, 409, "refund exceeded"},
	{ErrChannelRejected, 30006, 200, "channel rejected"},
	{ErrChannelUnsupported, 30007, 400, "unsupported channel"},
	{ErrPayeeInvalid, 30008, 400, "invalid payee"},
	{ErrAccountNotFound, 40001, 404, "account not found"},
	{ErrInsufficientBalance, 40002, 409, "insufficient balance"},
	{ErrUnbalancedEntries, 40003, 400, "unbalanced entries"},
	{ErrFreezeStatusConflict, 40004, 409, "freeze status conflict"},
	{ErrAccountInactive, 40005, 409, "account inactive"},
	{ErrChannelSignInvalid, 50001, 401, "invalid channel signature"},
	{ErrChannelAmountMismatch, 50002, 400, "channel amount mismatch"},
}

// BASE-06 验收 1：包装后的错误经 errors.Is 能识别原错误码。
func TestWrapErrorsIs(t *testing.T) {
	t.Parallel()
	cause := errors.New("account shard timeout: connection reset by peer")
	wrapped := Wrap(ErrInsufficientBalance, cause)
	if !errors.Is(wrapped, ErrInsufficientBalance) {
		t.Fatal("errors.Is(wrapped, ErrInsufficientBalance) = false")
	}
	outer := fmt.Errorf("freeze: %w", wrapped)
	if !errors.Is(outer, ErrInsufficientBalance) {
		t.Fatal("errors.Is(outer, ErrInsufficientBalance) = false")
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("Unwrap 后识别不到底层错误")
	}
	if wrapped.Msg != "insufficient balance" || wrapped.Error() != "insufficient balance" {
		t.Fatalf("对外文本 = msg %q error %q", wrapped.Msg, wrapped.Error())
	}
	if strings.Contains(wrapped.Msg, "connection reset") || strings.Contains(wrapped.Error(), "connection reset") {
		t.Fatalf("对外文本带了底层错误: msg %q error %q", wrapped.Msg, wrapped.Error())
	}
	if strings.Contains(outer.Error(), "connection reset") {
		t.Fatalf("再包装后的 Error() 带了底层错误: %q", outer.Error())
	}
	if errors.Unwrap(ErrInsufficientBalance) != nil {
		t.Fatal("Wrap 修改了哨兵")
	}
	if kept := Wrap(ErrInsufficientBalance, nil); kept != ErrInsufficientBalance || errors.Unwrap(kept) != nil {
		t.Fatal("cause 为 nil 时应返回原哨兵")
	}
}

// BASE-06 验收 2：Kitex 服务端返回 40002，客户端还原为同一错误码。
func TestKitexBizStatusRoundTrip(t *testing.T) {
	t.Parallel()
	serverErr := ToBizStatus(ErrInsufficientBalance)
	biz, ok := kerrors.FromBizStatusError(serverErr)
	if !ok {
		t.Fatal("kerrors.FromBizStatusError 未识别服务端错误")
	}
	if _, isReal := biz.(*kerrors.BizStatusError); !isReal {
		t.Fatalf("服务端错误类型 = %T，要 *kerrors.BizStatusError", biz)
	}
	if biz.BizStatusCode() != 40002 {
		t.Fatalf("服务端业务码 = %d，要 40002", biz.BizStatusCode())
	}
	if biz.BizMessage() != "insufficient balance" {
		t.Fatalf("服务端对外消息 = %q", biz.BizMessage())
	}

	got := FromBizStatus(serverErr)
	if got.Code != 40002 || !errors.Is(got, ErrInsufficientBalance) {
		t.Fatalf("客户端还原 = code %d，errors.Is = %v", got.Code, errors.Is(got, ErrInsufficientBalance))
	}
	if got.Msg != "insufficient balance" || got.HTTPStatus != 409 {
		t.Fatalf("还原后的 msg/http = %q/%d", got.Msg, got.HTTPStatus)
	}

	// 对端消息即使夹带内部句子，还原后仍用本表的稳定 msg。
	foreign := kerrors.NewBizStatusError(40002, "balance of M1 is 1 fen; token=abc")
	foreignBiz, ok := kerrors.FromBizStatusError(foreign)
	if !ok {
		t.Fatal("自造的真实 BizStatusError 未被 FromBizStatusError 识别")
	}
	if foreignBiz.BizStatusCode() != 40002 {
		t.Fatalf("对端码 = %d", foreignBiz.BizStatusCode())
	}
	restored := FromBizStatus(fmt.Errorf("client: %w", foreign))
	if restored.Code != 40002 || !errors.Is(restored, ErrInsufficientBalance) {
		t.Fatalf("包装后的对端错误还原 = code %d", restored.Code)
	}
	if restored.Msg != "insufficient balance" || restored.Error() != "insufficient balance" {
		t.Fatalf("还原对外文本 = msg %q error %q", restored.Msg, restored.Error())
	}
	if strings.Contains(restored.Msg, "token=") || strings.Contains(restored.Error(), "token=") {
		t.Fatalf("对外文本带了对端句子: %q", restored.Error())
	}
}

// BASE-06 规则 3：未知错误变成 10003，对外消息没有底层文本。
func TestUnknownErrorIsInternal(t *testing.T) {
	t.Parallel()
	raw := errors.New("sql boom: connection reset by peer")
	got := From(raw)
	if got.Code != 10003 || !errors.Is(got, ErrInternal) {
		t.Fatalf("From(unknown) = code %d", got.Code)
	}
	if got.Msg != "internal error" || got.Error() != "internal error" {
		t.Fatalf("未知错误对外文本 = msg %q error %q", got.Msg, got.Error())
	}
	if strings.Contains(got.Msg, "sql boom") || strings.Contains(got.Error(), "connection reset") {
		t.Fatalf("对外文本带了底层错误: msg %q error %q", got.Msg, got.Error())
	}
	if !errors.Is(got, raw) {
		t.Fatal("未知错误的底层原因不能通过 Unwrap 取得")
	}

	wrapped := Wrap(ErrInternal, raw)
	if wrapped.Code != 10003 || wrapped.Msg != "internal error" || wrapped.Error() != "internal error" {
		t.Fatalf("包装后的 10003 对外文本 = msg %q error %q", wrapped.Msg, wrapped.Error())
	}
	if strings.Contains(wrapped.Error(), "sql boom") || strings.Contains(wrapped.Msg, "connection reset") {
		t.Fatalf("包装后的 10003 对外文本带了底层错误: %q", wrapped.Error())
	}
	if !errors.Is(wrapped, raw) || !errors.Is(wrapped, ErrInternal) {
		t.Fatal("包装后的 10003 应同时识别原错误码和底层错误")
	}

	sent := ToBizStatus(wrapped)
	biz, ok := kerrors.FromBizStatusError(sent)
	if !ok {
		t.Fatal("10003 没有变成 BizStatusError")
	}
	if biz.BizStatusCode() != 10003 || biz.BizMessage() != "internal error" {
		t.Fatalf("10003 的 BizStatus = code %d msg %q", biz.BizStatusCode(), biz.BizMessage())
	}
	if strings.Contains(biz.Error(), "sql boom") || strings.Contains(biz.BizMessage(), "connection reset") {
		t.Fatalf("BizStatusError 带了底层错误: %q", biz.Error())
	}

	rpc := From(kerrors.ErrRPCTimeout)
	if rpc.Code != 10003 || rpc.Error() != "internal error" {
		t.Fatalf("非业务的 Kitex 错误 = code %d error %q", rpc.Code, rpc.Error())
	}
	if strings.Contains(rpc.Error(), "rpc timeout") || strings.Contains(rpc.Msg, "timeout") {
		t.Fatalf("非业务错误的对外文本带了底层句子: %q", rpc.Error())
	}
	if !errors.Is(rpc, kerrors.ErrRPCTimeout) {
		t.Fatal("非业务错误的原因不能通过 Unwrap 取得")
	}
	viaNil := Wrap(nil, raw)
	if viaNil.Code != 10003 || viaNil.Error() != "internal error" || !errors.Is(viaNil, raw) {
		t.Fatalf("Wrap(nil) = code %d error %q", viaNil.Code, viaNil.Error())
	}
	if strings.Contains(viaNil.Error(), "sql boom") {
		t.Fatalf("Wrap(nil) 的对外文本带了底层错误: %q", viaNil.Error())
	}
	fromWire := FromBizStatus(raw)
	if fromWire.Code != 10003 || fromWire.Error() != "internal error" || strings.Contains(fromWire.Error(), "sql boom") {
		t.Fatalf("FromBizStatus(普通错误) = code %d error %q", fromWire.Code, fromWire.Error())
	}
}

// BASE-06 规则 4：nil 是成功，不是 10003。
func TestNilIsSuccess(t *testing.T) {
	t.Parallel()
	got := From(nil)
	if got == nil {
		t.Fatal("From(nil) = nil，成功也应返回码 0")
	}
	if got.Code != 0 || got.Msg != "ok" || got.HTTPStatus != 200 || got.Error() != "ok" {
		t.Fatalf("From(nil) = code %d msg %q http %d error %q", got.Code, got.Msg, got.HTTPStatus, got.Error())
	}
	if !errors.Is(got, OK) || errors.Is(got, ErrInternal) {
		t.Fatal("nil 应是 OK，不应是 10003")
	}
	if ToBizStatus(nil) != nil || ToBizStatus(OK) != nil {
		t.Fatal("成功不能变成 BizStatusError")
	}
	if again := FromBizStatus(nil); again.Code != 0 || again.Msg != "ok" || again.HTTPStatus != 200 {
		t.Fatalf("FromBizStatus(nil) = code %d msg %q http %d", again.Code, again.Msg, again.HTTPStatus)
	}
}

// BASE-06 规则 1：表中每个已定义码的 Code、HTTPStatus、Msg 与约定一致。
func TestDefinedCodesMatchConvention(t *testing.T) {
	t.Parallel()
	if len(All()) != len(defined) {
		t.Fatalf("All() 数量 = %d，要 %d", len(All()), len(defined))
	}
	seen := make(map[int32]*Error, len(defined))
	for _, tc := range defined {
		t.Run(fmt.Sprintf("%d", tc.code), func(t *testing.T) {
			t.Parallel()
			if tc.err == nil {
				t.Fatal("哨兵是 nil")
			}
			if tc.err.Code != tc.code || tc.err.HTTPStatus != tc.httpStatus || tc.err.Msg != tc.msg {
				t.Fatalf("got code=%d http=%d msg=%q，want code=%d http=%d msg=%q",
					tc.err.Code, tc.err.HTTPStatus, tc.err.Msg, tc.code, tc.httpStatus, tc.msg)
			}
			if tc.err.Error() != tc.msg {
				t.Fatalf("Error() = %q，want %q", tc.err.Error(), tc.msg)
			}
			got, ok := ByCode(tc.code)
			if !ok || !errors.Is(got, tc.err) {
				t.Fatalf("ByCode(%d) ok=%v", tc.code, ok)
			}
			if got.HTTPStatus != tc.httpStatus || got.Msg != tc.msg {
				t.Fatalf("ByCode(%d) = http %d msg %q", tc.code, got.HTTPStatus, got.Msg)
			}
			again := From(fmt.Errorf("ctx: %w", tc.err))
			if again.Code != tc.code || again.Msg != tc.msg || again.Error() != tc.msg || !errors.Is(again, tc.err) {
				t.Fatalf("包装后的 From = code %d msg %q error %q", again.Code, again.Msg, again.Error())
			}
			if strings.Contains(again.Error(), "ctx:") {
				t.Fatalf("From 后的 Error() 带了包装前缀: %q", again.Error())
			}
			wire := ToBizStatus(tc.err)
			if tc.code == 0 {
				if wire != nil {
					t.Fatal("成功码不能变成 BizStatusError")
				}
				return
			}
			biz, bizOK := kerrors.FromBizStatusError(wire)
			if !bizOK {
				t.Fatal("ToBizStatus 不是 Kitex 能还原的 BizStatusError")
			}
			if _, isReal := biz.(*kerrors.BizStatusError); !isReal {
				t.Fatalf("类型 %T，要 *kerrors.BizStatusError", biz)
			}
			if biz.BizStatusCode() != tc.code || biz.BizMessage() != tc.msg {
				t.Fatalf("BizStatus = code %d msg %q", biz.BizStatusCode(), biz.BizMessage())
			}
			if strings.Contains(biz.Error(), "ctx:") {
				t.Fatalf("BizStatusError 带了包装前缀: %q", biz.Error())
			}
			back := FromBizStatus(wire)
			if back.Code != tc.code || back.Msg != tc.msg || back.HTTPStatus != tc.httpStatus || back.Error() != tc.msg {
				t.Fatalf("还原 = code %d msg %q http %d error %q", back.Code, back.Msg, back.HTTPStatus, back.Error())
			}
		})
		if prev, ok := seen[tc.code]; ok {
			t.Fatalf("码 %d 重复登记: %v 与 %v", tc.code, prev, tc.err)
		}
		seen[tc.code] = tc.err
	}
	for _, e := range All() {
		want, ok := seen[e.Code]
		if !ok || !errors.Is(e, want) {
			t.Errorf("All() 含有未登记的码 %d", e.Code)
		}
	}
}

// BASE-06 规则 5：30002 不存在为可用错误。
func TestReserved30002NotUsable(t *testing.T) {
	t.Parallel()
	if _, ok := ByCode(30002); ok {
		t.Fatal("ByCode(30002) 不应返回可用错误")
	}
	for _, e := range All() {
		if e.Code == 30002 {
			t.Fatalf("All() 含有 30002: %+v", e)
		}
	}
	wire := kerrors.NewBizStatusError(30002, "reserved exploded: connection reset by peer")
	biz, ok := kerrors.FromBizStatusError(wire)
	if !ok || biz.BizStatusCode() != 30002 {
		t.Fatal("测试应使用真实的 BizStatusError 携带保留码")
	}
	got := FromBizStatus(wire)
	if got.Code == 30002 {
		t.Fatal("30002 被还原成了可用错误")
	}
	if got.Code != 10003 || !errors.Is(got, ErrInternal) {
		t.Fatalf("保留码应视为 10003，得到 %d", got.Code)
	}
	if got.Msg != "internal error" || got.Error() != "internal error" {
		t.Fatalf("保留码对外文本 = msg %q error %q", got.Msg, got.Error())
	}
	if strings.Contains(got.Msg, "reserved") || strings.Contains(got.Error(), "connection reset") {
		t.Fatalf("保留码的对外文本带了对端句子: %q", got.Error())
	}
}
