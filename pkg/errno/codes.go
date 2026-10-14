package errno

import "net/http"

// 30002 保留不用（约定 §5.2：v1 曾表示幂等冲突，已由 10006 替代）。
// 下面故意不登记这个码。未登记码由 From / FromBizStatus 视为 10003。

var (
	byCode = map[int32]*Error{}
	all    []*Error
)

func mustRegister(e *Error) *Error {
	if e == nil || e.Msg == "" {
		panic("errno: empty error")
	}
	if _, ok := byCode[e.Code]; ok {
		panic("errno: duplicate code")
	}
	byCode[e.Code] = e
	all = append(all, e)
	return e
}

// ByCode 按业务码查找已登记的哨兵。
// 30002 和任何未登记码返回 false，不造一个可用错误。
func ByCode(code int32) (*Error, bool) {
	e, ok := byCode[code]
	return e, ok
}

// All 返回已登记的全部业务码（含成功码），顺序与约定 §5.2 一致，不含 30002。
func All() []*Error {
	out := make([]*Error, len(all))
	copy(out, all)
	return out
}

var (
	// OK 是成功码 0。nil 错误映射到这里，对外 msg 为 ok，HTTP 200。
	OK = mustRegister(&Error{
		Code:       0,
		Msg:        "ok",
		HTTPStatus: http.StatusOK,
	})

	// ErrInvalidParam 是 10001。HTTPStatus 固定 400。
	// 请求体超过 64KB 时返回 413 是网关行为，不在本包另造码。
	ErrInvalidParam = mustRegister(&Error{
		Code:       10001,
		Msg:        "invalid parameter",
		HTTPStatus: http.StatusBadRequest,
	})
	// ErrNotFound 是 10002，资源不存在（非订单类）。
	ErrNotFound = mustRegister(&Error{
		Code:       10002,
		Msg:        "not found",
		HTTPStatus: http.StatusNotFound,
	})
	// ErrInternal 是 10003。对外 msg 固定为 internal error。
	ErrInternal = mustRegister(&Error{
		Code:       10003,
		Msg:        "internal error",
		HTTPStatus: http.StatusInternalServerError,
	})
	// ErrUnavailable 是 10004，依赖暂时不可用。
	ErrUnavailable = mustRegister(&Error{
		Code:       10004,
		Msg:        "service unavailable",
		HTTPStatus: http.StatusServiceUnavailable,
	})
	// ErrRateLimited 是 10005。Retry-After 由网关填写，不在本包。
	ErrRateLimited = mustRegister(&Error{
		Code:       10005,
		Msg:        "rate limited",
		HTTPStatus: http.StatusTooManyRequests,
	})
	// ErrIdempotentConflict 是 10006，幂等键相同但参数不同。
	ErrIdempotentConflict = mustRegister(&Error{
		Code:       10006,
		Msg:        "idempotent conflict",
		HTTPStatus: http.StatusConflict,
	})

	// ErrSignInvalid 是 20001。不区分密钥错误和 body 被篡改。
	ErrSignInvalid = mustRegister(&Error{
		Code:       20001,
		Msg:        "invalid signature",
		HTTPStatus: http.StatusUnauthorized,
	})
	// ErrTimestampExpired 是 20002。
	ErrTimestampExpired = mustRegister(&Error{
		Code:       20002,
		Msg:        "timestamp expired",
		HTTPStatus: http.StatusUnauthorized,
	})
	// ErrNonceReplayed 是 20003。
	ErrNonceReplayed = mustRegister(&Error{
		Code:       20003,
		Msg:        "nonce replayed",
		HTTPStatus: http.StatusUnauthorized,
	})
	// ErrMerchantInvalid 是 20004，商户不存在或已停用。
	ErrMerchantInvalid = mustRegister(&Error{
		Code:       20004,
		Msg:        "invalid merchant",
		HTTPStatus: http.StatusUnauthorized,
	})
	// ErrAdminUnauthorized 是 20005，管理 Token 无效。
	ErrAdminUnauthorized = mustRegister(&Error{
		Code:       20005,
		Msg:        "admin unauthorized",
		HTTPStatus: http.StatusUnauthorized,
	})

	// ErrOrderNotFound 是 30001。
	ErrOrderNotFound = mustRegister(&Error{
		Code:       30001,
		Msg:        "order not found",
		HTTPStatus: http.StatusNotFound,
	})
	// ErrOrderStatusConflict 是 30003。
	ErrOrderStatusConflict = mustRegister(&Error{
		Code:       30003,
		Msg:        "order status conflict",
		HTTPStatus: http.StatusConflict,
	})
	// ErrOrderExpired 是 30004。
	ErrOrderExpired = mustRegister(&Error{
		Code:       30004,
		Msg:        "order expired",
		HTTPStatus: http.StatusConflict,
	})
	// ErrRefundExceeded 是 30005。
	ErrRefundExceeded = mustRegister(&Error{
		Code:       30005,
		Msg:        "refund exceeded",
		HTTPStatus: http.StatusConflict,
	})
	// ErrChannelRejected 是 30006。约定的 HTTP 状态是 200，单据由调用方放进 data。
	ErrChannelRejected = mustRegister(&Error{
		Code:       30006,
		Msg:        "channel rejected",
		HTTPStatus: http.StatusOK,
	})
	// ErrChannelUnsupported 是 30007。
	ErrChannelUnsupported = mustRegister(&Error{
		Code:       30007,
		Msg:        "unsupported channel",
		HTTPStatus: http.StatusBadRequest,
	})
	// ErrPayeeInvalid 是 30008。
	ErrPayeeInvalid = mustRegister(&Error{
		Code:       30008,
		Msg:        "invalid payee",
		HTTPStatus: http.StatusBadRequest,
	})

	// ErrAccountNotFound 是 40001。
	ErrAccountNotFound = mustRegister(&Error{
		Code:       40001,
		Msg:        "account not found",
		HTTPStatus: http.StatusNotFound,
	})
	// ErrInsufficientBalance 是 40002。
	ErrInsufficientBalance = mustRegister(&Error{
		Code:       40002,
		Msg:        "insufficient balance",
		HTTPStatus: http.StatusConflict,
	})
	// ErrUnbalancedEntries 是 40003。
	ErrUnbalancedEntries = mustRegister(&Error{
		Code:       40003,
		Msg:        "unbalanced entries",
		HTTPStatus: http.StatusBadRequest,
	})
	// ErrFreezeStatusConflict 是 40004。
	ErrFreezeStatusConflict = mustRegister(&Error{
		Code:       40004,
		Msg:        "freeze status conflict",
		HTTPStatus: http.StatusConflict,
	})
	// ErrAccountInactive 是 40005。
	ErrAccountInactive = mustRegister(&Error{
		Code:       40005,
		Msg:        "account inactive",
		HTTPStatus: http.StatusConflict,
	})

	// ErrChannelSignInvalid 是 50001。
	ErrChannelSignInvalid = mustRegister(&Error{
		Code:       50001,
		Msg:        "invalid channel signature",
		HTTPStatus: http.StatusUnauthorized,
	})
	// ErrChannelAmountMismatch 是 50002。
	ErrChannelAmountMismatch = mustRegister(&Error{
		Code:       50002,
		Msg:        "channel amount mismatch",
		HTTPStatus: http.StatusBadRequest,
	})
)
