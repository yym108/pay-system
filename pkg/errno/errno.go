// Package errno 是业务错误码的唯一定义处。
// 对外消息是稳定的短英文。底层原因只通过 Unwrap 取得，不进入 Error 的文本。
// MySQL 1062 不在本包映射。dbx.IsDuplicate 只识别错误号，不定义 ErrDuplicate。
package errno

import (
	"context"
	"errors"

	"github.com/cloudwego/kitex/pkg/kerrors"
)

// Error 是一个业务错误码。
// Msg 是对外稳定英文，不含内部细节。调用方不要修改哨兵的字段。
type Error struct {
	Code       int32
	Msg        string
	HTTPStatus int
	cause      error
}

// Error 返回对外消息，不包含底层错误文本。
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Msg
}

// Unwrap 返回包装的底层错误。没有时为 nil。
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Is 按业务码比较，使 errors.Is 能识别包装后的原错误码。
func (e *Error) Is(target error) bool {
	if e == nil || target == nil {
		return false
	}
	other, ok := target.(*Error)
	if !ok || other == nil {
		return false
	}
	return e.Code == other.Code
}

// Wrap 复制业务码并挂上 cause。
// cause 为 nil 时返回原哨兵，不修改哨兵，避免并发包装互相覆盖。
// e 为 nil 时与 From(cause) 相同。cause 不写入 Msg，也不进入 Error 的文本。
func Wrap(e *Error, cause error) *Error {
	if e == nil {
		return From(cause)
	}
	if cause == nil {
		return e
	}
	out := *e
	out.cause = cause
	return &out
}

// From 把任意 error 解释成业务码，供填写对外响应。
// nil 返回 OK（码 0，msg ok，HTTP 200），不是 10003。成功时返回值也是非 nil，不要把它当作失败再返回。
// 错误链上已有 *Error 时保留该码。Kitex BizStatusError 按码还原，不用对端消息替换本表 Msg。
// 其余非 nil 错误返回 10003，Msg 固定为 internal error，原错误只通过 Unwrap 取得。
func From(err error) *Error {
	if err == nil {
		return OK
	}
	var known *Error
	if errors.As(err, &known) && known != nil {
		return known
	}
	if biz, ok := kerrors.FromBizStatusError(err); ok && biz != nil {
		return fromBizCode(biz.BizStatusCode(), err)
	}
	return Wrap(ErrInternal, err)
}

// ToBizStatus 把业务错误交给 Kitex 服务端返回。
// nil 和成功码返回 nil，避免把成功变成业务错误。线上的消息只用 Msg。
func ToBizStatus(e *Error) kerrors.BizStatusErrorIface {
	if e == nil || e.Code == OK.Code {
		return nil
	}
	return kerrors.NewBizStatusError(e.Code, e.Msg)
}

// FromBizStatus 用 kerrors.FromBizStatusError 把客户端错误还原成同一个业务码。
// nil 返回 OK。不是 BizStatusError 时按 From 处理。
// 未登记的码（含保留的 30002）视为 10003，对外消息不用对端文本。
func FromBizStatus(err error) *Error {
	if err == nil {
		return OK
	}
	biz, ok := kerrors.FromBizStatusError(err)
	if !ok || biz == nil {
		return From(err)
	}
	return fromBizCode(biz.BizStatusCode(), err)
}

// RPCExit 把一个错误分成 RPC 的两种出口。
// carryObject 为 true 时，已登记且不是 10003 的业务错误把码放进 bizCode，rpcErr 为 nil。
// 调用方把 bizCode 写入响应的 biz_code，并按正常响应返回对象。
// 这只用于约定 §5.3 第 5 条点名的、必须连同对象返回的错误，例如退款和转账的 40002、30006。
// 调用方只在这些错误上把 carryObject 设为 true。10003 即使误设，仍走 BizStatusError，不放进 biz_code。
// 其他错误一律用 BizStatusError。上下文取消、截止时间和 Kitex 超时保持传输层超时，不变成 10003，
// 否则幂等调用方无法把“结果未知”和“内部错误”分开。
// nil 是成功：bizCode 为 0，rpcErr 为 nil。
func RPCExit(err error, carryObject bool) (bizCode int32, rpcErr error) {
	if err == nil {
		return 0, nil
	}
	if isTransportTimeout(err) {
		return 0, kerrors.ErrRPCTimeout
	}
	e := From(err)
	if e == nil || e.Code == OK.Code {
		return 0, nil
	}
	// 未知错误经 From 变成 10003。携带对象的出口不接收它，避免把内部错误伪装成正常响应。
	if carryObject && e.Code != ErrInternal.Code {
		return e.Code, nil
	}
	return 0, ToBizStatus(e)
}

func isTransportTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		kerrors.IsTimeoutError(err)
}

// fromBizCode 只用已登记的码查找哨兵。对端 msg 故意不采用。
// 未登记码视为 10003；cause 为 nil 时 Wrap 返回哨兵本身。
func fromBizCode(code int32, cause error) *Error {
	if known, ok := byCode[code]; ok {
		return known
	}
	return Wrap(ErrInternal, cause)
}
