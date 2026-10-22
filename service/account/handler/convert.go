package handler

import (
	"context"
	"log/slog"
	"math"
	"strconv"

	"github.com/cloudwego/kitex/pkg/kerrors"

	"github.com/yym108/pay-system/kitex_gen/account"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/service/account/biz"
)

// statusErr 把 biz 错误交给 Kitex。
// AccountService 没有携带对象的错误，一律 carryObject=false，走 BizStatusError。
// 约定 §5.3 第 5 条的 biz_code 由 errno.RPCExit 实现，给以后的 payment handler 用。
// 映射成 10003 且带有原因时，原因只进日志，不进 BizMessage。
func statusErr(ctx context.Context, err error) error {
	return exitErr(ctx, err)
}

func exitErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	_, rpcErr := errno.RPCExit(err, false)
	logInternalCause(ctx, err, rpcErr)
	return rpcErr
}

// logInternalCause 落实约定 §5.3 第 3 条：未识别错误转成 10003 时，原因进日志，不进对外 msg。
// 登记过的业务错误（含没有 Unwrap 的 10003 哨兵）不打 ERROR。
// 走 slog，进程已 SetDefault 时由 logx 脱敏。字段名用约定 §7.5：path 是方法名，err_code 是码。
func logInternalCause(ctx context.Context, raw, rpcErr error) {
	biz, ok := kerrors.FromBizStatusError(rpcErr)
	if !ok || biz == nil || biz.BizStatusCode() != errno.ErrInternal.Code {
		return
	}
	e := errno.From(raw)
	if e == nil || e.Unwrap() == nil {
		return
	}
	slog.ErrorContext(ctx, "map internal error",
		"path", methodName(ctx),
		"err_code", strconv.FormatInt(int64(errno.ErrInternal.Code), 10),
		"error", e.Unwrap(),
	)
}

// toAccount 映射账户。空账户号是平台汇总。
// biz 把汇总的 shard、sub_no 写成 0，表示省略，不是分片 0、子账户 0。
// 有账户号时即使这两个数是 0，也要传指针，否则开在分片 0 的户会被看成汇总。
func toAccount(in biz.Account) *account.Account {
	out := &account.Account{
		AccountNo:        in.AccountNo,
		OwnerType:        in.OwnerType,
		OwnerId:          in.OwnerID,
		AccountType:      in.AccountType,
		Currency:         in.Currency,
		BalanceDirection: in.BalanceDirection,
		Balance:          int64(in.Balance),
		FrozenAmount:     int64(in.FrozenAmount),
		AvailableAmount:  int64(in.AvailableAmount),
		Status:           in.Status,
		SubCount:         toI32(in.SubCount),
	}
	if in.AccountNo != "" {
		shardNo := toI32(in.Shard)
		subNo := toI32(in.SubNo)
		out.Shard = &shardNo
		out.SubNo = &subNo
	}
	return out
}

func toFailures(in []biz.BalanceFailure) []*account.BalanceFailure {
	if len(in) == 0 {
		return nil
	}
	out := make([]*account.BalanceFailure, 0, len(in))
	for _, item := range in {
		row := &account.BalanceFailure{
			Check:    item.Check,
			Expected: int64(item.Expected),
			Actual:   int64(item.Actual),
		}
		if item.Shard != nil {
			shardNo := toI32(*item.Shard)
			row.Shard = &shardNo
		}
		if item.AccountNo != "" {
			no := item.AccountNo
			row.AccountNo = &no
		}
		out = append(out, row)
	}
	return out
}

// toI32 把已经检查过范围的 int 收成 i32。分片只有 0 和 1，子账户个数远小于 int32。
func toI32(v int) int32 {
	if v > math.MaxInt32 || v < math.MinInt32 {
		return 0
	}
	return int32(v) //nolint:gosec // 上一行已经挡住溢出
}

func optionalString(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
