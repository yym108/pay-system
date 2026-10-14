// AccountService。方法只来自 docs/05-api.md §10.1。
// Post 是 account 进程内记账，不是 RPC，不在本文件中。
// 未标注 optional 的字段是本版必填（Thrift 默认 requiredness）。状态是字符串，取值与数据库 CHECK 一致。

namespace go account

// Account 是账户或平台汇总。平台汇总时 account_no 为空串，shard、sub_no 省略。
struct Account {
    1: string account_no
    2: string owner_type
    3: string owner_id
    4: string account_type
    5: optional i32 shard
    6: optional i32 sub_no
    7: string currency
    8: string balance_direction
    9: i64 balance
    10: i64 frozen_amount
    11: i64 available_amount
    // ACTIVE / FROZEN / CLOSED。
    12: string status
    13: i32 sub_count
}

struct OpenAccountRequest {
    1: string owner_type
    2: string owner_id
    3: string account_type
    4: i32 shard
    5: i32 sub_no
}

struct GetAccountRequest {
    1: string owner_type
    2: string owner_id
    3: string account_type
}

struct FreezeRequest {
    // REFUND / TRANSFER。
    1: string biz_type
    2: string biz_no
    3: string merchant_id
    4: i64 amount
}

struct FreezeResponse {
    1: string freeze_no
    // FROZEN / CONFIRMED / RELEASED。
    2: string status
}

struct ReleaseFreezeRequest {
    1: string biz_type
    2: string biz_no
    3: string merchant_id
    4: i64 amount
}

struct ReleaseFreezeResponse {
    1: string freeze_no
    // FROZEN / CONFIRMED / RELEASED。
    2: string status
}

struct ConfirmFreezeRequest {
    1: string biz_type
    2: string biz_no
    3: string merchant_id
    // TRANSFER 传空串。字段仍在，不省略。
    4: string channel
}

struct ConfirmFreezeResponse {
    1: string freeze_no
    2: string txn_no
}

struct TransferLocalRequest {
    1: string transfer_no
    2: string payer_merchant_id
    3: string payee_merchant_id
    4: i64 amount
}

struct TransferLocalResponse {
    // SUCCESS / FAILED。
    1: string status
    // 仅 SUCCESS。
    2: optional string txn_no
    // 仅 FAILED，取值为 INSUFFICIENT_BALANCE。
    3: optional string fail_code
}

struct TransferInTryRequest {
    1: string transfer_no
    2: string payee_merchant_id
    3: i64 amount
}

struct TransferInTryResponse {
    // TRIED。
    1: string status
}

struct TransferInConfirmRequest {
    1: string transfer_no
    2: string payee_merchant_id
}

struct TransferInConfirmResponse {
    // CONFIRMED。
    1: string status
    2: string txn_no
}

struct TransferInCancelRequest {
    1: string transfer_no
    2: string payee_merchant_id
    3: i64 amount
}

struct TransferInCancelResponse {
    // CANCELLED。
    1: string status
}

struct CheckBalanceRequest {
    // 省略或空串时全量核对两个分片。
    1: optional string merchant_id
    // 省略视为 false。为 true 时要求在途合计为 0，仅全量模式有效。
    2: optional bool expect_quiescent
}

// BalanceFailure 是一项核对失败。分片级核对项省略 account_no；TRANSIT_ZERO 省略 shard 与 account_no。
struct BalanceFailure {
    1: optional i32 shard
    2: optional string account_no
    3: string check
    4: i64 expected
    5: i64 actual
}

struct CheckBalanceResponse {
    1: bool passed
    2: list<BalanceFailure> failures
}

struct ReverseRequest {
    1: i32 shard
    2: string txn_no
    3: string reason
}

struct ReverseResponse {
    1: string reversal_txn_no
    2: bool duplicated
}

service AccountService {
    Account OpenAccount(1: OpenAccountRequest req)
    Account GetAccount(1: GetAccountRequest req)
    FreezeResponse Freeze(1: FreezeRequest req)
    ReleaseFreezeResponse ReleaseFreeze(1: ReleaseFreezeRequest req)
    ConfirmFreezeResponse ConfirmFreeze(1: ConfirmFreezeRequest req)
    TransferLocalResponse TransferLocal(1: TransferLocalRequest req)
    TransferInTryResponse TransferInTry(1: TransferInTryRequest req)
    TransferInConfirmResponse TransferInConfirm(1: TransferInConfirmRequest req)
    TransferInCancelResponse TransferInCancel(1: TransferInCancelRequest req)
    CheckBalanceResponse CheckBalance(1: CheckBalanceRequest req)
    ReverseResponse Reverse(1: ReverseRequest req)
}
