// ReconService。方法只来自 docs/05-api.md §10.3。
// 未标注 optional 的字段是本版必填（Thrift 默认 requiredness）。状态是字符串，取值与数据库 CHECK 一致。

include "base.thrift"

namespace go recon

struct ReconBatch {
    1: string batch_no
    2: string channel
    // YYYY-MM-DD，东八区。不是 RFC3339。
    3: string bill_date
    // INIT / DOWNLOADED / CHECKING / DONE / FAILED。
    4: string status
    5: i32 channel_count
    6: i64 channel_amount
    7: i32 local_count
    8: i64 local_amount
    9: i32 matched_count
    10: i32 diff_count
    11: string error_msg
    // 未进入 CHECKING 时省略。
    12: optional base.Timestamp started_at
    // 未结束时省略。
    13: optional base.Timestamp finished_at
}

struct ReconDiff {
    1: string diff_no
    2: string batch_no
    3: string channel
    4: string bill_date
    // LOCAL_MISSING / CHANNEL_MISSING / AMOUNT_MISMATCH / STATUS_MISMATCH / LEDGER_MISSING / TRANSFER_MISMATCH。
    5: string diff_type
    // PAY / REFUND / TRANSFER。
    6: string txn_type
    7: string biz_no
    // 无对应方时省略。
    8: optional string channel_txn_no
    9: optional i64 local_amount
    10: optional i64 channel_amount
    11: optional string local_status
    12: optional string channel_status
    // PENDING / RESOLVED / IGNORED。
    13: string status
    // 未处理时省略。REQUERY / REPLAY。
    14: optional string handle_action
    15: string handler
    16: string handle_note
    // 未解决时省略。
    17: optional base.Timestamp resolved_at
    18: base.Timestamp created_at
}

struct RunBatchRequest {
    1: string channel
    2: string bill_date
    // 省略视为 false。
    3: optional bool force
}

struct RunBatchResponse {
    1: ReconBatch batch
    // 最多 100 条，可以是空列表。
    2: list<ReconDiff> diffs
}

service ReconService {
    RunBatchResponse RunBatch(1: RunBatchRequest req)
}
