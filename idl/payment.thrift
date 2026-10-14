// PaymentService。方法只来自 docs/05-api.md §10.2。
// 未标注 optional 的字段是本版必填（Thrift 默认 requiredness）。状态是字符串，取值与数据库 CHECK 一致。

include "base.thrift"

namespace go payment

struct PaymentOrder {
    1: string pay_no
    2: string out_trade_no
    3: i64 amount
    4: i64 fee_amount
    5: i64 refunded_amount
    6: string currency
    7: string subject
    // INIT / PAYING / SUCCESS / FAILED / CLOSED。
    8: string status
    // 已发起支付后才有。
    9: optional string channel
    10: base.Timestamp expire_at
    // 仅 SUCCESS。
    11: optional base.Timestamp paid_at
    // 仅 CLOSED。
    12: optional base.Timestamp closed_at
    // 仅 FAILED。
    13: optional string fail_code
    14: optional string fail_msg
    15: base.Timestamp created_at
}

struct RefundOrder {
    1: string refund_no
    2: string out_refund_no
    3: string pay_no
    4: i64 amount
    5: string currency
    // 可为空串，字段仍在。
    6: string reason
    // INIT / PROCESSING / SUCCESS / FAILED。
    7: string status
    // 仅 SUCCESS。
    8: optional base.Timestamp succeeded_at
    // 仅 FAILED。
    9: optional string fail_code
    10: optional string fail_msg
    11: base.Timestamp created_at
}

struct Transfer {
    1: string transfer_no
    2: string out_transfer_no
    3: string merchant_id
    4: string payee_merchant_id
    5: i64 amount
    6: string currency
    // LOCAL / TCC。
    7: string mode
    // TRYING / CONFIRMING / CANCELLING / SUCCESS / FAILED。
    8: string status
    // 仅 CANCELLING、FAILED。
    9: optional string fail_code
    10: base.Timestamp created_at
    // 仅 SUCCESS、FAILED。
    11: optional base.Timestamp finished_at
}

struct CreatePaymentRequest {
    1: string merchant_id
    2: string out_trade_no
    3: i64 amount
    // 省略视为 CNY。
    4: optional string currency
    5: string subject
    6: optional string notify_url
    // 省略视为 15。
    7: optional i32 expire_minutes
}

struct PayRequest {
    1: string merchant_id
    2: string pay_no
    3: string channel
}

struct ClosePaymentRequest {
    1: string merchant_id
    2: string pay_no
}

struct QueryPaymentRequest {
    1: string merchant_id
    // pay_no 与 out_trade_no 二选一。
    2: optional string pay_no
    3: optional string out_trade_no
}

struct CreateRefundRequest {
    1: string merchant_id
    // pay_no 与 out_trade_no 二选一。
    2: optional string pay_no
    3: optional string out_trade_no
    4: string out_refund_no
    5: i64 amount
    6: optional string reason
}

// 正常响应带 refund_order。40002、30006 不走 BizStatusError，把错误码放在 biz_code。
struct CreateRefundResponse {
    1: base.BizCode biz_code
    2: RefundOrder refund_order
}

struct QueryRefundRequest {
    1: string merchant_id
    // refund_no 与 out_refund_no 二选一。
    2: optional string refund_no
    3: optional string out_refund_no
}

struct CreateTransferRequest {
    1: string merchant_id
    2: string out_transfer_no
    3: string payee_merchant_id
    4: i64 amount
    5: optional string remark
    6: optional string notify_url
}

// 正常响应带 transfer。本次执行到余额不足时 biz_code 为 40002，并携带已失败的转账单。
struct CreateTransferResponse {
    1: base.BizCode biz_code
    2: Transfer transfer
}

struct QueryTransferRequest {
    1: string merchant_id
    // transfer_no 与 out_transfer_no 二选一。
    2: optional string transfer_no
    3: optional string out_transfer_no
}

struct HandleChannelNotifyRequest {
    1: string channel
    2: string notify_id
    // PAY / REFUND。
    3: string txn_type
    4: string req_no
    5: string channel_txn_no
    6: i64 amount
    // SUCCESS / FAILED。
    7: string status
    8: base.Timestamp finished_at
}

struct HandleChannelNotifyResponse {
    // APPLIED / DUPLICATE / CONFLICT。
    1: string result
}

struct GetMerchantCredentialRequest {
    1: string app_id
}

struct GetMerchantCredentialResponse {
    1: string merchant_id
    // ACTIVE / DISABLED。
    2: string status
    // 明文密钥。调用方不得写入日志。
    3: string app_secret
    4: i32 secret_version
    5: string default_notify_url
}

struct SyncChannelTxnRequest {
    1: string biz_no
}

struct SyncChannelTxnResponse {
    // 同步后的本地渠道交易状态：PROCESSING / SUCCESS / FAILED / CLOSED。
    1: string local_status
    // 渠道无记录时为 NOT_FOUND。
    2: string channel_status
    3: bool changed
}

struct ReplayEventRequest {
    // event_id，或 biz_no + event_type，二选一。
    1: optional string event_id
    2: optional string biz_no
    3: optional string event_type
}

struct ReplayEventResponse {
    1: string event_id
    2: bool rebuilt
}

service PaymentService {
    PaymentOrder CreatePayment(1: CreatePaymentRequest req)
    PaymentOrder Pay(1: PayRequest req)
    PaymentOrder ClosePayment(1: ClosePaymentRequest req)
    PaymentOrder QueryPayment(1: QueryPaymentRequest req)
    CreateRefundResponse CreateRefund(1: CreateRefundRequest req)
    RefundOrder QueryRefund(1: QueryRefundRequest req)
    CreateTransferResponse CreateTransfer(1: CreateTransferRequest req)
    Transfer QueryTransfer(1: QueryTransferRequest req)
    HandleChannelNotifyResponse HandleChannelNotify(1: HandleChannelNotifyRequest req)
    GetMerchantCredentialResponse GetMerchantCredential(1: GetMerchantCredentialRequest req)
    SyncChannelTxnResponse SyncChannelTxn(1: SyncChannelTxnRequest req)
    ReplayEventResponse ReplayEvent(1: ReplayEventRequest req)
}
