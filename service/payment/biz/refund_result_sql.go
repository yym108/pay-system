package biz

// 退款渠道流水的 CAS。规则同 PAY-04 第 3 条，另加 txn_type = 'REFUND'，按退款单号不会改到支付流水。
// 四个变体都由常量拼成，不含调用方输入。
const (
	refundChannelSetSQL = `
UPDATE channel_txn
   SET status = ?,
       channel_txn_no = COALESCE(channel_txn_no, ?),
       channel_status_raw = ?,
       channel_finished_at = ?,
       next_query_at = NULL,`
	refundChannelTailSQL = `
       version = version + 1,
       updated_at = ?
 WHERE biz_no = ? AND txn_type = 'REFUND' AND status = 'PROCESSING'`

	applyRefundChannelSQL             = refundChannelSetSQL + refundChannelTailSQL
	applyRefundChannelAmountSQL       = applyRefundChannelSQL + ` AND amount = ?`
	applyRefundChannelNotifySQL       = refundChannelSetSQL + "\n       notify_count = notify_count + 1," + refundChannelTailSQL
	applyRefundChannelNotifyAmountSQL = applyRefundChannelNotifySQL + ` AND amount = ?`
)

const applyRefundSuccessSQL = `
UPDATE refund_order
   SET status = 'SUCCESS', succeeded_at = ?, version = version + 1, updated_at = ?
 WHERE refund_no = ? AND status = 'PROCESSING'`

const applyRefundFailedSQL = `
UPDATE refund_order
   SET status = 'FAILED', fail_code = ?, fail_msg = ?, version = version + 1, updated_at = ?
 WHERE refund_no = ? AND status = 'PROCESSING'`

const applyRefundInitFailedSQL = `
UPDATE refund_order
   SET status = 'FAILED', fail_code = ?, fail_msg = ?, version = version + 1, updated_at = ?
 WHERE refund_no = ? AND status = 'INIT'`

// 释放额度用条件更新，不先查后改。refunded_amount >= R 不成立说明数据已不一致（REF-03 第 5 条）。
const releaseRefundQuotaSQL = `
UPDATE payment_order
   SET refunded_amount = refunded_amount - ?, version = version + 1, updated_at = ?
 WHERE pay_no = ? AND refunded_amount >= ?`
