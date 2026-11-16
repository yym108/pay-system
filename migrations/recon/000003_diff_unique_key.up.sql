-- REC-03：将 recon_diff 唯一键由 (batch_no, txn_type, biz_no) 改为 (batch_no, txn_type, biz_no, diff_type)，
-- 使同一笔交易在同一批次可并存外部差错与内部差错。
ALTER TABLE recon_diff
  DROP INDEX uk_batch_type_biz_no,
  ADD UNIQUE KEY uk_batch_type_biz_no_diff_type (batch_no, txn_type, biz_no, diff_type);
