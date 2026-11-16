ALTER TABLE recon_diff
  DROP INDEX uk_batch_type_biz_no_diff_type,
  ADD UNIQUE KEY uk_batch_type_biz_no (batch_no, txn_type, biz_no);
