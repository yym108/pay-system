-- REC-01 新增：持久化 BILL_NOT_READY 重试次数。
ALTER TABLE recon_batch
  ADD COLUMN retry_count INT NOT NULL DEFAULT 0 AFTER error_msg;
