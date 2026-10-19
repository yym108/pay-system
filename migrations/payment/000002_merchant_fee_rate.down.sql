-- PAY-10 回滚。先删索引，再删费率历史。不删除 payment_order。

ALTER TABLE payment_order DROP INDEX idx_pay_merchant_created;

DROP TABLE IF EXISTS merchant_fee_rate;
