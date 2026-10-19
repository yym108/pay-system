-- PAY-10 商户费率历史。只追加，不改 merchant.fee_rate_bp。
-- 已有商户用当时的费率和 created_at 补第一档；没有商户时这条 INSERT 写入 0 行。

CREATE TABLE merchant_fee_rate (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  merchant_id    VARCHAR(32)     NOT NULL,
  fee_rate_bp    INT             NOT NULL,
  effective_from DATETIME(3)     NOT NULL,
  created_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_merchant_effective_from (merchant_id, effective_from),
  CONSTRAINT ck_mfr_fee_rate CHECK (fee_rate_bp BETWEEN 0 AND 10000)
) ENGINE = InnoDB COMMENT = '商户费率历史（只追加）';

INSERT INTO merchant_fee_rate (merchant_id, fee_rate_bp, effective_from)
SELECT merchant_id, fee_rate_bp, created_at FROM merchant;

ALTER TABLE payment_order
  ADD INDEX idx_pay_merchant_created (merchant_id, created_at);
