-- BASE-03 账户分片初始表。对 pay_account_0、pay_account_1 各执行一次，不要在本文件里 CREATE DATABASE。
-- 连接字符集必须是 utf8mb4，否则中文注释会按连接字符集写入。

CREATE TABLE account (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  account_no        VARCHAR(32)     NOT NULL COMMENT '账户号，AC 前缀',
  owner_type        VARCHAR(16)     NOT NULL COMMENT 'MERCHANT / PLATFORM',
  owner_id          VARCHAR(32)     NOT NULL COMMENT '商户号；平台账户为渠道编码（CHANNEL_RECEIVABLE）或 PLATFORM',
  account_type      VARCHAR(32)     NOT NULL COMMENT 'MERCHANT_SETTLE / CHANNEL_RECEIVABLE / FEE_INCOME / TRANSIT',
  currency          CHAR(3)         NOT NULL DEFAULT 'CNY',
  sub_no            INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '子账户号：商户账户恒为 0；平台账户 0..PLATFORM_SUB_ACCOUNTS-1',
  balance_direction VARCHAR(8)      NOT NULL COMMENT '余额方向 DEBIT / CREDIT',
  balance           BIGINT          NOT NULL DEFAULT 0 COMMENT '余额，分',
  frozen_amount     BIGINT          NOT NULL DEFAULT 0 COMMENT '冻结金额，分',
  allow_overdraft   TINYINT         NOT NULL DEFAULT 0 COMMENT '是否允许可用余额为负，仅平台账户',
  status            VARCHAR(16)     NOT NULL DEFAULT 'ACTIVE' COMMENT 'ACTIVE / FROZEN / CLOSED',
  version           BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_account_no (account_no),
  UNIQUE KEY uk_owner_account_type_sub (owner_type, owner_id, account_type, currency, sub_no),
  CONSTRAINT ck_account_frozen       CHECK (frozen_amount >= 0),
  CONSTRAINT ck_account_available    CHECK (allow_overdraft = 1 OR balance - frozen_amount >= 0),
  CONSTRAINT ck_account_currency     CHECK (currency = 'CNY'),
  CONSTRAINT ck_account_direction    CHECK (balance_direction IN ('DEBIT', 'CREDIT')),
  CONSTRAINT ck_account_status       CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
  -- 账户类型决定 owner_type、余额方向与是否允许透支（需求 §2.5.1）
  CONSTRAINT ck_account_type_rule    CHECK (
       (account_type = 'MERCHANT_SETTLE'    AND owner_type = 'MERCHANT' AND balance_direction = 'CREDIT' AND allow_overdraft = 0)
    OR (account_type = 'CHANNEL_RECEIVABLE' AND owner_type = 'PLATFORM' AND balance_direction = 'DEBIT'  AND allow_overdraft = 1)
    OR (account_type IN ('FEE_INCOME', 'TRANSIT') AND owner_type = 'PLATFORM' AND balance_direction = 'CREDIT' AND allow_overdraft = 1)
  ),
  CONSTRAINT ck_account_merchant_sub CHECK (owner_type <> 'MERCHANT' OR sub_no = 0)
) ENGINE = InnoDB COMMENT = '账户';

CREATE TABLE ledger_txn (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  txn_no      VARCHAR(32)     NOT NULL COMMENT '记账凭证号，TX 前缀',
  biz_type    VARCHAR(32)     NOT NULL COMMENT 'PAY / REFUND / TRANSFER / TRANSFER_OUT / TRANSFER_IN / REVERSAL',
  biz_no      VARCHAR(64)     NOT NULL COMMENT '业务单号，幂等键',
  amount      BIGINT          NOT NULL COMMENT '借方合计 = 贷方合计',
  currency    CHAR(3)         NOT NULL DEFAULT 'CNY',
  entry_count INT             NOT NULL,
  reversal_of VARCHAR(32)     NULL COMMENT '被冲正的 txn_no',
  memo        VARCHAR(255)    NOT NULL DEFAULT '',
  created_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_txn_no (txn_no),
  UNIQUE KEY uk_biz (biz_type, biz_no),
  UNIQUE KEY uk_reversal_of (reversal_of),
  KEY idx_created_at (created_at),
  CONSTRAINT ck_ledger_txn_amount   CHECK (amount > 0),
  CONSTRAINT ck_ledger_txn_entries  CHECK (entry_count >= 2),
  CONSTRAINT ck_ledger_txn_biz_type CHECK (biz_type IN ('PAY', 'REFUND', 'TRANSFER', 'TRANSFER_OUT', 'TRANSFER_IN', 'REVERSAL')),
  CONSTRAINT ck_ledger_txn_reversal CHECK ((biz_type = 'REVERSAL') = (reversal_of IS NOT NULL))
) ENGINE = InnoDB COMMENT = '记账凭证（只追加）';

CREATE TABLE ledger_entry (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  txn_no        VARCHAR(32)     NOT NULL,
  account_id    BIGINT UNSIGNED NOT NULL,
  direction     VARCHAR(8)      NOT NULL COMMENT 'DEBIT / CREDIT',
  amount        BIGINT          NOT NULL COMMENT '分，恒为正',
  balance_after BIGINT          NOT NULL COMMENT '本分录记账后账户余额',
  biz_type      VARCHAR(32)     NOT NULL COMMENT '冗余自 ledger_txn',
  biz_no        VARCHAR(64)     NOT NULL COMMENT '冗余自 ledger_txn',
  created_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_txn_account_direction (txn_no, account_id, direction),
  KEY idx_account_id_id (account_id, id),
  KEY idx_created_at (created_at),
  CONSTRAINT ck_ledger_entry_amount    CHECK (amount > 0),
  CONSTRAINT ck_ledger_entry_direction CHECK (direction IN ('DEBIT', 'CREDIT'))
) ENGINE = InnoDB COMMENT = '记账分录（只追加）';

CREATE TABLE account_freeze (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  freeze_no  VARCHAR(32)     NOT NULL COMMENT 'FZ 前缀',
  biz_type   VARCHAR(32)     NOT NULL COMMENT 'REFUND / TRANSFER',
  biz_no     VARCHAR(64)     NOT NULL COMMENT '业务单号，幂等键：refund_no 或 transfer_no',
  account_id BIGINT UNSIGNED NOT NULL,
  amount     BIGINT          NOT NULL,
  status     VARCHAR(16)     NOT NULL COMMENT 'FROZEN / CONFIRMED / RELEASED',
  txn_no     VARCHAR(32)     NULL COMMENT 'CONFIRMED 时对应的记账凭证（REFUND / TRANSFER_OUT）',
  version    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_freeze_no (freeze_no),
  UNIQUE KEY uk_biz (biz_type, biz_no),
  KEY idx_account_status (account_id, status),
  CONSTRAINT ck_freeze_amount   CHECK (amount > 0),
  CONSTRAINT ck_freeze_status   CHECK (status IN ('FROZEN', 'CONFIRMED', 'RELEASED')),
  CONSTRAINT ck_freeze_biz_type CHECK (biz_type IN ('REFUND', 'TRANSFER'))
) ENGINE = InnoDB COMMENT = '资金冻结单';

CREATE TABLE transfer_branch (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  transfer_no       VARCHAR(32)     NOT NULL COMMENT '转账单号，TF 前缀',
  role              VARCHAR(16)     NOT NULL COMMENT 'LOCAL（同分片转账结果）/ PAYEE（跨分片收款方分支）',
  payer_merchant_id VARCHAR(32)     NULL COMMENT '付款商户，仅 LOCAL',
  payee_merchant_id VARCHAR(32)     NOT NULL COMMENT '收款商户',
  amount            BIGINT          NOT NULL,
  status            VARCHAR(16)     NOT NULL COMMENT 'LOCAL：SUCCESS / FAILED；PAYEE：TRIED / CONFIRMED / CANCELLED',
  fail_code         VARCHAR(32)     NULL COMMENT 'LOCAL FAILED 的原因码',
  txn_no            VARCHAR(32)     NULL COMMENT 'LOCAL SUCCESS 的 TRANSFER 凭证或 PAYEE CONFIRMED 的 TRANSFER_IN 凭证',
  version           BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_transfer_role (transfer_no, role),
  CONSTRAINT ck_branch_amount    CHECK (amount > 0),
  CONSTRAINT ck_branch_role      CHECK (role IN ('LOCAL', 'PAYEE')),
  CONSTRAINT ck_branch_status    CHECK (
       (role = 'LOCAL' AND status IN ('SUCCESS', 'FAILED'))
    OR (role = 'PAYEE' AND status IN ('TRIED', 'CONFIRMED', 'CANCELLED'))
  ),
  CONSTRAINT ck_branch_payer     CHECK ((role = 'LOCAL') = (payer_merchant_id IS NOT NULL)),
  CONSTRAINT ck_branch_self      CHECK (payer_merchant_id IS NULL OR payer_merchant_id <> payee_merchant_id),
  CONSTRAINT ck_branch_fail_code CHECK ((role = 'LOCAL' AND status = 'FAILED') = (fail_code IS NOT NULL))
) ENGINE = InnoDB COMMENT = '转账分支（同分片结果 / 收款方分支）';
