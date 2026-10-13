-- pay-system 设计稿 DDL（MySQL 8.4）
-- 状态：设计稿。第 1 周按服务拆分为 migrations/<service>/000001_init.up.sql 后，以迁移文件为准并删除本文件。
-- 约定：每个服务独立 schema；金额 BIGINT 单位分；时间 DATETIME(3) 东八区；标识类字段大小写敏感（utf8mb4_bin）。
-- 账户分片：pay_account_0、pay_account_1 表结构完全相同（D17）。本文件在 pay_account_0 中完整定义，
--           pay_account_1 用 CREATE TABLE ... LIKE 复制（MySQL 8.0.16+ 同时复制索引与 CHECK 约束）。
--           迁移拆分后两个分片共用 migrations/account/，迁移容器对每个分片各执行一次（BASE-03、D16）。
--           注意：LIKE 复制的 CHECK 约束会被 MySQL 重命名为 <表名>_chk_<n>，因此本草稿中 pay_account_1 的约束名
--           与 pay_account_0 不同；按迁移文件分别执行时两个分片的约束名一致。

-- mysql 客户端经管道执行时可能默认 latin1，显式指定连接字符集，避免中文注释与 CHECK 字面量按 latin1 解析
SET NAMES utf8mb4;

-- =====================================================================
-- account 服务：分片 0（完整定义）
-- =====================================================================
CREATE DATABASE IF NOT EXISTS pay_account_0 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE pay_account_0;

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

-- =====================================================================
-- account 服务：分片 1（与分片 0 结构完全相同）
-- =====================================================================
CREATE DATABASE IF NOT EXISTS pay_account_1 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE pay_account_1.account         LIKE pay_account_0.account;
CREATE TABLE pay_account_1.ledger_txn      LIKE pay_account_0.ledger_txn;
CREATE TABLE pay_account_1.ledger_entry    LIKE pay_account_0.ledger_entry;
CREATE TABLE pay_account_1.account_freeze  LIKE pay_account_0.account_freeze;
CREATE TABLE pay_account_1.transfer_branch LIKE pay_account_0.transfer_branch;

-- =====================================================================
-- payment 服务
-- =====================================================================
CREATE DATABASE IF NOT EXISTS pay_payment DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE pay_payment;

CREATE TABLE merchant (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  merchant_id       VARCHAR(32)     NOT NULL,
  merchant_name     VARCHAR(128)    NOT NULL,
  app_id            VARCHAR(32)     NOT NULL,
  app_secret_cipher VARBINARY(256)  NOT NULL COMMENT 'AES-256-GCM 加密后的签名密钥',
  secret_version    INT             NOT NULL DEFAULT 1,
  notify_url        VARCHAR(512)    NOT NULL DEFAULT '',
  fee_rate_bp       INT             NOT NULL DEFAULT 0 COMMENT '费率，万分之一',
  status            VARCHAR(16)     NOT NULL DEFAULT 'ACTIVE' COMMENT 'ACTIVE / DISABLED',
  created_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_merchant_id (merchant_id),
  UNIQUE KEY uk_app_id (app_id),
  CONSTRAINT ck_merchant_fee_rate CHECK (fee_rate_bp BETWEEN 0 AND 10000),
  CONSTRAINT ck_merchant_status   CHECK (status IN ('ACTIVE', 'DISABLED'))
) ENGINE = InnoDB COMMENT = '商户';

CREATE TABLE payment_order (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  pay_no          VARCHAR(32)     NOT NULL COMMENT 'PA 前缀',
  merchant_id     VARCHAR(32)     NOT NULL,
  out_trade_no    VARCHAR(64)     NOT NULL COMMENT '商户订单号',
  amount          BIGINT          NOT NULL,
  currency        CHAR(3)         NOT NULL DEFAULT 'CNY',
  subject         VARCHAR(128)    NOT NULL,
  status          VARCHAR(16)     NOT NULL COMMENT 'INIT / PAYING / SUCCESS / FAILED / CLOSED',
  channel         VARCHAR(16)     NULL,
  fee_amount      BIGINT          NOT NULL DEFAULT 0,
  refunded_amount BIGINT          NOT NULL DEFAULT 0 COMMENT '已占用退款额 = 处理中 + 成功',
  notify_url      VARCHAR(512)    NOT NULL DEFAULT '',
  expire_at       DATETIME(3)     NOT NULL,
  paid_at         DATETIME(3)     NULL,
  closed_at       DATETIME(3)     NULL,
  fail_code       VARCHAR(32)     NULL,
  fail_msg        VARCHAR(255)    NULL,
  version         BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_pay_no (pay_no),
  UNIQUE KEY uk_merchant_out_trade_no (merchant_id, out_trade_no),
  KEY idx_status_expire_at (status, expire_at),
  CONSTRAINT ck_pay_amount   CHECK (amount > 0),
  CONSTRAINT ck_pay_fee      CHECK (fee_amount BETWEEN 0 AND amount),
  CONSTRAINT ck_pay_refunded CHECK (refunded_amount BETWEEN 0 AND amount),
  CONSTRAINT ck_pay_currency CHECK (currency = 'CNY'),
  CONSTRAINT ck_pay_status   CHECK (status IN ('INIT', 'PAYING', 'SUCCESS', 'FAILED', 'CLOSED'))
) ENGINE = InnoDB COMMENT = '支付单';

CREATE TABLE refund_order (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  refund_no     VARCHAR(32)     NOT NULL COMMENT 'RF 前缀',
  merchant_id   VARCHAR(32)     NOT NULL,
  out_refund_no VARCHAR(64)     NOT NULL COMMENT '商户退款单号',
  pay_no        VARCHAR(32)     NOT NULL,
  amount        BIGINT          NOT NULL,
  currency      CHAR(3)         NOT NULL DEFAULT 'CNY',
  reason        VARCHAR(255)    NOT NULL DEFAULT '',
  status        VARCHAR(16)     NOT NULL COMMENT 'INIT / PROCESSING / SUCCESS / FAILED',
  channel       VARCHAR(16)     NOT NULL,
  fail_code     VARCHAR(32)     NULL,
  fail_msg      VARCHAR(255)    NULL,
  succeeded_at  DATETIME(3)     NULL,
  version       BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_refund_no (refund_no),
  UNIQUE KEY uk_merchant_out_refund_no (merchant_id, out_refund_no),
  KEY idx_pay_no (pay_no),
  KEY idx_status_updated_at (status, updated_at),
  CONSTRAINT ck_refund_amount   CHECK (amount > 0),
  CONSTRAINT ck_refund_currency CHECK (currency = 'CNY'),
  CONSTRAINT ck_refund_status   CHECK (status IN ('INIT', 'PROCESSING', 'SUCCESS', 'FAILED'))
) ENGINE = InnoDB COMMENT = '退款单';

CREATE TABLE transfer_order (
  id                BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  transfer_no       VARCHAR(32)      NOT NULL COMMENT 'TF 前缀',
  merchant_id       VARCHAR(32)      NOT NULL COMMENT '付款商户',
  out_transfer_no   VARCHAR(64)      NOT NULL COMMENT '商户转账单号',
  payee_merchant_id VARCHAR(32)      NOT NULL COMMENT '收款商户',
  amount            BIGINT           NOT NULL,
  currency          CHAR(3)          NOT NULL DEFAULT 'CNY',
  remark            VARCHAR(128)     NULL,
  notify_url        VARCHAR(512)     NOT NULL DEFAULT '',
  mode              VARCHAR(16)      NOT NULL COMMENT 'LOCAL（同分片）/ TCC（跨分片）',
  payer_shard       TINYINT UNSIGNED NOT NULL COMMENT 'shard.Of(merchant_id)',
  payee_shard       TINYINT UNSIGNED NOT NULL COMMENT 'shard.Of(payee_merchant_id)',
  status            VARCHAR(16)      NOT NULL COMMENT 'TRYING / CONFIRMING / CANCELLING / SUCCESS / FAILED',
  fail_code         VARCHAR(32)      NULL,
  txn_no            VARCHAR(32)      NULL COMMENT 'LOCAL 成功时的 TRANSFER 凭证号',
  retry_count       INT              NOT NULL DEFAULT 0 COMMENT '恢复任务处理次数',
  next_retry_at     DATETIME(3)      NOT NULL COMMENT '恢复任务下次接管时间',
  finished_at       DATETIME(3)      NULL COMMENT '进入 SUCCESS / FAILED 的时间，内部对账按此归属账期',
  version           BIGINT UNSIGNED  NOT NULL DEFAULT 0,
  created_at        DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_transfer_no (transfer_no),
  UNIQUE KEY uk_merchant_out_transfer_no (merchant_id, out_transfer_no),
  KEY idx_status_next_retry_at (status, next_retry_at),
  KEY idx_finished_at (finished_at),
  CONSTRAINT ck_transfer_amount       CHECK (amount > 0),
  CONSTRAINT ck_transfer_currency     CHECK (currency = 'CNY'),
  CONSTRAINT ck_transfer_status       CHECK (status IN ('TRYING', 'CONFIRMING', 'CANCELLING', 'SUCCESS', 'FAILED')),
  CONSTRAINT ck_transfer_mode         CHECK (mode IN ('LOCAL', 'TCC')),
  CONSTRAINT ck_transfer_shard        CHECK (payer_shard < 2 AND payee_shard < 2),
  CONSTRAINT ck_transfer_mode_shard   CHECK ((mode = 'LOCAL') = (payer_shard = payee_shard)),
  CONSTRAINT ck_transfer_self         CHECK (merchant_id <> payee_merchant_id),
  CONSTRAINT ck_transfer_local_status CHECK (mode = 'TCC' OR status IN ('TRYING', 'SUCCESS', 'FAILED')),
  CONSTRAINT ck_transfer_fail_code    CHECK ((status IN ('CANCELLING', 'FAILED')) = (fail_code IS NOT NULL)),
  CONSTRAINT ck_transfer_finished     CHECK ((status IN ('SUCCESS', 'FAILED')) = (finished_at IS NOT NULL)),
  CONSTRAINT ck_transfer_txn_no       CHECK ((mode = 'LOCAL' AND status = 'SUCCESS') = (txn_no IS NOT NULL))
) ENGINE = InnoDB COMMENT = '转账单（TCC 协调者）';

CREATE TABLE channel_txn (
  id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  channel             VARCHAR(16)     NOT NULL,
  txn_type            VARCHAR(16)     NOT NULL COMMENT 'PAY / REFUND',
  biz_no              VARCHAR(32)     NOT NULL COMMENT 'pay_no 或 refund_no，作为渠道请求号 req_no',
  channel_txn_no      VARCHAR(64)     NULL COMMENT '渠道流水号，渠道受理后回填',
  amount              BIGINT          NOT NULL,
  status              VARCHAR(16)     NOT NULL COMMENT 'PROCESSING / SUCCESS / FAILED / CLOSED',
  channel_status_raw  VARCHAR(32)     NULL COMMENT '渠道原始状态码',
  channel_finished_at DATETIME(3)     NULL COMMENT '渠道侧完成时间，对账按此归属账期',
  notify_count        INT             NOT NULL DEFAULT 0 COMMENT '收到的回调次数',
  query_count         INT             NOT NULL DEFAULT 0,
  next_query_at       DATETIME(3)     NULL COMMENT '下次主动查单时间',
  last_query_at       DATETIME(3)     NULL,
  version             BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_biz_no (biz_no),
  UNIQUE KEY uk_channel_txn_no (channel, channel_txn_no),
  KEY idx_status_next_query_at (status, next_query_at),
  KEY idx_channel_finished_at (channel, channel_finished_at),
  CONSTRAINT ck_ctxn_amount CHECK (amount > 0),
  CONSTRAINT ck_ctxn_type   CHECK (txn_type IN ('PAY', 'REFUND')),
  CONSTRAINT ck_ctxn_status CHECK (status IN ('PROCESSING', 'SUCCESS', 'FAILED', 'CLOSED'))
) ENGINE = InnoDB COMMENT = '渠道交互流水';

CREATE TABLE outbox (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  event_id      VARCHAR(32)     NOT NULL COMMENT 'EV 前缀，消费端幂等键之一',
  event_type    VARCHAR(64)     NOT NULL COMMENT 'payment.succeeded / refund.succeeded / refund.failed / transfer.succeeded / transfer.failed',
  topic         VARCHAR(128)    NOT NULL,
  msg_key       VARCHAR(64)     NOT NULL COMMENT 'Kafka 分区键：支付、退款事件取原 pay_no，转账事件取 transfer_no',
  biz_no        VARCHAR(32)     NOT NULL COMMENT '事件对应的业务单号：支付事件 pay_no，退款事件 refund_no，转账事件 transfer_no',
  payload       JSON            NOT NULL,
  status        VARCHAR(16)     NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING / SENT / DEAD',
  retry_count   INT             NOT NULL DEFAULT 0,
  next_retry_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  last_error    VARCHAR(512)    NULL,
  sent_at       DATETIME(3)     NULL,
  created_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_event_id (event_id),
  KEY idx_status_next_retry_at (status, next_retry_at),
  KEY idx_biz_no_event_type (biz_no, event_type),
  CONSTRAINT ck_outbox_status CHECK (status IN ('PENDING', 'SENT', 'DEAD'))
) ENGINE = InnoDB COMMENT = '本地消息表';

CREATE TABLE merchant_notify (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  notify_no        VARCHAR(32)     NOT NULL COMMENT 'NT 前缀',
  event_id         VARCHAR(32)     NOT NULL COMMENT '来源事件，消费幂等',
  merchant_id      VARCHAR(32)     NOT NULL COMMENT '被通知商户；转账只通知付款商户',
  biz_type         VARCHAR(16)     NOT NULL COMMENT 'PAY / REFUND / TRANSFER',
  biz_no           VARCHAR(32)     NOT NULL COMMENT 'pay_no / refund_no / transfer_no',
  notify_url       VARCHAR(512)    NOT NULL,
  payload          JSON            NOT NULL,
  status           VARCHAR(16)     NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING / SUCCESS / DEAD',
  attempt_count    INT             NOT NULL DEFAULT 0,
  next_attempt_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  last_http_status INT             NULL,
  last_error       VARCHAR(512)    NULL,
  succeeded_at     DATETIME(3)     NULL,
  created_at       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_notify_no (notify_no),
  UNIQUE KEY uk_event_id (event_id),
  KEY idx_status_next_attempt_at (status, next_attempt_at),
  KEY idx_biz_no (biz_no),
  CONSTRAINT ck_notify_status CHECK (status IN ('PENDING', 'SUCCESS', 'DEAD'))
) ENGINE = InnoDB COMMENT = '商户异步通知';

-- =====================================================================
-- recon 任务
-- =====================================================================
CREATE DATABASE IF NOT EXISTS pay_recon DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE pay_recon;

CREATE TABLE recon_batch (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  batch_no       VARCHAR(32)     NOT NULL COMMENT 'RB 前缀',
  channel        VARCHAR(16)     NOT NULL,
  bill_date      DATE            NOT NULL COMMENT '账期（东八区自然日）',
  status         VARCHAR(16)     NOT NULL COMMENT 'INIT / DOWNLOADED / CHECKING / DONE / FAILED',
  bill_file_hash CHAR(64)        NULL COMMENT '账单文件 SHA-256',
  channel_count  INT             NOT NULL DEFAULT 0,
  channel_amount BIGINT          NOT NULL DEFAULT 0,
  local_count    INT             NOT NULL DEFAULT 0,
  local_amount   BIGINT          NOT NULL DEFAULT 0,
  matched_count  INT             NOT NULL DEFAULT 0,
  diff_count     INT             NOT NULL DEFAULT 0,
  error_msg      VARCHAR(512)    NULL,
  started_at     DATETIME(3)     NULL,
  finished_at    DATETIME(3)     NULL,
  created_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_batch_no (batch_no),
  UNIQUE KEY uk_channel_bill_date (channel, bill_date),
  CONSTRAINT ck_batch_status CHECK (status IN ('INIT', 'DOWNLOADED', 'CHECKING', 'DONE', 'FAILED'))
) ENGINE = InnoDB COMMENT = '对账批次';

CREATE TABLE recon_channel_bill (
  id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  batch_no            VARCHAR(32)     NOT NULL,
  channel             VARCHAR(16)     NOT NULL,
  bill_date           DATE            NOT NULL,
  txn_type            VARCHAR(16)     NOT NULL COMMENT 'PAY / REFUND',
  channel_txn_no      VARCHAR(64)     NOT NULL,
  biz_no              VARCHAR(32)     NOT NULL COMMENT '账单中的 req_no，即本地 pay_no / refund_no',
  amount              BIGINT          NOT NULL,
  channel_status      VARCHAR(16)     NOT NULL,
  channel_finished_at DATETIME(3)     NOT NULL,
  match_status        VARCHAR(16)     NOT NULL DEFAULT 'UNCHECKED' COMMENT 'UNCHECKED / MATCHED / DIFF',
  created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_batch_type_channel_txn_no (batch_no, txn_type, channel_txn_no),
  KEY idx_batch_biz_no (batch_no, biz_no),
  CONSTRAINT ck_bill_type  CHECK (txn_type IN ('PAY', 'REFUND')),
  CONSTRAINT ck_bill_match CHECK (match_status IN ('UNCHECKED', 'MATCHED', 'DIFF'))
) ENGINE = InnoDB COMMENT = '导入的渠道账单明细';

CREATE TABLE recon_diff (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  diff_no        VARCHAR(32)     NOT NULL COMMENT 'RD 前缀',
  batch_no       VARCHAR(32)     NOT NULL,
  channel        VARCHAR(16)     NOT NULL,
  bill_date      DATE            NOT NULL,
  diff_type      VARCHAR(32)     NOT NULL COMMENT 'LOCAL_MISSING(长款) / CHANNEL_MISSING(短款) / AMOUNT_MISMATCH / STATUS_MISMATCH / LEDGER_MISSING / TRANSFER_MISMATCH',
  txn_type       VARCHAR(16)     NOT NULL COMMENT 'PAY / REFUND / TRANSFER',
  biz_no         VARCHAR(32)     NOT NULL COMMENT 'pay_no / refund_no / transfer_no',
  channel_txn_no VARCHAR(64)     NULL,
  local_amount   BIGINT          NULL,
  channel_amount BIGINT          NULL,
  local_status   VARCHAR(16)     NULL,
  channel_status VARCHAR(16)     NULL,
  status         VARCHAR(16)     NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING / RESOLVED / IGNORED',
  handle_action  VARCHAR(32)     NULL COMMENT 'REQUERY / REPLAY，只由自动处理写入',
  handler        VARCHAR(64)     NULL,
  handle_note    VARCHAR(512)    NULL,
  resolved_at    DATETIME(3)     NULL,
  created_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_diff_no (diff_no),
  UNIQUE KEY uk_batch_type_biz_no (batch_no, txn_type, biz_no),
  KEY idx_status_bill_date (status, bill_date),
  CONSTRAINT ck_diff_type     CHECK (diff_type IN ('LOCAL_MISSING', 'CHANNEL_MISSING', 'AMOUNT_MISMATCH', 'STATUS_MISMATCH', 'LEDGER_MISSING', 'TRANSFER_MISMATCH')),
  CONSTRAINT ck_diff_txn_type CHECK (txn_type IN ('PAY', 'REFUND', 'TRANSFER')),
  CONSTRAINT ck_diff_status   CHECK (status IN ('PENDING', 'RESOLVED', 'IGNORED')),
  CONSTRAINT ck_diff_action   CHECK (handle_action IS NULL OR handle_action IN ('REQUERY', 'REPLAY'))
) ENGINE = InnoDB COMMENT = '对账差错';

-- =====================================================================
-- channel_mock（模拟渠道自身的数据，与平台数据隔离）
-- =====================================================================
CREATE DATABASE IF NOT EXISTS pay_channel_mock DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE pay_channel_mock;

CREATE TABLE mock_txn (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  channel_txn_no    VARCHAR(64)     NOT NULL COMMENT 'MC 前缀',
  txn_type          VARCHAR(16)     NOT NULL COMMENT 'PAY / REFUND',
  req_no            VARCHAR(32)     NOT NULL COMMENT '平台请求号（pay_no / refund_no）',
  orig_req_no       VARCHAR(32)     NULL COMMENT '退款对应的原支付 req_no',
  amount            BIGINT          NOT NULL,
  status            VARCHAR(16)     NOT NULL COMMENT 'PROCESSING / SUCCESS / FAILED / CLOSED',
  scenario          VARCHAR(32)     NOT NULL DEFAULT 'NORMAL' COMMENT '故障场景，见需求文档 CH-06',
  notify_url        VARCHAR(512)    NOT NULL DEFAULT '',
  notify_sent_count INT             NOT NULL DEFAULT 0,
  finished_at       DATETIME(3)     NULL,
  bill_date         DATE            NULL COMMENT '计入哪一天的账单',
  created_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_channel_txn_no (channel_txn_no),
  UNIQUE KEY uk_type_req_no (txn_type, req_no),
  KEY idx_bill_date (bill_date),
  CONSTRAINT ck_mock_type   CHECK (txn_type IN ('PAY', 'REFUND')),
  CONSTRAINT ck_mock_status CHECK (status IN ('PROCESSING', 'SUCCESS', 'FAILED', 'CLOSED'))
) ENGINE = InnoDB COMMENT = '模拟渠道交易';
