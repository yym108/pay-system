-- BASE-03 channel_mock 初始表。数据库 pay_channel_mock 由 bootstrap 创建。

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
