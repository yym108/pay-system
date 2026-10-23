# Database Schema

> **Last updated:** 2026-10-23
> **Database engine/version:** MySQL 8.4.11（InnoDB，utf8mb4 / utf8mb4_bin）
> **Schema sources:** `migrations/account/000001_init.up.sql`（`pay_account_0` 与 `pay_account_1` 各执行一次）、`migrations/payment/000001_init.up.sql`、`migrations/payment/000002_merchant_fee_rate.up.sql`、`migrations/recon/000001_init.up.sql`、`migrations/channel_mock/000001_init.up.sql`。设计稿 `docs/schema/schema_draft.sql` 已在 BASE-03 删除。golang-migrate 还会在每个 schema 创建工具表 `schema_migrations`（`version`、`dirty`），不在业务清单里。
> **Owner:** pay-system 项目作者
> **Status:** BASE-03 已把设计稿拆成迁移并在 MySQL 8.4.11 上执行 up/down。账号由 `cmd/bootstrap` 创建，不在迁移 DDL 中。

## 1. Scope and Conventions

| Item | Description |
| --- | --- |
| Scope | 5 个 schema：`pay_account_0`、`pay_account_1`（account 服务的两个分片，D17）、`pay_payment`（payment 服务）、`pay_recon`（对账任务）、`pay_channel_mock`（模拟渠道，代表外部系统）。共 17 种表、22 张物理表。覆盖本地 docker-compose 与集成测试环境 |
| Shard tables | `account`、`ledger_txn`、`ledger_entry`、`account_freeze`、`transfer_branch` 在两个分片中结构完全相同，本文只写一次。下文 `pay_account_{s}.<表>` 表示分片 `s`（0 或 1）中的该表；关系两端都写 `{s}` 表示同一分片内。分片号 `s = 商户号数字部分 % 2`（ACC-13）。所有唯一键只在分片内成立，跨分片的关系都是逻辑关系 |
| Naming | 表名、列名小写下划线；唯一索引 `uk_`，普通索引 `idx_`，检查约束 `ck_`；业务单号列以 `_no` 结尾；分片 schema 为 `pay_<服务>_<分片号>`（`docs/03-conventions.md` §7.2） |
| Primary key convention | 每表 `id BIGINT UNSIGNED AUTO_INCREMENT`，只作物理主键、不对外暴露；分片表的 `id` 只在分片内唯一。业务单号（2 位前缀 + 19 位雪花数字，见 `docs/01-tech-decisions.md` D5）全局唯一，走唯一索引 |
| Audit convention | `created_at`、`updated_at` 为 `DATETIME(3)`，由数据库默认值维护；只追加表（`ledger_txn`、`ledger_entry`、`recon_channel_bill`、`merchant_fee_rate`）没有 `updated_at`；可变的核心表带 `version` 用于排查并发更新 |
| Deletion convention | 一律不物理删除业务数据，不使用软删除标记；状态终结通过状态字段表达。例外：`outbox` 中 SENT 记录保留 7 天后清理（P1）；对账重跑时清理该批次的 `recon_channel_bill` 与 PENDING 差错 |
| Time convention | 数据库与连接时区均为 `+08:00`；毫秒精度；账期按东八区自然日 |
| Money convention | 金额统一 `BIGINT`，单位分，币种仅 `CNY`（CHECK 约束） |
| Sensitive data convention | 不存储卡号、手机号、身份证等个人信息；商户签名密钥以 AES-256-GCM 密文存储；日志按字段名脱敏（`docs/01-tech-decisions.md` D13） |
| Access control | 账号与权限由 `cmd/bootstrap` 创建，不在迁移 DDL 中（BASE-03）：`svc_account` 读写 `pay_account_0.*`、`pay_account_1.*`；`svc_payment` 读写 `pay_payment.*`；`svc_recon` 读写 `pay_recon.*`，只读 `pay_payment.*`、`pay_account_0.*`、`pay_account_1.*`；`svc_channel_mock` 读写 `pay_channel_mock.*`；`migrator` 仅迁移容器使用 |
| Isolation level | 各服务连接统一 READ COMMITTED；余额核对（ACC-10）与订单手续费核对（PAY-10）单独使用 REPEATABLE READ 一致性快照 |

## 2. Relationship Overview

### 2.1 Physical Foreign Keys

本项目不使用物理外键。原因：各 schema 归属不同服务、账户数据分为两个分片，跨 schema 引用不能用外键；同 schema 内也不加外键，避免级联锁和外键检查影响写入路径，一致性由事务内的条件更新、唯一索引和对账保证。

| Child table.column | Parent table.column | Cardinality | Delete/update behavior | Constraint | Source |
| --- | --- | --- | --- | --- | --- |
| 无 | — | — | — | — | 迁移文件未声明物理外键 |

### 2.2 Logical Business Relationships

| From table.column | To table.column | Cardinality | Enforcement/evidence | Business meaning | Consistency risk |
| --- | --- | --- | --- | --- | --- |
| `pay_account_{s}.ledger_entry.txn_no` | `pay_account_{s}.ledger_txn.txn_no` | N:1，每个凭证 ≥ 2 条分录 | 同一分片事务写入；`ledger_txn.entry_count`；ACC-10 核对 | 分录属于凭证，一张凭证的全部分录只在一个分片 | 分录缺失导致借贷不平，核对任务报警 |
| `pay_account_{s}.ledger_entry.account_id` | `pay_account_{s}.account.id` | N:1 | 记账事务先更新账户再写分录；ACC-03 校验分录账户属于同一分片 | 分录记到账户 | 账户余额与分录汇总不一致；跨分片分录由 ACC-03 拒绝（`40003`） |
| `pay_account_{s}.ledger_txn.reversal_of` | `pay_account_{s}.ledger_txn.txn_no` | 0..1:1 | `uk_reversal_of`，同一凭证只能冲正一次；`ck_ledger_txn_reversal` | 冲正凭证指向原凭证（P1） | 重复冲正 |
| `pay_account_{s}.account_freeze.account_id` | `pay_account_{s}.account.id` | N:1 | 冻结事务 | 冻结占用商户账户可用余额 | `frozen_amount` 与 FROZEN 冻结单合计不一致 |
| `pay_account_{s}.account_freeze.txn_no` | `pay_account_{s}.ledger_txn.txn_no` | 0..1:1 | ConfirmFreeze 同一事务回写 | 确认扣款对应的凭证（REFUND 或 TRANSFER_OUT） | — |
| `pay_account_{s}.transfer_branch.txn_no` | `pay_account_{s}.ledger_txn.txn_no` | 0..1:1 | ACC-07 / ACC-14 同一事务回写 | LOCAL 分支对应 TRANSFER 凭证；PAYEE 分支对应 TRANSFER_IN 凭证 | — |
| `pay_account_{s}.account.owner_id`（owner_type=MERCHANT） | `pay_payment.merchant.merchant_id` | 1:1（每种账户类型），只存在于 `s = shard.Of(merchant_id)` | 种子数据 / 开户 ACC-01 校验分片 | 商户结算账户 | 商户无账户时入账消息进入 DLQ；开错分片由 ACC-01 拒绝 |
| `pay_account_{s}.ledger_txn.(biz_type='PAY', biz_no)` | `pay_payment.payment_order.pay_no` | 0..1:1，商户所属分片 | 事件 `payment.succeeded`；内部对账 REC-05 | 支付成功入账 | 丢消息导致漏入账，由 LEDGER_MISSING 发现 |
| `pay_account_{s}.ledger_txn.(biz_type='REFUND', biz_no)` / `account_freeze.(biz_type='REFUND', biz_no)` | `pay_payment.refund_order.refund_no` | 0..1:1，商户所属分片 | 同步 Freeze RPC + 事件 `refund.*`；REC-05 | 退款冻结与扣款 | 冻结未确认或未释放，商户资金长期占用 |
| `pay_account_{s}.account_freeze.(biz_type='TRANSFER', biz_no)`，`s = payer_shard` | `pay_payment.transfer_order.transfer_no`（mode=TCC） | 0..1:1 | TRF-03 同步 Freeze / ConfirmFreeze / ReleaseFreeze；REC-05 | 跨分片转账付款方分支（Try = 冻结） | 冻结悬挂占用付款方资金；由空回滚墓碑与 TRF-04 收敛 |
| `pay_account_{s}.transfer_branch.(transfer_no, role='PAYEE')`，`s = payee_shard` | `pay_payment.transfer_order.transfer_no`（mode=TCC） | 0..1:1 | TRF-03 同步 TransferInTry / Confirm / Cancel；REC-05 | 跨分片转账收款方分支 | 分支状态与转账单终态不一致，REC-05 报 TRANSFER_MISMATCH |
| `pay_account_{s}.transfer_branch.(transfer_no, role='LOCAL')`，`s = payer_shard = payee_shard` | `pay_payment.transfer_order.transfer_no`（mode=LOCAL） | 0..1:1 | TRF-02 同步 TransferLocal；REC-05 | 同分片转账的确定结果 | 结果未回写转账单时由 TRF-04 重新调用取回 |
| `pay_account_{s}.ledger_txn.(biz_type='TRANSFER', biz_no)` | `pay_payment.transfer_order.transfer_no`（mode=LOCAL） | 0..1:1；`transfer_order.txn_no` 指回该凭证 | ACC-07 事务；不变量 12；REC-05 | 同分片转账凭证 | 凭证缺失或多出，REC-05 报 TRANSFER_MISMATCH |
| `pay_account_{payer_shard}.ledger_txn.(TRANSFER_OUT, biz_no)` 与 `pay_account_{payee_shard}.ledger_txn.(TRANSFER_IN, biz_no)` | `pay_payment.transfer_order.transfer_no`（mode=TCC） | 各 0..1:1 | ACC-06 / ACC-14 Confirm；不变量 11、12；REC-05 | 跨分片转账两段凭证，经各分片 `TRANSIT` 子账户过渡 | 只完成一段时在途余额非 0；长期如此由 ACC-10 在途守恒与 REC-05 发现 |
| `pay_account_{s}.account`（account_type=TRANSIT）余额合计 | `account_freeze`（TRANSFER, CONFIRMED）金额合计 − `transfer_branch`（PAYEE, CONFIRMED）金额合计，同一分片 | 聚合等式 | ACC-10 全量模式在同一快照内校验（不变量 11） | 在途守恒 | 不等即账务错误，核对失败 |
| `pay_payment.payment_order.merchant_id` | `pay_payment.merchant.merchant_id` | N:1 | 网关验签确定商户 | 订单归属 | — |
| `pay_payment.merchant_fee_rate.merchant_id` | `pay_payment.merchant.merchant_id` | N:1 | 登记费率前 `SELECT … FOR UPDATE` 商户行；无物理外键 | 商户的费率历史。生效档是 `effective_from <= payment_order.created_at` 的最近一行 | 漏插第一档时，该时刻之前的订单核对为“没有生效费率” |
| `pay_payment.refund_order.pay_no` | `pay_payment.payment_order.pay_no` | N:1 | 创建退款事务内条件更新 `refunded_amount` | 部分退款 | 退款累计超过订单金额（由条件更新阻止） |
| `pay_payment.transfer_order.merchant_id` / `payee_merchant_id` | `pay_payment.merchant.merchant_id` | N:1 / N:1 | 网关验签确定付款商户；TRF-01 校验收款商户存在且 ACTIVE；`ck_transfer_self` | 付款方、收款方 | — |
| `pay_payment.channel_txn.biz_no` | `payment_order.pay_no` 或 `refund_order.refund_no`（按 `txn_type`） | 1:1 | 与订单同一事务创建与推进 | 每笔订单一次渠道交易；转账不经过渠道 | 状态与订单不一致 |
| `pay_payment.outbox.msg_key` | `payment_order.pay_no` 或 `transfer_order.transfer_no` | N:1 | 业务事务内写入 | Kafka 分区键：支付、退款事件取原 pay_no，转账事件取 transfer_no | — |
| `pay_payment.outbox.biz_no` | `payment_order.pay_no`、`refund_order.refund_no` 或 `transfer_order.transfer_no` | N:1 | 业务事务内写入；`idx_biz_no_event_type` | 按业务单号重放（MQ-05） | 漏写导致无法按单号重放 |
| `pay_payment.merchant_notify.event_id` | `pay_payment.outbox.event_id` | 1:1 | `uk_event_id` | 每个事件一条通知 | — |
| `pay_payment.merchant_notify.(biz_type, biz_no)` | 支付单、退款单或转账单（`biz_type` = PAY / REFUND / TRANSFER） | N:1 | NTF-01 生成 | 通知对应的单据；转账只通知付款商户 | — |
| `pay_recon.recon_channel_bill.batch_no` / `recon_diff.batch_no` | `pay_recon.recon_batch.batch_no` | N:1 | 对账任务 | 批次明细 | 重跑时未清理导致重复 |
| `pay_recon.recon_channel_bill.biz_no`、`recon_diff.biz_no`（txn_type=PAY / REFUND） | `pay_payment.channel_txn.biz_no` | N:1（跨批次） | 对账匹配键 `(txn_type, biz_no)` | 账单与本地流水比对 | — |
| `pay_recon.recon_diff.biz_no`（txn_type=TRANSFER） | `pay_payment.transfer_order.transfer_no` | N:1（跨批次） | REC-05 写入 TRANSFER_MISMATCH | 转账内部对账差错 | — |
| `pay_channel_mock.mock_txn.req_no` | `pay_payment.channel_txn.biz_no` | 1:1 | 平台请求号即渠道 req_no；两边独立系统 | 外部系统对应关系 | 即对账要发现的差错 |

## 3. Table Inventory

| Table | Business name | Module/domain | Purpose | Lifecycle/sensitivity | Source |
| --- | --- | --- | --- | --- | --- |
| `pay_account_{0,1}.account` | 账户 | account | 余额与冻结金额；平台账户按子账户拆分 | 长期；内部 | `migrations/account/000001_init.up.sql` |
| `pay_account_{0,1}.ledger_txn` | 记账凭证 | account | 一次记账的头记录，幂等键 | 只追加，永久；内部 | `migrations/account/000001_init.up.sql` |
| `pay_account_{0,1}.ledger_entry` | 记账分录 | account | 借贷明细 | 只追加，永久；内部 | `migrations/account/000001_init.up.sql` |
| `pay_account_{0,1}.account_freeze` | 冻结单 | account | 退款冻结、跨分片转账付款方分支 | 长期；内部 | `migrations/account/000001_init.up.sql` |
| `pay_account_{0,1}.transfer_branch` | 转账分支 | account | 同分片转账结果、跨分片转账收款方分支 | 长期；内部 | `migrations/account/000001_init.up.sql` |
| `pay_payment.merchant` | 商户 | payment | 商户配置与密钥 | 长期；含高敏密文 | `migrations/payment/000001_init.up.sql` |
| `pay_payment.merchant_fee_rate` | 商户费率历史 | payment | 只追加的生效费率 | 只追加，永久；内部 | `migrations/payment/000002_merchant_fee_rate.up.sql` |
| `pay_payment.payment_order` | 支付单 | payment | 支付状态机 | 长期；内部 | `migrations/payment/000001_init.up.sql` |
| `pay_payment.refund_order` | 退款单 | payment | 退款状态机 | 长期；内部 | `migrations/payment/000001_init.up.sql` |
| `pay_payment.transfer_order` | 转账单 | payment | 转账状态机，跨分片 TCC 协调者 | 长期；内部 | `migrations/payment/000001_init.up.sql` |
| `pay_payment.channel_txn` | 渠道交互流水 | payment | 与渠道的交易、查单调度 | 长期；内部 | `migrations/payment/000001_init.up.sql` |
| `pay_payment.outbox` | 本地消息表 | payment | 事务性事件投递 | SENT 保留 7 天；内部 | `migrations/payment/000001_init.up.sql` |
| `pay_payment.merchant_notify` | 商户通知 | payment | 异步通知与重试 | 长期；内部 | `migrations/payment/000001_init.up.sql` |
| `pay_recon.recon_batch` | 对账批次 | recon | 每渠道每日一批 | 长期；内部 | `migrations/recon/000001_init.up.sql` |
| `pay_recon.recon_channel_bill` | 渠道账单明细 | recon | 导入的账单行 | 长期，可归档；内部 | `migrations/recon/000001_init.up.sql` |
| `pay_recon.recon_diff` | 对账差错 | recon | 差错与自动处理记录 | 长期；内部 | `migrations/recon/000001_init.up.sql` |
| `pay_channel_mock.mock_txn` | 模拟渠道交易 | channel_mock | 外部渠道自身数据 | 测试数据；无敏感 | `migrations/channel_mock/000001_init.up.sql` |

## 4. Table Details

分片表（4.1–4.5）在 `pay_account_0`、`pay_account_1` 中结构相同。设计稿中 `pay_account_1` 由 `CREATE TABLE ... LIKE` 复制，MySQL 会把复制出的 CHECK 约束重命名为 `<表名>_chk_<n>`，约束内容不变；下表列出的 `ck_` 名称以 `pay_account_0` 和迁移文件为准（迁移对每个分片各执行一次，名称一致）。

### 4.1 `account` — 账户

- **Purpose:** 保存账户余额和冻结金额，是余额的物化结果；真相来源是 `ledger_entry`。商户账户只有一行（`sub_no = 0`）；平台账户在每个分片各开 `PLATFORM_SUB_ACCOUNTS` 个子账户，记账时按 `crc32(biz_no) % N` 选择（D19、OPT-03）。
- **Lifecycle:** 开户后长期存在，状态 ACTIVE / FROZEN / CLOSED。
- **Sensitivity:** 内部财务数据。
- **Authoritative source:** `migrations/account/000001_init.up.sql`。
- **Write owners/read consumers:** account 服务写；recon 只读。
- **Retention/archival:** 不归档。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | 加锁顺序依据（升序）；只在分片内唯一 | 内部 |
| `account_no` | VARCHAR(32) | 否 | — | `uk_account_no` | 对外账户号，`AC` 前缀，全局唯一（雪花 ID） | 内部 |
| `owner_type` | VARCHAR(16) | 否 | — | `uk_owner_account_type_sub` 1、`ck_account_type_rule`、`ck_account_merchant_sub` | MERCHANT / PLATFORM | 内部 |
| `owner_id` | VARCHAR(32) | 否 | — | `uk_owner_account_type_sub` 2 | 商户号；平台账户为渠道编码（CHANNEL_RECEIVABLE，如 `mockpay`）或 `PLATFORM`（FEE_INCOME、TRANSIT） | 内部 |
| `account_type` | VARCHAR(32) | 否 | — | `uk_owner_account_type_sub` 3、`ck_account_type_rule` | MERCHANT_SETTLE / CHANNEL_RECEIVABLE / FEE_INCOME / TRANSIT（§2.5.1） | 内部 |
| `currency` | CHAR(3) | 否 | `'CNY'` | `uk_owner_account_type_sub` 4、`ck_account_currency` | 仅 CNY | 内部 |
| `sub_no` | INT UNSIGNED | 否 | 0 | `uk_owner_account_type_sub` 5、`ck_account_merchant_sub` | 子账户号：商户账户恒为 0；平台账户 `[0, PLATFORM_SUB_ACCOUNTS)` | 内部 |
| `balance_direction` | VARCHAR(8) | 否 | — | `ck_account_direction`、`ck_account_type_rule` | DEBIT / CREDIT，由账户类型决定：CHANNEL_RECEIVABLE 为 DEBIT，其余为 CREDIT | 内部 |
| `balance` | BIGINT | 否 | 0 | `ck_account_available` | 余额（分） | 财务 |
| `frozen_amount` | BIGINT | 否 | 0 | `ck_account_frozen` | 冻结金额（分），≥ 0，等于 FROZEN 冻结单合计 | 财务 |
| `allow_overdraft` | TINYINT | 否 | 0 | `ck_account_available`、`ck_account_type_rule` | 由账户类型决定：MERCHANT_SETTLE 为 0，平台账户（含 TRANSIT）为 1 | 内部 |
| `status` | VARCHAR(16) | 否 | `'ACTIVE'` | `ck_account_status` | ACTIVE / FROZEN / CLOSED。普通记账要求 ACTIVE。确认扣款的商户腿不要求 ACTIVE（ACC-06）；平台贷方仍要求 ACTIVE | 内部 |
| `version` | BIGINT UNSIGNED | 否 | 0 | — | 余额或冻结金额变更时 +1（ACC-04 冻结成功、ACC-05 解冻成功也加 1；ACC-05 墓碑插入不改账户）。ACC-06 确认成功时商户 `balance` 与 `frozen_amount` 同减，商户账户和平台账户的 `version` 各 +1 | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | 创建时间 | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | 更新时间 | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | 主键 | migrations/account/000001_init.up.sql |
| `uk_account_no` | UNIQUE | `account_no` | 对外编号唯一 | migrations/account/000001_init.up.sql |
| `uk_owner_account_type_sub` | UNIQUE | `owner_type, owner_id, account_type, currency, sub_no` | 开户幂等（ACC-01）；替代 v2 的 `uk_owner_account_type`，增加 `sub_no` 以容纳平台子账户 | migrations/account/000001_init.up.sql |
| `ck_account_frozen` | CHECK | `frozen_amount >= 0` | 冻结非负 | migrations/account/000001_init.up.sql |
| `ck_account_available` | CHECK | `allow_overdraft = 1 OR balance - frozen_amount >= 0` | 可用余额非负的最后防线（不变量 5） | migrations/account/000001_init.up.sql |
| `ck_account_currency` | CHECK | `currency = 'CNY'` | 单币种 | migrations/account/000001_init.up.sql |
| `ck_account_direction` | CHECK | `balance_direction IN ('DEBIT','CREDIT')` | 枚举 | migrations/account/000001_init.up.sql |
| `ck_account_status` | CHECK | `status IN ('ACTIVE','FROZEN','CLOSED')` | 枚举 | migrations/account/000001_init.up.sql |
| `ck_account_type_rule` | CHECK | MERCHANT_SETTLE ⇒ MERCHANT、CREDIT、不透支；CHANNEL_RECEIVABLE ⇒ PLATFORM、DEBIT、透支；FEE_INCOME / TRANSIT ⇒ PLATFORM、CREDIT、透支 | 账户类型决定所有者类型、余额方向与透支（§2.5.1、ACC-01 第 1 条），同时限定 `account_type` 枚举 | migrations/account/000001_init.up.sql |
| `ck_account_merchant_sub` | CHECK | `owner_type <> 'MERCHANT' OR sub_no = 0` | 商户账户不拆子账户（D19） | migrations/account/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** 被同分片 `ledger_entry.account_id`、`account_freeze.account_id` 引用；`owner_id`（MERCHANT）对应 `merchant.merchant_id`，且只出现在 `shard.Of(merchant_id)` 分片；TRANSIT 子账户余额合计参与在途守恒（不变量 11）。

#### Operational Notes

- **Backfill/migration considerations:** 种子数据（BASE-03）在每个分片为 `CHANNEL_RECEIVABLE(mockpay)`、`FEE_INCOME`、`TRANSIT` 各开 `sub_no = 0 … PLATFORM_SUB_ACCOUNTS − 1`；演示商户只在所属分片开 `MERCHANT_SETTLE`，初始余额 0。`PLATFORM_SUB_ACCOUNTS` 只允许调大，调大后重新执行种子补开子账户。
- **Compatibility/deprecation notes:** 余额与流水不一致时以流水为准，修复只能通过新增凭证。“商户账户只在所属分片”无法由数据库跨 schema 约束，由 ACC-01 校验与 BASE-03 验收保证。v3.0 删除差错挂账账户类型（见 §5）。

### 4.2 `ledger_txn` — 记账凭证

- **Purpose:** 一次记账的头记录；`uk_biz` 是记账幂等的数据库兜底。
- **Lifecycle:** 只追加，不更新、不删除。
- **Sensitivity:** 内部财务数据。
- **Authoritative source:** `migrations/account/000001_init.up.sql。
- **Write owners/read consumers:** account 写；recon 只读（内部对账）。
- **Retention/archival:** 永久保留。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `txn_no` | VARCHAR(32) | 否 | — | `uk_txn_no` | 凭证号，`TX` 前缀 | 内部 |
| `biz_type` | VARCHAR(32) | 否 | — | `uk_biz` 1、`ck_ledger_txn_biz_type` | PAY / REFUND / TRANSFER / TRANSFER_OUT / TRANSFER_IN / REVERSAL（§2.5.2） | 内部 |
| `biz_no` | VARCHAR(64) | 否 | — | `uk_biz` 2 | 业务单号：pay_no、refund_no、transfer_no；REVERSAL 时为原 txn_no | 内部 |
| `amount` | BIGINT | 否 | — | `ck_ledger_txn_amount` | 借方合计 = 贷方合计，> 0 | 财务 |
| `currency` | CHAR(3) | 否 | `'CNY'` | — | | 内部 |
| `entry_count` | INT | 否 | — | `ck_ledger_txn_entries` | 分录条数，≥ 2 | 内部 |
| `reversal_of` | VARCHAR(32) | 是 | NULL | `uk_reversal_of`、`ck_ledger_txn_reversal` | 被冲正的凭证号；当且仅当 `biz_type = REVERSAL` 时非空 | 内部 |
| `memo` | VARCHAR(255) | 否 | `''` | — | 备注，不得包含个人信息 | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | `idx_created_at` | 记账时间 | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/account/000001_init.up.sql |
| `uk_txn_no` | UNIQUE | `txn_no` | | migrations/account/000001_init.up.sql |
| `uk_biz` | UNIQUE | `biz_type, biz_no` | 同一业务在分片内只记账一次（不变量 6）；跨分片转账的两段凭证 `biz_type` 不同，分别落在两个分片 | migrations/account/000001_init.up.sql |
| `uk_reversal_of` | UNIQUE | `reversal_of`（NULL 不参与唯一） | 同一凭证只能冲正一次 | migrations/account/000001_init.up.sql |
| `idx_created_at` | INDEX | `created_at` | 按日核对、对账 | migrations/account/000001_init.up.sql |
| `ck_ledger_txn_amount` | CHECK | `amount > 0` | | migrations/account/000001_init.up.sql |
| `ck_ledger_txn_entries` | CHECK | `entry_count >= 2` | 复式记账 | migrations/account/000001_init.up.sql |
| `ck_ledger_txn_biz_type` | CHECK | `biz_type IN ('PAY','REFUND','TRANSFER','TRANSFER_OUT','TRANSFER_IN','REVERSAL')` | 凭证类型枚举（ACC-03 第 1 条） | migrations/account/000001_init.up.sql |
| `ck_ledger_txn_reversal` | CHECK | `(biz_type = 'REVERSAL') = (reversal_of IS NOT NULL)` | 冲正凭证必须指向原凭证，其他凭证不得填写（ACC-08） | migrations/account/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** 1:N 同分片 `ledger_entry`；`biz_no` 指向支付单、退款单、转账单或原凭证；被 `account_freeze.txn_no`、`transfer_branch.txn_no`、`transfer_order.txn_no` 引用。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** 原草案将幂等键放在 `ledger_entry(biz_type, biz_no, account_id, direction)`；改为凭证级 `uk(biz_type, biz_no)`，使“一笔业务一张凭证”在一次唯一冲突中判定，分录级唯一键改为 `uk(txn_no, account_id, direction)`。批量记账（OPT-02）预查 `(PAY, biz_no IN (...))` 走 `uk_biz`。同一 `transfer_no` 不应同时出现 TRANSFER 与 TRANSFER_OUT / TRANSFER_IN，数据库不约束，由 REC-05 检查。

### 4.3 `ledger_entry` — 记账分录

- **Purpose:** 借贷明细，账户余额的真相来源（不变量 2、3、4）。
- **Lifecycle:** 只追加。
- **Sensitivity:** 内部财务数据。
- **Authoritative source:** `migrations/account/000001_init.up.sql。
- **Write owners/read consumers:** account 写；account 核对任务、recon 只读。
- **Retention/archival:** 永久保留。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | 同一账户内递增，用于游标分页和“最后一条分录” | 内部 |
| `txn_no` | VARCHAR(32) | 否 | — | `uk_txn_account_direction` 1 | 所属凭证 | 内部 |
| `account_id` | BIGINT UNSIGNED | 否 | — | `uk_txn_account_direction` 2、`idx_account_id_id` 1 | 记账账户，与凭证同分片 | 内部 |
| `direction` | VARCHAR(8) | 否 | — | `uk_txn_account_direction` 3 | DEBIT / CREDIT | 内部 |
| `amount` | BIGINT | 否 | — | `ck_ledger_entry_amount` | > 0，方向由 `direction` 表达 | 财务 |
| `balance_after` | BIGINT | 否 | — | — | 本分录后账户余额，用于逐条核对；批量记账时按事件顺序依次累加（OPT-02） | 财务 |
| `biz_type` | VARCHAR(32) | 否 | — | — | 冗余自凭证，便于按账户查询 | 内部 |
| `biz_no` | VARCHAR(64) | 否 | — | — | 冗余自凭证 | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | `idx_created_at` | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/account/000001_init.up.sql |
| `uk_txn_account_direction` | UNIQUE | `txn_no, account_id, direction` | 同一凭证中同一账户同一方向只出现一次 | migrations/account/000001_init.up.sql |
| `idx_account_id_id` | INDEX | `account_id, id` | 按账户分页、核对汇总 | migrations/account/000001_init.up.sql |
| `idx_created_at` | INDEX | `created_at` | 按日统计 | migrations/account/000001_init.up.sql |
| `ck_ledger_entry_amount` | CHECK | `amount > 0` | | migrations/account/000001_init.up.sql |
| `ck_ledger_entry_direction` | CHECK | `direction IN ('DEBIT','CREDIT')` | | migrations/account/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** N:1 同分片 `ledger_txn`、N:1 同分片 `account`。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** 平台子账户（OPT-03）是 `account` 中的独立行，不改变本表结构；缓冲入账（OPT-05，P2）会使平台账户的 `balance_after` 失去意义，实施前需先追加决策。

### 4.4 `account_freeze` — 冻结单

- **Purpose:** 记录商户账户的资金冻结，支撑 Freeze / ConfirmFreeze / ReleaseFreeze 幂等。两种用途：退款（`biz_type = REFUND`，biz_no = refund_no）和跨分片转账付款方分支（`biz_type = TRANSFER`，biz_no = transfer_no，位于付款方分片）。
- **Lifecycle:** FROZEN → CONFIRMED / RELEASED，终态不可变。另有一种直接以 RELEASED 插入的“墓碑记录”：ReleaseFreeze 找不到冻结单时（空回滚）插入，不改变账户 `frozen_amount`，用于占住 `uk_biz`，让迟到的 Freeze 冲突并返回 `40004`（防悬挂，需求 ACC-05）。因此 RELEASED 记录不一定对应过一次真实冻结。
- **Sensitivity:** 内部财务数据。
- **Authoritative source:** `migrations/account/000001_init.up.sql。
- **Write owners/read consumers:** account 写；recon 只读（REC-05 检查 TRANSFER 冻结单）。
- **Retention/archival:** 长期保留。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `freeze_no` | VARCHAR(32) | 否 | — | `uk_freeze_no` | `FZ` 前缀 | 内部 |
| `biz_type` | VARCHAR(32) | 否 | — | `uk_biz` 1、`ck_freeze_biz_type` | REFUND / TRANSFER | 内部 |
| `biz_no` | VARCHAR(64) | 否 | — | `uk_biz` 2 | refund_no 或 transfer_no | 内部 |
| `account_id` | BIGINT UNSIGNED | 否 | — | `idx_account_status` 1 | 被冻结的商户账户（只允许 `allow_overdraft = 0`，ACC-04） | 内部 |
| `amount` | BIGINT | 否 | — | `ck_freeze_amount` | > 0 | 财务 |
| `status` | VARCHAR(16) | 否 | — | `idx_account_status` 2、`ck_freeze_status` | FROZEN / CONFIRMED / RELEASED | 内部 |
| `txn_no` | VARCHAR(32) | 是 | NULL | — | CONFIRMED 时的记账凭证：REFUND 对应 REFUND 凭证，TRANSFER 对应 TRANSFER_OUT 凭证。确认成功时与 `version + 1` 同一条 CAS 写入 | 内部 |
| `version` | BIGINT UNSIGNED | 否 | 0 | — | 解冻成功时 +1；确认成功时 +1 并写入 `txn_no`；直接插入的 RELEASED 墓碑保持 0，且不改账户（ACC-05） | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/account/000001_init.up.sql |
| `uk_freeze_no` | UNIQUE | `freeze_no` | | migrations/account/000001_init.up.sql |
| `uk_biz` | UNIQUE | `biz_type, biz_no` | 冻结幂等；墓碑占位实现防悬挂 | migrations/account/000001_init.up.sql |
| `idx_account_status` | INDEX | `account_id, status` | 核对 `frozen_amount` | migrations/account/000001_init.up.sql |
| `ck_freeze_amount` | CHECK | `amount > 0` | | migrations/account/000001_init.up.sql |
| `ck_freeze_status` | CHECK | `status IN ('FROZEN','CONFIRMED','RELEASED')` | | migrations/account/000001_init.up.sql |
| `ck_freeze_biz_type` | CHECK | `biz_type IN ('REFUND','TRANSFER')` | 冻结用途枚举（ACC-04 第 1 条） | migrations/account/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** N:1 同分片 `account`；0..1:1 同分片 `ledger_txn`；REFUND 时 1:1 `refund_order.refund_no`；TRANSFER 时 0..1:1 `transfer_order.transfer_no`（mode=TCC，付款方分片）。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** ACC-10 在途守恒按 `biz_type = 'TRANSFER' AND status = 'CONFIRMED'` 汇总金额，全量模式下扫描本表；数据量大时可考虑 `(biz_type, status)` 索引（§6）。

### 4.5 `transfer_branch` — 转账分支

- **Purpose:** account 侧的转账分支记录，`(transfer_no, role)` 在分片内唯一。两种角色：
  - `LOCAL`：同分片转账（ACC-07）的最终结果。成功时与 TRANSFER 凭证同一事务写入 SUCCESS；余额不足时在另一事务中写入 FAILED 墓碑（`fail_code = INSUFFICIENT_BALANCE`），之后同一 `transfer_no` 一律返回 FAILED，保证重试结果确定。
  - `PAYEE`：跨分片转账收款方分支（ACC-14），位于收款方分片。Try 写入 TRIED（不动余额），Confirm CAS 为 CONFIRMED 并记 TRANSFER_IN 凭证，Cancel CAS 为 CANCELLED；Cancel 先到时直接插入 CANCELLED 墓碑（空回滚），迟到的 Try 在唯一索引上冲突并返回 `40004`（防悬挂）。
- **Lifecycle:** LOCAL：直接写入 SUCCESS / FAILED，不再变化。PAYEE：TRIED → CONFIRMED / CANCELLED；空回滚时直接写入 CANCELLED。终态不可变（§2.7）。
- **Sensitivity:** 内部财务数据。
- **Authoritative source:** `migrations/account/000001_init.up.sql。
- **Write owners/read consumers:** account 写；account 核对任务（ACC-10 在途守恒）、recon 只读（REC-05）。
- **Retention/archival:** 永久保留（§5.10）。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `transfer_no` | VARCHAR(32) | 否 | — | `uk_transfer_role` 1 | 转账单号，`TF` 前缀，由 payment 生成 | 内部 |
| `role` | VARCHAR(16) | 否 | — | `uk_transfer_role` 2、`ck_branch_role` | LOCAL / PAYEE | 内部 |
| `payer_merchant_id` | VARCHAR(32) | 是 | NULL | `ck_branch_payer`、`ck_branch_self` | 付款商户；LOCAL 必填，PAYEE 为 NULL（收款方分支不感知付款方） | 内部 |
| `payee_merchant_id` | VARCHAR(32) | 否 | — | — | 收款商户 | 内部 |
| `amount` | BIGINT | 否 | — | `ck_branch_amount` | > 0；重复请求金额不一致返回 `10006` | 财务 |
| `status` | VARCHAR(16) | 否 | — | `ck_branch_status` | LOCAL：SUCCESS / FAILED；PAYEE：TRIED / CONFIRMED / CANCELLED | 内部 |
| `fail_code` | VARCHAR(32) | 是 | NULL | `ck_branch_fail_code` | 仅 LOCAL FAILED 填写，目前只有 `INSUFFICIENT_BALANCE`（`docs/03-conventions.md` §5.4） | 内部 |
| `txn_no` | VARCHAR(32) | 是 | NULL | — | LOCAL SUCCESS 的 TRANSFER 凭证、PAYEE CONFIRMED 的 TRANSFER_IN 凭证；同一事务内插入或 CAS 后回写 | 内部 |
| `version` | BIGINT UNSIGNED | 否 | 0 | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/account/000001_init.up.sql |
| `uk_transfer_role` | UNIQUE | `transfer_no, role` | 分支幂等、失败墓碑与空回滚墓碑的占位（`docs/03-conventions.md` §2.4）；REC-05 按 `transfer_no` 前缀查询 | migrations/account/000001_init.up.sql |
| `ck_branch_amount` | CHECK | `amount > 0` | | migrations/account/000001_init.up.sql |
| `ck_branch_role` | CHECK | `role IN ('LOCAL','PAYEE')` | | migrations/account/000001_init.up.sql |
| `ck_branch_status` | CHECK | `(role = 'LOCAL' AND status IN ('SUCCESS','FAILED')) OR (role = 'PAYEE' AND status IN ('TRIED','CONFIRMED','CANCELLED'))` | 角色与状态组合 | migrations/account/000001_init.up.sql |
| `ck_branch_payer` | CHECK | `(role = 'LOCAL') = (payer_merchant_id IS NOT NULL)` | LOCAL 记录双方，PAYEE 只记收款方 | migrations/account/000001_init.up.sql |
| `ck_branch_self` | CHECK | `payer_merchant_id IS NULL OR payer_merchant_id <> payee_merchant_id` | 不允许转给自己（ACC-07 第 1 条） | migrations/account/000001_init.up.sql |
| `ck_branch_fail_code` | CHECK | `(role = 'LOCAL' AND status = 'FAILED') = (fail_code IS NOT NULL)` | 失败墓碑必须带原因码，其他记录不带 | migrations/account/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** 0..1:1 `pay_payment.transfer_order.transfer_no`（LOCAL 在共同分片，PAYEE 在收款方分片）；0..1:1 同分片 `ledger_txn.txn_no`；`payee_merchant_id`、`payer_merchant_id` 对应同分片 MERCHANT `account.owner_id`。

#### Operational Notes

- **Backfill/migration considerations:** 无，v3.0 新增表。
- **Compatibility/deprecation notes:** ACC-07 事务 1 先插入 SUCCESS 再记账、最后回写 `txn_no`，因此数据库不约束“SUCCESS / CONFIRMED 必有 `txn_no`”，由同一事务保证。ACC-10 在途守恒按 `role = 'PAYEE' AND status = 'CONFIRMED'` 汇总金额，目前全表扫描（§6）。

### 4.6 `merchant` — 商户

- **Purpose:** 商户基础信息、签名密钥、通知地址、费率。
- **Lifecycle:** 种子数据创建，ACTIVE / DISABLED。
- **Sensitivity:** `app_secret_cipher` 为高敏数据（密文）。
- **Authoritative source:** `migrations/payment/000001_init.up.sql。
- **Write owners/read consumers:** payment 写；网关经 RPC 读取解密后的密钥并在进程内缓存。
- **Retention/archival:** 长期。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `merchant_id` | VARCHAR(32) | 否 | — | `uk_merchant_id` | 商户号，如 `M100001`；数字部分决定账户分片 | 内部 |
| `merchant_name` | VARCHAR(128) | 否 | — | — | 名称 | 内部 |
| `app_id` | VARCHAR(32) | 否 | — | `uk_app_id` | 请求头 `X-App-Id` | 内部 |
| `app_secret_cipher` | VARBINARY(256) | 否 | — | — | AES-256-GCM 密文（`version(1B)=1` + nonce(12B) + ciphertext + tag(16B)，无 AAD），由 `pkg/secretbox` 读写，主密钥来自环境变量 `SECRET_MASTER_KEY`。比明文多 29 字节，明文最长 227 字节 | 高敏，禁止日志输出 |
| `secret_version` | INT | 否 | 1 | — | 密钥轮换版本 | 内部 |
| `notify_url` | VARCHAR(512) | 否 | `''` | — | 默认通知地址，订单或转账单可覆盖 | 内部 |
| `fee_rate_bp` | INT | 否 | 0 | `ck_merchant_fee_rate` | 费率（万分之一），0–10000。种子写下的初始费率，不是生效费率 | 内部 |
| `status` | VARCHAR(16) | 否 | `'ACTIVE'` | `ck_merchant_status` | DISABLED 时网关拒绝请求，也不能作为转账收款方 | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/payment/000001_init.up.sql |
| `uk_merchant_id` | UNIQUE | `merchant_id` | | migrations/payment/000001_init.up.sql |
| `uk_app_id` | UNIQUE | `app_id` | 验签时按 app_id 查找 | migrations/payment/000001_init.up.sql |
| `ck_merchant_fee_rate` | CHECK | `fee_rate_bp BETWEEN 0 AND 10000` | | migrations/payment/000001_init.up.sql |
| `ck_merchant_status` | CHECK | 枚举 | | migrations/payment/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** 1:N `payment_order`、`refund_order`、`transfer_order`（付款方与收款方）；1:N `merchant_fee_rate`；1:1 所属分片的 `account`（MERCHANT_SETTLE）。

#### Operational Notes

- **Backfill/migration considerations:** BASE-03 种子按 BASE-14 的密文格式（`version(1B)+nonce(12B)+ciphertext+tag`，version=1）用标准库加密后写入。明文格式是 `DEMO_APP_SECRET_FORMAT`，只出现在环境文件。重复执行不覆盖已有密文。BASE-14 起种子改用 `pkg/secretbox` 加密，格式不变；BASE-03 写入的密文由 `pkg/secretbox` 直接解开，无需回填。
- **Compatibility/deprecation notes:** 密钥轮换（双密钥并行）为 P2。`fee_rate_bp` 自 PAY-10 起不是生效费率。

### 4.6.1 `merchant_fee_rate` — 商户费率历史

- **Purpose:** 记录商户生效费率。下单和订单手续费核对都取 `effective_from <= payment_order.created_at` 的最近一档。`merchant.fee_rate_bp` 不是生效费率。
- **Lifecycle:** 只追加。不更新、不删除已有档。新档的 `effective_from` 必须严格晚于该商户已有最大生效时间；相同时间相同费率是幂等成功。
- **Sensitivity:** 内部配置，无密钥。
- **Authoritative source:** `migrations/payment/000002_merchant_fee_rate.up.sql`。
- **Write owners/read consumers:** payment 追加；订单手续费核对只读。种子在商户事务内幂等补第一档。
- **Retention/archival:** 永久。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `merchant_id` | VARCHAR(32) | 否 | — | `uk_merchant_effective_from` 1 | 商户号。登记前锁 `merchant` 行；商户不存在则不插入 | 内部 |
| `fee_rate_bp` | INT | 否 | — | `ck_mfr_fee_rate` | 费率（万分之一），0–10000 | 内部 |
| `effective_from` | DATETIME(3) | 否 | — | `uk_merchant_effective_from` 2 | 生效时间，含该时刻。写入前截断到毫秒 | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | 追加时间。没有 `updated_at` | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/payment/000002_merchant_fee_rate.up.sql |
| `uk_merchant_effective_from` | UNIQUE | `merchant_id, effective_from` | 同一时刻只有一档；相同时间相同费率幂等，不同费率 `10006` | migrations/payment/000002_merchant_fee_rate.up.sql |
| `ck_mfr_fee_rate` | CHECK | `fee_rate_bp BETWEEN 0 AND 10000` | 与 `merchant.ck_merchant_fee_rate` 不是同一个约束 | migrations/payment/000002_merchant_fee_rate.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** N:1 `merchant`。核对时与 `payment_order.created_at` 比较，无物理外键。

#### Operational Notes

- **Backfill/migration considerations:** `000002` 的 up 用 `INSERT … SELECT` 把已有商户的 `fee_rate_bp`、`created_at` 抄成第一档；`merchant` 为空时插入 0 行。种子重复执行命中唯一键后不改费率。
- **Compatibility/deprecation notes:** 只追加。不要回写 `merchant.fee_rate_bp` 来表示当前费率。

### 4.7 `payment_order` — 支付单

- **Purpose:** 支付状态机（`docs/02-requirements.md` PAY-04）。
- **Lifecycle:** INIT → PAYING → SUCCESS / FAILED / CLOSED；INIT → CLOSED。
- **Sensitivity:** 内部交易数据，`subject` 不得包含个人信息。
- **Authoritative source:** `migrations/payment/000001_init.up.sql。
- **Write owners/read consumers:** payment 写；recon 只读。
- **Retention/archival:** 长期。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `pay_no` | VARCHAR(32) | 否 | — | `uk_pay_no` | `PA` 前缀，同时作为渠道请求号 | 内部 |
| `merchant_id` | VARCHAR(32) | 否 | — | `uk_merchant_out_trade_no` 1 | | 内部 |
| `out_trade_no` | VARCHAR(64) | 否 | — | `uk_merchant_out_trade_no` 2 | 商户订单号，下单幂等键 | 内部 |
| `amount` | BIGINT | 否 | — | `ck_pay_amount` | > 0，网关限制 ≤ 10,000,000 | 财务 |
| `currency` | CHAR(3) | 否 | `'CNY'` | `ck_pay_currency` | | 内部 |
| `subject` | VARCHAR(128) | 否 | — | — | 商品描述 | 内部 |
| `status` | VARCHAR(16) | 否 | — | `idx_status_expire_at` 1、`ck_pay_status` | 状态 | 内部 |
| `channel` | VARCHAR(16) | 是 | NULL | — | 发起支付时确定 | 内部 |
| `fee_amount` | BIGINT | 否 | 0 | `ck_pay_fee` | 下单时按当时生效费率固化，0 ≤ fee ≤ amount。核对只报告差异，不改本列 | 财务 |
| `refunded_amount` | BIGINT | 否 | 0 | `ck_pay_refunded` | 已占用退款额（处理中 + 成功），0–amount | 财务 |
| `notify_url` | VARCHAR(512) | 否 | `''` | — | 为空时使用商户默认地址 | 内部 |
| `expire_at` | DATETIME(3) | 否 | — | `idx_status_expire_at` 2 | 超时关单时间 | 内部 |
| `paid_at` | DATETIME(3) | 是 | NULL | — | 渠道完成时间，内部对账按此归属账期 | 内部 |
| `closed_at` | DATETIME(3) | 是 | NULL | — | | 内部 |
| `fail_code` | VARCHAR(32) | 是 | NULL | — | 失败原因码：`CHANNEL_FAILED`、`CHANNEL_REJECTED`（登记见 `docs/03-conventions.md` §5.4） | 内部 |
| `fail_msg` | VARCHAR(255) | 是 | NULL | — | | 内部 |
| `version` | BIGINT UNSIGNED | 否 | 0 | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | `idx_pay_merchant_created` 2 | 手续费核对按此与 `effective_from` 比较 | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | 手续费核对按 `id` 分页 | migrations/payment/000001_init.up.sql |
| `uk_pay_no` | UNIQUE | `pay_no` | | migrations/payment/000001_init.up.sql |
| `uk_merchant_out_trade_no` | UNIQUE | `merchant_id, out_trade_no` | 下单幂等兜底（已验证重复插入报 1062） | migrations/payment/000001_init.up.sql |
| `idx_status_expire_at` | INDEX | `status, expire_at` | 超时关单扫描 | migrations/payment/000001_init.up.sql |
| `idx_pay_merchant_created` | INDEX | `merchant_id, created_at` | 按商户和时间扫描订单 | migrations/payment/000002_merchant_fee_rate.up.sql |
| `ck_pay_amount` / `ck_pay_fee` / `ck_pay_refunded` / `ck_pay_currency` / `ck_pay_status` | CHECK | 见 DDL | 金额边界与枚举 | migrations/payment/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** N:1 `merchant`；1:N `refund_order`；1:1 `channel_txn`（PAY）；0..1:1 商户所属分片的 `ledger_txn`（PAY）。

#### Operational Notes

- **Backfill/migration considerations:** PAY-10 只加 `idx_pay_merchant_created`，不改已有列。手续费核对按主键分页，不回写 `fee_amount`。
- **Compatibility/deprecation notes:** 内部对账按 `paid_at` 扫描，数据量大时需要 `idx_paid_at`，第 6 周按实际查询补充。

### 4.8 `refund_order` — 退款单

- **Purpose:** 退款状态机（`docs/02-requirements.md` §4.5）。
- **Lifecycle:** INIT → PROCESSING → SUCCESS / FAILED；INIT → FAILED。
- **Sensitivity:** 内部交易数据。
- **Authoritative source:** `migrations/payment/000001_init.up.sql。
- **Write owners/read consumers:** payment 写；recon 只读。
- **Retention/archival:** 长期。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `refund_no` | VARCHAR(32) | 否 | — | `uk_refund_no` | `RF` 前缀 | 内部 |
| `merchant_id` | VARCHAR(32) | 否 | — | `uk_merchant_out_refund_no` 1 | | 内部 |
| `out_refund_no` | VARCHAR(64) | 否 | — | `uk_merchant_out_refund_no` 2 | 退款幂等键 | 内部 |
| `pay_no` | VARCHAR(32) | 否 | — | `idx_pay_no` | 原支付单 | 内部 |
| `amount` | BIGINT | 否 | — | `ck_refund_amount` | > 0 | 财务 |
| `currency` | CHAR(3) | 否 | `'CNY'` | `ck_refund_currency` | | 内部 |
| `reason` | VARCHAR(255) | 否 | `''` | — | | 内部 |
| `status` | VARCHAR(16) | 否 | — | `idx_status_updated_at` 1、`ck_refund_status` | | 内部 |
| `channel` | VARCHAR(16) | 否 | — | — | 与原支付渠道相同 | 内部 |
| `fail_code` | VARCHAR(32) | 是 | NULL | — | 失败原因码：`CHANNEL_FAILED`、`CHANNEL_REJECTED`、`INSUFFICIENT_BALANCE`、`FREEZE_CONFLICT`、`REFUND_TIMEOUT`（登记见 `docs/03-conventions.md` §5.4） | 内部 |
| `fail_msg` | VARCHAR(255) | 是 | NULL | — | | 内部 |
| `succeeded_at` | DATETIME(3) | 是 | NULL | — | 渠道完成时间 | 内部 |
| `version` | BIGINT UNSIGNED | 否 | 0 | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | `idx_status_updated_at` 2 | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/payment/000001_init.up.sql |
| `uk_refund_no` | UNIQUE | `refund_no` | | migrations/payment/000001_init.up.sql |
| `uk_merchant_out_refund_no` | UNIQUE | `merchant_id, out_refund_no` | 退款幂等兜底 | migrations/payment/000001_init.up.sql |
| `idx_pay_no` | INDEX | `pay_no` | 查订单下的退款 | migrations/payment/000001_init.up.sql |
| `idx_status_updated_at` | INDEX | `status, updated_at` | 卡单恢复扫描 INIT | migrations/payment/000001_init.up.sql |
| `ck_refund_*` | CHECK | 见 DDL | | migrations/payment/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** N:1 `payment_order`；1:1 `channel_txn`（REFUND）；1:1 商户所属分片的 `account_freeze`（REFUND）。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** 卡单恢复按 `updated_at` 判断（使用 `idx_status_updated_at`）。

### 4.9 `transfer_order` — 转账单

- **Purpose:** 商户间转账的状态机与跨分片 TCC 协调者的持久化状态（`docs/02-requirements.md` §4.6、D18）。`mode` 在创建时由双方分片决定：同分片 LOCAL，由 account 一个本地事务完成；跨分片 TCC，由 payment 驱动付款方冻结分支与收款方 `transfer_branch` 分支。
- **Lifecycle:** LOCAL：TRYING → SUCCESS / FAILED。TCC：TRYING → CONFIRMING → SUCCESS；TRYING → CANCELLING → FAILED。TRYING 是唯一可以改变决策的状态；CONFIRMING、CANCELLING 落盘后只能前进，由恢复任务（TRF-04）重试到终态。
- **Sensitivity:** 内部交易数据，`remark` 不得包含个人信息。
- **Authoritative source:** `migrations/payment/000001_init.up.sql。
- **Write owners/read consumers:** payment 写（TRF-01~04）；recon 只读（REC-05）。
- **Retention/archival:** 永久保留（§5.10）。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `transfer_no` | VARCHAR(32) | 否 | — | `uk_transfer_no` | `TF` 前缀；同时作为 account 侧冻结单 `biz_no`、分支 `transfer_no`、转账凭证 `biz_no` | 内部 |
| `merchant_id` | VARCHAR(32) | 否 | — | `uk_merchant_out_transfer_no` 1、`ck_transfer_self` | 付款商户（发起方），只有付款商户可查询（TRF-05） | 内部 |
| `out_transfer_no` | VARCHAR(64) | 否 | — | `uk_merchant_out_transfer_no` 2 | 商户转账单号，1–64 位 `[A-Za-z0-9_-]`，创建幂等键 | 内部 |
| `payee_merchant_id` | VARCHAR(32) | 否 | — | `ck_transfer_self` | 收款商户，创建时校验存在、ACTIVE 且不是付款商户（`30008`） | 内部 |
| `amount` | BIGINT | 否 | — | `ck_transfer_amount` | > 0，网关限制 ≤ 10,000,000；不收手续费 | 财务 |
| `currency` | CHAR(3) | 否 | `'CNY'` | `ck_transfer_currency` | | 内部 |
| `remark` | VARCHAR(128) | 是 | NULL | — | 转账附言，≤ 128 字符 | 内部 |
| `notify_url` | VARCHAR(512) | 否 | `''` | — | 为空时使用付款商户默认地址；参与幂等比较 | 内部 |
| `mode` | VARCHAR(16) | 否 | — | `ck_transfer_mode`、`ck_transfer_mode_shard` | LOCAL / TCC，创建时确定，不再变化 | 内部 |
| `payer_shard` | TINYINT UNSIGNED | 否 | — | `ck_transfer_shard`、`ck_transfer_mode_shard` | `shard.Of(merchant_id)`，0 或 1 | 内部 |
| `payee_shard` | TINYINT UNSIGNED | 否 | — | `ck_transfer_shard`、`ck_transfer_mode_shard` | `shard.Of(payee_merchant_id)`，0 或 1 | 内部 |
| `status` | VARCHAR(16) | 否 | — | `idx_status_next_retry_at` 1、`ck_transfer_status`、`ck_transfer_local_status` | TRYING / CONFIRMING / CANCELLING / SUCCESS / FAILED；LOCAL 不会进入 CONFIRMING、CANCELLING | 内部 |
| `fail_code` | VARCHAR(32) | 是 | NULL | `ck_transfer_fail_code` | `INSUFFICIENT_BALANCE`、`TRY_TIMEOUT`、`ACCOUNT_INVALID`（`docs/03-conventions.md` §5.4）；CANCELLING 时写入，FAILED 保留 | 内部 |
| `txn_no` | VARCHAR(32) | 是 | NULL | `ck_transfer_txn_no` | LOCAL 成功时的 TRANSFER 凭证号；TCC 的两张凭证不回写，按 `transfer_no` 到各分片查询 | 内部 |
| `retry_count` | INT | 否 | 0 | — | 恢复任务抢占次数，决定退避（`min(2^n 秒, 60 秒)`）；≥ 10 计入 `pay_transfer_stuck` | 内部 |
| `next_retry_at` | DATETIME(3) | 否 | — | `idx_status_next_retry_at` 2 | 恢复任务下次接管时间；创建时为 `now + TRANSFER_RECOVER_DELAY` | 内部 |
| `finished_at` | DATETIME(3) | 是 | NULL | `idx_finished_at`、`ck_transfer_finished` | 进入 SUCCESS / FAILED 的时间，内部对账按此归属账期 | 内部 |
| `version` | BIGINT UNSIGNED | 否 | 0 | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/payment/000001_init.up.sql |
| `uk_transfer_no` | UNIQUE | `transfer_no` | | migrations/payment/000001_init.up.sql |
| `uk_merchant_out_transfer_no` | UNIQUE | `merchant_id, out_transfer_no` | 创建幂等兜底（TRF-01 第 3 条），按商户单号查询（TRF-05） | migrations/payment/000001_init.up.sql |
| `idx_status_next_retry_at` | INDEX | `status, next_retry_at` | 恢复任务扫描（TRF-04，`FOR UPDATE SKIP LOCKED`）；已验证 EXPLAIN 走此索引（覆盖索引） | migrations/payment/000001_init.up.sql |
| `idx_finished_at` | INDEX | `finished_at` | 内部对账按日扫描终态转账（REC-05） | migrations/payment/000001_init.up.sql |
| `ck_transfer_amount` | CHECK | `amount > 0` | | migrations/payment/000001_init.up.sql |
| `ck_transfer_currency` | CHECK | `currency = 'CNY'` | | migrations/payment/000001_init.up.sql |
| `ck_transfer_status` | CHECK | `status IN ('TRYING','CONFIRMING','CANCELLING','SUCCESS','FAILED')` | | migrations/payment/000001_init.up.sql |
| `ck_transfer_mode` | CHECK | `mode IN ('LOCAL','TCC')` | | migrations/payment/000001_init.up.sql |
| `ck_transfer_shard` | CHECK | `payer_shard < 2 AND payee_shard < 2` | 分片数固定为 2（D17） | migrations/payment/000001_init.up.sql |
| `ck_transfer_mode_shard` | CHECK | `(mode = 'LOCAL') = (payer_shard = payee_shard)` | 同分片才走本地事务（TRF-01 第 4 条） | migrations/payment/000001_init.up.sql |
| `ck_transfer_self` | CHECK | `merchant_id <> payee_merchant_id` | 不允许转给自己 | migrations/payment/000001_init.up.sql |
| `ck_transfer_local_status` | CHECK | `mode = 'TCC' OR status IN ('TRYING','SUCCESS','FAILED')` | LOCAL 没有两阶段状态 | migrations/payment/000001_init.up.sql |
| `ck_transfer_fail_code` | CHECK | `(status IN ('CANCELLING','FAILED')) = (fail_code IS NOT NULL)` | 取消与失败必须带原因码；TRYING → CANCELLING 的 CAS 必须同时写 `fail_code` | migrations/payment/000001_init.up.sql |
| `ck_transfer_finished` | CHECK | `(status IN ('SUCCESS','FAILED')) = (finished_at IS NOT NULL)` | 终态必须写 `finished_at`，否则 REC-05 漏检 | migrations/payment/000001_init.up.sql |
| `ck_transfer_txn_no` | CHECK | `(mode = 'LOCAL' AND status = 'SUCCESS') = (txn_no IS NOT NULL)` | LOCAL 成功必须回写凭证号，其他情况不填 | migrations/payment/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** N:1 `merchant`（付款方 `merchant_id`、收款方 `payee_merchant_id`）。LOCAL：0..1:1 `pay_account_{payer_shard}.transfer_branch(role = LOCAL)` 与 `ledger_txn(TRANSFER)`。TCC：0..1:1 `pay_account_{payer_shard}.account_freeze(TRANSFER)` 与 `ledger_txn(TRANSFER_OUT)`，0..1:1 `pay_account_{payee_shard}.transfer_branch(role = PAYEE)` 与 `ledger_txn(TRANSFER_IN)`。1:N `outbox`（`transfer.succeeded` / `transfer.failed`），1:N `merchant_notify`（TRANSFER）。以上跨 schema 关系均由协调流程和 REC-05 保证。

#### Operational Notes

- **Backfill/migration considerations:** 无，v3.0 新增表。
- **Compatibility/deprecation notes:** 终态时 `next_retry_at` 保留最后一次值，扫描按 `status` 过滤，不需要清空。分片数固定为 2 且不在线扩容（D17），`payer_shard` / `payee_shard` 写入后不再变化。

### 4.10 `channel_txn` — 渠道交互流水

- **Purpose:** 与渠道的一次交易，记录渠道流水号、回调次数和查单调度。转账不经过渠道，不写本表。
- **Lifecycle:** PROCESSING → SUCCESS / FAILED / CLOSED。
- **Sensitivity:** 内部。
- **Authoritative source:** `migrations/payment/000001_init.up.sql。
- **Write owners/read consumers:** payment 写；recon 只读（外部对账的本地侧）。
- **Retention/archival:** 长期。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `channel` | VARCHAR(16) | 否 | — | `uk_channel_txn_no` 1、`idx_channel_finished_at` 1 | 如 `mockpay` | 内部 |
| `txn_type` | VARCHAR(16) | 否 | — | `ck_ctxn_type` | PAY / REFUND | 内部 |
| `biz_no` | VARCHAR(32) | 否 | — | `uk_biz_no` | pay_no 或 refund_no，作为渠道 req_no；前缀不同，跨类型不冲突 | 内部 |
| `channel_txn_no` | VARCHAR(64) | 是 | NULL | `uk_channel_txn_no` 2 | 渠道受理后回填；NULL 不参与唯一 | 内部 |
| `amount` | BIGINT | 否 | — | `ck_ctxn_amount` | 回调金额必须与此一致 | 财务 |
| `status` | VARCHAR(16) | 否 | — | `idx_status_next_query_at` 1、`ck_ctxn_status` | | 内部 |
| `channel_status_raw` | VARCHAR(32) | 是 | NULL | — | 渠道原始状态 | 内部 |
| `channel_finished_at` | DATETIME(3) | 是 | NULL | `idx_channel_finished_at` 2 | 渠道完成时间，外部对账归属账期 | 内部 |
| `notify_count` | INT | 否 | 0 | — | 收到回调次数，A3 验证依据之一 | 内部 |
| `query_count` | INT | 否 | 0 | — | 查单次数，决定退避档位 | 内部 |
| `next_query_at` | DATETIME(3) | 是 | NULL | `idx_status_next_query_at` 2 | 下次查单时间；终态后置 NULL | 内部 |
| `last_query_at` | DATETIME(3) | 是 | NULL | — | | 内部 |
| `version` | BIGINT UNSIGNED | 否 | 0 | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/payment/000001_init.up.sql |
| `uk_biz_no` | UNIQUE | `biz_no` | 一笔订单一次渠道交易 | migrations/payment/000001_init.up.sql |
| `uk_channel_txn_no` | UNIQUE | `channel, channel_txn_no` | 原草案约束，防止同一渠道流水关联两笔订单 | migrations/payment/000001_init.up.sql |
| `idx_status_next_query_at` | INDEX | `status, next_query_at` | 查单补偿扫描 | migrations/payment/000001_init.up.sql |
| `idx_channel_finished_at` | INDEX | `channel, channel_finished_at` | 对账按日扫描 | migrations/payment/000001_init.up.sql |
| `ck_ctxn_*` | CHECK | 见 DDL | | migrations/payment/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** 1:1 `payment_order` 或 `refund_order`；对应 `mock_txn.req_no`。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** 无。

### 4.11 `outbox` — 本地消息表

- **Purpose:** 与业务状态同事务写入的待投递事件（`docs/02-requirements.md` MQ-01、MQ-02）。事件类型：`payment.succeeded`、`refund.succeeded`、`refund.failed`、`transfer.succeeded`、`transfer.failed`。
- **Lifecycle:** PENDING → SENT / DEAD；重放时回到 PENDING。
- **Sensitivity:** payload 只含单号与金额，无个人信息。
- **Authoritative source:** `migrations/payment/000001_init.up.sql。
- **Write owners/read consumers:** payment 业务事务与投递器。
- **Retention/archival:** SENT 保留 7 天（P1 清理任务）。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | 投递按 id 顺序 | 内部 |
| `event_id` | VARCHAR(32) | 否 | — | `uk_event_id` | `EV` 前缀 | 内部 |
| `event_type` | VARCHAR(64) | 否 | — | `idx_biz_no_event_type` 2 | `<聚合>.<过去式事件>`，见 Purpose；不加 CHECK | 内部 |
| `topic` | VARCHAR(128) | 否 | — | — | Kafka topic | 内部 |
| `msg_key` | VARCHAR(64) | 否 | — | — | 分区键：支付、退款事件取原 pay_no，转账事件取 transfer_no | 内部 |
| `biz_no` | VARCHAR(32) | 否 | — | `idx_biz_no_event_type` 1 | 事件对应的业务单号：支付事件 pay_no，退款事件 refund_no，转账事件 transfer_no | 内部 |
| `payload` | JSON | 否 | — | — | 完整事件信封 | 内部 |
| `status` | VARCHAR(16) | 否 | `'PENDING'` | `idx_status_next_retry_at` 1 | | 内部 |
| `retry_count` | INT | 否 | 0 | — | 第 20 次失败后置 DEAD | 内部 |
| `next_retry_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | `idx_status_next_retry_at` 2 | 下次投递时间 | 内部 |
| `last_error` | VARCHAR(512) | 是 | NULL | — | 截断的错误信息 | 内部 |
| `sent_at` | DATETIME(3) | 是 | NULL | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/payment/000001_init.up.sql |
| `uk_event_id` | UNIQUE | `event_id` | | migrations/payment/000001_init.up.sql |
| `idx_status_next_retry_at` | INDEX | `status, next_retry_at` | 投递扫描；已验证 `FOR UPDATE SKIP LOCKED` 查询走此索引（覆盖索引） | migrations/payment/000001_init.up.sql |
| `idx_biz_no_event_type` | INDEX | `biz_no, event_type` | MQ-05 按业务单号重放 | migrations/payment/000001_init.up.sql |
| `ck_outbox_status` | CHECK | 枚举 | | migrations/payment/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** `msg_key` 为 `payment_order.pay_no` 或 `transfer_order.transfer_no`；`biz_no` 为 `payment_order.pay_no`、`refund_order.refund_no` 或 `transfer_order.transfer_no`；1:1 `merchant_notify.event_id`（通知类事件）。

#### Operational Notes

- **Backfill/migration considerations:** 若其他服务也需要发事件，在各自 schema 中创建同结构的 outbox 表，`pkg/outbox` 通用。
- **Compatibility/deprecation notes:** 转账事件只用于商户通知，不驱动记账（需求 §4.6）。

### 4.12 `merchant_notify` — 商户通知

- **Purpose:** 商户异步通知与重试（`docs/02-requirements.md` NTF-01、NTF-02）。
- **Lifecycle:** PENDING → SUCCESS / DEAD。
- **Sensitivity:** 内部。
- **Authoritative source:** `migrations/payment/000001_init.up.sql。
- **Write owners/read consumers:** payment 的 `payment-notify` 消费者与通知任务。
- **Retention/archival:** 长期，P2 归档。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `notify_no` | VARCHAR(32) | 否 | — | `uk_notify_no` | `NT` 前缀 | 内部 |
| `event_id` | VARCHAR(32) | 否 | — | `uk_event_id` | 消费幂等 | 内部 |
| `merchant_id` | VARCHAR(32) | 否 | — | — | 被通知商户；转账只通知付款商户 | 内部 |
| `biz_type` | VARCHAR(16) | 否 | — | — | PAY / REFUND / TRANSFER | 内部 |
| `biz_no` | VARCHAR(32) | 否 | — | `idx_biz_no` | pay_no / refund_no / transfer_no | 内部 |
| `notify_url` | VARCHAR(512) | 否 | — | — | 生成时确定：取原支付单（退款同样取原支付单）或转账单的地址，为空时取商户默认 | 内部 |
| `payload` | JSON | 否 | — | — | 通知 body | 内部 |
| `status` | VARCHAR(16) | 否 | `'PENDING'` | `idx_status_next_attempt_at` 1 | | 内部 |
| `attempt_count` | INT | 否 | 0 | — | 上限 `NOTIFY_MAX_ATTEMPTS`（默认 5） | 内部 |
| `next_attempt_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | `idx_status_next_attempt_at` 2 | 固定间隔 `NOTIFY_RETRY_INTERVAL`（默认 30 秒） | 内部 |
| `last_http_status` | INT | 是 | NULL | — | | 内部 |
| `last_error` | VARCHAR(512) | 是 | NULL | — | | 内部 |
| `succeeded_at` | DATETIME(3) | 是 | NULL | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/payment/000001_init.up.sql |
| `uk_notify_no` | UNIQUE | `notify_no` | | migrations/payment/000001_init.up.sql |
| `uk_event_id` | UNIQUE | `event_id` | 同一事件只生成一条通知 | migrations/payment/000001_init.up.sql |
| `idx_status_next_attempt_at` | INDEX | `status, next_attempt_at` | 通知任务扫描 | migrations/payment/000001_init.up.sql |
| `idx_biz_no` | INDEX | `biz_no` | 按单据排查 | migrations/payment/000001_init.up.sql |
| `ck_notify_status` | CHECK | 枚举 | | migrations/payment/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** 1:1 `outbox.event_id`；`biz_no` 指向支付单、退款单或转账单。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** `biz_type` 不加 CHECK，取值由 NTF-01 按事件类型映射。

### 4.13 `recon_batch` — 对账批次

- **Purpose:** 每渠道每账期一个批次及汇总结果。
- **Lifecycle:** INIT → DOWNLOADED → CHECKING → DONE；任一步 → FAILED；FAILED、DONE（force）可重跑。
- **Sensitivity:** 内部。
- **Authoritative source:** `migrations/recon/000001_init.up.sql。
- **Write owners/read consumers:** recon 写；管理接口读。
- **Retention/archival:** 长期。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `batch_no` | VARCHAR(32) | 否 | — | `uk_batch_no` | `RB` 前缀 | 内部 |
| `channel` | VARCHAR(16) | 否 | — | `uk_channel_bill_date` 1 | | 内部 |
| `bill_date` | DATE | 否 | — | `uk_channel_bill_date` 2 | 账期 | 内部 |
| `status` | VARCHAR(16) | 否 | — | `ck_batch_status` | | 内部 |
| `bill_file_hash` | CHAR(64) | 是 | NULL | — | 账单 SHA-256 | 内部 |
| `channel_count` | INT | 否 | 0 | — | 渠道侧笔数 | 内部 |
| `channel_amount` | BIGINT | 否 | 0 | — | 渠道侧金额 | 财务 |
| `local_count` | INT | 否 | 0 | — | 本地侧笔数 | 内部 |
| `local_amount` | BIGINT | 否 | 0 | — | 本地侧金额 | 财务 |
| `matched_count` | INT | 否 | 0 | — | 匹配笔数 | 内部 |
| `diff_count` | INT | 否 | 0 | — | 差错笔数 | 内部 |
| `error_msg` | VARCHAR(512) | 是 | NULL | — | FAILED 原因 | 内部 |
| `started_at` | DATETIME(3) | 是 | NULL | — | 进入 CHECKING 的时间，超过 30 分钟视为卡死（REC-01） | 内部 |
| `finished_at` | DATETIME(3) | 是 | NULL | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/recon/000001_init.up.sql |
| `uk_batch_no` | UNIQUE | `batch_no` | | migrations/recon/000001_init.up.sql |
| `uk_channel_bill_date` | UNIQUE | `channel, bill_date` | 同一账期只有一个批次，防止并发重复对账 | migrations/recon/000001_init.up.sql |
| `ck_batch_status` | CHECK | 枚举 | | migrations/recon/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** 1:N `recon_channel_bill`、`recon_diff`。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** 无。

### 4.14 `recon_channel_bill` — 渠道账单明细

- **Purpose:** 导入的渠道账单行，记录匹配结果。
- **Lifecycle:** 导入后只更新 `match_status`；重跑时按批次删除重导。
- **Sensitivity:** 内部。
- **Authoritative source:** `migrations/recon/000001_init.up.sql。
- **Write owners/read consumers:** recon。
- **Retention/archival:** 可按月归档。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `batch_no` | VARCHAR(32) | 否 | — | `uk_batch_type_channel_txn_no` 1、`idx_batch_biz_no` 1 | | 内部 |
| `channel` | VARCHAR(16) | 否 | — | — | | 内部 |
| `bill_date` | DATE | 否 | — | — | | 内部 |
| `txn_type` | VARCHAR(16) | 否 | — | `uk_batch_type_channel_txn_no` 2 | PAY / REFUND | 内部 |
| `channel_txn_no` | VARCHAR(64) | 否 | — | `uk_batch_type_channel_txn_no` 3 | | 内部 |
| `biz_no` | VARCHAR(32) | 否 | — | `idx_batch_biz_no` 2 | 匹配键 | 内部 |
| `amount` | BIGINT | 否 | — | — | 渠道侧金额 | 财务 |
| `channel_status` | VARCHAR(16) | 否 | — | — | 账单中的状态 | 内部 |
| `channel_finished_at` | DATETIME(3) | 否 | — | — | 渠道完成时间 | 内部 |
| `match_status` | VARCHAR(16) | 否 | `'UNCHECKED'` | `ck_bill_match` | UNCHECKED / MATCHED / DIFF | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/recon/000001_init.up.sql |
| `uk_batch_type_channel_txn_no` | UNIQUE | `batch_no, txn_type, channel_txn_no` | 防止重复导入 | migrations/recon/000001_init.up.sql |
| `idx_batch_biz_no` | INDEX | `batch_no, biz_no` | 按业务单号匹配 | migrations/recon/000001_init.up.sql |
| `ck_bill_type` / `ck_bill_match` | CHECK | 枚举 | | migrations/recon/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** N:1 `recon_batch`；`biz_no` 对应 `channel_txn.biz_no`。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** 跨日处理已砍掉（REC-04 CUT），比对只在本批次内按匹配键查询本地记录，不需要跨批次查询账单。

### 4.15 `recon_diff` — 对账差错

- **Purpose:** 差错明细与自动处理记录（REC-03、REC-05、REC-06）。人工处理与挂账已砍掉（REC-07 CUT），未自动解决的差错保持 PENDING，出现在对账报告和 ERROR 日志中。
- **Lifecycle:** PENDING → RESOLVED / IGNORED，只由自动处理推进（§2.7）。批次重跑时删除该批次的 PENDING 差错，保留 RESOLVED、IGNORED。
- **Sensitivity:** 内部，`handler` 为处理方标识（目前只有 `system`）。
- **Authoritative source:** `migrations/recon/000001_init.up.sql。
- **Write owners/read consumers:** recon；管理接口（触发对账的响应）只读。
- **Retention/archival:** 长期，作为审计记录。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 内部 |
| `diff_no` | VARCHAR(32) | 否 | — | `uk_diff_no` | `RD` 前缀 | 内部 |
| `batch_no` | VARCHAR(32) | 否 | — | `uk_batch_type_biz_no` 1 | 发现差错的批次 | 内部 |
| `channel` | VARCHAR(16) | 否 | — | — | 批次渠道；内部对账差错（LEDGER_MISSING、TRANSFER_MISMATCH）同样取批次渠道 | 内部 |
| `bill_date` | DATE | 否 | — | `idx_status_bill_date` 2 | | 内部 |
| `diff_type` | VARCHAR(32) | 否 | — | `ck_diff_type` | LOCAL_MISSING / CHANNEL_MISSING / AMOUNT_MISMATCH / STATUS_MISMATCH（外部对账）；LEDGER_MISSING / TRANSFER_MISMATCH（内部对账） | 内部 |
| `txn_type` | VARCHAR(16) | 否 | — | `uk_batch_type_biz_no` 2、`ck_diff_txn_type` | PAY / REFUND / TRANSFER；TRANSFER 只用于 TRANSFER_MISMATCH | 内部 |
| `biz_no` | VARCHAR(32) | 否 | — | `uk_batch_type_biz_no` 3 | pay_no / refund_no / transfer_no | 内部 |
| `channel_txn_no` | VARCHAR(64) | 是 | NULL | — | 内部对账差错为 NULL | 内部 |
| `local_amount` | BIGINT | 是 | NULL | — | 本地侧金额，一侧缺失时为 NULL | 财务 |
| `channel_amount` | BIGINT | 是 | NULL | — | 渠道侧金额，一侧缺失或内部对账时为 NULL | 财务 |
| `local_status` | VARCHAR(16) | 是 | NULL | — | 本地侧状态 | 内部 |
| `channel_status` | VARCHAR(16) | 是 | NULL | — | 渠道侧状态 | 内部 |
| `status` | VARCHAR(16) | 否 | `'PENDING'` | `idx_status_bill_date` 1、`ck_diff_status` | PENDING / RESOLVED / IGNORED | 内部 |
| `handle_action` | VARCHAR(32) | 是 | NULL | `ck_diff_action` | REQUERY（调用 `SyncChannelTxn`）/ REPLAY（LEDGER_MISSING 调用 `ReplayEvent`，写入后保持 PENDING，等待后续批次复核） | 内部 |
| `handler` | VARCHAR(64) | 是 | NULL | — | 自动处理为 `system` | 内部 |
| `handle_note` | VARCHAR(512) | 是 | NULL | — | 处理说明；LEDGER_MISSING 金额不等时注明双方金额；TRANSFER_MISMATCH 记录各分片实际情况 | 内部 |
| `resolved_at` | DATETIME(3) | 是 | NULL | — | | 内部 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | | 审计 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 审计 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/recon/000001_init.up.sql |
| `uk_diff_no` | UNIQUE | `diff_no` | | migrations/recon/000001_init.up.sql |
| `uk_batch_type_biz_no` | UNIQUE | `batch_no, txn_type, biz_no` | 同一批次同一交易只记一条差错 | migrations/recon/000001_init.up.sql |
| `idx_status_bill_date` | INDEX | `status, bill_date` | 待处理列表、LEDGER_MISSING 跨批次复核（`status = 'PENDING'` 前缀） | migrations/recon/000001_init.up.sql |
| `ck_diff_type` | CHECK | `diff_type IN ('LOCAL_MISSING','CHANNEL_MISSING','AMOUNT_MISMATCH','STATUS_MISMATCH','LEDGER_MISSING','TRANSFER_MISMATCH')` | | migrations/recon/000001_init.up.sql |
| `ck_diff_txn_type` | CHECK | `txn_type IN ('PAY','REFUND','TRANSFER')` | | migrations/recon/000001_init.up.sql |
| `ck_diff_status` | CHECK | `status IN ('PENDING','RESOLVED','IGNORED')` | | migrations/recon/000001_init.up.sql |
| `ck_diff_action` | CHECK | `handle_action IS NULL OR handle_action IN ('REQUERY','REPLAY')` | 只允许自动处理动作 | migrations/recon/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** N:1 `recon_batch`；`biz_no` 对应 `channel_txn` / 支付单 / 退款单（PAY、REFUND）或 `transfer_order`（TRANSFER）。

#### Operational Notes

- **Backfill/migration considerations:** 无。
- **Compatibility/deprecation notes:** v3.0 删除的列、状态和动作以及 `handle_action` 的改名见 §5。尚无数据，直接修改设计稿。

### 4.16 `mock_txn` — 模拟渠道交易

- **Purpose:** channel_mock 自身的交易记录，用于查单和生成账单；代表外部系统，平台服务不得直接访问。
- **Lifecycle:** PROCESSING → SUCCESS / FAILED / CLOSED。
- **Sensitivity:** 测试数据。
- **Authoritative source:** `migrations/channel_mock/000001_init.up.sql。
- **Write owners/read consumers:** 仅 channel_mock。
- **Retention/archival:** 可随时清空。

#### Columns

| Column | Type | Nullable | Default | Key/index role | Description and business rules | Sensitivity/source |
| --- | --- | --- | --- | --- | --- | --- |
| `id` | BIGINT UNSIGNED | 否 | AUTO_INCREMENT | PK | | 测试 |
| `channel_txn_no` | VARCHAR(64) | 否 | — | `uk_channel_txn_no` | `MC` 前缀 | 测试 |
| `txn_type` | VARCHAR(16) | 否 | — | `uk_type_req_no` 1 | PAY / REFUND | 测试 |
| `req_no` | VARCHAR(32) | 否 | — | `uk_type_req_no` 2 | 平台请求号，受理幂等 | 测试 |
| `orig_req_no` | VARCHAR(32) | 是 | NULL | — | 退款对应的原支付 req_no | 测试 |
| `amount` | BIGINT | 否 | — | — | | 测试 |
| `status` | VARCHAR(16) | 否 | — | `ck_mock_status` | 只用 CAS 推进：`WHERE id = ? AND status = 'PROCESSING'`（无 version 列） | 测试 |
| `scenario` | VARCHAR(32) | 否 | `'NORMAL'` | — | 由金额末两位决定的故障场景（CH-06） | 测试 |
| `notify_url` | VARCHAR(512) | 否 | `''` | — | | 测试 |
| `notify_sent_count` | INT | 否 | 0 | — | 已发送回调次数 | 测试 |
| `finished_at` | DATETIME(3) | 是 | NULL | — | 自动完成的实际时间，应用截到毫秒后写入（CH-01） | 测试 |
| `bill_date` | DATE | 是 | NULL | `idx_bill_date` | 计入账单的日期，= `finished_at` 的东八区日期；只在 SUCCESS 时写，BILL_MISSING 场景与 FAILED 为 NULL | 测试 |
| `created_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) | — | 受理时由 channel_mock 按自己的时钟显式写入（截到毫秒），自动完成的到期时刻 = `created_at` + delay | 测试 |
| `updated_at` | DATETIME(3) | 否 | CURRENT_TIMESTAMP(3) ON UPDATE | — | | 测试 |

#### Keys, Indexes, and Constraints

| Name | Type | Columns/order/predicate | Purpose | Source |
| --- | --- | --- | --- | --- |
| `PRIMARY` | PK | `id` | | migrations/channel_mock/000001_init.up.sql |
| `uk_channel_txn_no` | UNIQUE | `channel_txn_no` | | migrations/channel_mock/000001_init.up.sql |
| `uk_type_req_no` | UNIQUE | `txn_type, req_no` | 渠道侧受理幂等 | migrations/channel_mock/000001_init.up.sql |
| `idx_bill_date` | INDEX | `bill_date` | 生成账单 | migrations/channel_mock/000001_init.up.sql |
| `ck_mock_type` / `ck_mock_status` | CHECK | 枚举 | | migrations/channel_mock/000001_init.up.sql |

#### Relationships

- **Physical foreign keys:** 无。
- **Logical business relationships:** `req_no` 对应平台 `channel_txn.biz_no`（跨系统，不保证一致）。

#### Operational Notes

- **Backfill/migration considerations:** 账单注入（CH-09）的记录直接写入本表，`req_no` 使用平台不存在的单号。
- **Query patterns:** 受理先 INSERT，`uk_type_req_no` 冲突再按 `(txn_type, req_no)` 读已有行；INSERT 报其他错误时也按 `(txn_type, req_no)` 再读一次，确认本次的行是否已提交。自动完成写库结果不确定时，按主键 `id` 读回该行，比较 `status`、`finished_at` 确认那次是否已生效。进程启动时按主键分页扫描 `status = 'PROCESSING'` 重建调度；没有 `status` 索引，只在启动时执行，表很小时可接受。
- **Compatibility/deprecation notes:** 无。

## 5. Change History

| Date | Migration/commit | Tables/relationships affected | Summary | Compatibility/rollout notes |
| --- | --- | --- | --- | --- |
| 2026-10-09 | `docs/schema/schema_draft.sql`（未提交，仓库尚未初始化 git） | 全部 14 张表 | 初始设计稿。相对 PROJECT_GOALS §6：`account` 唯一键改为 `(owner_type, owner_id, account_type, currency)`；记账幂等键上移到 `ledger_txn.uk_biz`；新增 `ledger_txn`、`account_freeze`、`merchant`、`merchant_notify`、`recon_channel_bill`、`mock_txn` | 验证：`docker exec -i <container> mysql -uroot < docs/schema/schema_draft.sql`，目标为一次性容器 `mysql:8.4.11`，执行成功，4 个 schema 共 14 张表、33 个 CHECK 约束；另验证条件扣减余额不足时影响行数为 0、`ck_account_available` 拦截负余额（ERROR 3819）、`uk_merchant_out_trade_no` 拦截重复下单（ERROR 1062）、outbox 扫描使用 `idx_status_next_retry_at`。容器已删除 |
| 2026-10-11 | `docs/schema/schema_draft.sql`（仅注释） | `account_freeze`（语义）、`mock_txn`（注释） | 无 DDL 变化。`account_freeze` 增加墓碑记录语义（空回滚、防悬挂）；退款卡单恢复确定按 `updated_at` 判断（使用 `idx_status_updated_at`）；`mock_txn.scenario` 注释改为引用需求 CH-06 | 已在一次性 `mysql:8.4.11` 容器上重新执行 DDL |
| 2026-10-12 | 无（仅文档） | `payment_order`、`refund_order` | 无 DDL 变化。`fail_code` 明确为字符串原因码，取值统一登记在 `docs/03-conventions.md` §5.4，修正退款单中写成错误码 `40002` 的描述 | 无 SQL 变更，未执行数据库 |
| 2026-10-12 | `docs/schema/schema_draft.sql` | `outbox`、`recon_diff` | `outbox` 新增 `biz_no VARCHAR(32) NOT NULL` 与 `idx_biz_no_event_type (biz_no, event_type)`，支持按业务单号重放（R9）；`recon_diff.ck_diff_status` 新增 `HANDLING`，人工挂账先占位再记账（R11）；FIX_ORDER 后保持 PENDING 等待复核（R10） | 尚无数据，直接改设计稿；在一次性 `mysql:8.4.11` 容器上重新执行 DDL。HANDLING 已在 v3.0 移除（下一行） |
| 2026-10-12 | `docs/schema/schema_draft.sql`（v3.0，对应需求 v3.0、D17–D20） | schema `pay_account` → `pay_account_0` / `pay_account_1`；`account`、`ledger_txn`、`account_freeze`、`outbox`、`merchant_notify`、`recon_diff`；新增 `transfer_branch`、`transfer_order` | 账户分片：删除 `pay_account`，在 `pay_account_0` 完整定义 5 张表，`pay_account_1` 用 `CREATE TABLE ... LIKE` 复制。`account` 新增 `sub_no INT UNSIGNED`，唯一键 `uk_owner_account_type` 改为 `uk_owner_account_type_sub (owner_type, owner_id, account_type, currency, sub_no)`；账户类型改为 MERCHANT_SETTLE / CHANNEL_RECEIVABLE / FEE_INCOME / TRANSIT（删除 SUSPENSE），新增 `ck_account_type_rule`、`ck_account_merchant_sub`。`ledger_txn` 新增 `ck_ledger_txn_biz_type`（PAY / REFUND / TRANSFER / TRANSFER_OUT / TRANSFER_IN / REVERSAL，删除 ADJUST）与 `ck_ledger_txn_reversal`。`account_freeze` 新增 `ck_freeze_biz_type`（REFUND / TRANSFER）。新增分片表 `transfer_branch`（ACC-07 失败墓碑、ACC-14 收款方分支与 CANCELLED 墓碑）与 `pay_payment.transfer_order`（TRF-01~05）。`outbox`、`merchant_notify` 只改注释，允许转账事件（`msg_key` / `biz_no` = transfer_no，`biz_type` = TRANSFER）。`recon_diff`：`ck_diff_status` 只保留 PENDING / RESOLVED / IGNORED（删除 SUSPENDED、HANDLING），`diff_type` 新增 TRANSFER_MISMATCH，新增 `ck_diff_txn_type`（含 TRANSFER）与 `ck_diff_action`（REQUERY / REPLAY；FIX_ORDER 改名 REPLAY，删除 SUSPENSE、IGNORE、REFUND 人工动作），删除 `recheck_count`。DDL 开头增加 `SET NAMES utf8mb4` | 尚无数据，直接改设计稿。验证：`docker exec -i paysys-ddl-check mysql -uroot -proot < docs/schema/schema_draft.sql`，目标为一次性容器 `mysql:8.4.11`，执行成功，5 个 schema 共 21 张表、79 个 CHECK 约束（每个分片 22 个）；对比 `information_schema` 的列、索引、CHECK 表达式和表注释，两个分片一致，只有复制出的 CHECK 名称变为 `<表名>_chk_<n>`。负向测试：LOCAL + TRIED、PAYEE 带付款方、商户 `sub_no = 1`、TRANSIT 不透支、ADJUST 凭证、`recon_diff` HANDLING / SUSPENDED / FIX_ORDER、LOCAL 跨分片、LOCAL CONFIRMING、转给自己、FAILED 无 `finished_at` 均报 ERROR 3819；重复 `(transfer_no, role)`、`uk_owner_account_type_sub`、`uk_merchant_out_transfer_no` 报 ERROR 1062；TRF-04 扫描 EXPLAIN 使用 `idx_status_next_retry_at`。未加 `SET NAMES` 时管道执行按 latin1 解析，中文注释乱码，已修复。容器已删除 |
| 2026-10-14 | `migrations/*/000001_init.{up,down}.sql`；`cmd/bootstrap` | 全部业务表；新增工具表 `schema_migrations` | 设计稿拆成四个迁移目录。账户分片共用 `migrations/account/`，对 `pay_account_0`、`pay_account_1` 各执行一次，CHECK 约束名不再被 `LIKE` 重命名。删除 `docs/schema/schema_draft.sql`。bootstrap 创建五个 schema 和最小权限账号，不放进迁移文件 | 验证：compose 的 `mysql:8.4.11`（`SELECT VERSION()` = 8.4.11）上 `make migrate`、`make migrate DIR=down`、再 `make migrate` 均成功。表数：分片各 6（含 `schema_migrations`）、`pay_payment` 8、`pay_recon` 4、`pay_channel_mock` 2。CHECK：分片各 22、payment 26、recon 7、channel_mock 2，合计 79。两个分片没有 `%_chk_%` 约束名。`scripts/compare-account-shards.sh` 报告结构一致。集成测试用 testcontainers 的 `mysql:8.4.11` 复跑了同一批文件 |
| 2026-10-17 | `migrations/payment/000002_merchant_fee_rate.up.sql`、`.down.sql` | 新增 `merchant_fee_rate`；`payment_order` 增加 `idx_pay_merchant_created` | PAY-10。费率历史只追加，无 `updated_at`。`merchant.fee_rate_bp` 仍是种子初始费率，不是生效费率。up 用 `INSERT … SELECT` 为已有商户补第一档；down 先删索引再删新表，不删 `payment_order` | 验证见 PAY-10 集成测试：MySQL 8.4.11 上 up 后表和索引存在，空商户回填 0 行；执行 down 后表和索引消失、`payment_order` 仍在；再执行 up，已插入商户得到一档且 `effective_from = created_at` |
| 2026-10-19 | 无（仅文档） | `account.version` 的说明 | ACC-04：冻结成功时 `version` 也 +1，与余额变更使用同一列。无 DDL，不改 CHECK | 未执行迁移 |
| 2026-10-20 | 无（仅文档） | `account.version`、`account_freeze.version` 的说明 | ACC-05：解冻成功时冻结单和账户的 `version` 都 +1。墓碑插入不改账户，冻结单 `version` 保持 0。无 DDL，不改 CHECK | 未执行迁移 |
| 2026-10-20 | 无（仅文档） | `account.balance`、`account.frozen_amount`、`account.version`、`account.status`、`account_freeze.txn_no`、`account_freeze.version` 的说明 | ACC-06：确认成功时冻结单 `version` +1 且写入 `txn_no`；商户 `balance` 与 `frozen_amount` 同减，两边账户 `version` 各 +1。无 DDL，不改 CHECK，不改迁移 | 未执行迁移 |
| 2026-10-23 | 无（仅文档） | `merchant.app_secret_cipher` 的说明 | BASE-14：说明补上版本字节（取 1）、无 AAD、由 `pkg/secretbox` 读写、明文最长 227 字节。种子改用 `pkg/secretbox`，格式不变，已有密文不回填。无 DDL，不改迁移 | 未执行迁移；`go test -tags integration ./internal/dbmigrate` 用 `pkg/secretbox` 解开种子入库的三个商户密文 |
| 2026-10-23 | 无（仅文档） | `mock_txn`（说明） | CH-01：`created_at` 由应用时钟显式写入，`finished_at` 截到毫秒，`bill_date` 为其东八区日期且只在 SUCCESS（非 BILL_MISSING）时写；`status` 只用 CAS 推进，不加 version 列；启动扫描 PROCESSING 走主键分页。新增两处读：INSERT 报非 1062 错误后按 `uk_type_req_no` 再读，完成写库结果不确定后按主键读回。无 DDL | 未执行迁移 |

## 6. Open Questions

| Item | Evidence checked | Impact | Owner/status |
| --- | --- | --- | --- |
| `payment_order` 是否需要 `idx_paid_at`、`refund_order` 是否需要 `idx_succeeded_at` | 内部对账按 `paid_at` / `succeeded_at` 扫描（`docs/02-requirements.md` REC-05） | 数据量大时全表扫描 | 第 6 周实现对账时确认 |
| 是否为热点账户引入子账户表或子账户编号规则 | D19、OPT-03、ACC-13 第 4 条 | — | 已关闭（v3.0）：`account.sub_no` + `uk_owner_account_type_sub`，不新增表 |
| `recon_channel_bill` 跨批次按 `biz_no` 查询的索引 | REC-04 已 CUT，REC-03 只在本批次内匹配 | — | 已关闭（v3.0）：不需要 |
| ACC-10 在途守恒汇总 `account_freeze(TRANSFER, CONFIRMED)`、`transfer_branch(PAYEE, CONFIRMED)` 是否需要 `(biz_type, status)` / `(role, status)` 索引 | ACC-10 第 2 条只在全量模式、手动触发 | 全量核对时全表扫描，压测数据量下耗时待测 | 第 7 周压测后决定，待确认 |
| 转账差错归属哪个对账批次 | REC-01 批次按渠道建立，REC-05 第 4 条按 `finished_at` 检查转账；转账与渠道无关 | 目前只有 `mockpay` 一个渠道，TRANSFER_MISMATCH 写入当日该渠道批次；渠道增加后会在每个批次重复检查 | 待确认（需求未定义） |
| TRANSFER_MISMATCH 的 `local_amount` / `local_status` 取值 | REC-05 只规定 `handle_note` 记录各分片情况 | 报告展示口径 | 待确认；建议取转账单金额与状态 |
| 设计稿中 `pay_account_1` 的 CHECK 约束名与分片 0 不同 | BASE-03 在 MySQL 8.4.11 上对两个分片各执行 `migrations/account/000001_init.up.sql`，`%_chk_%` 计数为 0，比较脚本无差异 | 已消除 | 已关闭（2026-10-14） |
