# pay-system 全局约定与编号规范

> 版本：v2.0　最后修改：2026-10-12
> 适用：全部代码、文档、配置。需求见 `docs/02-requirements.md`，接口见 `docs/05-api.md`，表结构见 `docs/database.md`。

本文集中登记会被多个功能点共用的约定：编号体系、业务单号、金额、时间、错误码、幂等与事务规则、命名规范、配置项。新增或修改这些内容时，先改本文并在 §9 记录，再改代码。

## 1. 编号体系

### 1.1 文档内编号

| 编号 | 含义 | 定义位置 | 示例 |
|---|---|---|---|
| `D<n>` | 技术决策 | `docs/01-tech-decisions.md` | D6 |
| `<模块>-<nn>` | 功能点，开发与提交的最小追溯单位 | `docs/02-requirements.md` §3 | ACC-03 |
| `NFR-<类>-<nn>` | 非功能需求 | `docs/02-requirements.md` §5 | NFR-SEC-07 |
| `A<n>` | 验收项（A1–A12） | `PROJECT_GOALS.md` §7、`docs/02-requirements.md` §7 | A3 |
| `S<n>` | 压测场景 | `docs/02-requirements.md` QA-02 | S6 |
| `C<n>` | 故障演练场景 | `docs/02-requirements.md` QA-01 | C5 |
| `V<n>` | 签名测试向量 | `docs/05-api.md` §3.7 | V1 |
| `Q<n>` | 待定事项 | `docs/02-requirements.md` §8.1 | Q2 |
| `M<n>` | 里程碑，对应第 n 周 | `docs/04-dev-plan.md` | M3 |
| `SM-<nn>` / `SL-<nn>` / `CF-<nn>` | 状态机 / 泳道图 / 控制流图 | `docs/06-flows.md` | CF-10 |
| `R<n>` | 绘图中发现的问题及确认结论 | `docs/06-flows.md` §5 | R3 |
| `P0` / `P1` / `P2` | 优先级：必须 / 应该 / 加分 | `docs/02-requirements.md` §0.4 | P0 |

编号一经分配不复用。功能点被砍掉时状态改为 `CUT`，编号保留。

### 1.2 功能点模块前缀

| 前缀 | 模块 | 主要代码位置 |
|---|---|---|
| `BASE` | 工程基础与公共包 | 仓库根、`pkg/`、`deploy/`、`migrations/`、`idl/` |
| `ACC` | 账户与记账 | `service/account/` |
| `CH` | 模拟渠道 | `service/channel_mock/` |
| `PAY` | 支付 | `service/payment/` |
| `REF` | 退款 | `service/payment/` |
| `TRF` | 转账（协调者） | `service/payment/`（`biz/transfer`、`job/transfer_recover`） |
| `OPT` | 性能优化 | 分布在各服务；埋点在 `pkg/obs/` |
| `MQ` | 本地消息表与 Kafka | `pkg/outbox/`、`pkg/mq/`、`service/*/consumer/` |
| `NTF` | 商户通知 | `service/payment/notify/` |
| `GW` | 网关 | `gateway/` |
| `REC` | 对账 | `job/recon/` |
| `OPS` | 可观测与运维 | `pkg/obs/`、`pkg/server/`、`deploy/prometheus/` |
| `TL` | 工具 | `cmd/paycli/`、`cmd/merchant-demo/`、`scripts/` |
| `QA` | 质量保证 | `test/`、`pkg/fault/` |
| `DOC` | 交付文档 | `README.md`、`docs/` |

### 1.3 非功能需求分类

`COR` 资金正确性、`CON` 一致性时效、`PERF` 性能、`REL` 可靠性、`SEC` 安全、`OBS` 可观测、`TEST` 可测试性、`MNT` 可维护性、`DEP` 部署、`CAP` 容量与保留。

## 2. 业务单号与 ID

### 2.1 格式

平台生成的单号为 2 位大写前缀 + 19 位定长十进制，共 21 位，例如 `PA0000000123456789012`。

数字部分是 64 位雪花 ID（BASE-05）：

| 位 | 长度 | 内容 |
|---|---|---|
| 63 | 1 | 符号位，恒为 0 |
| 62–22 | 41 | 毫秒时间戳，纪元 `2026-01-01T00:00:00+08:00`，约可用 69 年 |
| 21–12 | 10 | worker ID（环境变量 `WORKER_ID`，0–1023） |
| 11–0 | 12 | 毫秒内序列号 |

### 2.2 前缀登记

| 前缀 | 对象 | 所在表 | 生成方 |
|---|---|---|---|
| `PA` | 支付单 | `payment_order.pay_no` | payment |
| `RF` | 退款单 | `refund_order.refund_no` | payment |
| `TF` | 转账单 | `transfer_order.transfer_no`；同时作为 `transfer_branch.transfer_no`、`account_freeze.biz_no`（TRANSFER） | payment |
| `TX` | 记账凭证 | `ledger_txn.txn_no` | account |
| `FZ` | 冻结单 | `account_freeze.freeze_no` | account |
| `AC` | 账户 | `account.account_no` | account |
| `EV` | 事件 | `outbox.event_id` | payment |
| `NT` | 商户通知 | `merchant_notify.notify_no` | payment |
| `RB` | 对账批次 | `recon_batch.batch_no` | recon |
| `RD` | 对账差错 | `recon_diff.diff_no` | recon |
| `MC` | 模拟渠道流水 | `mock_txn.channel_txn_no` | channel-mock |

新增前缀必须先在此表登记；`idgen.NewNo` 拒绝未登记的前缀。

### 2.3 其他标识

| 标识 | 格式 | 示例 |
|---|---|---|
| 商户号 `merchant_id` | `M` + 6 位数字 | `M100001` |
| 应用 ID `app_id` | `app_` + 商户号小写 | `app_m100001` |
| 渠道编码 `channel` | 小写字母 | `mockpay` |
| 商户订单号 `out_trade_no`、商户退款单号 `out_refund_no`、商户转账单号 `out_transfer_no` | 1–64 位 `[A-Za-z0-9_-]`，商户内唯一 | `T20261012001` |
| 账户分片 `shard` | 0 或 1，`商户号数字部分 % 2`（ACC-13） | `M100001` → 1 |
| 子账户号 `sub_no` | 商户账户恒为 0；平台账户 `[0, PLATFORM_SUB_ACCOUNTS)`，`crc32(biz_no) % N` | 3 |
| 请求 ID `request_id` | 32 位小写十六进制（W3C trace ID，无 trace 时随机生成） | `0af7651916cd43dd8448eb211c80319c` |
| nonce | 16–32 位 `[A-Za-z0-9]` | `AbCdEf0123456789` |

### 2.4 幂等键与渠道请求号

| 场景 | 幂等键 |
|---|---|
| 创建支付单 | `(merchant_id, out_trade_no)` |
| 创建退款单 | `(merchant_id, out_refund_no)` |
| 创建转账单 | `(merchant_id, out_transfer_no)` |
| 记账 | `(biz_type, biz_no)`，分片内唯一 |
| 冻结 / 解冻 / 确认 | `(biz_type, biz_no)`，分片内唯一 |
| 转账分支（同分片结果、收款方分支） | `(transfer_no, role)`，分片内唯一 |
| 渠道请求 | `req_no`，支付为 `pay_no`，退款为 `refund_no` |
| 渠道交易流水 | `channel_txn.biz_no` |
| 商户通知生成 | `event_id` |
| 对账批次 | `(channel, bill_date)` |
| 对账差错 | `(batch_no, txn_type, biz_no)` |

## 3. 金额

1. 类型 `money.Amount`（`int64`），单位分；数据库 `BIGINT`；IDL `i64`；JSON 整数。全链路禁止浮点数。
2. 币种只有 `CNY`，字段保留以便扩展。
3. 单笔支付、退款、转账 1–10,000,000 分（0.01–100,000.00 元）。
4. 手续费：`fee = (amount × rate_bp + 5000) / 10000`，四舍五入到分；费率单位为基点（1bp = 0.01%），范围 0–10000。下单时计算并固化到 `fee_amount`。
5. 退款不退手续费；转账不收手续费。
6. 只在日志和展示中转换为“元”字符串，转换函数为 `Amount.String()`。

## 4. 时间

1. 时区统一 `Asia/Shanghai`；程序 import `time/tzdata`，不依赖镜像中的时区文件。
2. 数据库 `DATETIME(3)`，DSN 设置 `loc=Asia/Shanghai`，MySQL 默认时区 `+08:00`。
3. 接口与事件中的时间为 RFC 3339 毫秒格式：`2026-10-12T10:00:00.000+08:00`。
4. 签名头中的时间戳为 Unix 秒。
5. 日期（账期、账单日期）为 `YYYY-MM-DD`，按东八区自然日划分。
6. 业务代码只通过注入的 `clock.Clock` 获取当前时间（BASE-07），`biz` 和 `job` 包禁止调用 `time.Now()`。
7. 时长配置使用 Go `time.ParseDuration` 格式（`500ms`、`10s`、`1h`）。

## 5. 错误码

### 5.1 码段

| 码段 | 范围 | 归属 |
|---|---|---|
| `0` | 成功 | — |
| `10xxx` | 通用：参数、限流、可用性、幂等 | 全部服务 |
| `20xxx` | 鉴权：签名、防重放、商户、管理 Token | gateway |
| `30xxx` | 订单：支付单、退款单 | payment |
| `40xxx` | 账户：记账、冻结 | account |
| `50xxx` | 渠道：渠道签名、渠道数据 | payment、gateway |

### 5.2 错误码表

| 码 | 名称 | HTTP | 商户可重试 | 说明 |
|---|---|---|---|---|
| 0 | `OK` | 200 | — | 成功 |
| 10001 | `INVALID_PARAM` | 400（请求体超过 64KB 时 413） | 否 | 参数缺失、格式或范围错误 |
| 10002 | `NOT_FOUND` | 404 | 否 | 资源不存在（非订单类，如对账批次、差错） |
| 10003 | `INTERNAL` | 500 | 是，参数不变 | 内部错误；对外只返回通用消息和 request_id |
| 10004 | `UNAVAILABLE` | 503 | 是，参数不变 | 依赖暂时不可用 |
| 10005 | `RATE_LIMITED` | 429 | 是，按 `Retry-After` | 请求过于频繁 |
| 10006 | `IDEMPOTENT_CONFLICT` | 409 | 否 | 幂等键相同但参数不同 |
| 20001 | `SIGN_INVALID` | 401 | 否 | 签名错误、缺少签名头、nonce 格式错误 |
| 20002 | `TIMESTAMP_EXPIRED` | 401 | 是，新时间戳重签 | 时间戳超出 ±300 秒 |
| 20003 | `NONCE_REPLAYED` | 401 | 是，新 nonce 重签 | nonce 重复 |
| 20004 | `MERCHANT_INVALID` | 401 | 否 | 商户不存在或已停用 |
| 20005 | `ADMIN_UNAUTHORIZED` | 401 | 否 | 管理 Token 无效 |
| 30001 | `ORDER_NOT_FOUND` | 404 | 否 | 支付单、退款单或转账单不存在，含访问其他商户的单据（收款方查询转账单同样返回此码） |
| 30002 | — | — | — | 保留不用（v1 需求中曾表示幂等冲突，已由 10006 替代） |
| 30003 | `ORDER_STATUS_CONFLICT` | 409 | 否 | 当前状态不允许该操作 |
| 30004 | `ORDER_EXPIRED` | 409 | 否 | 订单已过期或剩余时间不足 30 秒 |
| 30005 | `REFUND_EXCEEDED` | 409 | 否 | 累计退款金额超过订单金额 |
| 30006 | `CHANNEL_REJECTED` | 200 | 否 | 渠道明确拒绝，支付单或退款单已置为 FAILED；`data` 携带该单据 |
| 30007 | `CHANNEL_UNSUPPORTED` | 400 | 否 | 不支持的支付渠道 |
| 30008 | `PAYEE_INVALID` | 400 | 否 | 收款商户不存在、已停用或与付款商户相同 |
| 40001 | `ACCOUNT_NOT_FOUND` | 404 | 否 | 账户不存在 |
| 40002 | `INSUFFICIENT_BALANCE` | 409 | 否 | 可用余额不足；退款、转账失败时 `data` 携带该单据 |
| 40003 | `UNBALANCED_ENTRIES` | 400 | 否 | 借贷不平或分录非法，含分录跨分片（仅内部） |
| 40004 | `FREEZE_STATUS_CONFLICT` | 409 | 否 | 冻结单或转账分支的状态不允许该操作，含墓碑拦截迟到的 Try（仅内部 RPC） |
| 40005 | `ACCOUNT_INACTIVE` | 409 | 否 | 账户非 ACTIVE |
| 50001 | `CHANNEL_SIGN_INVALID` | 401 | — | 渠道签名错误（仅回调入口） |
| 50002 | `CHANNEL_AMOUNT_MISMATCH` | 400 | — | 渠道金额与本地不一致（仅内部） |

“商户可重试”中的重试都必须使用新的 nonce 和时间戳重新签名，业务参数保持不变，依靠幂等键保证不会重复下单或重复退款。

### 5.3 使用规则

1. 错误码统一定义在 `pkg/errno`，业务代码不得直接写数字。
2. Kitex 通过 `BizStatusError` 传递业务码；网关按本表映射 HTTP 状态码。
3. 未识别的错误一律转为 `10003`，对外消息固定为“internal error”，原始错误只进日志。
4. 鉴权失败不说明具体原因（例如不区分“密钥错误”和“body 被篡改”）。
5. 错误响应的 `data` 默认为空；本表“说明”中注明携带对象的，`data` 为该对象。Kitex 的 `BizStatusError` 不能同时返回响应体，因此携带对象的错误（如退款、转账的 `40002`，`30006`）在 RPC 层按正常响应返回：响应结构体带 `biz_code` 字段（`idl/base.thrift`）和该对象，网关看到非 0 的 `biz_code` 时按本表映射 HTTP 状态码并把对象放入 `data`。其他错误一律用 `BizStatusError`。
6. 新增错误码：先登记到本表（码段内递增，不复用），再在 `pkg/errno` 实现，并同步 `docs/05-api.md` 中受影响的接口。

### 5.4 失败原因码（fail_code）

`payment_order.fail_code`、`refund_order.fail_code`、`transfer_order.fail_code`、`transfer_branch.fail_code` 和事件中的 `fail_code` 使用以下字符串：

| fail_code | 适用 | 含义 |
|---|---|---|
| `CHANNEL_FAILED` | 支付、退款 | 渠道处理结果为失败 |
| `CHANNEL_REJECTED` | 支付、退款 | 渠道受理时明确拒绝，`fail_msg` 记录渠道 `reason` |
| `INSUFFICIENT_BALANCE` | 退款、转账 | 冻结或扣款时付款商户可用余额不足 |
| `FREEZE_CONFLICT` | 退款 | 冻结单已被解冻（account 返回 40004） |
| `REFUND_TIMEOUT` | 退款 | INIT 超过 1 小时仍未能提交渠道 |
| `TRY_TIMEOUT` | 转账（跨分片） | Try 超时、依赖不可用或被恢复任务判定超时，协调者决定取消 |
| `ACCOUNT_INVALID` | 转账 | 付款或收款账户不存在、非 ACTIVE（内部数据错误，伴随 ERROR 日志） |

## 6. 幂等、状态与事务

### 6.1 幂等的三层设计

| 层 | 机制 | 示例 |
|---|---|---|
| 请求层 | 幂等键（§2.4）相同、参数一致时返回首次结果；参数不一致返回 `10006` | `out_trade_no`、`out_refund_no`、`(biz_type, biz_no)` |
| 状态层 | CAS 更新，影响行数为 0 时重新读取，已是目标状态即幂等成功 | 回调重复推进 PAYING → SUCCESS |
| 存储层 | 唯一索引兜底，捕获 MySQL 1062 后按幂等处理 | `uk_merchant_out_trade_no`、`ledger_txn.uk_biz` |

### 6.2 状态更新

1. 状态字段只能通过 CAS 更新修改：`UPDATE ... SET status = ?, version = version + 1 WHERE <主键> AND status = ?`。
2. 影响行数为 0 时重新读取：已是目标状态 → 幂等成功；其他状态 → 返回该模块的状态冲突码（订单 `30003`，冻结与转账分支 `40004`）。协调者（转账单）CAS 失败时不报错，按数据库中的当前状态继续推进（TRF-03 第 3 条）。
3. 终态不可变，运营操作也不例外。
4. 状态值为大写英文，与数据库 CHECK 约束、IDL 字符串、JSON 取值完全一致。

### 6.3 事务

1. 隔离级别 READ COMMITTED；正确性依赖条件更新和唯一索引，不依赖间隙锁。余额核对使用一致性快照（ACC-10）。
2. 事务内禁止调用 RPC、HTTP、MQ；事务时长上限 3 秒。
3. 同一事务锁多行时按主键升序。
4. 死锁（1213）和锁等待超时（1205）由 `dbx.WithTx` 整体重试，最多 3 次，退避 10ms、20ms、40ms。
5. 需要“业务写入 + 发事件”时，事件通过 `outbox.Add(tx, ...)` 在同一事务写入。
6. 一个事务只访问一个账户分片；跨分片一致性只能通过协调者 + 幂等分支实现（不变量 10），禁止使用 XA。

## 7. 命名规范

### 7.1 Go 代码

- module：`github.com/<github-id>/pay-system`。
- 包名小写单词，不用下划线；目录名可用下划线（`channel_mock`），对应包名去掉下划线。
- 文件名小写下划线：`payment_order.go`、`payment_order_test.go`。
- 集成测试文件加 `//go:build integration`；端到端测试位于 `test/e2e/`，加 `//go:build e2e`。
- 测试函数注明覆盖的功能点与验收条目：`// ACC-03 验收 3：1000 goroutine 并发扣款`。

### 7.2 数据库

| 对象 | 规则 | 示例 |
|---|---|---|
| schema | `pay_<服务>`；分片为 `pay_<服务>_<分片号>` | `pay_payment`、`pay_account_1` |
| 表 | 单数、小写下划线 | `payment_order` |
| 主键 | `id BIGINT UNSIGNED AUTO_INCREMENT` | |
| 唯一索引 | `uk_<列>` | `uk_merchant_out_trade_no` |
| 普通索引 | `idx_<列>` | `idx_status_expire_at` |
| CHECK 约束 | `ck_<表简称>_<规则>` | `ck_account_available` |
| 金额列 | `BIGINT`，名称为 `amount` 或以 `_amount` 结尾 | `fee_amount` |
| 时间列 | `DATETIME(3)`，以 `_at` 结尾 | `paid_at` |
| 状态列 | `VARCHAR(16)`，大写枚举，配 CHECK 约束 | |
| 乐观锁列 | `version BIGINT UNSIGNED` | |
| 迁移文件 | `migrations/<服务>/<6 位序号>_<描述>.{up,down}.sql` | `000002_add_idx_paid_at.up.sql` |

字符集 `utf8mb4`，排序规则 `utf8mb4_bin`（标识类字段大小写敏感）。

### 7.3 接口与 IDL

- HTTP 路径：`/v1/<复数名词>`；对单据的动作作为子路径：`/v1/payments/{pay_no}/close`。
- JSON 字段 snake_case；枚举为大写字符串；金额字段以 `amount` 结尾。
- Thrift：服务名 `XxxService`，方法名 UpperCamelCase，字段 snake_case，新增字段一律 `optional`。

### 7.4 消息、缓存、指标

| 对象 | 规则 | 示例 |
|---|---|---|
| Kafka topic | `pay.<聚合>.<过去式事件>`；死信 `pay.dlq` | `pay.payment.succeeded` |
| event_type | `<聚合>.<过去式事件>` | `refund.failed` |
| 消费组 | `<服务>-<用途>` | `account-ledger` |
| 消息 key | 支付、退款事件为原支付单 `pay_no`；转账事件为 `transfer_no` | |
| Redis key | `<服务缩写>:<用途>:<标识...>`，必须设置过期时间 | `gw:nonce:{app_id}:{nonce}` |
| Prometheus 指标 | `pay_` 前缀；计数器 `_total`，时长 `_seconds`；标签低基数 | `pay_outbox_pending` |
| 故障注入点 | `<领域>.<after\|before>_<步骤>` | `refund.after_freeze` |

### 7.5 日志字段

- `msg` 用简短英文动宾短语，如 `payment status changed`。
- 字段名 snake_case，使用固定名称：`pay_no`、`refund_no`、`transfer_no`、`merchant_id`、`shard`、`biz_type`、`biz_no`、`from_status`、`to_status`、`source`（NOTIFY / QUERY / CLOSE / JOB / ADMIN）、`err_code`、`elapsed_ms`。

### 7.6 Git

- 分支：`feat/<功能点>-<简述>`、`fix/<功能点>-<简述>`、`docs/<简述>`，如 `feat/ACC-03-ledger-post`。
- 提交信息：`[<功能点>] <祈使句摘要>`，多个功能点用逗号分隔，如 `[ACC-04,ACC-05] add freeze and release`。不属于任何功能点的改动用 `[docs]`（只改文档）或 `[chore]`（工程配置），如 `[docs] add design baseline`。

## 8. 配置项

配置只来自环境变量（D12）。变量名大写下划线；渠道相关以 `CHANNEL_<渠道>_` 开头，模拟渠道自身以 `MOCK_` 开头。带 secret 标记的项启动日志输出为 `***`。

只列跨功能点的关键项，完整清单以各程序配置结构体为准（BASE-09）。

| 变量 | 程序 | 默认值 | 说明 |
|---|---|---|---|
| `WORKER_ID` | 全部服务 | 无（必填） | 0–1023，每个实例唯一 |
| `MYSQL_DSN` | payment、recon、channel-mock | 无（必填，secret） | 各服务使用自己的数据库账号；recon 用它访问 `pay_recon` 与只读的 `pay_payment` |
| `ACCOUNT_DB_DSN_0`、`ACCOUNT_DB_DSN_1` | account、recon | 无（必填，secret） | 两个账户分片的 DSN；recon 使用只读账号 |
| `ACCOUNT_SHARD_COUNT` | payment、account、recon | `2` | 固定值，不支持修改（D17） |
| `PLATFORM_SUB_ACCOUNTS` | account、seed | `8` | 平台账户子账户数，基线对比时为 `1`（OPT-03），只允许调大 |
| `LEDGER_BATCH_ENABLED`、`LEDGER_BATCH_SIZE` | account | `true`、`100` | 批量记账开关与单组上限（OPT-02） |
| `TRANSFER_PARALLEL_BRANCH` | payment | `true` | 跨分片转账分支是否并行（TRF-03、OPT-04） |
| `TRANSFER_RECOVER_DELAY`、`TRANSFER_RECOVER_INTERVAL` | payment | `10s`、`1s` | 转账创建后多久可被恢复任务接管；恢复任务间隔（TRF-04） |
| `GW_CREDENTIAL_CACHE_TTL`、`MERCHANT_CONFIG_CACHE_TTL` | gateway、payment | `60s`、`60s` | 为 0 时关闭缓存（GW-08、PAY-01、OPT-04） |
| `REDIS_ADDR` | gateway | `redis:6379` | |
| `KAFKA_BROKERS` | payment、account | `kafka:9092` | 逗号分隔 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 全部服务 | `jaeger:4317` | 为空时关闭 trace |
| `SECRET_MASTER_KEY` | payment、seed | 无（必填，secret） | 32 字节 base64 |
| `CHANNEL_MOCKPAY_KEY` | payment、gateway、channel-mock | 无（必填，secret） | 渠道签名密钥 |
| `CHANNEL_MOCKPAY_URL` | payment、recon | `http://channel-mock:9080` | |
| `CHANNEL_NOTIFY_URL` | payment | `http://gateway:8080/callback/mockpay` | 交给渠道的回调地址 |
| `ADMIN_TOKEN` | gateway | 无（必填，secret，≥ 32 字符） | 管理接口 Token |
| `NOTIFY_URL_ALLOW_HOSTS` | payment | `merchant-demo` | 商户通知地址主机白名单，逗号分隔 |
| `RATE_LIMIT_DEFAULT`、`RATE_LIMIT_OVERRIDES` | gateway | `100:200`、空 | 进程内商户限流 `速率:突发`；覆盖格式 `app_id:速率:突发,...`（GW-05） |
| `QUERY_BACKOFF` | payment | `10s,10s,30s,1m,2m,5m,10m,30m` | 查单退避，之后每小时（PAY-05） |
| `NOTIFY_RETRY_INTERVAL`、`NOTIFY_MAX_ATTEMPTS` | payment | `30s`、`5` | 商户通知固定间隔重试（NTF-02） |
| `RECON_RUN_AT` | recon | `10:00` | 每日对账时间 |
| `MOCK_DELAY_MIN_MS`、`MOCK_DELAY_MAX_MS` | channel-mock | `200`、`2000` | 自动完成延迟范围 |
| `MOCK_ALLOW_PARTIAL_BILL` | channel-mock | `false` | 允许下载当天账单（测试与演练） |
| `FAULT_POINTS` | 全部服务 | 空 | 故障注入点（QA-01） |
| `FAIL_FIRST_N` | merchant-demo | `0` | 每个单号前 N 次通知返回失败（TL-02） |

新增配置项：先登记到本表（或在对应功能点中说明并归入此表），再加入配置结构体和 `deploy/.env.example`。

## 9. 变更记录

| 版本 | 日期 | 变更 |
|---|---|---|
| v1.0 | 2026-10-12 | 从 `docs/02-requirements.md` v2.0 §2.6、§6.7 拆出；新增编号体系、码段划分、可重试说明、`fail_code` 登记、命名规范 |
| v1.1 | 2026-10-12 | §1.1 登记流程图编号 `SM`、`SL`、`CF` 与问题编号 `R`；`10001` 增加 413 映射（请求体超限）；`30006` 扩展到退款（R6、R14） |
| v2.0 | 2026-10-12 | 随需求 v3.0 聚焦三条链路：新增模块前缀 `TRF`、`OPT`，编号 `S<n>`，验收扩展到 A12；新增单号前缀 `TF`、标识 `out_transfer_no` / `shard` / `sub_no`；新增错误码 `30008`，扩展 `30001`、`40002`、`40003`、`40004` 的说明；新增 fail_code `TRY_TIMEOUT`、`ACCOUNT_INVALID`；事务规则增加“一个事务只访问一个分片、禁止 XA”；幂等键增加转账单与转账分支，删除差错挂账凭证；配置增加分片 DSN、子账户数、批量记账、转账、缓存开关，通知改为固定间隔，删除全局限流 `RATE_LIMIT_GLOBAL` 与 `NOTIFY_BACKOFF`；§5.3 第 5 条明确携带对象的错误在 RPC 层用响应字段 `biz_code` 传递 |
| v2.0 | 2026-10-13 | §7.6 补充不属于功能点的提交前缀 `[docs]`、`[chore]` |
