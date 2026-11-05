# pay-system 需求规格说明书

> 版本：v3.0　最后修改：2026-11-05
> 状态：v3.0 重新聚焦支付、退款、转账三条资金链路（变更见 §8.2），2026-10-13 已确认为开发基线；开发按 §3 功能点总表逐项推进
> 上游：`PROJECT_GOALS.md`（目标与约束）、`docs/01-tech-decisions.md`（技术选型与版本）
> 关联：`docs/03-conventions.md`（编号、错误码、命名等全局约定）、`docs/05-api.md`（接口文档）、`docs/database.md`（表结构）、`docs/04-dev-plan.md`（周计划）、`docs/06-flows.md`（流程图）

## 0. 文档说明

### 0.1 定位

本文是功能与非功能需求、以及实现所需业务规则的基线。开发一个功能点所需的全部信息来自四份文档：

| 文档 | 内容 | 冲突时以谁为准 |
|---|---|---|
| 本文 | 功能点规则、验收、非功能需求 | 业务规则 |
| `docs/03-conventions.md` | 单号、金额、时间、错误码、幂等与事务规则、命名、配置项 | 全局约定 |
| `docs/05-api.md` | 接口字段、签名、错误码使用、事件格式 | 接口契约 |
| `docs/database.md` | 表、字段、索引、约束 | 表结构 |

`docs/06-flows.md` 是本文的图形视图（状态机、泳道图、控制流图），与本文冲突时以本文为准。

具体编码流程（读文档、写测试、实现、验证、收尾）见 `.opencode/skills/pay-feature-dev/SKILL.md`，用 `/feature <功能点编号>` 启动。

### 0.2 功能点推进规则

1. 开发单位是功能点（§3 中的一行）。一次只推进一个功能点，按里程碑顺序领取，依赖未完成的功能点不能开始。
2. 每个功能点按顺序完成：确认规则 → 写测试（覆盖“验收”中的每一条）→ 实现 → 验证 → 更新 §3 状态。
3. 分支与提交信息命名见 `docs/03-conventions.md` §7.6。
4. 实现中发现规则缺失或矛盾，先改对应文档（本文在 §8.2 记录，另两份文档在各自的变更记录中登记），再改代码。
5. 状态取值：`TODO`、`DOING`、`DONE`、`CUT`（经确认砍掉）。
6. 功能点代码提交后，在 `docs/reports/` 出具一份自包含的功能点报告（技术讲解 + 已完成内容），格式见 `.opencode/skills/pay-feature-dev/report-template.md`。

### 0.3 功能点完成定义（DoD）

一个功能点标记为 `DONE` 前必须全部满足：

- [ ] “验收”中的每一条都有对应的自动化测试，且全部通过。两类例外：报告类验收（压测、演练、pprof、验收运行等，验收写明“报告给出……”或指向 `docs/` 下某个报告文件），以该报告文件及其中记录的真实运行（命令、开关、时间、原始输出或其摘录）为证据；人工验收（如 OpenAPI 校验、按 README 跑通演示），以功能点报告“验证记录”中的命令与输出为证据。两类都不能用未运行的结果代替。
- [ ] `make lint test` 通过；涉及数据库或 MQ 的功能点 `make test-integration` 通过；并发相关测试带 `-race`。
- [ ] 涉及资金变动的功能点，测试结束时调用 `CheckBalance`（ACC-10）全量核对通过；ACC-10 完成之前，用测试内的 SQL 断言代替。
- [ ] 表结构有变化时，迁移文件已在 MySQL 8.4.11 上执行，`docs/database.md` 已同步。
- [ ] 新增的错误码、单号前缀、配置项已登记到 `docs/03-conventions.md`，指标已登记到 OPS-03。
- [ ] 接口有变化时，`docs/05-api.md` 已更新版本号、修改时间和变更记录。
- [ ] 日志中没有敏感字段明文。

### 0.4 编写约定

- 优先级：P0 必须完成（支撑验收 A1–A12）；P1 应该完成；P2 加分项，P0、P1 全部完成后才开始。
- 里程碑：M1–M8 对应第 1–8 周。
- “表”字段只列主要读写的表，字段定义见 `docs/database.md`。
- 金额一律以“分”为单位的 `int64` 表述，示例中的 `10000` 即 100.00 元。

## 1. 项目概述

### 1.1 目标

构建一个可本地一键运行的小型支付系统，重点打磨三条资金链路：

| 链路 | 数据交互 | 一致性手段 | 性能关注点 |
|---|---|---|---|
| 支付 | 网关 → payment → 渠道；回调 / 查单 → outbox → Kafka → account 入账 | 订单 CAS、outbox、消费幂等、查单补偿、对账 | 下单与回调延迟、入账吞吐（批量记账）、平台热点账户（子账户） |
| 退款 | payment → account 同步冻结 → 渠道；结果经 outbox 异步确认扣款或解冻 | 简化 TCC、空回滚与防悬挂、卡单恢复 | 冻结 RPC 往返、退款接口延迟 |
| 转账 | payment 作为协调者；同分片本地事务，跨分片 TCC 两个 account 分片 | 协调者状态落盘、分支幂等、空回滚与防悬挂、恢复任务、在途账户守恒 | 两个分支并行、事务往返 |

其他模块（通知、网关安全、对账、运维）只做支撑三条链路所需的简单实现。核心不是功能数量，而是用测试、故障演练和压测证明资金正确性、一致性，并用前后对比数据证明性能优化有效。

### 1.2 范围

| 包含 | 不包含 |
|---|---|
| 商户账户与平台内部账户、复式记账、冻结；账户数据 2 个分片 | 真实银行与三方渠道对接 |
| 支付单、退款单（含部分退款）、转账单状态机 | 多币种、汇率（只支持 CNY） |
| 商户间转账：同分片本地事务、跨分片 TCC | KYC、实名、营销 |
| 一个模拟渠道：异步回调、重复回调、丢回调、超时、账单 | 分账、商户结算与提现、多渠道路由 |
| 本地消息表 + Kafka 最终一致性；批量记账 | 退款退手续费、撤销接口、转账手续费 |
| 商户异步通知（固定间隔重试） | 分片在线扩容、跨日对账、人工差错处理与挂账 |
| 对账：外部按单号比对、内部单据与凭证核对、两类差错自动处理 | 商户自助入驻（通过种子数据创建） |
| 网关签名、时间窗口、nonce、商户限流 | 复杂前端、监控面板与告警规则 |
| 指标、链路追踪、pprof、故障演练、压测与前后对比 | K8s 生产部署（P2） |

### 1.3 角色

| 角色 | 说明 | 交互方式 |
|---|---|---|
| 商户 | 下单、发起支付、关单、退款、转账、查询、接收通知 | 网关 HTTP API（HMAC 签名）；`paycli`；`merchant-demo` |
| 付款用户 | 不建模，由模拟渠道自动“完成付款” | 无 |
| 渠道 | channel_mock，模拟外部三方支付机构 | HTTP 接口、异步回调、账单下载 |
| 运营 | 重放事件、核对账户、手动触发对账 | 网关管理端口 `/admin/*` |

### 1.4 术语

| 术语 | 含义 |
|---|---|
| 凭证（ledger_txn） | 一次记账的头记录，包含 ≥ 2 条分录，借方合计 = 贷方合计 |
| 分录（ledger_entry） | 对单个账户的一次借记或贷记，只追加 |
| 余额方向 | 账户余额按“贷 − 借”（CREDIT）还是“借 − 贷”（DEBIT）计算 |
| 冻结 | 占用可用余额但不改变余额，之后确认扣款或解冻 |
| 空回滚 | 冻结从未成功，却收到解冻请求 |
| 悬挂 | 解冻（空回滚）之后，迟到的冻结请求又冻结成功，资金被永久占用 |
| outbox | 本地消息表，与业务数据同事务写入，再异步投递到 MQ |
| 查单 | 平台主动向渠道查询交易状态，用于弥补丢失的回调 |
| 账期 | 对账归属的自然日，按东八区计算 |
| 长款 | 渠道有成功记录，本地没有（平台多收了钱） |
| 短款 | 本地有成功记录，渠道没有（平台少收了钱） |
| CAS 更新 | 带旧状态条件的更新：`UPDATE ... SET status = 新 WHERE ... AND status = 旧` |
| 分片 | 账户数据所在的 schema（`pay_account_0` / `pay_account_1`），按商户号路由（ACC-13） |
| 子账户 | 同一平台账户拆出的多行账户记录，以 `sub_no` 区分，余额按子账户汇总（OPT-03） |
| 协调者 | 跨分片转账中持久化全局状态、驱动各分支 Try / Confirm / Cancel 的一方，即 payment |
| 分支 | 转账在某个分片上的一段操作：付款方分支是冻结单，收款方分支是 `transfer_branch` |
| 转账在途 | 平台账户 `TRANSIT`：跨分片转账在付款方分片贷记、在收款方分片借记，全部转账终结后各分片合计为 0 |
| 决策落盘 | 协调者先把 CONFIRMING / CANCELLING 写入数据库，再向分支发起 Confirm / Cancel；落盘后结论不再改变 |

## 2. 总体设计

### 2.1 架构

```
 商户 / paycli / merchant-demo
        │  HTTP + HMAC 签名
        ▼
 ┌──────────────────┐        Redis（nonce）
 │ gateway (Hertz)  │◄───────────────────────────────────────┐
 │ :8080 / :8090    │                                        │ 渠道回调
 └───┬──────────┬───┘                                        │ POST /callback/mockpay
     │ Kitex    │ Kitex（管理接口）                           │
     ▼          ▼                                            │
 ┌─────────┐  ┌─────────┐  下载账单      ┌──────────────────────┐
 │ payment │  │  recon  │──────────────►│ channel_mock (Hertz) │
 │  :8881  │  │  :8883  │               │        :9080         │
 └─┬──┬──┬─┘  └────┬────┘               └──────────────────────┘
   │  │  │         │ 只读 pay_payment / pay_account_0/1  ▲
   │  │  └─────────┼─────────── HTTP：受理/查单/关单/退款 ┘
   │  │ Kitex：Freeze / ReleaseFreeze / ConfirmFreeze / TransferLocal / TransferIn* / GetAccount
   │  ▼
   │ ┌─────────┐
   │ │ account │◄──── Kafka：pay.payment.succeeded / pay.refund.succeeded / pay.refund.failed
   │ │  :8882  │                         ▲                │
   │ └──┬───┬──┘                         │                └──► payment-notify（商户通知，含 pay.transfer.*）
   │    │   │  按商户号路由              │
   │    ▼   ▼                            │
   │ pay_account_0  pay_account_1        │
   └── outbox 投递器 ─────────────────────┘

 MySQL 8.4：pay_account_0 / pay_account_1 / pay_payment / pay_recon / pay_channel_mock
```

- 同步：gateway → payment / account / recon（Kitex）；payment → account（冻结类操作、转账分支、账户查询）；payment → channel_mock（HTTP）。
- 异步：payment outbox → Kafka → account（支付入账、退款确认或解冻）、payment-notify（商户通知）。转账不经过 Kafka 记账，两个分支都由协调者同步驱动。
- 分片：account 进程持有两个分片的连接池，每个请求只在一个分片上开启事务；跨分片的一致性由 payment 协调（TRF-03）。
- 对账：recon 从 channel_mock 下载账单，只读访问 `pay_payment` 和两个账户分片。

三条链路的主流程（细节见各功能点和 `docs/06-flows.md`）：

| 链路 | 同步部分 | 异步部分 |
|---|---|---|
| 支付 | 下单 → 发起支付（调渠道）→ 回调或查单把订单推进为 SUCCESS，同事务写 outbox | 投递 Kafka → account 批量入账 → 商户通知 |
| 退款 | 创建退款（占用额度）→ 冻结商户余额 → 调渠道退款 → 回调或查单推进结果，同事务写 outbox | 投递 Kafka → account 确认扣款或解冻 → 商户通知 |
| 转账 | 创建转账单 → 同分片：一次本地事务；跨分片：并行 Try → 决策落盘 → 并行 Confirm / Cancel → 终态同事务写 outbox | 恢复任务推进卡住的转账；投递 Kafka → 商户通知 |

### 2.2 进程与端口

| 进程 | 职责 | 业务端口 | 管理端口（metrics、pprof、healthz） |
|---|---|---|---|
| gateway | 对外 API、回调入口、管理接口 | 8080（商户 API、回调）、8090（admin） | 6060 |
| payment | 支付单、退款单、转账单（TCC 协调者）、渠道交互、outbox 投递、补偿任务、商户通知 | 8881（Kitex） | 6061 |
| account | 账户分片路由、记账、冻结、转账分支、事件消费 | 8882（Kitex） | 6062 |
| recon | 对账任务与差错处理 | 8883（Kitex） | 6063 |
| channel-mock | 模拟渠道 | 9080 | 6064 |
| merchant-demo | 模拟商户后台 | 9100 | — |

基础设施：MySQL 3306、Redis 6379、Kafka 9092（宿主机用 9094）、Prometheus 9090、Jaeger UI 16686 / OTLP 4317。除 8080、9080、9100 外，所有宿主机端口只绑定 `127.0.0.1`。

### 2.3 仓库结构与分层

```
pay-system/
  go.mod  Makefile  README.md  PROJECT_GOALS.md  .golangci.yml
  idl/            base.thrift account.thrift payment.thrift recon.thrift
  kitex_gen/      生成代码，入库
  cmd/            gateway payment account recon channel-mock merchant-demo paycli
                  bootstrap seed（初始化工具，不进 make build）
  internal/       bootstrap/ dbmigrate/ dbschema/ seed/ testkit/（集成测试基座，BASE-11）
  gateway/        handler/ middleware/
  service/
    account/      handler/ biz/ dal/ consumer/
    payment/      handler/ biz/ dal/ channel/ job/ notify/
    channel_mock/ handler/ biz/ scenario/ bill/ dal/
  job/recon/      biz/ dal/ bill/ handler/
  pkg/            money idgen errno clock dbx config logx sign secretbox outbox mq fault obs shard
                  accountclient（payment 侧 account 客户端，D22，ACC-11） server（健康检查与优雅退出，OPS-01）
  migrations/     account/（两个分片共用） payment/ recon/ channel_mock/
  deploy/         docker-compose.yml docker-compose.chaos.yml Dockerfile .env.example seed/ prometheus/
  test/           integration/ e2e/ chaos/ load/
  scripts/  docs/
```

分层约束：

- `handler` 只做协议转换和参数校验；`biz` 承载业务规则、状态机和事务编排；`dal` 只写 SQL。例外：account 的 SQL 写在 `biz/`，`dal/` 只放分片连接池与就绪检查（§8.1 Q5 已定，不拆）。现状偏离：payment 目前没有 `dal/`，SQL 写在 `biz/`，待 fix/PAY-01-dal 拆出 dal 层。
- `biz` 依赖 `dal` 的接口，不依赖具体实现，便于单测替换。
- 服务之间只能通过 `kitex_gen` 和事件结构交互，禁止 import 其他服务的 `service/<x>/...` 包（golangci-lint `depguard` 检查）。
- `make build` 只构建树中第一行列出的 7 个业务程序。`cmd/bootstrap`、`cmd/seed` 是运维/初始化工具，不进入这 7 个二进制，只经 `deploy/Dockerfile` 构建。`internal/` 只给 bootstrap、seed、迁移和测试使用，不被 `service/`、`gateway/`、`job/` 引用；`pkg/` 同样不得引用。`depguard` 拦截这些 import。
- 事务中禁止调用 RPC、HTTP、MQ。

核心服务的包内划分（2026-10-13 确认，来源 `docs/07-module-plan.md` §4；2026-10-27 按已实现代码修正 account 与 payment 的 biz）：

```
service/account/
  handler/                Kitex 实现，错误码转换
  biz/                    平铺一个包，不分子包，按方法分文件：
                          open.go、get.go：开户、查询（平台账户跨分片汇总）
                          post.go：Post，校验、加锁顺序、balance_after（进程内方法，不暴露 RPC）
                          freeze.go、release.go、confirm.go：Freeze / ReleaseFreeze / ConfirmFreeze，空回滚与防悬挂
                          transfer.go、transfer_in.go：TransferLocal（失败墓碑）、TransferIn Try / Confirm / Cancel
                          check.go：CheckBalance，分片快照、在途守恒
  dal/                    ShardDB(shard)，每个分片一个连接池；就绪检查（SQL 在 biz/，§8.1 Q5）
  consumer/               ledger_single.go（MQ-04 基线）、ledger_batch.go（OPT-02）

service/payment/
  handler/
  biz/                    平铺一个包，不分子包，按领域分文件：
                          merchant.go、fee_rate.go、fee_check.go：商户配置与凭证（带缓存开关）、费率与手续费核对
                          支付单状态机与 ApplyPayResult、退款单状态机与 ApplyRefundResult、
                          转账单（LOCAL 执行、TCC 协调器）在同一个包内按领域分文件
  dal/                    现状偏离：目前没有，SQL 写在 biz/，待 fix/PAY-01-dal 拆出 dal 层
  channel/                渠道 HTTP 客户端
  job/                    query_compensate、order_expire、refund_recover、transfer_recover、outbox_relay
  notify/                 通知消费与发送
```

转账协调者放在 payment，不单独建 transfer 服务：复用 outbox、补偿框架和通知，代价是 payment 职责变重。

### 2.4 技术栈

Go 1.27、Hertz v0.10.6、Kitex v0.16.3（Thrift）、MySQL 8.4.11、Redis 8.10.2、Kafka 4.3.1（franz-go）、sqlx、OpenTelemetry + Jaeger、Prometheus、docker-compose。完整版本表和选型理由见 `docs/01-tech-decisions.md`。

### 2.5 资金模型

#### 2.5.1 账户类型

| account_type | owner_type / owner_id | 余额方向 | 允许透支 | 每个分片 | 含义 |
|---|---|---|---|---|---|
| `MERCHANT_SETTLE` | MERCHANT / 商户号 | CREDIT | 否 | 只在商户所属分片，`sub_no = 0` | 平台欠商户的钱（负债） |
| `CHANNEL_RECEIVABLE` | PLATFORM / 渠道编码（`mockpay`） | DEBIT | 是 | 每个分片 N 个子账户 | 渠道已代收、尚未结算给平台的钱（资产） |
| `FEE_INCOME` | PLATFORM / `PLATFORM` | CREDIT | 是 | 每个分片 N 个子账户 | 手续费收入 |
| `TRANSIT` | PLATFORM / `PLATFORM` | CREDIT | 是 | 每个分片 N 个子账户 | 跨分片转账在途；付款方分片贷记、收款方分片借记 |

余额方向和是否允许透支由账户类型决定，开户时不可指定。N 为 `PLATFORM_SUB_ACCOUNTS`（默认 8，基线对比时为 1），子账户按 `crc32(biz_no) % N` 选择（OPT-03）。平台账户的对外余额是两个分片全部子账户之和。

分片规则（ACC-13）：`shard = 商户号数字部分 % 2`。种子商户 `M100001`、`M100003` 在分片 1，`M100002` 在分片 0，用于构造同分片和跨分片转账。一张凭证的全部分录只落在一个分片。

余额变动规则：CREDIT 方向账户，贷记加、借记减；DEBIT 方向账户，借记加、贷记减。只有“减少余额”的分录需要检查可用余额（`allow_overdraft = 0` 时）。

#### 2.5.2 分录规则

| 业务 | biz_type / biz_no | 所在分片 | 借方 | 贷方 |
|---|---|---|---|---|
| 支付成功（金额 A，手续费 F） | `PAY` / pay_no | 商户所属分片 | 渠道应收 A | 商户 A − F；手续费收入 F（F = 0 时省略该分录） |
| 退款确认（金额 R） | `REFUND` / refund_no | 商户所属分片 | 商户 R（同时减少冻结 R） | 渠道应收 R |
| 同分片转账（金额 T） | `TRANSFER` / transfer_no | 双方共同分片 | 付款商户 T | 收款商户 T |
| 跨分片转账，付款方确认 | `TRANSFER_OUT` / transfer_no | 付款方分片 | 付款商户 T（同时减少冻结 T） | 转账在途 T |
| 跨分片转账，收款方确认 | `TRANSFER_IN` / transfer_no | 收款方分片 | 转账在途 T | 收款商户 T |
| 冲正（P1） | `REVERSAL` / 原 txn_no | 原凭证分片 | 原贷方账户 | 原借方账户 |

示例：订单 10000 分，费率 60bp。支付成功后渠道应收 10000、商户 9940、手续费收入 60。部分退款 3000 后，商户 6940、渠道应收 7000。退款不退手续费。

跨分片转账示例：`M100001`（分片 1）向 `M100002`（分片 0）转 500。付款方确认后分片 1 在途 +500；收款方确认后分片 0 在途 −500，两个分片在途合计回到 0。转账不收手续费。

#### 2.5.3 资金不变量

以下规则任何实现都不得违反，对应 NFR-COR（§5.1）的验证方式：

1. 金额为 `int64` 分，禁止浮点数。
2. 分录只追加，不更新、不删除；更正通过冲正凭证完成。
3. 每个账户：CREDIT 方向余额 = 贷方合计 − 借方合计；DEBIT 方向余额 = 借方合计 − 贷方合计。最后一条分录的 `balance_after` 等于当前余额。
4. 每张凭证借方合计 = 贷方合计；全部分录借方总额 = 贷方总额（试算平衡）。
5. `allow_overdraft = 0` 的账户，`balance − frozen_amount ≥ 0`；所有账户 `frozen_amount ≥ 0`，且等于该账户 FROZEN 冻结单金额合计。
6. 所有写接口幂等，最终由数据库唯一索引兜底。
7. 状态只能按定义的状态机流转，更新必须是 CAS 更新。
8. 外部回调必须验签；与本地终态冲突时以查单和对账为准。
9. 日志、错误信息、trace 属性中不得出现敏感字段明文。
10. 一个数据库事务只访问一个账户分片；跨分片的资金移动必须经过转账在途账户，并由协调者驱动。
11. 在途守恒：两个分片 `TRANSIT` 余额合计 = 已确认的付款方转账冻结金额合计 − 已确认的收款方分支金额合计；没有进行中的跨分片转账时为 0。
12. 每个 SUCCESS 转账单：同分片恰好 1 张 `TRANSFER` 凭证；跨分片恰好 1 张 `TRANSFER_OUT` 和 1 张 `TRANSFER_IN` 凭证，金额都等于转账金额。每个 FAILED 转账单：没有任何转账凭证，冻结单为 RELEASED 或不存在，收款方分支为 CANCELLED 或不存在。

### 2.6 全局约定

编号体系、业务单号前缀、金额与时间格式、错误码、`fail_code`、幂等/状态/事务规则、命名规范、配置项统一登记在 `docs/03-conventions.md`，本文各功能点直接引用，不再重复。常用章节：

| 内容 | 位置 |
|---|---|
| 单号格式与前缀登记 | `docs/03-conventions.md` §2 |
| 金额与手续费 | `docs/03-conventions.md` §3 |
| 时间 | `docs/03-conventions.md` §4 |
| 错误码、`fail_code` | `docs/03-conventions.md` §5 |
| 幂等三层设计、CAS 状态更新、事务规则 | `docs/03-conventions.md` §6 |
| 命名规范（代码、表、topic、指标、Git） | `docs/03-conventions.md` §7 |
| 配置项 | `docs/03-conventions.md` §8 |

### 2.7 状态机汇总

| 对象 | 流转 | 详见 |
|---|---|---|
| `payment_order` | INIT → PAYING → SUCCESS / FAILED / CLOSED；INIT → CLOSED | PAY-04 |
| `refund_order` | INIT → PROCESSING → SUCCESS / FAILED；INIT → FAILED | §4.5 |
| `channel_txn` | PROCESSING → SUCCESS / FAILED / CLOSED，与订单在同一事务内推进 | PAY-04、REF-03 |
| `transfer_order` | 同分片：TRYING → SUCCESS / FAILED；跨分片：TRYING → CONFIRMING → SUCCESS，TRYING → CANCELLING → FAILED | TRF-02、TRF-03 |
| `account_freeze` | FROZEN → CONFIRMED / RELEASED；空回滚时直接写入 RELEASED | ACC-04~06 |
| `transfer_branch` | LOCAL：直接写入 SUCCESS / FAILED；PAYEE：TRIED → CONFIRMED / CANCELLED，空回滚时直接写入 CANCELLED | ACC-07、ACC-14 |
| `outbox` | PENDING → SENT / DEAD；SENT、DEAD → PENDING（仅重放） | MQ-02、MQ-05 |
| `merchant_notify` | PENDING → SUCCESS / DEAD | NTF-02 |
| `recon_batch` | INIT → DOWNLOADED → CHECKING → DONE；任一步 → FAILED；FAILED、DONE（force）可重跑 | REC-01 |
| `recon_diff` | PENDING → RESOLVED / IGNORED（只由自动处理推进） | REC-06 |

### 2.8 后台任务汇总

| 任务 | 进程 | 默认间隔 | 互斥方式 | 功能点 |
|---|---|---|---|---|
| outbox 投递 | payment | 200ms | `FOR UPDATE SKIP LOCKED` 领取并把 `next_retry_at` 推后为 30 秒租约后提交；事务外发送，按租约值 CAS 回写 | MQ-02 |
| 查单补偿 | payment | 1s | `SKIP LOCKED` 抢占 + 推后 `next_query_at` | PAY-05 |
| 超时关单 | payment | 5s | 状态 CAS | PAY-07 |
| 退款卡单恢复 | payment | 10s | 状态 CAS + 幂等 Freeze | REF-04 |
| 转账恢复 | payment | 1s | `SKIP LOCKED` 抢占 + 推后 `next_retry_at`；分支操作幂等 | TRF-04 |
| 商户通知发送 | payment | 1s | `SKIP LOCKED` 抢占 | NTF-02 |
| 订单手续费核对 | payment | 1 分钟 | 只读；多实例可同时跑 | PAY-10 |
| outbox 清理 | payment | 每天 03:00 | 按批删除，天然幂等 | MQ-06 |
| 余额核对 | account | 手动全量（管理接口、测试、`test/chaos/verify.sh`） | 只读 | ACC-10 |
| 对账 | recon | 每天 10:00，同时接管前 7 个账期中超时未终态的批次；`BILL_NOT_READY` 的批次 30 分钟后对该批次再触发一次，最多重试 6 次（`retry_count` 持久化）；不做后台扫描，超时（INIT / DOWNLOADED 按 `updated_at`、CHECKING 按 `started_at`，满 30 分钟）只在 RunBatch 被调用时判定并接管 | `uk_channel_bill_date` + 状态 CAS | REC-01 |
| 渠道自动完成与回调 | channel-mock | 进程内定时 | 单实例 | CH-01、CH-05 |

所有任务使用注入的 `clock.Clock` 和 `context`，单批 panic 被恢复并计数，进程退出时等待当前批次完成。订单手续费核对（PAY-10）本次只提供可调用的扫描函数，不在 `cmd/payment/main.go` 里启动；payment 进程常驻后再按上表间隔调用。

## 3. 功能点总表

依赖列只写直接依赖。状态在开发过程中更新。

| 编号 | 功能点 | 优先级 | 里程碑 | 依赖 | 状态 |
|---|---|---|---|---|---|
| BASE-01 | 工程骨架与构建 | P0 | M1 | — | DONE |
| BASE-02 | 本地环境编排 | P0 | M1 | BASE-01 | DONE |
| BASE-03 | 数据库迁移与种子数据 | P0 | M1 | BASE-02 | DONE |
| BASE-04 | 金额工具 | P0 | M1 | BASE-01 | DONE |
| BASE-05 | ID 生成 | P0 | M1 | BASE-01 | DONE |
| BASE-06 | 错误码 | P0 | M1 | BASE-01 | DONE |
| BASE-07 | 时钟 | P0 | M1 | BASE-01 | DONE |
| BASE-08 | 数据库访问与事务 | P0 | M1 | BASE-06 | DONE |
| BASE-09 | 配置 | P0 | M1 | BASE-01 | DONE |
| BASE-10 | 日志与脱敏 | P0 | M1 | BASE-01 | DONE |
| BASE-11 | 集成测试基座 | P0 | M1 | BASE-03、BASE-08 | DONE |
| BASE-12 | IDL 与代码生成 | P0 | M1 | BASE-01 | DONE |
| BASE-13 | 签名库 | P0 | M3 | BASE-01 | DONE |
| BASE-14 | 密钥加密 | P0 | M3 | BASE-01 | DONE |
| BASE-15 | 故障注入点 | P0 | M3 | BASE-01、BASE-09 | DONE |
| ACC-13 | 账户分片路由 | P0 | M1 | BASE-08、BASE-09 | DONE |
| ACC-01 | 开户 | P0 | M1 | ACC-13、BASE-05 | DONE |
| ACC-02 | 账户查询 | P0 | M1 | ACC-01 | DONE |
| ACC-03 | 通用记账 | P0 | M1 | ACC-01、BASE-04 | DONE |
| ACC-04 | 冻结 | P0 | M2 | ACC-03 | DONE |
| ACC-05 | 解冻（含空回滚、防悬挂） | P0 | M2 | ACC-04 | DONE |
| ACC-06 | 确认扣款 | P0 | M2 | ACC-04 | DONE |
| ACC-07 | 同分片转账（含失败墓碑） | P0 | M2 | ACC-03 | DONE |
| ACC-14 | 收款方转账分支（Try / Confirm / Cancel） | P0 | M2 | ACC-03 | DONE |
| ACC-08 | 冲正 | P1 | M8 | ACC-03 | TODO |
| ACC-09 | 流水查询 | P1 | — | — | CUT |
| ACC-10 | 余额核对（含分片与在途守恒） | P0 | M2 | ACC-03、ACC-04、ACC-14 | DONE |
| ACC-11 | account RPC 服务 | P0 | M2 | ACC-01~07、ACC-10、ACC-14、BASE-12 | DONE |
| ACC-12 | 热点账户优化 | P2 | — | — | CUT（拆为 OPT-03、OPT-05） |
| CH-01 | 渠道支付受理与自动完成 | P0 | M3 | BASE-13、BASE-03 | DONE |
| CH-02 | 渠道查单 | P0 | M3 | CH-01 | DONE |
| CH-03 | 渠道关单 | P0 | M3 | CH-01 | DONE |
| CH-04 | 渠道退款受理与查询 | P0 | M4 | CH-01 | DONE |
| CH-05 | 渠道异步回调 | P0 | M3 | CH-01 | DONE |
| CH-06 | 确定性故障场景 | P0 | M3–M4 | CH-01、CH-02、CH-03、CH-05 | DONE |
| CH-07 | 全局故障配置（压测延迟） | P0 | M6 | CH-06、PAY-05 | TODO |
| CH-08 | 账单下载 | P0 | M6 | CH-01、CH-04 | TODO |
| CH-09 | 账单注入 | P0 | M6 | CH-08 | TODO |
| PAY-01 | 商户与密钥 | P0 | M3 | BASE-14、BASE-03 | DONE |
| PAY-02 | 创建支付单 | P0 | M3 | PAY-01 | DONE |
| PAY-03 | 发起支付 | P0 | M3 | PAY-02、CH-01、BASE-15 | DONE |
| PAY-04 | 支付结果推进 | P0 | M3 | PAY-03、MQ-01 | DONE |
| PAY-05 | 查单补偿 | P0 | M4 | PAY-04、CH-02、CH-06、GW-06、REF-03、BASE-15 | TODO |
| PAY-06 | 商户关单 | P0 | M3 | PAY-04、CH-03 | DONE |
| PAY-07 | 超时关单 | P0 | M4 | PAY-06 | TODO |
| PAY-08 | 支付单查询 | P0 | M3 | PAY-02 | DONE |
| PAY-09 | 立即同步渠道状态 | P0 | M6 | PAY-05 | TODO |
| PAY-10 | 商户费率变更与订单手续费核对 | P0 | M3 | BASE-03、BASE-04、BASE-08 | DONE |
| REF-01 | 创建退款与额度占用 | P0 | M4 | PAY-04 | DONE |
| REF-02 | 退款冻结与渠道提交 | P0 | M4 | REF-01、ACC-11、CH-04、BASE-15 | DONE |
| REF-03 | 退款结果推进 | P0 | M4 | REF-02、GW-06 | DONE |
| REF-04 | 退款卡单恢复 | P0 | M4 | REF-02、REF-03、BASE-15 | TODO |
| REF-05 | 退款查询 | P0 | M4 | REF-01 | TODO |
| TRF-01 | 创建转账单 | P0 | M4 | PAY-01、ACC-13 | TODO |
| TRF-02 | 同分片转账执行 | P0 | M4 | TRF-01、ACC-07、ACC-11、MQ-01 | TODO |
| TRF-03 | 跨分片转账 TCC 协调 | P0 | M4 | TRF-01、ACC-04~06、ACC-14、ACC-11、MQ-01、BASE-15 | TODO |
| TRF-04 | 转账恢复任务 | P0 | M4 | TRF-02、TRF-03、BASE-15 | TODO |
| TRF-05 | 转账查询 | P0 | M4 | TRF-01 | TODO |
| MQ-01 | outbox 写入 | P0 | M3 | BASE-08、BASE-05 | DONE |
| MQ-02 | outbox 投递器 | P0 | M5 | MQ-01、MQ-03、BASE-15 | TODO |
| MQ-03 | Kafka 生产与消费框架 | P0 | M5 | BASE-02、OPS-01 | TODO |
| MQ-04 | account 事件消费（逐条模式） | P0 | M5 | MQ-02、MQ-03、ACC-03、ACC-05、ACC-06、PAY-06、REF-03、BASE-15 | TODO |
| MQ-05 | 死信与事件重放 | P0 | M5 | MQ-02、MQ-04 | TODO |
| MQ-06 | outbox 清理 | P1 | M8 | MQ-02 | TODO |
| NTF-01 | 通知记录生成 | P0 | M5 | MQ-03 | TODO |
| NTF-02 | 通知发送与固定间隔重试 | P0 | M5 | NTF-01、BASE-13、TL-02 | TODO |
| GW-01 | 网关骨架与统一响应 | P0 | M3 | BASE-12、BASE-13、TL-01 | DONE |
| GW-02 | 商户 API 路由（支付与余额） | P0 | M3 | GW-01、GW-03、PAY-02/03/06/08、ACC-02、ACC-11 | DONE |
| GW-03 | 商户验签 | P0 | M3 | GW-01、PAY-01 | DONE |
| GW-04 | 时间窗口与 nonce 防重放 | P0 | M5 | GW-03、GW-06、OPS-01 | TODO |
| GW-05 | 商户限流（进程内） | P0 | M5 | GW-03 | TODO |
| GW-06 | 渠道回调入口 | P0 | M3 | GW-01、PAY-04、CH-05、BASE-15 | DONE |
| GW-07 | 管理接口（重放、核对、触发对账） | P0 | M6 | GW-01、MQ-05、ACC-10、ACC-11、REC-01 | TODO |
| GW-08 | 商户密钥缓存 | P0 | M7 | GW-03、OPT-01、QA-02 | TODO |
| GW-09 | 商户 API 路由（退款与转账） | P0 | M4 | GW-01、GW-02、GW-06、TL-01、REF-01、REF-05、TRF-01、TRF-05 | TODO |
| REC-01 | 对账批次与调度 | P0 | M6 | BASE-03 | TODO |
| REC-02 | 账单下载与导入 | P0 | M6 | REC-01、CH-08 | TODO |
| REC-03 | 外部对账比对（按单号） | P0 | M6 | REC-02、CH-06、CH-09、PAY-05 | TODO |
| REC-04 | 跨日处理 | P0 | — | — | CUT |
| REC-05 | 内部对账（支付、退款、转账） | P0 | M6 | REC-01、PAY-04、REF-03、TRF-02、TRF-03、MQ-04 | TODO |
| REC-06 | 差错自动处理 | P0 | M6 | REC-03、REC-05、PAY-09、MQ-05 | TODO |
| REC-07 | 差错人工处理与挂账 | P0 | — | — | CUT |
| REC-08 | 对账报告 | P0 | M6 | REC-03、REC-05 | TODO |
| OPS-01 | 健康检查与优雅退出 | P0 | M3 | BASE-08、BASE-09 | DONE |
| OPS-02 | 链路追踪 | P0 | M5 | GW-01、GW-02、GW-06、ACC-11、MQ-03、MQ-04 | TODO |
| OPS-03 | 业务与运行指标 | P0 | M5 | OPS-01、GW-02、GW-06、MQ-04 | TODO |
| OPS-04 | 监控面板与告警规则 | P1 | — | — | CUT |
| OPS-05 | pprof 与管理端口 | P0 | M3 | OPS-01 | DONE |
| OPT-01 | 分阶段耗时埋点 | P0 | M5 | OPS-02、OPS-03、GW-02、GW-06、GW-09、MQ-02、MQ-04、REF-02、TRF-02、TRF-03 | TODO |
| OPT-02 | 批量记账合并提交 | P0 | M7 | MQ-04、QA-02、BASE-15 | TODO |
| OPT-03 | 热点平台账户子账户拆分 | P0 | M7 | ACC-03、QA-02 | TODO |
| OPT-04 | 链路延迟优化 | P0 | M7 | OPT-01、QA-02 | TODO |
| OPT-05 | 平台账户缓冲入账 | P2 | M8 | OPT-03 | TODO |
| TL-01 | paycli | P0 | M3 | BASE-13 | DONE |
| TL-02 | merchant-demo | P0 | M5 | BASE-13 | TODO |
| TL-03 | 一键演示脚本 | P1 | M8 | TL-01、GW-07、GW-09、MQ-04、REF-03、TRF-02、TRF-03、REC-08 | TODO |
| TL-04 | OpenAPI 文档 | P1 | M8 | GW-02、GW-09 | TODO |
| QA-01 | 故障演练 | P0 | M7 | BASE-15、QA-02、CH-06、GW-04、GW-06、GW-07、GW-09、PAY-05、REF-04、TRF-04、MQ-02、MQ-04、OPT-02、REC-03、REC-05、OPS-01 | TODO |
| QA-02 | 基线压测与报告 | P0 | M6 | CH-07、OPT-01、GW-02、GW-04、GW-05、GW-06、GW-07、GW-09、MQ-02、MQ-04、REF-03、REF-04、TRF-04、REC-03、REC-05 | TODO |
| QA-03 | pprof 调优记录 | P0 | M7 | QA-02 | TODO |
| QA-04 | 验收运行 | P0 | M8 | BASE-07、BASE-10、BASE-11、PAY-07、PAY-10、NTF-02、REC-06、REC-08、OPS-05、QA-01、QA-03、QA-05 | TODO |
| QA-05 | 优化后复测与对比报告 | P0 | M7 | QA-02、OPT-02、OPT-03、OPT-04、GW-08 | TODO |
| DOC-01 | README 与面试材料 | P0 | M8 | QA-04 | TODO |

## 4. 功能点详细需求

每个功能点包含：基本信息、说明、规则、数据、异常、验收（可直接转为测试用例）。

### 4.1 基础设施（BASE）

#### BASE-01 工程骨架与构建

`P0 · M1 · 依赖：无`

说明：建立单模块仓库、构建与静态检查流程。

规则：

1. `go.mod` 声明 `go 1.27.0`，module path 为 `github.com/yym108/pay-system`；依赖版本与 `docs/01-tech-decisions.md` §2 一致，显式 require `github.com/bytedance/sonic v1.15.4`。
2. 每个可执行程序位于 `cmd/<name>/main.go`，`main` 只做配置加载、依赖装配、启动与退出。
3. Makefile 目标：`gen`、`build`、`lint`、`test`、`test-integration`、`cover`、`up`、`down`、`migrate`、`seed`、`e2e`、`chaos`、`load`。
4. `.golangci.yml` 至少启用：`errcheck`、`govet`、`staticcheck`、`gosec`、`errorlint`、`bodyclose`、`sqlclosecheck`、`rowserrcheck`、`noctx`、`contextcheck`、`depguard`（落实 §2.3 的跨服务 import 限制）、`forbidigo`（禁止 `float32/float64` 出现在 `service/`、`pkg/money` 的金额字段，禁止 `time.Now()` 出现在 `biz` 包）。
5. 一个多阶段 `deploy/Dockerfile`，通过 `--build-arg APP=<name>` 构建任意程序；构建阶段 `golang:1.27.2-alpine`，运行阶段使用非 root 的最小镜像，开工时锁定镜像 digest。
6. `.gitignore` 排除构建产物、`.env`、覆盖率文件、IDE 目录。

验收：

- [x] `make build` 产出 7 个二进制。
- [x] `make lint` 无告警；在 `service/payment/biz` 中写一个 `time.Now()` 能被 lint 拦截。
- [x] `docker build --build-arg APP=gateway` 成功，容器以非 root 用户运行。

#### BASE-02 本地环境编排

`P0 · M1 · 依赖：BASE-01`

说明：`deploy/docker-compose.yml` 一条命令拉起本地基础设施。应用进程、迁移和种子的可运行内容不在本功能点交付，见下方口径澄清。

口径澄清（2026-10-13）：不改业务规则、资金流向、接口、表结构、错误码，也不改 §3 依赖列。

- 7 个 `cmd/*/main.go` 目前是空 `main`，进程会立即退出，不能保持 healthy。健康检查端点属于 OPS-01。
- `migrate`、`seed` 的迁移文件与种子数据属于 BASE-03。
- 因此本功能点的 `make up` 只启动基础设施长期容器和一次性 `kafka-init`。应用容器（gateway、payment、account、recon、channel-mock、merchant-demo）与 `migrate`、`seed` 写在 compose profile `apps` 中：声明 §2.2 端口、占位镜像和 `depends_on`，不由 `make up` / `make up-lite` 启动。占位命令以非 0 退出，避免被当成已完成。
- BASE-03 替换 `migrate` / `seed` 的命令，OPS-01 为应用容器提供 `/healthz` 之后，再把 profile `apps` 并入默认启动。A1（QA-04 在新克隆目录执行 `make up` 后全部服务可用）仍由 BASE-02、BASE-03、OPS-01 共同完成，本功能点不关闭 A1。
- “全部容器 healthy”在本功能点指 `make up` 实际启动的长期容器：mysql、redis、kafka、prometheus、jaeger。`kafka-init` 是一次性容器，成功标准是退出码 0，不是持续 healthy。
- NFR-DEP-01 的 3 分钟上限按上述范围执行。不在本功能点提前实现 OPS-01 的健康检查或 BASE-03 的迁移与种子。

规则：

1. 镜像版本与 `docs/01-tech-decisions.md` §2.2 一致。
2. 每个基础设施长期容器配置健康检查。`kafka-init` 在 kafka healthy 之后创建 topic，成功则退出 0。profile `apps` 中的应用容器声明 `depends_on`：基础设施 `service_healthy`，`migrate`、`kafka-init`、`seed` 为 `service_completed_successfully`。这些依赖现在只存在于 YAML，`make up` 不启用该 profile。
3. Kafka 单节点 KRaft，关闭自动建 topic；`kafka-init` 创建 `docs/05-api.md` §11.1 中的 topic，每个 3 分区、副本数 1。宿主机只映射 9094，容器网络内使用 `kafka:9092`。
4. 资源限制：Kafka 堆 512MB；MySQL `innodb_buffer_pool_size=256M`、`max_connections=500`、默认时区 `+08:00`。
5. 可观测组件（Prometheus、Jaeger）放在 compose profile `obs`，`make up` 默认包含，`make up-lite` 不包含。Jaeger 使用镜像默认 all-in-one；健康检查打容器内 `127.0.0.1:13133/status`，该端口不映射到宿主机（§2.2 未列出）。
6. 端口映射遵循 §2.2。`make up` 的渲染结果（profile `obs`）中，宿主机端口全部绑定 `127.0.0.1`。profile `apps` 中除 8080、9080、9100 外也绑定 `127.0.0.1`。
7. 敏感配置（数据库密码、`SECRET_MASTER_KEY`、`ADMIN_TOKEN`、渠道密钥）来自环境文件，仓库只提交 `deploy/.env.example`（演示值）。`make up` 默认读取 `deploy/.env`；验证可以用 `ENV_FILE` 指向 example 或临时副本，不提交、不留下 `deploy/.env`。
8. `docker-compose.chaos.yml` 作为覆盖文件，用于开启故障注入（QA-01）。本功能点只提供可合并的占位，不实现演练场景。

验收：

- [x] 复制 `.env.example` 后执行 `make up`（可用 `ENV_FILE` 指向副本或 example），在镜像已拉取的前提下 3 分钟内：mysql、redis、kafka、prometheus、jaeger 为 healthy，`kafka-init` 退出码 0。不启动 profile `apps`。
- [x] `make down` 停止 `make up` 与 `make up-lite` 会启动的全部容器；`make down V=1` 同时删除本项目数据卷。
- [x] `docker compose config`（含 profile `obs`，即 `make up` 的渲染结果）中所有宿主机端口绑定 `127.0.0.1`。`docker compose --profile apps config` 中除 8080、9080、9100 外的端口绑定 `127.0.0.1`。
- [x] `make up-lite` 不包含 prometheus、jaeger。
- [x] `kafka-init` 创建 §11.1 的 6 个 topic，每个 3 分区、副本数 1；自动建 topic 关闭。

#### BASE-03 数据库迁移与种子数据

`P0 · M1 · 依赖：BASE-02 · 表：全部`

规则：

1. 表结构以 `migrations/<schema>/` 下的迁移文件为准，每个 schema 从 `000001_init.up.sql` 和 `.down.sql` 开始；表结构文档是 `docs/database.md`，其来源是这些迁移文件（原设计草稿已在本功能点拆分后删除）。账户分片共用 `migrations/account/`，迁移容器对 `pay_account_0`、`pay_account_1` 各执行一次。
2. 迁移只向前演进：已合入的迁移文件不修改，变更通过新文件完成。
3. 数据库账号最小权限：

   | 账号 | 权限 |
   |---|---|
   | `svc_account` | `pay_account_0.*`、`pay_account_1.*` 读写 |
   | `svc_payment` | `pay_payment.*` 读写 |
   | `svc_recon` | `pay_recon.*` 读写；`pay_payment.*`、`pay_account_0.*`、`pay_account_1.*` 只读 |
   | `svc_channel_mock` | `pay_channel_mock.*` 读写 |
   | `migrator` | 全部 schema 的 DDL，仅迁移容器使用 |

4. 种子数据（幂等，可重复执行）：
   - 每个分片的平台账户：`CHANNEL_RECEIVABLE(mockpay)`、`FEE_INCOME`、`TRANSIT`，各开 `sub_no = 0 … PLATFORM_SUB_ACCOUNTS − 1` 个子账户。
   - 演示商户 `M100001`（费率 0，分片 1）、`M100002`（费率 60bp，分片 0）、`M100003`（费率 0，分片 1），各自在所属分片开 `MERCHANT_SETTLE` 账户，初始余额 0。商户余额只能通过支付入账获得，不允许种子直接写余额（不变量 3）。
   - 压测种子 `make seed-load`：额外创建 `M200000`–`M200099` 共 100 个商户（两个分片各 50 个），由 `scripts/load-fund.sh` 通过真实支付链路为每个商户入账，供退款和转账压测使用。
   - 商户密钥使用 `.env` 中的演示明文，经 BASE-14 加密后写入。

口径澄清（2026-10-14）：不改业务规则、资金流向、接口、表结构、错误码，也不改 §3 依赖列。`app_secret_cipher` 保持 NOT NULL。

- 密钥：BASE-14 属于 M3，不是本功能点依赖，本功能点不实现 `pkg/secretbox`。种子按 BASE-14 已写明的算法与密文格式（AES-256-GCM，`version(1B) + nonce(12B) + ciphertext + tag`，version 取值 1，主密钥 `SECRET_MASTER_KEY`）用标准库加密后写入。明文格式为 `DEMO_APP_SECRET_FORMAT`（默认 `demo_secret_%s_0123456789ab`，`%s` 为商户号），因此 `M100001` 的明文与 `docs/05-api.md` §3.7 测试向量一致。重复执行不覆盖已有密文（nonce 随机，覆盖会让密文变化）。BASE-14 实现时必须能解密这些种子密文，不得另起格式。2026-10-23 起种子改用 BASE-14 的 `pkg/secretbox` 加密，格式不变（见 BASE-14 说明）。
- 压测入账：`make seed-load` 在本功能点只幂等创建 `M200000`–`M200099` 及其所属分片的零余额结算账户，费率 0。不写分录、不写非零余额（不变量 3）。`scripts/load-fund.sh` 不在本功能点实现；真实支付链路尚未存在，入账留给 QA-02。本功能点不调用、不伪造该脚本。
- 启动范围：替换 profile `apps` 中 `migrate` / `seed` 的命令，使 `make migrate`、`make seed` 可单独执行。不把 profile `apps` 并入默认 `make up`（应用容器仍等 OPS-01 的 `/healthz`）。A1 仍由 BASE-02、BASE-03、OPS-01 共同完成。
- 开户：种子仍然直接插入账户行，不调用 `OpenAccount`（余额与冻结均为 0，不写分录）。余额方向和透支标志按 §2.5.1 由账户类型决定。`docs/06-flows.md` 中“开户只由 seed 调用、不对运营开放”仍是目标态，本功能点不改图。账户号用确定性的 `AC` + 19 位数字，保证重复执行命中同一行，而不是每次新开账户。商户显示名是 `演示商户` 或 `压测商户` 加商户号；`app_id` 按约定 §2.3 取 `app_` + 商户号小写。`notify_url` 留空，不发明通知地址。
- 账号口令：`svc_account`、`svc_payment` 的口令从已有 `ACCOUNT_DB_DSN_*`、`MYSQL_DSN` 解析，不另设重复配置。`svc_recon`、`svc_channel_mock`、`migrator` 的口令是新增配置，见约定 §8。bootstrap 会删除并重建这五个账号，使授权保持在规则 3 的范围内。

验收：

- [x] `make migrate` 在空库上执行成功，两个账户分片的表结构一致；`down` 后再次 `up` 成功。
- [x] `make seed` 连续执行两次，账户和商户数量不变；每个商户的结算账户只出现在所属分片。
- [x] 用 `svc_recon` 账号向 `pay_payment`、`pay_account_0` 写入被拒绝。

#### BASE-04 金额工具（pkg/money）

`P0 · M1 · 依赖：BASE-01`

规则：

1. 金额类型 `type Amount int64`，单位分；只提供整数运算。
2. `Add`、`Sub` 检测溢出，溢出返回错误而不是回绕。正溢出和负溢出都要检测，包括 `MinInt64` 参与的运算。
3. 手续费 `Fee(amount, rateBP) = (amount × rateBP + 5000) / 10000`，四舍五入；`rateBP` 范围 0–10000。`rateBP` 越界返回错误，不计算。`amount` 为负且 `rateBP` 不为 0 时返回错误（加 5000 的四舍五入只对非负金额成立）；`rateBP` 为 0 时结果为 0。
4. 校验函数 `ValidatePayAmount`：1 ≤ amount ≤ 10,000,000。越界返回错误。
5. `String()` 输出“元”格式（如 `100.00`），只用于日志和展示。

口径澄清（2026-10-14）：`Add`、`Sub`、`Fee`、`ValidatePayAmount` 的失败都是 `pkg/money` 的普通 `error`，不新增错误码，也不提前实现 `pkg/errno`（BASE-06）。不改资金流向、接口、表结构和已登记错误码。

验收（单元测试向量）：

- [x] `Fee(1, 60) = 0`；`Fee(10000, 60) = 60`；`Fee(8333, 60) = 50`；`Fee(1, 5000) = 1`（0.5 进位）；`Fee(10000000, 10000) = 10000000`；`Fee(x, 0) = 0`。
- [x] `Add(MaxInt64, 1)` 返回溢出错误。
- [x] `Amount(1).String() = "0.01"`，`Amount(-150).String() = "-1.50"`。

#### BASE-05 ID 生成（pkg/idgen）

`P0 · M1 · 依赖：BASE-01`

规则：

1. 64 位布局：1 位符号（恒为 0）+ 41 位毫秒时间戳（纪元 `2026-01-01T00:00:00+08:00`）+ 10 位 worker ID + 12 位序列号。时间早于纪元，或毫秒时间戳放不进 41 位时返回错误，不回绕。
2. worker ID 来自环境变量 `WORKER_ID`（0–1023）。`WorkerIDFromEnv` 在缺失、非整数或超出范围时返回错误，不调用 `os.Exit`；进程在启动装配时失败，本功能点不改 `cmd/*/main.go`。docker-compose 为每个会发号的实例写死互不相同的字面量，分配见 `docs/03-conventions.md` §2.5。
3. 同一毫秒序列号用尽（0–4095）时等待下一毫秒。等待观察注入的时钟，并且可以被取消。
4. 时钟回拨 ≤ 5ms 时等待追平；> 5ms 时返回本包错误 `ErrClockRollback`（可用 `errors.Is` 识别）。本包不返回数字码，也不包装 Kitex 错误。调用方（以后的服务）把该错误映射为已有的 `10004` / `UNAVAILABLE`，不新增错误码。
5. `Generator.NewNo(ctx, prefix)` 返回 `prefix + 19 位左补零十进制`。`ctx` 是实现约束：等下一毫秒或追平回拨时必须能取消，避免假时钟把调用挂死。前缀规则不变，必须是 `docs/03-conventions.md` §2.2 中已登记的值，未登记的拒绝。
6. `Parse(no)` 不阻塞、不带 context，解析出时间戳和 worker ID，便于排查。

本包自己定义只含 `Now() time.Time` 的时钟接口，测试注入它；生产路径使用真实时间。不实现 `pkg/clock`（BASE-07），也不实现 `pkg/errno`（BASE-06）。

验收：

- [x] 64 个 goroutine 共生成 100 万个 ID，无重复，单个 goroutine 内严格递增。
- [x] 注入回拨 3ms 的时钟，生成成功且耗时 ≥ 3ms；回拨 10ms 返回错误。
- [x] `NewNo("PA")` 长度 21，`Parse` 结果与生成时间误差 < 1ms。

#### BASE-06 错误码（pkg/errno）

`P0 · M1 · 依赖：BASE-01`

规则：

1. 定义 `docs/03-conventions.md` §5.2 的全部错误码，结构包含 `Code`、`Msg`、`HTTPStatus`，支持 `errors.Is` 和包装底层错误。
2. 提供与 Kitex `BizStatusError` 的双向转换。
3. 未知错误统一视为 `10003`；对外消息不包含底层错误文本。

口径澄清（2026-10-14）：

1. 本包定义约定 §5.2 里除 `30002` 以外的全部码。`30002` 保留不用、没有名称，代码里只注释说明跳过，不提供可用错误。未登记码（含 `30002`）和任何未知的非 nil 错误都视为 `10003`。
2. `nil` 表示成功：码 `0`，对外 msg `ok`，HTTP 200。不要把 nil 当成 `10003`。映射函数在成功时返回非 nil 的成功值，只供填写响应，调用方不要把它当作失败再返回。
3. 对外 `Msg` 是下表的稳定短英文，不含内部细节，与约定 §5.2 的“对外 msg”列一致。`Error()` 只返回 `Msg`。包装的底层错误只通过 `Unwrap` 取得，不拼进 `Msg` 或 `Error()`。`10003` 的对外消息固定为 `internal error`。
4. `10001` 的 `HTTPStatus` 是 400。表里“请求体超过 64KB 时 413”是网关行为（接口 §2.1），本包不实现网关，也不另造错误码。
5. 与 Kitex `kerrors.BizStatusError`（v0.16.3）双向转换：服务端把 errno 变成 `BizStatusError`（码 + 本表 msg）；客户端用 `kerrors.FromBizStatusError` 按码还原。对端消息与本表不一致时，对外仍用本表 msg。不生成 IDL，不起网络服务。不实现 `errno.ErrDuplicate` 和 MySQL 1062 映射（BASE-08，技术决策 D4）。

| 码 | 对外 msg |
|---|---|
| 0 | `ok` |
| 10001 | `invalid parameter` |
| 10002 | `not found` |
| 10003 | `internal error` |
| 10004 | `service unavailable` |
| 10005 | `rate limited` |
| 10006 | `idempotent conflict` |
| 20001 | `invalid signature` |
| 20002 | `timestamp expired` |
| 20003 | `nonce replayed` |
| 20004 | `invalid merchant` |
| 20005 | `admin unauthorized` |
| 30001 | `order not found` |
| 30003 | `order status conflict` |
| 30004 | `order expired` |
| 30005 | `refund exceeded` |
| 30006 | `channel rejected` |
| 30007 | `unsupported channel` |
| 30008 | `invalid payee` |
| 40001 | `account not found` |
| 40002 | `insufficient balance` |
| 40003 | `unbalanced entries` |
| 40004 | `freeze status conflict` |
| 40005 | `account inactive` |
| 50001 | `invalid channel signature` |
| 50002 | `channel amount mismatch` |

`30002` 没有对外 msg。

验收：

- [x] 包装后的错误经 `errors.Is` 能识别原错误码。
- [x] Kitex 服务端返回 `40002`，客户端还原为同一错误码。

#### BASE-07 时钟（pkg/clock）

`P0 · M1 · 依赖：BASE-01`

规则：`Clock` 接口提供 `Now()`；`Real` 实现返回东八区时间；`Fake` 实现支持 `Set`、`Advance`，并发安全。`biz` 和 `job` 包只能通过 `Clock` 取时间。

口径澄清（2026-10-15）：不改业务规则、资金流向、接口、表结构、错误码，也不改 §3 依赖列。不实现配置、日志和数据库。

- `Clock` 只有 `Now() time.Time`。`Real.Now` 返回这一瞬间的东八区时间。
- 运行镜像不安装 tzdata。`Real` 不调用 `time.LoadLocation`，也不 import `time/tzdata`：没有时区文件时，`LoadLocation("Asia/Shanghai")` 会失败。中国自 1991 年起没有夏令时，固定偏移与当前民用时间一致，因此用 `time.FixedZone("Asia/Shanghai", 8*60*60)`。地点名是 `Asia/Shanghai`，与 UTC 同一瞬间的墙钟相差 8 小时。这样不依赖 zoneinfo。本功能点不改 Dockerfile。DSN 里的 `loc=Asia/Shanghai` 仍由驱动调用 `LoadLocation`，继续靠 BASE-03 的 `-tags timetzdata`。
- `Fake` 提供 `Set`、`Advance`，用互斥锁保证并发安全。`Advance(10s)` 之后 `Now()` 比调用前精确多 10 秒。
- `biz` 和 `job` 只能通过注入的 `Clock` 取时间（约定 §4；golangci 已禁止这两处调用 `time.Now`）。不新建空的 biz 或 job 包。不把 `pkg/idgen` 改成依赖 `pkg/clock`，idgen 保持自己的窄时钟。
- “无时区文件的容器中也成立”不起容器验证：实现不读 zoneinfo。单元测试检查生产代码没有 `LoadLocation`、地点名是 `Asia/Shanghai`、偏移是 +8 小时，并用 1991-07-01 的夏令时差别证明地点不是 IANA 时区库的结果。

验收：

- [x] `Fake.Advance(10s)` 后 `Now()` 精确前进 10 秒。
- [x] `Real.Now().Location()` 为 `Asia/Shanghai`，且在无时区文件的容器中也成立。

#### BASE-08 数据库访问与事务（pkg/dbx）

`P0 · M1 · 依赖：BASE-06`

规则：

1. `Open` 统一 DSN 参数：`parseTime=true`、`loc=Asia/Shanghai`、`transaction_isolation='READ-COMMITTED'`、`timeout=1s`、`readTimeout=3s`、`writeTimeout=3s`；连接池参数可配置（默认最大 50 连接、空闲 10、连接最长存活 30 分钟）。
2. `WithTx(ctx, db, fn)`：开启事务，`fn` 返回错误或 panic 时回滚；遇到 1213、1205 时整体重试（`docs/03-conventions.md` §6.3）；事务总时长超过 3 秒取消。
3. `IsDuplicate(err)` 识别 1062；`IsCheckViolation(err)` 识别 3819。
4. `MustAffectOne(result)` 用于 CAS 场景的影响行数判断。
5. 所有 SQL 参数化，禁止字符串拼接用户输入。

口径澄清（2026-10-15）：不改资金流向、接口、表结构和已有错误码。不新增错误码，不实现 `errno.ErrDuplicate`，不注册 `30002`。

- 1213、1205：含第一次在内最多执行 4 次 `fn`；三次重试前分别等待 10ms、20ms、40ms。约定 §6.3 的“最多 3 次”是重试次数。
- 事务总时长默认 3 秒。用父 ctx 派生超时，先到期的那个生效；超时后取消并回滚，不能留下已提交的数据。
- `IsDuplicate` 只识别 MySQL 1062（含包装）。业务层以后自己决定是不是幂等成功，本包不转换成业务码。`MustAffectOne` 在影响行数不是 1 时返回普通错误，不映射为 `40002` 或其他业务码；错误文本不带 SQL 或参数。
- 连接池的 0 表示默认（50、10、30 分钟），负数返回错误。默认写在 `Open` 的参数里，不新增环境变量。
- 规则 1 的 DSN 参数在连接前强制覆盖，调用方写了别的值也要覆盖。已废弃的 `tx_isolation` 会去掉，避免和 `transaction_isolation` 互相覆盖。不在 `Open` 里执行业务 SQL。

验收：

- [x] `fn` 返回错误时数据未写入；`fn` panic 时回滚并重新抛出。
- [x] 构造两个事务交叉加锁触发死锁，`WithTx` 重试后成功。
- [x] 重复插入唯一键，`IsDuplicate` 返回 true。

#### BASE-09 配置（pkg/config）

`P0 · M1 · 依赖：BASE-01`

规则：

1. 每个程序一个配置结构体，字段来自环境变量（caarlos0/env v11.4.1），带默认值。环境只来自调用方传入的列表（`Options.Environment`），不合并进程环境。
2. 启动时校验必填项和取值范围。失败返回错误，错误文本包含缺失或非法变量的名字。各业务入口加载失败则把错误写到 stderr 并以非 0 退出；成功则把脱敏后的生效配置写到 stdout 后返回。不监听端口，不连接数据库，不启动 HTTP/RPC。
3. 生效配置里，带 `secret:"true"` 标签的字段输出为 `***`，没打这个标签的字段按原值输出。脱敏是 `pkg/config` 自己的反射，不实现 `pkg/logx`，也不扫描卡号或手机号。
4. 所有补偿任务间隔、重试计划、超时时间均可配置，测试可整体缩短。

DSN 只按驱动的文本规则装配，不连接数据库：

- payment、account、seed 的 DSN 原样保存，不改用户和口令。
- recon 不读取 `MYSQL_DSN`，避免把 payment 的口令放进对账进程。必填 `MYSQL_RECON_PASSWORD`、`ACCOUNT_DB_DSN_0`、`ACCOUNT_DB_DSN_1`。从 `ACCOUNT_DB_DSN_0` 解析 host、port、query，生成四条 secret DSN：用户都是 `svc_recon`，口令都是 `MYSQL_RECON_PASSWORD`，库名是 `pay_recon`、`pay_payment`，以及原来的分片库 `pay_account_0`、`pay_account_1`。
- channel-mock 不读取 `MYSQL_DSN`。必填 `MYSQL_CHANNEL_MOCK_PASSWORD`。`MYSQL_HOST` 默认 `mysql`（渠道进程在 compose 网络里；bootstrap 的默认仍是 `127.0.0.1`），`MYSQL_PORT` 默认 `3306`。查询参数固定为 `parseTime=true&loc=Asia%2FShanghai&charset=utf8mb4`。用户 `svc_channel_mock`，库 `pay_channel_mock`。
- seed、bootstrap 只提供配置结构体，不改现有加密、写库和建号流程。paycli 的四个变量由 TL-01 登记，加载时可以为空，不要求 `WORKER_ID`。不新建 `cmd/migrate`。

验收：

- [x] 缺少 `SECRET_MASTER_KEY` 时 payment 启动失败并提示该变量名。
- [x] 启动日志中不出现密钥明文。

#### BASE-10 日志与脱敏（pkg/logx）

`P0 · M1 · 依赖：BASE-01`

规则：

1. 使用 `log/slog` JSON 输出，固定字段：`time`、`level`、`msg`、`service`、`trace_id`、`span_id`、`request_id`。没有值时后三个是空字符串，字段仍在。`trace_id` 和 `span_id` 来自 `go.opentelemetry.io/otel/trace` 的 `SpanContextFromContext`（只要 trace API，不引入 sdk 或 exporter）。`request_id` 来自 `logx.WithRequestID`。这三个关联字段按原值写入，不做兜底扫描，避免 span id 里的连续数字被改成卡号。
2. 脱敏 Handler 按字段名处理。不区分大小写，名字包含片段即匹配。同时命中多条时，整段替换优先；否则按下表顺序用先命中的一条。长度按 Unicode 码点计。卡号短于 10、手机号短于 7、证件号短于 2 时，整段换成 `***`，避免前后重叠把原文露出来。

   | 字段名（不区分大小写，包含即匹配） | 处理 |
   |---|---|
   | `secret`、`password`、`token`、`signature`、`master_key` | 整体替换为 `***` |
   | `card_no`、`bank_card` | 保留前 6 后 4 位；短于 10 则整段 `***` |
   | `phone`、`mobile` | 保留前 3 后 4 位；短于 7 则整段 `***` |
   | `id_card` | 保留前 1 后 1 位；短于 2 则整段 `***` |

   分组的键同样匹配。键需要整段替换，或者值是分组而键属于卡号、手机号、证件号时，整个分组换成 `***`，不展开子字段。`error` 先转成 `Error()` 的字符串，再按字段名或兜底规则处理。
3. 对字符串值做兜底扫描，包括字段名不敏感的字段和 `msg`：连续 13–19 位 ASCII 数字按卡号脱敏，正好 11 位且以 `1` 开头的按手机号脱敏。11 位不是卡号。更短的数字（例如金额 `100`）、12 位、长于 19 位，以及中间被非数字断开的数字，都不脱敏。非字符串不做兜底扫描。
4. Hertz `hlog`、Kitex `klog` 通过适配器输出到同一个 slog。不在本包启动 HTTP 或 RPC。
5. 访问日志不记录请求体原文，只记录路径、状态码、耗时、商户号、错误码。不改 `pkg/config` 的脱敏。

验收：

- [x] `slog.Info("x", "app_secret", "abc")` 输出 `***`。
- [x] `slog.Info("x", "memo", "card 6222020200112233445")` 输出 `622202*********3445`。
- [x] 上下文中带 trace 时，日志包含对应 `trace_id`。

#### BASE-11 集成测试基座

`P0 · M1 · 依赖：BASE-03、BASE-08`

规则：

1. 集成测试文件带 `//go:build integration`，`make test-integration` 运行。
2. 默认用 testcontainers-go 启动 `mysql:8.4.11` 并执行迁移；设置 `TEST_MYSQL_DSN` 时复用已有实例。
3. 每个测试包使用独立 schema（随机后缀），测试结束删除，保证可并行。
4. 提供 Kafka 测试容器助手（`apache/kafka:4.3.1`），供 MQ 相关测试使用。
5. 提供 `Fake` 时钟、测试商户、测试账户的构造工具。

验收：

- [x] 两个集成测试包并行运行互不影响。
- [x] 复用模式下单个包启动开销 < 2 秒。

#### BASE-12 IDL 与代码生成

`P0 · M1 · 依赖：BASE-01`

规则：

1. `idl/base.thrift` 定义公共结构（时间字符串、业务码）；各服务一个 IDL 文件，方法清单见 `docs/05-api.md` §10。
2. 字段规则：只增不删；新增字段一律 `optional`；金额字段为 `i64`，命名以 `amount` 结尾；状态用 `string`，取值与数据库一致。
3. `make gen` 使用 kitex v0.16.3 生成代码到 `kitex_gen/`，生成结果入库；CI 检查生成代码与 IDL 一致（重新生成后 `git diff` 为空）。

验收：

- [x] `make gen && git diff --exit-code kitex_gen` 通过。

#### BASE-13 签名库（pkg/sign）

`P0 · M3 · 依赖：BASE-01`

规则：

1. 实现三种签名，算法以 `docs/05-api.md` 为准：商户请求签名（§3.1）、平台响应与商户通知签名（§3.3、§3.4）、渠道双向签名（§3.5）。
2. 签名函数只接收原始字节，不对 body 或查询串做任何规范化。时间窗口与 nonce 检查不在本包，由 GW-04、GW-06 负责。
3. 校验使用 `hmac.Equal` 常量时间比较。
4. 提供签名与验签两侧函数，供网关、payment（商户通知、渠道请求）、recon（账单请求）、paycli、merchant-demo、channel_mock 共用。
5. 入参：方法、RequestURI、时间戳、nonce 为请求头或请求行的原样字符串，body 为原始字节；nil body 与空 body 都按空串计算。本包不转大写、不解析时间戳、不检查时间戳与 nonce 的格式（GW-04 负责）。`FormatTimestamp` 把时间格式化为十进制 Unix 秒，供签名方生成 `X-Timestamp` / `X-Mock-Timestamp`。
6. 以换行分隔的字段（方法、RequestURI、时间戳、nonce）含换行时，签名与验签都返回错误：拼接串用换行分隔，字段含换行会让两组不同的字段得到同一个拼接串。合法请求的这些字段都不含换行，不改变接口契约。body 可以含换行。
7. 只接受规范的标准 base64 签名：44 个字符、带 `=` 填充、填充位为 0、解码后 32 字节。缺填充、URL-safe 字母表、hex、含空白或换行的签名一律验签失败。空密钥在签名与验签两侧都返回错误，不用空密钥计算 HMAC。
8. 错误是本包的普通 `error` 哨兵（`ErrEmptyKey`、`ErrInvalidField`、`ErrMalformedSignature`、`ErrSignatureMismatch`），文本固定，不含密钥、签名值和拼接串。本包不定义对外数字码，调用方按哪一侧、哪个错误映射：
   - 验签侧的 `ErrMalformedSignature`、`ErrSignatureMismatch`、`ErrInvalidField` 是对端签名问题：网关映射为 `20001`（GW-03）或 `50001`（GW-06），对外不区分原因（约定 §5.3 第 4 条）；merchant-demo、channel_mock 返回 HTTP 401；paycli 报告响应验签失败。
   - `ErrEmptyKey`（签名、验签两侧）以及签名侧的全部错误（网关签响应、payment 签通知、paycli 签请求、双方的渠道签名）是本端配置或内部故障，不是对端签名错误：平台服务按 `10003` 处理并打 ERROR 日志，日志不含密钥、签名值和拼接串；paycli 报错并以退出码 1 结束；merchant-demo、channel_mock 返回 HTTP 500。

验收：

- [x] `docs/05-api.md` §3.7 的全部测试向量通过。
- [x] body 改动 1 字节、查询串参数顺序改变、时间戳改变，验签均失败。

#### BASE-14 密钥加密（pkg/secretbox）

`P0 · M3 · 依赖：BASE-01`

规则：

1. AES-256-GCM，主密钥为 `SECRET_MASTER_KEY`（32 字节，base64 编码）；密文格式 `version(1B) + nonce(12B) + ciphertext + tag`。
2. 每次加密使用随机 nonce。
3. 解密失败返回错误，不输出密文或明文。

说明（BASE-14 口径澄清）：

- 入口：`secretbox.New(key []byte)` 接收解码后的 32 字节主密钥；`secretbox.NewFromBase64(s)` 接收 `SECRET_MASTER_KEY` 原值，按标准 base64（带填充，与 `pkg/config` 校验一致）解码。`(*Box).Seal(plain)`、`(*Box).Open(blob)` 加解密。`Box` 不保留调用方传入的 key 切片；内部 AES 轮密钥与主密钥等价，`Box` 应按密钥材料对待（不序列化、不打印、长期持有于进程内即可）。`Box` 可并发使用。
- 版本字节取 `1`，与 BASE-03 种子已写入的密文相同。不使用 AAD（附加认证数据）：种子密文没有 AAD，加上会让已有密文无法解密。nonce 用 `crypto/rand`。版本字节不受 GCM tag 保护，当前靠 `Open` 显式拒绝非 1 的版本；将来 v2 应把版本字节（及商户号）纳入 AAD。
- 错误为本包固定文本的哨兵：主密钥不是 32 字节 `ErrKeySize`，不是标准 base64 `ErrKeyEncoding`；密文短于 29 字节（1 + 12 + 16）`ErrMalformed`，版本字节不是 1 `ErrUnknownVersion`，认证失败（篡改、截断、主密钥不对）`ErrDecrypt`。文本不含主密钥、明文、密文。它们是本端配置或数据故障，平台服务按已有的 `10003` 处理并打 ERROR 日志，不新增错误码。
- 空明文可以加密，密文 29 字节。`merchant.app_secret_cipher` 为 `VARBINARY(256)`，明文最长 227 字节；本包不限制长度，超长由写库报错。
- BASE-14 完成后种子改用 `pkg/secretbox` 加密，`internal/seed` 不再自带加密实现，密文格式不变；种子在连库之前校验主密钥。

验收：

- [x] 加密后解密得到原文；同一明文两次加密结果不同。
- [x] 篡改任一字节，解密失败。
- [x] 主密钥长度不是 32 字节时初始化失败。

#### BASE-15 故障注入点（pkg/fault）

`P0 · M3 · 依赖：BASE-01、BASE-09`

说明：故障点原属 QA-01 规则 1、2。PAY-03、GW-06、REF-02、TRF-03、MQ-02、MQ-04、OPT-02 的规则在代码里放故障点，PAY-05、REF-04、TRF-04、MQ-04、OPT-02 和 QA-01 的验收靠它模拟崩溃，所以单独成为 M3 的基础包，QA-01 只负责演练。

规则：

1. `fault.Point(ctx, name)`：配置项 `FAULT_POINTS`（约定 §8，默认空）按逗号分隔，包含 `name` 时向本进程发送 SIGKILL，模拟 `kill -9`；不包含时立即返回。已取消的 `ctx` 不跳过已经启用的信号；`ctx` 为 nil 不 panic。没调用过初始化时与空配置相同。
2. `name:once` 表示只触发一次：发送 SIGKILL 之前先在标记目录（默认 `/var/run/fault`，由 `deploy/docker-compose.chaos.yml` 挂载，不是配置项）写入以故障点命名的标记文件，名称中的点保留；文件已存在时不再触发，因此进程重启后照常运行。写标记失败（目录不存在或没有权限）时不发信号，也不 panic。生产路径在发 `:once` 之前对该目录 `MkdirAll`（0755），没有挂载时本地演练也能写。
3. `FAULT_POINTS` 为空（默认）时 `Point` 什么都不做：不发信号，不创建、不读取标记目录，也不做 `MkdirAll`。生产与普通测试都是这个状态。
4. 发信号的函数和标记目录可在测试中替换（包内选项，不新增配置项），以便单元测试不真的结束进程。初始化每次替换整个启用集合；空字符串表示清空。
5. 启动校验：进程启动时用 `FAULT_POINTS` 初始化本包，各项去掉首尾空白和恰好一个 `:once` 后缀后必须是第 6 条登记表中的名称；出现未登记的名称（含拼写错误、空项、`:once` 以外的后缀）时初始化返回错误，错误信息带该项原文，进程启动失败。同一名称出现两次（不论是否带 `:once`）同样返回错误，错误信息含该名称，避免一次带 `:once`、一次不带，行为含糊。防止拼写错误使故障点静默失效、演练假通过。
6. 已登记的故障点（新增故障点须在此登记，并在 QA-01 演练表中给出场景）：

   | 故障点 | 所在功能点 | 位置 |
   |---|---|---|
   | `pay.after_tx1` | PAY-03 | 事务 1 提交后、调用渠道前 |
   | `callback.after_commit` | GW-06 | 回调事务提交后、返回响应前 |
   | `outbox.after_send` | MQ-02 | Kafka 确认后、CAS 回写前 |
   | `ledger.after_commit` | MQ-04 | 入账事务提交后、offset 提交前 |
   | `ledger.after_batch_commit` | OPT-02 | 任一分片组提交后、offset 提交前 |
   | `refund.after_freeze` | REF-02 | Freeze 成功后、置为 PROCESSING 前 |
   | `transfer.after_try` | TRF-03 | 两个 Try 结束后、决策落盘前 |
   | `transfer.after_decision` | TRF-03 | 决策落盘后、发起 Confirm / Cancel 前 |
   | `transfer.after_payer_confirm` | TRF-03 | 串行模式下付款方 Confirm 之后、收款方 Confirm 之前 |

验收：

- [x] 替换发信号函数后，`FAULT_POINTS=pay.after_tx1` 时 `Point(ctx, "pay.after_tx1")` 发信号 1 次，`Point(ctx, "callback.after_commit")` 发信号 0 次。
- [x] `FAULT_POINTS=pay.after_tx1:once`、标记目录为临时目录：第一次调用先写标记文件再发信号；用同一目录新建实例（模拟重启）后再调用，不发信号。
- [x] `FAULT_POINTS` 为空时任何名称都不发信号，临时标记目录中没有新建文件。
- [x] `FAULT_POINTS=pay.after_tx`（拼写错误）、`pay.after_tx1,,callback.after_commit`（空项）、`pay.after_tx1:twice` 初始化都返回错误，错误信息含出错的那一项；`pay.after_tx1:once, callback.after_commit` 初始化成功。
- [x] 以 `FAULT_POINTS=pay.after_tx` 启动的子进程在启动阶段以非 0 退出码退出，没有进入业务逻辑。
- [x] 子进程测试：以 `FAULT_POINTS=<name>` 启动的子进程执行到 `Point` 时被 SIGKILL 结束（父进程得到 `signal: killed`），`Point` 之后的语句没有执行。

### 4.2 账户与记账（ACC）

本节的“商户账户”指商户在所属分片的 `MERCHANT_SETTLE` 账户；“本分片平台账户”指与该商户同分片、按 `crc32(biz_no) % N` 选中的平台子账户。所有对外的 account RPC 以商户号为入参，由 account 内部路由，调用方不传账户号。

#### ACC-13 账户分片路由

`P0 · M1 · 依赖：BASE-08、BASE-09`

规则：

1. `pkg/shard.Of(merchantID) (int, error)`：商户号必须是 `M` + 恰好 6 位数字。取数字部分对固定的 2 取模。不读 `ACCOUNT_SHARD_COUNT`，也不读其他环境变量。格式非法返回错误，本包不返回业务码；调用方以后映射为 `10001`。payment 与 account 共用该函数。
2. `dal.Open` 用 `dbx.Open` 为 `ACCOUNT_DB_DSN_0`、`ACCOUNT_DB_DSN_1` 各建一个连接池。第二个失败时关闭第一个。启动时 `dbx.Open` 会 Ping，连不上则 Open 失败。运行中某个池被 `Close` 后，`/readyz` 返回 503；两个池都在 1 秒内 Ping 成功则 200。本功能点只提供这个处理函数，不监听端口，不实现 `/healthz`、Kafka 检查、SIGTERM 和 metrics。这些仍属 OPS-01。
3. `dal.ShardDB(shard)` 只接受 0 和 1。业务通过 `pkg/dbx.WithTx(ctx, ShardDB(s), fn)` 开启事务；`fn` 只能拿到这一个池的 `*sqlx.Tx`，从类型上避免跨分片事务（不变量 10）。
4. 平台子账户选择：`pkg/shard.Sub(bizNo, n) (int, error)` = `crc32.ChecksumIEEE([]byte(bizNo)) % uint32(n)`。`n` 由调用方传入（配置 `PLATFORM_SUB_ACCOUNTS`，默认 8）。`n < 1` 或 `bizNo` 为空返回错误。只允许把 `PLATFORM_SUB_ACCOUNTS` 调大是运维规则，函数不记录历史值。调大后重新执行种子开出新子账户；调小会让高位子账户不再被选中，但余额仍计入汇总。
5. 日志、trace 带整数属性 `shard`。

验收：

- [x] `M100001`、`M100003` 路由到 1，`M100002` 路由到 0；非法商户号返回错误。
- [x] 同一 `biz_no` 多次选择的子账户相同；10 万个随机 `biz_no` 在 8 个子账户上的分布偏差 < 5%。
- [x] 停掉一个分片的连接（测试中关闭连接池）后 `/readyz` 返回 503。

#### ACC-01 开户

`P0 · M1 · 依赖：ACC-13、BASE-05 · 表：account`

接口：`AccountService.OpenAccount(owner_type, owner_id, account_type, shard, sub_no)` → `Account`（内部方法，只供种子和测试使用）

规则：

1. `account_type` 与 `owner_type` 的组合必须符合 §2.5.1；`balance_direction`、`allow_overdraft` 由类型决定，调用方不能指定。
2. MERCHANT 账户：`shard` 必须等于 `shard.Of(owner_id)`，`sub_no` 必须为 0，否则 `10001`。PLATFORM 账户：`shard` 只能是 0 或 1，`sub_no` 范围 `[0, PLATFORM_SUB_ACCOUNTS)`。`CHANNEL_RECEIVABLE` 的 `owner_id` 非空且不超过 32 个字符；`FEE_INCOME`、`TRANSIT` 的 `owner_id` 必须是 `PLATFORM`。
3. 按 `uk_owner_account_type_sub` 幂等：已存在则返回已有账户，不把余额和冻结金额改回 0。
4. 新账户余额、冻结均为 0，状态 ACTIVE。币种固定 `CNY`。账户号在事务外用 `idgen.NewNo(ctx, "AC")` 生成。

异常：组合非法、分片或子账户号非法 → `10001`，不写任何行。账户号与另一组 owner 冲突 → 已有的 `10003`，对外消息不带 SQL。

验收：

- [x] 同一组合并发开户 50 次，只产生 1 个账户，50 次都返回同一 `account_no`。
- [x] `MERCHANT + FEE_INCOME` 返回 `10001`；把 `M100002` 开在分片 1 返回 `10001`。

#### ACC-02 账户查询

`P0 · M1 · 依赖：ACC-01 · 表：account`

接口：`AccountService.GetAccount(owner_type, owner_id, account_type)` → `Account{account_no, owner_type, owner_id, account_type, balance, frozen_amount, available_amount, status, sub_count}`

规则：

1. MERCHANT：路由到所属分片读取，`available_amount = balance − frozen_amount`。
2. PLATFORM：读取两个分片的全部子账户并求和，`account_no` 为空，`sub_count` 为参与汇总的行数；用于管理接口和测试，不在交易链路中调用。
3. 只读，不加锁。

异常：不存在 → `40001`。

验收：

- [x] 冻结 300 后查询，`available_amount` 减少 300、`balance` 不变。
- [x] 支付入账若干笔后，平台渠道应收的汇总余额等于各子账户余额之和。

#### ACC-03 通用记账

`P0 · M1 · 依赖：ACC-01、BASE-04 · 表：ledger_txn、ledger_entry、account`

接口：`biz.Post(ctx, tx, biz_type, biz_no, memo, entries[{account_id, direction, amount}])` → `{txn_no, duplicated}`（account 进程内部方法，不对外暴露 RPC；由 ACC-06、ACC-07、ACC-14、MQ-04 调用）

规则：

1. 校验（失败即 `40003`，不发号、不 INSERT）：分录 2–10 条；每条 `amount > 0`；借方合计 = 贷方合计（`money.Add` 分别累加，溢出视为 `40003`）；同一凭证中同一账户只能出现一次；`direction` 只能是 `DEBIT` 或 `CREDIT`；`biz_type` 只能是 `PAY`、`REFUND`、`TRANSFER`、`TRANSFER_OUT`、`TRANSFER_IN`（`REVERSAL` 由 ACC-08 写入，本方法没有 `reversal_of`，直接拒绝）；`biz_no` 非空且不超过 64 个码点；`memo` 不超过 255 个码点，可以为空。Post 在调用方传入的分片事务中执行，以便与冻结单、分支状态的变更同事务提交。Post 自己不 `BEGIN`、`COMMIT`、`ROLLBACK`，也不调用 `dbx.WithTx`。
2. 事务步骤（都在调用方事务内；下面的回滚是调用方收到错误后做的，不是 Post 自己回滚）：

   ```
   -- 调用方已经 BEGIN。纯校验通过后、拿行锁之前 NewNo(ctx, "TX")
   INSERT ledger_txn(txn_no, biz_type, biz_no, amount, entry_count, ...)
     -- 1062：不回滚调用方事务。用 dbx.IsDuplicate 判断后，同一事务按 (biz_type, biz_no)
     -- 读取已有凭证，返回 duplicated = true 和已有 txn_no。参数不同也返回第一次的结果。
     -- SELECT 不到则 10003
   按 account_id 升序 SELECT ... FOR UPDATE
     -- 本连接没有该行（含只存在于另一分片的账户）→ 40003，不是 40001
     -- 币种不是 CNY → 40003；status 不是 ACTIVE → 40005
   按 account_id 升序，对每个账户：
     UPDATE account SET balance = balance ± ?, version = version + 1
      WHERE id = ? AND status = 'ACTIVE'
        [AND balance - frozen_amount >= ?]   -- 仅余额减少且 allow_overdraft = 0
     -- 影响行数 0：重新读取，区分 40005（非 ACTIVE）、40002（余额不足），查不到则 40003
     -- 然后把错误返回给调用方，由调用方回滚
     SELECT balance FROM account WHERE id = ?        -- 已持有行锁，读到本事务更新后的值
   批量 INSERT ledger_entry(... balance_after ...)
   -- 成功由调用方 COMMIT。Post 不提交
   ```

3. 重复请求即使参数不同，也返回首次结果；参数是否一致由调用方的业务层保证（支付、退款的 biz_no 唯一对应金额）。

异常：校验失败、本连接查不到 `account_id`（含只存在于另一分片的账户）、币种不是 CNY，或 `biz_type` 为 `REVERSAL` 及其他非法值 → `40003`（对外英文仍是 `unbalanced entries`）；非 ACTIVE → `40005`；余额不足 → `40002`。Post 不回滚。调用方收到错误后回滚，凭证、分录、余额都不会留下。1062 不是失败，不回滚调用方事务。`GetAccount` 仍然用 `40001` 表示账户不存在。

验收：

- [x] 借贷不平、金额为 0、分录只有 1 条，均返回 `40003`。
- [x] 同一 `(biz_type, biz_no)` 并发提交 100 次，只生成 1 张凭证，余额只变动 1 次，其余 99 次 `duplicated = true`。
- [x] 1000 个 goroutine 对余额 500 的商户账户各扣 1（贷方为平台账户），恰好 500 次成功、500 次 `40002`，最终余额 0，分录数 = 成功次数 × 2（A2）。
- [x] 分录中混入另一分片的账户返回 `40003`。
- [x] 余额不足时，凭证、分录、余额均无变化。
- [x] 每条分录的 `balance_after` 与按顺序回放的结果一致。

#### ACC-04 冻结

`P0 · M2 · 依赖：ACC-03 · 表：account_freeze、account`

接口：`biz.Freeze(ctx, biz_type, biz_no, merchant_id, amount)` → `{freeze_no, status, duplicated}`（进程内方法。IDL 已有 `Freeze`，RPC handler 仍是 ACC-11，本功能点不监听、不改 IDL）

规则：

1. `biz_type` 只允许 `REFUND`（退款，biz_no = refund_no）或 `TRANSFER`（跨分片转账付款方 Try，biz_no = transfer_no）。冻结对象是该商户在 `shard.Of(merchant_id)` 上的 `MERCHANT_SETTLE`。商户号分片不合法、`biz_type` 非法、`biz_no` 为空或超过 64 个码点、金额不是正的 int64 分：`10001`，不写行。只允许冻结 `allow_overdraft = 0` 的账户，否则 `10001`，不写行。
2. `freeze_no` 在事务外用 `NewNo(ctx, "FZ")` 生成。`idgen.ErrClockRollback` 映射为已有的 `10004`。Freeze 自己用 `dbx.WithTx` 提交这一个分片事务（Post 仍由调用方提交，本功能点不改 Post）：

   ```
   INSERT account_freeze(..., status = 'FROZEN')
     -- 1062：只用 dbx.IsDuplicate（MySQL 1062），不解析错误文本。1062 不结束 InnoDB 事务。
     -- 同一事务按 (biz_type, biz_no) 读取。
     -- FROZEN 或 CONFIRMED，且金额、账户一致 → 返回已有 freeze_no 和状态，duplicated = true，不增加 frozen_amount
     -- RELEASED → 40004（防悬挂，见 ACC-05）。不再比较金额或账户
     -- 金额或账户不一致（仅 FROZEN / CONFIRMED）→ 10006，旧行不变
     -- 读不到行 → 10003
   UPDATE account
      SET frozen_amount = frozen_amount + ?, version = version + 1
    WHERE id = ? AND status = 'ACTIVE' AND balance - frozen_amount >= ?
     -- 不修改 balance。version +1 与 Post 改余额对齐，不改变冻结金额。
     -- 影响 1 行才提交。
     -- 影响 0 行：同一事务重读。非 ACTIVE → 40005（优先于余额不足）；可用余额不够 → 40002。
     -- 两者都回滚，不留冻结单。
     -- 重读后仍是 ACTIVE 且可用余额足够：不空转重试，返回 10003。
   ```

3. 分片上没有该商户账户 → `40001`，不写冻结单。nil service、nil ctx、nil db → `10003`。已取消的上下文原样返回，不改写成 `10003`。

异常：余额不足 → `40002`，不留下冻结单；账户不存在 → `40001`；非 ACTIVE → `40005`；已释放 → `40004`；幂等参数冲突 → `10006`。

验收：

- [x] 可用余额 1000，100 个 goroutine 各冻结 20（不同 biz_no），恰好 50 次成功，`frozen_amount = 1000`。
- [x] 同一 biz_no 重复冻结返回同一 `freeze_no`，冻结金额只增加一次。
- [x] 冻结与 ACC-03 扣减混合并发，可用余额始终 ≥ 0。

#### ACC-05 解冻（含空回滚、防悬挂）

`P0 · M2 · 依赖：ACC-04 · 表：account_freeze、account`

接口：`biz.ReleaseFreeze(ctx, biz_type, biz_no, merchant_id, amount)` → `{freeze_no, status, duplicated}`（进程内方法。IDL 已有 `ReleaseFreeze`，RPC handler 仍是 ACC-11，本功能点不监听、不改 IDL。响应契约仍是 `freeze_no`、`status`。`duplicated` 只在进程内，不进 RPC。退款失败、跨分片转账付款方 Cancel 共用）

规则：

1. 校验在进数据库之前，与 Freeze 相同。`biz_type` 只允许 `REFUND` 或 `TRANSFER`。`biz_no` 按码点 1–64。`amount > 0`。`shard.Of(merchant_id)` 必须成功。否则 `10001`，不发号、不写行。参数不合法时，即使上下文已取消，也先返回 `10001`。
2. 参数合法后：nil service、nil ctx、nil 发号器、`ShardDB` 失败 → `10003`。上下文已取消或已过截止时间，原样返回 `context.Canceled` / `context.DeadlineExceeded`，不发号，不改写成 `10003`。
3. `freeze_no` 在事务外 `NewNo(ctx, "FZ")`，只给可能的墓碑用。`idgen.ErrClockRollback` 映射为已有的 `10004`，原因仍可被 `errors.Is` 认出。发号的其他失败映射为 `10003`。`WithTx` 因 1213/1205 重试时沿用同一个单号。ReleaseFreeze 自己用 `dbx.WithTx` 提交本分片事务。不改 Post。事务内不调用 RPC、HTTP、MQ。
4. 事务里按 `(biz_type, biz_no)` 读冻结单。先动冻结单（读到就锁住这行，或插入占住 `uk_biz`），再更新账户。与 Freeze 相同，避免和冻结互相死锁。1062 只用 `dbx.IsDuplicate`，不结束 InnoDB 事务。隔离级别仍是 `WithTx` 的 `READ COMMITTED`。

   - 没有冻结单（空回滚）：查出该商户在 `shard.Of(merchant_id)` 上的 `MERCHANT_SETTLE`、`sub_no = 0`、`CNY`。没有这行 → `40001`，不插入。`allow_overdraft != 0` → `10001`，不插入。有账户则 `INSERT account_freeze`，状态 `RELEASED`，`version = 0`，`txn_no` 留空，`freeze_no` 用事务外生成的号，`amount` 用请求金额，`account_id` 用该结算账户。不修改 `account.balance`，也不修改 `frozen_amount`。插入成功：`duplicated = false`，状态 `RELEASED`。1062 后同一事务再读：读到 `RELEASED` 则幂等成功，`duplicated = true`，返回已有 `freeze_no`，不改账户；读到别的状态按已有行处理；读不到 → `10003`，包住原来的 1062。
   - 已是 `RELEASED`：幂等成功。返回已有 `freeze_no` 和 `RELEASED`，`duplicated = true`。不再比较金额和账户，不改冻结单，不改 `frozen_amount`。
   - 已是 `CONFIRMED`：`40004`。不再比较金额和账户。旧行不变，`frozen_amount` 不变。
   - 已是 `FROZEN`：请求金额必须等于冻结单金额，且 `account_id` 必须是该商户这笔结算账户。否则 `10006`，旧行不变，`frozen_amount` 不变。一致时，同一事务里：`UPDATE account_freeze SET status = 'RELEASED', version = version + 1 WHERE biz_type = ? AND biz_no = ? AND status = 'FROZEN' AND amount = ? AND account_id = ?`。影响 1 行后：`UPDATE account SET frozen_amount = frozen_amount - ?, version = version + 1 WHERE id = ? AND frozen_amount >= ?`。这条账户更新不看账户是不是 `ACTIVE`，也不改 `balance`。停用账户上的冻结也要能解开，不返回 `40005`。账户更新影响 1 行：提交，`duplicated = false`，状态 `RELEASED`，`freeze_no` 用已有的，不用刚生成的那个。账户更新影响 0 行：同一事务重读。行没了，或 `frozen_amount` 仍小于本次金额：`10003`，回滚，冻结单保持 `FROZEN`。不空转重试，不改写成 `40002`。冻结单 CAS 影响 0 行：同一事务重读。变成 `RELEASED` 则幂等成功；变成 `CONFIRMED` 则 `40004`；仍是 `FROZEN` 或读不到 → `10003`。
   - 其他状态：`10003`。

5. 防悬挂不在本方法里再写一遍 Freeze。墓碑占住 `(biz_type, biz_no)` 后，已有的 Freeze 看到 `RELEASED` 会返回 `40004` 且不增加 `frozen_amount`。
6. 不新增错误码。会用到的已有码：`10001`、`10003`、`10004`、`10006`、`40001`、`40004`。`Error()` 不带 SQL、地址、口令。

验收：

- [x] 冻结后解冻，`frozen_amount` 恢复；重复解冻幂等。
- [x] 对不存在的 biz_no 解冻成功并生成墓碑；随后对同一 biz_no 冻结返回 `40004`，账户冻结金额不变。
- [x] 已确认的冻结单解冻返回 `40004`。

#### ACC-06 确认扣款

`P0 · M2 · 依赖：ACC-04 · 表：account_freeze、ledger_txn、ledger_entry、account`

接口：`biz.ConfirmFreeze(ctx, biz_type, biz_no, merchant_id, channel)` → `{freeze_no, txn_no, duplicated}`（进程内方法。IDL 已有 `ConfirmFreeze`，RPC handler 仍是 ACC-11，本功能点不监听、不改 IDL。响应契约仍是 `freeze_no`、`txn_no`。`duplicated` 只在进程内，不进 RPC。请求没有金额，金额 R 只用冻结单上的 `amount`。第一次成功 `duplicated = false`，返回冻结单上已有的 `freeze_no`，不新发 `FZ`。已 `CONFIRMED` 的重放 `duplicated = true`，返回已有 `freeze_no` 和 `txn_no`，不再比较 `channel`，不再记账。）

规则：

1. 校验在进数据库之前，与 Release 一样：先校验，再判 nil。参数不合法时，即使 service 为 nil 或上下文已取消，也先返回已有的 `10001`，不发号、不写行。`biz_type` 只允许已有常量 `REFUND` / `TRANSFER`。`biz_no` 按码点 1–64。`shard.Of(merchant_id)` 失败 → `10001`。`REFUND` 的 `channel` 必须通过已有的渠道所有者校验（非空且不超过 32 个码点）。`TRANSFER` 的 `channel` 必须恰好是空串。其他情况 `10001`。
2. 参数合法后：nil service、nil ctx、nil 发号器、nil pools、`subCount < 1`、`ShardDB` 失败 → `10003`。`subCount < 1` 是服务配置问题，不是 `10001`。上下文已取消或已过截止时间，原样返回，不进事务，不改写成 `10003`。
3. ConfirmFreeze 自己不调用 `NewNo`。`TX` 号仍由记账在调用方事务内、账户行锁之前生成。本方法没有单独的时钟回拨路径；发号错误沿用记账的映射（时钟回拨仍是已有的 `10004`），不要再包一层。事务由 ConfirmFreeze 自己用 `dbx.WithTx` 提交该商户分片。记账仍然不 `BEGIN` / `COMMIT` / `ROLLBACK`，也不调用 `WithTx`。事务内不调用 RPC、HTTP、MQ。
4. 锁顺序：先 `SELECT ... FOR UPDATE` 锁冻结单，再按账户 id 升序锁账户。不要先改商户账户再锁更小 id 的平台账户，否则会和普通记账死锁。不要插入墓碑。确认用自己的查询，多读 `txn_no`，不改解冻用的冻结单列清单。
5. 冻结单分支：
   - 没有行，或状态 `RELEASED`：`40004`。不插入、不记账、不改余额、不改冻结金额。墓碑不能被确认成真实扣款。
   - `CONFIRMED`：`txn_no` 非空则幂等成功，`duplicated = true`，不记账，不再比较 `channel`。`txn_no` 为 NULL 或空串 → `10003`，不写。
   - 其他未知状态 → `10003`，不写。
   - `FROZEN`：用已有商户结算账户查询（`MERCHANT_SETTLE`、`sub_no = 0`、`CNY`、本分片）。没找到，或 `account_id` 不是这张结算账户 → `10006`，不写。不要为了“找不到商户”返回 `40001`。该结算账户 `allow_overdraft != 0` → `10001`，不写。子账户 `k = shard.Sub(biz_no, subCount)`，失败 → `10003`，不写。平台贷方只在本分片查：`REFUND` 为 `PLATFORM` + `owner_id = channel` + `CHANNEL_RECEIVABLE` + `CNY` + `sub_no = k`；`TRANSFER` 为 `PLATFORM` + `owner_id = PLATFORM` + `TRANSIT` + `CNY` + `sub_no = k`。查不到这行 → `40001`，不调用记账，不改冻结单。不要把“按所有者查不到”写成记账的 `40003`。平台账户 id 与商户账户 id 相同 → `10003`。然后在同一事务记账。公开的 `Post` 不加参数。
   - 记账成功且 `duplicated = false` 之后再 CAS：`UPDATE account_freeze SET status = 'CONFIRMED', txn_no = ?, version = version + 1 WHERE biz_type = ? AND biz_no = ? AND status = 'FROZEN' AND amount = ? AND account_id = ?`。影响 1 行：提交，`duplicated = false`，返回已有 `freeze_no` 和这次的 `txn_no`。影响 0 行：这行已被 `FOR UPDATE` 锁住，WHERE 没对上，返回 `10003` 让事务回滚。不要当成幂等成功，否则会把刚插入的凭证一起提交。不要空转重试。
   - 记账返回 `duplicated = true`（冻结单仍是 `FROZEN`，但凭证已存在）：返回 `10003`，不要 CAS，不要减冻结。本次事务回滚；已经提交的旧凭证不动。
   - 记账返回的业务错误原样交给事务回滚，冻结单保持 `FROZEN`。
6. 商户腿不能用普通记账的借方条件。普通借方是 `status = 'ACTIVE' AND balance - frozen_amount >= ?`，而且不改 `frozen_amount`。可用余额为 0 的全额冻结会被它拒绝，也不能把冻结减掉。公开 `Post` 行为不变：普通借方仍走原来的可用余额条件和 `ACTIVE` 检查，不改 `frozen_amount`。
7. 只有确认扣款设置未导出选项：被冻结的商户账户 id。为 0 时路径与现在完全一样。该账户在同一次记账里、账户已经按 id 升序锁住之后，用 `UPDATE account SET balance = balance + ?, frozen_amount = frozen_amount - ?, version = version + 1 WHERE id = ? AND frozen_amount >= ?` 代替普通借方更新。`balance` 的增量是这笔分录的有符号 delta（商户结算账户是 `CREDIT`，借方分录使余额减少，delta 为负）。冻结减去的是分录金额 R（正数）。不要求 `ACTIVE`。停用（账户 status 为 `FROZEN` 或 `CLOSED`）的商户仍然确认，商户腿不返回 `40005`。不使用可用余额条件，不返回 `40002`。影响 0 行：同一事务重读。行没了，或 `frozen_amount` 仍小于 R → `10003`。不空转，不改写成 `40002` 或 `40005`。MySQL CHECK 3819 若因假数据触发，变成 `10003`，不是 `40002`。这条更新只加一次 `version`。平台账户仍走原来的余额更新，`version` 再加 1。
8. 平台账户仍走现有记账规则：币种不是 `CNY` → `40003`；不是 `ACTIVE` → `40005`，整个事务回滚，冻结单保持 `FROZEN`。渠道应收和在途都允许透支，贷方不会因为余额不足失败。
9. 分录两条，金额都是 R。借：商户结算账户，`direction = DEBIT`。贷：上面选出的平台子账户，`direction = CREDIT`。`REFUND` 凭证的 `biz_type = REFUND`，`biz_no` 为冻结单的 `biz_no`。渠道应收是 `DEBIT` 方向账户，贷记使其 `balance` 减少。`TRANSFER` 凭证的 `biz_type = TRANSFER_OUT`，`biz_no` 为冻结单的 `biz_no`。冻结单自己的 `biz_type` 仍是 `TRANSFER`。两张表的唯一键互不占用。在途是 `CREDIT` 方向账户，贷记使其 `balance` 增加。`memo` 为空串。`balance_after` 仍由记账在更新后读到的 `balance` 写入。不改分录，只追加。商户余额和冻结同减 R，可用余额不变，`ck_account_available` 仍成立。
10. 不新增错误码。会用到的已有码：`10001`、`10003`、`10004`、`10006`、`40001`、`40003`、`40004`、`40005`。确认失败不返回 `40002`。`Error()` 不带 SQL、地址、口令。

验收：

- [x] 余额 9940、冻结 3000，确认后余额 6940、冻结 0，渠道应收减少 3000。
- [x] `TRANSFER` 冻结确认后，商户余额与冻结各减 T，本分片在途增加 T，凭证类型为 `TRANSFER_OUT`。
- [x] 重复确认只记一次账。
- [x] 对 RELEASED 冻结单确认返回 `40004`，无任何写入。

#### ACC-07 同分片转账（含失败墓碑）

`P0 · M2 · 依赖：ACC-03 · 表：transfer_branch、ledger_txn、ledger_entry、account`

接口：进程内 `TransferLocal(ctx, transfer_no, payer_merchant_id, payee_merchant_id, amount)` → `{status, txn_no, fail_code}`。RPC handler 仍是 ACC-11，本功能点不改 IDL。不增加进程内 `duplicated` 字段。

说明：同分片转账由 account 在本分片本地事务内完成。难点是结果必须确定：调用方超时重试时，第一次请求可能仍在执行，不能出现“第一次因余额不足失败、重试却成功”或反过来的情况。`transfer_branch(role = LOCAL)` 记录每笔转账的最终结果，重复请求只返回已有结果。

规则：

1. 本方法自己用 `dbx.WithTx` 提交本分片事务。`Post` 仍不 `BEGIN` / `COMMIT` / `ROLLBACK`。不选平台子账户，不因 `subCount < 1` 失败。凭证 `biz_type` 是 `TRANSFER`，不是 `TRANSFER_OUT` 或 `TRANSFER_IN`。不写冻结单，不写 `PAYEE` 角色。
2. 参数校验在 nil service / nil ctx 判断之前，与 ReleaseFreeze、ConfirmFreeze 相同。不合法返回已有的 `10001`，不写行；即使 service 为 nil 或上下文已取消也先返回 `10001`。`transfer_no` 为 1–32 个码点（列是 `VARCHAR(32)`），不沿用记账 `biz_no` 的 64，也不强制 `TF` 前缀。`amount` 必须是正的 int64 分。付款方与收款方都要能被 `shard.Of` 解析，必须同一分片，且不是同一商户。分片不同、转给自己、商户号非法、金额不是正数、单号长度非法，都是 `10001`。
3. 参数合法之后：nil service、nil ctx、nil 发号器、nil pools 为 `10003`。已取消或已过截止时间的上下文原样返回，不启事务。`ShardDB` 失败为 `10003`。
4. 事务前用普通 `SELECT` 查双方商户结算账户：`owner_type = MERCHANT`，`owner_id` 为该商户，`account_type = MERCHANT_SETTLE`，`currency = CNY`，`sub_no = 0`。任一不存在（含非 CNY 因而选不中）返回 `40001`。两个都存在时，任一 `status` 不是 `ACTIVE` 返回 `40005`。不存在优先于非 ACTIVE。这两个 `SELECT` 不加行锁，不在这里比较余额。两个账户 id 相同是数据损坏，返回 `10003`，不写。
5. 事务 1（本分片 `WithTx`）：

   ```
   BEGIN
     INSERT transfer_branch(transfer_no, role = 'LOCAL', status = 'SUCCESS',
            双方商户, amount, fail_code NULL, txn_no NULL, version 0)
       -- 1062：MySQL 1062 不会中止 InnoDB 事务。fn 返回错误让 WithTx 回滚，
       -- 然后在事务外读取已有 LOCAL 分支，按第 7 条返回。不要在未回滚时继续记账。
     Post(TRANSFER, transfer_no, memo 空串, 借付款商户 T，贷收款商户 T)
       -- 公开 Post，FrozenAccountID 保持 0，不走确认扣款的冻结同减路径。
       -- 可用余额条件是 balance - frozen_amount。账户按 id 升序加锁是 Post 已有行为，
       -- 不要在 Post 之前用相反顺序锁账户。
     UPDATE transfer_branch SET txn_no = ?, version = version + 1
      WHERE transfer_no = ? AND role = 'LOCAL' AND status = 'SUCCESS' AND txn_no IS NULL
        AND amount = ? AND payer_merchant_id = ? AND payee_merchant_id = ?
   COMMIT
   ```

   `Post` 返回 `error == nil` 时，无论 `Duplicated` 是 true 还是 false，都回写 `PostResult.TxnNo`。`Duplicated` true 表示这张 `TRANSFER` 凭证已经存在、这次没有改余额；按本规则仍把分支提交为 SUCCESS。这与 ACC-06“凭证已存在则 `10003`”不同，不要照搬。回写影响 1 行则让 `WithTx` 提交。影响 0 行返回 `10003`，由 `WithTx` 回滚，不能留下没有 `txn_no` 的 SUCCESS。
6. 只有 `Post` 返回 `errno.ErrInsufficientBalance`（`40002`）才在事务 1 回滚之后开事务 2，`INSERT` LOCAL FAILED 墓碑，`fail_code` 恰好是 `INSUFFICIENT_BALANCE`，`txn_no` NULL。事务 2 成功则返回 FAILED 业务结果，error 为 nil。事务 2 遇到 1062：回滚后读取已有分支，按第 7 条返回。事务 2 的其他错误走 `internalErr`，返回 `10003`，不把失败墓碑写成别的状态。`Post` 的其他错误（`40005`、`40003`、`10003`、`10004`、上下文错误）只回滚事务 1，不写墓碑，把该错误返回给调用方。事务前已经查过 ACTIVE，锁内才发现非 ACTIVE 仍然是 `40005`。冻结占住的钱不能转出。不要把 0 行余额更新改写成别的码，沿用 `Post` 的 classify。
7. 已有 LOCAL 分支：比较 `amount`、`payer_merchant_id`、`payee_merchant_id`。任一不一致返回 `10006`，不写、不记账。一致时：SUCCESS 且 `txn_no` 非空，返回 SUCCESS 和该 `txn_no`，不再记账。SUCCESS 但 `txn_no` 为空返回 `10003`，不写。FAILED 返回 FAILED 和库里的 `fail_code`，不再看当前余额，没有凭证。读不到行、未知 status、FAILED 却没有 `fail_code`，返回 `10003`。不要空转重试。1062 之后读不到行也是 `10003`。幂等重放不再比较之外的字段，但金额或双方不一致必须 `10006`。收款方在另一分片时根本进不了这个分片的行，直接 `10001`。
8. 余额不足返回业务结果 `status = FAILED`，不返回 `40002` 错误码，便于调用方区分“确定失败”和“调用异常”。成功扣款时 error 为 nil 且 `status = SUCCESS`、`txn_no` 非空、`fail_code` 空。

异常：参数不合法 → `10001`；账户不存在 → `40001`；非 ACTIVE → `40005`；幂等冲突 → `10006`。错误码只用 `pkg/errno` 里已有的值。对外 `Error()` 不得含 SQL、地址、密码。`internalErr` 保留已知 `*errno.Error` 和上下文取消或截止。CHECK 3819 等未知数据库错误包成 `10003`。

验收：

- [x] A、B 各 10000，A→B 与 B→A 各 500 次并发转 1，无死锁报错（死锁重试后成功也可），结束后 A + B = 20000 且 `CheckBalance` 通过（A2）。ACC-10 未做，按 §0.3 用 SQL 断言代替 `CheckBalance`，不实现、不调用核对函数。
- [x] 同一 `transfer_no` 并发 50 次，只生成 1 张凭证，全部返回同一结果。
- [x] 余额不足失败后，给付款方入账使余额充足，再用同一 `transfer_no` 请求仍返回 FAILED，没有凭证。
- [x] 双方不在同一分片、转给自己，返回 `10001`。

#### ACC-14 收款方转账分支（Try / Confirm / Cancel）

`P0 · M2 · 依赖：ACC-03 · 表：transfer_branch、ledger_txn、ledger_entry、account`

接口（均以收款商户所属分片执行。三个方法都是进程内方法，各自用 `dbx.WithTx` 提交该分片事务。RPC 仍是 ACC-11）：

- `TransferInTry(ctx, transfer_no, payee_merchant_id, amount)` → `{status}`。成功时 `txn_no` 为空。
- `TransferInConfirm(ctx, transfer_no, payee_merchant_id)` → `{status, txn_no}`。成功时 `txn_no` 非空。
- `TransferInCancel(ctx, transfer_no, payee_merchant_id, amount)` → `{status}`。成功时 `txn_no` 为空。

`error` 为 nil 才表示业务成功。返回错误时 `status` 和 `txn_no` 都是空字符串。没有进程内 `duplicated` 字段。

说明：收款方 Try 不动余额，只占位并确认收款账户可用，避免 Confirm 阶段才发现收款账户异常、资金滞留在途。分支记录同时承担空回滚与防悬挂。

规则：

1. 参数校验在 nil service / nil ctx 之前。不合法返回 `10001`，不写行。即使 service 为 nil 或 ctx 已取消也如此。`transfer_no` 为 1–32 个码点，不强制 `TF` 前缀。Try 和 Cancel 的 `amount` 必须是正的 int64 分。收款商户必须能被 `shard.Of` 解析，分片由收款商户决定。Confirm 没有金额。
2. 参数合法之后：nil service、nil ctx、nil pools 为 `10003`。已取消或已过截止时间的 ctx 原样返回，不启事务。ShardDB 失败为 `10003`。Try 和 Cancel 不选平台子账户，不因为 `subCount < 1` 或发号器为空失败。Confirm 在开事务前要求发号器非空且 `subCount >= 1`，否则 `10003`，因为 Post 要发 `TX` 号。子账户号用 `shard.Sub(transfer_no, subCount)`，不在业务里另算 crc32。`shard.Sub` 失败是 `10003`，不写。
3. 商户结算账户：`owner_type = MERCHANT`，`owner_id` 为收款商户，`account_type = MERCHANT_SETTLE`，`currency = CNY`，`sub_no = 0`。不加行锁，不读余额。Try：不存在（含非 CNY 选不中）为 `40001`；存在但不是 `ACTIVE` 为 `40005`。不写行。账户检查在插入之前，所以账户还不存在时看不到墓碑，返回 `40001`；账户后来建好且为 `ACTIVE`，再对 CANCELLED 调用 Try 才是 `40004`。Cancel 不查账户。没有行时直接插入 CANCELLED 墓碑，账户不存在也写。这是空回滚。
4. PAYEE 行：`role = PAYEE`，`payer_merchant_id` 为 SQL NULL，`fail_code` 为 SQL NULL，不写空字符串。同一 `transfer_no` 的 LOCAL 行不冲突，不读不改 LOCAL。
5. Try：账户通过后，事务里 INSERT `status = TRIED`，金额为请求金额，`txn_no` 为 NULL，`version` 为 0。不动余额，不调用 Post。1062 不中止 InnoDB。返回错误让 WithTx 回滚，再在事务外读已有 PAYEE 行。CANCELLED：返回 `40004`，不比较金额和收款商户，不插入。TRIED 或 CONFIRMED：金额或 `payee_merchant_id` 不一致为 `10006`，不写；一致则返回库里的 status，不记账。CONFIRMED 的 Try 不返回 `txn_no`。读不到行、未知 status 为 `10003`，不循环。
6. Confirm 在一个 WithTx 里：先 `SELECT ... FOR UPDATE` 锁 PAYEE 行，再按账户 id 升序锁账户（这是 Post 已有行为，不在 Post 之前用相反顺序锁）。没有行或 CANCELLED：`40004`，不插入，不记账。CONFIRMED 且 `txn_no` 非空：返回该 `txn_no` 和 status `CONFIRMED`，不再比较收款商户，不再记账。CONFIRMED 但 `txn_no` 为 SQL NULL 或空：`10003`，不写。未知 status：`10003`。TRIED 但收款商户不是本次请求：`10006`，不记账。TRIED 且收款商户一致：金额只用行上的 `amount`。查本分片 TRANSIT：`owner_type = PLATFORM`，`owner_id = PLATFORM`，`account_type = TRANSIT`，`currency = CNY`，`sub_no = shard.Sub(transfer_no, subCount)`。没有该行：`40001`，不调用 Post，不改分支，事务回滚后仍是 TRIED。商户结算账户此时不存在：`40001`，同样保持 TRIED。两个账户 id 相同，或任一 id 为 0：`10003`。然后公开 `Post(ctx, tx, TRANSFER_IN, transfer_no, memo 空串)`：借在途、贷收款商户。`FrozenAccountID` 保持 0。在途允许透支，正常行不会因为在途余额不足返回 `40002`。不把 `40002` 改成别的码。若库里的在途被改成不许透支并且不够扣，Post 仍返回 `40002`，本方法原样返回，回滚，分支保持 TRIED，不写墓碑。Post 返回 `40005`（商户或在途不是 ACTIVE）原样返回，回滚，保持 TRIED。这是严重不一致，本方法只返回 `40005`，不自己加 `pay_transfer_inconsistent_total`（指标仍由 TRF-03 负责）。Post 的 error 为 nil 时，无论 `Duplicated` 是 true 还是 false，都回写 `PostResult.TxnNo`。`Duplicated` true 表示这张 `TRANSFER_IN` 凭证已在、这次没改余额；仍把分支提交为 CONFIRMED。这和 ACC-06 不同。回写用一条 UPDATE：`status = CONFIRMED`，`txn_no` 为返回的号，`version = version + 1`，WHERE `role = PAYEE AND status = TRIED AND txn_no IS NULL` 且 amount 与 payee 等于刚读到的值。影响 1 行则提交。影响 0 行：`10003` 并回滚。行上有 FOR UPDATE，0 行不是并发幂等。Post 返回空字符串凭证号：`10003`，回滚。
7. Cancel 在一个 WithTx 里：先 `SELECT ... FOR UPDATE`。没有行：INSERT CANCELLED，金额用请求金额，payee 用请求商户，payer 为 NULL，fail_code 为 NULL，txn_no 为 NULL，version 为 0。不查账户，不改余额。1062 则回滚后按已有行处理。已 CANCELLED：幂等成功，不再比较金额和收款商户。已 CONFIRMED：`40004`，不再比较金额。TRIED 且金额或 payee 不一致：`10006`，不改行。TRIED 且一致：CAS 为 CANCELLED，`version + 1`，WHERE `status = TRIED` 且 amount 与 payee 匹配。余额不变，不调用 Post。0 行在持有 FOR UPDATE 时是 `10003` 并回滚。未知 status：`10003`。1062 之后若读到匹配的 TRIED，再开一个事务锁住后 CAS。第二次仍是 1062 则为 `10003`，不空转。
8. 错误只用 `pkg/errno` 已有值。内部错误保留已知 errno 和上下文取消或截止。CHECK 3819 收成 `10003`。`Error()` 不含 SQL、地址、密码。

异常：Try 账户不存在 → `40001`，非 ACTIVE → `40005`。Confirm 的在途或商户行不存在 → `40001`，Post 返回的 `40005` / `40002` 原样返回。分支状态冲突 → `40004`。幂等冲突 → `10006`。Cancel 的空回滚和已取消不查账户，所以没有 `40001` / `40005`。

验收：

- [x] Try → Confirm：收款商户余额 +T，本分片对应 TRANSIT 子账户余额 −T（在途允许透支，起始可以是 0，确认后为 −T）。凭证恰好 1 张，`biz_type = TRANSFER_IN`。重复 Confirm 返回同一 `txn_no`，仍是 1 张凭证。另一分片没有这次的分支和凭证。ACC-10 未做，CheckBalance 用 SQL 断言代替，不调用核对函数。
- [x] Try → Cancel：商户和在途余额不变，分支 CANCELLED。重复 Cancel 成功，仍是一行。CheckBalance 用 SQL 断言代替。
- [x] 没有行时先 Cancel，生成 CANCELLED 墓碑（不要求账户事先存在）。随后 Try 返回 `40004`，Confirm 返回 `40004`。没有凭证，余额不变。CheckBalance 用 SQL 断言代替。
- [x] 已 CONFIRMED 再 Cancel 返回 `40004`，分支仍是 CONFIRMED，凭证仍是 1 张，余额不再变。CheckBalance 用 SQL 断言代替。
- [x] 同一 `transfer_no`，Try 与 Cancel 各 20 次并发：结束时分支不是 CONFIRMED，没有 `TRANSFER_IN` 凭证，余额不变，最终是 CANCELLED。墓碑之后的 Try 返回 `40004`。CheckBalance 用 SQL 断言代替。

#### ACC-08 冲正

`P1 · M8 · 依赖：ACC-03 · 表：同 ACC-03`

接口：`AccountService.Reverse(shard, txn_no, reason)` → `{reversal_txn_no, duplicated}`（内部方法，P1；本项目的业务流程不依赖冲正；凭证号不编码分片，调用方须指定原凭证所在分片）

规则：

1. 生成新凭证：`biz_type = REVERSAL`、`biz_no = 原 txn_no`、`reversal_of = 原 txn_no`，分录与原凭证方向相反、金额相同。
2. 原凭证不变；同一凭证只能冲正一次（`uk_biz` 与 `uk_reversal_of`）。
3. 不允许冲正 REVERSAL 凭证。
4. 冲正同样受可用余额约束，例如冲正一笔给商户的入账时，商户可用余额不足则返回 `40002`。

验收：

- [ ] 冲正后各账户余额恢复到原凭证之前，分录数增加原凭证分录数。
- [ ] 重复冲正返回同一冲正凭证。
- [ ] 冲正 REVERSAL 凭证返回 `10001`。

#### ACC-09 流水查询

`CUT`：没有调用方（管理接口已精简，商户接口不提供流水）。排查时直接查询 `ledger_entry`。

#### ACC-10 余额核对（含分片与在途守恒）

`P0 · M2 · 依赖：ACC-03、ACC-04、ACC-14 · 表：account、ledger_entry、ledger_txn、account_freeze、transfer_branch`

接口：`AccountService.CheckBalance(merchant_id | all, expect_quiescent)` → `{passed, failures[{shard, account_no, check, expected, actual}]}`。ACC-10 交付进程内 `biz.CheckBalance`，不监听 RPC，不实现 `POST /admin/accounts/check`。

规则：

1. 签名是 `CheckBalance(ctx, merchantID, expectQuiescent)`。`merchantID` 为空是全量模式，不是错误。非空商户号必须能被 `shard.Of` 解析，否则 `10001`。这个校验在 nil service、nil ctx 和已取消的 ctx 之前；`10001` 不查询，`failures` 为空。商户模式忽略 `expectQuiescent`，不因此返回 `10001`。
2. 参数合法后，nil service、nil ctx、nil pools 为 `10003`。已取消或已过期的 ctx 原样返回，不开事务。`ShardDB` 失败、任一分片失败或 SQL 错误都是整次 `10003`，`failures` 为空，不返回部分失败。`Error()` 不带 SQL、地址或密码。未知 `balance_direction` 是数据损坏，整次 `10003`，不记成一条 failure。金额求和用 `money.Add` / `money.Sub`，溢出是 `10003`，不回绕。
3. `40001` 只表示点名的商户结算账户不存在：商户模式，`MERCHANT` / `MERCHANT_SETTLE` / `CNY` / `sub_no = 0`。非 CNY 行选不中算不存在。此时 `failures` 为空，计数不加。全量模式没有点名商户，不因某商户缺失返回 `40001`。空库全量通过。
4. `passed = false` 且 `failures` 非空时 error 为 nil，不是 `40001`，也不是 `10003`。此时进程内无标签计数 `pay_ledger_check_failures_total` 加 1，一次调用只加一次，并打一条 ERROR 日志。成功、`40001`、`10003` 都不加计数、不打这条日志。`/metrics` 暴露属于 OPS-03，本功能点不引入 Prometheus 客户端。
5. 本方法不写 `ledger_*` 或 `account` 行，不调用外部系统。重复调用不记账、不冻结、不通知。不要在写事务里调用它，它自己也不包进别的业务事务。不用 `dbx.WithTx`（READ COMMITTED，且驱动的 `BeginTx(ReadOnly)` 没有 `WITH CONSISTENT SNAPSHOT`）。每个分片用一条固定连接，按顺序执行 `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`、`START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT`、只读查询、`ROLLBACK`（成功也回滚）。两个分片是两次独立快照，不是同一时刻。查询都带调用方 ctx。全量先分片 0 再分片 1。
6. 返回全部失败，不在第一个停止。顺序：分片 0 然后分片 1；分片内账户按 `account.id` 升序；每个账户依次是 `BALANCE_SUM`、`LAST_BALANCE_AFTER`、`FROZEN_SUM`、`AVAILABLE_NON_NEGATIVE`；然后该分片的 `TXN_BALANCED`（凭证按 `ledger_txn.id` 升序）、`GLOBAL_BALANCED`、`TRANSIT_EXPECTED`。`TRANSIT_ZERO` 最后，且只在全量且 `expectQuiescent` 时做。商户模式只查该商户这一个结算账户的前四项。
7. 公式（金额是 int64 分）：
   - `BALANCE_SUM`：CREDIT 期望 = 贷方合计 − 借方合计；DEBIT 期望 = 借方合计 − 贷方合计；与 `account.balance` 比较。Actual 是账面余额。无分录时期望 0。
   - `LAST_BALANCE_AFTER`：该账户 `max(ledger_entry.id)` 的 `balance_after` 等于当前余额，不用 `created_at`。无分录且余额 0 通过。无分录且余额非 0：expected 0，actual 为余额。
   - `FROZEN_SUM`：`frozen_amount` = 该账户 `status = FROZEN` 的 `account_freeze.amount` 合计。RELEASED 和 CONFIRMED 不计。没有冻结时期望 0。
   - `AVAILABLE_NON_NEGATIVE`：仅 `allow_overdraft = 0`。可用 = `balance - frozen_amount`，必须 ≥ 0。允许透支（含 TRANSIT）即使为负也跳过。失败时 expected 0，actual 为负数的可用余额。
   - `TXN_BALANCED`（全量，`account_no` 空，shard 有值）：每张凭证借方合计 = 贷方合计，分录行数 = `entry_count`，且每条分录的 `account_id` 都在本分片 `account` 表。同一凭证可以因不同原因各有一行。`entry_count` 与行数一致即可，不额外要求至少 2 条。
   - `GLOBAL_BALANCED`（全量，`account_no` 空）：该分片全部分录借方总额 ≠ 贷方总额时一行。expected 是借方总额，actual 是贷方总额。
   - `TRANSIT_EXPECTED`（全量，`account_no` 空，每个失败分片一行）：expected = 本分片 `account_freeze` 中 `biz_type = TRANSFER` 且 `status = CONFIRMED` 的 amount 合计 − 本分片 `transfer_branch` 中 `role = PAYEE` 且 `status = CONFIRMED` 的 amount 合计。actual = 本分片全部 TRANSIT 账户（`PLATFORM` / `PLATFORM` / `TRANSIT` / `CNY`，所有 `sub_no`）的 balance 合计。LOCAL 成功分支和 REFUND 冻结不进公式。不拿两个分片互相比较，也不要求这个数是 0。
   - `TRANSIT_ZERO`（仅全量且 `expectQuiescent`）：expected 0，actual = 分片 0 的 TRANSIT 余额合计 + 分片 1 的 TRANSIT 余额合计。`shard` 与 `account_no` 都省略。`expectQuiescent = false` 时不要把两个分片的 TRANSIT 加总去和 0 比。

验收：

- [x] 正常数据核对通过。
- [x] 测试中直接用 SQL 篡改余额 1 分，核对失败并指出分片、账户和差额。
- [x] 跨分片转账只完成付款方 Confirm 时核对通过（在途按分支状态计算）；篡改一条 PAYEE 分支状态后核对失败。
- [x] 在并发记账和并发转账进行时执行全量核对，不出现误报。

#### ACC-11 account RPC 服务

`P0 · M2 · 依赖：ACC-01~07、ACC-10、ACC-14、BASE-12`

规则：

1. Kitex 服务端暴露 `docs/05-api.md` §10.1 中 AccountService 的全部方法。handler 只做协议转换和错误码转换，不改 biz 的幂等与 CAS，也不在事务里调用外部系统。`duplicated` 不进 RPC。平台汇总的 `account_no` 为空，`shard`、`sub_no` 在 biz 里是 0，表示省略，不是分片 0；映射时留空，不写成指向 0 的指针。有账户号的结果即使分片或子账户是 0，也要传指针。
2. 服务端中间件：panic 恢复（调用方收到 `10003`，panic 值不进日志和响应，只打一条不含值的 ERROR）、访问日志（字段同约定 §7.5，不记录请求体）、错误码转换。未识别错误转成 `10003` 时，原因用 `slog` 打一条 ERROR（`path` 为方法名，`err_code` 为码，`error` 为原因），不进对外 msg；登记过的业务错误不打这条 ERROR。trace 与指标只留接口和空实现，分别由 OPS-02、OPS-03 填入。本功能点不引入 OTel SDK 和 Prometheus 客户端，不连接 `OTEL_EXPORTER_OTLP_ENDPOINT`。
3. 服务端处理超时按方法区分：`CheckBalance` 全量模式（`merchant_id` 为空）120 秒，与 §5.4.1 网关对 `CheckBalance` 的 120 秒调用上限一致；`CheckBalance` 商户模式（单个商户账户）和其他全部方法 2 秒。进入 handler 的 ctx 截止时间取上述值，调用方 ctx 更短时先到期的生效。biz 返回的截止时间或取消转换成 Kitex 超时，不是 `10003`，这样幂等调用方能按第 4 条重试。不强制杀掉不看 ctx 的 goroutine。这是服务端上限，不改 §5.4.1 的客户端超时。（2026-10-27 修订：原文为全部方法 2 秒，全量核对超过 2 秒会被服务端取消，与网关 120 秒不符。已实现代码仍是统一 2 秒，待 fix/ACC-11-… 落地；本项状态与已勾验收不变。）
4. payment 侧客户端在 `pkg/accountclient`，只依赖 `kitex_gen` 和 `pkg`，不依赖 `service/account`。连接 100ms、调用 500ms；只对幂等方法在超时时重试 1 次：Freeze、ReleaseFreeze、ConfirmFreeze、TransferLocal、TransferInTry、TransferInConfirm、TransferInCancel、GetAccount。`OpenAccount` 即使 biz 幂等也不重试。`CheckBalance`、`Reverse` 不重试。业务错误不重试。调用方 ctx 已取消不重试。连接拒绝不是超时，不重试。gateway 不使用这个客户端（网关不重试，超时见 §5.4.1，GW-01 自己建客户端）。
5. 业务错误经 Kitex `BizStatusError` 传递，传输用 TTHeader。服务端和 payment 客户端都挂上 TTHeader meta handler。没有它，调用方会把业务错误看成成功的空响应。GW-01 建 account 客户端时必须同样启用。AccountService 没有携带对象的错误，不在响应里加 `biz_code`。约定 §5.3 第 5 条的 `biz_code` 出口在 `errno.RPCExit`，给以后的 payment handler 用。`TransferLocal` 余额不足仍是成功响应里的 `fail_code`，不是 `40002`，也不是 `biz_code`。
6. 健康检查、`/readyz`、SIGTERM 的 10 秒排空属于 OPS-01。本功能点启动 Kitex 并阻塞到框架收到 SIGINT/SIGTERM，然后关闭连接池。不实现 `/healthz`、`/readyz`，也不按 OPS-01 的顺序排空在途请求。不启动 Kafka 消费者（MQ-04）。监听地址固定 `:8882`，不新增环境变量。管理 HTTP `POST /admin/accounts/check` 仍是 GW-07，本功能点只提供 RPC。
7. `Reverse` 的方法存在，冲正业务是 ACC-08。在 ACC-08 完成前，任何调用返回 `10003`，不写行，不调用 biz。这不是 ACC-08 将来的 `10001` / `40001` / `40002`。

验收：

- [x] 通过 Kitex 客户端调用全部方法，错误码正确透传。
- [x] handler 中 panic 不导致进程退出，调用方收到 `10003`。

#### ACC-12 热点账户优化

`CUT`：子账户拆分改为 OPT-03（P0），缓冲入账改为 OPT-05（P2）。

### 4.3 模拟渠道（CH）

channel_mock 代表外部支付机构，拥有独立 schema `pay_channel_mock`，平台服务不得直接访问其数据库。接口规范见 `docs/05-api.md` §9。所有请求与回调使用 BASE-13 的渠道签名。

#### CH-01 渠道支付受理与自动完成

`P0 · M3 · 依赖：BASE-13、BASE-03 · 表：mock_txn`

接口：`POST /mockpay/v1/pay`

规则：

1. 验签失败返回 HTTP 401：缺 `X-Mock-Timestamp` 或 `X-Mock-Signature`、时间戳不是十进制整数、签名不符、时间戳与本地时钟相差超过 300 秒（按整秒比较：本地时间取 Unix 秒再相减，±300 秒边界内接受）。签名通过但参数非法返回 HTTP 400：body 不是合法 UTF-8、不是单个 JSON 对象、顶层有重复键（按转义还原后的键名，未知字段也算）、已知字段名大小写不对（只认全小写，如 `Amount`、`REQ_NO` 拒绝）、字段类型不符（如小数或字符串金额）、`req_no` 为空或超过 32 个字符或含 U+FFFD、`amount < 1`、`subject` 为空、`notify_url` 为空或超过 512 个字符或含 U+FFFD 或不是 http / https 绝对地址。JSON 里的孤立代理转义（如 `\udc00`）解码后是 U+FFFD，因此 `req_no`、`notify_url` 里的这类转义也返回 400。其余未知字段忽略。400、401、500 的 body 为空、不签名，也不写 `mock_txn`；200 的响应按 BASE-13 渠道签名，时间戳取渠道当前时间。
2. 按 `(PAY, req_no)` 幂等：先插入，唯一键 `uk_type_req_no` 冲突再读已有行，不做先查后插。已有行按以下顺序判断：已是 CLOSED → `REJECTED / ORDER_CLOSED`（不论金额）；金额一致 → `OK`，返回已有 `channel_txn_no` 和当前 `status`；金额不一致 → `REJECTED / PARAM_CONFLICT`。`REJECTED` 不带 `channel_txn_no`，`status` 为已有行的当前状态。重复受理不改原行，也不重复调度。插入报唯一键冲突以外的错误（提交结果不确定、请求被取消等）时，行可能已经提交：用不随请求取消的上下文、1 秒超时按 `(PAY, req_no)` 再读一次，读到本次写入的行（`channel_txn_no` 与本次生成的相同）就按新受理处理，调度并返回 `OK`（按该行回答，通常是 `PROCESSING`）；读不到、读失败或读到别的请求写入的行，返回 HTTP 500。
3. 新交易状态 PROCESSING，`scenario` 由金额末两位决定（CH-06），返回 `channel_txn_no`（`MC` 前缀）。`created_at` 由渠道按自己的时钟写入（不用列默认值），截到毫秒。
4. 自动完成：延迟 `delay = MOCK_DELAY_MIN_MS + hash(req_no) % (MOCK_DELAY_MAX_MS − MOCK_DELAY_MIN_MS)`，`hash` 为 `req_no` 的 CRC-32（IEEE）；MAX 等于 MIN 时 `delay = MIN`。默认取值范围 200–1999ms。到期时刻 = `created_at + delay`。到期后按场景置为终态：FAIL → FAILED；NEVER_FINISH 不调度、保持 PROCESSING；其余场景 → SUCCESS；未登记的场景值按 NORMAL。`finished_at` 写实际完成时间（截到毫秒，避免 DATETIME(3) 进位跨天）；SUCCESS 且场景不是 BILL_MISSING 时写入 `bill_date = finished_at 的东八区日期`，FAILED 不写。状态推进只用 CAS（`WHERE id = ? AND status = 'PROCESSING'`），已被关单等改走时不覆盖；写库失败 1 秒后重试。写库报错时结果不确定，记下这次尝试的 `status` 和 `finished_at`；之后重试的 CAS 得 0 行时按 id 读回，状态与 `finished_at` 和某次尝试一致就视为那次已生效，照常调用钩子；不一致（已被关单或别处改走）则不覆盖、不回调；读回失败 1 秒后再试。CAS 生效并提交后调用一次完成钩子，供 CH-05 发回调；钩子拿到的上下文不随完成任务结束而取消，自行设超时。
5. 定时完成由进程内调度实现；进程重启时、开始接收请求前，按 id 分页扫描全部 PAY 的 PROCESSING 记录重建调度，已过期的立即完成。

验收：

- [x] 同一 req_no 重复受理返回同一 `channel_txn_no`。
- [x] NORMAL 场景 2 秒内变为 SUCCESS；重启 channel_mock 后未完成的交易仍会完成。

#### CH-02 渠道查单

`P0 · M3 · 依赖：CH-01 · 表：mock_txn`

接口：`POST /mockpay/v1/query`，入参 `txn_type`、`req_no`。

规则：

1. 验签、空密钥、参数非法和内部错误的 HTTP 状态与 CH-01 规则 1 相同：验签失败（缺头、时间戳不是十进制整数、签名不符、与本地时钟整秒相差超过 300 秒）401；密钥为空 500，ERROR 日志不含密钥；签名正确但参数非法 400；内部错误 500。400、401、500 的 body 为空、不签名、不写库。200 的响应按渠道签名，时间戳取注入时钟。JSON 解析规则与受理相同。`txn_type` 只接受 `PAY` 或 `REFUND`；`req_no` 非空、不超过 32 个字符、不含 U+FFFD。
2. 按 `(txn_type, req_no)` 只读当前行，不更新、不插入、不调度、不调用完成钩子、不发回调。查单不看 scenario：DROP_NOTIFY、SLOW 等不改变返回的状态，也不推迟响应。
3. 找到时 `result` 为 `OK`，并带 `status`、`amount`（int64 分，含 0）、`channel_txn_no`。`finished_at` 仅当列非 NULL，格式为约定 §4 的 RFC 3339 毫秒、东八区固定偏移 `+08:00`。PROCESSING 的该列为空，所以省略；省略看的是列，不是状态名。
4. 无行时 `result` 为 `NOT_FOUND`，body 只有这个字段，不插入。`REFUND` 与 `PAY` 按键分开：没有退款行就是 `NOT_FOUND`，即使同 `req_no` 的 PAY 行存在。平台错误码 `10002` 与这里的 `result = NOT_FOUND` 不是一回事。

验收：

- [x] DROP_NOTIFY 场景下查单返回 SUCCESS。
- [x] 未受理的 req_no 返回 `NOT_FOUND`。

#### CH-03 渠道关单

`P0 · M3 · 依赖：CH-01`

接口：`POST /mockpay/v1/close`，只入参 `req_no`。只查 `(PAY, req_no)`。同号的 REFUND 行不关、不当成已存在。

规则：

| 当前状态 | 结果 |
|---|---|
| PROCESSING，场景不是 CLOSE_RACE | CAS 置为 CLOSED，返回 `OK / CLOSED`。不写 `finished_at`，不写 `bill_date`，不调用完成钩子 |
| PROCESSING 且场景为 CLOSE_RACE | CAS 置为 SUCCESS，写 `finished_at`（渠道时钟，截到毫秒）和 `bill_date`（该时间的东八区日期，与自动完成相同）。提交成功后调用一次完成钩子。返回 `ALREADY_PAID`，status 为 SUCCESS |
| SUCCESS | 不改行，不调用钩子，返回 `ALREADY_PAID`，status 为 SUCCESS |
| CLOSED | 不改行，返回 `OK / CLOSED` |
| FAILED | 不改行，返回 `OK / FAILED` |
| 不存在 | 插入 CLOSED 防悬挂行，返回 `OK / CLOSED`。之后同一 req_no 的受理返回 `ORDER_CLOSED`（不论金额）。这条记录 `txn_type = PAY`，`amount` 固定写 0，`channel_txn_no` 用 `MC` 前缀，`created_at` 用渠道时钟，`scenario`、`notify_url` 取列默认值（`NORMAL`、空串）。不写 `finished_at`、`bill_date`。不调度，不调用钩子。请求里的未知字段忽略，不能拿来填这行。`amount = 0` 只出现在这种记录上；它不进入账单（只有 SUCCESS 有 `bill_date`），查单（CH-02）原样返回 0 |

补充（不改变上表的六个结果）：

1. 先读。没有行就插入。唯一键冲突（1062）再读，按读到的当前状态走上表，不当成错误抛出。
2. INSERT 报非 1062：不随请求取消，1 秒后再读。`channel_txn_no` 与本次生成的相同，就按墓碑已写入返回 `OK / CLOSED`。读不到、读失败或是别的行，返回原错误（HTTP 500）。
3. PROCESSING 的更新只用 CAS：`WHERE id = ? AND status = 'PROCESSING'`。影响 0 行就按 id 再读，按新状态回答。0 行且没有“这次写入结果不确定”的尝试时，不调用钩子。
4. 写入报错、结果不确定时：记下这次想写的状态和 `finished_at`。再读到的行与这次尝试一致，普通关单视为已生效（不调钩子）；CLOSE_RACE 与这次的 SUCCESS 和 `finished_at` 一致才调一次钩子。不一致则按读到的状态回答，不覆盖、不调钩子。读回失败返回 500，不猜测。
5. 完成钩子在 CAS 确认提交之后调用，事务内不调用。钩子使用的 context 不随本次请求取消。本次“触发回调”只调用钩子，不 POST `notify_url`，不增加 `notify_sent_count`（HTTP 回调是 CH-05）。
6. 关单不取消已经排好的自动完成定时器。定时器之后触发时 CAS 得 0 行，不得把 CLOSED 改回 SUCCESS，也不得再调钩子。CLOSE_RACE 若自动完成已经先写成 SUCCESS，关单只返回 `ALREADY_PAID`，钩子次数保持自动完成那一次。
7. 重复关单不改金额、状态、流水号、`finished_at`、`notify_sent_count`。
8. HTTP 与受理、查单相同：验签失败 401；密钥为空 500（ERROR 日志不含密钥）；参数非法 400；内部错误 500。400/401/500 body 为空、不签名、不写库。只有 200 带渠道签名。200 的 body 只有 `result` 和 `status`。`req_no` 非空、不超过 32 个字符、不含 U+FFFD。

验收：

- [x] 表中每一行一个测试。
- [x] 对未知 req_no 关单后再受理，返回 `ORDER_CLOSED`。

#### CH-04 渠道退款受理与查询

`P0 · M4 · 依赖：CH-01 · 表：mock_txn`

接口：`POST /mockpay/v1/refund`；查询复用 `POST /mockpay/v1/query`（`txn_type = REFUND`）。

规则：

1. 先按 `(REFUND, req_no)` 幂等，规则同 CH-01：已存在则直接返回已有结果，不再做下面的额度校验（避免重新提交时把本笔重复计入累计金额）。
2. 原支付 `orig_req_no` 必须为 SUCCESS，否则 `REJECTED / ORIG_NOT_SUCCESS`。
3. 该原支付下非 FAILED 退款金额合计 + 本次金额 ≤ 原支付金额，否则 `REJECTED / REFUND_EXCEEDED`。
4. 自动完成与回调规则同 CH-01、CH-05；场景按退款金额末两位决定，仅 01（FAIL）、02（DROP_NOTIFY）生效，其余为 NORMAL。

先按 `(REFUND, req_no)` 返回已有结果；新单在同一事务里 `FOR UPDATE` 锁原支付，再合计 status 不是 FAILED 的退款。场景只有 01 FAIL 和 02 DROP_NOTIFY，其余 NORMAL。当日账单只断言 `bill_date`，CSV 仍是 CH-08。

验收：

- [x] 超额退款被拒绝；原支付未成功被拒绝。
- [x] 全额退款受理后，用同一 `req_no` 重新提交返回已有结果，而不是 `REFUND_EXCEEDED`。
- [x] 退款成功后进入当日账单（`txn_type = REFUND`）。

#### CH-05 渠道异步回调

`P0 · M3 · 依赖：CH-01 · 表：mock_txn`

规则：

1. 交易进入 SUCCESS 或 FAILED 后，向受理时的 `notify_url` 发渠道签名的 HTTP 回调（body 见 `docs/05-api.md` §9.7）。CF-24 里只有「其他」走普通发送。本功能点发送的场景是 NORMAL、FAIL、ACCEPT_TIMEOUT、BILL_MISSING、BILL_AMOUNT_DIFF、CLOSE_RACE，以及未登记的场景值；状态必须是 SUCCESS 或 FAILED。FAIL 的 body `status` 是 FAILED，这是普通终态回调。DROP_NOTIFY 不回调。SLOW 的首次回调由 CH-06 排到 finished_at 之后 30 秒，失败后仍按本功能点的 5s/15s/30s/60s 重试。DUP_NOTIFY 由 CH-06 发送：第一波并发 5 次，之后按注入时钟每秒 1 次再发 5 次，共 10 次，不论平台响应；不走本功能点最多 5 次和 5s/15s/30s/60s 的退避。`status` 不是 SUCCESS 或 FAILED、`notify_url` 为空、`finished_at` 为零值时不发、不加计数。NEVER_FINISH 本来就不调用完成钩子。不取消已有自动完成定时器，不改查单和关单的返回。以上是文字澄清，不改六个状态的资金含义。
2. 完成钩子 `OnFinished` 在 CAS 提交之后调用。事务内不发 HTTP，`dbx.WithTx` 的回调里也不发。`Service.Pay`、`Close`、`finish` 不直接发 HTTP；只在 channel-mock 进程入口、`New` 之后把 `OnFinished` 接到通知器。钩子在调度 goroutine 里执行，不能长时间阻塞：`Handle` 只按注入时钟把第一次发送排进 `Scheduler`，然后立刻返回。第一次发送和重试都由 `Scheduler` 执行。CLOSE_RACE 调用同一个 `OnFinished`，同样只入队。
3. 平台回 HTTP 200 且 body 字节恰好是 `SUCCESS` 才算送达。大小写、引号、尾部换行都不算。否则按已经发出的次数等待：发出 1 次仍失败等 5s，2 次等 15s，3 次等 30s，4 次等 60s。连第一次最多 5 次，第 5 次失败后停止，不再排第 6 次。间隔用注入时钟的 `Scheduler`。不使用 `time.Sleep`，`biz` 和 `job` 不使用 `time.Now`。
4. 每次确定要发出 HTTP 之前，用单独的短事务做条件更新：`UPDATE mock_txn SET notify_sent_count = notify_sent_count + 1 WHERE id = ? AND notify_sent_count = ?`。期望值是这次发送前读到的计数。影响 1 行才在事务外发 HTTP。影响 0 行就再读：已经 ≥ 5，或状态不再是 SUCCESS / FAILED，则停止，不覆盖状态。更新报错则不发 HTTP，1 秒后按原计数重试，不把这次算进 5 次。DUP_NOTIFY 不用这条按期望值相等的更新，它用 `notify_sent_count < 10`，见 CH-06。
5. HTTP 使用标准库 `Client`。单次超时 5 秒（上下文超时）。不跟随重定向，3xx 不是送达。响应体最多读 1024 字节，超出或读失败都算未送达。超时、连接失败、非 200、body 不符都算这一次已经发送（计数已加）。
6. body 用 `encoding/json` 对固定字段顺序的结构体编码，紧凑、无 HTML 转义，字段不要 `omitempty`。顺序固定为 `notify_id`、`txn_type`、`req_no`、`channel_txn_no`、`amount`、`status`、`finished_at`。`amount` 是 JSON 数字（int64 分）。`finished_at` 与查单相同：东八区 `FixedZone` `+08:00`，先 `Truncate(time.Millisecond)`，布局 `2006-01-02T15:04:05.000Z07:00`。签名用已有 `pkg/sign.SignChannel`，canonical = `X-Mock-Timestamp` + 换行 + 原始 body。请求头 `X-Mock-Timestamp`、`X-Mock-Signature`。时间戳用注入时钟的 Unix 秒，不用 `time.Now`。接口 §3.7 的 V4 仍是签名算法的短示例，不改那条向量，回调 body 也不是那三个字段。
7. `notify_id` 用 idgen，前缀 `CN`（约定 §2.2：渠道回调发送号，不落库，只出现在 body）。总长仍是前缀 + 19 位。不新增错误码。发号失败或时钟回拨：这次不算发送，不加计数，ERROR 日志不含密钥，1 秒后再排同一次。空密钥（`sign.ErrEmptyKey`）同样不算发送，不加计数，ERROR 日志不含密钥和签名，1 秒后再试，不在调用栈上自旋。
8. 同一 `id` 已有一条发送链时，再次 `Handle` 直接返回，避免 CH-01 提到的钩子双调用把次数打双。用进程内标记即可，不落库。进程退出后未发出的重试丢失，不在 `Recover` 里扫描已终态的行。查单仍是补偿手段。
9. 日志用 `slog`，失败用 `ErrorContext`。不记录密钥、签名、完整 URL 查询里的口令、DSN。

验收：

- [x] 平台前 2 次返回 500，第 3 次成功，渠道停止重试，`notify_sent_count = 3`。

#### CH-06 确定性故障场景

`P0 · M3–M4 · 依赖：CH-01、CH-02、CH-03、CH-05 · 表：mock_txn`

依赖不含 CH-04：这四个支付场景不调用退款。CH-04 仍负责退款受理，不在本切片。这是文字澄清，对齐下面已有的「M3 完成 01、02、03、08，其余在 M4」。

规则：按支付金额（分）的末两位触发：

| 末两位 | 场景 | 行为 | 用于验证 |
|---|---|---|---|
| 01 | FAIL | 交易失败并回调 | 失败路径 |
| 02 | DROP_NOTIFY | 交易成功，不回调 | PAY-05、A4 |
| 03 | DUP_NOTIFY | 交易成功，并发回调 5 次，之后每秒 1 次再发 5 次，不论平台响应 | PAY-04、A3 |
| 04 | SLOW | 交易按时成功（查单可见），但 30 秒后才回调 | 回调与查单竞争 |
| 05 | ACCEPT_TIMEOUT | 受理接口阻塞 5 秒再响应（平台超时 3 秒），交易实际成功 | PAY-03 未知结果处理 |
| 06 | BILL_MISSING | 交易成功，但不出现在账单 | 短款 |
| 07 | BILL_AMOUNT_DIFF | 交易成功，账单金额 +1 分 | 金额不符 |
| 08 | NEVER_FINISH | 一直 PROCESSING，可被关单 | PAY-06、PAY-07 |
| 09 | CLOSE_RACE | 收到关单请求时变为成功 | PAY-06 竞争 |
| 其他 | NORMAL | 正常成功 | 主流程 |

M3 完成了 01、02、03、08。当时本切片只做这四个，不实现 SLOW 的 30 秒延迟，也不实现 ACCEPT_TIMEOUT 的受理阻塞。M4 已经按规则 6 和 7 实现这两件事。BILL_MISSING、BILL_AMOUNT_DIFF 的账单表现仍在 CH-08，CLOSE_RACE 仍不重做（关单抢成功已由 CH-03 实现）。账单下载是 CH-08。CH-06 的「账单内容」只断言行上的 `bill_date`（有或 NULL），不产生 CSV。

1. FAIL：自动完成变为 FAILED，不写 `bill_date`，并走 CH-05 的普通回调。body 的 `status` 是 FAILED。不另写一套重试。平台回 HTTP 200 且 body 字节恰好是 `SUCCESS` 时 `notify_sent_count = 1`，不再发第 2 次。
2. DROP_NOTIFY：自动完成变为 SUCCESS，`bill_date` 为 `finished_at` 的东八区日期。不发回调，HTTP 0 次，`notify_sent_count` 保持 0。
3. NEVER_FINISH：受理时不调度，保持 PROCESSING，不调用完成钩子，不回调。把时钟和调度向前推进后仍是 PROCESSING，没有完成任务，HTTP 0 次，`bill_date` 为空。可被关单：关成 CLOSED，`finished_at` 仍为空，仍然 0 次 HTTP。
4. DUP_NOTIFY：交易进入 SUCCESS 后由通知器按下述发送。不改 `status`、`finished_at`、`bill_date`。
   - `Handle` 仍只入队，不在钩子里发 HTTP。同一 `id` 已有发送链则直接返回（进程内标记，规则同 CH-05）。
   - 第一波在一个调度任务里用 `golang.org/x/sync/errgroup` 同时发起 5 次。
   - 每一次发送：先发号和签名（前缀仍是 `CN`，`pkg/sign.SignChannel`）。失败则这次不占计数，1 秒后再补这一次，不在调用栈上自旋。成功后用单独短事务占名额，再在事务外 POST。
   - 占名额的 SQL 与普通回调不同，因为 5 次并发不能共用同一个期望计数：`UPDATE mock_txn SET notify_sent_count = notify_sent_count + 1 WHERE id = ? AND notify_sent_count < 10`。影响 1 行才 POST。影响 0 行说明已经到 10，不再发。更新报错则不 POST，1 秒后只补这一次。普通回调的 `WHERE notify_sent_count = ?` 且最多 5 次不改。
   - 第一波 5 次都已 POST 返回之后（不论 200、500、连接失败、body 是不是 `SUCCESS`），用注入时钟把后 5 次排在 now+1s、now+2s、now+3s、now+4s、now+5s。这 5 次同样先占名额再 POST，不看平台响应，不走 5s/15s/30s/60s。满 10 次停止。对方一直回 `SUCCESS` 也要发满 10 次。
   - HTTP 超时 5 秒、不跟随重定向、最多读 1024 字节，与 CH-05 相同。日志不含密钥、签名、带口令的 URL。
   - `biz` 禁止 `time.Now`。延迟只用注入的 `clock.Clock` 和已有 `Scheduler`。
5. M3 当时 SLOW 不发、不加计数，也不实现 30 秒延迟。M4 已按规则 6 实现。

M4：

6. SLOW：只把首次回调推迟到 `finished_at` 之后 30 秒（注入时钟）发出，其余行为与 NORMAL 一致：按自动完成时间变为 SUCCESS，查单立即可见，`bill_date` 照常写入；首次回调失败后按 CH-05 第 3、4 条重试（按已发次数等 5s / 15s / 30s / 60s，连首次最多 5 次，计数用 `WHERE notify_sent_count = ?` 的条件更新）。（2026-10-27 用户决定）
7. ACCEPT_TIMEOUT：只在新插入成功的受理上等待。先写入 `mock_txn` 并调度自动完成，再用调度器等到注入时钟到达 `created_at` 之后 5 秒才返回 `OK`。重复受理和 `recoverInsert` 不延迟。受理 HTTP 上下文上限是 8 秒，查单和关单仍是 3 秒；平台客户端的 3 秒总超时不改，仍把这次看成结果未知。
8. CLOSE_RACE 的关单抢成功已由 CH-03 实现并验收，本功能点不重做。BILL_MISSING、BILL_AMOUNT_DIFF 的账单表现属于 CH-08，在 CH-08 验收；本功能点只保证 CH-01 规则 4 的 `bill_date` 写法（BILL_MISSING 不写）。

验收：

- [x] M3：FAIL、DROP_NOTIFY、DUP_NOTIFY、NEVER_FINISH 各至少一个集成测试。FAIL 为 FAILED、`bill_date` 为空、平台回 SUCCESS 时 `notify_sent_count = 1` 且没有第 2 次、body 的 `status` 为 FAILED。DROP_NOTIFY 为 SUCCESS、`bill_date` 为 `finished_at` 的东八区日期、HTTP 0 次、`notify_sent_count = 0`。DUP_NOTIFY 第一波 5 次同时在途，之后每秒 1 次再 5 次，共 10 次，不因 SUCCESS 停止。NEVER_FINISH 推进时钟和调度后仍是 PROCESSING、没有完成任务、HTTP 0 次、`bill_date` 为空；再关单得到 CLOSED，`finished_at` 仍空，仍然 0 次 HTTP。账单只断言 `bill_date`。
- [x] M4 SLOW：自动完成后查单立即为 SUCCESS，`bill_date` 与 NORMAL 相同；注入时钟推进到 `finished_at` + 30 秒之前回调 HTTP 0 次，推进到 30 秒后发出第 1 次回调。
- [x] M4 SLOW 重试：平台对首次回调返回 500、第 2 次返回 `SUCCESS`：第 2 次在首次之后 5 秒（注入时钟）发出，之后不再发送，`notify_sent_count = 2`。
- [x] M4 ACCEPT_TIMEOUT：受理请求在注入时钟推进 5 秒之前不返回，推进后返回 200 `OK`；返回前 `mock_txn` 已有该 `req_no` 的行，之后照常自动完成为 SUCCESS。

#### CH-07 全局故障配置

`P0 · M6 · 依赖：CH-06、PAY-05`

接口：`PUT /mockpay/admin/config`、`GET /mockpay/admin/config`（仅管理端口）

规则：可配置完成延迟范围、回调重复概率、丢回调概率、受理接口错误率（返回 500）与超时概率，作用于 NORMAL 场景，用于压测和演练；重启后恢复默认值。

验收：

- [ ] 丢回调概率设为 1 时，所有 NORMAL 交易都不回调，平台通过查单全部完成。

#### CH-08 账单下载

`P0 · M6 · 依赖：CH-01、CH-04 · 表：mock_txn`

接口：`GET /mockpay/v1/bills?date=YYYY-MM-DD`

规则：

1. 内容为 `bill_date = date` 的 SUCCESS 交易（支付与退款），CSV 格式见 `docs/05-api.md` §9.6，按 `finished_at`、`channel_txn_no` 排序。
2. BILL_AMOUNT_DIFF 场景输出金额 +1。
3. 响应头 `X-Bill-Sha256`、`X-Bill-Count`。
4. `date` 为今天或未来时返回 `BILL_NOT_READY`；配置 `MOCK_ALLOW_PARTIAL_BILL=true` 时允许下载今天的账单（测试和演练使用）。
5. 同一天的账单在内容不变时多次下载结果字节一致。

验收：

- [ ] 账单行数与 `X-Bill-Count` 一致，内容哈希与 `X-Bill-Sha256` 一致。
- [ ] BILL_MISSING 交易不在账单中，BILL_AMOUNT_DIFF 交易金额 +1（由 CH-06 移来：这两个场景的账单表现在本功能点验收）。

#### CH-09 账单注入

`P0 · M6 · 依赖：CH-08`

接口：`POST /mockpay/admin/bill-inject`，入参 `date`、`txn_type`、`amount`、可选 `req_no`。

规则：插入一条 SUCCESS 的 `mock_txn`，`req_no` 默认为平台不存在的单号，`bill_date = date`，用于构造长款。

验收：

- [ ] 注入后该记录出现在对应日期账单中。

### 4.4 支付（PAY）

#### PAY-01 商户与密钥

`P0 · M3 · 依赖：BASE-14、BASE-03 · 表：merchant`

接口：`PaymentService.GetMerchantCredential(app_id)` → `{merchant_id, status, app_secret, secret_version, default_notify_url}`

规则：

1. 只通过内部 RPC 提供，网关调用；返回的明文密钥不得写入日志或 trace。
2. 商户 DISABLED 时仍返回记录，由网关返回 `20004`。
3. payment 内部提供按 `merchant_id` 读取商户配置（费率、默认通知地址、状态）的方法，进程内缓存 `MERCHANT_CONFIG_CACHE_TTL`（默认 60 秒，设为 0 关闭，用于 OPT-04 对比）。

说明（PAY-01 口径澄清）：

- `app_id` 按字节精确匹配。`merchant` 表列跟随库默认 `utf8mb4_bin`（约定 §7.2，区分大小写，但 PAD SPACE 比较时忽略尾部空格，`'app_m100001 '` 也能查到 `app_m100001`）；集成测试临时库为 `utf8mb4_0900_ai_ci`（不区分大小写与重音）。两种规则下 SQL 都可能匹配到字节不同的值，所以服务端读回 `app_id` 再逐字节比较。为空、不存在、只是大小写或首尾空格不同都返回 `20004`；为空时不查库。原因：`X-App-Id` 不在签名串里（接口 §3.1），放宽匹配会让同一请求换个大小写绕过按 `app_id` 记的 nonce。按 `merchant_id` 读配置同样精确匹配。
- 解密用 `secretbox.NewFromBase64(SECRET_MASTER_KEY)` 建的 Box。`Open` 失败返回 `10003`，打一条 ERROR 日志，字段只有 `app_id`、`merchant_id`，不带密文、明文和 secretbox 错误。返回的是不带原因的 `10003`，handler 不再打第二条。
- payment 端不缓存凭证，也不缓存“不存在”；网关侧凭证缓存属于 GW-08。明文密钥用专用类型 `biz.Secret` 包装，只有 `Reveal()` 取得到明文，只在 handler 组 Kitex 响应时调用。`fmt` 的所有格式动词、`json.Marshal`、slog（含 JSON handler）都输出 `***`，凭证嵌在切片、结构体或未导出字段里也一样（明文放在指针后面，fmt 走反射时只打印地址）。
- 规则 3 的缓存只存静态配置：`merchant_id`、`status`、`notify_url`（默认通知地址）。键是 `merchant_id`，并发安全，时间取注入的 `clock.Clock`。条目在 `[装载时刻, 装载时刻 + TTL)` 内命中，装载时刻取读库前的时间；时钟回拨到装载时刻之前视为过期。TTL 为 0 时每次读库。不存在（`20004`）和读库失败不进缓存。
- 费率不进缓存。生效费率与 PAY-10 同口径，读 `merchant_fee_rate` 中 `effective_from <= at` 的最近一档，不读 `merchant.fee_rate_bp`，每次读库。原因：费率按时刻生效，缓存当前档会让追加新档后最多一个 TTL 内算错手续费。`at` 为零值或 `merchant_id` 为空返回 `10001`；没有已生效的档时返回“未找到”而不是错误码，由 PAY-02 决定如何处理。
- payment 的 Kitex 服务端按 ACC-11 的模式在 PAY-01 起：端口 8881（接口 §10.2），中间件同 account（TTHeader 的 `BizStatusError`、panic 恢复为 `10003`、访问日志、trace 与指标空实现），服务端处理超时 2 秒。只实现 `GetMerchantCredential`；其余方法在各自功能点实现前返回 `10003`，不进 biz，不是这些方法将来的语义。要调渠道的方法（Pay、ClosePayment、CreateRefund、CreateTransfer）的服务端超时由各自功能点按 §5.4.1 重新确定。中间件不读请求和响应，访问日志的 `merchant_id` 留空。
- `cmd/payment` 启动顺序：打印脱敏配置 → `logx.SetDefault` → 校验主密钥 → 3 秒内连库（失败只输出固定文本，不带地址和口令）→ 启动 Kitex。健康检查、`/readyz` 和 SIGTERM 10 秒排空仍属 OPS-01。网关侧客户端与验签属于 GW-03。

验收：

- [x] 种子商户可查到，密钥解密结果与 `.env` 演示值一致。
- [x] 不存在的 app_id 返回 `20004`。

#### PAY-02 创建支付单

`P0 · M3 · 依赖：PAY-01 · 表：payment_order`

接口：`POST /v1/payments` → `PaymentService.CreatePayment`

规则：

1. 入参（`docs/05-api.md` §4.1）：`out_trade_no` 为 1–64 且匹配 `[A-Za-z0-9_-]`；`amount` 为 1–10,000,000 分；`subject` 为 1–128 个码点，不 trim；`currency` 省略或空串为 `CNY`，其他值（含 `cny`）为 `10001`；`expire_minutes` 未设置时默认 15，已设置但不在 1–120（含 0）为 `10001`。请求为 nil 返回 `10001`。
2. 未传 `notify_url` 时存空串 `''`。通知时才用商户默认地址（NTF，本功能点不发送）。白名单只在请求带了 `notify_url` 时检查：逗号分隔、trim、忽略空段；scheme 只能是 http 或 https，主机去掉端口后与名单大小写不敏感精确比较；不解析 DNS、不访问该 URL、不跟随重定向。长度按 Unicode 码点 ≤ 512。解析失败、空 host、scheme 不对、主机不在名单、超长，以及名单解析后为空时的任何显式 URL，都是 `10001`。显式传入的地址即使等于商户默认地址，也原样入库。幂等比较用填充后的值，缺省比较的是空串。
3. `merchant_id` 为空、商户不存在、或状态不是 `ACTIVE`：返回 `20004`，不插入。商户状态走 `MerchantConfig` 缓存；不是 `ACTIVE`（含 DISABLED）读到配置后仍拒绝。
4. `pay_no` 前缀 `PA`，在插入前生成。时钟回拨超过 5ms 返回 `10004`，不插入。`created_at` 与 `expire_at` 用同一个注入时钟瞬间，`expire_at = created_at + expire_minutes`。INSERT 显式写 `created_at` 和 `updated_at`。手续费用 `FeeRateAt(merchant_id, created_at)` 取 `effective_from <= created_at` 的最近一档，再用 `money.Fee` 固化到 `fee_amount`。不读 `merchant.fee_rate_bp`，不缓存费率。没有生效档不插入，返回 `10003`，不把手续费当成 0。新建行 `status = INIT`，`channel`、`paid_at`、`closed_at`、`fail_code`、`fail_msg` 为 NULL，`refunded_amount = 0`，`version = 0`。
5. 插入遇 `uk_merchant_out_trade_no` 冲突时读取已有订单：`amount`、`currency`、`subject`、`notify_url` 全部一致 → 返回已有订单（不论当前状态），不更新；否则 `10006`。`expire_minutes` / `expire_at` 不参与比较。1062 但按 `(merchant_id, out_trade_no)` 读不到行（例如撞了 `uk_pay_no`）返回 `10003`，不当作幂等成功，不重试。

异常：参数错误 → `10001`；商户不存在或已停用 → `20004`；没有生效档或内部故障 → `10003`；时钟回拨 → `10004`；幂等冲突 → `10006`。

验收：

- [x] 100 个并发相同请求只生成 1 条订单，全部返回同一 `pay_no`。
- [x] 相同 `out_trade_no`、不同金额返回 `10006`。
- [x] `expire_minutes = 0` 或 `121`、`notify_url` 不在白名单，返回 `10001`。
- [x] 费率 60bp 的商户下单 10000，`fee_amount = 60`。

#### PAY-03 发起支付

`P0 · M3 · 依赖：PAY-02、CH-01、BASE-15 · 表：payment_order、channel_txn`

接口：`POST /v1/payments/{pay_no}/pay` → `PaymentService.Pay`

规则：

1. 请求为 nil → `10001`。`channel` 不是恰好 `mockpay`（含空串、大小写或首尾空格不同）→ `30007`，不读库。
2. 按 `pay_no` 读订单。没有，或 `merchant_id` 与订单逐字节不同（含空商户号）→ `30001`。两种情况同一错误码，不泄露另一商户的订单是否存在。
3. `PAYING` 或 `SUCCESS`：返回当前订单，`biz_code = 0`，不再调渠道，不再插入 `channel_txn`。`FAILED` 或 `CLOSED` → `30003`，走 `BizStatusError`，不携带订单。`INIT` 且 `expire_at ≤ now + 30s`（含等于）→ `30004`。`now` 来自注入的 `clock.Clock`，先截到毫秒。`biz` 不调用 `time.Now()`。
4. 事务 1（`dbx.WithTx`，事务内不调用 HTTP、RPC、MQ）：`UPDATE payment_order SET status='PAYING', channel='mockpay', version=version+1, updated_at=这次时钟 WHERE pay_no=? AND merchant_id=? AND status='INIT'`。影响 1 行才 `INSERT channel_txn`：`txn_type=PAY`，`biz_no=pay_no`，`channel=mockpay`，`amount` 为订单金额，`status=PROCESSING`，`channel_txn_no` 为 NULL，`next_query_at=now+10s`，`notify_count=0`，`query_count=0`，`version=0`，`created_at` / `updated_at` 显式写这次时钟。影响 0 行：回滚，事务外再读一次，按读到的状态返回（PAYING / SUCCESS 幂等，FAILED / CLOSED 为 `30003`，仍是 INIT 就返回这张 INIT 单）。不循环重试，不 `time.Sleep`。只有本次 CAS 成功的调用者继续调渠道。
5. 事务成功提交之后、HTTP 之前，调用已有的 `fault.Point(ctx, "pay.after_tx1")`。不新增故障点名称，不在事务内部调用。杀掉进程时事务已经提交，订单停在 PAYING。
6. 然后调渠道受理。`req_no=pay_no`，`subject` 为订单标题，`notify_url` 用配置 `CHANNEL_NOTIFY_URL`，不是订单上的商户通知地址。金额是订单金额。连接超时 500ms，总超时 3 秒，不重试，不跟随重定向。签名用 `pkg/sign` 与 `CHANNEL_MOCKPAY_KEY`，算法与 CH-01 一致。地址是 `CHANNEL_MOCKPAY_URL` 加 `/mockpay/v1/pay`。密钥不进日志和错误。签名或地址配置在发出请求前失败，按已有的 `10003` 处理。渠道客户端未装配时在 CAS 之前返回 `10003`，订单保持 INIT。
7. 渠道结果：
   - `OK` 且带 `channel_txn_no`：`UPDATE channel_txn SET channel_txn_no=? WHERE biz_no=? AND channel_txn_no IS NULL`（同时写 `updated_at = updated_at`，避免 `ON UPDATE CURRENT_TIMESTAMP` 改成数据库时钟）。然后返回 PAYING 订单，`biz_code = 0`。影响 0 行就再读，不覆盖已有流水号。回填撞上唯一键（1062）返回 `10003`，不猜测该写哪一条。
   - `REJECTED`：调用 `ApplyPayResult`，入参为结构体：`biz_no=pay_no`、结果 `FAILED`、`fail_code=CHANNEL_REJECTED`、`fail_msg` 为渠道 `reason`（可以是空字符串）、`source=SYNC`，不带金额。`SYNC` 只表示渠道受理的同步拒绝。已有来源名是 NOTIFY / QUERY / CLOSE / JOB / ADMIN，没有同义名称，因此新增这个来源名，不是新的错误码。PAY-04 成功把订单写成 FAILED 后，本方法返回该订单和 `30006`。`30006` 放在 `PayResponse.biz_code`，error 为 nil，并携带订单。事务、outbox 和冲突计数属于 PAY-04。
   - 超时、网络错误、5xx、其他非 200（含 400、401），HTTP 200 但验签失败，以及 `OK` 但没有流水号：结果未知。不改订单状态，不回填，返回 PAYING，`biz_code = 0`。
8. `Pay` 的服务端处理超时是 5 秒，与 §5.4.1、GW-01 第 5 条的网关调用超时一致，让 3 秒的渠道超时先到期并返回 PAYING。本功能点只改 `Pay`。`CreateRefund` 的 5 秒在 REF-02，原因相同。其余 PaymentService 方法仍是 2 秒。不新增环境变量。渠道 HTTP 的 3 秒从调用 ctx 派生，父 ctx 先到期时先到期的生效。

验收的归属：`REJECTED` 分支调用 `ApplyPayResult`（PAY-04），在 PAY-04 验收；“2 秒内 SUCCESS”需要回调入口，在 GW-06 验收；Pay 与 Close 并发在 PAY-06 验收；ACCEPT_TIMEOUT 需要 CH-06 的 M4 场景和查单，在 PAY-05 验收。

验收：

- [x] 同一订单并发调用 Pay 20 次，只插入 1 条 `channel_txn`，渠道只受理 1 次。
- [x] 渠道返回 `OK`：接口返回 PAYING，订单为 PAYING，`channel_txn` 为 PROCESSING 且已回填 `channel_txn_no`。
- [x] 假渠道客户端超过 3 秒不响应：接口返回 PAYING，订单仍为 PAYING，`channel_txn` 仍为 PROCESSING、`channel_txn_no` 为空。

#### PAY-04 支付结果推进

`P0 · M3 · 依赖：PAY-03、MQ-01 · 表：channel_txn、payment_order、outbox`

说明：回调（GW-06）、查单（PAY-05）、关单（PAY-06）、同步拒绝（PAY-03）共用 `ApplyPayResult`。入参是结构体，字段包括 `biz_no`、`result`（SUCCESS / FAILED / CLOSED）、`source`、`fail_code`、`fail_msg`、可选金额、可选 `channel_txn_no`、`channel_status_raw`、渠道完成时间。

规则：

1. 状态机：

   ```
   INIT ──Pay──► PAYING ──SUCCESS──► SUCCESS
    │              ├──FAILED───► FAILED
    │              └──CLOSED───► CLOSED
    └──关单/超时──────────────► CLOSED
   ```

   SUCCESS、FAILED、CLOSED 为终态。一笔支付单只对应一次渠道交易，失败后商户需用新的 `out_trade_no` 重新下单。

2. 空 `biz_no`、未知 `result`、未知 `source`，以及 SUCCESS 未传渠道完成时间：返回 `10001`，不写库。来源只接受已有的 NOTIFY / QUERY / CLOSE / JOB / ADMIN / SYNC。
3. 一次 `dbx.WithTx`。事务内不调用 HTTP、RPC 或 Kafka，也不调用 `fault.Point`。`callback.after_commit` 仍由 GW-06 在调用本函数之后触发。顺序如下。金额是 int64 分。渠道完成时间截到毫秒。`updated_at` 显式写本次注入时钟截到毫秒，避免 `ON UPDATE CURRENT_TIMESTAMP` 改成数据库时钟。

   ```
   UPDATE channel_txn
      SET status = ?, channel_txn_no = COALESCE(channel_txn_no, ?), channel_status_raw = ?,
          channel_finished_at = ?, next_query_at = NULL, version = version + 1, updated_at = ?
          [, notify_count = notify_count + 1]          -- 仅 source = NOTIFY
    WHERE biz_no = ? AND status = 'PROCESSING' [AND amount = ?]   -- 结果携带金额时
   ```

   `channel_txn_no`、`channel_status_raw` 为空则 SQL 用 NULL，COALESCE 不会把已有流水号盖成空。未传渠道完成时间时 `channel_finished_at` 保持空。

   影响 1 行才更新订单：

   ```
   UPDATE payment_order
      SET status = ?, version = version + 1, updated_at = ?
    WHERE pay_no = ? AND status = 'PAYING'
   ```

   SUCCESS：`paid_at` 用调用方传入的渠道完成时间，不用本地时钟。FAILED：`fail_code` 用调用方传入的非空值，否则用 `CHANNEL_FAILED`；`fail_msg` 用传入值（可以是空字符串）。CLOSED：`closed_at` 用渠道完成时间；没传则用这次注入时钟截到毫秒。订单 CAS 影响不是 1 行：整笔回滚（`channel_txn` 回到 PROCESSING），ERROR 日志只记 `pay_no` 和 `err_code`，返回 `10003`。

   只有订单 CAS 成功且结果是 SUCCESS 时，在同一个事务里调用已有的 `outbox.Add` 写 `payment.succeeded`。`msg_key` 和 `biz_no` 都是 `pay_no`。data 用订单上的 `pay_no`、`merchant_id`、`out_trade_no`、`channel`、`amount`、`fee_amount`，`paid_at` 用渠道完成时间。`event_id` 由 `outbox.Add` 发，支付不再插第二行 outbox。Add 失败则整个事务失败；发号时钟回拨映射为已有的 `10004`。FAILED 和 CLOSED 不写 outbox。

   渠道更新影响 0 行：事务回滚，事务外再读 `channel_txn`（按 `biz_no`）。先比金额，再看终态：

   - 没有行 → `30001`。
   - 结果带了金额且与该行 `amount` 不同 → `50002`，不改订单、不改流水，ERROR 日志。不新增指标。已是终态时也先比金额，金额不符不是冲突。
   - 已是同一个终态 → 返回当前订单，error 为 nil。仅 `source=NOTIFY` 时另做 `UPDATE channel_txn SET notify_count = notify_count + 1 WHERE biz_no = ? AND status = 该终态`，不改 status，不再写 outbox。
   - 已是另一个终态 → 不改状态，进程内计数 `pay_callback_conflicts_total` 加 1（标签是 channel），ERROR 日志，返回当前订单且 error 为 nil。等待对账处理。
   - 仍是 PROCESSING（金额相符或这次没带金额）→ `10003`，不加冲突计数。

   SYNC、QUERY、CLOSE、JOB、ADMIN 都不加 `notify_count`。

4. 回调只携带终态，终态不可变，因此乱序回调不会导致状态倒退。Pay 的同步拒绝在本函数把单写成 FAILED 后，由 Pay 返回该订单和 `30006`；本函数自己的成功推进返回订单且 error 为 nil。

验收：

- [x] DUP_NOTIFY 的订单层：同一笔 SUCCESS 结果以 `source = NOTIFY` 调用 `ApplyPayResult` 10 次（其中 5 次并发），订单只推进一次，outbox 只有 1 条 `payment.succeeded`，`notify_count = 10`（A3 订单层；经网关的端到端在 GW-06 验收）。
- [x] Pay 时假渠道客户端返回 `REJECTED`（由 PAY-03 移来）：订单 FAILED，`fail_code = CHANNEL_REJECTED`，`fail_msg` 为渠道 `reason`，接口返回 `30006` 和订单。
- [x] 回调金额与本地不一致返回 `50002`，订单仍为 PAYING。
- [x] 先收到 FAILED 再收到 SUCCESS，订单保持 FAILED，冲突指标 +1。
- [x] 测试中让 `payment_order` 的 CAS 失败（人为改状态），整个事务回滚，`channel_txn` 未变化。

#### PAY-05 查单补偿

`P0 · M4 · 依赖：PAY-04、CH-02、CH-06、GW-06、REF-03、BASE-15 · 表：channel_txn`

说明：对支付和退款的 PROCESSING 渠道交易统一查单。

规则：

1. 每秒执行一次。抢占：

   ```
   BEGIN
     SELECT id, biz_no, txn_type, query_count, created_at FROM channel_txn
      WHERE status = 'PROCESSING' AND next_query_at <= now
      ORDER BY next_query_at LIMIT 100 FOR UPDATE SKIP LOCKED
     UPDATE channel_txn SET query_count = query_count + 1, last_query_at = now,
            next_query_at = now + backoff(query_count) WHERE id IN (...)
   COMMIT
   ```

   先推后下次查单时间再在事务外查询，避免持锁调用外部接口；多实例天然互斥。
2. 事务外以最多 10 个并发查询渠道（errgroup + 限流），单次超时 3 秒。
3. 结果分发：

   | 渠道结果 | PAY | REFUND |
   |---|---|---|
   | SUCCESS / FAILED | `ApplyPayResult`（source = QUERY） | `ApplyRefundResult`（REF-03） |
   | PROCESSING | 不处理 | 不处理 |
   | NOT_FOUND 且创建超过 60 秒 | 调用渠道关单：`CLOSED` → `ApplyPayResult(CLOSED)`；`ALREADY_PAID`（受理请求迟到落库）→ 立即查单并 `ApplyPayResult(SUCCESS)`；`FAILED` → `ApplyPayResult(FAILED, CHANNEL_FAILED)`；调用失败 → 等待下一档 | 渠道未收到退款请求，重新提交渠道退款（渠道按 req_no 幂等）：`OK` → 回填 `channel_txn_no`；`REJECTED` → `ApplyRefundResult(FAILED, CHANNEL_REJECTED)`；结果未知 → 等待下一档 |
   | NOT_FOUND 且创建不足 60 秒 | 不处理 | 不处理 |
   | 调用失败 | 等待下一档 | 等待下一档 |

4. 退避计划：10s、10s、30s、1m、2m、5m、10m、30m，之后每小时一次，直到创建后 24 小时；超过后置 `next_query_at = NULL`，`pay_channel_query_exhausted_total + 1`，交给对账处理。
5. 测试配置下所有间隔为 2 秒。

验收：

- [ ] DROP_NOTIFY 场景：测试配置下 30 秒内订单变为 SUCCESS（A4）。
- [ ] 两个 payment 实例同时运行，同一条记录在同一档只被查询一次（统计渠道查单次数）。
- [ ] 订单 PAYING 而渠道无记录（模拟崩溃点 `pay.after_tx1`），最终订单 CLOSED，渠道侧也为 CLOSED。
- [ ] SLOW 场景下查单先于回调完成，随后经 GW-06 到达的回调按重复处理（返回 200 `SUCCESS`，`notify_count + 1`，outbox 仍只有 1 条）。
- [ ] ACCEPT_TIMEOUT 场景（由 PAY-03 移来）：Pay 接口在 3 秒以上、5 秒以内返回 PAYING，订单最终 SUCCESS。
- [ ] 退款 `channel_txn` 查得 SUCCESS / FAILED 时调用 `ApplyRefundResult`，退款单进入相同终态。

#### PAY-06 商户关单

`P0 · M3 · 依赖：PAY-04、CH-03 · 表：payment_order、channel_txn`

接口：`POST /v1/payments/{pay_no}/close` → `PaymentService.ClosePayment`

规则：

| 当前状态 | 处理 | 返回 |
|---|---|---|
| INIT | CAS `INIT → CLOSED`，写 `closed_at` | CLOSED |
| PAYING | 本地还没有渠道流水号时先查单，查无此单返回 `10004`，不调用关单。已有流水号，或查到渠道已有行，再在事务外关单：`CLOSED` → `ApplyPayResult(CLOSED)`；`ALREADY_PAID` → 立即查单并 `ApplyPayResult(SUCCESS)`；`FAILED`（渠道交易已失败）→ `ApplyPayResult(FAILED, CHANNEL_FAILED)`；调用失败 → 不改状态 | CLOSED；已支付返回 `30003` 和 SUCCESS 订单；已失败返回 `30003` 和 FAILED 订单；调用失败返回 `10004` |
| CLOSED | 幂等 | CLOSED |
| SUCCESS、FAILED | 不处理 | `30003` |

PAYING 状态必须等渠道确认关闭后才能 CLOSED，避免“本地已关、渠道扣款成功”。

说明：NEVER_FINISH、CLOSE_RACE 由 CH-01 的场景识别和 CH-03 的关单实现。CH-06 现已 DONE，但本功能点的验收仍不使用 SLOW 和 ACCEPT_TIMEOUT，所以依赖列仍不写 CH-06。“商户最终入账”需要 MQ-02 / MQ-04，移到 MQ-04 验收。

验收：

- [x] NEVER_FINISH 场景关单后订单 CLOSED，渠道侧 `mock_txn` 为 CLOSED，之后不再有回调。
- [x] CLOSE_RACE 场景关单返回 `30003`，订单 SUCCESS，outbox 恰好 1 条 `payment.succeeded`。
- [x] 对 INIT 订单并发执行 Pay 与 Close（由 PAY-03 移来），结果只能是 PAYING 或 CLOSED 之一。

#### PAY-07 超时关单

`P0 · M4 · 依赖：PAY-06 · 表：payment_order`

规则：

1. 每 5 秒扫描 `status IN ('INIT', 'PAYING') AND expire_at <= now`，每批最多 200 条（使用 `idx_status_expire_at`）。
2. 对每条执行 PAY-06 的同一逻辑；多实例同时执行时由 CAS 和渠道关单幂等保证正确。

验收：

- [ ] 使用 Fake 时钟推进到过期后，INIT 订单在下一轮变为 CLOSED。
- [ ] 过期的 NEVER_FINISH 订单被关闭，渠道侧同为 CLOSED。

#### PAY-08 支付单查询

`P0 · M3 · 依赖：PAY-02 · 表：payment_order`

接口：`GET /v1/payments/{pay_no}`、`GET /v1/payments?out_trade_no=` → `PaymentService.QueryPayment`

规则：只能查询当前商户的订单。直接读 `payment_order`，不做缓存，不写行，不推进状态。`pay_no` 与 `out_trade_no` 二选一，空串算没提供，不 trim；两个都空或两个都非空返回已有的 `10001`，不读库。只提供 `pay_no` 时，没有行，或 `merchant_id` 与库中逐字节不同（含空商户号），都返回已有的 `30001`。只提供 `out_trade_no` 时，没有行返回 `30001`；他人的商户订单号因此也是 `30001`。两种情况同一个错误，不泄露订单是否存在。查到且商户匹配则原样返回该行，状态可以是 INIT、PAYING、SUCCESS、FAILED、CLOSED。

验收：

- [x] 用商户 B 查询商户 A 的 pay_no 返回 `30001`。

#### PAY-09 立即同步渠道状态

`P0 · M6 · 依赖：PAY-05`

接口：`PaymentService.SyncChannelTxn(biz_no)` → `{local_status, channel_status, changed}`（仅对账自动处理 REC-06 使用）

规则：立即对该渠道交易查单一次；本地仍为 PROCESSING 时按 PAY-05 的分发规则推进；本地已是终态时只返回双方状态，不做修改。

验收：

- [ ] 本地 PAYING、渠道 SUCCESS，调用后订单 SUCCESS，`changed = true`。
- [ ] 本地 CLOSED、渠道 SUCCESS，调用后订单仍为 CLOSED，返回双方状态。

#### PAY-10 商户费率变更与订单手续费核对

`P0 · M3 · 依赖：BASE-03、BASE-04、BASE-08 · 表：merchant_fee_rate、merchant、payment_order`

说明：生效费率放在只追加的 `merchant_fee_rate`。`merchant.fee_rate_bp` 仍只是种子写下的初始费率，更改费率不更新该列。订单手续费核对只报告差异，不改 `payment_order.fee_amount`，不改账，也不重算流水。流水核对仍是 ACC-10。不新增错误码，不提供管理端口，不把费率放进 Redis。核对是可调用的扫描函数；本次不在 `cmd/payment/main.go` 里启动死循环，进程常驻后按 §2.8 每 1 分钟调一次。

规则：

1. 新迁移 `migrations/payment/000002_merchant_fee_rate.up.sql` 建 `merchant_fee_rate`（只追加，无 `updated_at`）。列：`id`、`merchant_id`、`fee_rate_bp`（0–10000）、`effective_from`、`created_at`。唯一键 `(merchant_id, effective_from)`。up 用一条 `INSERT … SELECT` 给已有商户补第一档，费率和生效时间分别取 `merchant.fee_rate_bp`、`merchant.created_at`；没有商户时插入 0 行。另给 `payment_order` 加索引 `idx_pay_merchant_created (merchant_id, created_at)`。down 删该索引并删除新表。不改已有迁移。
2. `AppendFeeRate(ctx, merchantID, rateBP, effectiveFrom)` 接收调用方传入的 `*sqlx.DB`（由构造函数持有），写路径用 `dbx.WithTx`。`s`、`ctx` 或库为 nil 返回 `10003`；`ctx` 已取消则原样返回该错误。`merchantID` 为空、`rateBP` 不在 0–10000、`effectiveFrom` 为零值：返回 `10001`，不写行。
3. 比较和写入前把 `effectiveFrom` 截断到毫秒。时区保持调用方传入的时刻，不调用 `time.Now`，不调用 `time.LoadLocation`。
4. 同一事务 `SELECT merchant_id FROM merchant WHERE merchant_id = ? FOR UPDATE`。没有行返回 `20004`。DISABLED 也允许登记；停用只拒绝交易，不拒绝登记费率。
5. 读取该商户 `effective_from` 最大的一档。没有历史行时，这一次就是第一档，直接插入。已有一档时，新时间必须严格晚于它才算新档；更早返回 `10001`，不插入。相同时间不在这里拒绝，交给下面的唯一键。商户行锁保证“晚于当前最大生效时间”在提交时仍成立。
6. 插入。1062 用 `dbx.IsDuplicate` 识别，不解析错误文本，也不因此回滚（1062 不会中止当前事务）。再读同一 `(merchant_id, effective_from)`：费率相同则 `Duplicated: true` 并返回已有行；费率不同返回 `10006`，不改旧行。不修改 `merchant` 的任何列，不改已有费率行。
7. `CheckOrderFees(ctx, from, to)` 一次只读事务，隔离级别 `sql.LevelRepeatableRead`，读完 Rollback。不用 `dbx.WithTx`（它是 READ COMMITTED，且最长 3 秒）。不在这条 BEGIN 之外再开事务，不更新任何行。`from` 含、`to` 不含，比较 `payment_order.created_at`。`from` 或 `to` 为零值表示该侧没有界。
8. 先读出全部费率行，按商户、`effective_from` 升序放内存。再按 `payment_order.id` 升序分页读订单（每页 200 行），列只有 `pay_no`、`merchant_id`、`amount`、`fee_amount`、`created_at`。对每张单取 `effective_from <= created_at` 的最近一档。没有这一档：计入不符，费率记 -1，应收手续费记 0。有这一档：用 `money.Fee(amount, rateBP)` 得到应收手续费，与 `fee_amount` 不同则计入不符。`money.Fee` 返回错误时整次核对失败（`10003`）并停止，不把该单当成不符，也不返回部分成功结果。
9. 返回扫描张数、不符张数，以及最多 100 条明细（`pay_no`、商户号、创建时间、订单金额、实收手续费、应收手续费、费率）。不符张数是全量，不是明细条数。金额用 `money.Amount`。不读、不写商品标题。不自动改 `fee_amount`，不自动改账。
10. 扫描开始之后新提交的费率，要等下一轮才能看见。这是这一轮 REPEATABLE READ 只读快照的结果，不是漏扫后补写。
11. 种子在商户插入成功或已存在后，于同一个 payment 事务里幂等补第一档：`INSERT INTO merchant_fee_rate … SELECT merchant.merchant_id, merchant.fee_rate_bp, merchant.created_at FROM merchant WHERE merchant.merchant_id = ?`。冲突时 `ON DUPLICATE KEY UPDATE merchant_id = merchant_fee_rate.merchant_id`。未限定的 `merchant_id` 在这条 `INSERT … SELECT` 里会报 1052，所以列名带表名；赋值不碰 `fee_rate_bp` 和 `effective_from`。用库里的费率和 `created_at`，不用本次参数覆盖已有商户。重复种子不新增第二档，也不改费率，也不改已有密文。

异常：参数不合法或新时间早于已有最大生效时间 → `10001`；商户不存在 → `20004`；相同生效时间但费率不同 → `10006`；依赖失败 → `10003`。已取消的 context 原样返回。

验收：

- [x] 已有一档之后，更晚的时间追加成功，行数加 1，`merchant.fee_rate_bp` 不变。
- [x] 同一生效时间再提交同一费率，返回已有行且 `Duplicated == true`，行数仍是 1。并发提交同一时间同一费率也只留 1 行。
- [x] 同一时间不同费率返回 `10006`，旧费率还在。
- [x] 更早的时间返回 `10001`，不新增行。
- [x] 不存在的商户返回 `20004`。
- [x] 两档费率：0bp 从 T0，60bp 从 T1。金额 10000 分的订单，创建时间在 T1 之前且 `fee_amount = 0` 通过；创建时间在 T1 之后且 `fee_amount = 60` 通过；创建时间在 T1 之后但 `fee_amount = 0` 被报出，库里的 `fee_amount` 仍是 0。没有落在任何档内的订单被报出。扫描张数和不符张数正确。
- [x] 种子：`M100002` 有且只有一档 60bp，生效时间等于该商户 `created_at`。再执行一次种子，档数仍是 1，费率仍是 60。

### 4.5 退款（REF）

退款状态机：

```
INIT ──冻结成功──► PROCESSING ──渠道成功──► SUCCESS
 │                    └────────渠道失败──► FAILED
 └──冻结失败 / 超时未提交──────────────────► FAILED
```

INIT 表示已受理、尚未提交渠道；PROCESSING 表示已冻结商户资金并已（或正在）提交渠道。资金处理采用简化的 TCC：冻结（Try）→ 确认扣款（Confirm）或解冻（Cancel）。

#### REF-01 创建退款与额度占用

`P0 · M4 · 依赖：PAY-04 · 表：refund_order、payment_order`

接口：`POST /v1/refunds` → `PaymentService.CreateRefund`

规则：

1. 入参（`docs/05-api.md` §4.5）：`pay_no` 或 `out_trade_no` 二选一，`out_refund_no`、`amount` 必填，`reason` 可选（≤ 255）。
2. 原订单必须属于当前商户（否则 `30001`）且为 SUCCESS（否则 `30003`）。
3. 幂等：`(merchant_id, out_refund_no)` 已存在时，`pay_no` 与 `amount` 一致返回已有退款单，否则 `10006`；插入时的 1062 同样处理。
4. 事务：

   ```
   UPDATE payment_order SET refunded_amount = refunded_amount + ?, version = version + 1
    WHERE pay_no = ? AND status = 'SUCCESS' AND refunded_amount + ? <= amount
   -- 影响行数 0 → 30005
   INSERT refund_order(refund_no, ..., status = 'INIT', channel = 原订单渠道)
   ```

5. 事务提交后在同一请求内执行 REF-02，返回 REF-02 结束时的退款单。

本功能点事务提交后返回刚插入的 INIT 退款单；同一请求内冻结和提交渠道是 REF-02，本次不调用 account、不调用渠道、不写 outbox。条件更新影响 0 行或插入 1062 时，事务外再读 (merchant_id, out_refund_no)，一致则返回已有单，避免并发重复请求被报成 30005。

验收：

- [x] 订单 10000，10 个并发请求各退 3000（不同 out_refund_no），恰好 3 个创建成功，其余 `30005`，`refunded_amount = 9000`。
- [x] 相同 `out_refund_no` 重复请求返回同一 `refund_no`；金额不同返回 `10006`。
- [x] 对 PAYING 订单退款返回 `30003`。

#### REF-02 退款冻结与渠道提交

`P0 · M4 · 依赖：REF-01、ACC-11、CH-04、BASE-15 · 表：refund_order、channel_txn`

规则：

1. 调用 `AccountService.Freeze(REFUND, refund_no, merchant_id, amount)`（account 按商户号路由到所属分片，payment 不需要先查账户号）：
   - 成功 → 第 2 步。
   - `40002` 余额不足 → `ApplyRefundResult(FAILED, fail_code = INSUFFICIENT_BALANCE)`，接口返回 `40002` 和退款单。
   - `40004`（冻结单已被解冻）→ `ApplyRefundResult(FAILED, fail_code = FREEZE_CONFLICT)`，接口返回 code 0 和 FAILED 退款单。
   - 超时或不可用 → 保持 INIT，接口返回 INIT 状态，由 REF-04 继续。
   - `40001`（账户不存在）、`40005`（账户非 ACTIVE）→ 属于内部数据错误（种子数据保证账户存在，且没有停用入口）：保持 INIT，接口返回 `10003`，错误日志并告警；REF-04 满 1 小时后按 `REFUND_TIMEOUT` 失败。
2. 故障点 `refund.after_freeze`。事务：CAS `INIT → PROCESSING`；插入 `channel_txn(REFUND, PROCESSING, next_query_at = now + 10s)`。
3. 事务外调用渠道退款，`req_no = refund_no`、`orig_req_no = pay_no`；超时规则同 PAY-03：
   - `OK` → 回填 `channel_txn_no`。
   - `REJECTED` → `ApplyRefundResult(FAILED, fail_code = CHANNEL_REJECTED)`，`fail_msg` 记录渠道 `reason`，接口返回 `30006` 和 FAILED 退款单（与 PAY-03 一致）。
   - 未知 → 不改状态，由回调或查单完成。
4. 已知边界：支付成功到商户入账之间有秒级延迟，入账前发起的退款会因余额不足失败，商户可用新的 `out_refund_no` 重试。

新插入的退款提交后才冻结并提交渠道。已有 `(merchant_id, out_refund_no)` 直接返回已有单，不再次冻结。冻结超时或不可用返回 INIT，code 0，不写 `channel_txn`。account 把处理超时包成远程 `TransError`（消息仍是 rpc timeout）时与客户端超时同一口径，也走这条 INIT 路径。连接拒绝仍是 `10003`，不走这条路径。`40001` 与 `40005` 保持 INIT 并返回 `10003`。`40002`、`40004` 和渠道 `REJECTED` 调用 `ApplyRefundResult`，落库规则见 REF-03（2026-11-05 已接通）。`CreateRefund` 的服务端处理超时是 5 秒，与 `Pay` 和 §5.4.1 的网关调用超时一致，让渠道退款的 3 秒总超时先到期。`ClosePayment` 和其他方法仍是 2 秒。不新增环境变量。

验收的归属：规则 1、3 中置 FAILED 的分支调用 `ApplyRefundResult`（REF-03），在 REF-03 接通并验收；“随后 SUCCESS”经回调推进，在 REF-03 验收；“恢复后完成”是 REF-04 的扫描，在 REF-04 验收；“冻结最终被解冻”是 account 消费 `refund.failed`，在 MQ-04 验收。测试中商户余额用 account biz 夹具直接入账，不依赖 MQ-04。

验收：

- [x] 正常退款：接口返回 PROCESSING；商户有 1 张 FROZEN 冻结单（金额等于退款金额），`channel_txn(REFUND)` 1 条且为 PROCESSING，渠道受理 1 次。
- [x] account 不可用（Freeze 超时）：接口返回 INIT，退款单保持 INIT，没有 `channel_txn`，渠道未收到请求。

#### REF-03 退款结果推进

`P0 · M4 · 依赖：REF-02、GW-06 · 表：refund_order、channel_txn、payment_order、outbox`

说明：`ApplyRefundResult(refund_no, result, source)`，被回调、查单、REF-02 共用。本功能点同时接通 REF-02 规则 1、3 的 FAILED 分支和 GW-06 规则 3 的 REFUND 回调分支。account 侧的确认扣款与解冻（规则 3）在 MQ-04 验收。

规则：

1. 渠道结果（PROCESSING 退款）事务：CAS `channel_txn PROCESSING → X`（规则同 PAY-04 第 2 条）；CAS `refund_order PROCESSING → X`；
   - SUCCESS：写 `succeeded_at`（渠道完成时间），`outbox.Add(refund.succeeded)`。
   - FAILED：`fail_code = CHANNEL_FAILED`；`UPDATE payment_order SET refunded_amount = refunded_amount - ? WHERE pay_no = ? AND refunded_amount >= ?`，`outbox.Add(refund.failed)`。
2. 冻结失败或超时失败（INIT 退款）事务：CAS `refund_order INIT → FAILED`；释放 `refunded_amount`，条件同第 1 条：`UPDATE payment_order SET refunded_amount = refunded_amount - R WHERE pay_no = ? AND refunded_amount >= R`（R 为退款金额）；`outbox.Add(refund.failed)`。
3. account 消费 `refund.succeeded` 执行 ConfirmFreeze（贷方为本分片渠道应收子账户），消费 `refund.failed` 执行 ReleaseFreeze（冻结不存在时为空回滚，ACC-05）。
4. 重复、冲突结果的处理同 PAY-04。
5. 第 1、2 条释放 `refunded_amount` 的 UPDATE 影响 0 行（`refunded_amount < R`，数据已不一致）时：整个事务回滚，`channel_txn`、`refund_order` 的 CAS 和 outbox 都不提交；返回 `10003`；输出 ERROR 日志（`refund_no`、`pay_no`、R，不含密钥），`pay_refund_quota_inconsistent_total + 1`。退款保持原状态，由回调重试（网关对 `10003` 返回 HTTP 500，见 GW-06 第 4 条，渠道按 CH-05 重试）、查单补偿（PAY-05）或卡单恢复（REF-04）再次调用，每次都再次暴露，直到人工修复数据。（2026-10-27 用户决定）

实现说明（2026-11-05，REF-03 落地时的澄清，不改资金流向、表结构和错误码）：

- 分支按这次结果的意图选，不按库里读到的状态选。FAILED 且 `fail_code` 为 `INSUFFICIENT_BALANCE`、`FREEZE_CONFLICT`、`REFUND_TIMEOUT` 走第 2 条，只 CAS `refund_order INIT → FAILED`，不碰 `channel_txn`。其余结果走第 1 条：SUCCESS，以及 FAILED 且 `fail_code` 为空、`CHANNEL_FAILED` 或 `CHANNEL_REJECTED`。原因：REF-04 判 INIT 超时与 REF-02 把同一笔退款推进为 PROCESSING 可能并发；按意图选时第 2 条的 CAS 未命中并返回 `10003`，不会把已提交渠道的退款判为失败。
- 第 1 条 FAILED 的 `fail_code` 用调用方传入的值，未传时为 `CHANNEL_FAILED`，`fail_msg` 原样写入。REF-02 的渠道受理拒绝因此写 `CHANNEL_REJECTED`，`fail_msg` 为渠道 `reason`。
- 第 1 条的渠道 CAS 另加 `txn_type = 'REFUND'`，同号的 PAY 流水不会被改。渠道 CAS 未命中时按 PAY-04 第 3 条处理：读不到或读到的是 PAY 流水为 `30001`；金额不符为 `50002`，ERROR 日志；同一终态返回当前退款单，`source = NOTIFY` 时 `notify_count + 1`；另一终态计 `pay_callback_conflicts_total`（标签 channel）、ERROR 日志并返回当前退款单；仍是 PROCESSING 为 `10003`。渠道 CAS 命中但退款单 CAS 未命中时整个事务回滚，ERROR 日志，返回 `10003`。
- 第 2 条的 CAS 未命中时，事务外再读退款单：不存在为 `30001`；已 FAILED 视为重复，返回该单；已 SUCCESS 返回该单且 error 为 nil，WARN 日志，不计 `pay_callback_conflicts_total`；仍是 INIT 或 PROCESSING 为 `10003`，WARN 日志。已 SUCCESS 是并发竞争（例如 REF-04 判超时的同时另一路冻结、提交并收到 SUCCESS），不是渠道给出的矛盾结果，所以不算冲突。返回该单而不是 `10003`，是因为退款已到终态，与 PAY-04 “已是终态则返回当前单”一致：REF-02 据此返回 code 0 和 SUCCESS 退款单，REF-04 按返回的状态判断，不必重试。
- 入参不合法返回 `10001`，不写库：`refund_no` 为空；结果不是 SUCCESS / FAILED；来源不是 NOTIFY / QUERY / JOB / ADMIN / SYNC（退款没有关单，不接受 CLOSE）；SUCCESS 没有渠道完成时间或带了 `fail_code`；`fail_code` 不是空串、`CHANNEL_FAILED`、`CHANNEL_REJECTED`、`INSUFFICIENT_BALANCE`、`FREEZE_CONFLICT`、`REFUND_TIMEOUT` 之一。
- 回调入口 `ApplyRefundNotify` 只接受 `source = NOTIFY`，且 `fail_code` 不能是第 2 条的三种。提交后调用故障点 `callback.after_commit`，返回 `APPLIED` / `DUPLICATE` / `CONFLICT`，判定与 GW-06 的 PAY 分支相同。GW-06 handler 收到既不是 PAY 也不是 REFUND 的 `txn_type` 时返回 `10001`（原为占位的 `10003`，网关对两者都返回 HTTP 500）。
- SUCCESS 的 `succeeded_at` 与 `refund.succeeded` 的 `succeeded_at` 都用渠道完成时间；`refund.failed` 的 `fail_code` 用退款单上落库的值。
- 第 5 条的计数先在进程内累加，由 OPS-03 暴露。ERROR 日志在事务回滚后输出一次，记录时回滚已经完成（释放额度影响 0 行返回的错误不是 MySQL 错误号，`dbx.WithTx` 不重试）；消息为 `refund quota inconsistent`，字段 `refund_no`、`pay_no`、`amount`（R，int64 分）。生产格式的 `refund_no`、`pay_no` 都是两位前缀加 19 位数字，按约定 §7.5 的兜底扫描会被部分遮盖（数字段只留前 6 位、后 4 位），日志里看不到完整单号。人工修复时先看 `pay_refund_quota_inconsistent_total` 发现问题，再用 SQL 定位：按 `pay_no` 比较 `payment_order.refunded_amount` 与该支付单下 status 不是 FAILED 的 `refund_order.amount` 合计，两者不等的支付单就是数据不一致的那笔。不改 `pkg/logx`。

验收：

- [x] 正常退款经渠道回调（GW-06 REFUND 分支）推进为 SUCCESS，写 `succeeded_at`，outbox 恰好 1 条 `refund.succeeded`（由 REF-02 移来）。
- [x] FAIL 场景（退款金额末两位 01；测试可直接以渠道 FAILED 结果调用 `ApplyRefundResult`）：退款 FAILED，`fail_code = CHANNEL_FAILED`，`refunded_amount` 恢复，outbox 恰好 1 条 `refund.failed`。
- [x] 商户余额不足（由 REF-02 移来）：接口返回 `40002`，退款 FAILED（`INSUFFICIENT_BALANCE`），`refunded_amount` 恢复，没有 FROZEN 冻结单，outbox 1 条 `refund.failed`。
- [x] 渠道受理拒绝（由 REF-02 移来，用假渠道客户端注入 `REJECTED`）：接口返回 `30006`，退款单 FAILED（`CHANNEL_REJECTED`），`refunded_amount` 恢复，outbox 1 条 `refund.failed`。
- [x] 重复回调只推进一次、只产生 1 条事件。
- [x] 测试用 SQL 把 `refunded_amount` 改为小于 R：以渠道 FAILED 结果调用 `ApplyRefundResult`（PROCESSING 退款），以及对 INIT 退款执行冻结失败分支，都返回 `10003`；退款单、`channel_txn` 保持原状态，`refunded_amount` 不变，outbox 没有 `refund.failed`，`pay_refund_quota_inconsistent_total` 各 +1，各有一条 ERROR 日志。

#### REF-04 退款卡单恢复

`P0 · M4 · 依赖：REF-02、REF-03、BASE-15 · 表：refund_order`

规则：

1. 每 10 秒扫描 `status = 'INIT' AND updated_at <= now − 30s`（使用 `idx_status_updated_at`），从 REF-02 第 1 步重新执行。Freeze 幂等、CAS 互斥、`channel_txn.uk_biz_no` 唯一，因此重复执行安全。
2. INIT 超过 1 小时仍未能推进，按 REF-03 第 2 条置为 FAILED（`fail_code = REFUND_TIMEOUT`）。此时渠道一定未收到请求（提交渠道前已置为 PROCESSING），失败不会造成资损。

验收：

- [ ] 在故障点 `refund.after_freeze` 处 kill 进程，重启后退款被推进为 PROCESSING 并提交渠道（渠道受理 1 次），冻结单只有 1 张（C6）。
- [ ] account 不可用期间退款保持 INIT；account 恢复后的下一轮扫描把它推进为 PROCESSING 并提交渠道（由 REF-02 移来）。
- [ ] account 持续不可用 1 小时（Fake 时钟），退款 FAILED（`REFUND_TIMEOUT`），`refunded_amount` 恢复。

#### REF-05 退款查询

`P0 · M4 · 依赖：REF-01 · 表：refund_order`

接口：`GET /v1/refunds/{refund_no}`、`GET /v1/refunds?out_refund_no=` → `PaymentService.QueryRefund`

规则：同 PAY-08，只能查本商户退款单。

验收：

- [ ] 越权查询返回 `30001`。

### 4.6 转账（TRF）

商户之间的余额划转，由付款商户发起。payment 持有 `transfer_order` 并作为协调者；account 提供幂等的分支操作。

转账单状态机：

```
同分片（mode = LOCAL）：
TRYING ──TransferLocal 成功──► SUCCESS
   └────TransferLocal 失败──► FAILED

跨分片（mode = TCC）：
TRYING ──两个 Try 都成功（决策落盘）──► CONFIRMING ──两个 Confirm 都成功──► SUCCESS
   └────任一 Try 失败或结果未知 ────► CANCELLING ──两个 Cancel 都成功──► FAILED
```

- TRYING 是唯一可以“改变主意”的状态；CONFIRMING、CANCELLING 一旦写入只能前进，所有分支操作重试到成功为止。
- 转账不经过 Kafka 记账：两个分支都由协调者同步驱动，事件只用于商户通知。这是 D6“加钱异步”的例外：收款方入账与付款方扣款必须在同一个协调过程中完成，才能保证在途守恒。

#### TRF-01 创建转账单

`P0 · M4 · 依赖：PAY-01、ACC-13 · 表：transfer_order、merchant`

接口：`POST /v1/transfers` → `PaymentService.CreateTransfer`

规则：

1. 入参（`docs/05-api.md` §4.8）：`out_transfer_no`（1–64 位 `[A-Za-z0-9_-]`）、`payee_merchant_id`、`amount`（1–10,000,000）必填；`remark` ≤ 128 字符；`notify_url` 可选，白名单校验同 PAY-02，未传时存空串、通知时取商户默认地址（同支付单）。
2. 收款商户必须存在、ACTIVE，且不是付款商户本身，否则 `30008`。
3. 幂等：`(merchant_id, out_transfer_no)` 已存在时，`payee_merchant_id`、`amount`、`notify_url` 全部一致 → 返回已有转账单（不论当前状态，不在本次请求中重新执行）；否则 `10006`。插入遇 1062 同样处理。
4. 插入 `transfer_order`：`transfer_no`（`TF` 前缀）；`payer_shard`、`payee_shard` 由 `shard.Of` 计算；两者相同 `mode = LOCAL`，否则 `TCC`；`status = TRYING`；`next_retry_at = now + TRANSFER_RECOVER_DELAY`（默认 10 秒，供 TRF-04 接管）。
5. 提交后在同一请求内执行 TRF-02 或 TRF-03，返回执行结束时的转账单。
6. 响应：本次请求执行到 FAILED 且 `fail_code = INSUFFICIENT_BALANCE` 时返回 `40002` 并在 `data` 中携带转账单（与 REF-02 一致）；其他情况（SUCCESS、处理中状态 TRYING / CONFIRMING / CANCELLING、其他 FAILED，以及第 3 条的幂等命中）一律返回 code 0 和转账单。商户应把非终态视为“处理中”，以通知或查询为准。

异常：参数错误 → `10001`；收款方非法 → `30008`；幂等冲突 → `10006`。

验收：

- [ ] 100 个并发相同请求只生成 1 张转账单，全部返回同一 `transfer_no`。
- [ ] 相同 `out_transfer_no`、不同金额返回 `10006`。
- [ ] 转给自己、转给不存在或停用的商户返回 `30008`。
- [ ] `M100001 → M100003` 为 LOCAL，`M100001 → M100002` 为 TCC。

#### TRF-02 同分片转账执行

`P0 · M4 · 依赖：TRF-01、ACC-07、ACC-11、MQ-01 · 表：transfer_order、outbox`

规则：

1. 调用 `AccountService.TransferLocal(transfer_no, payer, payee, amount)`。
2. 返回 SUCCESS → 事务：CAS `TRYING → SUCCESS`，写 `txn_no`、`finished_at`；`outbox.Add(transfer.succeeded)`。
3. 返回 FAILED → 事务：CAS `TRYING → FAILED`，`fail_code` 取返回值，写 `finished_at`；`outbox.Add(transfer.failed)`。
4. 超时、`10003`、`10004` → 保持 TRYING，接口返回 TRYING，由 TRF-04 重新调用（ACC-07 保证结果确定）。`10001`、`10006` 说明协调者与 account 数据不一致，同样保持 TRYING 并输出 ERROR 日志，由 `pay_transfer_stuck` 暴露；不能直接置 FAILED，因为 `10006` 时 account 可能已有 SUCCESS 分支（R15）。
5. `40001`、`40005` → 属于内部数据错误（创建时已校验收款方，账户没有停用入口），事务：CAS `TRYING → FAILED`（`fail_code = ACCOUNT_INVALID`），写 `finished_at`，`outbox.Add(transfer.failed)`，ERROR 日志。
6. CAS 影响 0 行：重新读取，按当前状态返回。

验收：

- [ ] A 余额 1000，100 个并发请求各转 20 给同分片的 C（不同 `out_transfer_no`），恰好 50 个 SUCCESS、50 个 FAILED（`40002`），A + C 不变，`CheckBalance` 通过。
- [ ] account 不可用：接口返回 TRYING，转账单保持 TRYING，没有转账凭证（恢复后的推进由 TRF-04 验收）。

#### TRF-03 跨分片转账 TCC 协调

`P0 · M4 · 依赖：TRF-01、ACC-04~06、ACC-14、ACC-11、MQ-01、BASE-15 · 表：transfer_order、outbox`

规则：

1. Try：`TRANSFER_PARALLEL_BRANCH = true`（默认）时用 errgroup 并行发起，否则先付款方后收款方：
   - 付款方：`Freeze(TRANSFER, transfer_no, payer, amount)`。
   - 收款方：`TransferInTry(transfer_no, payee, amount)`。
   - 两个调用都等待结束（一个失败不取消另一个，避免对方结果未知），每个调用按 ACC-11 超时并重试 1 次。
2. 故障点 `transfer.after_try`。
3. 决策：
   - 两个 Try 都成功 → CAS `TRYING → CONFIRMING`。
   - 否则 → CAS `TRYING → CANCELLING`，写 `fail_code`：付款方 `40002` → `INSUFFICIENT_BALANCE`；`40001` / `40005` → `ACCOUNT_INVALID`；超时、`10003`、`10004`、`40004`，以及不应出现的 `10001`、`10006`（另输出 ERROR 日志，R15）→ `TRY_TIMEOUT`。两个分支都失败时取付款方的原因。
   - CAS 影响 0 行：决策已被 TRF-04 做出，重新读取并按当前状态继续第 5 或第 6 步。
4. 故障点 `transfer.after_decision`。
5. CONFIRMING：并行 `ConfirmFreeze(TRANSFER, transfer_no, payer, "")` 与 `TransferInConfirm(transfer_no, payee)`（串行模式下先付款方，之后经过故障点 `transfer.after_payer_confirm`）。
   - 都成功 → 事务：CAS `CONFIRMING → SUCCESS`，写 `finished_at`；`outbox.Add(transfer.succeeded)`。
   - 任一失败 → 保持 CONFIRMING，接口返回 CONFIRMING，由 TRF-04 重试。
   - 返回 `40004`（冻结单或分支已被取消）→ 严重不一致：`pay_transfer_inconsistent_total + 1`，ERROR 日志，保持 CONFIRMING。按设计不可能出现，由 ACC-14 验收与 C9–C11 证明。
6. CANCELLING：并行 `ReleaseFreeze(TRANSFER, transfer_no, payer, amount)` 与 `TransferInCancel(transfer_no, payee, amount)`。分支不存在时两者都按空回滚写墓碑，之后迟到的 Try 返回 `40004`，不会冻结资金或占用分支（防悬挂）。
   - 都成功 → 事务：CAS `CANCELLING → FAILED`，写 `finished_at`；`outbox.Add(transfer.failed)`。
   - 任一失败 → 保持 CANCELLING，由 TRF-04 重试；返回 `40004`（已确认）按第 5 条的严重不一致处理。
7. 各阶段耗时埋点 `pay_stage_duration_seconds{chain="transfer_tcc", stage}` 由 OPT-01（M5）加入并验收，本功能点不实现。

验收：

- [ ] `M100001 → M100002` 转 500：转账 SUCCESS；分片 1 有 `TRANSFER_OUT`，分片 0 有 `TRANSFER_IN`；两个分片在途合计 0；`CheckBalance` 通过。
- [ ] 付款方余额不足：转账 FAILED（`40002`），收款方分支 CANCELLED，付款方冻结单 RELEASED 墓碑，没有任何转账凭证。
- [ ] 注入收款方 Try 超时（测试替身）：转账 FAILED（`TRY_TIMEOUT`），付款方冻结被释放，可用余额恢复。
- [ ] 注入付款方 Confirm 失败一次：接口返回 CONFIRMING，转账单保持 CONFIRMING（之后的推进由 TRF-04 验收）。
- [ ] A、B 分属两个分片且各 10000，A→B 与 B→A 各 200 笔并发转 1：全部终态后 A + B = 20000，在途合计 0，`CheckBalance` 通过。
- [ ] 并行与串行模式下以上结果一致。

#### TRF-04 转账恢复任务

`P0 · M4 · 依赖：TRF-02、TRF-03、BASE-15 · 表：transfer_order`

规则：

1. 每 `TRANSFER_RECOVER_INTERVAL`（默认 1 秒）抢占：

   ```
   BEGIN
     SELECT * FROM transfer_order
      WHERE status IN ('TRYING', 'CONFIRMING', 'CANCELLING') AND next_retry_at <= now
      ORDER BY next_retry_at LIMIT 100 FOR UPDATE SKIP LOCKED
     UPDATE ... SET retry_count = retry_count + 1, next_retry_at = now + min(2^retry_count 秒, 60 秒)
   COMMIT
   ```

   事务外以最多 20 个并发处理。
2. 处理：
   - TRYING + LOCAL：重新执行 TRF-02（TransferLocal 幂等且结果确定）。
   - TRYING + TCC：CAS `TRYING → CANCELLING`（`fail_code = TRY_TIMEOUT`），然后执行 TRF-03 第 6 步。原请求线程即使还活着，也会在第 3 步 CAS 失败；它发出的迟到 Try 被墓碑拦截。
   - CONFIRMING：执行 TRF-03 第 5 步。
   - CANCELLING：执行 TRF-03 第 6 步。
3. CONFIRMING、CANCELLING 永不放弃。`retry_count ≥ 10` 的转账计入 gauge `pay_transfer_stuck`，每次处理失败输出 ERROR 日志。
4. 所有分支操作幂等、所有状态推进为 CAS，因此多实例并发运行安全（NFR-REL-02）。

验收：

- [ ] 在 `transfer.after_try` kill payment，重启后转账 FAILED，两个分支都已取消，冻结已释放（C9）。
- [ ] 在 `transfer.after_decision` kill payment，重启后转账 SUCCESS，凭证各 1 张（C10）。
- [ ] 串行模式下在 `transfer.after_payer_confirm` kill payment：重启前在途合计为 T，重启后转账 SUCCESS，在途合计回到 0（C11）。
- [ ] account 停止 1 分钟期间发起的 TCC 转账，恢复后全部终态，`CheckBalance` 通过。
- [ ] LOCAL 转账在 account 不可用期间保持 TRYING，account 恢复后推进为终态，只有 1 张 `TRANSFER` 凭证（由 TRF-02 移来）。
- [ ] 付款方 Confirm 注入失败一次后停在 CONFIRMING 的转账，由本任务推进为 SUCCESS，`TRANSFER_OUT`、`TRANSFER_IN` 各 1 张（由 TRF-03 移来）。

#### TRF-05 转账查询

`P0 · M4 · 依赖：TRF-01 · 表：transfer_order`

接口：`GET /v1/transfers/{transfer_no}`、`GET /v1/transfers?out_transfer_no=` → `PaymentService.QueryTransfer`

规则：只有付款商户可以查询；收款商户与其他商户查询返回 `30001`。

验收：

- [ ] 收款商户查询返回 `30001`。

### 4.7 本地消息表与 MQ（MQ）

#### MQ-01 outbox 写入

`P0 · M3 · 依赖：BASE-08、BASE-05 · 表：outbox`

`pkg/outbox.Add` 在调用方已经打开的事务里插入一行 `outbox`。本功能点不投递、不发送 Kafka、不从 `Pay` 或 `ApplyPayResult` 调用。

规则：

1. 签名为 `Add(ctx context.Context, tx *sqlx.Tx, evt Event, ids *idgen.Generator, clk clock.Clock) error`。`tx == nil` 返回错误，不插行。函数自己不执行 `BEGIN`、`COMMIT` 或 `ROLLBACK`。调用方用 `dbx.WithTx` 包住业务写入和 `Add`，`Add` 在提交前执行。
2. `event_id` 只用 `(*idgen.Generator).NewNo(ctx, "EV")`。前缀 `EV` 已在约定 §2.2 登记。时钟回拨返回 `idgen.ErrClockRollback`，不插行，不映射成新的错误码（调用方以后映射为已有的 `10004`）。`ids`、`clk` 或 `ctx` 为 nil 时返回错误，不插行。
3. 只接受下面五种 `event_type`。缺字段、未知类型、金额不是正整数分、`fee_amount` 为负、转账 `status` 不是该事件要求的取值，都返回包内哨兵错误，不发号、不插行，不登记新的 errno。空字符串和零值时间算缺字段。`fee_amount` 可以为 0。1062 返回错误，不换一个 `event_id` 重试，也不当成成功。

   | event_type | topic | msg_key | biz_no | data 字段 |
   |---|---|---|---|---|
   | `payment.succeeded` | `pay.payment.succeeded` | `pay_no` | `pay_no` | `pay_no`、`merchant_id`、`out_trade_no`、`channel`、`amount`、`fee_amount`、`paid_at` |
   | `refund.succeeded` | `pay.refund.succeeded` | 原支付单 `pay_no` | `refund_no` | `refund_no`、`pay_no`、`merchant_id`、`out_refund_no`、`channel`、`amount`、`succeeded_at` |
   | `refund.failed` | `pay.refund.failed` | 原支付单 `pay_no` | `refund_no` | `refund_no`、`pay_no`、`merchant_id`、`out_refund_no`、`amount`、`fail_code` |
   | `transfer.succeeded` | `pay.transfer.succeeded` | `transfer_no` | `transfer_no` | `transfer_no`、`out_transfer_no`、`merchant_id`、`payee_merchant_id`、`amount`、`mode`、`status`（必须是 `SUCCESS`）、`finished_at` |
   | `transfer.failed` | `pay.transfer.failed` | `transfer_no` | `transfer_no` | `transfer_no`、`out_transfer_no`、`merchant_id`、`payee_merchant_id`、`amount`、`mode`、`status`（必须是 `FAILED`）、`fail_code`、`finished_at` |

   退款的 `msg_key` 是 `pay_no`，不是 `refund_no`。`biz_no` 才是 `refund_no`。
4. 信封写入 `payload`（JSON）。`version` 为数字 1。`event_id`、`event_type` 与列一致。金额在 JSON 里是整数分，禁止浮点。`paid_at`、`succeeded_at`、`finished_at` 由调用方传入，先截到毫秒，再格式化为东八区固定偏移的 RFC3339（示例 `2026-10-12T10:00:01.300+08:00`），不改成 `Add` 自己的时钟。
5. `occurred_at` 取这次 `Add` 的注入时钟，先截到毫秒，格式与上一款相同。`Add` 在提交前执行，不能改成提交后再读时钟。`next_retry_at`、`created_at`、`updated_at` 用同一次时钟。时区用 `time.FixedZone("Asia/Shanghai", 8*60*60)`，不调用 `time.LoadLocation`，也不调用 `time.Now`。INSERT 用参数化 SQL，显式写 `created_at`、`updated_at`。
6. `trace_context` 从 `ctx` 取 OpenTelemetry span。trace id 和 span id 都有效时，写成 W3C `traceparent`：`00-<32位trace hex>-<16位span hex>-<2位flags hex>`。没有有效 span 时，`trace_context` 是空对象 `{}`。不新增别的上下文字段。
7. 状态列 `PENDING`，`retry_count` 为 0，`last_error` 为 NULL，`sent_at` 为 NULL。日志不打印整份 payload；payload 里没有密钥，日志里也不要出现密钥。

验收：

- [x] 业务事务回滚时 outbox 无记录；提交时恰好 1 条。

#### MQ-02 outbox 投递器

`P0 · M5 · 依赖：MQ-01、MQ-03、BASE-15 · 表：outbox`

规则：

1. payment 进程内运行，每 200ms 执行一轮。领取、发送、回写分开，发送不在事务里，不改表结构，租约放在 `next_retry_at`：

   ```
   -- 事务 1：领取并写租约
   BEGIN
     SELECT * FROM outbox WHERE status = 'PENDING' AND next_retry_at <= now
      ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
     UPDATE outbox SET next_retry_at = L WHERE id IN (...)    -- L = now + 30s，截到毫秒（与列精度一致）
   COMMIT

   -- 事务外：批量同步发送（acks=all、幂等生产者，等待全部确认，超时 5s）
   故障点 outbox.after_send                                    -- 发送之后、回写之前

   -- 逐条 CAS 回写，条件都是 WHERE id = ? AND status = 'PENDING' AND next_retry_at = L
   成功：status = 'SENT', sent_at = now
   失败：retry_count + 1，next_retry_at = now + min(2^retry_count 秒, 5 分钟)，last_error 截断到 512 字符
         retry_count 达到 20：status = 'DEAD'
   影响 0 行：租约已过期并被其他实例重新领取，本次结果丢弃，不报错
   ```

2. 发送期间不持有行锁。租约 30 秒大于发送超时 5 秒与事务上限 3 秒之和，正常情况下回写在租约内完成，其他实例领不到同一行，所以不注入故障时每条事件只发送一次。
3. 发送后、回写前崩溃，或回写失败，该行仍是 PENDING、`next_retry_at = L`；租约到期后被任一实例重新领取并再次发送，重复由消费端幂等吸收。
4. 业务正确性不依赖消息顺序：退款冻结在创建时同步完成，入账与退款确认之间没有先后依赖。
5. 每 10 秒更新指标 `pay_outbox_pending`、`pay_outbox_dead`。

验收：

- [ ] Kafka 停止 2 分钟期间事件积压，恢复后全部投递，`pay_outbox_pending` 回落到 0（C7）。
- [ ] 不注入故障，两个投递器并发运行 1 万条事件：Kafka 中每个 `event_id` 恰好出现 1 次，1 万条全部为 SENT。
- [ ] 注入发送失败的假生产者，20 次后状态为 DEAD，`pay_outbox_dead` 为 1。
- [ ] 发送期间不持有行锁：假生产者阻塞发送时，另一个连接对已领取的行执行 `SELECT … FOR UPDATE NOWAIT` 立即成功。
- [ ] 过期租约可被接管：实例 A 领取后假生产者阻塞，Fake 时钟推进 31 秒，实例 B 领取到同一行、发送成功并置为 SENT。
- [ ] 过期回写不覆盖新租约：上一条中 B 领取之后、回写之前放行 A，A 的成功和失败回写都影响 0 行，该行的 `status`、`next_retry_at`、`retry_count` 保持 B 写入的值。

#### MQ-03 Kafka 生产与消费框架（pkg/mq）

`P0 · M5 · 依赖：BASE-02、OPS-01`

规则：

1. 生产者：franz-go，`acks=all`，开启幂等生产，`linger=5ms`；消息头携带 `event_id`、`event_type`、`traceparent`。
2. 消费者：消费组模式，关闭自动提交；每个分区一个处理协程，分区内顺序处理；处理成功（数据库事务已提交）后才提交 offset。
3. 处理函数返回的错误分两类：
   - 可重试（数据库、网络、依赖不可用）：原地退避重试，100ms 起翻倍、上限 5 秒，不提交 offset，持续重试并告警。
   - 不可重试（反序列化失败、数据缺失、业务冲突）：写入 `pay.dlq`（消息头附原 topic、分区、offset、错误信息），提交 offset，`pay_mq_dlq_total + 1`。
4. 分区再平衡时等待当前消息处理完成并提交。
5. 每个分区上报消费延迟 `pay_mq_consume_lag`。
6. 优雅退出：停止拉取，处理完在途消息并提交后关闭。
7. 提供 Kafka 连接就绪检查，payment、account 把它注册到 OPS-01 的 `/readyz`（单项超时 1 秒）；退出时由 OPS-01 的排空流程调用生产者刷新（由 OPS-01 规则 1、2 移来）。

验收：

- [ ] 处理函数前 3 次返回可重试错误、第 4 次成功：消息只成功处理一次，offset 已提交。
- [ ] Kafka 不可用时 payment、account 的 `/readyz` 返回 503，恢复后返回 200（由 OPS-01 移来）。
- [ ] 不可重试错误的消息进入 `pay.dlq`，后续消息继续消费。
- [ ] 处理成功、提交前 kill 进程，重启后重新投递（由业务幂等吸收）。

#### MQ-04 account 事件消费

`P0 · M5 · 依赖：MQ-02、MQ-03、ACC-03、ACC-05、ACC-06、PAY-06、REF-03、BASE-15`

消费组 `account-ledger`，在 account 进程内直接调用 biz 层（不经过 RPC）。本功能点实现逐条模式（每条消息一个分片事务），是 OPT-02 批量模式的基线和退化路径。依赖 MQ-02、PAY-06、REF-03 是为了端到端验收：事件经投递器投递，来源包括 CLOSE_RACE 关单和退款终态。

| 事件 | 处理 | 不可重试情况 |
|---|---|---|
| `payment.succeeded` | 按 `merchant_id` 路由分片，`Post(PAY, pay_no)`，分录按 §2.5.2：借本分片渠道应收子账户 A，贷商户 A − F，贷本分片手续费收入子账户 F（F = 0 时省略手续费分录，凭证只有 2 条分录） | 商户账户不存在；`Post` 返回 `40005`（账户非 ACTIVE） |
| `refund.succeeded` | `ConfirmFreeze(REFUND, refund_no, merchant_id, channel)`，贷本分片渠道应收子账户 | 返回 `40004`（冻结单缺失或已解冻，属于严重不一致，计数并输出 ERROR 日志） |
| `refund.failed` | `ReleaseFreeze(REFUND, refund_no, merchant_id, amount)`，冻结不存在时为空回滚 | 返回 `40004`（已确认扣款） |

故障点 `ledger.after_commit`（数据库提交后、offset 提交前）。

`Post` 返回 `40005` 视为不可重试：按 MQ-03 写入 DLQ 并提交 offset，`pay_mq_dlq_total + 1`，输出 ERROR 日志（`event_id`、`event_type`、`merchant_id`，不含密钥）。账户恢复 ACTIVE 后由 MQ-05 重放修复。（2026-10-27 用户决定）

验收：

- [ ] 同一 `payment.succeeded` 消息投递 5 次，只生成 1 张凭证（A3 账务层）。
- [ ] 在 `ledger.after_commit` kill 进程，重启后重复消费不重复入账（C5）。
- [ ] 商户账户不存在时消息进入 DLQ，后续消息正常处理。
- [ ] 商户结算账户被测试改为非 ACTIVE：该 `payment.succeeded` 不重试，进入 DLQ，`pay_mq_dlq_total` +1，有一条 ERROR 日志，没有新凭证；后续消息正常处理。
- [ ] `fee_amount = 0` 的 `payment.succeeded`：凭证只有借渠道应收 A、贷商户 A 两条分录，手续费收入余额不变，`CheckBalance` 通过。
- [ ] CLOSE_RACE 关单返回 `30003` 的订单（由 PAY-06 移来）：`payment.succeeded` 经投递器投递并消费后，商户入账恰好 1 次。
- [ ] 订单 10000 入账后退款 3000 成功（由 REF-03 移来）：消费 `refund.succeeded` 后商户余额 −3000、冻结 0、渠道应收 −3000，`CheckBalance` 通过。
- [ ] 退款 FAILED（渠道 FAIL 或受理拒绝，由 REF-02、REF-03 移来）：消费 `refund.failed` 后冻结单 RELEASED，`frozen_amount` 恢复，用新的 `out_refund_no` 再次退款成功。

#### MQ-05 死信与事件重放

`P0 · M5 · 依赖：MQ-02、MQ-04 · 表：outbox`

接口：`PaymentService.ReplayEvent(event_id | biz_no + event_type)`；管理接口 `POST /admin/outbox/replay`

规则：

1. 按 `event_id` 重放：`UPDATE outbox SET status = 'PENDING', retry_count = 0, next_retry_at = now WHERE event_id = ? AND status IN ('SENT', 'DEAD')`。
   影响 0 行时：事件不存在 → `10002`；事件已是 PENDING（等待投递中）→ 不做修改，返回 code 0、`rebuilt = false`。
2. 按业务单号重放：按 `idx_biz_no_event_type` 找到对应事件后同上；事件已被清理（MQ-06）时，根据订单当前数据重建一条新事件（新 `event_id`），前提是订单状态与事件类型一致（如 SUCCESS 才能重建 `payment.succeeded`）。
3. 重放依赖消费端幂等，因此对已成功处理的事件重放无副作用。
4. DLQ 消息不自动消费；`pay_mq_dlq_total` 大于 0 时告警，人工排查后通过重放修复。

验收：

- [ ] 删除一张 PAY 凭证所对应的消费结果（测试环境中构造“漏入账”），重放后入账完成；对正常事件重放不产生新凭证。
- [ ] 对 PAYING 订单重建 `payment.succeeded` 被拒绝（`30003`）。
- [ ] 因商户账户非 ACTIVE 进入 DLQ 的 `payment.succeeded`（MQ-04）：账户恢复 ACTIVE 后按 `event_id` 重放，入账恰好 1 次，`CheckBalance` 通过。

#### MQ-06 outbox 清理

`P1 · M8 · 依赖：MQ-02`

规则：每天 03:00 删除 `status = 'SENT' AND sent_at < now − 7d` 的记录，每批 1000 条，批间休眠 100ms。outbox 不属于账务数据，允许删除。

验收：

- [ ] 8 天前的 SENT 记录被删除，PENDING 和 DEAD 记录保留。

### 4.8 商户通知（NTF）

#### NTF-01 通知记录生成

`P0 · M5 · 依赖：MQ-03 · 表：merchant_notify`

规则：

1. 消费组 `payment-notify`（payment 进程内）消费 `payment.succeeded`、`refund.succeeded`、`refund.failed`、`transfer.succeeded`、`transfer.failed`。
2. 插入 `merchant_notify(PENDING)`，`event_id` 唯一，冲突即视为已处理；`notify_url` 取原支付单（退款事件同样取原支付单）或转账单的 `notify_url`，为空时取商户默认通知地址；转账只通知付款商户；`payload` 按 `docs/05-api.md` §6 组装。

验收：

- [ ] 同一事件消费 3 次，只有 1 条通知记录。

#### NTF-02 通知发送与重试

`P0 · M5 · 依赖：NTF-01、BASE-13、TL-02 · 表：merchant_notify`

规则：

1. 每秒抢占到期的 PENDING 记录（`FOR UPDATE SKIP LOCKED`，抢占时 `attempt_count + 1` 并推后 `next_attempt_at`，方式同 PAY-05），事务外以最多 20 个并发发送。
2. 请求：`POST notify_url`，JSON body，按 BASE-13 平台签名方式签名；超时 5 秒；不跟随重定向；响应体最多读取 1KB。签名失败（如商户密钥为空，BASE-13 规则 8）按内部错误 `10003` 处理：本次不发送，打 ERROR 日志（不含密钥），`last_error` 只记 `internal error`，按发送失败进入第 4 条的重试与 DEAD 判定。
3. 成功判定：HTTP 200 且去除首尾空白后 body 为 `SUCCESS`，置为 SUCCESS。
4. 固定间隔重试：首次立即发送，之后每隔 `NOTIFY_RETRY_INTERVAL`（默认 30 秒）重试，共 `NOTIFY_MAX_ATTEMPTS`（默认 5）次；5 次仍失败置为 DEAD，`pay_notify_attempts_total{result="dead"} + 1`。真实系统常用递增间隔（如 15s、15s、30s、3m …），本项目不是重点，简化为固定间隔。
5. 发送前再次校验 `notify_url` 主机在白名单内（NFR-SEC-07）；不在白名单时不发送，直接置为 DEAD，`last_error` 记录原因，`pay_notify_attempts_total{result="dead"} + 1` 并告警（白名单不会因重试而改变）。
6. 商户必须按 `pay_no` / `refund_no` / `transfer_no` 幂等处理，平台不保证只通知一次。
7. 发送结果回写用 CAS：`UPDATE merchant_notify SET … WHERE id = ? AND status = 'PENDING'`（源状态只有 PENDING），SUCCESS、DEAD 和失败后写 `last_error` 都走这个条件。影响 0 行说明记录已被其他实例推进为终态，本次结果丢弃，不报错，不再更新。

验收：

- [ ] merchant-demo 配置前 3 次返回失败，第 4 次成功，记录 SUCCESS 且 `attempt_count = 4`。
- [ ] 发送期间测试把记录改为 SUCCESS（模拟另一实例已完成），本次失败结果回写影响 0 行，记录保持 SUCCESS，`last_error` 不变。
- [ ] 一直失败时，Fake 时钟推进后共尝试 5 次，相邻两次间隔 30 秒，状态 DEAD。
- [ ] merchant-demo 验证通知签名通过；篡改密钥后验签失败。

### 4.9 网关（GW）

#### GW-01 网关骨架与统一响应

`P0 · M3 · 依赖：BASE-12、BASE-13、TL-01`

规则：

1. 两个 Hertz 实例：8080（商户 API、渠道回调）、8090（管理接口），请求体上限 64KB，超过返回 413 / `10001`（不签名）。
2. 商户路由中间件顺序：Recovery → RequestID → Trace → AccessLog → 时间窗口（GW-04）→ 验签（GW-03）→ 商户限流（GW-05）→ nonce（GW-04）→ handler。限流放在 nonce 之前，超限请求不消耗 Redis 调用；被限流的请求不占用 nonce，商户可用同一 nonce 重试。
3. 统一响应：`{"code": 0, "msg": "ok", "request_id": "...", "data": {...}}`；响应头 `X-Request-Id`；验签通过的请求，响应带平台签名头 `X-Timestamp`、`X-Nonce`、`X-Signature`；验签前失败的响应不签名。平台签名失败（BASE-13 规则 8）时改为返回 500 / `10003`，该响应不签名，打 ERROR 日志（不含密钥）；商户按 `10003` 原参数重试，由幂等键保证不重复处理。`X-Request-Id` 与 body 的 `request_id` 由服务端生成 32 位小写十六进制，忽略客户端传入的同名头。
4. 错误码按 `docs/03-conventions.md` §5.2 映射 HTTP 状态；`10003` 只返回通用消息和 request_id。未知路径返回 404 / `10002`（`msg` 为 `not found`，`data` 为 `{}`），不签名。
5. Kitex 客户端：连接超时 100ms，调用超时 1 秒；`Pay`、`ClosePayment`、`CreateRefund` 含渠道调用，调用超时 5 秒；`CreateTransfer` 调用超时 3 秒（与 §5.4.1 一致）；网关不重试写请求。

验收：

- [x] handler panic 返回 500 / `10003`，进程不退出。
- [x] 超过 64KB 的请求被拒绝。
- [x] 成功响应可用 paycli 验证签名。

#### GW-02 商户 API 路由（支付与余额）

`P0 · M3 · 依赖：GW-01、GW-03、PAY-02/03/06/08、ACC-02、ACC-11`

说明：原 GW-02 横跨 M3–M4，退款与转账路由依赖 M4 的 REF、TRF。2026-10-27 拆分：本功能点只做 M3 能完成的支付与余额路由，退款与转账路由是 GW-09。

规则：

1. 路由：`docs/05-api.md` §4.1–§4.4（创建、发起、关闭、查询支付单）与 §4.7（查询余额 `GET /v1/balance` → `AccountService.GetAccount`）；RPC 映射见 §1、§10.1、§10.2。
2. `merchant_id` 只来自验签结果，忽略请求体或查询串中的同名字段。
3. 参数校验在网关完成一次（格式、长度、范围），payment 再做一次业务校验（本条的通用规则 GW-09 同样适用）：
   - `amount`：整数，1–10,000,000。
   - `out_trade_no`：1–64 位 `[A-Za-z0-9_-]`。
   - `subject`：1–128 字符。
   - `notify_url`：≤ 512，http/https。
   - JSON 中的未知字段忽略；类型错误（如金额为字符串或小数）返回 `10001`。

验收：

- [x] 创建支付单时 `amount` 为 `100.5`、`"100"`、`0`、`10000001` 均返回 `10001`。
- [x] 请求体中伪造 `merchant_id` 不影响订单归属。
- [x] `GET /v1/balance` 只返回当前验签商户的结算账户余额，查询串中的其他 `merchant_id` 被忽略。

#### GW-03 商户验签

`P0 · M3 · 依赖：GW-01、PAY-01`

规则：

1. 必需请求头：`X-App-Id`、`X-Timestamp`、`X-Nonce`、`X-Signature`，缺任一 → `20001`。
2. 按 `app_id` 取商户凭证（GW-08 完成前每次调用 `GetMerchantCredential`）；不存在或 DISABLED → `20004`；调用失败（payment 不可用）→ 503 / `10004`，不缓存失败结果。
3. 按 BASE-13 计算签名，常量时间比较，不一致 → `20001`；签名格式非法、字段含换行同为 `20001`。商户密钥为空（`ErrEmptyKey`）是服务端数据问题，不是签名错误 → 500 / `10003`，打 ERROR 日志（只记 `app_id`，不记密钥），不进入 handler。
4. 通过后把 `merchant_id` 写入请求上下文。
5. 日志只记录 `app_id` 和结果，不记录签名值和 canonical 串。

验收（A7）：

- [x] 签名错误、缺少任一请求头、未知 app_id、商户停用，分别返回对应错误码，且不进入 handler。
- [x] 正确签名的请求通过。

#### GW-04 时间窗口与 nonce 防重放

`P0 · M5 · 依赖：GW-03、GW-06、OPS-01`

规则：

1. `X-Timestamp` 为 Unix 秒，与服务器时间相差超过 ±300 秒 → `20002`。该检查在验签之前执行（成本低）。
2. `X-Nonce` 为 16–32 位 `[A-Za-z0-9]`，格式错误 → `20001`。
3. 验签通过后执行 `SET gw:nonce:{app_id}:{nonce} 1 NX EX 600`；已存在 → `20003`。验签失败的请求不占用 nonce，防止攻击者用伪造请求抢占合法 nonce。
4. Redis 超时（50ms）或不可用 → `10004`（fail-closed），`pay_nonce_unavailable_total + 1`。
5. gateway 的 `/readyz` 增加 Redis 检查（OPS-01，单项超时 1 秒；由 OPS-01 规则 1 移来）。

验收（A7）：

- [ ] 时间戳超前或落后 301 秒返回 `20002`，299 秒通过。
- [ ] 同一 nonce 第二次使用返回 `20003`；签名错误的请求使用过的 nonce 仍可被合法请求使用。
- [ ] Redis 停止时商户接口返回 503 / `10004`，gateway `/readyz` 返回 503。
- [ ] Redis 停止时渠道回调仍能正常推进订单（由 GW-06 移来：回调路由不经过 nonce 中间件）。

#### GW-05 商户限流（进程内）

`P0 · M5 · 依赖：GW-03`

规则：

1. 商户限流放在验签之后（防止伪造 app_id 耗尽他人配额），nonce 检查之前。
2. 进程内按 `app_id` 的令牌桶（`golang.org/x/time/rate`），默认 100 req/s、突发 200，可通过 `RATE_LIMIT_OVERRIDES`（如 `M100001:1000:2000`）按商户覆盖；限流器按需创建，存放在 `sync.Map`。
3. 超限返回 429 / `10005`，带 `Retry-After` 头。
4. 不做全局限流和 Redis 分布式限流。网关多实例时实际配额为“单实例配额 × 实例数”，README 中说明。

验收：

- [ ] 默认配额（100 req/s、突发 200）下，单商户在注入时钟的同一时刻连续发出 300 个请求：恰好 200 个通过，其余 100 个返回 429 / `10005` 且带 `Retry-After` 头。
- [ ] 一个商户被限流不影响另一个商户。

#### GW-06 渠道回调入口

`P0 · M3 · 依赖：GW-01、PAY-04、CH-05、BASE-15`

接口：`POST /callback/mockpay` → `PaymentService.HandleChannelNotify`

说明：第 3 条的 REFUND 分支在 REF-03 接通并验收（REF-03 依赖本功能点，2026-11-05 已接通）；本功能点验收 PAY 分支。

规则：

1. 校验渠道签名与时间窗口（±300 秒），失败返回 401 / `50001`，不调用 payment。渠道密钥为空（`ErrEmptyKey`）是本端配置故障，不是渠道签名错误：按 `10003` 返回 HTTP 500、body `FAIL`，打 ERROR 日志（不含密钥），不调用 payment，渠道按 CH-05 重试。
2. 不做 nonce 检查（回调处理本身幂等），因此 Redis 故障不影响回调。
3. 按 `txn_type` 分发到 `ApplyPayResult` 或 `ApplyRefundResult`（source = NOTIFY）。
4. payment 返回成功、幂等、冲突（已记录告警）时，网关返回 HTTP 200、body `SUCCESS`；返回 `10003`、`10004` 时返回 HTTP 500，让渠道重试。
5. payment 返回 `30001`（未知 req_no）、`50002`（金额不一致）时，重试不会自愈：网关返回 HTTP 200、body `SUCCESS`，记错误日志，`pay_callback_rejected_total{code}` + 1 并告警，差异交给对账（LOCAL_MISSING / AMOUNT_MISMATCH）。
6. payment 侧故障点 `callback.after_commit`：事务提交后、返回响应前。

验收：

- [x] 签名错误的回调不改变任何订单（A7）。
- [x] 签名正确但 req_no 未知、金额不一致的回调返回 200 SUCCESS，订单不变，告警指标 + 1。
- [x] 正常场景（由 PAY-03 移来）：Pay 返回 PAYING，经 channel_mock 自动完成与 CH-05 回调，2 秒内订单变为 SUCCESS。
- [x] DUP_NOTIFY 端到端（A3 订单层）：渠道 10 次回调都得到 200 `SUCCESS`，订单只推进一次，outbox 恰好 1 条 `payment.succeeded`，`notify_count = 10`。

#### GW-07 管理接口

`P0 · M6 · 依赖：GW-01、MQ-05、ACC-10、ACC-11、REC-01`

规则：

1. 只在 8090 端口提供，宿主机只绑定 `127.0.0.1`。
2. 请求头 `X-Admin-Token` 与配置 `ADMIN_TOKEN`（≥ 32 字符，启动时校验）常量时间比较，失败 → 401 / `20005`。
3. 写操作必须带 `X-Admin-Operator`，写入审计日志（操作人、接口、参数摘要、结果）。
4. 只提供三个接口（清单见 `docs/05-api.md` §8）：事件重放（MQ-05）、余额核对（ACC-10）、手动触发对账（REC-01）。对账结果在触发接口的响应中返回，不提供批次和差错的列表查询。
5. 这是适合本地演示的简化鉴权，不提供细粒度权限，README 中需说明生产环境的替代方案。

验收：

- [ ] 无 Token、错误 Token 返回 401。
- [ ] 写操作缺少 `X-Admin-Operator` 返回 `10001`。
- [ ] 三个接口各调用一次成功：重放返回 MQ-05 的结果，余额核对返回 `passed = true`，触发对账返回批次与差错；每次写操作产生一条审计日志。

#### GW-08 商户密钥缓存

`P0 · M7 · 依赖：GW-03、OPT-01、QA-02`

规则：进程内缓存商户凭证 60 秒（`GW_CREDENTIAL_CACHE_TTL`，设为 0 关闭，作为对比基线）；同一 app_id 并发未命中时用 singleflight 只回源一次；不存在的 app_id 负缓存 10 秒；商户停用最长 60 秒后生效（README 说明）。属于 OPT-04 的优化项之一。

验收：

- [ ] 1000 个并发请求同一 app_id，只回源 1 次。
- [ ] 压测报告给出开启前后三条链路网关阶段的 P99 对比。

#### GW-09 商户 API 路由（退款与转账）

`P0 · M4 · 依赖：GW-01、GW-02、GW-06、TL-01、REF-01、REF-05、TRF-01、TRF-05`

说明：2026-10-27 从原 GW-02 拆出的 M4 部分。中间件链、`merchant_id` 来源与通用参数校验沿用 GW-02；依赖 GW-06 与 TL-01 是为了用 paycli 跑通含回调的完整流程（原 TL-01 的验收移到这里）。M4 时入账消费 MQ-04 还没有，测试中商户余额用 account biz 夹具直接入账。

规则：

1. 路由：`docs/05-api.md` §4.5、§4.6（创建、查询退款）与 §4.8、§4.9（创建、查询转账）；RPC 映射见 §1、§10.2。`CreateRefund` 调用超时 5 秒、`CreateTransfer` 3 秒（GW-01 第 5 条）。
2. `merchant_id` 只来自验签结果；转账的付款方同样只来自验签结果，忽略请求体中的同名字段。
3. 参数校验（规则同 GW-02 第 3 条）：
   - `amount`：整数，1–10,000,000。
   - `out_refund_no`、`out_transfer_no`：1–64 位 `[A-Za-z0-9_-]`。
   - `payee_merchant_id`：`M` + 6 位数字。
   - `remark`：≤ 128 字符；`reason`：≤ 255 字符。
   - `notify_url`（转账）：≤ 512，http/https。
   - JSON 中的未知字段忽略；类型错误返回 `10001`。

验收：

- [ ] 创建退款与创建转账时 `amount` 为 `100.5`、`"100"`、`0`、`10000001` 均返回 `10001`；`payee_merchant_id` 为 `M12345`、`X100002` 返回 `10001`。
- [ ] 转账请求体中伪造付款方 `merchant_id` 不影响付款方，转账单的付款方是验签商户。
- [ ] 用 paycli 经网关完成“下单 → 支付 → 查询 → 退款 → 查询退款 → 同分片转账 → 跨分片转账 → 查询转账”（由 TL-01 移来），每一步退出码 0 且响应签名校验通过；支付经回调变为 SUCCESS 后再退款。

### 4.10 对账（REC）

#### REC-01 对账批次与调度

`P0 · M6 · 依赖：BASE-03 · 表：recon_batch（新增列 retry_count）`

接口：`ReconService.RunBatch(channel, bill_date, force)` → `{batch, diffs[≤ 100]}`；CLI `recon run --channel mockpay --date YYYY-MM-DD [--force]`；管理接口 `POST /admin/recon/run`

规则：

1. 每天 `RECON_RUN_AT`（默认 10:00）对 D−1 发起对账，渠道列表来自配置（目前只有 `mockpay`）。同一次定时触发还顺带处理前 7 个账期（`bill_date` 为 D−8 到 D−2）中状态为 INIT、DOWNLOADED、CHECKING 的已有批次：按 `uk_channel_bill_date` 的 `channel, bill_date` 前缀范围查出后，逐个调用 RunBatch，由第 5 条判定是否超时接管。不为缺失的账期补建批次，FAILED、DONE 不处理（重跑仍需 force 或手工触发）。只有每日定时触发做这一步，第 6 条的单批次延迟重试、CLI、管理接口不做；这不是独立的后台扫描。
2. 按 `uk_channel_bill_date` 创建批次。已存在时按状态处理，`force` 只对 DONE 生效：
   - DONE、未指定 force → 直接返回已有结果；DONE 且 `force = true` → 按第 3 条重跑。
   - FAILED → 按第 3 条重跑，带不带 force 都一样。
   - 未超时的 INIT、DOWNLOADED、CHECKING → 返回当前批次，不重复执行；`force = true` 也一样，不会与正在执行的运行并发重跑。
   - 超时的 INIT、DOWNLOADED、CHECKING → 按第 5 条接管，带不带 force 都一样，不按第 3 条重跑。（2026-10-27 用户决定）
3. 重跑：删除该批次的 `recon_channel_bill`，以及该批次中尚未自动处理的差错（`status = 'PENDING' AND handle_action IS NULL`）；保留已自动处理的 PENDING 差错（`handle_action` 非空，如已重放待复核的 `LEDGER_MISSING`）和 RESOLVED、IGNORED 的差错及处理记录。然后 CAS 把批次重置为 INIT（同时 `retry_count = 0`），从下载开始执行。重新比对时与保留差错同键的结果按 REC-03 第 4 条跳过，不会触发 1062。（2026-10-27 用户决定，方案 A）
4. 批次状态：`INIT → DOWNLOADED → CHECKING → DONE`，任一步失败 → FAILED 并记录 `error_msg`。
5. 并发控制：CAS 推进状态；超时视为卡死，可被重新接管。超时指距该时间戳满 30 分钟（now − 时间戳 ≥ 30 分钟）：CHECKING 按 `started_at`，INIT、DOWNLOADED 按 `updated_at`。接管用 CAS，条件带原状态与原时间戳，并把该时间戳改为当前时间，同一时刻只有一个接管者。接管后从哪一步继续：
   - INIT：从下载开始（REC-02）。INIT 时还没有账单行，REC-02 第 5 条的导入与置 DOWNLOADED 在同一事务提交。
   - DOWNLOADED：账单行已在进入 DOWNLOADED 的同一事务中导入（REC-02 第 5 条），不重新下载；接管 CAS 即 `DOWNLOADED → CHECKING`（条件带原 `updated_at`，写 `started_at = now`），然后执行比对。
   - CHECKING：CAS 把 `started_at` 改为 now 后，先按第 3 条的条件删除本批次 `status = 'PENDING' AND handle_action IS NULL` 的差错（不删账单行），再重新执行比对（REC-03、REC-05、REC-06）。中断的那次运行留下的已自动处理差错、RESOLVED、IGNORED 差错保留，重新比对时同键跳过（REC-03 第 4 条）。

   CHECKING 运行把批次推进为 DONE 或 FAILED 的 CAS 条件带本次运行写入的 `started_at`。批次被接管后，旧运行（若仍在执行）的这条 CAS 影响 0 行，旧运行放弃，不覆盖新运行的结果。

   改写 `recon_batch` 的每条 UPDATE 都显式写 `updated_at = now`（注入时钟，截到毫秒），不依赖 ON UPDATE 的数据库时间。超时只在 RunBatch 被调用时判定，调用时机是：每日定时（第 1 条，含前 7 个账期）、第 6 条的单批次延迟重试、CLI、管理接口；不加独立的后台扫描。（2026-10-27 用户决定，§8.1 Q6）
6. 渠道返回 `BILL_NOT_READY` 时（REC-02 第 4 条），重试状态持久化在 `recon_batch.retry_count`（`INT NOT NULL DEFAULT 0`，迁移随本功能点新增）：
   - `retry_count < 6`：CAS `UPDATE recon_batch SET retry_count = retry_count + 1, updated_at = now WHERE id = ? AND status = 'INIT' AND retry_count = ?`（now 为注入时钟），批次保持 INIT。
   - `retry_count` 已达到 6：CAS 置 FAILED，`error_msg = BILL_NOT_READY`，条件同上；之后按第 2 条可重跑。
   - 距上次 `updated_at` 满 30 分钟才重试，即第 5 条的 INIT 超时接管。recon 进程在写入后用注入时钟的调度器在 30 分钟后对该批次再调用一次 RunBatch，只调度这一个批次。
   - 计数与时间都在表里，进程重启不丢。重启后调度器里待执行的那次调用会丢，该批次由下一次每日定时触发（第 1 条，前 7 个账期）、CLI 或管理接口按第 5 条接管，并从已有的 `retry_count` 继续计数。
   - 首次下载之后最多重试 6 次（账单请求共 7 次）。（2026-10-27 用户决定）

验收：

- [ ] 同一账期并发触发两次，只有一次执行。
- [ ] force 不打断执行中的批次：批次分别停在 INIT、DOWNLOADED、CHECKING，Fake 时钟推进 29 分钟时以 `force = true` 调用 RunBatch，都返回当前批次，状态与时间戳不变，没有删除账单行或差错，没有发出账单请求；推进到 30 分钟时以 `force = true` 调用，按第 5 条接管（DOWNLOADED 不重新下载，`retry_count` 不归 0），而不是按第 3 条重跑。
- [ ] 重跑后差错结果与首次一致，已处理的差错不丢失。
- [ ] 重跑不触发 1062、已自动处理的差错保留：DONE 批次中有 IGNORED 的 `CHANNEL_MISSING`、RESOLVED 的 `STATUS_MISMATCH`、PENDING 且 `handle_action = REPLAY` 的 `LEDGER_MISSING`、PENDING 且 `handle_action` 为空的 `LOCAL_MISSING`；force 重跑执行到 DONE，没有 1062；前三条的 `id`、`status`、`handle_action`、`handle_note` 都不变，`LOCAL_MISSING` 被删除后重新生成（新 `id`，键相同）。
- [ ] 中断后接管与一次性运行一致：同一份账单和本地数据准备两份。一份一次执行到 DONE。另一份构造中断状态：批次 CHECKING、`started_at` 早于 30 分钟，已插入部分差错，其中一部分已由 REC-06 自动处理（`LEDGER_MISSING` 已写 `REPLAY`，`CHANNEL_MISSING` 已置 IGNORED），其余 `handle_action` 为空；接管后执行到 DONE。两份的差错集合按 (`txn_type`, `biz_no`, `diff_type`, `status`) 比较完全一致，`diff_count`、匹配数、双方笔数与金额一致，过程中没有 1062。
- [ ] 接管后旧运行不覆盖结果：批次被接管后，旧运行用原 `started_at` 把批次置为 DONE 或 FAILED 的 CAS 影响 0 行。
- [ ] 批次停在 INIT、DOWNLOADED、CHECKING 各构造一次（INIT、DOWNLOADED 由 `updated_at`、CHECKING 由 `started_at` 计时）：Fake 时钟推进 29 分钟时调用 RunBatch 返回当前批次、不执行；推进到 31 分钟时调用 RunBatch 被接管并执行到 DONE；两个接管者并发时只有一个执行。
- [ ] 没有后台扫描：停在 DOWNLOADED 的批次在 Fake 时钟推进 2 小时、期间没有 RunBatch 调用时，状态与 `updated_at` 都不变。
- [ ] 假渠道一直返回 `BILL_NOT_READY`：首次下载之后每 30 分钟（注入时钟）重试一次，`retry_count` 依次为 1 … 6，第 6 次重试（第 7 次请求）后批次 FAILED、`error_msg = BILL_NOT_READY`，之后不再自动请求。
- [ ] recon 重启后计数不丢：`retry_count = 3` 时用同一数据库新建 recon 实例（模拟重启，原调度任务丢失），下一次每日定时触发接管该批次并发起下载；仍为 `BILL_NOT_READY` 时 `retry_count` 变为 4，而不是从 0 开始。
- [ ] 旧账期由定时触发接管：构造 `bill_date` 为 D−3 的 DOWNLOADED 批次和 D−5 的 CHECKING 批次（各自时间戳早于 30 分钟），以及 D−9 的 INIT 批次和 D−4 的 FAILED 批次；一次每日定时触发后，D−3、D−5 执行到 DONE，D−9、D−4 不变；DOWNLOADED 批次没有发出账单请求。
- [ ] 重跑把 `retry_count` 重置为 0：因 `BILL_NOT_READY` FAILED 的批次 force 重跑后 `retry_count = 0`、状态 INIT。
- [ ] 假渠道前 3 次返回 `BILL_NOT_READY`、第 4 次返回账单：批次在第 3 次重试时执行到 DONE，之后不再请求账单。

#### REC-02 账单下载与导入

`P0 · M6 · 依赖：REC-01、CH-08 · 表：recon_channel_bill`

规则：

1. 下载超时 30 秒，失败重试 3 次（间隔 10 秒）。
2. 校验 `X-Bill-Sha256` 与 `X-Bill-Count`，不一致 → FAILED。
3. 严格解析 CSV：表头必须与 `docs/05-api.md` §9.6 一致；金额为整数；时间格式合法；任一行错误 → FAILED，错误信息带行号。
4. 渠道返回 `BILL_NOT_READY` → 批次保持 INIT，`retry_count + 1`，满 30 分钟后重试，最多重试 6 次，之后置 FAILED（REC-01 第 6 条）。
5. 在一个事务内每 1000 行批量插入，状态置为 DOWNLOADED，写入 `bill_file_hash`。

验收：

- [ ] 篡改账单内容导致哈希不一致，批次 FAILED。
- [ ] 1 万行账单导入耗时 < 5 秒。

#### REC-03 外部对账比对

`P0 · M6 · 依赖：REC-02、CH-06、CH-09、PAY-05 · 表：recon_channel_bill、recon_diff（唯一键改为 uk_batch_type_biz_no_diff_type）；只读 channel_txn`

规则：

1. 渠道侧集合 B：本批次的账单行。本地侧集合 L：`channel_txn` 中 `channel = ?`、`status = 'SUCCESS'`、`channel_finished_at ∈ [D 00:00, D+1 00:00)` 的记录。匹配键 `(txn_type, biz_no)`。
2. 对 B 中每一行，按匹配键查询本地记录（不限日期和状态）：

   | 情况 | 结果 |
   |---|---|
   | 本地无记录 | `LOCAL_MISSING`（长款） |
   | 本地状态不是 SUCCESS | `STATUS_MISMATCH` |
   | 本地 SUCCESS，金额不同 | `AMOUNT_MISMATCH` |
   | 本地 SUCCESS，金额相同（不论本地归属哪天） | 匹配 |

3. L 中未匹配的记录 → `CHANNEL_MISSING`（短款）。不做跨日处理：模拟渠道账单的账期与本地 `channel_finished_at` 都取自渠道完成时间，两边不会归入不同账期。真实渠道存在日切差异，需要挂起复核，本项目不实现（REC-04 CUT）。
4. 更新账单行 `match_status`；差错按 `uk_batch_type_biz_no_diff_type`（`batch_no, txn_type, biz_no, diff_type`）插入。该唯一键替换原 `uk_batch_type_biz_no`（`batch_no, txn_type, biz_no`），迁移随本功能点新增，使同一笔交易在同一批次可以同时有外部差错和内部差错（如 `AMOUNT_MISMATCH` 与 `LEDGER_MISSING`）。本批次已有同键差错（重跑或接管时保留的行，REC-01 第 3、5 条）时跳过，不改已有行的任何列。写法用 `INSERT … ON DUPLICATE KEY UPDATE id = id`，不用 `INSERT IGNORE`（它会把 CHECK 约束失败等其他错误也降为警告），也不用 `REPLACE`。插入影响 1 行才计入 `pay_recon_diffs_total`。批次写入本次比对重新计算的双方笔数、金额、匹配数；`diff_count` 按比对完成后本批次 `recon_diff` 的实际行数统计（含保留的行）。（2026-10-27 用户决定）
5. 数据量假设：单渠道单日 ≤ 100 万笔。P0 实现可在内存中按匹配键建索引；超过假设规模时改为按 `biz_no` 排序分段归并。

验收（A6）：

- [ ] 构造账单：注入一笔平台不存在的交易（长款）、BILL_MISSING（短款）、BILL_AMOUNT_DIFF（金额不符）、DROP_NOTIFY 且关闭查单补偿（状态不符），四类各识别 1 条，其余全部匹配。
- [ ] 全部正常的账单差错数为 0。
- [ ] 外部差错与内部差错并存：同一笔支付本地 SUCCESS、金额与账单不同，且没有 PAY 凭证。同一批次得到 `AMOUNT_MISMATCH`（REC-03）和 `LEDGER_MISSING`（REC-05）两条差错，键只差 `diff_type`，没有 1062，也没有被跳过，`diff_count` 计 2。
- [ ] 同键跳过：本批次已有一条 IGNORED 的 `CHANNEL_MISSING`，重新比对再次得到同键差错时不报 1062，已有行的 `status`、`handle_action`、`handle_note`、`updated_at` 都不变，`pay_recon_diffs_total` 不增加，`diff_count` 等于本批次差错实际行数。

#### REC-04 跨日处理

`CUT`：见 REC-03 第 3 条。

#### REC-05 内部对账（支付、退款、转账）

`P0 · M6 · 依赖：REC-01、PAY-04、REF-03、TRF-02、TRF-03、MQ-04 · 只读：payment_order、refund_order、transfer_order、两个分片的 ledger_txn、account_freeze、transfer_branch`

规则：

1. D 日 `paid_at` 落在当日的 SUCCESS 支付单，商户所属分片必须存在 `ledger_txn(PAY, pay_no)` 且 `amount` 等于订单金额。
2. D 日 `succeeded_at` 落在当日的 SUCCESS 退款单，必须存在 `ledger_txn(REFUND, refund_no)` 且金额相等。
3. 缺失或金额不等 → `LEDGER_MISSING`（金额不等时在 `handle_note` 中注明双方金额）。
4. D 日 `finished_at` 落在当日的转账单，按不变量 12 检查：
   - SUCCESS + LOCAL：分片内恰好 1 张 `TRANSFER` 凭证。
   - SUCCESS + TCC：付款方分片 `TRANSFER_OUT`、收款方分片 `TRANSFER_IN` 各 1 张，冻结单 CONFIRMED，收款方分支 CONFIRMED。
   - FAILED：两个分片都没有该 `transfer_no` 的转账凭证；冻结单不存在或 RELEASED；收款方分支不存在或 CANCELLED。
   - 不符 → `TRANSFER_MISMATCH`，`txn_type = TRANSFER`，`local_amount` / `local_status` 取转账单的金额与状态，渠道字段为空，`handle_note` 记录各分片实际情况。
   - 转账不经过渠道，但只有一个渠道（mockpay），转账检查挂在该账期的 mockpay 批次中执行，差错归入该批次；批次重跑时一并重算（REC-01 第 3 条）。
5. 分别从各 schema 读取后在内存比对，不做跨 schema JOIN（模拟数据来自不同数据源）。
6. `LEDGER_MISSING`、`TRANSFER_MISMATCH` 的插入规则同 REC-03 第 4 条：本批次已有同键差错时跳过，不改已有行，`diff_count` 按实际行数统计。

验收：

- [ ] 删除某笔支付对应的 outbox 事件并阻止入账后，对账识别为 `LEDGER_MISSING`。
- [ ] 测试中删除一张 `TRANSFER_IN` 凭证（构造数据），对账识别为 `TRANSFER_MISMATCH`；正常转账差错数为 0。

#### REC-06 差错自动处理

`P0 · M6 · 依赖：REC-03、REC-05、PAY-09、MQ-05 · 表：recon_diff`

规则：

| 差错 | 自动处理 | 成功后 |
|---|---|---|
| `STATUS_MISMATCH`，本地 PROCESSING | `PaymentService.SyncChannelTxn`（`REQUERY`） | RESOLVED |
| `CHANNEL_MISSING` | `SyncChannelTxn` 查单（`REQUERY`）；渠道确认 SUCCESS 说明账单遗漏 | IGNORED，说明“渠道账单遗漏，查单确认成功”；渠道确认不是成功见下文“严重不一致” |
| `LEDGER_MISSING` | `PaymentService.ReplayEvent(biz_no, event_type)`，写 `handle_action = REPLAY`，保持 PENDING | 由下面的复核推进为 RESOLVED |
| `LOCAL_MISSING`、`AMOUNT_MISMATCH`、`STATUS_MISMATCH`（本地已是终态）、`TRANSFER_MISMATCH` | 无 | 保持 PENDING，ERROR 日志，写入对账报告；真实系统需要人工处理与挂账，本项目不实现（REC-07 CUT） |

自动处理记录 `handle_action`、`handler = system`。处理失败（如 `SyncChannelTxn` 调用出错或超时）时同样写 `handle_action`（`REQUERY` 或 `REPLAY`）、`handler = system`，`handle_note` 记录失败原因，差错保持 PENDING。写入用 CAS `WHERE id = ? AND status = 'PENDING'`。因为 `handle_action` 非空，重跑和接管（REC-01 第 3、5 条）都不会删除它。后续批次重试：每次批次运行（任意账期）都查询自动重试窗口内（见下）全部 `status = 'PENDING' AND handle_action = 'REQUERY'` 的差错（不限批次，与下面的 LEDGER_MISSING 复核相同），排除已判定为严重不一致的 `CHANNEL_MISSING`（`channel_status` 非空，见下），再执行一次 `SyncChannelTxn`，成功后按上表置为终态。`REPLAY` 失败的差错由 LEDGER_MISSING 复核发现凭证仍缺失后再次 `ReplayEvent`。（2026-10-27 用户决定）

自动重试窗口：跨批次的 REQUERY 重试和 LEDGER_MISSING 复核（含再次重放）只处理 `bill_date` 在 D−8 到 D−2 之间的差错。D 为本次运行时注入时钟的东八区日期，与批次自己的账期无关；这个范围就是 REC-01 第 1 条的前 7 个账期。D−1 账期的差错由它自己的批次运行处理，次日起进入窗口。查询走 `idx_status_bill_date`（`status = 'PENDING'` 加 `bill_date` 范围）。窗口外仍为 PENDING 的差错不再自动处理，保持 PENDING 等人工，列入 REC-08 报告。本批次运行对本次新插入的差错执行上表的自动处理，不受窗口限制；重跑或接管时保留的已自动处理差错不在本批次运行里重复处理，只走跨批次重试。（2026-10-27 用户决定）

严重不一致：`CHANNEL_MISSING` 查单成功返回、但渠道状态不是 SUCCESS（本地 SUCCESS、渠道 FAILED、PROCESSING 或查无此单）时，CAS（`WHERE id = ? AND status = 'PENDING'`）写 `handle_action = REQUERY`、`handler = system`、`channel_status` 为 `SyncChannelTxn` 返回的渠道状态，`handle_note` 记录渠道状态。差错保持 PENDING，输出 ERROR 日志（`diff_no`、`biz_no`、本地与渠道状态），`pay_recon_channel_inconsistent_total{channel}` + 1。之后不再自动重试，等人工。`channel_status` 非空就是“已判定、不再重试”的标记：`CHANNEL_MISSING` 插入时渠道侧没有账单行，`channel_status` 为空；查单调用出错时也不写它，因此调用失败的仍会重试。（2026-10-27 用户决定）

置为 RESOLVED 或 IGNORED 的更新都用 CAS：`UPDATE recon_diff SET status = ?, handler = 'system', handle_note = ? … WHERE id = ? AND status = 'PENDING'`（本次处理要写的 `handle_action` 在同一条语句里写）。影响 0 行说明该差错已被其他运行（另一账期的批次、并发触发）处理：重新读取，按当前状态返回，不报错，不覆盖已有的 `handle_action`、`handler`、`handle_note`。重新读取查不到该行（已被删除）时，按“已不存在”返回：不报错、不重试、不重新插入，输出 WARN 日志（`diff_no`、`batch_no`）。按 REC-01 第 3、5 条，只有 `handle_action` 为空的 PENDING 差错会被删除，删除它的那次运行会重新比对生成，所以这种情况只出现在两种场景：旧运行在批次被接管或重跑后仍在处理一条尚未写入 `handle_action` 的差错；人工误删。

LEDGER_MISSING 复核：每次批次运行（任意账期）都查询全部 `diff_type = LEDGER_MISSING AND status = 'PENDING' AND handle_action = 'REPLAY'` 的差错（不限批次），到对应账户分片确认凭证存在且金额一致 → CAS `PENDING → RESOLVED`（`handler = system`，说明“重放后复核通过”）；否则再执行一次 `ReplayEvent`（重放幂等，MQ-05 第 3 条），更新 `handle_note`，差错保持 PENDING。

验收：

- [ ] 状态不符（本地 PAYING）自动修复后，订单 SUCCESS 且商户入账。
- [ ] `LEDGER_MISSING` 经重放后，下一次任意账期的对账运行（差错的 `bill_date` 在自动重试窗口内）把它置为 RESOLVED。
- [ ] 自动处理失败也写 `handle_action`：假渠道查单返回错误时，`STATUS_MISMATCH`（本地 PROCESSING）差错保持 PENDING，`handle_action = REQUERY`、`handler = system`，`handle_note` 含失败原因；对同一批次 force 重跑后该行仍在（`id` 不变）；下一次任意账期的批次运行（差错在窗口内）在查单恢复后把它置为 RESOLVED。
- [ ] `ReplayEvent` 调用失败的 `LEDGER_MISSING`：`handle_action = REPLAY`、保持 PENDING；下一次任意账期的批次运行复核时凭证仍缺失，再次重放；重放入账后的下一次运行置为 RESOLVED。
- [ ] 自动重试窗口：Fake 时钟日期为 D，构造 `handle_action = REQUERY` 的 PENDING 差错，`bill_date` 分别为 D−1、D−2、D−8、D−9，查单都会成功；一次任意账期的批次运行只对 D−2、D−8 两条调用 `SyncChannelTxn` 并置为终态，D−1、D−9 不变。对 `handle_action = REPLAY`、凭证仍缺失、`bill_date` 为 D−9 的 `LEDGER_MISSING`，复核不再重放，差错保持 PENDING。
- [ ] 严重不一致：本地 SUCCESS 的 `CHANNEL_MISSING`，假渠道查单返回 FAILED：差错保持 PENDING，`handle_action = REQUERY`，`channel_status = FAILED`，`handle_note` 含渠道状态；有一条 ERROR 日志，`pay_recon_channel_inconsistent_total` +1；之后窗口内的批次运行都不再对它查单，指标不再增加。查单返回错误的同类差错 `channel_status` 仍为空，下一次运行会重试。
- [ ] CAS 读不到行：测试删除一条正在处理的 PENDING 差错后，处理方的 CAS 影响 0 行，重新读取查不到，返回“已不存在”，不报错、不重新插入，有一条 WARN 日志。
- [ ] 同一条 PENDING 差错被两个运行并发置为终态：只有一次更新影响 1 行；另一次影响 0 行，重新读取后返回 RESOLVED（或 IGNORED），不报错，`handle_note` 保持先完成那次写入的值。

#### REC-07 差错人工处理与挂账

`CUT`：不提供人工处理接口和差错挂账账户。未自动解决的差错保持 PENDING，出现在对账报告和 ERROR 日志中。

#### REC-08 对账报告

`P0 · M6 · 依赖：REC-03、REC-05`

规则：

1. 批次完成后，CLI 和 `POST /admin/recon/run` 的响应输出：`recon_batch` 中的账期、渠道、双方笔数与金额、匹配数、差错数；按 `recon_diff` 实时聚合的各类差错数与自动处理数（`handle_action` 非空）；前 100 条差错。不按支付、退款分列。另列“等待人工处理”：不限批次，`status = 'PENDING' AND handle_action IS NOT NULL` 且 `bill_date` 早于自动重试窗口（REC-06，早于 D−8）的差错，以及已判定为严重不一致的 `CHANNEL_MISSING`（`channel_status` 非空）。响应字段为 `manual_total`（总数）和 `manual_diffs`（`ReconDiff` 数组）。`manual_diffs` 按 `bill_date` 升序、同一 `bill_date` 内按 `id` 升序取前 100 条。（2026-10-27 用户决定）
2. 指标 `pay_recon_diffs_total{diff_type}`；PENDING 差错数作为 gauge `pay_recon_pending_diffs`。

验收：

- [ ] 报告中的笔数和金额与账单、本地数据的手工统计一致。
- [ ] “等待人工处理”：构造 `bill_date` 为 D−9、`handle_action = REQUERY` 的 PENDING 差错，D−3、`channel_status = FAILED` 的 `CHANNEL_MISSING`，D−3 窗口内 `handle_action = REQUERY`、`channel_status` 为空的差错，以及 D−9、`handle_action` 为空的 `LOCAL_MISSING`。报告的“等待人工处理”恰好列出前两条，`manual_total = 2`，`manual_diffs` 依次为 D−9、D−3 那条。
- [ ] “等待人工处理”的排序与截断：构造 101 条符合条件的差错，其中 3 条 `bill_date` 相同。`manual_total = 101`，`manual_diffs` 有 100 条，按 `bill_date` 升序；同一 `bill_date` 内按 `id` 升序；被截掉的是排序最后的那条。

### 4.11 可观测与运维（OPS）

#### OPS-01 健康检查与优雅退出

`P0 · M3 · 依赖：BASE-08、BASE-09`

规则：

1. `pkg/server` 在管理端口提供 `/healthz`（进程存活即 200）和 `/readyz`（依赖检查，单项超时 1 秒，任一项失败返回 503）：数据库必检。检查项可注册：gateway 的 Redis 检查由 GW-04 加入，payment、account 的 Kafka 检查由 MQ-03 加入。
2. 收到 SIGTERM：`/readyz` 立即返回 503 → 停止接收新请求和新消息 → 等待在途请求、消息处理和任务批次完成（上限 10 秒）→ 刷新已注册的生产者（MQ-03）→ 关闭连接。
3. docker-compose 中 `stop_grace_period: 15s`。

验收：

- [x] 收到 SIGTERM 后 `/readyz` 立即返回 503；用阻塞 1 秒的测试 handler 构造的在途请求正常完成并返回 200；新连接被拒绝；进程在 10 秒内退出。
- [x] 在途请求超过 10 秒未完成时，进程在 10 秒上限到达后退出，不无限等待。
- [x] 数据库不可用时 `/readyz` 返回 503、`/healthz` 仍为 200；数据库恢复后 `/readyz` 返回 200。

原验收“压测进行中 `docker stop` payment”需要 QA-02 的压测脚本，移到 QA-01。

#### OPS-02 链路追踪

`P0 · M5 · 依赖：GW-01、GW-02、GW-06、ACC-11、MQ-03、MQ-04`

规则：

1. OTel SDK，OTLP/gRPC 上报 Jaeger；`service.name` 为进程名；默认全量采样，可配置比例。
2. 覆盖：Hertz 服务端、Kitex 客户端与服务端、payment → channel 的 HTTP 调用、Kafka 生产与消费、补偿任务每一批。
3. 消费端从信封 `trace_context` 提取上下文，作为 link 关联到原请求链路。
4. span 属性只放单号、状态、错误码，不放金额以外的业务明细，不放密钥和签名。

验收：

- [ ] 在 Jaeger 中能按 `pay_no` 找到从网关下单、发起支付、回调到入账消费的完整链路。

#### OPS-03 业务与运行指标

`P0 · M5 · 依赖：OPS-01、GW-02、GW-06、MQ-04`

规则：各进程在管理端口暴露 `/metrics`，包含 Go runtime 与进程指标，以及：

| 指标 | 类型 | 标签 |
|---|---|---|
| `pay_http_requests_total`、`pay_http_request_duration_seconds` | counter、histogram | route、code |
| `pay_rpc_duration_seconds` | histogram | service、method、biz_code |
| `pay_order_transitions_total` | counter | type、from、to |
| `pay_outbox_pending`、`pay_outbox_dead` | gauge | topic |
| `pay_mq_consume_lag`、`pay_mq_consume_errors_total`、`pay_mq_dlq_total` | gauge、counter、counter | topic、group |
| `pay_channel_requests_total`、`pay_channel_request_duration_seconds` | counter、histogram | api、result |
| `pay_channel_query_total`、`pay_channel_query_exhausted_total` | counter | result |
| `pay_callback_conflicts_total` | counter | channel |
| `pay_callback_rejected_total` | counter | channel、code |
| `pay_notify_attempts_total` | counter | result |
| `pay_ledger_check_failures_total` | counter | — |
| `pay_ledger_post_duration_seconds` | histogram | biz_type、shard |
| `pay_ledger_batch_size`、`pay_ledger_batch_fallback_total` | histogram、counter | shard |
| `pay_stage_duration_seconds` | histogram | chain、stage（OPT-01） |
| `pay_transfer_stuck`、`pay_transfer_inconsistent_total` | gauge、counter | mode |
| `pay_refund_quota_inconsistent_total` | counter | —（REF-03 第 5 条） |
| `pay_recon_diffs_total`、`pay_recon_pending_diffs` | counter、gauge | diff_type |
| `pay_recon_channel_inconsistent_total` | counter | channel（REC-06 严重不一致） |
| `pay_ratelimit_rejected_total`、`pay_nonce_unavailable_total` | counter | — |

ACC-10 只在进程内把无标签的 `pay_ledger_check_failures_total` 加 1（一次失败的完成调用加 1），不起 HTTP，也不引入 Prometheus 客户端。把这个计数通过 `/metrics` 暴露出去由本功能点负责。

约束：标签中不得出现单号、商户号以外的高基数值（商户号也只用于限流类指标）；延迟直方图桶 5ms、10ms、25ms、50ms、100ms、250ms、500ms、1s、2.5s、5s。

验收：

- [ ] 执行一次完整支付（下单 → 支付 → 回调 → 入账）后，`/metrics` 中：创建与发起支付两条路由的 `pay_http_requests_total` 各 +1；`pay_order_transitions_total` 的 INIT→PAYING、PAYING→SUCCESS 各 +1；`pay_channel_requests_total` +1；`pay_ledger_post_duration_seconds{biz_type="PAY"}` 样本数 +1。
- [ ] ACC-10 一次失败的全量核对后，`/metrics` 中 `pay_ledger_check_failures_total` +1。

#### OPS-04 监控面板与告警规则

`CUT`：不部署 Grafana，不配置告警规则。本文中的“告警”统一指“对应指标加一并输出 ERROR 日志”。压测报告直接使用 Prometheus 查询结果。

#### OPS-05 pprof 与管理端口

`P0 · M3 · 依赖：OPS-01`

规则：`net/http/pprof` 只注册在管理端口，与业务端口分离；宿主机只绑定 `127.0.0.1`。

进程仍监听 `:606x`（该进程网络里的所有接口），不改为 `127.0.0.1`。宿主机端口发布仍是 compose 里已有的 `127.0.0.1`。payment（:8881）、account（:8882）的业务端口是 Kitex，不提供 HTTP `/debug/pprof/`。channel-mock 业务端口 :9080 访问该路径返回 404。`/metrics` 仍由 OPS-03 提供，本功能点不注册。管理端口 `WriteTimeout` 为 45 秒，以便写出 30 秒 CPU profile；`ReadHeaderTimeout` 仍是 1 秒。

验收：

- [x] 业务端口访问 `/debug/pprof/` 返回 404；管理端口可采集 30 秒 CPU profile。

### 4.12 性能优化（OPT）

每个优化项都有独立开关，关闭时即为基线。报告在 `docs/perf-report.md` 中按“现象与证据 → 改动 → 同一场景前后数据 → 结论”记录，无效的尝试也要记录。

#### OPT-01 分阶段耗时埋点

`P0 · M5 · 依赖：OPS-02、OPS-03、GW-02、GW-06、GW-09、MQ-02、MQ-04、REF-02、TRF-02、TRF-03`

说明：依赖列出的是规则 2 中各阶段所在的功能点，埋点要加在它们的代码里。基线报告（原验收第 2 条）在 QA-02 验收。

规则：

1. `pkg/obs.Stage(ctx, chain, stage)` 返回结束函数，上报 `pay_stage_duration_seconds{chain, stage}`，同时作为 span 事件写入 trace。
2. 必须覆盖的阶段：

   | chain | stage |
   |---|---|
   | `pay_create` | `gw_auth`（验签含凭证获取）、`gw_nonce`、`rpc`、`db_insert` |
   | `pay_submit` | `gw_auth`、`db_tx1`、`channel_call`、`db_tx2` |
   | `pay_settle` | `callback_tx`（回调事务）、`outbox_wait`（写入到投递）、`mq_wait`（投递到消费）、`ledger_tx` |
   | `refund` | `gw_auth`、`db_quota_tx`、`rpc_freeze`、`db_tx`、`channel_call` |
   | `transfer_local` | `gw_auth`、`db_insert`、`rpc_transfer_local`、`db_finish_tx` |
   | `transfer_tcc` | `gw_auth`、`db_insert`、`try`、`db_decision`、`confirm`（或 `cancel`）、`db_finish_tx` |

3. QA-02 基线压测时导出每条链路各阶段的 P50 / P99 和占比，作为 OPT-02 ~ OPT-04 选择优化项的依据。

验收：

- [ ] 一次跨分片转账在 Jaeger 中能看到上述阶段，各阶段耗时之和与接口耗时的差值 < 5%。
- [ ] 6 条链路各执行一次后，`/metrics` 中规则 2 表内每个 `(chain, stage)` 的 `pay_stage_duration_seconds` 样本数都 ≥ 1（跨分片转账的 `transfer_tcc` 各阶段由 TRF-03 第 7 条移来）。

#### OPT-02 批量记账合并提交

`P0 · M7 · 依赖：MQ-04、QA-02、BASE-15 · 表：ledger_txn、ledger_entry、account`

规则：

1. 开关 `LEDGER_BATCH_ENABLED`（默认 true；false 即 MQ-04 逐条模式）。只批量处理 `payment.succeeded`；同一批中的其他事件在批量部分提交后按原顺序逐条处理。
2. 每个分区一次拉取的记录中，最多取 `LEDGER_BATCH_SIZE`（默认 100）条，按 `shard.Of(merchant_id)` 分组，每组一个分片事务：

   ```
   BEGIN
     SELECT biz_no FROM ledger_txn WHERE biz_type = 'PAY' AND biz_no IN (...)   -- 已入账的跳过
     批量 INSERT ledger_txn（剩余事件）
       -- 1062（并发消费者或重放抢先入账）：ROLLBACK，本组退化为逐条处理
     按账户聚合增量（同一商户、同一平台子账户的多笔合并）
     按 account_id 升序：SELECT ... FOR UPDATE；任一账户 status 不是 ACTIVE：ROLLBACK，本组退化为逐条处理（逐条 Post 返回 40005）
     UPDATE account SET balance = balance + Σ, version = version + 1          -- 行锁已持有，状态不会再变
     按事件顺序计算每条分录的 balance_after（从加锁时读到的余额依次累加）
     批量 INSERT ledger_entry
   COMMIT
   ```

3. 故障点 `ledger.after_batch_commit`：任一分片组提交后、offset 提交前。
4. 所有分组提交成功后才提交该批 offset。部分分组已提交时崩溃，重新消费时第 2 步的预查会跳过已入账事件。
5. 反序列化失败的消息取不到 `merchant_id`，在分组前识别并直接按 MQ-03 进入 DLQ，不影响任何分组（R16）。组内事件不可重试（商户账户不存在）：本组回滚并退化为逐条处理，坏消息进入 DLQ，其余事件正常入账，`pay_ledger_batch_fallback_total + 1`。组内有非 ACTIVE 账户（第 2 步的 `40005` 分支）同样回滚并退化为逐条处理、`pay_ledger_batch_fallback_total + 1`，该事件按 MQ-04 逐条模式处理 Post 返回的 `40005`：不可重试，进入 DLQ 并输出 ERROR 日志，可由 MQ-05 重放；其余事件正常入账。
6. 入账事件不依赖顺序（MQ-02 第 4 条），批量合并不改变正确性；`balance_after` 仍逐条正确，ACC-10 核对不需要特殊处理。
7. 每个事件的分录与 MQ-04 逐条模式相同：手续费 F = 0 时省略该事件的手续费分录（凭证只有借渠道应收 A、贷商户 A 两条），F = 0 的事件不计入手续费收入子账户的聚合增量，也不为它锁手续费收入子账户。

验收：

- [ ] 1000 条 `payment.succeeded`（含 100 条重复、2 条坏消息）：凭证恰好 898 张，坏消息进入 DLQ，`CheckBalance` 通过。
- [ ] 在 `ledger.after_batch_commit` kill account，重启后不重复入账（C12）。
- [ ] 同一批中混有 `fee_amount = 0` 与 `fee_amount > 0` 的事件：F = 0 的凭证只有 2 条分录；同一组事件分别用批量模式和逐条模式入账，两边每张凭证的分录（账户、方向、金额）和各账户最终余额一致，`CheckBalance` 通过。
- [ ] 一批中有一个商户账户被测试改为非 ACTIVE：该组回滚并退化为逐条，`pay_ledger_batch_fallback_total` +1，其余事件各入账 1 次；该事件进入 DLQ、`pay_mq_dlq_total` +1、没有凭证，与关闭批量开关时相同。
- [ ] 压测报告给出逐条与批量模式下入账消费吞吐、`pay_mq_consume_lag`、数据库提交次数的对比（目标见 NFR-PERF-03）。

#### OPT-03 热点平台账户子账户拆分

`P0 · M7 · 依赖：ACC-03、QA-02 · 表：account`

规则：

1. 平台账户按 `PLATFORM_SUB_ACCOUNTS` 拆分（ACC-13 第 4 条、§2.5.1），从 ACC-01 开始即按子账户建模；本功能点负责压测对比和结论。
2. 基线：`PLATFORM_SUB_ACCOUNTS = 1`；优化：`= 8`。两次压测使用同一场景（支付全链路，逐条记账模式，排除 OPT-02 的干扰），再做一次 8 + 批量模式的组合测试。
3. 采集：`performance_schema.data_lock_waits` 中等待平台账户行锁的次数与时长、`pay_ledger_post_duration_seconds` P99、入账吞吐。

验收：

- [ ] 报告给出 1 与 8 个子账户下的行锁等待、记账 P99、入账吞吐对比（目标见 NFR-PERF-07）。
- [ ] 两种配置下压测结束后 `CheckBalance` 都通过，平台账户汇总余额一致。

#### OPT-04 链路延迟优化

`P0 · M7 · 依赖：OPT-01、QA-02`

规则：

1. 根据 OPT-01 基线中占比最高的阶段选择优化项，支付、退款、转账三条链路各至少完成 1 项有数据证明的优化。候选项（每项有开关）：

   | 优化项 | 开关 | 影响链路 |
   |---|---|---|
   | 跨分片转账两个分支并行 Try / Confirm | `TRANSFER_PARALLEL_BRANCH` | 转账 |
   | 商户凭证进程内缓存（GW-08） | `GW_CREDENTIAL_CACHE_TTL` | 全部 |
   | 商户配置进程内缓存（PAY-01 第 3 条） | `MERCHANT_CONFIG_CACHE_TTL` | 支付、退款、转账 |
   | 事务内减少往返：多条 INSERT 合并、去掉不必要的回读 | 代码对比，记录提交号 | 全部 |
   | 连接池参数（`MaxOpenConns`、`MaxIdleConns`）与 Kitex 长连接池 | 配置 | 全部 |

2. 每项优化单独压测：其余开关保持不变，只切换一个，记录 P50 / P99 与阶段耗时变化。
3. 与 QA-03 的关系：pprof 定位的 CPU / 分配热点优化记录在 `docs/pprof-tuning.md`，本功能点记录延迟结构类优化，两者可以重叠引用。

验收：

- [ ] 报告中三条链路各至少 1 项优化，附前后 P50 / P99 与阶段耗时对比。
- [ ] 并行分支使跨分片转账 P99 降低（目标见 NFR-PERF-06）。

#### OPT-05 平台账户缓冲入账

`P2 · M8 · 依赖：OPT-03`

说明：平台账户的分录照常写入，余额改为写入缓冲表，由后台任务按账户汇总后批量更新。可以彻底消除平台账户行锁，但分录的 `balance_after` 失去意义，核对逻辑需要改为“账户余额 + 未应用缓冲 = 分录汇总”。P0、P1 全部完成后才考虑；做之前先在 `docs/01-tech-decisions.md` 追加决策。

### 4.13 工具（TL）

#### TL-01 paycli

`P0 · M3 · 依赖：BASE-13`

规则：

1. 配置来自环境变量或参数，参数覆盖同名环境变量。网关地址 `PAYCLI_GATEWAY_URL` / `--gateway`，`app_id` `PAYCLI_APP_ID` / `--app-id`，`app_secret` `PAYCLI_APP_SECRET` / `--app-secret`，管理 Token `PAYCLI_ADMIN_TOKEN` / `--admin-token`。加载时不因另一个命令用不到的字段为空而失败。任一命令缺网关地址、商户命令缺 `app_id` 或 `app_secret`、管理命令缺 Token，退出码 1，错误文本不含密钥。HTTP 客户端超时取 §5.4.1 网关调用超时：默认 1 秒；`pay`、`close`、`refund` 为 5 秒；`transfer` 为 3 秒；`admin recon-run` 与 `admin check` 为 120 秒。
2. 子命令：`create`、`pay`、`query`、`close`、`refund`、`query-refund`、`transfer`、`query-transfer`、`balance`；`admin recon-run`、`admin replay`、`admin check`。
3. 商户命令自动生成 nonce（16–32 位 `[A-Za-z0-9]`，生产用 `crypto/rand`）和时间戳（注入的时钟）并按接口 §3.1 调用 `pkg/sign` 签名。响应按 §3.3 验签，失败时报错且退出码 1，不把响应当成成功结果写出。管理命令只带 `X-Admin-Token`；写操作另带 `X-Admin-Operator`。管理响应不签名，不因缺少 `X-Signature` 失败。
4. 验签通过（管理命令不验签）后把响应体原始字节写到 stdout。`code` 不等于 0 时仍然写出该 JSON，进程退出码为 1。无子命令时退出码 1，并在 stderr 给出用法。`query` 的 `pay_no` 与 `out_trade_no` 必须二选一；查询退款、查询转账、创建退款的原单号、事件重放同样按接口的二选一校验。不合法则退出码 1，不发请求。密钥和 Token 不出现在 stdout、stderr 或日志中。

说明：本功能点在 M3，不依赖网关，验收用测试内的假服务端（httptest）。经网关跑通完整流程的验收移到 GW-09。

验收：

- [x] 签名与 `docs/05-api.md` §3.7 测试向量一致。
- [x] 每个子命令对假服务端发出的请求，方法、路径、body 与 `X-App-Id`、`X-Timestamp`、`X-Nonce`、`X-Signature` 头正确，假服务端按 §3.1 验签通过；两次调用的 nonce 不同。
- [x] 假服务端返回正确签名的响应时输出 JSON、退出码 0；响应签名被篡改时报错、退出码 1；返回 `code ≠ 0` 时退出码 1。

#### TL-02 merchant-demo

`P0 · M5 · 依赖：BASE-13`

规则：

1. `POST /notify` 验证平台签名，失败返回 401；成功返回 `SUCCESS`。
2. 环境变量 `FAIL_FIRST_N` 控制每个单号前 N 次通知返回 500。
3. `GET /stats?biz_no=` 返回收到的通知次数和最后一次内容（内存保存）。

验收：

- [ ] `FAIL_FIRST_N=3`：同一单号的前 3 次正确签名通知返回 500，第 4 次返回 200、body `SUCCESS`。
- [ ] 签名错误的通知（用另一单号）返回 401。
- [ ] 第 1 条的 4 次通知之后，`GET /stats?biz_no=<该单号>` 返回次数 4 和第 4 次通知的 body。

#### TL-03 一键演示脚本

`P1 · M8 · 依赖：TL-01、GW-07、GW-09、MQ-04、REF-03、TRF-02、TRF-03、REC-08`

规则：`scripts/demo.sh` 依次演示下单、支付、回调、入账、退款、同分片转账、跨分片转账、余额查询、余额核对、当天对账，每一步打印关键结果。

验收：

- [ ] 在 `make up` 后执行成功，全程 < 1 分钟。

#### TL-04 OpenAPI 文档

`P1 · M8 · 依赖：GW-02、GW-09`

规则：`docs/api/openapi.yaml` 覆盖 `docs/05-api.md` §4 全部商户接口，包含签名说明与错误码。

验收：

- [ ] 通过 OpenAPI 3.1 校验工具检查。

### 4.14 质量保证与交付（QA、DOC）

#### QA-01 故障演练

`P0 · M7 · 依赖：BASE-15、QA-02、CH-06、GW-04、GW-06、GW-07、GW-09、PAY-05、REF-04、TRF-04、MQ-02、MQ-04、OPT-02、REC-03、REC-05、OPS-01`

说明：`pkg/fault` 与故障点清单移到 BASE-15，本功能点只做演练。依赖列出的是 C1–C13 各场景涉及的功能点；C7、C13 和优雅退出演练要在压测中进行，所以依赖 QA-02 的压测脚本与 `test/chaos/verify.sh`。

规则：

1. 故障点由 BASE-15 提供，清单见 BASE-15 规则 6；演练通过 `FAULT_POINTS` 打开。
2. 演练场景（`test/chaos/`，通过 `docker-compose.chaos.yml` 启动）：

   | # | 场景 | 方式 | 期望 |
   |---|---|---|---|
   | C1 | 渠道已扣款、回调处理前 payment 崩溃 | SLOW 场景下单，5 秒后 `docker kill -s KILL payment`，40 秒后启动 | 回调重试或查单使订单 SUCCESS，只入账一次 |
   | C2 | 订单 PAYING 后、调渠道前崩溃 | `pay.after_tx1` | 查单 NOT_FOUND → 关单 → CLOSED，无入账 |
   | C3 | 回调事务提交后、响应渠道前崩溃 | `callback.after_commit` | 渠道重试被识别为重复，事件只有 1 条 |
   | C4 | 投递 Kafka 成功、outbox 回写前崩溃 | `outbox.after_send` | 该行仍为 PENDING，租约到期后被重新投递；消息重复，只入账一次 |
   | C5 | 入账提交后、offset 提交前崩溃 | `ledger.after_commit` | 重复消费，只入账一次 |
   | C6 | 退款冻结后、置为 PROCESSING 前崩溃 | `refund.after_freeze` | 卡单恢复完成退款，冻结单只有 1 张 |
   | C7 | 压测中 Kafka 停止 2 分钟 | `docker stop kafka` | outbox 积压，恢复后全部投递 |
   | C8 | MySQL 重启；Redis 停止 | `docker restart mysql`、`docker stop redis` | 服务自动重连；Redis 停止期间商户接口 503、回调正常 |
   | C9 | 跨分片转账两个 Try 成功、决策落盘前崩溃 | `transfer.after_try` | 恢复任务取消：冻结 RELEASED、收款分支 CANCELLED、转账 FAILED，无凭证 |
   | C10 | 决策 CONFIRMING 落盘后、发起 Confirm 前崩溃 | `transfer.after_decision` | 恢复任务确认：转账 SUCCESS，`TRANSFER_OUT`、`TRANSFER_IN` 各 1 张 |
   | C11 | 付款方已确认、收款方未确认时崩溃（串行模式） | `transfer.after_payer_confirm` | 崩溃期间在途合计 = T；恢复后 SUCCESS，在途合计 0 |
   | C12 | 批量入账某分片组提交后、offset 提交前崩溃 | `ledger.after_batch_commit` | 重复消费被预查跳过，每笔支付只入账一次 |
   | C13 | 跨分片转账压测中 kill account 并在 20 秒后启动 | `docker kill -s KILL account` | 全部转账最终终态，在途合计 0，`CheckBalance` 通过 |

3. 每个场景结束后执行 `test/chaos/verify.sh`（QA-02 交付，检查项见 QA-02 第 7 条）。
4. 优雅退出演练（由 OPS-01 移来）：压测进行中对 payment 执行 `docker stop`，再启动。

验收（A5、A11）：

- [ ] C1–C13 全部通过 verify，结果记录到 `docs/chaos-report.md`。
- [ ] 优雅退出演练：客户端除连接被拒外没有 5xx 以外的异常，重启后 verify 通过，结果记录到 `docs/chaos-report.md`。

#### QA-02 基线压测与报告

`P0 · M6 · 依赖：CH-07、OPT-01、GW-02、GW-04、GW-05、GW-06、GW-07、GW-09、MQ-02、MQ-04、REF-03、REF-04、TRF-04、REC-03、REC-05`

说明：2026-10-27 拆分。原 QA-02 横跨 M6–M7，M7 的优化后复测依赖 OPT-02 ~ OPT-04，而它们又依赖 QA-02 的基线，形成循环。本功能点只做 M6 的基线，优化后复测是 QA-05。依赖列出的是 S1–S7 经过的功能点，以及 verify 用到的管理接口与对账。

规则：

1. k6 脚本（`test/load/`），用 `k6/crypto` 计算签名，每个虚拟用户使用不同 nonce。
2. 场景（使用 `make seed-load` 的 100 个压测商户，限流配额调高，channel_mock 延迟通过 CH-07 设为 50–200ms）：

   | 场景 | 内容 |
   |---|---|
   | S1 | 只下单 |
   | S2 | 下单 + 查询（1:3） |
   | S3 | 支付全链路（下单 → 支付 → 回调 → 入账） |
   | S4 | 退款（对已入账订单发起部分退款） |
   | S5 | 同分片转账（同分片商户之间随机转账） |
   | S6 | 跨分片转账（跨分片商户之间随机转账） |
   | S7 | 混合：S3 : S4 : S5 : S6 = 5 : 1 : 2 : 2 |

3. 全部优化开关关闭时跑 S1–S7 基线（含 OPT-01 分阶段耗时）。优化后复测见 QA-05。
4. 每个场景至少持续 5 分钟，先预热 1 分钟。
5. 记录：QPS / TPS、P50/P95/P99、错误率、分阶段耗时、各进程 CPU 与内存、MySQL 慢查询与行锁等待（`performance_schema.data_lock_waits`）、outbox 积压、消费延迟。
6. 报告 `docs/perf-report.md`：环境、场景、基线结果、6 条链路的分阶段耗时表（P50 / P99 / 占比）、瓶颈分析（指出证据）、与 NFR-PERF 目标的对比。
7. 交付 `test/chaos/verify.sh`（QA-01 演练也用它），压测结束后执行，资金核对必须通过。检查项：
   - 不存在超过阈值仍为 PROCESSING 的 `channel_txn`（除 NEVER_FINISH 场景），不存在超过阈值仍未终态的 `transfer_order`。
   - 每个 SUCCESS 支付单恰好 1 张 PAY 凭证，每个 SUCCESS 退款单恰好 1 张 REFUND 凭证，转账单满足不变量 12。
   - 全量 `CheckBalance(expect_quiescent = true)` 通过，含两个分片在途合计为 0。
   - 对当天执行一次对账，差错数为 0（场景预期的差错除外）。
   - 任一项不通过时退出码非 0，并输出不通过的检查项。

验收（A8）：

- [ ] `docs/perf-report.md` 包含规则 6 的全部内容，S1–S7 每个场景有规则 5 的各项记录，6 条链路各有分阶段耗时表（由 OPT-01 移来）；报告写明脚本、命令与开关值，按此可复现。
- [ ] 每个场景压测结束后 `verify.sh` 退出码 0，结果记录在报告中。
- [ ] `verify.sh` 在正常数据上退出码 0；测试中用 SQL 篡改一笔余额、删除一张 PAY 凭证，分别使其退出码非 0 并指出对应检查项。

#### QA-03 pprof 调优记录

`P0 · M7 · 依赖：QA-02`

规则：

1. 至少完成一次基于 pprof（CPU、alloc、block 或 mutex）的真实优化。
2. 候选方向（以实测为准）：JSON 编解码与日志分配、签名计算的内存分配、记账事务中的 SQL 拼装、mutex / block profile 中的锁竞争。行锁、批量提交、延迟结构类优化归入 OPT-02 ~ OPT-04。
3. `docs/pprof-tuning.md` 记录格式：现象 → profile / 指标证据 → 假设 → 改动 → 前后数据 → 结论。

验收（A9）：

- [ ] 记录中附优化前后的 profile 截图或 top 输出，以及同一压测场景的前后数据。

#### QA-04 验收运行

`P0 · M8 · 依赖：BASE-07、BASE-10、BASE-11、PAY-07、PAY-10、NTF-02、REC-06、REC-08、OPS-05、QA-01、QA-03、QA-05`

说明：依赖列只写直接依赖，列出的是没有被其他 P0 功能点依赖的 P0 功能点（DOC-01 除外，它依赖本功能点）；它们的传递闭包覆盖除 DOC-01 外的全部 P0。新增 P0 功能点时，若它不被其他 P0 依赖，须加到这里。

规则：

1. 在新克隆目录执行 `make up`，验证 A1。
2. `make cover` 输出 `service/account`、`service/payment`、`pkg/dbx`（幂等与事务）、`pkg/outbox` 的覆盖率，均 ≥ 70%（A10）。
3. 按 §7 逐条执行验收，结果记录到 `docs/acceptance-report.md`。

验收：

- [ ] A1–A12 全部通过。

#### QA-05 优化后复测与对比报告

`P0 · M7 · 依赖：QA-02、OPT-02、OPT-03、OPT-04、GW-08`

说明：2026-10-27 从原 QA-02 的 M7 阶段拆出。OPT-02、OPT-03、OPT-04、GW-08 各自用同一场景给出单项对比；本功能点在它们都完成后，用 QA-02 的脚本与环境统一复测，并把结果汇总成 A8、A12 的证据。依赖 GW-08 是因为全部打开时包含它的开关。

规则：

1. 环境、脚本、场景、预热与持续时间、记录项都与 QA-02 相同（QA-02 第 1、2、4、5 条），只改开关。
2. 按 OPT-02、OPT-03、OPT-04（含 GW-08 的 `GW_CREDENTIAL_CACHE_TTL`）逐项打开后重跑受影响的场景：一次只切换一个开关，其余保持基线值。
3. 最后全部开关打开跑 S7，与 QA-02 的基线 S7 对比。
4. 每次复测结束后执行 `test/chaos/verify.sh`，资金核对必须通过。
5. 结果追加到 `docs/perf-report.md` 的对比部分：每项写开关名与取值、对应提交号、受影响场景的前后 P50 / P99、吞吐与分阶段耗时，以及结论（无效的尝试也写）。

验收（A8、A12）：

- [ ] `docs/perf-report.md` 中每项优化（OPT-02、OPT-03、OPT-04 各项、GW-08）都有同一场景开关前后的 P50 / P99 与分阶段耗时对比，并写明开关取值与提交号。
- [ ] 全部开关打开的 S7 结果与基线 S7 并列，并与 NFR-PERF 目标逐项对比。
- [ ] 每次复测后 `verify.sh` 退出码 0，记录在报告中。

#### DOC-01 README 与面试材料

`P0 · M8 · 依赖：QA-04`

规则：

1. README：项目简介、架构图、一键启动、演示步骤、目录说明、文档索引、已知限制（管理接口简化鉴权、无 TLS、单节点 Kafka）。
2. 本文中的状态机、关键流程补充 Mermaid 图。
3. `docs/interview-notes.md` 覆盖 `PROJECT_GOALS.md` §9 全部要点，每一点关联到代码位置和测试或演练数据。

验收：

- [ ] 不熟悉项目的人按 README 能在 10 分钟内跑通演示。

## 5. 非功能需求

每条非功能需求都有编号和验证方式，在功能点的验收或 QA 功能点中落地。

### 5.1 资金正确性（NFR-COR）

| 编号 | 要求 | 验证 |
|---|---|---|
| NFR-COR-01 | §2.5.3 不变量 1：无浮点金额 | BASE-01 lint 规则 |
| NFR-COR-02 | 不变量 2：分录只追加 | 代码中不存在对 `ledger_txn`、`ledger_entry` 的 UPDATE/DELETE（lint 或 grep 检查进 CI）；运行账号无需此权限时可再收紧 |
| NFR-COR-03 | 不变量 3、4、5：余额、借贷、冻结一致 | ACC-10 `CheckBalance`；数据库 CHECK 约束兜底 |
| NFR-COR-04 | 不变量 6：写接口幂等 | 每个写接口都有“并发重复请求”测试 |
| NFR-COR-05 | 不变量 7：CAS 状态流转 | 每个状态机都有“并发推进”测试 |
| NFR-COR-06 | 任意单点崩溃后，补偿和对账完成时资金零差错 | QA-01 C1–C13 + `test/chaos/verify.sh`（QA-02） |
| NFR-COR-07 | 每个 SUCCESS 支付单恰好 1 张 PAY 凭证，每个 SUCCESS 退款单恰好 1 张 REFUND 凭证 | REC-05 内部对账；`verify.sh` |
| NFR-COR-08 | 不变量 10：一个事务只访问一个分片 | ACC-13 的类型约束；代码评审 |
| NFR-COR-09 | 不变量 11、12：在途守恒，转账凭证与转账单一致 | ACC-10；REC-05；C9–C13 |

### 5.2 一致性时效（NFR-CON）

默认配置下（非测试配置）的目标：

| 编号 | 指标 | 目标 |
|---|---|---|
| NFR-CON-01 | 渠道回调到达 → 商户余额入账 | P99 ≤ 3 秒（Kafka 正常） |
| NFR-CON-02 | 回调丢失 → 订单修正 | ≤ 1 分钟（查单退避前 3 档） |
| NFR-CON-03 | 订单成功 → 首次商户通知 | P99 ≤ 5 秒 |
| NFR-CON-04 | 退款卡在 INIT → 被恢复任务接手 | ≤ 1 分钟 |
| NFR-CON-05 | D 日对账结果产出 | D+1 日 10:30 前 |
| NFR-CON-06 | Kafka 恢复 → outbox 积压清空 | ≤ 积压时长 + 1 分钟（积压 ≤ 10 万条时） |
| NFR-CON-07 | 转账卡在非终态（account 已恢复）→ 终态 | ≤ 30 秒 |

### 5.3 性能（NFR-PERF）

环境：单台开发机（Apple Silicon，≥ 8 核、≥ 16GB 内存），全部组件运行在 docker-compose 中，各服务单实例。以下是初始目标，第一次基线压测后可在 §8 记录调整，但必须保留数据依据。

| 编号 | 场景 | 目标 |
|---|---|---|
| NFR-PERF-01 | 下单（S1，含验签、nonce、限流） | ≥ 1000 QPS，P99 ≤ 50ms，错误率 < 0.1% |
| NFR-PERF-02 | 查询（S2） | ≥ 2000 QPS，P99 ≤ 30ms |
| NFR-PERF-03 | 支付全链路（S3） | 基线 ≥ 200 TPS 持续 5 分钟，无积压持续增长；批量记账后入账消费吞吐 ≥ 基线 3 倍 |
| NFR-PERF-04 | 退款（S4） | ≥ 200 TPS；扣除渠道模拟延迟后接口 P99 ≤ 50ms |
| NFR-PERF-05 | 同分片转账（S5） | ≥ 500 TPS，P99 ≤ 30ms |
| NFR-PERF-06 | 跨分片转账（S6） | ≥ 300 TPS，P99 ≤ 60ms；并行分支比串行 P99 降低 ≥ 30% |
| NFR-PERF-07 | 热点平台账户 | 8 个子账户比 1 个子账户的平台账户行锁等待总时长降低 ≥ 80% |
| NFR-PERF-08 | 单次记账 `Post` | P99 ≤ 20ms |
| NFR-PERF-09 | 对账 10 万笔/日 | ≤ 60 秒 |

压测期间业务正确性同样要满足：压测结束后执行 `verify.sh`。

### 5.4 可靠性（NFR-REL）

#### 5.4.1 超时与重试

| 调用 | 超时 | 重试 |
|---|---|---|
| gateway → payment / account / recon（Kitex） | 连接 100ms，调用 1s（`Pay`、`ClosePayment`、`CreateRefund` 为 5s，因含渠道调用；`CreateTransfer` 为 3s，因含最多两轮分支调用；`RunBatch`、`CheckBalance` 为 120s，因同步执行整批对账或全量核对） | 不重试 |
| payment → account（Kitex） | 连接 100ms，调用 500ms | 幂等方法超时重试 1 次（清单见 ACC-11） |
| payment → channel（HTTP） | 连接 500ms，总 3s | 不自动重试，由查单补偿 |
| payment → 商户 notify_url | 5s | NTF-02 计划 |
| recon → 账单下载 | 30s | 3 次，间隔 10s |
| Redis | 50ms | 不重试 |
| MySQL | 连接 1s，读写 3s，事务 3s | 死锁和锁等待超时重试 3 次 |
| Kafka 生产 | 5s | outbox 退避 |

原则：只有幂等操作才自动重试；结果未知时不猜测，保持中间状态交给补偿。

#### 5.4.2 依赖故障时的行为

| 依赖 | 故障时 | 恢复后 |
|---|---|---|
| MySQL | 写接口返回 `10004`；`/readyz` 503；补偿任务跳过本轮 | 连接池自动重连，任务继续 |
| Redis | 商户接口返回 `10004`（nonce fail-closed）；渠道回调不受影响 | 自动恢复 |
| Kafka | 下单、支付、回调、转账不受影响；事件积压在 outbox；入账和通知延迟 | 投递器追平积压 |
| account | 下单、支付不受影响；退款保持 INIT；转账停在 TRYING / CONFIRMING / CANCELLING；入账消费暂停（消息保留在 Kafka） | REF-04、TRF-04 与消费者追平 |
| channel_mock | 发起支付结果未知、订单保持 PAYING；关单返回 `10004` | 查单补偿推进 |
| recon | 不影响交易 | 下次调度补跑 |

#### 5.4.3 进程级

| 编号 | 要求 |
|---|---|
| NFR-REL-01 | 任一服务被 `kill -9` 后，重启 30 秒内恢复服务，不需要人工处理数据 |
| NFR-REL-02 | 所有后台任务支持多实例并发运行（SKIP LOCKED 或 CAS 互斥），不依赖单实例 |
| NFR-REL-03 | SIGTERM 优雅退出 ≤ 10 秒（OPS-01） |
| NFR-REL-04 | 进程启动时不依赖 Kafka 可用（投递器和消费者自动重连） |
| NFR-REL-05 | 任何 goroutine 的 panic 都被恢复并记录，不导致进程退出（主流程启动失败除外） |

### 5.5 安全（NFR-SEC）

| 编号 | 要求 | 落地 |
|---|---|---|
| NFR-SEC-01 | 商户请求必须签名，签名覆盖方法、路径、查询串、时间戳、nonce、body | GW-03、BASE-13 |
| NFR-SEC-02 | 防重放：时间窗口 ±300 秒 + nonce 10 分钟去重 | GW-04 |
| NFR-SEC-03 | 平台响应和商户通知签名，商户可验证来源 | GW-01、NTF-02 |
| NFR-SEC-04 | 渠道回调必须验签，未验签的数据不得改变状态 | GW-06 |
| NFR-SEC-05 | 商户密钥 AES-256-GCM 加密存储；主密钥只来自环境变量；日志、trace、错误信息不含密钥 | BASE-14、BASE-10 |
| NFR-SEC-06 | 数据库账号最小权限，recon 对交易库只读 | BASE-03 |
| NFR-SEC-07 | 防 SSRF：`notify_url` 主机必须在 `NOTIFY_URL_ALLOW_HOSTS` 中，下单和发送时各校验一次；不跟随重定向 | PAY-02、NTF-02 |
| NFR-SEC-08 | 输入校验：请求体 ≤ 64KB，字段长度与格式校验，SQL 全部参数化 | GW-01、GW-02、GW-09、BASE-08 |
| NFR-SEC-09 | 不泄露信息：越权访问返回 `30001`；`10003` 不返回内部错误；验签失败不说明具体原因 | PAY-08、GW-01 |
| NFR-SEC-10 | 管理接口与 pprof 不暴露在业务端口，宿主机只绑定 `127.0.0.1`，管理接口需 Token | GW-07、OPS-05 |
| NFR-SEC-11 | 依赖版本锁定；CI 运行 `govulncheck`，有已知漏洞时处理或在 §8 记录 | BASE-01 |
| NFR-SEC-12 | 容器以非 root 用户运行 | BASE-01 |

已知限制（在 README 中说明）：服务间与对外通信无 TLS；管理接口为静态 Token；单节点 Kafka 与 MySQL 无高可用。

### 5.6 可观测与可追溯（NFR-OBS）

| 编号 | 要求 |
|---|---|
| NFR-OBS-01 | 日志结构化，包含 `trace_id`，敏感字段脱敏（BASE-10） |
| NFR-OBS-02 | 一笔支付从网关到入账的链路可在 Jaeger 中关联查看（OPS-02） |
| NFR-OBS-03 | OPS-03 中的指标全部暴露；资金类异常（DLQ、对账差错、核对失败、转账不一致、转账卡单）同时输出 ERROR 日志 |
| NFR-OBS-04 | 资金可追溯：给定任一 `pay_no`，可查到支付单、渠道交易（含回调次数、查单次数）、outbox 事件、记账凭证与分录、商户通知、对账结果；给定任一 `transfer_no`，可查到转账单、两个分片上的冻结单与分支、凭证与分录、通知；给定任一分录，可通过 `biz_type + biz_no` 反查业务单 |
| NFR-OBS-05 | 状态变化、补偿动作、差错处理、管理操作都有日志，包含单号、前后状态、来源（NOTIFY / QUERY / CLOSE / JOB / ADMIN） |

### 5.7 可测试性（NFR-TEST）

| 编号 | 要求 |
|---|---|
| NFR-TEST-01 | `biz` 层依赖接口，可用内存或假实现做单元测试 |
| NFR-TEST-02 | 时间通过 `clock.Clock` 注入；所有任务间隔、重试计划可配置，测试配置下完整流程分钟级完成 |
| NFR-TEST-03 | 渠道行为可确定性复现（CH-06 按金额触发），测试不依赖随机数 |
| NFR-TEST-04 | 并发测试一律带 `-race` |
| NFR-TEST-05 | 覆盖率：`service/account`、`service/payment`、`pkg/dbx`、`pkg/outbox` ≥ 70%；`pkg` 其余包 ≥ 80% |
| NFR-TEST-06 | 测试分层：单元（`make test`）、集成（`make test-integration`，真实 MySQL、Kafka）、端到端（`make e2e`，compose 环境）、演练（`make chaos`）、压测（`make load`） |
| NFR-TEST-07 | 测试之间互不依赖、可并行；不依赖执行顺序 |

### 5.8 可维护性（NFR-MNT）

| 编号 | 要求 |
|---|---|
| NFR-MNT-01 | 遵守 §2.3 分层与 import 约束，由 `depguard` 检查 |
| NFR-MNT-02 | IDL 只增不删（BASE-12）；事件结构只增字段，消费端忽略未知字段 |
| NFR-MNT-03 | 迁移只向前（BASE-03）；表结构变化同步 `docs/database.md` |
| NFR-MNT-04 | 错误码、指标、配置项、事件类型集中登记：错误码与配置项在 `docs/03-conventions.md` §5、§8，指标在 OPS-03，事件类型在 `docs/05-api.md` §11 |
| NFR-MNT-05 | 每个功能点的提交可追溯到编号（§0.2） |

### 5.9 部署与运行环境（NFR-DEP）

| 编号 | 要求 |
|---|---|
| NFR-DEP-01 | `make up` 一键启动，镜像已拉取时 ≤ 3 分钟全部 healthy（A1）。BASE-02 阶段的 healthy 范围见该功能点验收；应用进程在 OPS-01 提供健康检查后纳入 |
| NFR-DEP-02 | 支持 darwin/arm64 与 linux/amd64 |
| NFR-DEP-03 | 全部容器常驻内存合计 ≤ 6GB（`docker stats` 验证） |
| NFR-DEP-04 | 配置只来自环境变量，无配置中心依赖 |
| NFR-DEP-05 | 不依赖外网（镜像拉取除外） |

### 5.10 容量与数据保留（NFR-CAP）

- 设计容量：单表千万级（索引按此设计），演示和压测数据在百万级以内。账户数据按商户号分 2 个分片（D17），用于展示跨分片一致性，不以容量为目的；交易库不分片。
- 数据保留：

  | 数据 | 保留 |
  |---|---|
  | 账户、凭证、分录、冻结单、转账分支、支付单、退款单、转账单、渠道交易、对账数据 | 永久，不删除 |
  | outbox（SENT） | 7 天（MQ-06） |
  | merchant_notify | 永久（P1 可加归档） |
  | Kafka 消息 | 7 天 |
  | Redis nonce | 10 分钟 |
  | 日志、trace、指标 | 本地环境不做要求 |

## 6. 接口规范

全部接口（商户 API、签名规则与测试向量、商户通知、渠道回调入口、管理 API、模拟渠道 API、内部 RPC、事件）的字段、错误码与示例见 `docs/05-api.md`，该文档单独维护版本号与修改时间。

| 内容 | 位置 |
|---|---|
| 签名、鉴权、测试向量 V1–V4 | `docs/05-api.md` §3 |
| 商户 API | `docs/05-api.md` §4、§5 |
| 商户通知 | `docs/05-api.md` §6 |
| 渠道回调入口 | `docs/05-api.md` §7 |
| 管理 API | `docs/05-api.md` §8 |
| 模拟渠道 API、账单格式 | `docs/05-api.md` §9 |
| 内部 RPC 方法 | `docs/05-api.md` §10 |
| 事件 topic、信封、data | `docs/05-api.md` §11 |

功能点实现改变了接口行为时，按 `docs/05-api.md` §0.1 先更新接口文档。

## 7. 验收映射

| 验收 | 内容 | 功能点 | 证据 |
|---|---|---|---|
| A1 | 一键启动 | BASE-02、BASE-03、OPS-01 | QA-04 在新克隆目录执行 `make up` |
| A2 | 1000 goroutine 并发扣款无超扣 | ACC-03、ACC-04、ACC-07 | ACC-03、ACC-07、TRF-02 的并发测试 |
| A3 | 重复回调只入账一次 | PAY-04（订单层）、GW-06（端到端）、MQ-04（账务层） | 两层各自的集成测试 + GW-06 的端到端 DUP_NOTIFY |
| A4 | 回调丢失时补偿修正订单 | PAY-05、CH-06 | DROP_NOTIFY 集成测试 |
| A5 | 崩溃后资金零差错 | BASE-15、QA-01、PAY-05、REF-04、MQ-02、MQ-04、REC-* | `docs/chaos-report.md` C1–C8、C12 |
| A6 | 识别四类差错 | REC-03、CH-08、CH-09 | REC-03 构造账单测试 |
| A7 | 拒绝签名错误、时间戳过期、nonce 重复 | GW-03、GW-04、GW-06 | 网关单元测试 |
| A8 | 压测报告 | QA-02（基线）、QA-05（优化后复测） | `docs/perf-report.md` |
| A9 | pprof 真实优化 | QA-03 | `docs/pprof-tuning.md` |
| A10 | 核心包覆盖率 ≥ 70% | 全部 P0 | `make cover` |
| A11 | 跨分片转账在并发与任意崩溃点下资金守恒 | ACC-04~06、ACC-10、ACC-14、TRF-03、TRF-04 | TRF-03 并发测试；`docs/chaos-report.md` C9–C11、C13 |
| A12 | 三条链路的性能优化有前后对比数据 | OPT-01 ~ OPT-04、GW-08、QA-05 | `docs/perf-report.md` |

A5 的 C1–C8、C12 与 A11 的 C9–C11、C13 合起来正好是 QA-01 的 C1–C13：前者覆盖支付、退款、入账链路的崩溃与中间件故障，后者覆盖跨分片转账的崩溃。两行各自引用、不重复计入。

## 8. 待定事项与变更记录

### 8.1 待定事项

| # | 事项 | 默认处理 | 最晚决定 |
|---|---|---|---|
| Q1 | GitHub module path | 已定：`github.com/yym108/pay-system`（2026-10-13） | 已关闭 |
| Q2 | NFR-PERF 目标是否需要调整 | 首次基线压测后按数据调整 | M6 基线压测后 |
| Q3 | 热点账户是否做缓冲入账（OPT-05） | 子账户拆分（OPT-03）后行锁仍是瓶颈且 P0、P1 已完成才做 | M8 |
| Q4 | `STATUS_MISMATCH`（本地已终态）的处理 | 已随 REC-07 砍掉，保持 PENDING 进入报告 | 已关闭 |
| Q5 | account 的 SQL 是否像 payment 一样从 `biz/` 拆到 `dal/` | 已定（2026-10-27）：不拆，SQL 保持在 `biz/`，`dal/` 只放分片连接池与就绪检查（§2.3） | 已关闭 |
| Q6 | REC-01 超时接管的细节：INIT、DOWNLOADED 用哪个时间列判定；扫描间隔；`BILL_NOT_READY` 的重试上限 | 已定（2026-10-27）：INIT、DOWNLOADED 用 `updated_at` 超过 30 分钟判定，不改表（CHECKING 仍用 `started_at`）；不加独立扫描，只在现有 RunBatch 触发时机（定时、CLI、管理接口）判定；`BILL_NOT_READY` 每 30 分钟重试一次，最多 6 次，之后置 FAILED；同日补充：重试次数持久化在新增列 `recon_batch.retry_count`，每日定时触发顺带接管前 7 个账期中超时未终态的批次（REC-01 第 1、5、6 条） | 已关闭 |

### 8.2 变更记录

| 日期 | 版本 | 变更 |
|---|---|---|
| 2026-10-09 | v1.0 | 初版需求（FR 编号）与设计文档分开编写 |
| 2026-10-11 | v2.0 | 合并需求与设计为本文；改为按功能点编号推进；补充非功能需求；新增空回滚与防悬挂（ACC-05）、SSRF 白名单（NFR-SEC-07）、商户限流移到验签之后（GW-05）、通用幂等冲突码 `10006`；同一凭证中同一账户只能出现一次（ACC-03）；`docs/03-design.md` 内容并入本文 |
| 2026-10-12 | v2.1 | §2.6 全局约定拆到 `docs/03-conventions.md`，§6 接口规范拆到 `docs/05-api.md`，本文只保留索引；BASE-13 的签名算法改为引用接口文档；明确 `fail_code` 取字符串（`CHANNEL_FAILED`、`CHANNEL_REJECTED`、`INSUFFICIENT_BALANCE`、`FREEZE_CONFLICT`、`REFUND_TIMEOUT`），修正 REF-02 中 `fail_code = 40002` 的写法；编码流程移到 `.opencode/skills/pay-feature-dev/` |
| 2026-10-12 | v2.2 | 按 `docs/06-flows.md` §5 的 R1–R14 补齐规则并确认基线：GW-01 网关调用超时与 §5.4.1 对齐、请求体超限 413 / `10001`（R1、R14）；GW-06 未知单号与金额不一致返回 200 并告警，新增 `pay_callback_rejected_total`（R2）；PAY-05 关单 ALREADY_PAID / FAILED、退款重提 REJECTED 的处理（R3、R4）；PAY-06 关单渠道已失败（R5）；REF-02 渠道拒绝返回 `30006`、冻结 `40001` / `40005` 按内部错误处理（R6、R7）；CH-04 先幂等再校验额度（R8）；outbox 新增 `biz_no` 与 `idx_biz_no_event_type`（R9）；LEDGER_MISSING 跨批次复核、人工 FIX_ORDER 不直接 RESOLVED（R10）；`recon_diff` 新增 HANDLING，人工挂账先占位再记账（R11）；NTF-02 白名单校验失败直接 DEAD（R12）；GW-03 凭证回源失败 `10004`（R13）；ACC-04 透支账户冻结 `10001`、ReplayEvent 遇 PENDING 返回成功、批次重跑从 INIT 开始（R14） |
| 2026-10-12 | v3.0 | 项目聚焦支付、退款、转账三条链路（§1.1）。新增：账户 2 分片与路由 ACC-13、平台账户每分片一套并拆子账户、转账在途账户 `TRANSIT`（§2.5、D17、D19）；不变量 10–12；转账模块 TRF-01~05（同分片本地事务 + 失败墓碑 ACC-07，跨分片 TCC + 收款方分支 ACC-14，恢复任务）；性能模块 OPT-01~05（分阶段耗时、批量记账、子账户、链路延迟、缓冲入账 P2）；故障点与演练 C9–C13；NFR-PERF 增加退款、转账、热点账户目标；验收 A11、A12。修改：Freeze / ReleaseFreeze / ConfirmFreeze 改为以商户号为入参；Post 改为进程内方法；ACC-10 增加分片与在途守恒；CH-07、GW-08 升为 P0；NTF-02 改为固定间隔 5 次；GW-05 改为进程内商户限流，去掉全局限流与 Redis 限流；管理接口只保留重放、核对、触发对账；`recon_diff` 只保留 PENDING / RESOLVED / IGNORED，新增 `TRANSFER_MISMATCH`，`handle_action` FIX_ORDER 改名 REPLAY。砍掉：ACC-09、ACC-12、REC-04、REC-07、OPS-04、`SUSPENSE` 账户与 `ADJUST` 凭证；ACC-08 降为 P1。原 R11（HANDLING）随 REC-07 一并移除。按 `docs/06-flows.md` R15、R16 补充：分支调用返回 `10001` / `10006` 的处理（TRF-02、TRF-03），反序列化失败的消息在分组前直接进 DLQ（OPT-02） |
| 2026-10-13 | v3.0 | §0.2 新增第 6 条：功能点代码提交后在 `docs/reports/` 出具自包含报告，模板见 `.opencode/skills/pay-feature-dev/report-template.md`；流程只增加收尾步骤，不改业务规则 |
| 2026-10-13 | v3.0 | v3.0 确认为开发基线；§2.3 补充 account、payment 的包内划分，明确转账协调者放在 payment（来源 `docs/07-module-plan.md`）；Q1 关闭，module path 定为 `github.com/yym108/pay-system` |
| 2026-10-13 | v3.0 | BASE-02 口径澄清：`make up` 的 healthy 范围是 mysql、redis、kafka、prometheus、jaeger，外加 `kafka-init` 退出码 0。应用容器与 migrate/seed 放在 profile `apps` 占位，不启动；健康检查仍属 OPS-01，迁移与种子仍属 BASE-03。不改业务规则、资金流向、接口、表结构、错误码和 §3 依赖。A1 仍由 BASE-02、BASE-03、OPS-01 共同完成。NFR-DEP-01 补上同一范围说明。验收补上 `make up-lite` 与 topic 分区/副本，二者原来只写在规则里 |
| 2026-10-14 | v3.0 | BASE-03 口径澄清：种子按 BASE-14 已写明的密文格式用标准库加密，不实现 `pkg/secretbox`、不把 BASE-14 加入依赖、不把 `app_secret_cipher` 改为可空。`make seed-load` 只创建 100 个零余额压测商户，`scripts/load-fund.sh` 留给 QA-02，不直接写余额。`make migrate` / `make seed` 不并入默认 `make up`。不改资金规则、接口、表结构和错误码 |
| 2026-10-14 | v3.0 | BASE-03 补充 §2.3：`cmd/bootstrap`、`cmd/seed` 与 `internal/` 是初始化工具，不进 `make build` 的 7 个二进制，只经 Dockerfile 构建。`service/`、`gateway/`、`job/`、`pkg/` 不得 import `internal/`。运行镜像不安装 tzdata，时区数据用 `-tags timetzdata` 编进二进制。不改资金规则、接口、表结构和错误码 |
| 2026-10-14 | v3.0 | BASE-04 口径澄清：`Add` / `Sub` 的溢出含负数与 `MinInt64`，返回普通 error 而不回绕。`Fee` 的 `rateBP` 越界返回错误；负金额且费率非 0 返回错误（四舍五入加数只对非负金额成立），费率为 0 时结果为 0。`ValidatePayAmount` 越界返回错误。不新增错误码，不实现 `pkg/errno`。不改资金流向、接口和表结构 |
| 2026-10-14 | v3.0 | BASE-05 口径澄清：发号入口是 `Generator.NewNo(ctx, prefix)`，`ctx` 只用于取消等待，前缀规则不变。时间早于纪元或超出 41 位时返回错误，不回绕。回拨超过 5ms 时本包返回 `ErrClockRollback`，不返回数字码、不包装 Kitex 错误；调用方以后映射为已有的 `10004`。`WorkerIDFromEnv` 只返回错误，不 `os.Exit`。不实现 `pkg/clock` 和 `pkg/errno`。不改资金流向、接口、表结构和已登记错误码 |
| 2026-10-14 | v3.0 | BASE-06 口径澄清：`pkg/errno` 定义约定 §5.2 除 `30002` 外的全部码。`30002` 保留不用、没有名称，不提供可用错误。`nil` 是成功（码 `0`，msg `ok`，HTTP 200），不是 `10003`。未知非 nil 错误和未登记码视为 `10003`，对外消息固定 `internal error`，不含底层文本。`Error()` 只返回 `Msg`，底层错误只通过 `Unwrap` 取得。对外 msg 是稳定短英文，表见本节 BASE-06，并登记到约定 §5.2。`10001` 的 `HTTPStatus` 是 400；请求体超过 64KB 返回 413 是网关行为，本包不实现网关，也不另造码。Kitex 只用 `kerrors.BizStatusError` 双向转换，不生成 IDL。不实现 `errno.ErrDuplicate`（BASE-08）。不改资金流向、接口字段、表结构和已有码的含义 |
| 2026-10-15 | v3.0 | BASE-07 口径澄清：`Clock.Now` 返回 `time.Time`。`Real` 用 `time.FixedZone("Asia/Shanghai", 8*60*60)`，不调用 `time.LoadLocation`，因此不依赖 zoneinfo；中国自 1991 年起没有夏令时，固定 +8 与当前民用时间一致。运行镜像不安装 tzdata，本功能点不改 Dockerfile。`Fake` 的 `Set`、`Advance` 并发安全，`Advance(10s)` 后 `Now()` 精确前进 10 秒。`biz` 和 `job` 只通过 `Clock` 取时间，不新建空包，也不把 `pkg/idgen` 改成依赖 `pkg/clock`。不实现配置、日志和数据库。不改资金流向、接口、表结构和错误码 |
| 2026-10-15 | v3.0 | BASE-09 口径澄清：缺少或非法的环境变量，错误文本包含变量名。`secret:"true"` 的字段输出 `***`，不实现 `pkg/logx`。recon 不读 `MYSQL_DSN`，用 `ACCOUNT_DB_DSN_0` 的 host、port、query 和 `MYSQL_RECON_PASSWORD` 装配四条 `svc_recon` DSN（`pay_recon`、`pay_payment`、`pay_account_0`、`pay_account_1`）。channel-mock 不读 `MYSQL_DSN`，`MYSQL_HOST` 默认 `mysql`、`MYSQL_PORT` 默认 `3306`，装配 `svc_channel_mock` 到 `pay_channel_mock`。payment、account、seed 的 DSN 原样保存。装配不连接数据库。seed、bootstrap 只增加配置结构体，不改建号和种子流程。不改资金流向、接口、表结构和错误码 |
| 2026-10-15 | v3.0 | BASE-10 口径澄清：字段名不区分大小写，包含片段即匹配。卡号短于 10、手机号短于 7、证件号短于 2 时整段换成 `***`。分组键和 error 走同一套规则，error 先转成字符串。兜底扫描只认 ASCII 连续数字；正好 11 位且以 1 开头的按手机号，不当卡号；更短的数字（如金额）和长于 19 位的不脱敏。`trace_id`、`span_id` 来自 otel trace API 的 `SpanContextFromContext`，不引入 sdk。`request_id` 来自 `WithRequestID`。这三个关联字段按原值写入。访问日志只有路径、状态码、耗时、商户号、错误码，不记录请求体。不改 `pkg/config` 的脱敏。不改资金流向、接口、表结构和错误码 |
| 2026-10-15 | v3.0 | BASE-08 口径澄清：`WithTx` 对 1213、1205 含第一次在内最多执行 4 次，三次重试前等待 10ms、20ms、40ms（约定 §6.3 的 3 是重试次数）。事务总时长默认 3 秒，用父 ctx 派生，先到期的生效，超时取消并回滚。`IsDuplicate` 只识别 1062，不实现 `errno.ErrDuplicate`，不注册 `30002`。`MustAffectOne` 不映射业务码。不改资金流向、接口、表结构和已有错误码 |
| 2026-10-15 | v3.0 | BASE-11 口径澄清：每个测试包使用带随机后缀的独立 schema，结束时只删除这个库，不碰 `pay_account_0` 这类固定库名。设置 `TEST_MYSQL_DSN` 时复用已有 MySQL，不启动容器。2 秒的计时从 MySQL 已经接受连接开始，包括建库和执行调用方指定的 `migrations/<服务>`，不包括拉取镜像和容器第一次启动。Kafka 助手使用已锁定的 `apache/kafka:4.3.1`，不在每个 MySQL 测试里启动。不改资金流向、接口、表结构和错误码 |
| 2026-10-15 | v3.0 | BASE-12：IDL 为 `idl/base.thrift`、`idl/account.thrift`、`idl/payment.thrift`、`idl/recon.thrift`。记账 `Post` 是 account 进程内方法，不进入 RPC，IDL 里没有这个方法。一致性检查命令是 `make gen && git diff --exit-code kitex_gen`。不改资金流向、接口、表结构和错误码 |
| 2026-10-15 | v3.0 | ACC-13 口径澄清：`/readyz` 本功能点只提供处理函数，监听、Kafka、SIGTERM 仍属 OPS-01；`Of` 不读环境变量，分片数写死 2；`Sub` 的 N 由调用方传入。不改变资金规则、接口、表结构和错误码 |
| 2026-10-15 | v3.0 | ACC-01 口径澄清：幂等命中 `uk_owner_account_type_sub` 时返回已有行，不把余额和冻结金额改回 0。PLATFORM 账户 `shard` 只能是 0 或 1；`CHANNEL_RECEIVABLE` 的 `owner_id` 非空且不超过 32 个字符；`FEE_INCOME` 与 `TRANSIT` 的 `owner_id` 必须是 `PLATFORM`。账户号与另一组 owner 冲突时返回已有的 `10003`，对外消息不带 SQL。不改变资金规则、接口、表结构和错误码语义 |
| 2026-10-15 | v3.0 | ACC-02 口径澄清：平台渠道应收汇总的验收不调用记账。入账本身是 ACC-03，本测试用 SQL 写入余额，不改变资金规则 |
| 2026-10-15 | v3.0 | ACC-03 口径澄清：需求草稿里的 ROLLBACK 是调用方事务边界，Post 不执行 BEGIN、COMMIT、ROLLBACK，也不调用 `dbx.WithTx`。1062 不回滚调用方事务（MySQL 1062 不会中止 InnoDB 事务），用 `dbx.IsDuplicate` 判断后，在同一事务按 `(biz_type, biz_no)` SELECT 已有凭证，返回 `duplicated = true` 和已有 `txn_no`；即使这次的金额或分录和第一次不同，也返回第一次的结果，不插分录、不改余额；SELECT 不到则 `10003`。本连接查不到的 `account_id`（含只存在于另一分片的账户）返回 `40003`，不是 `40001`，因为本连接分不清“没有这个 id”和“在另一个库”；`GetAccount` 仍然用 `40001`。`biz_type = REVERSAL` 返回 `40003`，不写任何行：Post 没有 `reversal_of` 参数，表约束 `ck_ledger_txn_reversal` 要求 REVERSAL 必须有 `reversal_of`，冲正是 ACC-08，本功能点不加参数、不改表。不新增错误码，`40003` 对外英文仍是 `unbalanced entries`。验收 A2 的 1000 goroutine 不用 `dbx.WithTx`：`WithTx` 把单次事务限制在 3 秒，且包含等行锁的时间，1000 个等待者会被取消，成功和 `40002` 就数不成 500/500。该用例用 `BeginTxx(READ COMMITTED)`，父 ctx 至少 60 秒，Post 返回 nil 才 Commit，否则 Rollback。生产路径的单次记账仍由其他测试用 `dbx.WithTx` 覆盖。不改资金规则、接口、表结构和错误码 |
| 2026-10-19 | v3.0 | PAY-10 口径：商户费率历史表只追加；新生效时间必须严格晚于该商户已有最大 `effective_from`；相同时间相同费率幂等成功，相同时间不同费率 `10006`；不改 `merchant.fee_rate_bp`；订单手续费核对只报告，不改订单和账；扫描开始后新提交的费率下一轮才看得见。流水核对仍是 ACC-10。PAY-02 第 3 条改为按下单时刻已生效费率（`effective_from <= created_at` 的最近一档）计算并固化到 `fee_amount`，PAY-02 仍是 TODO。不新增错误码，不在 `cmd/payment/main.go` 启动核对循环。种子冲突更新写成 `merchant_id = merchant_fee_rate.merchant_id`，避免 `INSERT … SELECT` 未限定列名在 MySQL 8.4 报 1052 |
| 2026-10-19 | v3.0 | ACC-04 口径澄清：Freeze 是进程内方法。IDL 已有 Freeze，本功能点不实现 RPC、不改 IDL。`biz_type` 只允许 REFUND / TRANSFER；商户号分片不合法、`biz_no` 为空或超过 64 个码点、金额不是正的 int64 分，返回已有的 `10001`，不写行。冻结对象是 `shard.Of(merchant_id)` 上的 MERCHANT_SETTLE。`freeze_no` 在事务外 `NewNo(ctx, "FZ")`，`ErrClockRollback` 映射为已有的 `10004`。事务由 Freeze 自己 `dbx.WithTx` 提交，Post 仍不提交。UPDATE 同时 `version = version + 1`，与 Post 改余额对齐，不改变冻结金额，也不改 `balance`。影响 0 行时同一事务重读：非 ACTIVE 返回 `40005`（优先于余额不足），可用余额不够返回 `40002`，两者回滚且不留冻结单；重读后仍是 ACTIVE 且可用余额足够则返回 `10003`，不空转重试。1062 只用 `dbx.IsDuplicate`，不解析错误文本，同一事务按 `(biz_type, biz_no)` 读取：FROZEN / CONFIRMED 且金额和账户一致则返回已有结果且 `duplicated = true`，不增加冻结；RELEASED 一律 `40004`，不再比较金额；FROZEN / CONFIRMED 金额或账户不一致为 `10006`，旧行不变；读不到行是 `10003`。`duplicated` 只在进程内结果里，与 Post 一样，不改 RPC 字段。不新增错误码，不改资金规则、表结构和接口 |
| 2026-10-20 | v3.0 | ACC-05 口径澄清：ReleaseFreeze 是进程内方法。IDL 已有 ReleaseFreeze，本功能点不实现 RPC、不改 IDL。校验与 Freeze 相同，不合法返回已有的 `10001`，不发号、不写行；上下文已取消时参数不合法仍先返回 `10001`。参数合法后 nil service、nil ctx、nil 发号器、ShardDB 失败为 `10003`；已取消或已过截止时间的上下文原样返回，不发号。`freeze_no` 在事务外 `NewNo(ctx, "FZ")`，只给墓碑；`ErrClockRollback` 映射为已有的 `10004`。事务由 ReleaseFreeze 自己 `dbx.WithTx` 提交。空回滚插入 RELEASED 墓碑，不改 `balance` 和 `frozen_amount`；账户不存在 `40001`；`allow_overdraft` 不是 0 则 `10001`，不插入。已 RELEASED 幂等成功且不再比较金额。已 CONFIRMED 一律 `40004`。FROZEN 必须金额和账户一致，否则 `10006`。解冻不要求账户 ACTIVE，不返回 `40005`，不改 `balance`；冻结单和账户的 `version` 各加 1。账户更新 0 行则 `10003` 并回滚，冻结单保持 FROZEN，不改写成 `40002`。`duplicated` 只在进程内。不新增错误码，不改资金总规则，不实现 ACC-06 |
| 2026-10-20 | v3.0 | ACC-06 口径澄清：ConfirmFreeze 是进程内方法。IDL 已有 ConfirmFreeze，本功能点不实现 RPC、不改 IDL。请求没有金额，R 只用冻结单 `amount`。校验与 Release 一样先于 nil 判断，不合法返回已有的 `10001`。参数合法后 nil service、nil ctx、nil 发号器、nil pools、`subCount < 1`、ShardDB 失败为 `10003`；已取消或已过截止时间的上下文原样返回。本方法不调用 `NewNo`，`TX` 号仍由记账在锁账户之前生成，发号错误沿用记账映射。事务由 ConfirmFreeze 自己 `dbx.WithTx` 提交。先锁冻结单，再按账户 id 升序锁账户。不存在或 RELEASED 为 `40004`，不插入。CONFIRMED 且 `txn_no` 非空则幂等，不再比较 channel。FROZEN 但不是该商户结算账户为 `10006`，不返回 `40001`。本分片平台子账户不存在为 `40001`，不是 `40003`。商户腿用 `balance` 与 `frozen_amount` 同减的更新，不要求 ACTIVE，不返回 `40002`；平台非 ACTIVE 为 `40005`，回滚后冻结单保持 FROZEN。凭证已存在而冻结单仍是 FROZEN，或确认 CAS 影响 0 行，为 `10003` 并回滚，不当成幂等成功。REFUND 凭证借商户贷渠道应收；TRANSFER 冻结单仍是 TRANSFER，凭证是 TRANSFER_OUT，贷在途。`duplicated` 只在进程内。不新增错误码，不改 CHECK，不实现 ACC-07、ACC-14、ACC-11、ACC-10、MQ-04、OPT-03 |
| 2026-10-21 | v3.0 | ACC-07 口径澄清：TransferLocal 是进程内方法，自己用 `dbx.WithTx` 提交本分片事务。RPC handler 仍是 ACC-11，不改 IDL，不增加进程内 `duplicated`。响应是 `status`、`txn_no`、`fail_code`。校验先于 nil 判断，不合法返回已有的 `10001`。`transfer_no` 为 1–32 个码点，不强制 `TF` 前缀。双方必须同一分片且不是同一商户。事务前普通 SELECT 双方 `MERCHANT_SETTLE` / `CNY` / `sub_no = 0`，不存在优先于非 ACTIVE，不加行锁、不比较余额。事务 1 插入 LOCAL SUCCESS 后调用公开 `Post(TRANSFER)`，借付款商户、贷收款商户，可用余额扣掉 `frozen_amount`。`Duplicated` 为 true 仍回写已有 `txn_no` 并提交 SUCCESS，与 ACC-06 不同。回写 0 行是 `10003` 并回滚。只有 `40002` 在事务 1 回滚后另开事务写 `INSUFFICIENT_BALANCE` 墓碑，error 为 nil；之后余额充足仍返回 FAILED。1062 必须先回滚再在事务外读取。金额或双方不一致为 `10006`。SUCCESS 但 `txn_no` 为空是 `10003`。验收第 1 条的 CheckBalance 在 ACC-10 完成前用 SQL 断言代替。不新增错误码，不改 CHECK，不实现 ACC-14、ACC-11、ACC-10、ACC-08、TRF-02、MQ |
| 2026-10-21 | v3.0 | ACC-14 口径澄清：TransferInTry、TransferInConfirm、TransferInCancel 是进程内方法，各自用 `dbx.WithTx` 提交收款商户所在分片事务。RPC 仍是 ACC-11。校验先于 nil 判断，不合法为已有的 `10001`。`transfer_no` 为 1–32 个码点，不强制 `TF` 前缀。Try 先查收款商户结算账户，不存在 `40001`，非 ACTIVE `40005`，通过后才插入 TRIED，不动余额。1062 先回滚再读。CANCELLED 上的 Try 一律 `40004`，不再比较金额和收款商户。Confirm 先锁分支，再由公开 Post 按账户 id 升序锁账户。借本分片 TRANSIT 子账户、贷收款商户，凭证 `TRANSFER_IN`。在途允许透支，余额 0 也能确认。在途或商户行不存在为 `40001`，分支保持 TRIED。`Duplicated` 为 true 仍回写已有 `txn_no` 并提交 CONFIRMED，与 ACC-06 不同。回写 0 行、空凭证号、两个账户 id 相同为 `10003` 并回滚。Post 的 `40002` / `40005` 原样返回，不写墓碑，不自己加 `pay_transfer_inconsistent_total`。Cancel 不查账户；没有行时插入 CANCELLED 墓碑。TRIED 金额或收款商户不一致为 `10006`。已 CONFIRMED 再 Cancel 为 `40004`。验收五条的 CheckBalance 在 ACC-10 完成前用 SQL 断言代替。不新增错误码，不改 CHECK，不实现 ACC-11、ACC-10、ACC-08、TRF-03 |
| 2026-10-21 | v3.0 | ACC-10 口径澄清：CheckBalance 是进程内方法，不监听 RPC，不实现管理 HTTP。不用 `dbx.WithTx`。每个分片在固定连接上执行 `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` 和 `START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT`，查完 ROLLBACK。商户号不合法的 `10001` 先于 nil service 和已取消的 ctx。nil 依赖、ShardDB 失败、SQL 错误、未知方向和金额溢出是 `10003`，failures 为空。`40001` 只表示点名的商户结算账户不存在。`passed = false` 不是错误码。商户模式忽略 `expect_quiescent`。最后一条分录是 `max(id)`，不是 `created_at`。一次失败的完成调用把进程内无标签计数 `pay_ledger_check_failures_total` 加 1，`/metrics` 暴露仍是 OPS-01。不新增错误码，不改 IDL，不改表，不实现 RPC 和 HTTP |
| 2026-10-22 | v3.0 | ACC-11 口径澄清：handler 只做协议转换和错误码转换。trace、指标只留空实现，分别属于 OPS-02、OPS-03。健康检查、`/readyz` 和 SIGTERM 10 秒排空仍属 OPS-01。管理 HTTP 仍是 GW-07。payment 客户端放在 `pkg/accountclient`，不让 payment 依赖 `service/account`。`OpenAccount` 不重试。`Reverse` 在 ACC-08 前返回已有的 `10003`，不写行。业务错误走 TTHeader 的 `BizStatusError`；AccountService 不加 `biz_code`。服务端 2 秒是处理上限，不改 §5.4.1 的客户端超时。不新增错误码，不改 IDL，不改表 |
| 2026-10-22 | v3.0 | ACC-11 审查修正：未识别错误转成 `10003` 时，原因打一条 ERROR，不进对外 msg。登记过的业务错误不打这条日志。panic 仍只记固定句子，不记 panic 值。不新增错误码 | 
| 2026-10-23 | v3.0 | BASE-13 口径澄清：入参取原样字符串和原始 body，nil 与空 body 等价，本包不转大写、不解析也不检查时间戳与 nonce 格式；新增 `FormatTimestamp`（十进制 Unix 秒）。方法、RequestURI、时间戳、nonce 含换行时签名与验签都返回错误，避免拼接串歧义，body 不受限。只接受 44 字符、带填充、填充位为 0 的标准 base64 签名。空密钥两侧都拒绝。错误为本包普通 `error` 哨兵，不含密钥、签名和拼接串，调用方映射为已有的 `20001` / `50001`。签名算法、请求头、测试向量不变，不新增错误码，不改接口文档 |
| 2026-10-23 | v3.0 | BASE-13 审查修正：规则 8 的错误映射按签名、验签两侧区分。只有验签侧的 `ErrMalformedSignature`、`ErrSignatureMismatch`、`ErrInvalidField` 映射为 `20001` / `50001`；`ErrEmptyKey` 与签名侧的全部错误是本端配置或内部故障，平台服务按 `10003` 处理并打 ERROR 日志（不含密钥）。同步澄清 GW-01 第 3 条（签响应失败返回 500 / `10003`，不签名）、GW-03 第 3 条（商户密钥为空 500 / `10003`）、GW-06 第 1 条（渠道密钥为空 500 `FAIL`，渠道重试）、NTF-02 第 2 条（签名失败本次不发送，按发送失败重试）；接口文档 §7 同步一句，`docs/06-flows.md` CF-01、CF-02、CF-21 补出口。不新增错误码，不改签名算法 |
| 2026-10-23 | v3.0 | BASE-13 规则 4 使用方补上 payment（商户通知、渠道请求）和 recon（账单请求），与报告的报文清单一致；文字修正，不改算法。 |
| 2026-10-23 | v3.0 | BASE-14 口径澄清：入口为 `New`（32 字节）与 `NewFromBase64`（标准 base64，带填充）；版本字节取 1，与 BASE-03 种子密文相同；不使用 AAD（种子密文没有 AAD，加上会让已有密文失效）。失败返回固定文本哨兵 `ErrKeySize`、`ErrKeyEncoding`、`ErrMalformed`（短于 29 字节）、`ErrUnknownVersion`、`ErrDecrypt`，不含主密钥、明文、密文，平台服务按已有的 `10003` 处理。空明文可加密；明文最长 227 字节由 `VARBINARY(256)` 决定，本包不限长。种子改用 `pkg/secretbox`，删除 `internal/seed` 自带的加密实现，并在连库前校验主密钥。审查修正：`Box` 不保留传入的 key 切片，但内部 AES 轮密钥与主密钥等价，按密钥材料对待；版本字节不受 GCM tag 保护，靠 `Open` 显式拒绝非 1 版本，将来 v2 应把版本字节（及商户号）纳入 AAD。不改密文格式、表结构、接口和错误码 |
| 2026-10-23 | v3.0 | CH-01 口径澄清（接口文档 2.1.0）：签名正确但参数非法返回 HTTP 400（body 为空、不签名、不写 `mock_txn`），列出参数校验项；时间窗 ±300 秒（含边界）外按验签失败 401。幂等判断顺序：CLOSED 优先返回 `ORDER_CLOSED`（不论金额），其次金额一致返回 `OK`、不一致返回 `PARAM_CONFLICT`；`REJECTED` 不带 `channel_txn_no`，`status` 为已有行当前状态。`hash` 定为 CRC-32（IEEE），MAX 等于 MIN 时取 MIN，默认范围 200–1999ms；到期时刻 = `created_at + delay`，`created_at` 由渠道时钟写入；`finished_at` 截到毫秒，`bill_date` 取其东八区日期，FAILED 与 BILL_MISSING 不写。状态推进用 CAS（表无 version 列，不加迁移），写库失败 1 秒后重试，CAS 提交后调用完成钩子（CH-05 使用）。NEVER_FINISH 不调度。重启扫描按 id 分页、在接收请求前完成。§2.3 目录 `channel_mock/` 增加 `biz/`。PAY-03 规则 6 补一句：渠道其他非 200（含 400、401）也视为结果未知 |
| 2026-10-23 | v3.0 | CH-01 审查修正（接口文档 2.1.0 同步）：规则 1 的 400 补上 body 非法 UTF-8、顶层重复键、已知字段名大小写不对、`req_no` / `notify_url` 含 U+FFFD（含孤立代理转义）；时间窗按整秒比较。规则 2 补插入报非 1062 错误后的再读：读到本次写入的行就调度并返回 `OK`，否则 500。规则 4 补完成写库结果不确定时按 id 读回确认、一致才调用钩子，钩子上下文不随任务取消。不改资金流向、表结构与错误码 |
| 2026-10-26 | v3.0 | CH-03 口径确认（用户决定）：对未知 `req_no` 关单插入的防悬挂 CLOSED 记录 `amount` 固定为 0，`scenario`、`notify_url` 取列默认值；该记录不进入账单，查单原样返回 0。同步接口文档 2.1.1 §9.4、`docs/database.md` `mock_txn.amount`、`docs/06-flows.md` CF-23。不改表结构与状态机 |
| 2026-10-26 | v3.0 | CH-02 口径澄清：查单按 `(txn_type, req_no)` 只读当前行，不更新、不调度、不调用完成钩子、不发回调，也不按 DROP_NOTIFY、SLOW 等场景藏状态或推迟返回。无行时 `result` 为 `NOT_FOUND`，不插入，body 不带 status、amount、channel_txn_no、finished_at。找到时返回 status、amount（含 0）、channel_txn_no；finished_at 仅当列非 NULL，格式为约定 §4 的 RFC 3339 毫秒、东八区 `+08:00`，PROCESSING 因该列为空而省略。PAY 与 REFUND 不串单。平台错误码 `10002` 与渠道 `result = NOT_FOUND` 不是一回事。同步接口文档 2.1.2 §9.3。不改资金流向、表结构与错误码 |
| 2026-10-26 | v3.0 | CH-03 口径澄清：只关 `(PAY, req_no)`，同号 REFUND 不参与。PROCESSING 且不是 CLOSE_RACE 时 CAS 置 CLOSED，不写 `finished_at`、`bill_date`，不调钩子。CLOSE_RACE 时 CAS 置 SUCCESS，写 `finished_at` 与 `bill_date`（与自动完成相同），提交后调用一次完成钩子，返回 `ALREADY_PAID`。SUCCESS / CLOSED / FAILED 不改行。未知 `req_no` 插入 `amount = 0` 的 CLOSED 防悬挂行。1062 与结果不确定的写入再读后按当前状态回答。不新增错误码，不改表结构，不发 HTTP 回调 |
| 2026-10-26 | v3.0 | CH-05 口径澄清：只对 SUCCESS / FAILED，且场景为 NORMAL、FAIL、ACCEPT_TIMEOUT、BILL_MISSING、BILL_AMOUNT_DIFF、CLOSE_RACE 或未登记值，发普通回调。FAIL 的 body `status` 为 FAILED，不是把 CH-06 做完。DROP_NOTIFY 不回调。DUP_NOTIFY 与 SLOW 的特殊发送仍是 CH-06，现在不发、不加计数，也不套普通重试。空 `notify_url`、`finished_at` 为零、非 SUCCESS / FAILED 不发。不取消已有自动完成定时器，不改查单和关单的返回。送达仅当 HTTP 200 且 body 字节恰好是 `SUCCESS`。连第一次最多 5 次，间隔按已发出次数为 5s、15s、30s、60s。计数在单独短事务里条件加一，HTTP 在事务外。单次超时 5 秒，不跟随重定向，响应体最多 1024 字节。`notify_id` 前缀 `CN`，不落库。同一 id 的发送链只在进程内标记；进程退出后未发出的重试丢失，`Recover` 不扫描已终态的行，查单仍是补偿。不改六个状态的资金含义，不新增错误码，不加迁移 |
| 2026-10-27 | v3.0 | CH-06 文字澄清：总表依赖由 CH-01~05 改为 CH-01、CH-02、CH-03、CH-05，对齐已有的「M3 完成 01、02、03、08，其余在 M4」。CH-04 仍负责退款受理，不在本切片。§3 改为 DOING，不标 DONE。验收拆成已勾的 M3（FAIL、DROP_NOTIFY、DUP_NOTIFY、NEVER_FINISH）和未勾的 M4（SLOW、ACCEPT_TIMEOUT、BILL_MISSING、BILL_AMOUNT_DIFF、CLOSE_RACE）。DUP_NOTIFY 按 10 次发送：一个调度任务里并发 5 次，POST 都返回后按注入时钟再于 now+1s 至 now+5s 各发 1 次，不论平台响应；占名额用 `notify_sent_count < 10`。普通回调仍是 `notify_sent_count = ?` 且最多 5 次。SLOW 仍不发。本切片只断言 `bill_date`，不实现 CSV。CH-05 规则改为 DUP 由 CH-06 发送、SLOW 仍不发。不新增错误码，不加迁移 |
| 2026-10-27 | v3.0 | PAY-01 口径澄清：`app_id` 与 `merchant_id` 按字节精确匹配（列排序规则不区分大小写，服务端读回比较），为空、不存在、大小写不同都为 `20004`。解密失败 `10003`，ERROR 日志只有 `app_id`、`merchant_id`。payment 不缓存凭证；明文密钥用 `biz.Secret` 包装，只有 `Reveal()` 取明文，fmt、json、slog 嵌套输出都为 `***`。规则 3 的缓存只存 `merchant_id`、`status`、`notify_url`，费率不缓存，另按 PAY-10 口径读 `merchant_fee_rate` 的生效档；没有生效档返回“未找到”，由 PAY-02 决定处理。TTL 区间 `[装载, 装载 + TTL)`，时钟回拨视为过期，TTL 为 0 每次读库，不存在与读库失败不缓存。payment Kitex 服务端在 PAY-01 起：端口 8881，中间件同 ACC-11，服务端超时 2 秒，只实现 `GetMerchantCredential`，其余方法返回 `10003`。OPS-01、GW-03、GW-08 不在本功能点。不新增错误码，不改 IDL，不改表，同步接口文档 2.1.6 |
| 2026-10-27 | v3.0 | PAY-01 文字更正（接口文档 2.1.7）：上一行与 PAY-01 说明里“列排序规则不区分大小写”有误。`merchant` 表列跟随库默认 `utf8mb4_bin`（区分大小写，但 PAD SPACE 忽略尾部空格）；集成测试临时库为 `utf8mb4_0900_ai_ci`（不区分大小写与重音）。两种规则下 SQL 都可能匹配到字节不同的值，所以读回后逐字节比较。行为、错误码、IDL、表结构都不变 |
| 2026-10-27 | v3.0 | 文档审查文字修正，不改规则：CH-07 明细标头改为与 §3 一致的 `P0 · M6`；QA-02 明细标头的里程碑与依赖改为与 §3 一致的 `M6–M7`、`全链路、OPT-01、CH-07`；BASE-03 规则 1 不再引用已删除的设计草稿，改为指向 `migrations/` 与 `docs/database.md`；本表 PAY-10 一行的日期按其提交改为 2026-10-19 并按日期排序；§5.2 的 NFR-CON-07 移到 NFR-CON-06 之后；Q2 最晚决定改为 M6 基线压测后（基线压测在 M6）；`/metrics` 暴露统一归 OPS-03，ACC-10 规则 4 与 OPS-03 末段原写的“仍是 OPS-01”作废（上面 2026-10-21 ACC-10 一行同此） |
| 2026-10-27 | v3.0 | 文档审查第 2 阶段，按用户确认的决定修改规则（A1、A3、A4、A5、A6、A7、A9）。A1：MQ-02 改为“短事务领取并把 `next_retry_at` 推后为 30 秒租约 → 事务外发送 → 按租约值 CAS 回写”，发送期间不持有行锁，不改表结构；验收改为不注入故障时每个 `event_id` 恰好 1 次，并新增无行锁、过期租约接管、过期回写不覆盖三条；§2.8 与 QA-01 C4 同步。A3：§3 与明细标头的依赖按“验收用到的功能点都列为直接依赖、不依赖更晚里程碑、无环”重排，TL-03、QA-04 不再写“全链路”“全部 P0”（QA-04 改为列出不被其他 P0 依赖的 P0 功能点）；新增 BASE-15 故障注入点（M3，原 QA-01 规则 1、2，`pkg/fault`），QA-01 改名“故障演练”只做演练；GW-02 拆为 M3 的支付与余额路由和新增的 GW-09 退款与转账路由（M4）；QA-02 拆为 M6 基线压测（交付 `test/chaos/verify.sh`）和新增的 QA-05 优化后复测（M7），消除 QA-02 与 OPT-02 ~ OPT-04 的循环；OPT-01 改名“分阶段耗时埋点”，基线报告移到 QA-02。A4：无法判定的验收改为可判定并移到能验收的功能点：PAY-03 的 REJECTED、2 秒内成功、Pay 与 Close 并发、ACCEPT_TIMEOUT 分别移到 PAY-04、GW-06、PAY-06、PAY-05，PAY-03 补 OK 与超时两条；REF-02 的 FAILED 分支、回调成功、恢复后完成、解冻分别移到 REF-03、REF-04、MQ-04；TRF-02、TRF-03 的“由 TRF-04 推进”移到 TRF-04；PAY-06 的“商户最终入账”、REF-03 的账务结果移到 MQ-04；TL-01 的完整流程移到 GW-09，TL-01 改用假服务端验收；OPS-01 的压测中 `docker stop` 移到 QA-01，Kafka、Redis 就绪检查分别移到 MQ-03、GW-04；GW-06 的“Redis 停止时回调正常”移到 GW-04；GW-05 改为恰好 200 通过、100 个 429；OPS-03、TL-02、GW-07 验收写明具体计数；MQ-04 补 F = 0 只记两条分录；NTF-02 补 CAS 回写；REC-01 补 INIT、DOWNLOADED 超时接管和 `BILL_NOT_READY` 重试路径（细节见 §8.1 Q6）；REF-03 规则 2 写明释放条件 `refunded_amount >= R`；OPT-02 补非 ACTIVE 账户退化为逐条。A5：CH-06 保持 DOING，补 M4 的 SLOW、ACCEPT_TIMEOUT 规则与可判定验收；CLOSE_RACE 由 CH-03 覆盖不重做；BILL_MISSING、BILL_AMOUNT_DIFF 的账单表现移到 CH-08 验收。A6：§2.3 按已实现代码修正：`internal/testkit`、`pkg/accountclient`、`pkg/server` 入图，account biz 为平铺单包，payment biz 现为平铺文件；记录 payment、account 的 SQL 都在 `biz/` 的现状偏离（account 是否拆分见 §8.1 Q5）。A7：ACC-11 规则 3 改为按方法区分服务端超时，`CheckBalance` 全量模式 120 秒，其余 2 秒。A9：§0.3 DoD 增加报告类与人工验收的证据口径。已完成功能点的状态与已勾验收不变；需要改代码的后续工作待 fix/ACC-11-…、fix/PAY-01-dal、[chore] 落地：ACC-11 服务端超时按方法区分（fix/ACC-11-…）、payment SQL 拆到 `dal/`（fix/PAY-01-dal）、`make cover` 加 `integration` 构建标签（[chore]） |
| 2026-10-27 | v3.0 | 文档审查第 2 阶段遗留问题的用户决定：REF-03 新增第 5 条，释放 `refunded_amount` 的 UPDATE 影响 0 行时整个事务回滚、返回 `10003`、ERROR 日志并计 `pay_refund_quota_inconsistent_total`（已登记到 OPS-03），由回调重试、查单补偿或卡单恢复再次暴露；REC-01 第 5、6 条：INIT、DOWNLOADED 按 `updated_at` 超过 30 分钟接管（CHECKING 仍按 `started_at`），UPDATE 显式写注入时钟的 `updated_at`，只在现有 RunBatch 触发时机判定、不加后台扫描，`BILL_NOT_READY` 每 30 分钟对该批次重试一次、当天最多 6 次后置 FAILED（重试计数与待执行重试只在进程内），REC-02 第 4 条与 §2.8 同步，§8.1 Q6 关闭；BASE-15 新增第 5 条启动校验，`FAULT_POINTS` 默认空、逗号分隔，出现未登记名称（含拼写错误、空项、`:once` 以外的后缀）时启动失败，登记表顺延为第 6 条；MQ-04 中 `Post` 返回 `40005` 为不可重试，进 DLQ 并输出 ERROR 日志，可由 MQ-05 重放，OPT-02 退化路径一致，MQ-04、MQ-05、OPT-02 补验收；§8.1 Q5 关闭，account 的 SQL 保持在 `biz/`；§2.3 的 payment biz 按现状写成平铺单包，只保留 payment SQL 待 fix/PAY-01-dal 拆出的偏离；CH-06 第 6 条 SLOW 只推迟首次回调，失败后按 CH-05 重试，其余与 NORMAL 一致，补重试验收；§7 注明 A5 的 C1–C8、C12 与 A11 的 C9–C11、C13 合起来是 QA-01 的 C1–C13。`docs/07-module-plan.md` 同步 §4，并把消息行、性能优化行的周改为实际里程碑。不改表结构、错误码与资金流向 |
| 2026-10-27 | v3.0 | 文档审查最后一轮补充（用户决定）：REC-01 的 `BILL_NOT_READY` 重试改为持久化，`recon_batch` 新增列 `retry_count INT NOT NULL DEFAULT 0`（迁移随 REC-01 新增，标头“表”已注明）。每次 `BILL_NOT_READY` CAS 写 `retry_count + 1` 和注入时钟的 `updated_at`，满 30 分钟才重试，已重试 6 次后 CAS 置 FAILED（`error_msg = BILL_NOT_READY`），重跑时 `retry_count` 归 0；上一行“当天最多 6 次”“计数只在进程内、重启丢失”的表述作废。每日定时触发顺带对前 7 个账期（D−8 到 D−2）中 INIT、DOWNLOADED、CHECKING 的已有批次调用 RunBatch，按第 5 条超时接管，仍不加独立后台扫描。第 5 条的超时改为“满 30 分钟”（≥），并写明接管后的续跑点：INIT 从下载开始，DOWNLOADED 以 CAS `DOWNLOADED → CHECKING` 继续比对（账单行已在进入 DOWNLOADED 的同一事务导入），CHECKING 重新比对。REC-02 第 4 条、§2.8、§8.1 Q6 同步；补重启计数不丢、旧账期由定时触发接管、重跑归 0 的验收。REC-06：`recon_diff` 置 RESOLVED、IGNORED 用 CAS `WHERE id = ? AND status = 'PENDING'`，影响 0 行时重新读取按当前状态返回，补并发验收。OPT-02 新增第 7 条：F = 0 时同样省略手续费分录，与逐条模式结果一致，补验收。表结构文档由另一 coder 同步；不改错误码与资金流向 |
| 2026-10-27 | v3.0 | 文档审查补充（用户选择方案 A）：重新比对前只删除本批次 `status = 'PENDING' AND handle_action IS NULL` 的差错，已自动处理的 PENDING 差错和 RESOLVED、IGNORED 的保留；REC-01 第 3 条重跑与第 5 条 CHECKING 接管共用这一条件（第 3 条原来的“删除状态为 PENDING 的差错”改为此条件），并写明 CHECKING 推进为 DONE、FAILED 的 CAS 带本次 `started_at`，被接管后旧运行的 CAS 影响 0 行。REC-03 第 4 条与 REC-05 第 6 条：本批次已有同一 `uk_batch_type_biz_no` 键时跳过、不改已有行（`ON DUPLICATE KEY UPDATE id = id`），`diff_count` 按实际行数统计，插入影响 1 行才计 `pay_recon_diffs_total`。REC-06：自动处理失败也写 `handle_action` 并保持 PENDING，后续任意账期的批次运行跨批次重试 `REQUERY`，LEDGER_MISSING 复核在凭证仍缺失时再次重放；CAS 影响 0 行且重新读取查不到时按“已不存在”返回并输出 WARN 日志。补重跑无 1062、自动处理差错保留、中断接管与一次性运行一致、旧运行不覆盖、同键跳过、处理失败重试、读不到行的验收。不改表结构、错误码与资金流向 |
| 2026-10-27 | v3.0 | 文档审查补充（用户决定）：REC-06 跨批次的 REQUERY 重试与 LEDGER_MISSING 复核（含再次重放）只处理 `bill_date` 在 D−8 到 D−2 的差错（D 为运行时注入时钟的东八区日期，与 REC-01 第 1 条的前 7 个账期一致）；窗口外仍 PENDING 的已自动处理差错保持 PENDING 等人工，REC-08 报告新增“等待人工处理”列表。`recon_diff` 唯一键由 `uk_batch_type_biz_no` 改为 `uk_batch_type_biz_no_diff_type`（`batch_no, txn_type, biz_no, diff_type`），迁移随 REC-03 新增，REC-03 标头注明，同键跳过按新键，同一笔交易的外部差错与 `LEDGER_MISSING` 可在同一批次并存。REC-06 新增严重不一致：`CHANNEL_MISSING` 查单确认渠道不是 SUCCESS 时保持 PENDING，`handle_action = REQUERY`，`channel_status` 写渠道状态并作为不再自动重试的标记，`handle_note` 记录渠道状态，ERROR 日志并计 `pay_recon_channel_inconsistent_total{channel}`（已登记 OPS-03），等人工。补窗口、严重不一致、等待人工列表、外部与内部差错并存的验收。错误码与资金流向不变 |
| 2026-10-27 | v3.0 | 文档审查补充（用户决定）：REC-01 第 2 条按状态拆写，`force` 只对 DONE 生效（FAILED 本来就重跑）；未超时的 INIT、DOWNLOADED、CHECKING 带 `force = true` 仍返回当前批次，不并发重跑；超时的带不带 force 都按第 5 条接管，不按第 3 条重跑；补验收。REC-08 的“等待人工处理”定名为响应字段 `manual_total`、`manual_diffs`，`manual_diffs` 按 `bill_date` 升序、同一 `bill_date` 内按 `id` 升序取前 100 条，补排序与截断验收。`ReconBatch` 不输出 `retry_count`。表结构、错误码与资金流向不变 |
| 2026-10-27 | v3.0 | BASE-15：实现了 `pkg/fault`；重复名称视为初始化错误；标记写失败不发信号；空配置不碰标记目录。不改 9 个名称，不改资金，不新增错误码 |
| 2026-10-28 | v3.0 | PAY-02 文字澄清：未传 `notify_url` 存空串，通知时才取商户默认地址；显式地址即使等于商户默认地址也原样入库。商户不存在或非 `ACTIVE` 为已有的 `20004`，不插入。没有生效费率档为已有的 `10003`，不把手续费当成 0，不读 `merchant.fee_rate_bp`，不缓存费率。时钟回拨映射为已有的 `10004`。1062 后按商户订单号读不到行返回 `10003`。不新增错误码，不改表，不改 IDL |
| 2026-10-28 | v3.0 | PAY-03：`Pay` 服务端处理超时 5 秒，其他方法仍 2 秒。渠道同步拒绝的 `source` 名为 `SYNC`。PAY-04 之前，生产用的 `ApplyPayResult` 不改订单并返回已有的 `10003`。不新增错误码，不新增环境变量，不改表 |
| 2026-10-28 | v3.0 | MQ-01 文字澄清：`occurred_at` 取 `Add` 时的注入时钟（提交前读取，不能改成提交后再读），无有效 span 时 `trace_context` 为 `{}`，不新增错误码。五种事件的 topic、msg_key、biz_no 与 data 字段写入规则。不改资金规则 |
| 2026-11-01 | v3.0 | PAY-04：`ApplyPayResult` 用结构体入参，一次事务按渠道流水 CAS、订单 CAS、仅 SUCCESS 调用已有 `outbox.Add` 的顺序推进。不新增错误码。`callback.after_commit` 仍留给 GW-06，本次不调用 `fault.Point`。文字澄清：金额比较先于同一终态和不同终态，已终态但金额不符仍是 `50002`；CLOSED 传了渠道完成时间则 `closed_at` 用该时间，没传则用本次注入时钟截到毫秒，`channel_finished_at` 保持空；SUCCESS 未传完成时间、未知 `source` 在写库前返回 `10001`。不改资金流向 |
| 2026-11-01 | v3.0 | PAY-06：没有新错误码。PAYING 必须先等渠道确认关闭，不能在渠道返回之前把订单改成 CLOSED。本地还没有渠道流水号时先查单，查无此单返回 10004，不插入渠道占位 |
| 2026-11-01 | v3.0 | PAY-08：直接读 `payment_order`，不缓存，不写。他人订单与不存在都是已有的 `30001`。`pay_no` 与 `out_trade_no` 二选一不合法是已有的 `10001`。不新增错误码 |
| 2026-11-01 | v3.0 | OPS-01：`pkg/server` 在管理端口提供 `/healthz` 与 `/readyz`。数据库必检，检查可注册；Redis、Kafka 只留注册口，本次不连接，也不因此失败。SIGTERM 后 `/readyz` 立即 503，停止新请求，在途等待上限默认 10 秒且可注入，再刷新已注册生产者（本次为空）并关闭连接。account 沿用两个分片各自 1 秒的 Ping。端口固定为 gateway 6060、payment 6061、account 6062、recon 6063、channel-mock 6064；本次进程内监听的是 account、payment、channel-mock。compose 六个应用服务 `stop_grace_period: 15s`，不把 profile `apps` 并入 `make up`，A1 仍不关闭。不新增错误码，不改表，不改商户接口 |
| 2026-11-01 | v3.0 | OPS-05：`net/http/pprof` 只注册在管理端口的私有 mux 上，不交给业务端口。channel-mock 业务端口访问 `/debug/pprof/` 为 404；payment、account 的业务端口是 Kitex，不另挂 HTTP。管理端口可采集 30 秒 CPU profile。`WriteTimeout` 改为 45 秒，`ReadHeaderTimeout` 仍是 1 秒。进程仍听 `:606x`，宿主机发布仍是 `127.0.0.1`。`/metrics` 仍不提供。不新增错误码，不改表，不改商户接口 |
| 2026-11-01 | v3.0 | TL-01：paycli 用环境变量或参数调用商户接口和管理接口。商户请求经 `pkg/sign` 签名，响应按 §3.3 验签；管理响应不签名。验收用 httptest 假服务端，不连真网关。经网关的完整流程仍是 GW-09。新增可选配置 `PAYCLI_GATEWAY_URL`、`PAYCLI_APP_ID`、`PAYCLI_APP_SECRET`、`PAYCLI_ADMIN_TOKEN`，加载时允许为空。BASE-09 中“paycli 结构体为空”的说法由本功能点取代。不新增错误码，不改表，不改服务端契约 |
| 2026-11-01 | v3.0 | GW-01：两个 Hertz 实例听 `:8080`（商户 API 与渠道回调）和 `:8090`（管理接口），不写成 `127.0.0.1`，这次不听 `:6060`，不改 compose。请求体超过 65536 字节返回 HTTP 413 / `10001`，不签名；恰好 65536 字节放行。handler panic 返回 500 / `10003`，进程不退出。`request_id` 由服务端生成 32 位小写十六进制，忽略客户端 `X-Request-Id`。未知路径返回 404 / `10002`，不签名。时间窗口、验签、限流、nonce 只占位。生产 Register 没有 `/v1/balance`；paycli 验签只走测试里临时加上的路由。Kitex 客户端连接 100ms，默认调用 1 秒；`Pay`、`ClosePayment`、`CreateRefund` 5 秒，`CreateTransfer` 3 秒，`RunBatch`、`CheckBalance` 120 秒，由 `RPCTimeout` 在调用时使用，客户端默认仍是 1 秒，GW-01 没有 HTTP 路由调用它们。地址写死 `payment:8881`、`account:8882`、`recon:8883`，不重试。不新增错误码，不改 IDL，不改表。 |
| 2026-11-01 | v3.0 | GW-03：商户端口 `/v1` 缺头或验签失败返回已有的 20001，不到路由；验签通过后路径仍不存在则 404 / 10002，且该 404 带平台签名；管理端口与非 `/v1` 仍不验签、404 不签名；每次 `GetMerchantCredential`，不缓存，调用超时 1 秒，不重试；DISABLED 与非 ACTIVE 为 20004；空密钥与空 merchant_id 为 10003；响应签名用本次查到的 app_secret，没有查凭证的已标记请求仍用进程内 WithSignSecret。不新增错误码，不改表，不改 IDL。 |
| 2026-11-01 | v3.0 | GW-02：商户端口提供创建、发起、关闭、查询支付单和查询余额。merchant_id 只取验签结果。金额在网关按 JSON 整数校验。余额只返回五个字段，available_amount 原样透传。不新增错误码，不改表，不改 IDL。 |
| 2026-11-01 | v3.0 | GW-06：商户端口接受 POST /callback/mockpay。响应正文只有 SUCCESS 或 FAIL。空密钥是 500。签名或时间窗失败是 401，不调用 payment。30001 与 50002 回 200 SUCCESS，并把进程内 pay_callback_rejected_total 加一。PAY 转发到 ApplyPayResult，成功返回后触发 callback.after_commit。REFUND 仍是 10003。两秒自动完成和 DUP_NOTIFY 端到端还没测。不新增错误码，不改表，不改 IDL。 |
| 2026-11-02 | v3.0 | GW-06：未知 req_no 与金额不一致在 MySQL 上保持原订单，HTTP 200 正文 SUCCESS，进程内 pay_callback_rejected_total 按 30001 或 50002 加一。Pay 返回 PAYING 后，测试把自动完成延迟固定为 50 毫秒，经 channel_mock 回调在 2 秒内订单变为 SUCCESS，channel_txn 与订单同为 SUCCESS，outbox 恰好 1 条 payment.succeeded。DUP_NOTIFY 十次回调后 notify_count 为 10，订单只推进一次，outbox 仍是 1 条。不新增错误码，不改表，不改 IDL。 |
| 2026-11-02 | v3.0 | CH-06 M4：SLOW 的首次回调排在 finished_at 之后 30 秒，失败后仍按 CH-05 的 5s/15s/30s/60s 重试，连首次最多 5 次；ACCEPT_TIMEOUT 只在新插入成功的受理上用调度器等到 created_at 之后 5 秒再返回 OK。受理 HTTP 上下文上限改为 8 秒，查单和关单仍是 3 秒。BILL 的账单表现仍在 CH-08，CLOSE_RACE 仍不重做。不新增错误码，不改表，不改 IDL。§3 改为 DONE。PAY-06 说明改为：CH-06 已 DONE，但关单验收仍不使用 SLOW 和 ACCEPT_TIMEOUT，依赖列仍不写 CH-06。 |
| 2026-11-03 | v3.0 | CH-04：先按 (REFUND, req_no) 返回已有结果；新单在同一事务里 FOR UPDATE 锁原支付，再合计 status 不是 FAILED 的退款。场景只有 01 FAIL 和 02 DROP_NOTIFY，其余 NORMAL。当日账单只断言 bill_date，CSV 仍是 CH-08。不新增错误码，不改表，不改 IDL。§3 改为 DONE。 |
| 2026-11-03 | v3.0 | REF-01：本功能点事务提交后返回刚插入的 INIT 退款单；同一请求内冻结和提交渠道是 REF-02，本次不调用 account、不调用渠道、不写 outbox。条件更新影响 0 行或插入 1062 时，事务外再读 (merchant_id, out_refund_no)，一致则返回已有单，避免并发重复请求被报成 30005。不新增错误码，不改表，不改 IDL。§3 改为 DONE。 |
| 2026-11-04 | v3.0 | REF-02：新插入的退款在额度事务提交后冻结并提交渠道。冻结超时或不可用返回 INIT，code 0，不写 channel_txn。account 把处理超时包成 TransError（消息仍是 rpc timeout）时同样返回 INIT。连接拒绝仍是 10003。40001 与 40005 保持 INIT 并返回 10003。40002、40004 和渠道 REJECTED 调用 ApplyRefundResult；该函数的落库属于 REF-03，本切片生产实现返回 10003，不把退款改成 FAILED，不减少 refunded_amount。已有 (merchant_id, out_refund_no) 直接返回已有单，不再次冻结。CreateRefund 的服务端处理超时改为 5 秒，ClosePayment 和其他方法仍是 2 秒。不新增错误码，不改表，不改 IDL。§3 改为 DONE。 |
| 2026-11-05 | v3.0 | REF-03：ApplyRefundResult 落库。分支按结果意图选：FAILED 且 fail_code 为 INSUFFICIENT_BALANCE、FREEZE_CONFLICT、REFUND_TIMEOUT 走第 2 条（只 CAS INIT → FAILED，不碰 channel_txn），其余走第 1 条，REF-04 判超时与推进为 PROCESSING 并发时返回 10003，不把已提交渠道的退款判失败。第 1 条 FAILED 的 fail_code 用传入值，未传为 CHANNEL_FAILED。渠道 CAS 另加 txn_type = REFUND，读到 PAY 流水按 30001 处理。第 2 条 CAS 未命中时已 FAILED 视为重复，已 SUCCESS 是并发竞争、WARN 日志、不计冲突并返回该单，仍 INIT 或 PROCESSING 为 10003。来源不接受 CLOSE。第 5 条的 ERROR 日志与计数在事务回滚后各记一次；日志里的 refund_no、pay_no 被兜底扫描部分遮盖，靠指标加 SQL 定位。GW-06 handler 的 REFUND 分支接到 ApplyRefundNotify，提交后调用 callback.after_commit；未知 txn_type 由 10003 改为 10001，网关表现不变。REF-02 删去“本切片生产实现返回 10003”。不新增错误码，不改表，不改 IDL。§3 改为 DONE。 |
| 2026-11-05 | v3.0 | REF-03 文字澄清：实现说明里“事务重试不会重复记”改为写明真实原因，ERROR 日志在回滚之后输出，记录时回滚已完成；释放额度影响 0 行返回的错误不是 MySQL 错误号，`dbx.WithTx` 不重试。不改规则，不改代码逻辑，不新增错误码，不改表，不改 IDL。 |
