# pay-system 需求规格说明书

> 版本：v3.0　最后修改：2026-10-20
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

- [ ] “验收”中的每一条都有对应的自动化测试，且全部通过。
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
  internal/       bootstrap/ dbmigrate/ dbschema/ seed/
  gateway/        handler/ middleware/
  service/
    account/      handler/ biz/ dal/ consumer/
    payment/      handler/ biz/ dal/ channel/ job/ notify/
    channel_mock/ handler/ scenario/ bill/ dal/
  job/recon/      biz/ dal/ bill/ handler/
  pkg/            money idgen errno clock dbx config logx sign secretbox outbox mq fault obs shard
  migrations/     account/（两个分片共用） payment/ recon/ channel_mock/
  deploy/         docker-compose.yml docker-compose.chaos.yml Dockerfile .env.example seed/ prometheus/
  test/           integration/ e2e/ chaos/ load/
  scripts/  docs/
```

分层约束：

- `handler` 只做协议转换和参数校验；`biz` 承载业务规则、状态机和事务编排；`dal` 只写 SQL。
- `biz` 依赖 `dal` 的接口，不依赖具体实现，便于单测替换。
- 服务之间只能通过 `kitex_gen` 和事件结构交互，禁止 import 其他服务的 `service/<x>/...` 包（golangci-lint `depguard` 检查）。
- `make build` 只构建树中第一行列出的 7 个业务程序。`cmd/bootstrap`、`cmd/seed` 是运维/初始化工具，不进入这 7 个二进制，只经 `deploy/Dockerfile` 构建。`internal/` 只给 bootstrap、seed、迁移和测试使用，不被 `service/`、`gateway/`、`job/` 引用；`pkg/` 同样不得引用。`depguard` 拦截这些 import。
- 事务中禁止调用 RPC、HTTP、MQ。

核心服务的包内划分（2026-10-13 确认，来源 `docs/07-module-plan.md` §4）：

```
service/account/
  handler/                Kitex 实现，错误码转换
  biz/
    account/              开户、查询（平台账户跨分片汇总）
    ledger/               Post：校验、加锁顺序、balance_after（进程内方法，不暴露 RPC）
    freeze/               Freeze / ReleaseFreeze / ConfirmFreeze，空回滚与防悬挂
    transfer/             TransferLocal（失败墓碑）、TransferIn Try / Confirm / Cancel
    check/                CheckBalance：分片快照、在途守恒
  dal/                    ShardDB(shard)，每个分片一个连接池
  consumer/               ledger_single.go（MQ-04 基线）、ledger_batch.go（OPT-02）

service/payment/
  handler/
  biz/
    merchant/             商户配置与凭证（带缓存开关）
    order/                支付单状态机、ApplyPayResult
    refund/               退款单状态机、ApplyRefundResult
    transfer/             转账单、LOCAL 执行、TCC 协调器
  dal/
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
| outbox 投递 | payment | 200ms | `FOR UPDATE SKIP LOCKED` | MQ-02 |
| 查单补偿 | payment | 1s | `SKIP LOCKED` 抢占 + 推后 `next_query_at` | PAY-05 |
| 超时关单 | payment | 5s | 状态 CAS | PAY-07 |
| 退款卡单恢复 | payment | 10s | 状态 CAS + 幂等 Freeze | REF-04 |
| 转账恢复 | payment | 1s | `SKIP LOCKED` 抢占 + 推后 `next_retry_at`；分支操作幂等 | TRF-04 |
| 商户通知发送 | payment | 1s | `SKIP LOCKED` 抢占 | NTF-02 |
| 订单手续费核对 | payment | 1 分钟 | 只读；多实例可同时跑 | PAY-10 |
| outbox 清理 | payment | 每天 03:00 | 按批删除，天然幂等 | MQ-06 |
| 余额核对 | account | 手动全量（管理接口、测试、`verify.sh`） | 只读 | ACC-10 |
| 对账 | recon | 每天 10:00 | `uk_channel_bill_date` + 状态 CAS | REC-01 |
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
| BASE-13 | 签名库 | P0 | M3 | BASE-01 | TODO |
| BASE-14 | 密钥加密 | P0 | M3 | BASE-01 | TODO |
| ACC-13 | 账户分片路由 | P0 | M1 | BASE-08、BASE-09 | DONE |
| ACC-01 | 开户 | P0 | M1 | ACC-13、BASE-05 | DONE |
| ACC-02 | 账户查询 | P0 | M1 | ACC-01 | DONE |
| ACC-03 | 通用记账 | P0 | M1 | ACC-01、BASE-04 | DONE |
| ACC-04 | 冻结 | P0 | M2 | ACC-03 | DONE |
| ACC-05 | 解冻（含空回滚、防悬挂） | P0 | M2 | ACC-04 | DONE |
| ACC-06 | 确认扣款 | P0 | M2 | ACC-04 | DONE |
| ACC-07 | 同分片转账（含失败墓碑） | P0 | M2 | ACC-03 | TODO |
| ACC-14 | 收款方转账分支（Try / Confirm / Cancel） | P0 | M2 | ACC-03 | TODO |
| ACC-08 | 冲正 | P1 | M8 | ACC-03 | TODO |
| ACC-09 | 流水查询 | P1 | — | — | CUT |
| ACC-10 | 余额核对（含分片与在途守恒） | P0 | M2 | ACC-03、ACC-04、ACC-14 | TODO |
| ACC-11 | account RPC 服务 | P0 | M2 | ACC-01~07、ACC-14、BASE-12 | TODO |
| ACC-12 | 热点账户优化 | P2 | — | — | CUT（拆为 OPT-03、OPT-05） |
| CH-01 | 渠道支付受理与自动完成 | P0 | M3 | BASE-13、BASE-03 | TODO |
| CH-02 | 渠道查单 | P0 | M3 | CH-01 | TODO |
| CH-03 | 渠道关单 | P0 | M3 | CH-01 | TODO |
| CH-04 | 渠道退款受理与查询 | P0 | M4 | CH-01 | TODO |
| CH-05 | 渠道异步回调 | P0 | M3 | CH-01 | TODO |
| CH-06 | 确定性故障场景 | P0 | M3–M4 | CH-01~05 | TODO |
| CH-07 | 全局故障配置（压测延迟） | P0 | M6 | CH-06 | TODO |
| CH-08 | 账单下载 | P0 | M6 | CH-01、CH-04 | TODO |
| CH-09 | 账单注入 | P0 | M6 | CH-08 | TODO |
| PAY-01 | 商户与密钥 | P0 | M3 | BASE-14、BASE-03 | TODO |
| PAY-02 | 创建支付单 | P0 | M3 | PAY-01 | TODO |
| PAY-03 | 发起支付 | P0 | M3 | PAY-02、CH-01 | TODO |
| PAY-04 | 支付结果推进 | P0 | M3 | PAY-03、MQ-01 | TODO |
| PAY-05 | 查单补偿 | P0 | M4 | PAY-04、CH-02 | TODO |
| PAY-06 | 商户关单 | P0 | M3 | PAY-04、CH-03 | TODO |
| PAY-07 | 超时关单 | P0 | M4 | PAY-06 | TODO |
| PAY-08 | 支付单查询 | P0 | M3 | PAY-02 | TODO |
| PAY-09 | 立即同步渠道状态 | P0 | M6 | PAY-05 | TODO |
| PAY-10 | 商户费率变更与订单手续费核对 | P0 | M3 | BASE-03、BASE-04、BASE-08 | DONE |
| REF-01 | 创建退款与额度占用 | P0 | M4 | PAY-04 | TODO |
| REF-02 | 退款冻结与渠道提交 | P0 | M4 | REF-01、ACC-11、CH-04 | TODO |
| REF-03 | 退款结果推进 | P0 | M4 | REF-02 | TODO |
| REF-04 | 退款卡单恢复 | P0 | M4 | REF-02 | TODO |
| REF-05 | 退款查询 | P0 | M4 | REF-01 | TODO |
| TRF-01 | 创建转账单 | P0 | M4 | PAY-01、ACC-13 | TODO |
| TRF-02 | 同分片转账执行 | P0 | M4 | TRF-01、ACC-07、ACC-11、MQ-01 | TODO |
| TRF-03 | 跨分片转账 TCC 协调 | P0 | M4 | TRF-01、ACC-04~06、ACC-14、ACC-11、MQ-01 | TODO |
| TRF-04 | 转账恢复任务 | P0 | M4 | TRF-02、TRF-03 | TODO |
| TRF-05 | 转账查询 | P0 | M4 | TRF-01 | TODO |
| MQ-01 | outbox 写入 | P0 | M3 | BASE-08、BASE-05 | TODO |
| MQ-02 | outbox 投递器 | P0 | M5 | MQ-01、MQ-03 | TODO |
| MQ-03 | Kafka 生产与消费框架 | P0 | M5 | BASE-02 | TODO |
| MQ-04 | account 事件消费（逐条模式） | P0 | M5 | MQ-03、ACC-03、ACC-05、ACC-06 | TODO |
| MQ-05 | 死信与事件重放 | P0 | M5 | MQ-02、MQ-04 | TODO |
| MQ-06 | outbox 清理 | P1 | M8 | MQ-02 | TODO |
| NTF-01 | 通知记录生成 | P0 | M5 | MQ-03 | TODO |
| NTF-02 | 通知发送与固定间隔重试 | P0 | M5 | NTF-01、BASE-13 | TODO |
| GW-01 | 网关骨架与统一响应 | P0 | M3 | BASE-12、BASE-13 | TODO |
| GW-02 | 商户 API 路由 | P0 | M3–M4 | GW-01、PAY-02/03/06/08、REF-01/05、TRF-01/05 | TODO |
| GW-03 | 商户验签 | P0 | M3 | GW-01、PAY-01 | TODO |
| GW-04 | 时间窗口与 nonce 防重放 | P0 | M5 | GW-03 | TODO |
| GW-05 | 商户限流（进程内） | P0 | M5 | GW-03 | TODO |
| GW-06 | 渠道回调入口 | P0 | M3 | GW-01、PAY-04 | TODO |
| GW-07 | 管理接口（重放、核对、触发对账） | P0 | M6 | GW-01 | TODO |
| GW-08 | 商户密钥缓存 | P0 | M7 | GW-03、OPT-01 | TODO |
| REC-01 | 对账批次与调度 | P0 | M6 | BASE-03 | TODO |
| REC-02 | 账单下载与导入 | P0 | M6 | REC-01、CH-08 | TODO |
| REC-03 | 外部对账比对（按单号） | P0 | M6 | REC-02 | TODO |
| REC-04 | 跨日处理 | P0 | — | — | CUT |
| REC-05 | 内部对账（支付、退款、转账） | P0 | M6 | REC-01 | TODO |
| REC-06 | 差错自动处理 | P0 | M6 | REC-03、REC-05、PAY-09、MQ-05 | TODO |
| REC-07 | 差错人工处理与挂账 | P0 | — | — | CUT |
| REC-08 | 对账报告 | P0 | M6 | REC-03、REC-05 | TODO |
| OPS-01 | 健康检查与优雅退出 | P0 | M3 | BASE-09 | TODO |
| OPS-02 | 链路追踪 | P0 | M5 | GW-01、ACC-11、MQ-03 | TODO |
| OPS-03 | 业务与运行指标 | P0 | M5 | OPS-01 | TODO |
| OPS-04 | 监控面板与告警规则 | P1 | — | — | CUT |
| OPS-05 | pprof 与管理端口 | P0 | M3 | OPS-01 | TODO |
| OPT-01 | 分阶段耗时埋点与基线 | P0 | M5 | OPS-03 | TODO |
| OPT-02 | 批量记账合并提交 | P0 | M7 | MQ-04、QA-02 | TODO |
| OPT-03 | 热点平台账户子账户拆分 | P0 | M7 | ACC-03、QA-02 | TODO |
| OPT-04 | 链路延迟优化 | P0 | M7 | OPT-01、QA-02 | TODO |
| OPT-05 | 平台账户缓冲入账 | P2 | M8 | OPT-03 | TODO |
| TL-01 | paycli | P0 | M3 | BASE-13 | TODO |
| TL-02 | merchant-demo | P0 | M5 | BASE-13 | TODO |
| TL-03 | 一键演示脚本 | P1 | M8 | 全链路 | TODO |
| TL-04 | OpenAPI 文档 | P1 | M8 | GW-02 | TODO |
| QA-01 | 故障注入与演练 | P0 | M7 | 全链路 | TODO |
| QA-02 | 压测与报告（基线 + 优化对比） | P0 | M6–M7 | 全链路、OPT-01、CH-07 | TODO |
| QA-03 | pprof 调优记录 | P0 | M7 | QA-02 | TODO |
| QA-04 | 验收运行 | P0 | M8 | 全部 P0 | TODO |
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

1. 将 `docs/schema/schema_draft.sql` 按 schema 拆分为 `migrations/<schema>/000001_init.up.sql` 和 `.down.sql`，之后删除草稿，`docs/database.md` 的来源改为迁移文件。账户分片共用 `migrations/account/`，迁移容器对 `pay_account_0`、`pay_account_1` 各执行一次。
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

- 密钥：BASE-14 属于 M3，不是本功能点依赖，本功能点不实现 `pkg/secretbox`。种子按 BASE-14 已写明的算法与密文格式（AES-256-GCM，`version(1B) + nonce(12B) + ciphertext + tag`，version 取值 1，主密钥 `SECRET_MASTER_KEY`）用标准库加密后写入。明文格式为 `DEMO_APP_SECRET_FORMAT`（默认 `demo_secret_%s_0123456789ab`，`%s` 为商户号），因此 `M100001` 的明文与 `docs/05-api.md` §3.7 测试向量一致。重复执行不覆盖已有密文（nonce 随机，覆盖会让密文变化）。BASE-14 实现时必须能解密这些种子密文，不得另起格式。
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
- seed、bootstrap 只提供配置结构体，不改现有加密、写库和建号流程。paycli 没有专属变量，结构体为空，不要求 `WORKER_ID`。不新建 `cmd/migrate`。

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
4. 提供签名与验签两侧函数，供网关、paycli、merchant-demo、channel_mock 共用。

验收：

- [ ] `docs/05-api.md` §3.7 的全部测试向量通过。
- [ ] body 改动 1 字节、查询串参数顺序改变、时间戳改变，验签均失败。

#### BASE-14 密钥加密（pkg/secretbox）

`P0 · M3 · 依赖：BASE-01`

规则：

1. AES-256-GCM，主密钥为 `SECRET_MASTER_KEY`（32 字节，base64 编码）；密文格式 `version(1B) + nonce(12B) + ciphertext + tag`。
2. 每次加密使用随机 nonce。
3. 解密失败返回错误，不输出密文或明文。

验收：

- [ ] 加密后解密得到原文；同一明文两次加密结果不同。
- [ ] 篡改任一字节，解密失败。
- [ ] 主密钥长度不是 32 字节时初始化失败。

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

接口：`AccountService.TransferLocal(transfer_no, payer_merchant_id, payee_merchant_id, amount)` → `{status, txn_no, fail_code}`

说明：同分片转账由 account 在一个本地事务内完成。难点是结果必须确定：调用方超时重试时，第一次请求可能仍在执行，不能出现“第一次因余额不足失败、重试却成功”或反过来的情况。`transfer_branch(role = LOCAL)` 记录每笔转账的最终结果，重复请求只返回已有结果。

规则：

1. 双方必须在同一分片（`shard.Of` 相同）、都是 ACTIVE 的商户账户，且不是同一商户，否则 `10001` / `40001` / `40005`。
2. 事务 1：

   ```
   BEGIN
     INSERT transfer_branch(transfer_no, role = 'LOCAL', status = 'SUCCESS', ...)
       -- 1062：ROLLBACK，读取已有分支并按第 3 条返回
     Post(TRANSFER, transfer_no, 借付款商户 T，贷收款商户 T)   -- 账户按 id 升序加锁
     UPDATE transfer_branch SET txn_no = ? WHERE id = ?
   COMMIT
   ```

3. 已有分支：SUCCESS → 返回 SUCCESS 与 `txn_no`；FAILED → 返回 FAILED 与 `fail_code`；金额或双方不一致 → `10006`。
4. 事务 1 中 Post 返回 `40002`：ROLLBACK 后执行事务 2 写失败墓碑 `INSERT transfer_branch(LOCAL, FAILED, fail_code = INSUFFICIENT_BALANCE)`；遇 1062 说明并发请求已写入结果，读取后按第 3 条返回。之后同一 `transfer_no` 的请求一律返回 FAILED，即使余额已经充足。
5. 余额不足返回业务结果 `status = FAILED`，不返回 `40002` 错误码，便于调用方区分“确定失败”和“调用异常”。

异常：参数不合法 → `10001`；账户不存在 → `40001`；非 ACTIVE → `40005`；幂等冲突 → `10006`。

验收：

- [ ] A、B 各 10000，A→B 与 B→A 各 500 次并发转 1，无死锁报错（死锁重试后成功也可），结束后 A + B = 20000 且 `CheckBalance` 通过（A2）。
- [ ] 同一 `transfer_no` 并发 50 次，只生成 1 张凭证，全部返回同一结果。
- [ ] 余额不足失败后，给付款方入账使余额充足，再用同一 `transfer_no` 请求仍返回 FAILED，没有凭证。
- [ ] 双方不在同一分片、转给自己，返回 `10001`。

#### ACC-14 收款方转账分支（Try / Confirm / Cancel）

`P0 · M2 · 依赖：ACC-03 · 表：transfer_branch、ledger_txn、ledger_entry、account`

接口（均以收款商户所属分片执行）：

- `AccountService.TransferInTry(transfer_no, payee_merchant_id, amount)` → `{status}`
- `AccountService.TransferInConfirm(transfer_no, payee_merchant_id)` → `{status, txn_no}`
- `AccountService.TransferInCancel(transfer_no, payee_merchant_id, amount)` → `{status}`

说明：收款方 Try 不动余额，只占位并确认收款账户可用，避免 Confirm 阶段才发现收款账户异常、资金滞留在途。分支记录同时承担空回滚与防悬挂。

规则：

1. Try：校验收款商户账户存在且 ACTIVE（否则 `40001` / `40005`）；`INSERT transfer_branch(role = 'PAYEE', status = 'TRIED', amount)`。遇 1062 读取已有分支：TRIED、CONFIRMED 且金额一致 → 幂等成功；CANCELLED → `40004`（防悬挂）；金额不一致 → `10006`。
2. Confirm（同一事务）：CAS `TRIED → CONFIRMED`；`Post(TRANSFER_IN, transfer_no, 借本分片 TRANSIT 子账户 T，贷收款商户 T)`；回写 `txn_no`。已 CONFIRMED → 返回已有 `txn_no`。CANCELLED 或不存在 → `40004`：协调者只会在两个 Try 都成功后发起 Confirm，出现即为严重不一致，调用方记错误日志并计入 `pay_transfer_inconsistent_total`。
3. Cancel：CAS `TRIED → CANCELLED`。已 CANCELLED → 幂等成功。不存在 → 插入 CANCELLED 墓碑（空回滚），之后迟到的 Try 返回 `40004`。已 CONFIRMED → `40004`。
4. 收款方 Confirm 只增加商户余额、减少平台在途（允许透支），不会因余额不足失败；唯一的业务失败是账户在 Try 之后被停用（Post 返回 `40005`），本项目没有停用入口，按严重不一致处理。

异常：账户不存在 → `40001`；非 ACTIVE → `40005`；分支状态冲突 → `40004`；幂等冲突 → `10006`。

验收：

- [ ] Try → Confirm：收款商户余额 +T，本分片在途 −T，凭证类型 `TRANSFER_IN`；重复 Confirm 只记一次账。
- [ ] Try → Cancel：余额无变化，分支 CANCELLED；重复 Cancel 幂等。
- [ ] 先 Cancel（无记录）生成墓碑，随后 Try 返回 `40004`，Confirm 返回 `40004`，无任何记账。
- [ ] 对已 CONFIRMED 的分支 Cancel 返回 `40004`。
- [ ] 同一 `transfer_no` 并发 Try 与 Cancel 各 20 次：结果要么 TRIED 后被取消（最终 CANCELLED），要么 CANCELLED 墓碑且 Try 全部返回 `40004`；不存在 CONFIRMED。

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

接口：`AccountService.CheckBalance(merchant_id | all, expect_quiescent)` → `{passed, failures[{shard, account_no, check, expected, actual}]}`

规则：

1. 每个分片分别校验（不变量 3、4、5），每个分片一个只读一致性快照（`START TRANSACTION WITH CONSISTENT SNAPSHOT`，该事务单独用 REPEATABLE READ）：
   - 账户余额 = 按余额方向汇总的分录金额。
   - 账户最后一条分录的 `balance_after` = 当前余额（无分录时余额为 0）。
   - `frozen_amount` = 该账户 FROZEN 冻结单金额合计。
   - `allow_overdraft = 0` 的账户可用余额 ≥ 0。
   - 全量模式：每张凭证借贷平衡且分录数 = `entry_count`；全部分录借方总额 = 贷方总额；每张凭证的分录账户都属于本分片。
2. 在途守恒（不变量 11，仅全量模式）：在同一个分片快照内计算该分片的“预期在途” = 本分片 `TRANSFER` 冻结单中 CONFIRMED 的金额合计 − 本分片 PAYEE 分支中 CONFIRMED 的金额合计，与本分片 `TRANSIT` 子账户余额合计比较。两个分片分别相等，合计自然相等。不直接比较“两个分片合计 = 0”，因为两个快照不是同一时刻，进行中的转账会造成误报；参数 `expect_quiescent = true`（`verify.sh` 在全部转账终结后使用）时才额外校验两个分片在途合计为 0。
3. 只在管理接口、测试和 `verify.sh` 中手动触发；失败时 `pay_ledger_check_failures_total` 加一并输出错误日志。

验收：

- [ ] 正常数据核对通过。
- [ ] 测试中直接用 SQL 篡改余额 1 分，核对失败并指出分片、账户和差额。
- [ ] 跨分片转账只完成付款方 Confirm 时核对通过（在途按分支状态计算）；篡改一条 PAYEE 分支状态后核对失败。
- [ ] 在并发记账和并发转账进行时执行全量核对，不出现误报。

#### ACC-11 account RPC 服务

`P0 · M2 · 依赖：ACC-01~07、ACC-14、BASE-12`

规则：

1. Kitex 服务端暴露 `docs/05-api.md` §10.1 中 AccountService 的全部方法。
2. 服务端中间件：panic 恢复（返回 `10003`）、访问日志、trace、指标、错误码转换。
3. 服务端处理超时 2 秒。
4. payment 侧客户端超时：连接 100ms、调用 500ms；只对幂等方法在超时时重试 1 次（gateway 侧不重试，见 §5.4.1）：Freeze、ReleaseFreeze、ConfirmFreeze、TransferLocal、TransferInTry、TransferInConfirm、TransferInCancel、GetAccount。

验收：

- [ ] 通过 Kitex 客户端调用全部方法，错误码正确透传。
- [ ] handler 中 panic 不导致进程退出，调用方收到 `10003`。

#### ACC-12 热点账户优化

`CUT`：子账户拆分改为 OPT-03（P0），缓冲入账改为 OPT-05（P2）。

### 4.3 模拟渠道（CH）

channel_mock 代表外部支付机构，拥有独立 schema `pay_channel_mock`，平台服务不得直接访问其数据库。接口规范见 `docs/05-api.md` §9。所有请求与回调使用 BASE-13 的渠道签名。

#### CH-01 渠道支付受理与自动完成

`P0 · M3 · 依赖：BASE-13、BASE-03 · 表：mock_txn`

接口：`POST /mockpay/v1/pay`

规则：

1. 验签失败返回 HTTP 401。
2. 按 `(PAY, req_no)` 幂等：已存在且金额一致 → 返回已有结果；金额不一致 → `REJECTED / PARAM_CONFLICT`；已是 CLOSED → `REJECTED / ORDER_CLOSED`。
3. 新交易状态 PROCESSING，`scenario` 由金额末两位决定（CH-06），返回 `channel_txn_no`。
4. 自动完成：延迟 `delay = MOCK_DELAY_MIN_MS + hash(req_no) % (MOCK_DELAY_MAX_MS − MOCK_DELAY_MIN_MS)`（默认 200–2000ms）后，按场景置为 SUCCESS 或 FAILED，写入 `finished_at`；SUCCESS 且场景不是 BILL_MISSING 时写入 `bill_date = finished_at 的日期`。
5. 定时完成由进程内调度实现；进程重启时扫描全部 PROCESSING 记录重建调度。

验收：

- [ ] 同一 req_no 重复受理返回同一 `channel_txn_no`。
- [ ] NORMAL 场景 2 秒内变为 SUCCESS；重启 channel_mock 后未完成的交易仍会完成。

#### CH-02 渠道查单

`P0 · M3 · 依赖：CH-01`

接口：`POST /mockpay/v1/query`，入参 `txn_type`、`req_no`。

规则：返回当前状态、金额、`channel_txn_no`、`finished_at`；不存在返回 `NOT_FOUND`。查单结果不受 DROP_NOTIFY、SLOW 等回调场景影响。

验收：

- [ ] DROP_NOTIFY 场景下查单返回 SUCCESS。
- [ ] 未受理的 req_no 返回 `NOT_FOUND`。

#### CH-03 渠道关单

`P0 · M3 · 依赖：CH-01`

接口：`POST /mockpay/v1/close`，入参 `req_no`。

规则：

| 当前状态 | 结果 |
|---|---|
| PROCESSING | 置为 CLOSED，返回 `OK / CLOSED`（CLOSE_RACE 场景除外） |
| PROCESSING 且场景为 CLOSE_RACE | 立即置为 SUCCESS 并触发回调，返回 `ALREADY_PAID` |
| SUCCESS | 返回 `ALREADY_PAID` |
| CLOSED | 返回 `OK / CLOSED` |
| FAILED | 返回 `OK / FAILED` |
| 不存在 | 插入一条 CLOSED 记录后返回 `OK / CLOSED`，之后同一 req_no 的受理请求返回 `ORDER_CLOSED`（渠道侧防悬挂） |

验收：

- [ ] 表中每一行一个测试。
- [ ] 对未知 req_no 关单后再受理，返回 `ORDER_CLOSED`。

#### CH-04 渠道退款受理与查询

`P0 · M4 · 依赖：CH-01 · 表：mock_txn`

接口：`POST /mockpay/v1/refund`；查询复用 `POST /mockpay/v1/query`（`txn_type = REFUND`）。

规则：

1. 先按 `(REFUND, req_no)` 幂等，规则同 CH-01：已存在则直接返回已有结果，不再做下面的额度校验（避免重新提交时把本笔重复计入累计金额）。
2. 原支付 `orig_req_no` 必须为 SUCCESS，否则 `REJECTED / ORIG_NOT_SUCCESS`。
3. 该原支付下非 FAILED 退款金额合计 + 本次金额 ≤ 原支付金额，否则 `REJECTED / REFUND_EXCEEDED`。
4. 自动完成与回调规则同 CH-01、CH-05；场景按退款金额末两位决定，仅 01（FAIL）、02（DROP_NOTIFY）生效，其余为 NORMAL。

验收：

- [ ] 超额退款被拒绝；原支付未成功被拒绝。
- [ ] 全额退款受理后，用同一 `req_no` 重新提交返回已有结果，而不是 `REFUND_EXCEEDED`。
- [ ] 退款成功后进入当日账单（`txn_type = REFUND`）。

#### CH-05 渠道异步回调

`P0 · M3 · 依赖：CH-01`

规则：

1. 交易进入终态后，向受理时提供的 `notify_url` 发送签名回调（body 见 `docs/05-api.md` §9.7）。
2. 平台返回 HTTP 200 且 body 为 `SUCCESS` 视为送达；否则按 5s、15s、30s、60s 重试，共最多 5 次。
3. 每次发送 `notify_sent_count + 1`。
4. 回调使用独立的 `notify_id`（每次发送不同），便于平台日志排查。

验收：

- [ ] 平台前 2 次返回 500，第 3 次成功，渠道停止重试，`notify_sent_count = 3`。

#### CH-06 确定性故障场景

`P0 · M3–M4 · 依赖：CH-01~05`

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

M3 完成 01、02、03、08，其余在 M4 完成（06、07 的账单部分在 CH-08 中实现）。

验收：

- [ ] 每个场景至少一个集成测试，断言渠道侧行为（回调次数、状态、账单内容）。

#### CH-07 全局故障配置

`P1 · M7 · 依赖：CH-06`

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
- [ ] BILL_MISSING 交易不在账单中，BILL_AMOUNT_DIFF 交易金额 +1。

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

验收：

- [ ] 种子商户可查到，密钥解密结果与 `.env` 演示值一致。
- [ ] 不存在的 app_id 返回 `20004`。

#### PAY-02 创建支付单

`P0 · M3 · 依赖：PAY-01 · 表：payment_order`

接口：`POST /v1/payments` → `PaymentService.CreatePayment`

规则：

1. 入参（`docs/05-api.md` §4.1）：`out_trade_no`、`amount`、`subject` 必填；`currency` 默认 `CNY`；`notify_url` 默认商户配置；`expire_minutes` 默认 15，范围 1–120。
2. `notify_url` 必须是 http/https，且主机在 `NOTIFY_URL_ALLOW_HOSTS` 白名单内（防 SSRF，见 NFR-SEC-07）。
3. 生成 `pay_no`；`expire_at = now + expire_minutes`；手续费按下单时刻已生效的费率计算，即 `effective_from <= created_at` 的最近一档，并固化到 `fee_amount`；状态 INIT。PAY-02 仍是 TODO，本条不在 PAY-10 实现下单。
4. 插入遇 `uk_merchant_out_trade_no` 冲突时读取已有订单：`amount`、`currency`、`subject`、`notify_url` 全部一致 → 返回已有订单（不论当前状态）；否则 `10006`。

异常：参数错误 → `10001`；幂等冲突 → `10006`。

验收：

- [ ] 100 个并发相同请求只生成 1 条订单，全部返回同一 `pay_no`。
- [ ] 相同 `out_trade_no`、不同金额返回 `10006`。
- [ ] `expire_minutes = 0` 或 `121`、`notify_url` 不在白名单，返回 `10001`。
- [ ] 费率 60bp 的商户下单 10000，`fee_amount = 60`。

#### PAY-03 发起支付

`P0 · M3 · 依赖：PAY-02、CH-01 · 表：payment_order、channel_txn`

接口：`POST /v1/payments/{pay_no}/pay` → `PaymentService.Pay`

规则：

1. `channel` 目前只支持 `mockpay`，否则 `30007`。
2. 订单不属于当前商户 → `30001`。
3. 当前状态为 PAYING 或 SUCCESS → 返回当前订单（幂等）；FAILED、CLOSED → `30003`；INIT 且 `expire_at ≤ now + 30s` → `30004`（为渠道处理留出余量）。
4. 事务 1：CAS `INIT → PAYING` 并写入 `channel`；插入 `channel_txn(PAY, PROCESSING, next_query_at = now + 10s)`。故障点 `pay.after_tx1`。
5. 事务提交后调用渠道受理，`req_no = pay_no`，`notify_url` 为平台回调地址（配置 `CHANNEL_NOTIFY_URL`），连接超时 500ms、总超时 3 秒，不自动重试。
6. 渠道结果：
   - `OK`：回填 `channel_txn_no`（`WHERE channel_txn_no IS NULL`），返回 PAYING。
   - `REJECTED`：调用 PAY-04 推进为 FAILED（`fail_code = CHANNEL_REJECTED`，`fail_msg` 记录渠道 `reason`），返回 `30006` 和订单。
   - 超时、网络错误、5xx：结果未知，不改状态，返回 PAYING，由回调或查单得到最终结果。

验收：

- [ ] 正常场景返回 PAYING，2 秒内订单变为 SUCCESS。
- [ ] 同一订单并发调用 Pay 20 次，只插入 1 条 `channel_txn`，渠道只受理 1 次。
- [ ] 对 INIT 订单并发执行 Pay 与 Close，结果只能是 PAYING 或 CLOSED 之一。
- [ ] ACCEPT_TIMEOUT 场景接口在约 3 秒返回 PAYING，最终订单 SUCCESS。
- [ ] 渠道返回 REJECTED（用假渠道客户端注入）时订单为 FAILED，返回 `30006`。

#### PAY-04 支付结果推进

`P0 · M3 · 依赖：PAY-03、MQ-01 · 表：channel_txn、payment_order、outbox`

说明：回调（GW-06）、查单（PAY-05）、关单（PAY-06）、同步拒绝（PAY-03）共用的状态推进函数 `ApplyPayResult(biz_no, result, source)`。

规则：

1. 状态机：

   ```
   INIT ──Pay──► PAYING ──SUCCESS──► SUCCESS
    │              ├──FAILED───► FAILED
    │              └──CLOSED───► CLOSED
    └──关单/超时──────────────► CLOSED
   ```

   SUCCESS、FAILED、CLOSED 为终态。一笔支付单只对应一次渠道交易，失败后商户需用新的 `out_trade_no` 重新下单。

2. 事务：

   ```
   UPDATE channel_txn
      SET status = ?, channel_txn_no = COALESCE(channel_txn_no, ?), channel_status_raw = ?,
          channel_finished_at = ?, next_query_at = NULL, version = version + 1
          [, notify_count = notify_count + 1]          -- source = NOTIFY 时
    WHERE biz_no = ? AND status = 'PROCESSING' [AND amount = ?]   -- 结果携带金额时
   影响行数 = 1：
     UPDATE payment_order SET status = ?, paid_at | closed_at | fail_code, fail_msg = ?, version = version + 1
      WHERE pay_no = ? AND status = 'PAYING'          -- 必须为 1，否则回滚、告警、返回 10003
     SUCCESS 时 outbox.Add(payment.succeeded)         -- MQ-01
   影响行数 = 0：重新读取 channel_txn
     不存在                → 30001
     金额与本地不一致       → 50002，不改状态，告警
     已是同一终态           → 幂等成功（NOTIFY 时 notify_count + 1）
     是不同终态             → 不改状态，pay_callback_conflicts_total + 1，记录错误日志，等待对账处理
   ```

3. `paid_at` 取渠道完成时间，不取本地处理时间。渠道结果为 FAILED 时 `fail_code = CHANNEL_FAILED`（取值登记见 `docs/03-conventions.md` §5.4）。
4. 回调只携带终态，终态不可变，因此乱序回调不会导致状态倒退。

验收：

- [ ] DUP_NOTIFY 场景：10 次回调后订单只推进一次，outbox 只有 1 条 `payment.succeeded`，`notify_count = 10`（A3 订单层）。
- [ ] 回调金额与本地不一致返回 `50002`，订单仍为 PAYING。
- [ ] 先收到 FAILED 再收到 SUCCESS，订单保持 FAILED，冲突指标 +1。
- [ ] 测试中让 `payment_order` 的 CAS 失败（人为改状态），整个事务回滚，`channel_txn` 未变化。

#### PAY-05 查单补偿

`P0 · M4 · 依赖：PAY-04、CH-02 · 表：channel_txn`

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
- [ ] SLOW 场景下查单先于回调完成，随后的回调按重复处理。

#### PAY-06 商户关单

`P0 · M3 · 依赖：PAY-04、CH-03 · 表：payment_order、channel_txn`

接口：`POST /v1/payments/{pay_no}/close` → `PaymentService.ClosePayment`

规则：

| 当前状态 | 处理 | 返回 |
|---|---|---|
| INIT | CAS `INIT → CLOSED`，写 `closed_at` | CLOSED |
| PAYING | 事务外调用渠道关单：`CLOSED` → `ApplyPayResult(CLOSED)`；`ALREADY_PAID` → 立即查单并 `ApplyPayResult(SUCCESS)`；`FAILED`（渠道交易已失败）→ `ApplyPayResult(FAILED, CHANNEL_FAILED)`；调用失败 → 不改状态 | CLOSED；已支付返回 `30003` 和 SUCCESS 订单；已失败返回 `30003` 和 FAILED 订单；调用失败返回 `10004` |
| CLOSED | 幂等 | CLOSED |
| SUCCESS、FAILED | 不处理 | `30003` |

PAYING 状态必须等渠道确认关闭后才能 CLOSED，避免“本地已关、渠道扣款成功”。

验收：

- [ ] NEVER_FINISH 场景关单后订单 CLOSED，之后渠道不会再回调成功。
- [ ] CLOSE_RACE 场景关单返回 `30003`，订单 SUCCESS，商户最终入账。

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

规则：只能查询当前商户的订单，他人订单返回 `30001`（不泄露存在性）；直接读库，不做缓存。

验收：

- [ ] 用商户 B 查询商户 A 的 pay_no 返回 `30001`。

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

验收：

- [ ] 订单 10000，10 个并发请求各退 3000（不同 out_refund_no），恰好 3 个创建成功，其余 `30005`，`refunded_amount = 9000`。
- [ ] 相同 `out_refund_no` 重复请求返回同一 `refund_no`；金额不同返回 `10006`。
- [ ] 对 PAYING 订单退款返回 `30003`。

#### REF-02 退款冻结与渠道提交

`P0 · M4 · 依赖：REF-01、ACC-11、CH-04 · 表：refund_order、channel_txn`

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

验收：

- [ ] 正常退款：返回 PROCESSING，随后 SUCCESS。
- [ ] 商户余额不足：退款 FAILED，`refunded_amount` 恢复，无冻结残留。
- [ ] account 不可用：退款保持 INIT，恢复后由 REF-04 完成。
- [ ] 渠道受理拒绝：接口返回 `30006`，退款单 FAILED，冻结最终被解冻。

#### REF-03 退款结果推进

`P0 · M4 · 依赖：REF-02 · 表：refund_order、channel_txn、payment_order、outbox`

说明：`ApplyRefundResult(refund_no, result, source)`，被回调、查单、REF-02 共用。

规则：

1. 渠道结果（PROCESSING 退款）事务：CAS `channel_txn PROCESSING → X`（规则同 PAY-04 第 2 条）；CAS `refund_order PROCESSING → X`；
   - SUCCESS：写 `succeeded_at`（渠道完成时间），`outbox.Add(refund.succeeded)`。
   - FAILED：`fail_code = CHANNEL_FAILED`；`UPDATE payment_order SET refunded_amount = refunded_amount - ? WHERE pay_no = ? AND refunded_amount >= ?`，`outbox.Add(refund.failed)`。
2. 冻结失败或超时失败（INIT 退款）事务：CAS `refund_order INIT → FAILED`；释放 `refunded_amount`；`outbox.Add(refund.failed)`。
3. account 消费 `refund.succeeded` 执行 ConfirmFreeze（贷方为本分片渠道应收子账户），消费 `refund.failed` 执行 ReleaseFreeze（冻结不存在时为空回滚，ACC-05）。
4. 重复、冲突结果的处理同 PAY-04。

验收：

- [ ] 退款 3000 成功并完成消费后：商户余额 −3000、冻结 0、渠道应收 −3000，`CheckBalance` 通过。
- [ ] FAIL 场景（退款金额末两位 01）：退款 FAILED，冻结释放，`refunded_amount` 恢复，可再次退款。
- [ ] 重复回调只推进一次、只产生 1 条事件。

#### REF-04 退款卡单恢复

`P0 · M4 · 依赖：REF-02 · 表：refund_order`

规则：

1. 每 10 秒扫描 `status = 'INIT' AND updated_at <= now − 30s`（使用 `idx_status_updated_at`），从 REF-02 第 1 步重新执行。Freeze 幂等、CAS 互斥、`channel_txn.uk_biz_no` 唯一，因此重复执行安全。
2. INIT 超过 1 小时仍未能推进，按 REF-03 第 2 条置为 FAILED（`fail_code = REFUND_TIMEOUT`）。此时渠道一定未收到请求（提交渠道前已置为 PROCESSING），失败不会造成资损。

验收：

- [ ] 在故障点 `refund.after_freeze` 处 kill 进程，重启后退款继续完成，冻结单只有 1 张（C6）。
- [ ] account 持续不可用 1 小时（Fake 时钟），退款 FAILED，`refunded_amount` 恢复。

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
- [ ] account 不可用：转账保持 TRYING，恢复后由 TRF-04 推进为终态，只有 1 张凭证。

#### TRF-03 跨分片转账 TCC 协调

`P0 · M4 · 依赖：TRF-01、ACC-04~06、ACC-14、ACC-11、MQ-01 · 表：transfer_order、outbox`

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
7. 各阶段耗时上报 `pay_stage_duration_seconds{chain="transfer_tcc", stage}`（OPT-01）。

验收：

- [ ] `M100001 → M100002` 转 500：转账 SUCCESS；分片 1 有 `TRANSFER_OUT`，分片 0 有 `TRANSFER_IN`；两个分片在途合计 0；`CheckBalance` 通过。
- [ ] 付款方余额不足：转账 FAILED（`40002`），收款方分支 CANCELLED，付款方冻结单 RELEASED 墓碑，没有任何转账凭证。
- [ ] 注入收款方 Try 超时（测试替身）：转账 FAILED（`TRY_TIMEOUT`），付款方冻结被释放，可用余额恢复。
- [ ] 注入付款方 Confirm 失败一次：接口返回 CONFIRMING，TRF-04 推进为 SUCCESS，凭证各 1 张。
- [ ] A、B 分属两个分片且各 10000，A→B 与 B→A 各 200 笔并发转 1：全部终态后 A + B = 20000，在途合计 0，`CheckBalance` 通过。
- [ ] 并行与串行模式下以上结果一致。

#### TRF-04 转账恢复任务

`P0 · M4 · 依赖：TRF-02、TRF-03 · 表：transfer_order`

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

#### TRF-05 转账查询

`P0 · M4 · 依赖：TRF-01 · 表：transfer_order`

接口：`GET /v1/transfers/{transfer_no}`、`GET /v1/transfers?out_transfer_no=` → `PaymentService.QueryTransfer`

规则：只有付款商户可以查询；收款商户与其他商户查询返回 `30001`。

验收：

- [ ] 收款商户查询返回 `30001`。

### 4.7 本地消息表与 MQ（MQ）

#### MQ-01 outbox 写入

`P0 · M3 · 依赖：BASE-08、BASE-05 · 表：outbox`

规则：

1. `outbox.Add(tx *sqlx.Tx, evt)` 只接受事务参数，保证与业务数据同事务提交。
2. 生成 `event_id`，按 `docs/05-api.md` §11.3 组装事件信封写入 `payload`；`msg_key` 为 `pay_no`（退款事件也用原 `pay_no`，便于同一订单的消息落在同一分区；转账事件用 `transfer_no`）；`biz_no` 为事件对应的业务单号（支付事件为 `pay_no`，退款事件为 `refund_no`，转账事件为 `transfer_no`），供 MQ-05 按业务单号重放；状态 PENDING，`next_retry_at = now`。
3. 写入时把当前 trace context 存入信封 `trace_context`，供消费端关联链路（OPS-02）。

验收：

- [ ] 业务事务回滚时 outbox 无记录；提交时恰好 1 条。

#### MQ-02 outbox 投递器

`P0 · M5 · 依赖：MQ-01、MQ-03 · 表：outbox`

规则：

1. payment 进程内运行，每 200ms 执行：

   ```
   BEGIN
     SELECT * FROM outbox WHERE status = 'PENDING' AND next_retry_at <= now
      ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
     批量同步发送（等待全部确认，超时 5s）
     故障点 outbox.after_send
     成功：status = 'SENT', sent_at = now
     失败：retry_count + 1，next_retry_at = now + min(2^retry_count 秒, 5 分钟)，last_error 截断到 512 字符
           retry_count 达到 20：status = 'DEAD'
   COMMIT
   ```

2. 发送期间持有行锁（最长约 5 秒），换取实现简单与多实例天然互斥。
3. Kafka 确认后、COMMIT 前崩溃会导致重复发送，由消费端幂等吸收。
4. 业务正确性不依赖消息顺序：退款冻结在创建时同步完成，入账与退款确认之间没有先后依赖。
5. 每 10 秒更新指标 `pay_outbox_pending`、`pay_outbox_dead`。

验收：

- [ ] Kafka 停止 2 分钟期间事件积压，恢复后全部投递，`pay_outbox_pending` 回落到 0（C7）。
- [ ] 两个投递器并发运行 1 万条事件，Kafka 中每个 `event_id` 至少出现 1 次，且正常情况下不重复。
- [ ] 注入发送失败的假生产者，20 次后状态为 DEAD，`pay_outbox_dead` 为 1。

#### MQ-03 Kafka 生产与消费框架（pkg/mq）

`P0 · M5 · 依赖：BASE-02`

规则：

1. 生产者：franz-go，`acks=all`，开启幂等生产，`linger=5ms`；消息头携带 `event_id`、`event_type`、`traceparent`。
2. 消费者：消费组模式，关闭自动提交；每个分区一个处理协程，分区内顺序处理；处理成功（数据库事务已提交）后才提交 offset。
3. 处理函数返回的错误分两类：
   - 可重试（数据库、网络、依赖不可用）：原地退避重试，100ms 起翻倍、上限 5 秒，不提交 offset，持续重试并告警。
   - 不可重试（反序列化失败、数据缺失、业务冲突）：写入 `pay.dlq`（消息头附原 topic、分区、offset、错误信息），提交 offset，`pay_mq_dlq_total + 1`。
4. 分区再平衡时等待当前消息处理完成并提交。
5. 每个分区上报消费延迟 `pay_mq_consume_lag`。
6. 优雅退出：停止拉取，处理完在途消息并提交后关闭。

验收：

- [ ] 处理函数前 3 次返回可重试错误、第 4 次成功：消息只成功处理一次，offset 已提交。
- [ ] 不可重试错误的消息进入 `pay.dlq`，后续消息继续消费。
- [ ] 处理成功、提交前 kill 进程，重启后重新投递（由业务幂等吸收）。

#### MQ-04 account 事件消费

`P0 · M5 · 依赖：MQ-03、ACC-03、ACC-05、ACC-06`

消费组 `account-ledger`，在 account 进程内直接调用 biz 层（不经过 RPC）。本功能点实现逐条模式（每条消息一个分片事务），是 OPT-02 批量模式的基线和退化路径：

| 事件 | 处理 | 不可重试情况 |
|---|---|---|
| `payment.succeeded` | 按 `merchant_id` 路由分片，`Post(PAY, pay_no)`，分录按 §2.5.2：借本分片渠道应收子账户 A，贷商户 A − F，贷本分片手续费收入子账户 F | 商户账户不存在 |
| `refund.succeeded` | `ConfirmFreeze(REFUND, refund_no, merchant_id, channel)`，贷本分片渠道应收子账户 | 返回 `40004`（冻结单缺失或已解冻，属于严重不一致，计数并输出 ERROR 日志） |
| `refund.failed` | `ReleaseFreeze(REFUND, refund_no, merchant_id, amount)`，冻结不存在时为空回滚 | 返回 `40004`（已确认扣款） |

故障点 `ledger.after_commit`（数据库提交后、offset 提交前）。

验收：

- [ ] 同一 `payment.succeeded` 消息投递 5 次，只生成 1 张凭证（A3 账务层）。
- [ ] 在 `ledger.after_commit` kill 进程，重启后重复消费不重复入账（C5）。
- [ ] 商户账户不存在时消息进入 DLQ，后续消息正常处理。

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

`P0 · M5 · 依赖：NTF-01、BASE-13 · 表：merchant_notify`

规则：

1. 每秒抢占到期的 PENDING 记录（`FOR UPDATE SKIP LOCKED`，抢占时 `attempt_count + 1` 并推后 `next_attempt_at`，方式同 PAY-05），事务外以最多 20 个并发发送。
2. 请求：`POST notify_url`，JSON body，按 BASE-13 平台签名方式签名；超时 5 秒；不跟随重定向；响应体最多读取 1KB。
3. 成功判定：HTTP 200 且去除首尾空白后 body 为 `SUCCESS`，置为 SUCCESS。
4. 固定间隔重试：首次立即发送，之后每隔 `NOTIFY_RETRY_INTERVAL`（默认 30 秒）重试，共 `NOTIFY_MAX_ATTEMPTS`（默认 5）次；5 次仍失败置为 DEAD，`pay_notify_attempts_total{result="dead"} + 1`。真实系统常用递增间隔（如 15s、15s、30s、3m …），本项目不是重点，简化为固定间隔。
5. 发送前再次校验 `notify_url` 主机在白名单内（NFR-SEC-07）；不在白名单时不发送，直接置为 DEAD，`last_error` 记录原因，`pay_notify_attempts_total{result="dead"} + 1` 并告警（白名单不会因重试而改变）。
6. 商户必须按 `pay_no` / `refund_no` / `transfer_no` 幂等处理，平台不保证只通知一次。

验收：

- [ ] merchant-demo 配置前 3 次返回失败，第 4 次成功，记录 SUCCESS 且 `attempt_count = 4`。
- [ ] 一直失败时，Fake 时钟推进后共尝试 5 次，相邻两次间隔 30 秒，状态 DEAD。
- [ ] merchant-demo 验证通知签名通过；篡改密钥后验签失败。

### 4.9 网关（GW）

#### GW-01 网关骨架与统一响应

`P0 · M3 · 依赖：BASE-12、BASE-13`

规则：

1. 两个 Hertz 实例：8080（商户 API、渠道回调）、8090（管理接口），请求体上限 64KB，超过返回 413 / `10001`（不签名）。
2. 商户路由中间件顺序：Recovery → RequestID → Trace → AccessLog → 时间窗口（GW-04）→ 验签（GW-03）→ 商户限流（GW-05）→ nonce（GW-04）→ handler。限流放在 nonce 之前，超限请求不消耗 Redis 调用；被限流的请求不占用 nonce，商户可用同一 nonce 重试。
3. 统一响应：`{"code": 0, "msg": "ok", "request_id": "...", "data": {...}}`；响应头 `X-Request-Id`；验签通过的请求，响应带平台签名头 `X-Timestamp`、`X-Nonce`、`X-Signature`；验签前失败的响应不签名。
4. 错误码按 `docs/03-conventions.md` §5.2 映射 HTTP 状态；`10003` 只返回通用消息和 request_id。
5. Kitex 客户端：连接超时 100ms，调用超时 1 秒；`Pay`、`ClosePayment`、`CreateRefund` 含渠道调用，调用超时 5 秒；`CreateTransfer` 调用超时 3 秒（与 §5.4.1 一致）；网关不重试写请求。

验收：

- [ ] handler panic 返回 500 / `10003`，进程不退出。
- [ ] 超过 64KB 的请求被拒绝。
- [ ] 成功响应可用 paycli 验证签名。

#### GW-02 商户 API 路由

`P0 · M3–M4 · 依赖：GW-01、PAY-02/03/06/08、REF-01/05、TRF-01/05`

规则：

1. 路由与 RPC 映射见 `docs/05-api.md` §1、§10.2。
2. `merchant_id` 只来自验签结果，忽略请求体或查询串中的同名字段（转账的付款方同样只来自验签结果）。
3. 参数校验在网关完成一次（格式、长度、范围），payment 再做一次业务校验：
   - `amount`：整数，1–10,000,000。
   - `out_trade_no`、`out_refund_no`、`out_transfer_no`：1–64 位 `[A-Za-z0-9_-]`。
   - `payee_merchant_id`：`M` + 6 位数字。
   - `subject`：1–128 字符；`remark`：≤ 128 字符；`reason`：≤ 255 字符。
   - `notify_url`：≤ 512，http/https。
   - JSON 中的未知字段忽略；类型错误（如金额为字符串或小数）返回 `10001`。

验收：

- [ ] `amount` 为 `100.5`、`"100"`、`0`、`10000001` 均返回 `10001`。
- [ ] 请求体中伪造 `merchant_id` 不影响订单归属。

#### GW-03 商户验签

`P0 · M3 · 依赖：GW-01、PAY-01`

规则：

1. 必需请求头：`X-App-Id`、`X-Timestamp`、`X-Nonce`、`X-Signature`，缺任一 → `20001`。
2. 按 `app_id` 取商户凭证（GW-08 完成前每次调用 `GetMerchantCredential`）；不存在或 DISABLED → `20004`；调用失败（payment 不可用）→ 503 / `10004`，不缓存失败结果。
3. 按 BASE-13 计算签名，常量时间比较，不一致 → `20001`。
4. 通过后把 `merchant_id` 写入请求上下文。
5. 日志只记录 `app_id` 和结果，不记录签名值和 canonical 串。

验收（A7）：

- [ ] 签名错误、缺少任一请求头、未知 app_id、商户停用，分别返回对应错误码，且不进入 handler。
- [ ] 正确签名的请求通过。

#### GW-04 时间窗口与 nonce 防重放

`P0 · M5 · 依赖：GW-03`

规则：

1. `X-Timestamp` 为 Unix 秒，与服务器时间相差超过 ±300 秒 → `20002`。该检查在验签之前执行（成本低）。
2. `X-Nonce` 为 16–32 位 `[A-Za-z0-9]`，格式错误 → `20001`。
3. 验签通过后执行 `SET gw:nonce:{app_id}:{nonce} 1 NX EX 600`；已存在 → `20003`。验签失败的请求不占用 nonce，防止攻击者用伪造请求抢占合法 nonce。
4. Redis 超时（50ms）或不可用 → `10004`（fail-closed），`pay_nonce_unavailable_total + 1`。

验收（A7）：

- [ ] 时间戳超前或落后 301 秒返回 `20002`，299 秒通过。
- [ ] 同一 nonce 第二次使用返回 `20003`；签名错误的请求使用过的 nonce 仍可被合法请求使用。
- [ ] Redis 停止时商户接口返回 503 / `10004`。

#### GW-05 商户限流（进程内）

`P0 · M5 · 依赖：GW-03`

规则：

1. 商户限流放在验签之后（防止伪造 app_id 耗尽他人配额），nonce 检查之前。
2. 进程内按 `app_id` 的令牌桶（`golang.org/x/time/rate`），默认 100 req/s、突发 200，可通过 `RATE_LIMIT_OVERRIDES`（如 `M100001:1000:2000`）按商户覆盖；限流器按需创建，存放在 `sync.Map`。
3. 超限返回 429 / `10005`，带 `Retry-After` 头。
4. 不做全局限流和 Redis 分布式限流。网关多实例时实际配额为“单实例配额 × 实例数”，README 中说明。

验收：

- [ ] 单商户 1 秒内 300 个请求，约 200 个通过（突发容量），其余 429。
- [ ] 一个商户被限流不影响另一个商户。

#### GW-06 渠道回调入口

`P0 · M3 · 依赖：GW-01、PAY-04`

接口：`POST /callback/mockpay` → `PaymentService.HandleChannelNotify`

规则：

1. 校验渠道签名与时间窗口（±300 秒），失败返回 401 / `50001`，不调用 payment。
2. 不做 nonce 检查（回调处理本身幂等），因此 Redis 故障不影响回调。
3. 按 `txn_type` 分发到 `ApplyPayResult` 或 `ApplyRefundResult`（source = NOTIFY）。
4. payment 返回成功、幂等、冲突（已记录告警）时，网关返回 HTTP 200、body `SUCCESS`；返回 `10003`、`10004` 时返回 HTTP 500，让渠道重试。
5. payment 返回 `30001`（未知 req_no）、`50002`（金额不一致）时，重试不会自愈：网关返回 HTTP 200、body `SUCCESS`，记错误日志，`pay_callback_rejected_total{code}` + 1 并告警，差异交给对账（LOCAL_MISSING / AMOUNT_MISMATCH）。
6. payment 侧故障点 `callback.after_commit`：事务提交后、返回响应前。

验收：

- [ ] 签名错误的回调不改变任何订单（A7）。
- [ ] 签名正确但 req_no 未知、金额不一致的回调返回 200 SUCCESS，订单不变，告警指标 + 1。
- [ ] Redis 停止时回调仍能正常推进订单。

#### GW-07 管理接口

`P0 · M6 · 依赖：GW-01`

规则：

1. 只在 8090 端口提供，宿主机只绑定 `127.0.0.1`。
2. 请求头 `X-Admin-Token` 与配置 `ADMIN_TOKEN`（≥ 32 字符，启动时校验）常量时间比较，失败 → 401 / `20005`。
3. 写操作必须带 `X-Admin-Operator`，写入审计日志（操作人、接口、参数摘要、结果）。
4. 只提供三个接口（清单见 `docs/05-api.md` §8）：事件重放（MQ-05）、余额核对（ACC-10）、手动触发对账（REC-01）。对账结果在触发接口的响应中返回，不提供批次和差错的列表查询。
5. 这是适合本地演示的简化鉴权，不提供细粒度权限，README 中需说明生产环境的替代方案。

验收：

- [ ] 无 Token、错误 Token 返回 401。
- [ ] 写操作缺少 `X-Admin-Operator` 返回 `10001`。

#### GW-08 商户密钥缓存

`P0 · M7 · 依赖：GW-03、OPT-01`

规则：进程内缓存商户凭证 60 秒（`GW_CREDENTIAL_CACHE_TTL`，设为 0 关闭，作为对比基线）；同一 app_id 并发未命中时用 singleflight 只回源一次；不存在的 app_id 负缓存 10 秒；商户停用最长 60 秒后生效（README 说明）。属于 OPT-04 的优化项之一。

验收：

- [ ] 1000 个并发请求同一 app_id，只回源 1 次。
- [ ] 压测报告给出开启前后三条链路网关阶段的 P99 对比。

### 4.10 对账（REC）

#### REC-01 对账批次与调度

`P0 · M6 · 依赖：BASE-03 · 表：recon_batch`

接口：`ReconService.RunBatch(channel, bill_date, force)` → `{batch, diffs[≤ 100]}`；CLI `recon run --channel mockpay --date YYYY-MM-DD [--force]`；管理接口 `POST /admin/recon/run`

规则：

1. 每天 `RECON_RUN_AT`（默认 10:00）对 D−1 发起对账，渠道列表来自配置（目前只有 `mockpay`）。
2. 按 `uk_channel_bill_date` 创建批次；已存在且 DONE、未指定 force → 直接返回已有结果；FAILED 或 force → 重跑；INIT、DOWNLOADED 或未超时的 CHECKING → 返回当前批次，不重复执行。
3. 重跑：删除该批次的 `recon_channel_bill` 和状态为 PENDING 的差错，保留 RESOLVED、IGNORED 的差错及处理记录；然后 CAS 把批次重置为 INIT，从下载开始执行。
4. 批次状态：`INIT → DOWNLOADED → CHECKING → DONE`，任一步失败 → FAILED 并记录 `error_msg`。
5. 并发控制：CAS 推进状态；处于 CHECKING 且 `started_at` 超过 30 分钟视为卡死，可被重新接管。

验收：

- [ ] 同一账期并发触发两次，只有一次执行。
- [ ] 重跑后差错结果与首次一致，已处理的差错不丢失。

#### REC-02 账单下载与导入

`P0 · M6 · 依赖：REC-01、CH-08 · 表：recon_channel_bill`

规则：

1. 下载超时 30 秒，失败重试 3 次（间隔 10 秒）。
2. 校验 `X-Bill-Sha256` 与 `X-Bill-Count`，不一致 → FAILED。
3. 严格解析 CSV：表头必须与 `docs/05-api.md` §9.6 一致；金额为整数；时间格式合法；任一行错误 → FAILED，错误信息带行号。
4. 渠道返回 `BILL_NOT_READY` → 批次保持 INIT，30 分钟后重试。
5. 在一个事务内每 1000 行批量插入，状态置为 DOWNLOADED，写入 `bill_file_hash`。

验收：

- [ ] 篡改账单内容导致哈希不一致，批次 FAILED。
- [ ] 1 万行账单导入耗时 < 5 秒。

#### REC-03 外部对账比对

`P0 · M6 · 依赖：REC-02 · 表：recon_channel_bill、recon_diff；只读 channel_txn`

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
4. 更新账单行 `match_status`；差错按 `uk_batch_type_biz_no` 插入；批次写入双方笔数、金额、匹配数、差错数。
5. 数据量假设：单渠道单日 ≤ 100 万笔。P0 实现可在内存中按匹配键建索引；超过假设规模时改为按 `biz_no` 排序分段归并。

验收（A6）：

- [ ] 构造账单：注入一笔平台不存在的交易（长款）、BILL_MISSING（短款）、BILL_AMOUNT_DIFF（金额不符）、DROP_NOTIFY 且关闭查单补偿（状态不符），四类各识别 1 条，其余全部匹配。
- [ ] 全部正常的账单差错数为 0。

#### REC-04 跨日处理

`CUT`：见 REC-03 第 3 条。

#### REC-05 内部对账（支付、退款、转账）

`P0 · M6 · 依赖：REC-01 · 只读：payment_order、refund_order、transfer_order、两个分片的 ledger_txn、account_freeze、transfer_branch`

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

验收：

- [ ] 删除某笔支付对应的 outbox 事件并阻止入账后，对账识别为 `LEDGER_MISSING`。
- [ ] 测试中删除一张 `TRANSFER_IN` 凭证（构造数据），对账识别为 `TRANSFER_MISMATCH`；正常转账差错数为 0。

#### REC-06 差错自动处理

`P0 · M6 · 依赖：REC-03、REC-05、PAY-09、MQ-05 · 表：recon_diff`

规则：

| 差错 | 自动处理 | 成功后 |
|---|---|---|
| `STATUS_MISMATCH`，本地 PROCESSING | `PaymentService.SyncChannelTxn`（`REQUERY`） | RESOLVED |
| `CHANNEL_MISSING` | `SyncChannelTxn` 查单（`REQUERY`）；渠道确认 SUCCESS 说明账单遗漏 | IGNORED，说明“渠道账单遗漏，查单确认成功” |
| `LEDGER_MISSING` | `PaymentService.ReplayEvent(biz_no, event_type)`，写 `handle_action = REPLAY`，保持 PENDING | 由下面的复核推进为 RESOLVED |
| `LOCAL_MISSING`、`AMOUNT_MISMATCH`、`STATUS_MISMATCH`（本地已是终态）、`TRANSFER_MISMATCH` | 无 | 保持 PENDING，ERROR 日志，写入对账报告；真实系统需要人工处理与挂账，本项目不实现（REC-07 CUT） |

自动处理记录 `handle_action`、`handler = system`；处理失败保持 PENDING，下次批次再试。

LEDGER_MISSING 复核：每次批次运行（任意账期）都查询全部 `diff_type = LEDGER_MISSING AND status = 'PENDING' AND handle_action = 'REPLAY'` 的差错（不限批次），到对应账户分片确认凭证存在且金额一致 → CAS `PENDING → RESOLVED`（`handler = system`，说明“重放后复核通过”）；否则保持 PENDING。

验收：

- [ ] 状态不符（本地 PAYING）自动修复后，订单 SUCCESS 且商户入账。
- [ ] `LEDGER_MISSING` 经重放后，下一次任意账期的对账运行把它置为 RESOLVED。

#### REC-07 差错人工处理与挂账

`CUT`：不提供人工处理接口和差错挂账账户。未自动解决的差错保持 PENDING，出现在对账报告和 ERROR 日志中。

#### REC-08 对账报告

`P0 · M6 · 依赖：REC-03、REC-05`

规则：

1. 批次完成后，CLI 和 `POST /admin/recon/run` 的响应输出：`recon_batch` 中的账期、渠道、双方笔数与金额、匹配数、差错数；按 `recon_diff` 实时聚合的各类差错数与自动处理数（`handle_action` 非空）；前 100 条差错。不按支付、退款分列。
2. 指标 `pay_recon_diffs_total{diff_type}`；PENDING 差错数作为 gauge `pay_recon_pending_diffs`。

验收：

- [ ] 报告中的笔数和金额与账单、本地数据的手工统计一致。

### 4.11 可观测与运维（OPS）

#### OPS-01 健康检查与优雅退出

`P0 · M3 · 依赖：BASE-09`

规则：

1. 管理端口提供 `/healthz`（进程存活即 200）和 `/readyz`（依赖检查，单项超时 1 秒）：数据库必检；gateway 检查 Redis；payment、account 检查 Kafka 连接。
2. 收到 SIGTERM：`/readyz` 立即返回 503 → 停止接收新请求和新消息 → 等待在途请求、消息处理和任务批次完成（上限 10 秒）→ 刷新生产者 → 关闭连接。
3. docker-compose 中 `stop_grace_period: 15s`。

验收：

- [ ] 压测进行中对 payment 执行 `docker stop`，客户端无 5xx 以外的异常（连接被拒除外），重启后无数据不一致。

#### OPS-02 链路追踪

`P0 · M5 · 依赖：GW-01、ACC-11、MQ-03`

规则：

1. OTel SDK，OTLP/gRPC 上报 Jaeger；`service.name` 为进程名；默认全量采样，可配置比例。
2. 覆盖：Hertz 服务端、Kitex 客户端与服务端、payment → channel 的 HTTP 调用、Kafka 生产与消费、补偿任务每一批。
3. 消费端从信封 `trace_context` 提取上下文，作为 link 关联到原请求链路。
4. span 属性只放单号、状态、错误码，不放金额以外的业务明细，不放密钥和签名。

验收：

- [ ] 在 Jaeger 中能按 `pay_no` 找到从网关下单、发起支付、回调到入账消费的完整链路。

#### OPS-03 业务与运行指标

`P0 · M5 · 依赖：OPS-01`

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
| `pay_recon_diffs_total`、`pay_recon_pending_diffs` | counter、gauge | diff_type |
| `pay_ratelimit_rejected_total`、`pay_nonce_unavailable_total` | counter | — |

约束：标签中不得出现单号、商户号以外的高基数值（商户号也只用于限流类指标）；延迟直方图桶 5ms、10ms、25ms、50ms、100ms、250ms、500ms、1s、2.5s、5s。

验收：

- [ ] 执行一次完整支付后，相关计数器按预期增加。

#### OPS-04 监控面板与告警规则

`CUT`：不部署 Grafana，不配置告警规则。本文中的“告警”统一指“对应指标加一并输出 ERROR 日志”。压测报告直接使用 Prometheus 查询结果。

#### OPS-05 pprof 与管理端口

`P0 · M3 · 依赖：OPS-01`

规则：`net/http/pprof` 只注册在管理端口，与业务端口分离；宿主机只绑定 `127.0.0.1`。

验收：

- [ ] 业务端口访问 `/debug/pprof/` 返回 404；管理端口可采集 30 秒 CPU profile。

### 4.12 性能优化（OPT）

每个优化项都有独立开关，关闭时即为基线。报告在 `docs/perf-report.md` 中按“现象与证据 → 改动 → 同一场景前后数据 → 结论”记录，无效的尝试也要记录。

#### OPT-01 分阶段耗时埋点与基线

`P0 · M5 · 依赖：OPS-03`

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
- [ ] 基线报告包含 6 条链路的分阶段耗时表。

#### OPT-02 批量记账合并提交

`P0 · M7 · 依赖：MQ-04、QA-02 · 表：ledger_txn、ledger_entry、account`

规则：

1. 开关 `LEDGER_BATCH_ENABLED`（默认 true；false 即 MQ-04 逐条模式）。只批量处理 `payment.succeeded`；同一批中的其他事件在批量部分提交后按原顺序逐条处理。
2. 每个分区一次拉取的记录中，最多取 `LEDGER_BATCH_SIZE`（默认 100）条，按 `shard.Of(merchant_id)` 分组，每组一个分片事务：

   ```
   BEGIN
     SELECT biz_no FROM ledger_txn WHERE biz_type = 'PAY' AND biz_no IN (...)   -- 已入账的跳过
     批量 INSERT ledger_txn（剩余事件）
       -- 1062（并发消费者或重放抢先入账）：ROLLBACK，本组退化为逐条处理
     按账户聚合增量（同一商户、同一平台子账户的多笔合并）
     按 account_id 升序：SELECT ... FOR UPDATE；UPDATE account SET balance = balance + Σ, version = version + 1
     按事件顺序计算每条分录的 balance_after（从加锁时读到的余额依次累加）
     批量 INSERT ledger_entry
   COMMIT
   ```

3. 故障点 `ledger.after_batch_commit`：任一分片组提交后、offset 提交前。
4. 所有分组提交成功后才提交该批 offset。部分分组已提交时崩溃，重新消费时第 2 步的预查会跳过已入账事件。
5. 反序列化失败的消息取不到 `merchant_id`，在分组前识别并直接按 MQ-03 进入 DLQ，不影响任何分组（R16）。组内事件不可重试（商户账户不存在）：本组回滚并退化为逐条处理，坏消息进入 DLQ，其余事件正常入账，`pay_ledger_batch_fallback_total + 1`。
6. 入账事件不依赖顺序（MQ-02 第 4 条），批量合并不改变正确性；`balance_after` 仍逐条正确，ACC-10 核对不需要特殊处理。

验收：

- [ ] 1000 条 `payment.succeeded`（含 100 条重复、2 条坏消息）：凭证恰好 898 张，坏消息进入 DLQ，`CheckBalance` 通过。
- [ ] 在 `ledger.after_batch_commit` kill account，重启后不重复入账（C12）。
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

1. 配置来自环境变量或参数：网关地址、`app_id`、`app_secret`、管理 Token。
2. 子命令：`create`、`pay`、`query`、`close`、`refund`、`query-refund`、`transfer`、`query-transfer`、`balance`；`admin recon-run`、`admin replay`、`admin check`。
3. 自动生成 nonce 和时间戳并签名；校验响应签名，失败时报错。
4. 输出 JSON；返回码 ≠ 0 时进程退出码为 1。

验收：

- [ ] 用 paycli 完成“下单 → 支付 → 查询 → 退款 → 查询退款 → 同分片转账 → 跨分片转账 → 查询转账”。

#### TL-02 merchant-demo

`P0 · M5 · 依赖：BASE-13`

规则：

1. `POST /notify` 验证平台签名，失败返回 401；成功返回 `SUCCESS`。
2. 环境变量 `FAIL_FIRST_N` 控制每个单号前 N 次通知返回 500。
3. `GET /stats?biz_no=` 返回收到的通知次数和最后一次内容（内存保存）。

验收：

- [ ] 配合 NTF-02 的重试测试使用。

#### TL-03 一键演示脚本

`P1 · M8 · 依赖：全链路`

规则：`scripts/demo.sh` 依次演示下单、支付、回调、入账、退款、同分片转账、跨分片转账、余额查询、余额核对、当天对账，每一步打印关键结果。

验收：

- [ ] 在 `make up` 后执行成功，全程 < 1 分钟。

#### TL-04 OpenAPI 文档

`P1 · M8 · 依赖：GW-02`

规则：`docs/api/openapi.yaml` 覆盖 `docs/05-api.md` §4 全部商户接口，包含签名说明与错误码。

验收：

- [ ] 通过 OpenAPI 3.1 校验工具检查。

### 4.14 质量保证与交付（QA、DOC）

#### QA-01 故障注入与演练

`P0 · M7 · 依赖：全链路`

规则：

1. `pkg/fault.Point(ctx, name)`：当环境变量 `FAULT_POINTS` 包含 `name` 时，向自身发送 SIGKILL 模拟 `kill -9`。`name:once` 表示只触发一次（在挂载目录 `/var/run/fault` 写标记文件，重启后不再触发）。默认构建中该函数什么都不做。
2. 故障点：`pay.after_tx1`、`callback.after_commit`、`outbox.after_send`、`ledger.after_commit`、`ledger.after_batch_commit`、`refund.after_freeze`、`transfer.after_try`、`transfer.after_decision`、`transfer.after_payer_confirm`。
3. 演练场景（`test/chaos/`，通过 `docker-compose.chaos.yml` 启动）：

   | # | 场景 | 方式 | 期望 |
   |---|---|---|---|
   | C1 | 渠道已扣款、回调处理前 payment 崩溃 | SLOW 场景下单，5 秒后 `docker kill -s KILL payment`，40 秒后启动 | 回调重试或查单使订单 SUCCESS，只入账一次 |
   | C2 | 订单 PAYING 后、调渠道前崩溃 | `pay.after_tx1` | 查单 NOT_FOUND → 关单 → CLOSED，无入账 |
   | C3 | 回调事务提交后、响应渠道前崩溃 | `callback.after_commit` | 渠道重试被识别为重复，事件只有 1 条 |
   | C4 | 投递 Kafka 成功、outbox 提交前崩溃 | `outbox.after_send` | 消息重复，只入账一次 |
   | C5 | 入账提交后、offset 提交前崩溃 | `ledger.after_commit` | 重复消费，只入账一次 |
   | C6 | 退款冻结后、置为 PROCESSING 前崩溃 | `refund.after_freeze` | 卡单恢复完成退款，冻结单只有 1 张 |
   | C7 | 压测中 Kafka 停止 2 分钟 | `docker stop kafka` | outbox 积压，恢复后全部投递 |
   | C8 | MySQL 重启；Redis 停止 | `docker restart mysql`、`docker stop redis` | 服务自动重连；Redis 停止期间商户接口 503、回调正常 |
   | C9 | 跨分片转账两个 Try 成功、决策落盘前崩溃 | `transfer.after_try` | 恢复任务取消：冻结 RELEASED、收款分支 CANCELLED、转账 FAILED，无凭证 |
   | C10 | 决策 CONFIRMING 落盘后、发起 Confirm 前崩溃 | `transfer.after_decision` | 恢复任务确认：转账 SUCCESS，`TRANSFER_OUT`、`TRANSFER_IN` 各 1 张 |
   | C11 | 付款方已确认、收款方未确认时崩溃（串行模式） | `transfer.after_payer_confirm` | 崩溃期间在途合计 = T；恢复后 SUCCESS，在途合计 0 |
   | C12 | 批量入账某分片组提交后、offset 提交前崩溃 | `ledger.after_batch_commit` | 重复消费被预查跳过，每笔支付只入账一次 |
   | C13 | 跨分片转账压测中 kill account 并在 20 秒后启动 | `docker kill -s KILL account` | 全部转账最终终态，在途合计 0，`CheckBalance` 通过 |

4. 每个场景结束后执行 `test/chaos/verify.sh`：
   - 不存在超过阈值仍为 PROCESSING 的 `channel_txn`（除 NEVER_FINISH 场景），不存在超过阈值仍未终态的 `transfer_order`。
   - 每个 SUCCESS 支付单恰好 1 张 PAY 凭证，每个 SUCCESS 退款单恰好 1 张 REFUND 凭证，转账单满足不变量 12。
   - 全量 `CheckBalance(expect_quiescent = true)` 通过，含两个分片在途合计为 0。
   - 对当天执行一次对账，差错数为 0（场景预期的差错除外）。

验收（A5、A11）：

- [ ] C1–C13 全部通过 verify，结果记录到 `docs/chaos-report.md`。

#### QA-02 压测与报告

`P0 · M7 · 依赖：全链路、OPS-03`

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

3. 两个阶段：M6 在全部优化开关关闭时跑基线（含 OPT-01 分阶段耗时）；M7 按 OPT-02 ~ OPT-04 逐项打开后重跑对应场景，最后全部打开跑 S7。
4. 每个场景至少持续 5 分钟，先预热 1 分钟。
5. 记录：QPS / TPS、P50/P95/P99、错误率、分阶段耗时、各进程 CPU 与内存、MySQL 慢查询与行锁等待（`performance_schema.data_lock_waits`）、outbox 积压、消费延迟。
6. 报告 `docs/perf-report.md`：环境、场景、基线结果、每项优化的前后对比、瓶颈分析（指出证据）、与 NFR-PERF 目标的对比。
7. 压测结束后执行 `verify.sh`，资金核对必须通过。

验收（A8）：

- [ ] 报告包含上述全部内容，基线和每项优化的结果可按脚本与开关复现。

#### QA-03 pprof 调优记录

`P0 · M7 · 依赖：QA-02`

规则：

1. 至少完成一次基于 pprof（CPU、alloc、block 或 mutex）的真实优化。
2. 候选方向（以实测为准）：JSON 编解码与日志分配、签名计算的内存分配、记账事务中的 SQL 拼装、mutex / block profile 中的锁竞争。行锁、批量提交、延迟结构类优化归入 OPT-02 ~ OPT-04。
3. `docs/pprof-tuning.md` 记录格式：现象 → profile / 指标证据 → 假设 → 改动 → 前后数据 → 结论。

验收（A9）：

- [ ] 记录中附优化前后的 profile 截图或 top 输出，以及同一压测场景的前后数据。

#### QA-04 验收运行

`P0 · M8 · 依赖：全部 P0`

规则：

1. 在新克隆目录执行 `make up`，验证 A1。
2. `make cover` 输出 `service/account`、`service/payment`、`pkg/dbx`（幂等与事务）、`pkg/outbox` 的覆盖率，均 ≥ 70%（A10）。
3. 按 §7 逐条执行验收，结果记录到 `docs/acceptance-report.md`。

验收：

- [ ] A1–A12 全部通过。

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
| NFR-COR-06 | 任意单点崩溃后，补偿和对账完成时资金零差错 | QA-01 C1–C13 + `verify.sh` |
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
| NFR-CON-07 | 转账卡在非终态（account 已恢复）→ 终态 | ≤ 30 秒 |
| NFR-CON-05 | D 日对账结果产出 | D+1 日 10:30 前 |
| NFR-CON-06 | Kafka 恢复 → outbox 积压清空 | ≤ 积压时长 + 1 分钟（积压 ≤ 10 万条时） |

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
| NFR-SEC-08 | 输入校验：请求体 ≤ 64KB，字段长度与格式校验，SQL 全部参数化 | GW-01、GW-02、BASE-08 |
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
| A3 | 重复回调只入账一次 | PAY-04（订单层）、MQ-04（账务层） | 两层各自的集成测试 + 端到端 DUP_NOTIFY |
| A4 | 回调丢失时补偿修正订单 | PAY-05、CH-06 | DROP_NOTIFY 集成测试 |
| A5 | 崩溃后资金零差错 | QA-01、PAY-05、REF-04、MQ-02、MQ-04、REC-* | `docs/chaos-report.md` C1–C8、C12 |
| A6 | 识别四类差错 | REC-03、CH-08、CH-09 | REC-03 构造账单测试 |
| A7 | 拒绝签名错误、时间戳过期、nonce 重复 | GW-03、GW-04、GW-06 | 网关单元测试 |
| A8 | 压测报告 | QA-02 | `docs/perf-report.md` |
| A9 | pprof 真实优化 | QA-03 | `docs/pprof-tuning.md` |
| A10 | 核心包覆盖率 ≥ 70% | 全部 P0 | `make cover` |
| A11 | 跨分片转账在并发与任意崩溃点下资金守恒 | ACC-04~06、ACC-10、ACC-14、TRF-03、TRF-04 | TRF-03 并发测试；`docs/chaos-report.md` C9–C11、C13 |
| A12 | 三条链路的性能优化有前后对比数据 | OPT-01 ~ OPT-04、GW-08 | `docs/perf-report.md` |

## 8. 待定事项与变更记录

### 8.1 待定事项

| # | 事项 | 默认处理 | 最晚决定 |
|---|---|---|---|
| Q1 | GitHub module path | 已定：`github.com/yym108/pay-system`（2026-10-13） | 已关闭 |
| Q2 | NFR-PERF 目标是否需要调整 | 首次基线压测后按数据调整 | M7 第一次压测后 |
| Q3 | 热点账户是否做缓冲入账（OPT-05） | 子账户拆分（OPT-03）后行锁仍是瓶颈且 P0、P1 已完成才做 | M8 |
| Q4 | `STATUS_MISMATCH`（本地已终态）的处理 | 已随 REC-07 砍掉，保持 PENDING 进入报告 | 已关闭 |

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
| 2026-10-17 | v3.0 | PAY-10 口径：商户费率历史表只追加；新生效时间必须严格晚于该商户已有最大 `effective_from`；相同时间相同费率幂等成功，相同时间不同费率 `10006`；不改 `merchant.fee_rate_bp`；订单手续费核对只报告，不改订单和账；扫描开始后新提交的费率下一轮才看得见。流水核对仍是 ACC-10。PAY-02 第 3 条改为按下单时刻已生效费率（`effective_from <= created_at` 的最近一档）计算并固化到 `fee_amount`，PAY-02 仍是 TODO。不新增错误码，不在 `cmd/payment/main.go` 启动核对循环。种子冲突更新写成 `merchant_id = merchant_fee_rate.merchant_id`，避免 `INSERT … SELECT` 未限定列名在 MySQL 8.4 报 1052 |
| 2026-10-15 | v3.0 | ACC-03 口径澄清：需求草稿里的 ROLLBACK 是调用方事务边界，Post 不执行 BEGIN、COMMIT、ROLLBACK，也不调用 `dbx.WithTx`。1062 不回滚调用方事务（MySQL 1062 不会中止 InnoDB 事务），用 `dbx.IsDuplicate` 判断后，在同一事务按 `(biz_type, biz_no)` SELECT 已有凭证，返回 `duplicated = true` 和已有 `txn_no`；即使这次的金额或分录和第一次不同，也返回第一次的结果，不插分录、不改余额；SELECT 不到则 `10003`。本连接查不到的 `account_id`（含只存在于另一分片的账户）返回 `40003`，不是 `40001`，因为本连接分不清“没有这个 id”和“在另一个库”；`GetAccount` 仍然用 `40001`。`biz_type = REVERSAL` 返回 `40003`，不写任何行：Post 没有 `reversal_of` 参数，表约束 `ck_ledger_txn_reversal` 要求 REVERSAL 必须有 `reversal_of`，冲正是 ACC-08，本功能点不加参数、不改表。不新增错误码，`40003` 对外英文仍是 `unbalanced entries`。验收 A2 的 1000 goroutine 不用 `dbx.WithTx`：`WithTx` 把单次事务限制在 3 秒，且包含等行锁的时间，1000 个等待者会被取消，成功和 `40002` 就数不成 500/500。该用例用 `BeginTxx(READ COMMITTED)`，父 ctx 至少 60 秒，Post 返回 nil 才 Commit，否则 Rollback。生产路径的单次记账仍由其他测试用 `dbx.WithTx` 覆盖。不改资金规则、接口、表结构和错误码 |
| 2026-10-19 | v3.0 | ACC-04 口径澄清：Freeze 是进程内方法。IDL 已有 Freeze，本功能点不实现 RPC、不改 IDL。`biz_type` 只允许 REFUND / TRANSFER；商户号分片不合法、`biz_no` 为空或超过 64 个码点、金额不是正的 int64 分，返回已有的 `10001`，不写行。冻结对象是 `shard.Of(merchant_id)` 上的 MERCHANT_SETTLE。`freeze_no` 在事务外 `NewNo(ctx, "FZ")`，`ErrClockRollback` 映射为已有的 `10004`。事务由 Freeze 自己 `dbx.WithTx` 提交，Post 仍不提交。UPDATE 同时 `version = version + 1`，与 Post 改余额对齐，不改变冻结金额，也不改 `balance`。影响 0 行时同一事务重读：非 ACTIVE 返回 `40005`（优先于余额不足），可用余额不够返回 `40002`，两者回滚且不留冻结单；重读后仍是 ACTIVE 且可用余额足够则返回 `10003`，不空转重试。1062 只用 `dbx.IsDuplicate`，不解析错误文本，同一事务按 `(biz_type, biz_no)` 读取：FROZEN / CONFIRMED 且金额和账户一致则返回已有结果且 `duplicated = true`，不增加冻结；RELEASED 一律 `40004`，不再比较金额；FROZEN / CONFIRMED 金额或账户不一致为 `10006`，旧行不变；读不到行是 `10003`。`duplicated` 只在进程内结果里，与 Post 一样，不改 RPC 字段。不新增错误码，不改资金规则、表结构和接口 |
| 2026-10-20 | v3.0 | ACC-05 口径澄清：ReleaseFreeze 是进程内方法。IDL 已有 ReleaseFreeze，本功能点不实现 RPC、不改 IDL。校验与 Freeze 相同，不合法返回已有的 `10001`，不发号、不写行；上下文已取消时参数不合法仍先返回 `10001`。参数合法后 nil service、nil ctx、nil 发号器、ShardDB 失败为 `10003`；已取消或已过截止时间的上下文原样返回，不发号。`freeze_no` 在事务外 `NewNo(ctx, "FZ")`，只给墓碑；`ErrClockRollback` 映射为已有的 `10004`。事务由 ReleaseFreeze 自己 `dbx.WithTx` 提交。空回滚插入 RELEASED 墓碑，不改 `balance` 和 `frozen_amount`；账户不存在 `40001`；`allow_overdraft` 不是 0 则 `10001`，不插入。已 RELEASED 幂等成功且不再比较金额。已 CONFIRMED 一律 `40004`。FROZEN 必须金额和账户一致，否则 `10006`。解冻不要求账户 ACTIVE，不返回 `40005`，不改 `balance`；冻结单和账户的 `version` 各加 1。账户更新 0 行则 `10003` 并回滚，冻结单保持 FROZEN，不改写成 `40002`。`duplicated` 只在进程内。不新增错误码，不改资金总规则，不实现 ACC-06 |
| 2026-10-20 | v3.0 | ACC-06 口径澄清：ConfirmFreeze 是进程内方法。IDL 已有 ConfirmFreeze，本功能点不实现 RPC、不改 IDL。请求没有金额，R 只用冻结单 `amount`。校验与 Release 一样先于 nil 判断，不合法返回已有的 `10001`。参数合法后 nil service、nil ctx、nil 发号器、nil pools、`subCount < 1`、ShardDB 失败为 `10003`；已取消或已过截止时间的上下文原样返回。本方法不调用 `NewNo`，`TX` 号仍由记账在锁账户之前生成，发号错误沿用记账映射。事务由 ConfirmFreeze 自己 `dbx.WithTx` 提交。先锁冻结单，再按账户 id 升序锁账户。不存在或 RELEASED 为 `40004`，不插入。CONFIRMED 且 `txn_no` 非空则幂等，不再比较 channel。FROZEN 但不是该商户结算账户为 `10006`，不返回 `40001`。本分片平台子账户不存在为 `40001`，不是 `40003`。商户腿用 `balance` 与 `frozen_amount` 同减的更新，不要求 ACTIVE，不返回 `40002`；平台非 ACTIVE 为 `40005`，回滚后冻结单保持 FROZEN。凭证已存在而冻结单仍是 FROZEN，或确认 CAS 影响 0 行，为 `10003` 并回滚，不当成幂等成功。REFUND 凭证借商户贷渠道应收；TRANSFER 冻结单仍是 TRANSFER，凭证是 TRANSFER_OUT，贷在途。`duplicated` 只在进程内。不新增错误码，不改 CHECK，不实现 ACC-07、ACC-14、ACC-11、ACC-10、MQ-04、OPT-03 |
