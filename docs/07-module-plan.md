# 项目模块规划

> 版本：v1.0　最后修改：2026-10-13　状态：已确认（2026-10-13）
> 用途：一页纸确认整体布局。规则以 `docs/02-requirements.md` v3.0 为准，本文只做归纳；§4 包内划分已并入需求 §2.3。

## 1. 总体布局

```
                     商户 / paycli / k6                      channel-mock（模拟渠道）
                            │ HTTP + HMAC                       ▲ 受理/查单/退款   │ 回调
                            ▼                                   │                  ▼
 ┌──────────────────────────────────────── gateway（Hertz）─────────────────────────────┐
 │ 验签 → 商户限流 → nonce（Redis）→ 路由          回调入口            管理接口（3 个）  │
 └───────────────┬───────────────────────────────────┬──────────────────────┬───────────┘
                 │ Kitex                             │ Kitex                │ Kitex
                 ▼                                   ▼                      ▼
 ┌──────────── payment ────────────┐  Kitex  ┌──────── account ────────┐  ┌── recon ──┐
 │ 支付单 / 退款单 / 转账单（协调者）│────────►│ 分片路由 → shard 0 / 1   │  │ 对账批次  │
 │ outbox 投递、补偿、恢复、通知     │         │ 记账 / 冻结 / 转账分支   │  │ 差错处理  │
 └──────┬───────────────▲──────────┘         │ 入账消费（逐条 / 批量）  │  └─────┬─────┘
        │ outbox        │ 通知事件            └───────────▲─────────────┘        │ 只读
        ▼               │                                 │                      ▼
     Kafka ─────────────┴─────────────────────────────────┘          pay_payment、pay_account_0/1
```

存储：`pay_payment`（payment）、`pay_account_0` / `pay_account_1`（account，两个分片）、`pay_recon`（recon）、`pay_channel_mock`（channel-mock）。一个服务只写自己的 schema。

## 2. 模块清单

按投入分三档：核心（项目卖点，做深）、支撑（三条链路跑通必需，做完整但不扩展）、简化（只做最小可用）。

| 模块 | 档位 | 职责 | 代码位置 | 功能点 | 周 |
|---|---|---|---|---|---|
| 账户与记账 | 核心 | 分片路由、开户、复式记账、冻结三件套、同分片转账、收款方分支、余额核对 | `service/account/`、`pkg/shard` | ACC-01~07、10、11、13、14 | 1–2 |
| 支付 | 核心 | 支付单状态机、发起支付、回调与查单推进、关单、补偿 | `service/payment/` | PAY-01~09 | 3–4 |
| 退款 | 核心 | 额度占用、同步冻结、渠道退款、结果推进、卡单恢复 | `service/payment/` | REF-01~05 | 4 |
| 转账 | 核心 | 转账单、同分片执行、跨分片 TCC 协调、恢复任务 | `service/payment/` | TRF-01~05 | 4 |
| 消息与入账 | 核心 | outbox 写入与投递、Kafka 框架、account 入账消费、死信与重放 | `pkg/outbox`、`pkg/mq`、`service/*/consumer/` | MQ-01~06 | 3、5 |
| 性能优化 | 核心 | 分阶段埋点、批量记账、子账户对比、链路延迟优化 | 分布在各服务；埋点 `pkg/obs` | OPT-01~05 | 5–7 |
| 质量与证据 | 核心 | 故障注入与 C1–C13 演练、压测 S1–S7、pprof 调优、验收 | `test/`、`pkg/fault` | QA-01~04 | 6–8 |
| 基础设施 | 支撑 | 工程骨架、编排、迁移种子、公共包、IDL | 根目录、`pkg/`、`deploy/`、`migrations/`、`idl/` | BASE-01~14 | 1、3 |
| 模拟渠道 | 支撑 | 受理、查单、关单、退款、回调、故障场景、账单 | `service/channel_mock/` | CH-01~09 | 3–4、6 |
| 网关 | 支撑 | 统一响应、商户路由、验签、时间窗口与 nonce、回调入口、凭证缓存 | `gateway/` | GW-01~04、06、08 | 3、5、7 |
| 可观测 | 支撑 | 健康检查与优雅退出、链路追踪、指标、pprof | `pkg/obs`、`pkg/server` | OPS-01~03、05 | 3、5 |
| 商户通知 | 简化 | 消费事件生成通知、固定间隔重试 5 次 | `service/payment/notify/` | NTF-01~02 | 5 |
| 网关限流 / 管理 | 简化 | 进程内商户限流；只有重放、核对、触发对账 3 个管理接口 | `gateway/` | GW-05、GW-07 | 5–6 |
| 对账 | 简化 | 按单号外部比对、内部单据与凭证核对、两类差错自动处理、报告 | `job/recon/` | REC-01~03、05、06、08 | 6 |
| 工具与文档 | 简化 | paycli、merchant-demo、演示脚本、OpenAPI、README | `cmd/`、`scripts/`、`docs/` | TL-01~04、DOC-01 | 3、5、8 |

已砍掉：ACC-09、ACC-12、REC-04、REC-07、OPS-04；ACC-08 降为 P1，OPT-05 为 P2。

## 3. 三条链路经过的模块

| 链路 | gateway | payment | account | Kafka | channel-mock | recon | 通知 |
|---|---|---|---|---|---|---|---|
| 支付 | 下单、支付、回调入口 | 支付单 CAS、outbox、查单补偿 | 入账消费（逐条 / 批量），本分片平台子账户 | `payment.succeeded` | 受理、回调、查单 | 外部比对 + PAY 凭证核对 | 是 |
| 退款 | 退款接口 | 额度占用、调冻结、调渠道、卡单恢复 | 冻结（同步 RPC）、确认扣款 / 解冻（消费事件） | `refund.*` | 退款、回调、查单 | 外部比对 + REFUND 凭证核对 | 是 |
| 转账 | 转账接口 | 转账单、TCC 协调、恢复任务 | 同分片 TransferLocal；跨分片付款方冻结 + 收款方分支，经 TRANSIT | 仅 `transfer.*` 通知事件 | 不涉及 | 不变量 12 核对 | 是（付款方） |

## 4. 核心服务的包内划分（建议）

需求只规定到 `handler / biz / dal` 三层，以下是 `biz` 和后台任务的细分建议，开工时按此建目录：

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

pkg/shard                 Of(merchant_id)、Sub(biz_no)，payment 与 account 共用
pkg/obs                   metrics、trace、Stage(ctx, chain, stage) 分阶段埋点
pkg/fault                 故障点（含 transfer.after_try 等 4 个新故障点）
```

## 5. 依赖规则

1. 服务之间只通过 `kitex_gen` 和事件结构交互，禁止 import 其他服务的包（`depguard` 检查）。
2. `pkg/` 不依赖任何 `service/`、`gateway/`、`job/` 包。
3. account 的事务函数只能拿到一个分片的 `*sqlx.Tx`；跨分片只能由 payment 协调。
4. 事务内不调用 RPC、HTTP、MQ；需要发事件时同事务写 outbox。
5. 每个性能优化项都有开关，关闭即基线，两条路径共用同一组验收测试。

## 6. 确认结果（2026-10-13）

1. 布局：转账协调者放在 payment，不单独建 transfer 服务。好处是复用 outbox、补偿框架和通知；代价是 payment 职责变重。
2. 投入分档：按 §2 的核心 / 支撑 / 简化划分执行。
3. 包内划分：按 §4 执行，已并入需求 §2.3。
