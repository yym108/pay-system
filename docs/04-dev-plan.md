# 开发计划

> 最后更新：2026-10-15
> 周期 8 周。功能点的规则与验收见 `docs/02-requirements.md`（下称“需求”），本文只安排顺序和跟踪进度。
> 进度以需求 §3 功能点总表的“状态”列为准，本文的勾选只用于周内跟踪。
> 2026-10-12：按需求 v3.0 重排，重心放在支付、退款、转账三条链路的一致性与性能对比；对账、通知、网关、运维按简化范围安排。

## 1. 工作方式

按需求 §0.2 推进：一次一个功能点，依赖完成才开始；确认规则 → 写测试 → 实现 → 验证 → 更新状态。完成定义见需求 §0.3，具体编码流程见 `.opencode/skills/pay-feature-dev/SKILL.md`（`/feature <功能点编号>` 启动）。

补充约定：

- 重要取舍记入 `docs/01-tech-decisions.md`；规则变化先改需求文档并在需求 §8.2 记录。
- 面试素材（踩坑、数据、取舍）随手记入 `docs/interview-notes.md`。
- 每个功能点提交后出具报告到 `docs/reports/`（需求 §0.2 第 6 条）。
- 每周最后半天留给回归：`make lint test test-integration`，并检查本周功能点的验收是否都有测试。
- 性能优化项从第一天起保留开关（需求 §4.12）：先写基线实现，优化只在开关打开时生效，方便第 6–7 周做前后对比。

## 2. 周计划

每周按列出的顺序推进，同一行内的功能点可以并行。

### 第 1 周（M1）：工程骨架、分片与记账核心

- [x] BASE-01 工程骨架
- [x] BASE-02 本地环境（基础设施与 kafka-init；应用与 migrate/seed 仍是 profile `apps` 占位）
- [x] BASE-03 迁移与种子（两个账户分片、平台子账户、3 个演示商户）
- [x] BASE-04 金额
- [x] BASE-05 ID
- [x] BASE-06 错误码
- [x] BASE-07 时钟
- [x] BASE-09 配置
- [x] BASE-10 日志
- [x] BASE-08 数据库访问
- [x] BASE-11 集成测试基座
- [x] BASE-12 IDL 与代码生成
- [x] ACC-13 分片路由
- [x] ACC-01 开户
- [ ] ACC-02 账户查询 → ACC-03 通用记账

交付：`make up && make migrate && make test-integration` 跑通；两个分片表结构一致；ACC-03 并发扣款用例通过（A2 的主体）。

### 第 2 周（M2）：冻结、转账分支与核对

- [ ] ACC-04 冻结 → ACC-05 解冻（空回滚、防悬挂）→ ACC-06 确认扣款（REFUND、TRANSFER 两种贷方）
- [ ] ACC-07 同分片转账（失败墓碑）、ACC-14 收款方分支（Try / Confirm / Cancel）
- [ ] ACC-10 余额核对（分片 + 在途守恒；之后所有资金类测试结束时调用）
- [ ] ACC-11 account RPC 服务

交付：A2 全部用例通过；ACC-14 的 Try / Cancel 并发用例通过；`service/account` 覆盖率 ≥ 70%。

### 第 3 周（M3）：支付主链路与模拟渠道

- [ ] BASE-13 签名库（`docs/05-api.md` §3.7 测试向量）、BASE-14 密钥加密
- [ ] CH-01 渠道受理 → CH-02 查单、CH-03 关单、CH-05 回调；CH-06 的 FAIL、DROP_NOTIFY、DUP_NOTIFY、NEVER_FINISH
- [ ] PAY-01 商户与密钥（含配置缓存开关）→ PAY-02 创建支付单 → PAY-03 发起支付
- [ ] MQ-01 outbox 写入 → PAY-04 支付结果推进 → PAY-06 商户关单、PAY-08 查询
- [ ] OPS-01 健康检查与优雅退出、OPS-05 管理端口
- [ ] GW-01 网关骨架 → GW-03 商户验签 → GW-02 支付路由、GW-06 渠道回调入口
- [ ] TL-01 paycli

交付：`paycli create → pay → query`，订单最终 SUCCESS，`channel_txn` 与订单一致，outbox 有 1 条事件；A3 订单层用例通过。

本周功能点最多，落后时 OPS-05 和 GW-03 可顺延到第 4 周前两天。

### 第 4 周（M4）：补偿、退款与转账

- [ ] PAY-05 查单补偿、PAY-07 超时关单
- [ ] CH-06 其余场景：SLOW、ACCEPT_TIMEOUT、BILL_MISSING、BILL_AMOUNT_DIFF（标记部分）、CLOSE_RACE；CH-04 渠道退款
- [ ] REF-01 创建退款 → REF-02 冻结与提交 → REF-03 结果推进 → REF-04 卡单恢复；REF-05 查询
- [ ] TRF-01 创建转账单 → TRF-02 同分片执行 → TRF-03 跨分片 TCC → TRF-04 恢复任务；TRF-05 查询
- [ ] GW-02 退款、转账路由

交付：A4；退款与转账主链路跑通（入账消费在第 5 周接入，本周退款测试中用直接调用 account biz 层代替消费）；TRF-03 跨分片并发用例通过（A11 主体）；`service/payment` 覆盖率 ≥ 70%。

本周是三条链路的核心周。落后时 TRF-04 的故障点用例（C9–C11）移到第 7 周和 QA-01 一起做，TRF-04 本身不顺延。

### 第 5 周（M5）：最终一致性、通知、网关安全、埋点

- [ ] MQ-03 Kafka 框架 → MQ-02 投递器 → MQ-04 account 消费（逐条模式）→ MQ-05 死信与重放
- [ ] NTF-01 通知记录（含转账事件）→ NTF-02 固定间隔重试；TL-02 merchant-demo
- [ ] GW-04 时间窗口与 nonce、GW-05 商户限流（进程内）
- [ ] OPS-02 链路追踪、OPS-03 指标 → OPT-01 分阶段耗时埋点

交付：A3 完整（订单层 + 账务层）、A7；一次跨分片转账在 Jaeger 中可见全部阶段；`pkg/outbox`、`pkg/dbx` 覆盖率 ≥ 70%。

### 第 6 周（M6）：简化对账与压测基线

- [ ] CH-08 账单下载 → CH-09 账单注入；CH-07 全局故障配置（压测延迟）
- [ ] REC-01 批次与调度 → REC-02 下载与导入 → REC-03 外部比对（按单号）
- [ ] REC-05 内部对账（支付、退款、转账）
- [ ] PAY-09 立即同步渠道状态 → REC-06 自动处理 → REC-08 报告；GW-07 管理接口（重放、核对、触发对账）
- [ ] QA-02 第一阶段：`make seed-load`、k6 场景 S1–S7、全部优化开关关闭跑基线，导出分阶段耗时

交付：A6；`docs/perf-report.md` 基线部分（6 条链路的分阶段耗时表），据此确定第 7 周的优化顺序。

### 第 7 周（M7）：性能优化对比、演练、调优

- [ ] OPT-02 批量记账合并提交（C12）
- [ ] OPT-03 子账户拆分对比（1 vs 8）
- [ ] OPT-04 链路延迟优化（含 GW-08 凭证缓存、转账分支并行），三条链路各至少 1 项
- [ ] QA-02 第二阶段：逐项打开开关重跑，最后全开跑 S7
- [ ] QA-01 故障注入与 C1–C13 演练 → QA-03 pprof 调优记录

交付：A5、A8、A9、A11、A12。

### 第 8 周（M8）：验收与收尾

- [ ] MQ-06 outbox 清理（P1）、TL-03 演示脚本（P1）、TL-04 OpenAPI（P1）、ACC-08 冲正（P1）
- [ ] OPT-05 缓冲入账（P2，仅在 P0、P1 全部完成且 OPT-03 后行锁仍是瓶颈时）
- [ ] QA-04 验收运行（A1、A10 及 A1–A12 复核）
- [ ] DOC-01 README 与面试材料（三条链路的数据交互、一致性设计、性能对比）

## 3. 风险与应对

| 风险 | 影响 | 应对 |
|---|---|---|
| Go 1.27 与 CloudWeGo 依赖兼容 | 运行时告警或性能回退 | 已验证 sonic v1.15.4 正常；如出现新问题，降级到 Go 1.26.9（镜像 `golang:1.26.9` 可用） |
| 第 3、4 周功能点过多 | 后续周连锁延期 | 按各周说明顺延 OPS-05、GW-03、C9–C11 用例；不压缩测试 |
| 进度落后 | 验收项无法完成 | 削减顺序：P2 → P1（TL-04、ACC-08 优先削减）→ OPT-04 中收益最小的候选项；三条链路的 P0 和 A1–A12 不削减 |
| 本机压测资源有限，优化效果被噪声淹没 | 前后对比不可信 | 同一场景至少跑 3 次取中位数；记录机器负载；对比时只切换一个开关 |
| 两个分片在同一 MySQL 实例 | 跨分片故障不够真实 | 用进程崩溃和 RPC 超时注入（C9–C13）；README 说明换成两个实例只需改 DSN |
| 本机 Docker 内存不足 | compose 启动失败或压测失真 | Kafka 堆 512MB、MySQL `innodb_buffer_pool_size=256M`；`make up-lite` 不启动可观测组件 |
| hertz-contrib OTel 中间件过旧 | 链路断开 | 自写 Hertz tracing 中间件 |
| testcontainers 启动慢 | 测试反馈慢 | 本地复用 `TEST_MYSQL_DSN`，每个测试包独立 schema |

## 4. 第一次开工会话清单

1. 确认 `docs/01-tech-decisions.md` 中的决策和需求 §8.1 的待定事项，有异议先改文档再写代码。
2. module path 已定为 `github.com/yym108/pay-system`，远程仓库 `git@github.com:yym108/pay-system.git`。
3. 安装工具（版本固定）：

   ```bash
   go install github.com/cloudwego/kitex/tool/cmd/kitex@v0.16.3
   go install github.com/cloudwego/thriftgo@v0.4.5
   # golangci-lint v2.14.0：按官方文档安装预编译二进制
   ```

4. 按顺序完成 BASE-01、BASE-02、BASE-03，确认 `make up && make migrate` 在本机通过，两个账户分片都已建表。
5. 从 BASE-04、BASE-05 开始，按“测试 → 实现”推进。
