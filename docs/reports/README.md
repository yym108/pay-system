# 功能点报告索引

每个功能点的代码提交后出具一份报告，讲解技术原理和已完成内容，不需要先读其他文档。格式见 `.opencode/skills/pay-feature-dev/report-template.md`。

| 功能点 | 名称 | 所属链路 | 代码提交 | 日期 | 报告 |
|---|---|---|---|---|---|
| BASE-01 | 工程骨架与构建 | 基础设施 | `85f103d` | 2026-10-13 | [BASE-01-skeleton.md](BASE-01-skeleton.md) |
| BASE-02 | 本地环境编排 | 基础设施 | `df47a17` | 2026-10-14 | [BASE-02-local-env.md](BASE-02-local-env.md) |
| BASE-03 | 数据库迁移与种子数据 | 基础设施 | `28061d8` | 2026-10-14 | [BASE-03-migrate-seed.md](BASE-03-migrate-seed.md) |
| BASE-04 | 金额工具 | 基础设施 | `5df4729` | 2026-10-14 | [BASE-04-money.md](BASE-04-money.md) |
| BASE-05 | ID 生成 | 基础设施 | `795cc26` | 2026-10-14 | [BASE-05-idgen.md](BASE-05-idgen.md) |
| BASE-06 | 错误码 | 基础设施 | `d81c442` | 2026-10-15 | [BASE-06-errno.md](BASE-06-errno.md) |
| BASE-07 | 时钟 | 基础设施 | `15e2022` | 2026-10-15 | [BASE-07-clock.md](BASE-07-clock.md) |
| BASE-09 | 配置 | 基础设施 | `cb968f5` | 2026-10-15 | [BASE-09-config.md](BASE-09-config.md) |
| BASE-10 | 日志与脱敏 | 基础设施 | `5d83baf` | 2026-10-15 | [BASE-10-logx.md](BASE-10-logx.md) |
| BASE-08 | 数据库访问与事务 | 基础设施 | `f535462` | 2026-10-15 | [BASE-08-dbx.md](BASE-08-dbx.md) |
| BASE-11 | 集成测试基座 | 基础设施 | `2abe147` | 2026-10-15 | [BASE-11-integration.md](BASE-11-integration.md) |
| BASE-12 | IDL 与代码生成 | 基础设施 | `6763c71` | 2026-10-15 | [BASE-12-idl.md](BASE-12-idl.md) |
| ACC-13 | 账户分片路由 | 基础设施 | `a8be4d2` | 2026-10-15 | [ACC-13-shard.md](ACC-13-shard.md) |
| ACC-01 | 开户 | 基础设施 | `1b2a54b` | 2026-10-15 | [ACC-01-open-account.md](ACC-01-open-account.md) |
| ACC-02 | 账户查询 | 基础设施 | `b72e591` | 2026-10-15 | [ACC-02-get-account.md](ACC-02-get-account.md) |
| ACC-03 | 通用记账 | 基础设施 | `31fff62` | 2026-10-15 | [ACC-03-ledger-post.md](ACC-03-ledger-post.md) |
| PAY-10 | 商户费率变更与订单手续费核对 | 支付 | `9587a08` | 2026-10-19 | [PAY-10-fee-rate.md](PAY-10-fee-rate.md) |
| ACC-04 | 冻结 | 基础设施 | `747a27a` | 2026-10-20 | [ACC-04-freeze.md](ACC-04-freeze.md) |
| ACC-05 | 解冻 | 基础设施 | `1db50ae` | 2026-10-20 | [ACC-05-release.md](ACC-05-release.md) |
| ACC-06 | 确认扣款 | 基础设施 | `0608708` | 2026-10-20 | [ACC-06-confirm.md](ACC-06-confirm.md) |
| ACC-07 | 同分片转账 | 基础设施 | `7138c9f` | 2026-10-21 | [ACC-07-transfer-local.md](ACC-07-transfer-local.md) |
| ACC-14 | 收款方转账分支 | 基础设施 | `3cfa46b` | 2026-10-21 | [ACC-14-transfer-in.md](ACC-14-transfer-in.md) |
| ACC-10 | 余额核对 | 基础设施 | `db8c0db` | 2026-10-21 | [ACC-10-check-balance.md](ACC-10-check-balance.md) |
| ACC-11 | account RPC 服务 | 基础设施 | `5c7e33a` | 2026-10-22 | [ACC-11-account-rpc.md](ACC-11-account-rpc.md) |
| BASE-13 | 签名库 | 基础设施 | `46e1cc4` | 2026-10-23 | [BASE-13-sign.md](BASE-13-sign.md) |
| BASE-14 | 密钥加密 | 基础设施 | `20a5de7` | 2026-10-23 | [BASE-14-secretbox.md](BASE-14-secretbox.md) |
| CH-01 | 渠道支付受理与自动完成 | 支付 | `a3433af` | 2026-10-23 | [CH-01-mock-pay.md](CH-01-mock-pay.md) |
| CH-02 | 渠道查单 | 支付 | `6eec88d` | 2026-10-26 | [CH-02-query.md](CH-02-query.md) |
| CH-03 | 渠道关单 | 支付 | `ffbecce` | 2026-10-26 | [CH-03-close.md](CH-03-close.md) |
| CH-05 | 渠道异步回调 | 支付 | `9ef8bff` | 2026-10-26 | [CH-05-notify.md](CH-05-notify.md) |
| CH-06 | 确定性故障场景（M3） | 支付 | `86049ab` | 2026-10-27 | [CH-06-scenarios.md](CH-06-scenarios.md) |
| PAY-01 | 商户与密钥 | 支付 | `8a0cf99` | 2026-10-27 | [PAY-01-merchant-credential.md](PAY-01-merchant-credential.md) |
| BASE-15 | 故障注入点 | 基础设施 | `9de0678` | 2026-10-28 | [BASE-15-fault.md](BASE-15-fault.md) |
| PAY-02 | 创建支付单 | 支付 | `8934e12` | 2026-10-28 | [PAY-02-create.md](PAY-02-create.md) |
| PAY-03 | 发起支付 | 支付 | `7a5e19f` | 2026-10-28 | [PAY-03-pay.md](PAY-03-pay.md) |
| MQ-01 | outbox 写入 | 支付 | `917c870` | 2026-11-01 | [MQ-01-outbox.md](MQ-01-outbox.md) |
| PAY-04 | 支付结果推进 | 支付 | `d0f4fe9` | 2026-11-01 | [PAY-04-apply-result.md](PAY-04-apply-result.md) |
| PAY-06 | 商户关单 | 支付 | `24b55db` | 2026-11-01 | [PAY-06-close.md](PAY-06-close.md) |
| PAY-08 | 支付单查询 | 支付 | `ddfe22c` | 2026-11-01 | [PAY-08-query.md](PAY-08-query.md) |
| OPS-01 | 健康检查与优雅退出 | 基础设施 | `7f9b2f6` | 2026-11-01 | [OPS-01-health.md](OPS-01-health.md) |
| OPS-05 | pprof 与管理端口 | 基础设施 | `40792e8` | 2026-11-01 | [OPS-05-pprof.md](OPS-05-pprof.md) |
| TL-01 | paycli | 基础设施 | `0f18bd6` | 2026-11-01 | [TL-01-paycli.md](TL-01-paycli.md) |
| GW-01 | 网关骨架 | 基础设施 | `97b776d` | 2026-11-01 | [GW-01-gateway-skeleton.md](GW-01-gateway-skeleton.md) |
