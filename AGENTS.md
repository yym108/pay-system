# pay-system

- 编写、修改、测试任何功能点的代码前，先加载 `pay-feature-dev` 技能（`.opencode/skills/pay-feature-dev/SKILL.md`）并按其流程执行；也可以用 `/feature <功能点编号>` 启动。
- 角色分工：默认主代理 `lead` 只编排、核验和执行 Git，不写代码；代码、测试、功能点文档同步和报告由 `coder` 子代理（Grok 4.7）完成，审查用 `reviewer`，报告可读性自检用 `report-checker`。定义见 `.opencode/agents/`。
- 文档基线：需求 `docs/02-requirements.md`，全局约定 `docs/03-conventions.md`，接口 `docs/05-api.md`，表结构 `docs/database.md`。代码与文档不一致时，先改文档再改代码。
- 功能点代码提交后，按 `.opencode/skills/pay-feature-dev/report-template.md` 出具报告到 `docs/reports/`，读者不读其他文档也能看懂；补写或重写用 `/feature-report <功能点编号>`。
- 资金硬规则：金额只用 int64 分；分录只追加不修改；写操作必须幂等；状态只用 CAS 更新；事务内不调用外部系统。
