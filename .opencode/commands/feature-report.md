---
description: 为已提交的功能点出具或重写报告，例如 /feature-report ACC-03
agent: lead
---

为功能点 $ARGUMENTS 出具报告：你（lead）负责编排和提交，报告由 `coder` 子代理按 `pay-feature-dev` 技能 §11 和 `.opencode/skills/pay-feature-dev/report-template.md` 编写。

- 先用 `git log --grep '\[<功能点>'` 找出该功能点的全部代码提交；找不到提交时停下，告诉我该功能点还没有提交。
- 派发 `coder` 时给出提交 hash 列表。报告已存在时，要求它保留原有结构，按最新代码和测试结果更新，并在“修订记录”追加一行。
- 验收对照与验证记录必须重新运行对应命令获得，不沿用旧报告中的数据。
- 资金链路和性能类功能点用 `report-checker` 做可读性自检，反馈交给 `coder` 修改。
- 写完并通过可读性自检后立即提交报告和 `docs/reports/README.md`，提交信息 `[<功能点>] add report`。不要等确认，也不要把报告留成未跟踪文件。提交有误时另起提交修改，或用 `git revert` 回退。
