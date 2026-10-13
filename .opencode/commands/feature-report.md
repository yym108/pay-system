---
description: 为已提交的功能点出具或重写报告，例如 /feature-report ACC-03
---

加载 `pay-feature-dev` 技能，按其 §11 和 `.opencode/skills/pay-feature-dev/report-template.md` 为功能点 $ARGUMENTS 出具报告。

- 用 `git log --grep '\[<功能点>'` 找出该功能点的全部代码提交；找不到提交时停下，告诉我该功能点还没有提交。
- 报告已存在时，保留原有内容的结构，按最新代码和测试结果更新，并在“修订记录”追加一行。
- 验收对照与验证记录必须重新运行对应命令获得，不沿用旧报告中的数据。
- 写完后先给我报告路径和自检结果，等我确认再提交报告。
