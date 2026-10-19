---
description: 由 lead 编排、coder 子代理实现一个功能点，例如 /feature ACC-03
agent: lead
---

按你（lead）的“功能点流程”推进功能点 $ARGUMENTS：你负责领取、建分支、核验、审查编排、汇报和 Git；代码、测试、文档同步与报告全部交给 `coder` 子代理，它按 `pay-feature-dev` 技能执行。

- 未提供功能点编号时，从 `docs/02-requirements.md` §3 中找出依赖已全部 DONE、按里程碑顺序最靠前的 TODO 功能点，告诉我编号和理由，等我确认后再开始。
- 提供了多个编号时，只做第一个，完成后列出剩余编号。
- 该功能点做完后按 `pay-feature-dev` 技能 §10 立即提交代码，再提交报告。不要询问是否提交，也不要留下未跟踪的已完成文件。
