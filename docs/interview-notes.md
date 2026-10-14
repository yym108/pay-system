# 面试笔记

随手记录踩坑、数据和取舍。不替代需求与技术决策文档。

## BASE-03 迁移与种子

- 不能用 shell 的 `.` 去读 `.env.example`。DSN 里的 `tcp(mysql:3306)` 会被当成子 shell，脚本在建库前就失败。按行 `export KEY=VALUE` 才不会解释括号。
- 官方 MySQL 镜像初始化时会先起一个临时服务器，日志也写 `ready for connections`，但端口是 0。测试要等 `port: 3306`，否则第一次连接是 unexpected EOF。
- `migrate/migrate:v4.20.1` 的 `down` 不带 `-all` 会在非交互环境等待确认并失败。回滚必须写 `down -all`。
- 种子不能等 BASE-14。密文格式已经写在 BASE-14 的规则里，种子用标准库按这个格式加密，`pkg/secretbox` 以后必须能解开。列保持 NOT NULL，没有改成可空。
- 重复种子不能 `ON DUPLICATE KEY UPDATE` 余额，也不能重写密文。nonce 是随机的，重写会让第二次执行改变密文；余额列一更新就会破坏“种子不写余额”。

## BASE-02 本地环境

- 空 `main` 不能被标成 healthy。健康检查属于 OPS-01，迁移和种子属于 BASE-03。BASE-02 把验收收成基础设施容器 healthy，加上 `kafka-init` 退出码 0；应用放在 profile `apps` 里占位。A1 仍要等后两个功能点。
- Kafka 4.3.1 单节点的默认副本数是 3，包括 share coordinator 的内部 topic。不显式改成 1，broker 起不来。这是镜像兼容约束，不是新组件。
- `docker compose --project-directory` 设为仓库根之后，bind mount 相对仓库根，不是 compose 文件目录。第一次 `make up` 因此把 Prometheus 配置挂到了不存在的路径。
- `docker compose ps` 模板里 Health 为空时，用空格分列会把 ExitCode 错位。`kafka-init` 已经成功创建 topic，等待脚本仍把它判失败。分隔符改成 `|`。
- 本机 Docker 29.4.0 / darwin-arm64，镜像已在本地时，`make up` 到 mysql、redis、kafka、prometheus、jaeger 全部 healthy 用时 14 秒（2026-10-13，`go test -race -tags integration -run TestBASE02Stack -v`）。
