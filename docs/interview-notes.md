# 面试笔记

随手记录踩坑、数据和取舍。不替代需求与技术决策文档。

## BASE-09 配置

- caarlos0/env 在变量被显式设成空字符串、同时字段又有 `envDefault` 时，会改回默认值。`OTEL_EXPORTER_OTLP_ENDPOINT` 的空字符串表示关闭 trace，所以不能写 `envDefault`：键不在环境里才填 `jaeger:4317`，键在且值为空就保持关闭。`QUERY_BACKOFF` 也一样，显式空串是非法（至少一段时长），不能被默认计划悄悄盖住。
- 装配 DSN 不调用 `mysql.ParseDSN`。驱动解析 `loc=Asia/Shanghai` 会 `LoadLocation`，配置加载就会依赖 zoneinfo。这里只按 `user:password@tcp(host:port)/dbname?query` 切文本，不连接数据库。测试再用驱动解析装配结果，核对用户、库名和口令，失败信息里不打印口令。

## BASE-07 时钟

- 不要用 `time.LoadLocation("Asia/Shanghai")` 做业务时钟。运行镜像是没有 tzdata 的 Alpine，容器里这个调用会得到 `unknown time zone Asia/Shanghai`。中国自 1991 年起没有夏令时，`time.FixedZone("Asia/Shanghai", 8*60*60)` 的地点名和 +8 偏移都固定，不读 zoneinfo。
- 数据库 DSN 的 `loc=Asia/Shanghai` 仍会让驱动调用 `LoadLocation`。那条路径继续靠 `-tags timetzdata`。时钟本身不靠它，所以本功能点不改 Dockerfile。
- `FixedZone` 和 IANA 的 `Asia/Shanghai` 在当前日期都是 +8，只看现在的偏移分不出来。1991-07-01 的夏令时在 IANA 里是 +9，固定时区仍是 +8。测试用这个差别证明没有走 `LoadLocation`。
- idgen 继续用自己的窄时钟。发号要观察回拨和等待，和业务上注入的 `Clock` 不是同一次取时。

## BASE-06 错误码

- `Error()` 不带底层原因。`slog` 和 `fmt` 的默认打印走 `Error()`，把 SQL 或连接串拼进去就会漏到日志的 msg，甚至被当成对外文案。原因只放在 `Unwrap`，排查时显式取。
- 不在本包起 Kitex 服务。服务端就是 `kerrors.NewBizStatusError`，客户端就是 `kerrors.FromBizStatusError`，测试直接打这两个函数和 `*kerrors.BizStatusError`。对端 msg 即使夹带内部句子，还原后仍用约定表里的稳定英文。
- `30002` 不设哨兵。未登记码进 `10003`，避免调用方 `errors.Is` 到一个“保留错误”。`nil` 是成功码 `0`，不能走这条未知错误路径。
- `10001` 的 `HTTPStatus` 只记 400。请求体超过 64KB 返回 413 发生在网关读 body 之前，业务包看不到那次请求，所以不另造码。

## BASE-04 金额

- 手续费不用浮点。`(amount × rateBP + 5000) / 10000` 对非负金额是四舍五入，正好 0.5 才进位。10000 分 × 60bp 的真值是 60.0 分，加 5000 后整除仍是 60，不是 61。
- `amount × rateBP` 在 `MaxInt64` 附近会超出 int64。按 10000 拆成商和余数再乘，两部分都落在 int64 里，不靠回绕后的结果。
- 溢出在相加前比较边界，不用回绕后的符号判断。`MinInt64` 不能取相反数；减法上界是 `MaxInt64+b`，b 为 `MinInt64` 时这个和是 -1，比较本身不溢出。
- 越界不进错误码表。BASE-06 还没做，本包返回普通 `error`，调用方以后再映射。

## BASE-05 ID 生成

- 回拨必须看墙上时间。`time.Now()` 同时带单调钟，NTP 把墙上时钟往回拨时，`Before` 仍认为时间在前进，雪花会发出更小的时间戳。比较前用 `Round(0)` 去掉单调读数。
- 验收要求回拨 3ms 的真实耗时 ≥ 3ms。测试时钟是“下一次读数等于上一次读数减 3ms，然后跟着真实时间走”。如果只等到当前毫秒桶的起点，等待会短于 3ms，断言会抖。超过 5ms 直接返回，不睡这 10ms。
- 本包不返回 `10004`。数字码留给以后的服务把 `ErrClockRollback` 映射成已有的 `UNAVAILABLE`，避免在这里提前做 `pkg/errno`。

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
