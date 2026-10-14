# 面试笔记

随手记录踩坑、数据和取舍。不替代需求与技术决策文档。

## BASE-12 IDL

- 生成命令必须写死 `kitex@v0.16.3`。`go run pkg@version` 不会把生成器写进业务 `go.mod`；真正进来的是生成代码 import 的 `github.com/cloudwego/gopkg v0.2.0`，版本跟 kitex v0.16.3 的 go.mod 走，不能顺手升。
- 第一版必填字段不要写 Thrift 的 `required` 关键字，用默认 requiredness。`required` 一旦写上，缺字段就解码失败，以后也不能改成 optional。文档写可省略的才标 `optional`。
- 状态用 string，不用 Thrift enum。enum 在线上是数字，和数据库里的 `ACTIVE`、`SUCCESS` 对不上。
- `Post` 不要出现在 AccountService 里。它是进程内记账，确认冻结和转账在本进程调用；做成 RPC 就会在事务里跨网络。
- `CreateRefund`、`CreateTransfer` 的 `40002`（退款还有 `30006`）必须连同单据返回。Kitex 的 `BizStatusError` 带不了响应体，所以这两支成功返回，把码放在 `biz_code`。别的业务错误仍然走 `BizStatusError`。

## BASE-08 数据库访问

- `transaction_isolation` 的值必须自己带单引号。驱动连接后执行 `SET transaction_isolation = 'READ-COMMITTED'`，引号是值的一部分。不带引号时 MySQL 会把它当成标识符。`tx_isolation` 如果留在参数里，和 `transaction_isolation` 一起进 map，`SET` 的顺序不固定，隔离级别可能被改回去。强制参数时把这个旧名字删掉。
- 死锁牺牲者不是抛硬币。InnoDB 回滚权重更小的事务。对侧先多更新几行（结束时仍回滚），`WithTx` 更容易变成牺牲者。回调在重试时不能再等已经 close 过的 channel，否则第二次执行会卡死。2026-10-15 本机这一轮交叉 `FOR UPDATE` 在 0.05 秒内看到 1213 并提交了两行的最终值。
- `BeginTxx` 的 ctx 到期后，`database/sql` 会在旁边把事务回滚。回调看不到那个派生 ctx，超时后仍可能返回 nil。接着 `Commit` 会失败，不能把它当成提交成功。回滚错误也不要盖住这次失败。

## BASE-10 日志

- slog 的 `ReplaceAttr` 不会在分组键上调用。字段名是 `password`，值如果先 `Resolve` 成 group，子字段只按自己的名字处理，普通字符串就漏出去。整段替换要在 `Resolve` 之前做完。
- 兜底扫描不能打在 `trace_id`、`span_id`、`request_id` 上。span id 是 16 位十六进制，全是数字时正好落进 13–19 位卡号规则，改写之后链路对不上。这三个字段按 otel 或 `WithRequestID` 的原值写。
- hertz v0.10.6 的 go.mod 依赖 sonic v1.15.0。只 import `hlog` 时，tidy 不会把 sonic 拉回去：根模块已经显式 require v1.15.4，`tools/tools.go` 还引用着它。kitex 保持 v0.16.3。trace API 是独立模块 `go.opentelemetry.io/otel/trace`，不要为了“用一下 trace”把 `otel/sdk` 加进来。

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
