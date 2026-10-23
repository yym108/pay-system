# 面试笔记

随手记录踩坑、数据和取舍。不替代需求与技术决策文档。

## BASE-14 密钥加密

- 先有密文、后有解密包。BASE-03 的种子已经按格式写进库里，BASE-14 的第一件事是在删掉种子自带实现之前，用旧代码生成固定的密文向量写进 `pkg/secretbox` 的测试。之后种子改用新包，“旧密文能解开”仍有独立的证据，不会变成自己加密自己解密的循环论证。
- AAD 看起来是免费的加固（绑定商户号，防止两行密文互换），但它不改变字节布局，却会让所有已有密文认证失败。这类“格式不变、语义变了”的改动只能配新版本号和回填一起做，不能顺手加。
- 篡改测试逐字节、每字节翻两个位。第 0 字节是版本号，改了先报“未知版本”而不是“认证失败”；测试按位置区分期望的哨兵。

## BASE-13 签名库

- 拼接串用换行分隔字段。字段本身含换行时，`ts="1\n2", nonce="3"` 和 `ts="1", nonce="2\n3"` 的拼接串相同，同一个签名能套到另一组头上。HTTP 头本来不能带换行，本包仍在签名和验签两侧拒绝含换行的字段；body 在最后（请求、平台签名里还先取摘要），不受影响。
- `base64.StdEncoding.Strict()` 仍会跳过 `\r\n`。只靠解码会接受嵌了换行的签名，所以先要求长度正好 44，再要求解码后正好 32 字节。Strict 还拒绝填充位不为 0 的写法，一个 MAC 只对应一个合法字符串。
- 测试里把签名整串转小写，本想得到“格式合法但不一致”，结果末字符 `Q` 变 `q` 让填充位非 0，变成了格式错误。改成只翻转首字符。
- 先比较格式再算 HMAC 不泄露密钥：格式检查只看攻击者自己给的输入。真正比较 MAC 用 `hmac.Equal`，测试扫描源码确认没有 `bytes.Equal` 或 `==`。
- 1KB body 的 `VerifyRequest` 在 Apple M5 上约 1.5µs/op、808 B/op、17 allocs/op（`go test -bench BenchmarkVerifyRequest -benchmem`，2026-10-23）。pprof `alloc_objects`：`io.WriteString` 把字符串写进哈希约占 39%，`hmac.New`（含内部两个 sha256）约 31%。是否优化留给 QA-03（pprof 调优记录，候选方向已列出“签名计算的内存分配”）按压测 profile 决定。

## ACC-10 余额核对

- 两个分片是两次独立快照，不是同一时刻。进行中的跨分片转账会让两边在途加总暂时不为 0，所以在途合计为 0 只在全量且 `expect_quiescent` 时才查。
- 这次扫描是 REPEATABLE READ 一致性快照：`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` 再 `START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT`。`dbx.WithTx` 是 READ COMMITTED，驱动的 `BeginTx(ReadOnly)` 也不会带上 `WITH CONSISTENT SNAPSHOT`。
- 最后一条分录按 `ledger_entry.id` 的 `max(id)` 取 `balance_after`，不是 `created_at`。同一秒里后写入的分录 id 更大。

## ACC-14 收款方转账分支

- Confirm 先 `SELECT ... FOR UPDATE` 锁 PAYEE 分支，再把账户锁交给公开 `Post`，按账户 id 升序。先锁账户再锁分支，会和只按 id 加锁的记账死锁。1062 不中止 InnoDB：Try 和 Cancel 都要让 `WithTx` 先回滚，再在事务外读已有行。Cancel 读到匹配的 TRIED 后另开事务做 CAS，不在未回滚的事务里继续写。
- 在途账户允许透支。本分片对应 TRANSIT 子账户余额是 0 也能确认，确认后变成 −T。正常行不会因为在途不足返回 `40002`。若有人把在途改成不许透支且不够扣，Post 仍返回 `40002`，原样交回，分支保持 TRIED。
- Cancel 的空回滚不查账户，账户不存在也插入 CANCELLED 墓碑。迟到的 Try 在账户已是 ACTIVE 时一律 `40004`，不再比较金额和收款商户。Try 的账户检查在插入之前，所以墓碑还在、账户尚未建好时，Try 先返回 `40001`。

## ACC-07 同分片转账

- 普通借方条件会扣掉冻结。公开 `Post` 的可用余额是 `balance - frozen_amount`，全额冻结时可用是 0，钱转不走，也不会把冻结一起扣掉。账户按 id 升序加锁是 Post 已有行为，A→B 与 B→A 不会因为锁顺序死锁。
- 失败墓碑是另一个事务。事务 1 因 `40002` 回滚之后，事务 2 才插入 `FAILED` / `INSUFFICIENT_BALANCE`。之后给付款方加钱，同一 `transfer_no` 仍然 FAILED，不再看当前余额。
- 1062 不中止 InnoDB 事务。必须让回调返回错误，由 `WithTx` 回滚，再在事务外读已有 LOCAL 分支。未回滚就继续记账，第二次的分录会留在这个还没结束的事务里。

## ACC-06 确认扣款

- 普通扣款条件不能用于确认。`updateBalanceGuardSQL` 要求 `status = 'ACTIVE'` 并且 `balance - frozen_amount >= R`，而且不改 `frozen_amount`。全额冻结时可用余额是 0，这条会返回 `40002`，冻结也减不掉。确认的商户腿改成 `balance` 加上有符号 delta、`frozen_amount` 减去 R，条件只剩 `frozen_amount >= R`。停用商户也能确认。公开的 `Post` 不带这个选项，普通借方仍走原来的条件和 `ACTIVE` 检查。
- 锁顺序是先 `SELECT ... FOR UPDATE` 锁冻结单，再按账户 id 升序锁账户。如果先改商户账户、再去锁 id 更小的平台账户，会和只按 id 升序锁账户的普通记账死锁。确认不插入墓碑。
- 商户冻结更新影响 0 行是 `10003`，不是 `40002`。同一事务重读：行没了，或冻结仍然小于 R，都说明冻结单和账户对不上，不是可用余额不足。改成 `40002` 会让调用方当成“钱不够”去重试。确认 CAS 影响 0 行也是 `10003`：这行已经被 `FOR UPDATE` 锁住，WHERE 对不上不能当成幂等成功，否则刚插入的凭证会跟着提交。

## ACC-05 解冻

- 解冻先 `SELECT ... FOR UPDATE` 锁冻结单，再减 `frozen_amount`。空回滚则插入墓碑占住 `uk_biz`。Freeze 是先插入再改账户。两边都不会先锁账户，所以和冻结不会组成 AB-BA。
- 停用账户也要能解冻。条件更新不看 `status`，也不返回 `40005`。否则账户关掉之后，这笔冻结会一直占着可用余额。
- 账户更新影响 0 行是 `10003`，整笔事务回滚，冻结单保持 `FROZEN`。不要写成 `40002`。调用方看到余额不足会重试，但这里不是可用余额不够，是冻结金额和冻结单对不上。
- `RELEASED` 幂等不再比较金额。空回滚墓碑上的金额可能和后来的解冻请求不一样，比了就会把防悬挂误判成 `10006`。`CONFIRMED` 一律 `40004`，金额不同也不是 `10006`。
- 真正解冻返回原来的 `freeze_no`。事务外生成的号只在插入墓碑时使用。`WithTx` 遇到 1213 / 1205 重试时沿用同一个号。
- 16 个 goroutine 抢同一行时，`WithTx` 把单次事务限在 3 秒，含等连接。测试把这个库的打开连接收到 8。这是测试设置。

## ACC-04 冻结

- 行锁顺序是先 `INSERT account_freeze`（占住 `uk_biz`），再 `UPDATE account` 增加 `frozen_amount`。Post 只锁 `account`，不碰冻结单，所以和 Freeze 不会组成 AB-BA。两笔不同 `biz_no` 的 Freeze 先各锁自己的冻结单，再抢同一账户行，后面的排队，不会互相死锁。1213 / 1205 仍交给 `dbx.WithTx` 把整段回调重试。`freeze_no` 在事务外生成，重试沿用同一个号；回滚过的插入不会留下行。
- 1062 不结束 InnoDB 事务。不要回滚再另开连接去读。用 `dbx.IsDuplicate` 认错误号，同一事务按 `(biz_type, biz_no)` 读出已有冻结单。FROZEN / CONFIRMED 且金额、账户一致就返回已有单，不再加冻结。RELEASED 直接 `40004`，这是防悬挂，即使金额不同也不改成 `10006`。
- 透支分支只在同包假实现里测。`ck_account_type_rule` 要求 `MERCHANT_SETTLE` 的 `allow_overdraft = 0`，集成测试插不进透支商户账户。不为了覆盖这条去改 CHECK，也不写非法行。
- 条件更新影响 0 行时，在同一事务重读。非 ACTIVE 优先于余额不足。重读后仍然够冻结就返回 `10003`，不在事务里空转，避免占着行锁反复抢。
- `version` 和 Post 一样 +1。冻结不改 `balance`。需求里的 UPDATE 没写 version，这里只是和记账的版本列对齐。
- 100 个 goroutine 走 Freeze 自己的 `WithTx`。单次事务上限 3 秒，含等连接的时间。测试把这个库的打开连接收到 8，锁等待就不会堆过驱动的 3 秒读超时。验收要求 Freeze 自己提交，不能改成 Post 那种调用方 `BeginTxx`。

## PAY-10 商户费率

- 生效费率在 `merchant_fee_rate`，只追加。`merchant.fee_rate_bp` 仍是种子写下的初始值，改费率不更新它。
- 追加前先锁商户行，锁持有到提交，所以“晚于当前最大生效时间”在提交时还成立。停用商户也能登记。
- 相同生效时间走唯一键。1062 不回滚当前事务，再读这一档：费率相同算幂等，不同是 `10006`，旧行不动。
- 核对按 `payment_order.id` 分页，不扫分录。用分录 id 对账的流水核对仍是 ACC-10，这次不做，也不改 `ledger_entry`。
- `effective_from` 写入前截断到毫秒，和 `created_at` 在同一毫秒上比较，相等算已经生效。调用方传入的时区保持不变。
- 核对是 REPEATABLE READ 只读快照。这一轮开始之后新提交的费率，下一轮才看得见。只报告差额，不改 `fee_amount`，不改账。
- 种子的 `INSERT … SELECT` 要给 `merchant_id` 加上表名。不写表名时 MySQL 8.4 报 1052。冲突更新只写回本表的商户号，不改费率和生效时间。

## ACC-03 通用记账

- Post 不自己提交，也不 `BEGIN` / `ROLLBACK`。调用方已经打开本分片事务。Post 返回错误时，调用方回滚，凭证、分录和余额一起撤销。
- 1062 不会中止 InnoDB 事务。不要在 Post 里回滚。用 `dbx.IsDuplicate` 认错误号，然后在同一事务按 `(biz_type, biz_no)` 读出第一次的 `txn_no`。参数不同也返回第一次的结果，不再改余额。
- 本连接上没有的 `account_id` 是 `40003`，不是 `40001`。这条连接分不清“没有这个 id”和“账户在另一个分片”。`GetAccount` 仍然用 `40001`。
- `REVERSAL` 直接拒绝。Post 没有 `reversal_of`，而表约束要求冲正凭证必须带它。冲正是 ACC-08，这里不加参数、不改表。
- 多行锁按 `account_id` 升序 `SELECT ... FOR UPDATE`，再按同样的顺序更新。交叉记账时不会死锁。
- `txn_no` 在拿行锁之前用 `NewNo(ctx, "TX")` 生成。发号可能要等下一毫秒，不能占着账户行。
- 1000 个 goroutine 抢同一行时不要用 `dbx.WithTx`。它把整次事务限在 3 秒，等锁的时间也算进去，等待者会被取消，500/500 就数不准。这个用例自己 `BeginTxx`，父 ctx 放长，Post 仍然不提交。

## ACC-11 account RPC

- Kitex 的 `BizStatusError` 不会自动出现在 Thrift 响应体里。服务端把它记在 invocation 上，靠 TTHeader meta handler 写回头。客户端不启用 TTHeader 时，业务错误会变成成功的空响应。payment 客户端因此固定 TTHeader；网关以后自己建客户端时也必须启用。
- 服务端 2 秒超时如果直接返回 `10003`，payment 客户端不会重试，因为重试只认超时。biz 观察到的截止时间要保持为 Kitex 超时。服务端把 `ErrRPCTimeout` 写成 Thrift 异常后，客户端看到的是 `TransError`，消息仍是 `rpc timeout`，重试判断要认这一种，不能只认 `IsTimeoutError`。
- 平台汇总的 `shard`、`sub_no` 留空。有账户号时即使是 0 也要传指针。见 ACC-02。

## ACC-02 账户查询

- 平台汇总是两次独立只读，各用各的连接，不放进跨库事务，也没有跨分片快照。两个分片的余额可能不是同一时刻。这是管理查询和测试用的，不在交易链路里调用。交易只读商户所属分片的一行。
- 可用金额是合计余额减合计冻结，用 `money.Sub`，单位分。先加总再减，不把各行可用金额相加，也不用浮点。
- 查询不加锁，不用 `FOR UPDATE`。加锁会和以后的记账互相等待，而查询不修改余额。读到的是已提交值。
- `40001` 只表示该读的分片里没有行（平台是两个分片都没有）。查询失败是 `10003`，`Error()` 只有 `internal error`。不能把数据库故障写成账户不存在，否则调用方会去开户或当余额为 0。
- 汇总的 `shard`、`sub_no` 在 biz 里是 0，表示省略，不是分片 0、子账户 0。IDL 里这两个字段是 optional 指针。ACC-11 映射时要留空，不能写成指向 0 的指针，否则会被看成真的分片 0。
- 汇总不按当前 `PLATFORM_SUB_ACCOUNTS` 截断。配置调小后，高位子账户不再被选中，余额仍要计入（ACC-13）。
- 验收里的“支付入账”还没有 `Post`。入账是 ACC-03。测试用 SQL 写入余额，不改变资金规则，也不实现冻结。

## ACC-01 开户

- 1062 不解析驱动错误文本。同一事务里按 `owner_type, owner_id, account_type, currency, sub_no` 再查：查到同一组 owner 就返回已有行，余额和冻结保持原值；查不到说明撞的是账户号，返回已有的 `10003`。对外 `Error()` 只有 `internal error`，驱动文本留在 `Unwrap`。
- 账户号在事务外生成。发号可能为了追平不超过 5ms 的回拨，或等到下一毫秒，放进事务会占着锁。发号失败时还没有插入，不会留下半成品行。
- 幂等读取不加 `FOR UPDATE`。开户不改余额，加锁会和以后的记账交叉。返回的是已提交余额。

## ACC-13 分片路由

- 分布偏差用 `abs(count-expected)/expected`。10 万个样本、8 个桶，expected = 12500。小于 5% 就是绝对差严格小于 625。种子写死 `20261015`。2026-10-15 这一轮八个桶的计数是 12499、12645、12387、12549、12535、12456、12595、12334，最大偏差 0.01328，总和 10 万。某个种子失败就换种子，不放宽 5%。
- `Of` 不读 `ACCOUNT_SHARD_COUNT`。配置仍只允许 2，路由函数把 2 写死，避免纯函数去读环境。非法商户号的错误不回显原文：`M10001` 这种非法值本身就含有 `10001`。
- `Sub` 的 N 每次由调用方传入。调大子账户数是运维规则，函数不记旧的 N。N 超过 uint32 时直接返回错误：`uint32(n)` 截断成 0 后取模会 panic。
- `/readyz` 只是一个 `http.Handler`。两个池各有 1 秒 Ping，都成功才 200，正文 `ok`。池被 Close 后是 503，正文固定 `unavailable`，不带 ping 错误，因为错误里可能有地址。监听端口、`/healthz`、Kafka 和 SIGTERM 仍是 OPS-01。启动时连不上会在 `dbx.Open` 失败，本功能点不起进程。

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
