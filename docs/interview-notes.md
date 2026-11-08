# 面试笔记

随手记录踩坑、数据和取舍。不替代需求与技术决策文档。

## TL-01 paycli

- 验收用测试里的 httptest 假服务端，不连本机 8080。经网关跑通下单到转账的完整流程仍是 GW-09。
- 商户响应按接口 §3.3 验签；管理接口 §8 写明不签名，没有 `X-Signature` 不算失败。`code ≠ 0` 仍把 JSON 写到 stdout，退出码为 1。
- 客户端总超时不另造：按需求 §5.4.1 网关调用超时，默认 1 秒，`pay` / `close` / `refund` 5 秒，`transfer` 3 秒，`admin recon-run` 与 `admin check` 120 秒。

## GW-01 网关骨架

- compose 的 apps profile 仍是占位，应用容器不在 `make up` 里启动，占位进程保持退出码 1。这次没有改 compose。
- 网关进程听 `:8080`（商户 API、渠道回调）和 `:8090`（管理接口），不写成 `127.0.0.1`。`:6060` 这次不听，探针仍是 OPS-01。
- 时间窗口、验签、限流、nonce 四个中间件只把自己的名字放进链并调用 Next。不访问 Redis，不验签，不限流。
- 客户端 `WithRPCTimeout` 是 1 秒。更长的超时只在真正调用时用 `RPCTimeout(method)` 配合 Kitex 的 per-call option。本功能点没有这种调用。

## GW-03 商户验签

- 只验商户端口的 `/v1`（路径恰好是 `/v1` 或以 `/v1/` 开头）。时间窗口和 nonce 仍是 GW-04。限流仍是 GW-05。
- 响应用本次查到的 secret 签名。GW-01 那种无 lookup 的测试路由仍用 WithSignSecret。
- 每次调用 GetMerchantCredential，不缓存，1 秒 RPCTimeout，不重试。
- 未签名的 `/v1/balance` 从 GW-01 的 404 改为已有的 20001。

## GW-02 支付与余额路由

- 只做创建、发起、关闭、查询和余额。退款与转账是 GW-09。渠道回调是 GW-06。
- merchant_id 只来自验签。金额用 UseNumber 加 ParseInt，小数和字符串是 10001。
- 余额只回五个字段，available_amount 原样透传，不重算。传输失败是一次 10004，不重试。

## GW-06 渠道回调入口

- `POST /callback/mockpay` 不走商户验签。正文只有 `SUCCESS` 或 `FAIL`。
- 渠道密钥为空是 500，不是 401。时间窗是 ±300 秒，301 秒失败。
- 30001 和 50002 也回 `SUCCESS`，并给进程内计数加一，避免渠道空转重试。退款分支仍是 10003，留给 REF-03。
- 未知单号、金额不一致、2 秒内 SUCCESS、DUP_NOTIFY 十次，由 payment 的集成测试打到进程内网关和 channel-mock。测试把自动完成延迟固定为 50 毫秒，生产默认延迟不变。

## PAY-01 商户与密钥

- `merchant` 表列跟随库默认 `utf8mb4_bin`：区分大小写，但 PAD SPACE 比较时忽略尾部空格，compose 上实测 `WHERE app_id = 'app_m100001 '` 也能查到 `app_m100001`。集成测试临时库是 `utf8mb4_0900_ai_ci`，不区分大小写和重音，`'APP_M100001'` 也能查到。两种规则下 SQL 都会匹配字节不同的值。`X-App-Id` 不在签名串里，若 nonce 防重按请求带来的 app_id 记键，换个写法同一个签名就能重放。查询后在应用里逐字节比较读回的 app_id、merchant_id，不一致按不存在返回 `20004`。去掉这次比较，集成测试 `TestPAY01UnknownAppID` 立刻失败（测试库下靠大小写触发）；尾部空格目前只在 compose 手测过。
- Kitex 生成的结构体 `String()` 会把 `app_secret` 原样打出来。访问日志中间件不读请求和响应对象。
- 只给 `MerchantCredential` 写 `String`、`LogValue` 不够：凭证放进切片交给 slog JSON handler 会走 `json.Marshal`，作为未导出字段被 `%+v` 打印时 fmt 调不了方法、改走反射，三条路径都实测带出了明文。改成专用类型 `Secret`，`Format`、`MarshalJSON`、`LogValue` 一律输出 `***`，明文放在指针后面：反射对非顶层指针只打印地址，不解引用。取明文只能显式调 `Reveal()`。
- 配置缓存只存商户号、状态、默认通知地址，费率不进缓存。费率变更是按 `effective_from` 生效的，缓存住了会在生效时刻前后读到旧档，PAY-10 的核对口径就对不上。凭证也不缓存，轮换后下一次请求即生效。

## CH-01 模拟渠道受理与自动完成

- DATETIME(3) 写入时四舍五入，不是截断。东八区 23:59:59.9996 写进库会变成次日 00:00:00.000，而应用按原值算出的 `bill_date` 还是当天，同一行的两列对不上。应用先截到毫秒再写，`finished_at` 和 `bill_date` 用同一个截断后的值。
- 到期时刻用 `created_at + delay`，delay 由 `req_no` 的 CRC-32 决定。重启扫描时不用另存 due，按同一公式就能还原；`created_at` 因此改由应用时钟写入，不用列默认值，否则测试时钟和数据库时钟会对不上。
- 表上没有 version 列，状态推进只用 `WHERE id = ? AND status = 'PROCESSING'`。集成测试让 30 笔自动完成与外部关单 SQL 并发，2026-10-23 在 `a3433af` 上跑了 4 轮，关单赢 / 完成赢分别是 8 / 22、12 / 18、11 / 19、6 / 24，每笔都只有一方生效，关单赢的那笔没有 `finished_at`，也没调用钩子。
- Hertz 默认的 netpoll 传输层监听失败直接 panic，`Spin` 遇到 `Run` 错误也只打日志。服务改为先用 `net.ListenConfig` 自己监听、再交给标准库传输层，用 `SetCustomSignalWaiter` 把 `Run` 的错误带出来，端口被占用才能作为错误返回。
- `encoding/json` 对签名过的 body 也“宽容”：非法 UTF-8 字节换成 U+FFFD，孤立代理转义 `\udc00` 也解码成 U+FFFD，字段名不区分大小写（连 `ſubject` 都算 `subject`），重复键取最后一个。业务层的 `utf8.ValidString` 因此永远为真。改为解码前 `utf8.Valid(body)`，再用 `Decoder.Token` 逐个读顶层键，拒绝重复键和大小写变体；`req_no`、`notify_url` 另外拒绝 U+FFFD。
- 报错不等于没写进去。INSERT 或 CAS 在提交之后、返回之前断开时，调用方拿到错误，行却已生效。INSERT 这边用不随请求取消的上下文再读一次，`channel_txn_no` 是本次生成的就照常调度；CAS 这边记下每次尝试的 `finished_at`（毫秒，每次不同），重试得 0 行时按 id 读回比较，一致说明那次生效了，补调钩子，否则当作被关单。
- 时间窗原来用 `Now().Sub(time.Unix(sec, 0))`，本地时间带毫秒、请求时间戳只有整秒，恰好 300 秒前的请求会算成 300.9 秒被拒。改成 `Now().Unix()` 与整秒比较。
- 本机 compose 实测（2026-10-23，`a3433af`，channel-mock 二进制跑在宿主机上）：`PA0000000000000000001` 算出的延迟 1832ms，库里 `finished_at − created_at` 为 1833ms（重跑一次 1834ms）；受理两笔后立即 SIGTERM，退出用时 30ms（重跑 33ms），约 10 秒后重启，扫描到 2 笔并在启动当刻完成（都已过期）。

## CH-06 SLOW 与 ACCEPT_TIMEOUT

- SLOW 只推迟第一次回调：排在 `finished_at` 之后 30 秒，失败后的 5 秒、15 秒、30 秒、60 秒从当时的注入时钟算。
- ACCEPT_TIMEOUT 用调度器等注入时钟走到 `created_at` 之后 5 秒。受理 HTTP 上限改为 8 秒，原来的 3 秒会把这次 200 取消掉。查单和关单仍是 3 秒。

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

## OPS-01 健康检查与优雅退出

- 管理端口进程内监听 `:606x`，不要写成 `127.0.0.1:606x`。compose 把宿主机端口绑在回环地址上，发布进容器时连的是容器网卡。进程若只听容器里的回环，探活连不上。merchant-demo 没有管理端口。
- Kitex 自己的退出等待默认 5 秒，短于排空上限。生产把 `ExitWait` 设成 10 秒，并且由 `pkg/server` 独占 SIGTERM。两处同时 `signal.Notify` 会抢信号。
- account 两个分片是串行 Ping，各 1 秒。外层如果只给这一项 1 秒，第二个分片会被提前取消。注册时这项给 2 秒，Ping 规则本身不改。
- gateway 和 recon 的 `run` 打印完配置就返回，现有测试也这么断言。本次不在那里阻塞监听。6060 和 6063 只有常量。Redis、Kafka 和生产者也只留注册口。
- A1 仍开着。profile `apps` 还是占位，没有并进 `make up`。`stop_grace_period: 15s` 只写在六个应用服务上。
- 管理端口写超时从 3 秒改成 45 秒，是为了 30 秒 CPU profile；进程监听地址没改。

## CH-04 渠道退款

两笔全额退款如果先各自查出“还没退过”，再分别插入，两笔都会通过，同一笔支付会被退两次。必须在同一事务里 `FOR UPDATE` 锁住原支付行，锁内再合计 status 不是 FAILED 的退款，超额的那笔不插入。已有同一个 `(REFUND, req_no)` 要直接返回已有结果，不能再做额度校验，否则全额退款重提会被自己挡成 `REFUND_EXCEEDED`。退款场景只保留金额末两位 01 的 FAIL 和 02 的 DROP_NOTIFY，其余都按 NORMAL 完成。

## REF-01 创建退款

条件更新影响 0 行或插入 1062 之后，要按 (merchant_id, out_refund_no) 再读一次；一致就返回已有 refund_no，否则并发的重复请求会被报成 30005。再读放在事务结束之后，是因为 1062 必须先让事务回滚，额度才会撤销；0 行和 1062 两种结局于是共用同一条事务外的普通读，和主路径用的是同一个读方法。早先这里写的理由是“事务里再读看不到另一笔刚提交的退款”，在项目的 READ COMMITTED 下不成立：每条语句都读最新已提交的数据，事务内再读也能看到。

## REF-02 退款冻结与渠道提交

未装上冻结客户端时，CreateRefund 仍只返回刚插入的 INIT，已有 REF-01 测试不用改。故障点 `refund.after_freeze` 放在冻结成功之后、PROCESSING 事务之前；杀在这里时退款单还是 INIT，冻结单已经有一张。`channel_txn` 插入 1062 要把错误交回事务，CAS 一起回滚，再按 refund_no 读当前行，这次不调用渠道。冻结超时或不可用同样不插入 `channel_txn`。account 把处理超时写成 TransError、消息仍是 rpc timeout 时，也按不可用返回 INIT；连接拒绝仍是 10003。`CreateRefund` 的服务端处理超时是 5 秒，否则 2 秒截止会在渠道 3 秒总超时之前取消，第 2 秒到第 3 秒的 OK 或 REJECTED 会被当成未知。`ApplyRefundResult` 的落库、释放额度和 outbox 仍是 REF-03；本切片的生产方法返回 10003，不把退款改成 FAILED。

## REF-03 退款结果推进

`ApplyRefundResult` 按这次结果的意图选分支，不按库里读到的退款状态选。如果按状态选，REF-04 判 INIT 超时的同时 REF-02 已把这笔推进为 PROCESSING 并提交了渠道，读到 PROCESSING 后会走渠道分支，把一笔渠道可能已经成功的退款写成 FAILED 并释放额度，造成资损。按意图选时，`REFUND_TIMEOUT` 只 CAS `INIT → FAILED`，未命中就返回 10003，交给回调或查单推进。释放额度影响 0 行时整个事务回滚，日志和计数放在回滚之后，这样记录时回滚已经完成，日志描述的就是“这次没有留下改动”的最终结果。这里不存在重复记的问题：影响 0 行返回的错误不是 MySQL 错误号，`dbx.WithTx` 只对死锁和锁等待超时重试，所以放在闭包里也只记一次。退款单号和支付单号都带 19 位连续数字，日志兜底扫描会把它们当卡号部分遮盖，测试按遮盖后的形态断言，人工修复靠指标加 SQL 定位。INIT 分支 CAS 未命中再读到 SUCCESS 是两路处理的并发竞争，不是渠道给了矛盾结果，不能计入回调冲突指标，否则会把正常竞争报成渠道问题。

## PAY-05 查单补偿

抢占和查渠道拆成两步：短事务里 `FOR UPDATE SKIP LOCKED` 取到期行并推后 `next_query_at`，提交后再在事务外查渠道，持锁期间不发外部请求。两个实例同时跑时，SKIP LOCKED 让各自拿到不同的行；把它去掉做变异检查，两实例用例里同一条流水在同一档被查了 4 次，测试失败。抢占不改 `version`，因为它不是状态变化，终态 CAS 也不比较 version。每个任务用 `context.WithoutCancel` 派生自己的 10 秒超时：进程停止时批次 ctx 立即取消，等并发槽的任务不再启动，已经发出的关单或重提退款等到结果再写库。并发槽用带 `select ctx.Done()` 的信号量，而不是 `errgroup.SetLimit`：后者的 `Go` 在槽满时阻塞且看不到取消，槽一空就会再启动一个。审查发现的另一处：批次慢于退避间隔时，排在后面的行已经到期，会被另一个实例抢走进入下一档，本实例再查就是同一档查两次。本实例的下一批不会抢：同一实例一次只跑一个批次，`RunQueryBatch` 要等 `wg.Wait` 结束才返回。修法是任务带上抢占时写入的 `next_query_at`，启动时已到期就跳过，这一档交给接手的一方。即使中途被杀，“渠道已关、本地未写”的行下一档会查得 CLOSED 并补写，只是多等一档。定时循环放在 `service/payment/job`，但 depguard 禁止 job 目录 import `service/`，所以 job 只接收一个 `func(context.Context) error`，由 `cmd/payment` 传入 `RunQueryBatch`。插入流水时的 10 秒写死在 PAY-03、REF-02，测试配置把 `QUERY_BACKOFF` 全设成 2 秒也不影响第一次查单。

## PAY-07 超时关单

第一版扫描是一条 `status IN ('INIT', 'PAYING') AND expire_at <= now ORDER BY status, expire_at LIMIT 200`。`IN` 让 `idx_status_expire_at` 变成两段范围，只写 `ORDER BY expire_at` 会 filesort，加上 `status` 后按索引顺序读够就停。审查指出这样 INIT 永远排在前面：过期 INIT 超过 200 条时 PAYING 一条都扫不到。改成 INIT、PAYING 各一条 `status = ? ... ORDER BY expire_at LIMIT 100`，等值条件下按 `expire_at` 排序就是索引顺序，两条都没有 filesort，合计仍是每批 200 条。第一版还是逐条执行，渠道挂起时一批 PAYING 要依次等每条的超时，5 秒一轮名存实亡；改为 INIT 串行（只是本地 CAS），PAYING 复用 PAY-05 带 `select ctx.Done()` 的信号量，并发 10。成功日志原来写成“本实例完成了关单”，但 CAS 未命中后重读到别人写的 CLOSED、`ApplyPayResult` 对相同终态的幂等返回，拿到的订单完全一样，要区分得改 PAY-04 的返回值，所以只把日志改成中性的“已是该状态”，谁写的以 outbox 和 version 为准。关单逻辑没有另写，只给 PAY-06 的 `closeStatus` 加了一个 `source` 参数，超时关单传 JOB。两实例用例第一次出现 `10003`，原因是测试里第二个实例没装发号器，`ApplyPayResult` 的就绪检查拒绝了它；单行失败只记日志、最终状态仍正确，所以只看终态的断言发现不了这类装配问题，要同时看日志。

## TRF-02 同分片转账执行

账户调用放在 payment 的事务外面。`TransferLocal` 自己提交账户分片；payment 只在看到 SUCCESS 或 FAILED 之后，另开一个事务做 CAS 并写 outbox。超时和 `10003`、`10004` 不写终态，因为账户可能已经记账，当成失败会和账户的成功结果打架。`10001`、`10006` 同样留在 TRYING，只打日志：`10006` 时账户侧可能已经有 SUCCESS 分支。`40002` 只出现在这一次执行走到 `INSUFFICIENT_BALANCE` 的请求上；同一商户单号再来，返回已有的 FAILED 单，code 是 0，也不会再调账户。本库 CAS 如果在账户已经成功之后失败，行仍是 TRYING，这次请求是 `10003` 或 `10004`，相同单号的重试不再执行，要等 TRF-04 用同一个 `transfer_no` 再调一次，账户侧幂等。

## TRF-01 创建转账单

插入是单条自动提交的 INSERT。1062 之后另起一次读才能看见已提交的赢者，按 `(merchant_id, out_transfer_no)` 读到且参数一致就返回它，不再执行；执行只由插入成功的那次请求负责，否则 100 个并发相同请求会各自调一次 account。读不到说明撞的是 `uk_transfer_no`（发号撞号），返回 10003，不当作幂等命中。执行器做成接口注入，TRF-02、TRF-03 完成前不装，CreateTransfer 返回刚插入的 TRYING，由 TRF-04 按 `next_retry_at` 接管。幂等读放在收款方校验之前。最初按需求的书写顺序先校验收款方，问题是建单后收款商户被停用，商户用相同参数重试会得到 30008，看不到那张已经存在、可能已经成功的转账单，违背“超时重试拿已有单”的承诺。改为先读已有单：命中只比较 payee、amount、notify_url，一致就返回，不一致是 10006；没有已有单才校验收款方。1062 后的再读同样不校验收款方。

## REF-04 退款卡单恢复

“在 `refund.after_freeze` 处 kill 进程”用真实的 kill -9 验收：集成测试把自己的测试二进制作为子进程重新执行，子进程用 `fault.Init` 打开故障点，以真实 account biz 冻结后在故障点被 SIGKILL；父进程断言退出原因是 `signal: killed`、退款仍是 INIT、冻结单已有 1 张、没有渠道流水，再新建一个 `Payments`（相当于重启后的进程）跑一轮恢复。子进程的 DSN 起初放在环境变量里，`gosec` 的 SSRF 污点分析（G704）从 `os.Getenv` 一路追到渠道 HTTP 客户端的 `http.NewRequestWithContext` 报警，虽然两者并不相连；改为从标准输入传 DSN 后告警消失，DSN 里的密码也不再出现在子进程环境里。恢复任务不抢占、不加锁：重复执行靠冻结幂等、`INIT → PROCESSING` 的 CAS 和 `channel_txn.uk_biz_no`，两实例同时跑同一批卡单，每笔仍只有 1 张冻结单、1 条流水、渠道只受理 1 次。1 小时边界取严格大于，把比较改成 `>=` 做变异检查，恰好 1 小时的用例失败。

## TRF-03 跨分片转账 TCC

并行的两个 Try 用 `errgroup.Group`，不能用 `WithContext`。`WithContext` 会在一个 Try 失败时取消另一个，另一边的结果变成未知，协调者无法决定确认还是取消。失败的那一边返回后，另一边的 ctx 必须仍然有效，两边都要等到结束。

TCC 成功不把 `txn_no` 写在 payment 的 `transfer_order` 上。付款和收款不在同一个库，一张凭证放不下两次记账。两张凭证分别在付款分片的 `TRANSFER_OUT` 和收款分片的 `TRANSFER_IN`。

Confirm 失败一次，本请求就停在 CONFIRMING，不在本请求里重试，也不取消已经成功的分支。恢复是 TRF-04 的事。例外是账户事务 3 秒预算打满：`DeadlineExceeded` 或 MySQL 1213/1205（包在 10003 里也算）且父 ctx 仍有效时，Try、Confirm 和 Cancel 整组最多再试 5 次；`sql.ErrTxDone`（事务已经结束）同样整组再试。`sql: Rows are closed`（结果集已经关闭）同样整组再试；业务错误不重试。Try 用尽后仍是 TRY_TIMEOUT，Confirm 和 Cancel 用尽后仍停在中间态。

## TRF-04 转账恢复任务

抢占和请求线程会抢同一行的决策 CAS。恢复看到仍是 TRYING 的 TCC，一律改成 CANCELLING、`fail_code = TRY_TIMEOUT`，然后 Cancel，不再 Try。请求线程如果先把决策提交成 CONFIRMING，恢复这次 CAS 影响 0 行，只再读一次并改走 Confirm。已经是 SUCCESS 或 FAILED 就结束，不再写第二条 outbox。迟到的 Try 由墓碑拦住，恢复自己不再发 Try。

不在恢复里重跑 Try。决策窗口已经关掉：再 Try 可能在已经决定取消之后又成功，或者和还活着的请求线程各做一次。Confirm 和 Cancel 本身幂等，下一轮重试它们是安全的。恢复也不走 `CreateTransfer` 和 `executeTransfer`。`CreateTransfer` 对相同商户单号不会重新执行；注入的 `TransferExecutor` 会抢走所有模式，只留给请求路径。卡住的 TRYING + LOCAL 直接调用已有的 `executeLocal`。

`pay_transfer_stuck` 是本进程里当前卡住的 `transfer_no` 集合的大小，不是只增计数，也不是 Prometheus。`retry_count >= 10` 且仍非终态才计入，到达终态就从集合去掉。没有 mode 标签。OPS-03 以后再暴露。

多实例靠 `FOR UPDATE SKIP LOCKED` 加状态 CAS。一个实例锁住的行，另一个实例跳过。抢占只加 `retry_count` 并推后 `next_retry_at`，不改 `version`：`version` 只跟状态 CAS 走。`updated_at` 有 `ON UPDATE CURRENT_TIMESTAMP`，抢占必须把注入时钟写进去，否则这一列会变成数据库的 `NOW()`。
