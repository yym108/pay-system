# GW-06 渠道回调入口

只读这一份就能看懂这次改了什么。正文里的行为、边界和证据不依赖别的文档。

| 项 | 内容 |
| --- | --- |
| 功能点 | GW-06（P0，第 3 周） |
| 所属链路 | 基础设施 |
| 代码提交 | `dcd1718` `[GW-06] add channel callback`（完整 `dcd17189a9b15063b80d15ef912504642fc60499`）；`34671d0` `[GW-06] prove channel callback acceptance`（完整 `34671d080764fed468ac0f1e943502449b8d6b45`，2026-11-02 10:45:06 +0800） |
| 分支 | `feat/GW-06-callback-accept`（验收补齐从已合入 main 的 `feat/GW-06-channel-callback` 继续） |
| 日期 | 2026-11-02 |
| 状态 | DONE。四条验收已勾，开发计划里的 GW-06 已勾。周交付段落 `paycli create → pay → query` 这次没有执行，不能把那一段写成已经完成。正文一句话结论里“这四件事都还没有在 MySQL 上证明”已被文末修订取代。 |

本报告写在上面这次代码提交之后，不在那次提交的文件里。报告撰写时尚未提交。

## 一句话结论

商户端口新增 `POST /callback/mockpay`。渠道签名和时间窗通过后，网关把这一次通知转成已有的 `HandleChannelNotify`，payment 只把 `txn_type=PAY` 交给已有的 `ApplyPayResult`。响应正文只有 `SUCCESS` 或 `FAIL` 这 7 或 4 个字节，没有 JSON，也没有平台签名。签名错误返回 401 且 RPC 次数为 0，这是本报告 4.3 的 `TestGW06BadSignature`。未知单号、金额不一致、2 秒走完一笔真实订单、10 次回调只推进一次，这四件事都还没有在 MySQL 上证明。

## 1. 背景

这次补的是渠道通知进支付单的 HTTP 入口。记账仍在 payment 已有的结果推进里，网关不算手续费，也不改余额。

### 1.1 系统简介

pay-system 是一个用 Go 编写的小型支付后端，模拟商户收款、退款、商户间转账三条资金链路，重点在数据一致性和性能。系统由 5 个部分组成：gateway（对外 HTTP 入口，负责签名校验和限流）、payment（支付单、退款单、转账单的状态管理和流程推进）、account（账户余额与记账，数据按商户分到 2 个数据库分片）、channel-mock（模拟微信、支付宝这类外部支付渠道）、recon（对账，核对系统内外数据是否一致）。服务之间用 Kitex RPC 同步调用，或通过 Kafka 消息异步通知；数据存在 MySQL 8.4，Redis 用于防重放。所有金额用整数“分”表示，所有记账只追加不修改。当前已完成：工程骨架、账户分片与记账、渠道模拟、支付单的创建、发起、结果推进、关单和查询、outbox 写入、健康检查、pprof、paycli、网关骨架、商户验签、支付与余额 HTTP 路由，以及本次尚未收口的渠道回调入口。

账户分片是把账户按键拆开存放。渠道模拟是单独的 channel-mock 进程，不访问真实的微信或支付宝。支付单是一笔支付订单。结果推进是 payment 用一次数据库事务把渠道终态写进渠道流水和支付单。outbox 是和业务写在同一个事务里的待投递事件。paycli 是本仓库的命令行客户端。商户验签是更早的 GW-03，只保护 `/v1` 商户接口。渠道回调不走那一套商户签名。

### 1.2 位置

本功能点站在 channel-mock 的 HTTP 通知和 payment 已有的结果推进之间。

```mermaid
flowchart LR
  mock[channel-mock] --> http[POST /callback/mockpay]
  http --> chk[渠道签名与时间窗]
  chk --> rpc[HandleChannelNotify]
  rpc --> apply[ApplyPayResult]
  apply --> fault[callback.after_commit]
  fault --> body[正文 SUCCESS 或 FAIL]
```

channel-mock 在自己的事务提交之后，向网关商户端口发 POST。网关先验渠道签名和时间窗。通过后调用 payment 的 `HandleChannelNotify`，超时沿用已有的 1 秒，不重试。payment 只处理 `txn_type=PAY`，调用已有的 `ApplyPayResult`，source 固定为 `NOTIFY`。那次数据库事务返回成功之后，才触发已注册的故障点 `callback.after_commit`。然后网关按结果写 `SUCCESS` 或 `FAIL`。管理端口不注册这条路由。退款结果推进留给后面的 REF-03，本入口收到 `REFUND` 时 payment 返回已有的 `10003`，网关因此回 `FAIL`。

### 1.3 为什么需要

渠道会重复通知，也会把通知发到不存在的单号上。如果签名错误也去改订单，别人可以伪造一笔成功。如果未知单号让渠道一直重试，重试永远不会把订单变出来。

具体数字：一笔 10000 分（100.00 元）的支付。渠道正文里的 `amount` 是 JSON 整数 10000。签名被改掉最后一个字节时，网关回 HTTP 401，正文是 `FAIL`，payment 调用次数是 0。这不能把订单改成 SUCCESS，因为请求没有离开网关。测试用的时钟是 `time.Unix(1_791_500_000, 0)`，密钥字节是 `mockpay-plain-key`。

同一笔如果单号在库里不存在，或金额和渠道流水不一致，重试同一份正文也不会变成一笔新订单。这种情况下网关要回 HTTP 200 和正文 `SUCCESS`，让渠道停止重试，并在进程内把拒绝次数加 1。这个 HTTP 行为有假 RPC 测试。订单行是否真的没被改，还没有拿 MySQL 证明。

## 2. 术语

下面这些词在本报告里只表示这里写的含义。

| 词 | 含义 |
| --- | --- |
| 分 | 金额的最小单位，类型是 int64。1 元 = 100 分。10000 分 = 100.00 元。 |
| 渠道回调 | channel-mock 发给网关的 HTTP POST。路径是 `/callback/mockpay`，不在 `/v1` 下面。 |
| 渠道签名 | 用环境变量 `CHANNEL_MOCKPAY_KEY` 对“时间戳、换行、原始 body”计算的签名。请求头是 `X-Mock-Timestamp` 和 `X-Mock-Signature`。网关不裁剪这两个头。 |
| 时间窗 | 请求头里的 Unix 秒和网关注入时钟的 Unix 秒之差的绝对值。300 秒以内放行，301 秒拒绝。 |
| req_no | 回调正文里的平台支付单号，前缀 `PA`。网关把它放进 RPC 的 `ReqNo`。 |
| notify_id | 回调正文里的通知号，前缀 `CN`。网关转发这个字段，不把它写入订单表。 |
| channel_txn_no | 渠道侧流水号，前缀 `MC`。 |
| ApplyPayResult | payment 里已有的函数。它在一次事务里按渠道流水和支付单做比较并交换，只有 SUCCESS 写一条 outbox。本功能点不改这个函数。 |
| CAS | 比较并交换。更新语句带上当前状态，影响行数是 0 就表示状态已经变了。网关自己不执行这条 SQL。 |
| outbox | 和订单更新在同一个事务里插入的一行待投递事件。事件名 `payment.succeeded` 只在 SUCCESS 时插入。 |
| notify_count | 渠道流水上的通知次数。已有的 `ApplyPayResult` 在 NOTIFY 路径上递增。本功能点没有新的计数 SQL。 |
| 幂等 | 同一笔结果再来一次，订单只推进一次。重复通知的去重在 `ApplyPayResult` 里，不在网关。网关不重试 RPC。 |
| 10001 | 已有的参数错误。回调正文不是一个合法对象、金额不是十进制整数、状态或时间格式不对时，网关回 HTTP 500、正文 `FAIL`，并打 `err_code` `10001`。payment 收到空请求时也返回这个码。 |
| 10003 | 已有的内部错误。渠道密钥为空、签名函数报空密钥、RPC 客户端是 nil、`txn_type` 不是 `PAY`，都走这个码。网关侧对应 HTTP 500 和正文 `FAIL`。 |
| 10004 | 已有的下游不可用。RPC 超时属于这一类。网关回 HTTP 500、正文 `FAIL`，不把拒绝计数加一。 |
| 30001 | 已有的订单不存在。payment 以业务错误返回时，网关回 HTTP 200、正文 `SUCCESS`，并把进程内计数加 1。 |
| 50001 | 错误码表里已有的渠道签名失败。这条路由的正文不写这个数字，签名失败的正文就是 `FAIL`，HTTP 状态是 401。 |
| 50002 | 已有的渠道金额不一致。处理和 30001 相同：HTTP 200、正文 `SUCCESS`、进程内计数加 1。 |
| pay_callback_rejected_total | 网关进程内的计数，键是 `30001` 或 `50002`。不是 Prometheus，`/metrics` 仍是 404。 |
| callback.after_commit | 已注册的故障点名字。`ApplyPayResult` 成功返回之后、结果字符串返回之前调用。事务已经结束。本次没有把 `FAULT_POINTS` 设成真的杀进程。 |
| DUP_NOTIFY | channel-mock 的一种场景：同一笔通知发 10 次。经本 HTTP 入口的那 10 次还没有测。 |

## 3. 技术讲解

### 3.1 要解决的问题

1. 签名错误或时间窗外的通知不能调用 payment，因此不能改订单。
2. 渠道密钥没配是本进程的配置问题，要让渠道重试，不能伪装成对方签名错误。
3. 未知单号和金额不一致重试也不会变好，要让渠道停止重试，同时留下拒绝次数。
4. 传输失败要让渠道重试，网关自己不重试。
5. 正文必须是渠道已经约定好的 `SUCCESS` 或 `FAIL`。多一个换行，渠道都不会当成送达。
6. 回调不查 Redis，也不做 nonce。重复通知靠 payment 里已有的幂等。
7. 退款通知先留在 payment 的 `10003`，等 REF-03 再接 `ApplyRefundResult`。

### 3.2 方案

网关先看密钥长度。长度是 0 就 HTTP 500、正文 `FAIL`，日志 `err_code` 是 `10003`，不调用 payment。然后读两个渠道头。缺一个、头里有 CR 或 LF、时间戳不是整数、或和时钟相差超过 300 秒，都是 HTTP 401、正文 `FAIL`，不调用 payment。相差正好 300 秒放行。签名用已有的 `sign.VerifyChannel`。对不上是 401。空密钥错误仍是 500。

正文用 `json.Decoder` 加 `UseNumber`，只接受一个 JSON 对象。未知字段忽略。第二个 JSON 值算非法。必填字段是 `notify_id`、`txn_type`（`PAY` 或 `REFUND`）、`req_no`、`channel_txn_no`、`amount`、`status`（`SUCCESS` 或 `FAILED`）、`finished_at`。时间格式是 `2006-01-02T15:04:05.000Z07:00`。金额必须匹配 `^(0|[1-9][0-9]*)$` 再用 `ParseInt`。`100.5` 不匹配，HTTP 500、正文 `FAIL`，不调用 payment。0 可以转发。带负号的数不匹配这个表达式，同样是 500，不调用 payment。网关把 RPC 的 `channel` 固定写成 `mockpay`，不从正文里取渠道名。

RPC 只调用一次，超时是已有的 `RPCTimeout("HandleChannelNotify")`，也就是 1 秒。返回的 `result` 只有 `APPLIED`、`DUPLICATE`、`CONFLICT` 才是 HTTP 200、正文 `SUCCESS`，且不加拒绝计数。空响应或其他字符串是 500。业务码 `30001` 和 `50002` 是 HTTP 200、正文 `SUCCESS`，ERROR 日志带 `err_code` 和 `req_no`，进程内计数加 1。其他业务码、RPC 超时、以及不是业务错误的失败，都是 500，不加这个计数。

payment 的 `HandleChannelNotify`：请求是 nil 返回 `10001`。`txn_type` 不是 `PAY`（包括 `REFUND`）返回 `10003`，不调用业务，也不触发故障点。`PAY` 解析完成时间，空 `req_no` 或状态不是 `SUCCESS`/`FAILED` 则返回 `10001`。金额是 int64 分的指针。source 是 `NOTIFY`。`ApplyChannelNotify` 先做已有的参数校验，再读一次渠道流水，然后调用已有的 `ApplyPayResult`。只有这次调用没有错误，才执行 `fault.Point(ctx, "callback.after_commit")`。成功后如果订单号是空，返回 `10003`。结果字符串：调用前流水已是终态且和这次结果不同，标 `CONFLICT`；调用前状态和这次结果相同，标 `DUPLICATE`；其余标 `APPLIED`。并发时这个标签可能标错，HTTP 正文仍然是 `SUCCESS`。

写出响应用 `SetStatusCode`、`Content-Type: text/plain; charset=utf-8` 和 `SetBodyRaw`。不走 JSON 信封，也不加平台签名。超过 64KB 的 body 仍是已有的 HTTP 413 和 JSON `10001`，这次没有改那个限制。

这段代码在做金额校验。看正则：只有 0 或不带前导 0 的正整数才会进入 RPC。

```go
if len(obj.Amount) == 0 || !jsonIntPattern.Match(obj.Amount) {
    return nil, false
}
amount, err := strconv.ParseInt(string(obj.Amount), 10, 64)
```

### 3.3 为什么这样做

| 方案 | 做法 | 优点 | 缺点 | 结论 |
| --- | --- | --- | --- | --- |
| 采用：网关只做验签和转发 | 订单推进仍用已有的 `ApplyPayResult` | 幂等、outbox、金额比较不用再写一套 | 网关测试看不到 MySQL 里的行 | 采用 |
| 备选：回调里直接改订单 | 网关连支付库并更新状态 | 少一次 RPC | 网关会变成第二个记账点，也违反服务之间不互相访问数据库 | 不采用 |

### 3.4 正确性

网关不计算手续费，也不改余额。订单是否只推进一次，取决于已有的 `ApplyPayResult`。下表只写这次实际证明了的行为。

| 场景 | 系统行为 | 最终状态 | 证明 |
| --- | --- | --- | --- |
| 签名最后一个字节被改掉 | HTTP 401，正文 `FAIL`，RPC 0 次 | 假 RPC 没有收到请求。没有读 MySQL | `TestGW06BadSignature` |
| 时间戳比时钟多 301 秒 | HTTP 401，正文 `FAIL`，RPC 0 次 | 同上 | `TestGW06TimestampSkew` |
| 时间戳比时钟多 300 秒且签名正确 | HTTP 200，正文 `SUCCESS`，RPC 1 次，`channel=mockpay`，`amount=10000`，`txn_type=PAY` | 假 RPC 收到这一次。没有读 MySQL | `TestGW06TimestampSkew` |
| 网关没配渠道密钥 | HTTP 500，正文 `FAIL`，RPC 0 次 | 假 RPC 没有收到请求 | `TestGW06EmptyKey` |
| 假 RPC 返回业务码 30001 或 50002 | HTTP 200，正文 `SUCCESS`，对应计数加 1，RPC 1 次 | 没有读订单行，不能证明行未改 | `TestGW06RejectedCodes` |
| 假 RPC 返回 RPC 超时 | HTTP 500，正文 `FAIL`，RPC 1 次，拒绝计数不变 | 网关不重试 | `TestGW06TransportOnce` |
| 金额 JSON 是 100.5 | HTTP 500，正文 `FAIL`，RPC 0 次 | 没有把 100.5 分送进 RPC | `TestGW06AmountNotInteger` |
| payment 收到空的 `PayResult` | 返回已有的参数错误 | 不写库 | `TestApplyChannelNotifyRejectsEmpty` |
| 合法 `PayResult` 但订单存储是 nil | 返回已有的内部错误 | 不写库 | `TestApplyChannelNotifyNilStore` |
| payment 收到 nil 的回调请求 | 返回 `10001`，业务调用 0 次 | 不写库 | `TestUnimplementedMethodsReturnInternal` |
| 两个回调并发 | 未在本入口测试 | 未验证 | 无 |
| `ApplyPayResult` 提交之后进程被故障点杀掉 | 代码在成功返回后调用 `callback.after_commit`。本次没有设置 `FAULT_POINTS` | 未验证 | 无 |
| 10 次 DUP_NOTIFY 打到本路由 | 未测试 | 未验证 | 无 |
| Pay 之后 2 秒内订单变成 SUCCESS | 未测试 | 未验证 | 无 |

### 3.5 性能

不涉及。这次没有压测，也没有性能开关。

## 4. 已完成内容

### 4.1 改动清单

| 文件 | 新增 / 修改 | 作用 |
| --- | --- | --- |
| `service/gateway/callback.go` | 新增 | 注册路由、验签、解析、一次 RPC、写 `SUCCESS`/`FAIL`、拒绝计数 |
| `service/gateway/callback_test.go` | 新增 | 上面 3.4 里网关那几条测试 |
| `service/gateway/server.go` | 修改 | 商户端口在 `registerV1` 旁边注册回调；保存渠道密钥和 notify 客户端 |
| `cmd/gateway/main.go` | 修改 | `serve` 增加渠道密钥参数，并把它和 payment 客户端交给商户端口 |
| `service/payment/biz/notify.go` | 新增 | `ApplyChannelNotify`：调用已有的 `ApplyPayResult`，成功后触发故障点 |
| `service/payment/biz/notify_test.go` | 新增 | 空参数和 nil 存储两条 |
| `service/payment/biz/payment.go` | 修改 | `App.ApplyChannelNotify` 转给 `Payments`；空 App 返回 `10003` |
| `service/payment/handler/handler.go` | 修改 | `HandleChannelNotify` 处理 PAY；其他类型返回 `10003` |
| `service/payment/handler/service.go` | 修改 | 业务接口增加 `ApplyChannelNotify` |
| `service/payment/handler/handler_test.go` | 修改 | nil 请求期望 `10001` 且不调用业务 |
| `docs/02-requirements.md` | 修改 | §3 状态改为 DOING；第一条验收打勾；§8.2 增加 2026-11-01 的说明 |
| `docs/05-api.md` | 修改 | 文档版本 3.3.4；写明正文不是 JSON |
| `docs/interview-notes.md` | 修改 | 记下空密钥是 500、时间窗、30001/50002 也回 SUCCESS |

`docs/04-dev-plan.md` 没有改。GW-06 勾选框仍是未勾。交付段落仍是 `paycli create → pay → query`，订单最终 SUCCESS，`channel_txn` 与订单一致，outbox 有 1 条事件，A3 订单层用例通过。这几句还没有被这次运行证明。

### 4.2 对外变化

| 类别 | 变化 |
| --- | --- |
| 接口 | 商户端口注册 `POST /callback/mockpay`。正文是 `SUCCESS` 或 `FAIL`。字段和错误码数字没有新增。IDL 没有改。 |
| 表结构 | 无。 |
| 配置项 | 无。仍用已有的 `CHANNEL_MOCKPAY_KEY`。 |
| 错误码 | 无新错误码。 |
| 指标 / 故障点 | 进程内 `pay_callback_rejected_total`，键为 `30001` 或 `50002`。故障点名字 `callback.after_commit` 是原来已注册的，这次在成功返回后调用它。 |

### 4.3 验收对照

| 验收条目 | 测试函数 | 类型 | 结果 |
| --- | --- | --- | --- |
| 签名错误的回调不改变任何订单 | `TestGW06BadSignature` | 单元。假 RPC，没有 MySQL | 通过。HTTP 401，正文 `FAIL`，调用 0 次。订单行未读，不变是由“没有调用”推出的 |
| 签名正确但 req_no 未知、金额不一致：200 SUCCESS，订单不变，告警指标 +1 | `TestGW06RejectedCodes` | 单元。假 RPC | HTTP 200 和计数 +1 通过。订单行未读，所以这条验收仍未勾 |
| Pay 返回 PAYING 后，2 秒内经 channel-mock 自动完成并回调，订单变为 SUCCESS | 无 | 未验证 | 未测，验收未勾 |
| 渠道 10 次回调都是 200 SUCCESS，订单只推进一次，outbox 恰好 1 条 `payment.succeeded`，`notify_count = 10` | 无。订单层已有别的测试，不经过本 HTTP 入口 | 未验证 | 未测，验收未勾 |

### 4.4 验证记录

环境：2026-11-02，go1.27.2 darwin/arm64，golangci-lint 2.14.0 built with go1.27.1 from 114493f on 2026-09-24T10:54:53Z。下面是同一次顺序执行、并且全部退出码为 0 的结果。`service/gateway` 在 `make test`、`make test-integration` 和 `make cover` 里是 cached，因为这一轮之前同一份网关代码已经跑过。`service/payment/biz` 不是 cached。

| 命令 | 结果 |
| --- | --- |
| `gofmt -l .` | 退出码 0，无文件名。real 0.05s，user 0.20s，sys 0.04s |
| `make lint` | 退出码 0，`0 issues.`。real 1.46s，user 2.55s，sys 3.99s |
| `make test` | 退出码 0。real 6.65s，user 4.24s，sys 4.82s。根包 4.833s。`service/payment/biz` 5.574s。`service/gateway`、`service/payment/handler`、`cmd/gateway` 为 cached |
| `make test-integration` | 退出码 0。real 36.76s，user 7.14s，sys 7.07s。根包 36.330s。`service/payment/biz` 25.442s。`service/gateway` 为 cached |
| `make cover` | 退出码 0。real 37.16s，user 8.50s，sys 10.04s。全仓库 total 31.0%（含生成代码，不是门槛）。`service/payment/biz` 25.477s / 85.1%（门槛内，不低于 70%）。`service/gateway` cached 76.2%（不在门槛内）。`service/payment/handler` cached 81.7%。`cmd/gateway` cached 25.6%。`pkg/money` cached 100%，`pkg/sign` cached 100%，`pkg/idgen` cached 88.1%，`pkg/outbox` cached 95.0% |

同一次 `make cover` 的函数覆盖：`channelCallback` 74.0%，`parseCallback` 68.2%，`callbackResult` 81.8%，`writeCallback` 85.7%，`CallbackRejected` 85.7%，`addCallbackRejected` 83.3%，`registerCallback` 66.7%，`skew` 100%，`WithNotifyAPI` 100%，`WithChannelKey` 100%。`Payments.ApplyChannelNotify` 25.0%。`App.ApplyChannelNotify` 0.0%。`HandleChannelNotify` 33.3%。`cmd/gateway` 的 `serve` 7.4%。

`make gen` 未运行。IDL 和 `kitex_gen/` 没有改。`mmdc` 未运行。`docs/06-flows.md` 没有改。没有用 paycli 打到正在运行的环境。

### 4.5 同步的文档

- 需求 §3 的 GW-06 从 TODO 改为 DOING。第一条验收已勾。后三条未勾。§8.2 有一行 2026-11-01、版本仍是 v3.0 的说明，写明两秒路径和 DUP_NOTIFY 端到端还没测。需求版本仍是 v3.0。
- 接口文档版本改为 3.3.4，变更记录日期是 2026-11-01。写明商户端口已注册该路由，正文是 `SUCCESS` 或 `FAIL`，不是 JSON。`txn_type` 不是 PAY 时仍是已有的 `10003`。
- 约定版本仍是 v2.25。没有新错误码、新配置、新单号前缀。
- 面试笔记增加 GW-06 三条：回调不走商户签名；空密钥是 500 而时间窗是 ±300 秒；30001 和 50002 也回 SUCCESS，退款仍是 10003。
- 开发计划没有勾选 GW-06。

## 5. 未完成与风险

状态保持 DOING。还差三件事，所以周计划不能勾，周交付段落也不能写成已经完成：

1. 签名正确但单号未知或金额不一致时，HTTP 已经能回 `SUCCESS` 并加计数。MySQL 里的订单行是否保持原样，没有测。
2. Pay 得到 PAYING 之后，channel-mock 自动完成并经本路由回调，2 秒内订单变为 SUCCESS，没有测。
3. 10 次回调都回 `SUCCESS`，订单只推进一次，outbox 恰好 1 条 `payment.succeeded`，`notify_count = 10`，没有经本路由测过。

其他限制：`REFUND` 在 payment 返回 `10003`，网关因此回 HTTP 500。这条路径没有单独的测试。`ApplyChannelNotify` 的 `APPLIED` / `DUPLICATE` / `CONFLICT` 分支覆盖率停在函数的 25.0%，因为测试没有 MySQL。并发下结果字符串可能标错，HTTP 正文仍按成功处理；这个竞态没有测试。进程内计数在进程重启后归零。`/metrics` 仍是 404。

## 6. 如何复现

在仓库根目录执行。集成测试和覆盖率需要 Docker 或已有的测试数据库。不要把数据库地址写进报告。预期是每条命令退出码 0。

```bash
gofmt -l .
make lint
make test
make test-integration
make cover
```

`gofmt -l .` 没有输出。`make lint` 最后一行是 `0 issues.`。`make cover` 里 `service/payment/biz` 的语句覆盖率是 85.1%，全仓库 total 是 31.0%。

## 7. 讲解要点

1. 问：签名错了为什么不是 JSON 里的 50001？答：渠道只认正文恰好是 `SUCCESS` 或 `FAIL`。签名失败是 HTTP 401 和正文 `FAIL`，不写 JSON。
2. 问：未知单号为什么回成功？答：再重试也不会把订单变出来。回 `SUCCESS` 是让渠道停止。拒绝次数留在进程内计数里。订单行不变这件事还没有 MySQL 测试。
3. 问：网关会不会把 10000 分重算成手续费？答：不会。10000 分原样放进 RPC。手续费在已有的支付单创建里。

## 延伸阅读

- `docs/02-requirements.md` 的 GW-06 小节
- `docs/05-api.md` 的 §7 与版本 3.3.4
- `docs/reports/GW-02-payment-routes.md`
- `docs/reports/PAY-04-apply-result.md`

## 修订记录

| 日期 | 提交 | 修订内容 |
| --- | --- | --- |
| 2026-11-02 | 尚未提交 | 首版。对应代码提交 `dcd1718`。撰写时本报告还未提交。 |
| 2026-11-02 | 尚未提交 | 验收补齐。代码提交 `34671d0`。取代正文里“四件事都还没有在 MySQL 上证明”，以及 4.3 后三行、第 5 节前三条、4.5 里“后三条未勾”“开发计划没有勾选”、术语表 DUP_NOTIFY 的“还没有测”、讲解要点第 2 条末句。本修订撰写时报告提交还未发生。 |

### 2026-11-02 验收补齐

这一节取代正文里已经过时的结论。第一次报告提交是 `5f96b1b` `[GW-06] add report`（完整 `5f96b1b0d0420073e7673b522131d69eacf9bde1`）。当时未知单号、金额不一致、2 秒内 SUCCESS、DUP_NOTIFY 十次都还没有在 MySQL 上证明，所以状态是 DOING。代码提交 `34671d0` 补上了这三件事。签名错误那一条仍然只由原来的单元测试 `TestGW06BadSignature` 证明：HTTP 401，正文 `FAIL`，假 RPC 调用 0 次，没有读 MySQL。

新增文件只有 `service/payment/biz/callback_integration_test.go`。它带 `//go:build integration`。同一提交还改了需求、开发计划和面试笔记。生产代码、IDL、表、错误码、环境变量都没有改。`ApplyPayResult` 的 CAS 和 outbox 规则没有改。网关不算手续费，也不改余额。

测试里的 `gwNotify` 不是 Kitex 的 `HandleChannelNotify`。`service/payment/biz` 的测试不能 import `service/payment/handler`，否则和 handler 对 biz 的引用成环。`gwNotify` 只复制了 PAY 分支：空请求或非 PAY 返回已有的 `10003` 或 `10001`，然后调用真正的 `Payments.ApplyChannelNotify`。真正走到的是网关 HTTP、渠道签名、进程内拒绝计数、channel-mock 的通知器，以及 `ApplyChannelNotify` / `ApplyPayResult`。Kitex handler 函数本身没有被调用，所以它的覆盖率仍是 33.3%。

三条新测试：

| 验收 | 测试 | 证明了什么 |
| --- | --- | --- |
| 签名正确但 req_no 未知、金额不一致：HTTP 200，正文恰好 `SUCCESS`，订单不变，拒绝计数 +1 | `TestGW06RejectKeepsOrder` | 商户 `M100080`，商户单号 `GW06-REJ`，金额 10000 分（100.00 元）。先用假的渠道受理把订单停在 PAYING，不启动自动回调，避免和“订单不变”抢跑。未知单号 `PA0000000000000000099`：HTTP 200，正文 `SUCCESS`，`CallbackRejected("30001")` 加 1，`payment_order.pay_no` 和 `channel_txn.biz_no` 对这个单号的行数都是 0，原订单和原渠道流水与回调前逐字段相同。然后对真实 `pay_no` 把金额改成 1 分再投一次：HTTP 200，正文 `SUCCESS`，`CallbackRejected("50002")` 加 1，订单和渠道流水仍与回调前相同，流水状态 PROCESSING、金额 10000、`notify_count` 为 0，outbox 为空。 |
| Pay 返回 PAYING 后，channel-mock 自动完成并经本路由回调，2 秒内订单变为 SUCCESS | `TestGW06PaySucceedsWithinTwoSeconds` | 商户 `M100081`，商户单号 `GW06-FAST`，金额 10000 分，场景 NORMAL。测试把自动完成延迟的最小值和最大值都设成 50 毫秒，所以这次延迟就是 50 毫秒。生产默认最大延迟没有改。计时从 `Pay` 返回 PAYING 开始，2 秒内轮询到订单 SUCCESS。订单只有 1 行，`paid_at` 有值。渠道流水 SUCCESS，金额 10000，`notify_count` 为 1，`channel_txn_no` 已填。outbox 恰好 1 条 `payment.succeeded`，`biz_no` 等于 `pay_no`。mock 侧 1 行，场景 NORMAL，状态 SUCCESS，`notify_sent_count` 为 1，金额 10000。 |
| 渠道 10 次回调后订单只推进一次，outbox 恰好 1 条 `payment.succeeded`，`notify_count` 为 10 | `TestGW06DupNotifyTenCallbacks` | 商户 `M100082`，商户单号 `GW06-DUP`，金额 10003 分（100.03 元），末两位 03 是 DUP_NOTIFY。轮询上限 15 秒，直到 `notify_count` 为 10。订单只有 1 行且为 SUCCESS，金额 10003。渠道流水 SUCCESS，金额 10003，`notify_count` 为 10。outbox 恰好 1 条 `payment.succeeded`。mock 侧 1 行，场景 DUP_NOTIFY，状态 SUCCESS，`notify_sent_count` 为 10，金额 10003。 |

DUP_NOTIFY 有一个限制：channel-mock 在 POST 之前就把 `notify_sent_count` 加一，所以 mock 侧等于 10 不能单独证明这 10 次 HTTP 正文都是 `SUCCESS`。payment 的 `notify_count` 等于 10，表示 `ApplyPayResult` 的 NOTIFY 路径成功匹配了 10 次：第一次是 APPLIED，后面是 DUPLICATE。网关对这两种结果写 HTTP 200 和正文 `SUCCESS`。测试没有把这 10 次 HTTP 响应体记下来。后 5 次按真实时钟落在大约 +1 秒到 +5 秒，所以等待用的是 15 秒，没有放宽 2 秒那一条。2 秒那一条只约束 NORMAL 的 10000 分。

同一轮验证，2026-11-02，go1.27.2 darwin/arm64，golangci-lint 2.14.0 built with go1.27.1 from 114493f on 2026-09-24T10:54:53Z。日志起点 2026-11-02 10:39:56 +0800。五条命令都是退出码 0。这是验收补齐这一版的数字，不替换第 4.4 节。

| 命令 | 结果 |
| --- | --- |
| `gofmt -l .` | 退出码 0，无文件名。real 0.07s，user 0.30s，sys 0.06s |
| `make lint` | 退出码 0，`0 issues.`。real 3.15s，user 4.85s，sys 9.99s |
| `make test` | 退出码 0。real 8.81s，user 6.62s，sys 7.32s。根包 8.146s。新文件有 integration 构建标签，这条命令不编译它。`service/payment/biz` 与 `service/gateway` 为 cached |
| `make test-integration` | 退出码 0。real 57.33s，user 12.35s，sys 12.09s。根包 53.343s。`service/payment/biz` 54.747s。`service/gateway` 为 cached |
| `make cover` | 退出码 0。real 58.43s，user 16.25s，sys 18.20s。全仓库 total 31.1%（含生成代码，不是门槛）。`service/payment/biz` 51.665s / 86.1%（门槛内）。`service/payment/handler` cached 81.7%。`service/gateway` cached 76.2%（不在门槛内）。`cmd/gateway` cached 25.6%。`pkg/money` cached 100%，`pkg/sign` cached 100%，`pkg/idgen` cached 88.1%，`pkg/outbox` cached 95.0% |

同一次 `make cover` 的函数覆盖：`notify.go` 里的 `ApplyChannelNotify` 80.0%。`payment.go` 里的 `App.ApplyChannelNotify` 0.0%。`HandleChannelNotify` 33.3%。`cmd/gateway` 的 `serve` 7.4%。网关包是 cached，函数比例与第 4.4 节相同：`channelCallback` 74.0%，`parseCallback` 68.2%，`callbackResult` 81.8%，`writeCallback` 85.7%，`CallbackRejected` 85.7%，`addCallbackRejected` 83.3%，`registerCallback` 66.7%，`skew` 100%。新测试在 payment 的测试进程里调用了网关，但 `make cover` 只统计正在被测试的那个包，所以没有把这些执行算进 `service/gateway` 的 76.2%。

`make gen` 未运行。`mmdc` 未运行。`docs/06-flows.md` 没有改。没有用 paycli 打到正在运行的环境。

`34671d0` 同步的文档：需求版本仍是 v3.0，最后修改改为 2026-11-02。§3 的 GW-06 从 DOING 改为 DONE。后三条验收改为已勾，第一条保持已勾。§8.2 追加一行 2026-11-02，2026-11-01 那一行没有改写。开发计划里 GW-06 已勾。同一段下面的交付句 `paycli create → pay → query` 没有改，这次也没有执行。面试笔记加了一条：这三项由 payment 的集成测试打到进程内网关和 channel-mock，延迟固定 50 毫秒，生产默认延迟不变。

仍然没做的：`REFUND` 仍返回已有的 `10003`，这次没有单独测试，留给 REF-03。故障点 `callback.after_commit` 没有设成真的杀进程。进程内计数重启后归零，`/metrics` 仍是 404。周交付那一句要等以后对正在运行的环境执行 paycli，这次不能写成已经完成。
