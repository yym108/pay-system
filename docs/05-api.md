# pay-system 接口文档

| 项 | 值 |
|---|---|
| 文档版本 | 2.0.8 |
| API 版本 | v1（路径前缀 `/v1`） |
| 最后修改 | 2026-10-22 |
| 状态 | 设计基线，实现须与本文一致 |

关联文档：需求 `docs/02-requirements.md`（各接口的业务规则以功能点为准）、约定 `docs/03-conventions.md`（错误码、单号、时间格式）。

## 0. 版本管理

### 0.1 版本规则

文档版本采用 `MAJOR.MINOR.PATCH`：

| 级别 | 触发条件 | 要求 |
|---|---|---|
| MAJOR | 不兼容变更：删除或重命名字段、改变字段类型或语义、改变签名算法、删除接口 | 商户 API 必须新增路径版本（`/v2`），旧版本保留到迁移完成 |
| MINOR | 兼容变更：新增接口、可选字段、错误码、枚举值 | 调用方必须忽略未知字段、容忍未知枚举值 |
| PATCH | 文字修正、补充示例，不改变行为 | — |

每次修改接口：

1. 先改本文，再改代码；代码与本文不一致视为缺陷。
2. 更新文档头的“文档版本”和“最后修改”。
3. 在 §0.2 追加一行，写明影响的接口和对应功能点。
4. 更新 §1 总览表中该接口的“最后修改”列。
5. 涉及 IDL 时同步 `idl/*.thrift`，遵守“只增不删、新增字段 optional”。

### 0.2 变更记录

| 版本 | 日期 | 变更 | 影响接口 | 功能点 |
|---|---|---|---|---|
| 1.0.0 | 2026-10-12 | 初版。从需求文档 v2.0 §6 拆出，补全字段约束、错误码、示例、管理接口与模拟渠道接口参数、RPC 与事件定义 | 全部 | — |
| 1.1.0 | 2026-10-12 | 按 `docs/06-flows.md` R1–R14：请求体超限返回 413 / `10001`；关单渠道已失败返回 `30003` + FAILED 订单；创建退款新增 `30006`（渠道拒绝）、冻结冲突明确为 code 0；渠道回调未知单号与金额不一致返回 200；差错处理与事件重放规则补充；模拟渠道退款先幂等再校验额度 | §2.1、§4.3、§4.5、§7、§8、§9.5、§10.2 | GW-01、GW-06、PAY-06、REF-02、REC-07、MQ-05、CH-04 |
| 2.0.0 | 2026-10-12 | 随需求 v3.0。新增：创建转账 `POST /v1/transfers`（§4.8）、查询转账（§4.9）、`Transfer` 对象（§5.3）、通知与事件 `transfer.succeeded` / `transfer.failed`（只通知付款商户，account 不消费）；错误码 `30008`，`40002` 扩展到转账；AccountService 新增 `TransferLocal`、`TransferInTry` / `TransferInConfirm` / `TransferInCancel`，PaymentService 新增 `CreateTransfer`、`QueryTransfer`。修改：account RPC 改为以商户号为入参、由 account 路由分片（`Freeze` / `ReleaseFreeze` / `ConfirmFreeze` / `GetAccount` / `CheckBalance`），`OpenAccount` 增加 `shard`、`sub_no`，`GetAccount` 平台账户返回两分片子账户汇总；`CheckBalance` 增加 `expect_quiescent`、失败项带 `shard`；查询余额路由到商户所属分片；商户通知改为 30 秒固定间隔共 5 次；`RunBatch` 返回批次与前 100 条差错；`ReconDiff` 状态只保留 PENDING / RESOLVED / IGNORED，新增 `TRANSFER_MISMATCH`，`handle_action` 为 REQUERY / REPLAY；§1 总览增加 RPC 列；明确商户限流为验签后的进程内令牌桶。删除：管理接口批次列表、差错列表、差错处理、同步渠道状态、流水查询；RPC `Post`（改为进程内方法）、`Transfer`、`ListEntries`、`ListBatches`、`ListDiffs`、`ResolveDiff`；§2.6 分页（已无列表接口）。商户 API 只有兼容新增，路径版本仍为 `/v1`；删除项只涉及管理 API 与内部 RPC，尚无实现与存量调用方，IDL 按本版直接生成 | §1、§2.4、§2.5、§4.7–§4.9、§5.3、§6、§8、§10、§11 | ACC-01~07、ACC-10、ACC-11、ACC-14、TRF-01~05、NTF-01、NTF-02、GW-02、GW-05、GW-07、MQ-01、MQ-05、REC-01、REC-05、REC-06、REC-08 |
| 2.0.1 | 2026-10-14 | 澄清通用响应 `msg` 使用约定 §5.2 的稳定英文（成功 `ok`，`10003` 为 `internal error`，`40002` 为 `insufficient balance`）。不新增字段，不改路径、错误码数字和已有示例。§1 各接口的“最后修改”不变 | §2.3 | BASE-06 |
| 2.0.2 | 2026-10-19 | 文字对齐：FROZEN 或 CONFIRMED 必须金额和账户都一致才返回已有 `freeze_no`、`status`；RELEASED 一律 `40004`，不再比较金额；FROZEN 或 CONFIRMED 的金额或账户不一致为 `10006`。ACC-04 交付进程内 `biz.Freeze`，RPC handler 仍是 ACC-11，契约保留，响应不增加字段。不改错误码数字，不改 IDL。§1 各接口的“最后修改”不变 | AccountService.Freeze | ACC-04 |
| 2.0.3 | 2026-10-20 | 文字对齐：ReleaseFreeze 是进程内方法，RPC handler 仍是 ACC-11。空回滚账户不存在为 `40001`；FROZEN 金额或账户不一致为 `10006`；CONFIRMED 一律 `40004`；RELEASED 幂等且不再比较金额；解冻不要求账户 ACTIVE，不改 `balance`。不新增字段，不改错误码数字，不改 IDL。§1 各接口的“最后修改”不变 | AccountService.ReleaseFreeze | ACC-05 |
| 2.0.4 | 2026-10-20 | 文字对齐：ConfirmFreeze 是进程内方法，RPC handler 仍是 ACC-11。不新增字段、不改错误码数字、不改 IDL。§1 各接口的“最后修改”不变 | AccountService.ConfirmFreeze | ACC-06 |
| 2.0.5 | 2026-10-21 | 文字对齐：TransferLocal 是进程内方法，自己用 `dbx.WithTx` 提交本分片事务，RPC handler 仍是 ACC-11。不新增字段、不改错误码数字、不改 IDL。余额不足是业务结果 `FAILED` / `INSUFFICIENT_BALANCE`，不把 `40002` 列为该方法的业务错误。§1 各接口的“最后修改”不变 | AccountService.TransferLocal | ACC-07 |
| 2.0.6 | 2026-10-21 | 文字对齐：TransferInTry、TransferInConfirm、TransferInCancel 是进程内方法，各自用 `dbx.WithTx` 提交收款商户所在分片的事务，RPC handler 仍是 ACC-11。不新增字段、不改错误码数字、不改 IDL。Confirm 的业务错误补上 `10006` 与 `40001`，Cancel 的业务错误补上 `10006`。`10003` / `10004` 仍不进业务错误列。§1 各接口的“最后修改”不变 | AccountService.TransferInTry、TransferInConfirm、TransferInCancel | ACC-14 |
| 2.0.7 | 2026-10-21 | 文字对齐：CheckBalance 是进程内方法，自己打开 REPEATABLE READ 一致性快照，不用 `dbx.WithTx`，RPC handler 仍是 ACC-11。业务错误列仍只有 `40001`。`passed = false` 不是错误码。商户模式忽略 `expect_quiescent`。最后一条分录是 `max(id)`。一次失败的完成调用把进程内计数加 1。不新增字段、不改错误码数字、不改 IDL。§1 各接口的“最后修改”不变 | AccountService.CheckBalance、`POST /admin/accounts/check` | ACC-10 |
| 2.0.8 | 2026-10-22 | 文字对齐：AccountService 的 Kitex handler 由 ACC-11 提供，只做协议转换和错误码转换。业务错误走 TTHeader 的 `BizStatusError`；本服务没有携带对象的错误，响应不加 `biz_code`。`POST /admin/accounts/check` 仍是 GW-07，不在 account 进程上提供。不新增字段、不改错误码数字、不改 IDL。§1 各接口的“最后修改”不变 | §8.4、§10、§10.1 | ACC-11 |

## 1. 接口总览

| 类别 | 服务 / 端口 | 鉴权 | 调用方 |
|---|---|---|---|
| 商户 API | gateway :8080 | 商户 HMAC 签名 | 商户、paycli、k6 |
| 商户通知 | payment → `notify_url` | 平台签名 | 商户（merchant-demo） |
| 渠道回调入口 | gateway :8080 | 渠道签名 | channel-mock |
| 管理 API | gateway :8090（仅 127.0.0.1） | `X-Admin-Token` | 运营、paycli admin |
| 模拟渠道 API | channel-mock :9080；管理 :6064 | 渠道签名；管理端口不鉴权（仅 127.0.0.1） | payment、recon、测试 |
| 内部 RPC | account :8882、payment :8881、recon :8883 | 无（内网） | gateway、payment、recon |
| 事件 | Kafka | — | account（支付、退款事件）、payment（全部事件，用于商户通知） |

接口清单（“RPC”列即网关路由与 RPC 的映射，GW-02）：

| 接口 | 章节 | RPC | 功能点 | 引入版本 | 最后修改 |
|---|---|---|---|---|---|
| `POST /v1/payments` 创建支付单 | §4.1 | `PaymentService.CreatePayment` | PAY-02 | 1.0.0 | 2026-10-12 |
| `POST /v1/payments/{pay_no}/pay` 发起支付 | §4.2 | `PaymentService.Pay` | PAY-03 | 1.0.0 | 2026-10-12 |
| `POST /v1/payments/{pay_no}/close` 关闭支付单 | §4.3 | `PaymentService.ClosePayment` | PAY-06 | 1.0.0 | 2026-10-12 |
| `GET /v1/payments/{pay_no}`、`GET /v1/payments?out_trade_no=` 查询支付单 | §4.4 | `PaymentService.QueryPayment` | PAY-08 | 1.0.0 | 2026-10-12 |
| `POST /v1/refunds` 创建退款 | §4.5 | `PaymentService.CreateRefund` | REF-01、REF-02 | 1.0.0 | 2026-10-12 |
| `GET /v1/refunds/{refund_no}`、`GET /v1/refunds?out_refund_no=` 查询退款 | §4.6 | `PaymentService.QueryRefund` | REF-05 | 1.0.0 | 2026-10-12 |
| `GET /v1/balance` 查询余额 | §4.7 | `AccountService.GetAccount` | ACC-02 | 1.0.0 | 2026-10-12 |
| `POST /v1/transfers` 创建转账 | §4.8 | `PaymentService.CreateTransfer` | TRF-01~03 | 2.0.0 | 2026-10-12 |
| `GET /v1/transfers/{transfer_no}`、`GET /v1/transfers?out_transfer_no=` 查询转账 | §4.9 | `PaymentService.QueryTransfer` | TRF-05 | 2.0.0 | 2026-10-12 |
| 商户异步通知 | §6 | — | NTF-01、NTF-02 | 1.0.0 | 2026-10-12 |
| `POST /callback/mockpay` 渠道回调入口 | §7 | `PaymentService.HandleChannelNotify` | GW-06 | 1.0.0 | 2026-10-12 |
| `POST /admin/recon/run` 执行对账 | §8.2 | `ReconService.RunBatch` | REC-01、REC-08 | 1.0.0 | 2026-10-12 |
| `POST /admin/outbox/replay` 事件重放 | §8.3 | `PaymentService.ReplayEvent` | MQ-05 | 1.0.0 | 2026-10-12 |
| `POST /admin/accounts/check` 余额核对 | §8.4 | `AccountService.CheckBalance` | ACC-10 | 1.0.0 | 2026-10-12 |
| 模拟渠道 `pay` / `query` / `close` / `refund` / `bills` | §9.2–§9.6 | — | CH-01~04、CH-08 | 1.0.0 | 2026-10-12 |
| 模拟渠道回调 | §9.7 | — | CH-05 | 1.0.0 | 2026-10-12 |
| 模拟渠道管理 `config` / `bill-inject` | §9.8 | — | CH-07、CH-09 | 1.0.0 | 2026-10-12 |
| AccountService / PaymentService / ReconService | §10 | — | 见各方法 | 1.0.0 | 2026-10-12 |
| 事件 `payment.succeeded`、`refund.succeeded`、`refund.failed`、`transfer.succeeded`、`transfer.failed` | §11 | — | MQ-01、MQ-04、NTF-01 | 1.0.0 | 2026-10-12 |

## 2. 通用约定

### 2.1 传输

- HTTP/1.1，`Content-Type: application/json; charset=utf-8`；请求体上限 64KB，超过返回 HTTP 413 / `10001`（响应不签名）。
- 本地环境不启用 TLS（已知限制）。
- 无请求体的 POST 可以不传 body，签名时按空串计算。

### 2.2 数据格式

| 类型 | 规则 |
|---|---|
| 金额 | JSON 整数，单位分；小数、字符串、负数均视为参数错误 |
| 时间 | RFC 3339 毫秒，`2026-10-12T10:00:00.000+08:00`；未发生的时间字段省略 |
| 日期 | `YYYY-MM-DD`，东八区 |
| 枚举 | 大写字符串 |
| 字段名 | snake_case |
| 未知字段 | 忽略 |
| 类型错误 | 返回 `10001` |

### 2.3 响应结构

商户 API 与管理 API 统一返回：

```json
{
  "code": 0,
  "msg": "ok",
  "request_id": "0af7651916cd43dd8448eb211c80319c",
  "data": {}
}
```

| 字段 | 说明 |
|---|---|
| `code` | 业务码，`0` 成功，其他见 `docs/03-conventions.md` §5 |
| `msg` | 简短说明，不包含内部细节。取值是约定 §5.2 的对外 msg，例如成功 `ok`、`10003` 为 `internal error`、`40002` 为 `insufficient balance` |
| `request_id` | 请求 ID，同响应头 `X-Request-Id`，排查问题时提供 |
| `data` | 业务数据；错误时一般为空，部分错误携带当前对象（各接口中注明） |

HTTP 状态码按错误码表映射。调用方应以 `code` 为准判断结果。

### 2.4 通用错误

以下错误可能出现在任何商户 API，各接口不再重复列出：`10001`、`10003`、`10004`、`10005`、`20001`、`20002`、`20003`、`20004`。

`10005` 来自网关的商户限流（GW-05）：验签通过后、nonce 检查前，按 `app_id` 使用进程内令牌桶（默认 100 req/s、突发 200），超限返回 HTTP 429 并带 `Retry-After` 头。限流在每个网关实例内独立计数，不做全局限流。

### 2.5 幂等与重试

| 接口 | 幂等键 | 重复请求的结果 |
|---|---|---|
| 创建支付单 | `out_trade_no` | 参数一致返回已有订单（不论状态）；不一致返回 `10006` |
| 发起支付 | `pay_no` | PAYING 或 SUCCESS 返回当前订单 |
| 关闭支付单 | `pay_no` | 已 CLOSED 返回当前订单 |
| 创建退款 | `out_refund_no` | `pay_no` 与 `amount` 一致返回已有退款单（不论状态）；不一致返回 `10006` |
| 创建转账 | `out_transfer_no` | `payee_merchant_id`、`amount`、`notify_url` 一致返回已有转账单（不论状态，不在本次请求中重新执行）；不一致返回 `10006` |
| 查询类 | — | 天然幂等 |

商户在超时、`10003`、`10004` 时可以重试：业务参数保持不变，但必须使用新的 `X-Nonce` 和 `X-Timestamp` 重新签名，否则会收到 `20003`。

## 3. 签名与鉴权

### 3.1 商户请求签名

请求头：

| 头 | 必填 | 说明 |
|---|---|---|
| `X-App-Id` | 是 | 商户应用 ID |
| `X-Timestamp` | 是 | Unix 秒 |
| `X-Nonce` | 是 | 16–32 位 `[A-Za-z0-9]`，10 分钟内不可重复 |
| `X-Signature` | 是 | 签名，见下文 |

计算方法：

```
canonical = METHOD + "\n"
          + RequestURI + "\n"
          + X-Timestamp + "\n"
          + X-Nonce + "\n"
          + hex(sha256(body))
signature = base64(HMAC-SHA256(app_secret, canonical))
```

- `METHOD` 为大写方法名。
- `RequestURI` 为路径加原始查询串（含 `?`），与实际发送的字节完全一致；签名方不得在签名后重新编码或调整参数顺序。
- body 为实际发送的原始字节；无 body 时为空串，`hex(sha256(""))` 为 `e3b0c442...b855`。
- `hex` 为小写十六进制；`base64` 为标准编码（带 `=` 填充）。

### 3.2 时间窗口与 nonce

1. 服务器时间与 `X-Timestamp` 相差超过 300 秒 → `20002`。
2. 验签通过后检查 nonce：同一 `app_id` 下 10 分钟内重复 → `20003`。签名错误的请求不占用 nonce。
3. nonce 存储（Redis）超时或不可用时拒绝请求（`10004`，fail-closed）。

### 3.3 平台响应签名

验签通过的请求，响应带 `X-Timestamp`、`X-Nonce`、`X-Signature`：

```
canonical = X-Timestamp + "\n" + X-Nonce + "\n" + hex(sha256(response_body))
signature = base64(HMAC-SHA256(app_secret, canonical))
```

验签失败、时间戳过期等发生在验签之前或验签本身的错误，响应不带签名。商户应校验响应签名，校验失败时视为结果未知并查询确认。

### 3.4 商户通知签名

平台发往商户 `notify_url` 的请求，签名方式与 §3.3 相同（密钥为该商户的 `app_secret`），请求头为 `X-Timestamp`、`X-Nonce`、`X-Signature`。

### 3.5 渠道签名

平台与模拟渠道之间的请求、响应、回调都使用渠道密钥签名：

```
canonical = X-Mock-Timestamp + "\n" + body
signature = base64(HMAC-SHA256(channel_key, canonical))
```

请求头 `X-Mock-Timestamp`（Unix 秒）、`X-Mock-Signature`；时间窗口 ±300 秒；GET 请求 body 为空串。双方都必须验签。

### 3.6 管理 Token

管理 API 请求头 `X-Admin-Token` 与服务端配置 `ADMIN_TOKEN` 做常量时间比较，不一致 → 401 / `20005`。写操作必须带 `X-Admin-Operator`（操作人，1–64 字符），缺失 → `10001`。这是适合本地演示的简化鉴权，不提供细粒度权限。

### 3.7 测试向量

以下结果已用 Python `hmac` 计算，签名实现必须逐条通过。

商户密钥 `demo_secret_M100001_0123456789ab`：

| # | 场景 | canonical（`\n` 表示换行） | 签名 |
|---|---|---|---|
| V1 | 下单 | `POST\n/v1/payments\n1791500000\nAbCdEf0123456789\nc2e7954ac287ebcaff20ff9cdaf71f72e6cf4def6aec049a8b69a3c468e0a1e1` | `hFYKid0+k7q9vCvqh/i6YnvoaAJYzf2niNuvSkMCwoQ=` |
| V2 | 查询（空 body） | `GET\n/v1/payments?out_trade_no=T20261009001\n1791500000\nAbCdEf0123456789\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | `D1t81263+AFhLO6+FPSoVQqNFANBAKpi0akuyE9gVu4=` |
| V3 | 平台响应 | `1791500001\nZyXwVu9876543210\n8196328a69b72a8da9bd189c88ce3409371ce889ce2b414719d4095d5708571c` | `TuLVqqAxs3SyvTtDXp1cpv+Rpv2D5URAFywIHBNjAh4=` |

- V1 body：`{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`
- V3 body：`{"code":0,"msg":"ok","data":{"pay_no":"PA0000000000000000001"}}`

渠道密钥 `demo_channel_key_mockpay_0123456`：

| # | 场景 | canonical | 签名 |
|---|---|---|---|
| V4 | 渠道回调 | `1791500002\n{"txn_type":"PAY","req_no":"PA0000000000000000001","status":"SUCCESS"}` | `d1zUa8yWKf9rXU7JL+9BMbK+Mg5ZuQHLao4iLCVdKKY=` |

时间戳 `1791500000` 为 `2026-10-09T06:53:20+08:00`，测试中用 Fake 时钟固定到该时刻。

## 4. 商户 API

### 4.1 创建支付单

`POST /v1/payments` · PAY-02

请求：

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `out_trade_no` | string | 是 | 1–64 位 `[A-Za-z0-9_-]` | 商户订单号，商户内唯一 |
| `amount` | int64 | 是 | 1–10,000,000 | 金额，分 |
| `currency` | string | 否 | 只能为 `CNY` | 默认 `CNY` |
| `subject` | string | 是 | 1–128 字符 | 商品描述 |
| `notify_url` | string | 否 | ≤ 512，http/https，主机在平台白名单内 | 默认使用商户配置的通知地址 |
| `expire_minutes` | int | 否 | 1–120 | 默认 15 |

幂等比较字段：`amount`、`currency`、`subject`、`notify_url`（缺省字段按默认值填充后比较）。`expire_minutes` 不参与比较。

响应 `data`：`PaymentOrder`（§5.1），新建时 `status = INIT`。

业务错误：`10006`。

示例：

```json
// 请求
{"out_trade_no": "T20261012001", "amount": 10000, "subject": "demo",
 "notify_url": "http://merchant-demo:9100/notify", "expire_minutes": 15}

// 响应
{"code": 0, "msg": "ok", "request_id": "0af7651916cd43dd8448eb211c80319c",
 "data": {"pay_no": "PA0000000123456789012", "out_trade_no": "T20261012001", "amount": 10000,
          "fee_amount": 60, "refunded_amount": 0, "currency": "CNY", "subject": "demo",
          "status": "INIT", "expire_at": "2026-10-12T10:15:00.000+08:00",
          "created_at": "2026-10-12T10:00:00.000+08:00"}}
```

### 4.2 发起支付

`POST /v1/payments/{pay_no}/pay` · PAY-03

请求：

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `channel` | string | 是 | 目前只有 `mockpay` | 支付渠道 |

响应 `data`：`PaymentOrder`。正常为 `PAYING`，最终结果通过商户通知或查询获得。渠道结果未知（超时）时同样返回 `PAYING`。

业务错误：

| 码 | 情况 | data |
|---|---|---|
| 30001 | 订单不存在或不属于当前商户 | 空 |
| 30003 | 订单为 FAILED 或 CLOSED | 空 |
| 30004 | 订单已过期或剩余不足 30 秒 | 空 |
| 30006 | 渠道明确拒绝，订单已置为 FAILED | `PaymentOrder` |
| 30007 | 不支持的渠道 | 空 |

### 4.3 关闭支付单

`POST /v1/payments/{pay_no}/close` · PAY-06

请求：无 body。

响应 `data`：`PaymentOrder`，`status = CLOSED`。

业务错误：

| 码 | 情况 | data |
|---|---|---|
| 30001 | 订单不存在或不属于当前商户 | 空 |
| 30003 | 订单为 SUCCESS 或 FAILED；或关单时渠道返回已支付（订单推进为 SUCCESS）、渠道交易已失败（订单推进为 FAILED） | `PaymentOrder`（推进后的当前状态） |
| 10004 | 订单 PAYING 且渠道关单调用失败，订单状态未变 | 空 |

### 4.4 查询支付单

`GET /v1/payments/{pay_no}` 或 `GET /v1/payments?out_trade_no=<商户订单号>` · PAY-08

只能查询当前商户的订单。查询串方式必须且只能提供 `out_trade_no`。

响应 `data`：`PaymentOrder`。

业务错误：`30001`。

### 4.5 创建退款

`POST /v1/refunds` · REF-01、REF-02

请求：

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `pay_no` | string | 二选一 | 21 位平台单号 | 原支付单号 |
| `out_trade_no` | string | 二选一 | 同 §4.1 | 原商户订单号 |
| `out_refund_no` | string | 是 | 1–64 位 `[A-Za-z0-9_-]` | 商户退款单号，商户内唯一 |
| `amount` | int64 | 是 | ≥ 1，且累计退款不超过订单金额 | 退款金额，分 |
| `reason` | string | 否 | ≤ 255 字符 | 退款原因 |

`pay_no` 与 `out_trade_no` 同时提供或都不提供 → `10001`。退款通知发往原支付单的 `notify_url`。手续费不退。

响应 `data`：`RefundOrder`（§5.2）。

| 返回状态 | 含义 |
|---|---|
| `PROCESSING` | 已冻结商户资金并提交渠道，等待渠道结果 |
| `INIT` | 已受理，冻结结果暂未确定，平台会自动继续处理 |
| `FAILED` | 已失败。余额不足时伴随错误码 `40002`，渠道拒绝时伴随 `30006`；冻结冲突或幂等返回已失败的退款单时 `code = 0`，按 `fail_code` 判断原因 |

业务错误：

| 码 | 情况 | data |
|---|---|---|
| 30001 | 原订单不存在或不属于当前商户 | 空 |
| 30003 | 原订单不是 SUCCESS | 空 |
| 30005 | 累计退款超过订单金额 | 空 |
| 10006 | `out_refund_no` 已存在但 `pay_no` 或 `amount` 不同 | 空 |
| 40002 | 商户可用余额不足，退款单已置为 FAILED | `RefundOrder` |
| 30006 | 渠道明确拒绝退款，退款单已置为 FAILED（`fail_code = CHANNEL_REJECTED`），冻结资金随后解冻 | `RefundOrder` |

已知边界：支付成功到商户入账之间有秒级延迟，此期间发起的退款可能返回 `40002`，可换新的 `out_refund_no` 重试。

### 4.6 查询退款

`GET /v1/refunds/{refund_no}` 或 `GET /v1/refunds?out_refund_no=<商户退款单号>` · REF-05

响应 `data`：`RefundOrder`。

业务错误：`30001`。

### 4.7 查询余额

`GET /v1/balance` · ACC-02

网关调用 `AccountService.GetAccount(MERCHANT, merchant_id, MERCHANT_SETTLE)`，`merchant_id` 取自验签结果；account 路由到商户所属分片读取，不加锁。

响应 `data`：

| 字段 | 类型 | 说明 |
|---|---|---|
| `account_no` | string | 商户结算账户号 |
| `currency` | string | `CNY` |
| `balance` | int64 | 余额 |
| `frozen_amount` | int64 | 冻结金额（处理中的退款、处理中的跨分片转账付款方冻结） |
| `available_amount` | int64 | 可用余额 = `balance − frozen_amount` |

支付成功后入账是异步的（经 Kafka），余额有秒级延迟。

业务错误：`40001`。

### 4.8 创建转账

`POST /v1/transfers` · TRF-01、TRF-02、TRF-03

付款方为当前商户（`merchant_id` 只取自验签结果，请求中的同名字段忽略）。转账不收手续费。

请求：

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `out_transfer_no` | string | 是 | 1–64 位 `[A-Za-z0-9_-]` | 商户转账单号，付款商户内唯一 |
| `payee_merchant_id` | string | 是 | `M` + 6 位数字；存在、ACTIVE，且不是付款商户本身 | 收款商户号 |
| `amount` | int64 | 是 | 1–10,000,000 | 金额，分 |
| `remark` | string | 否 | ≤ 128 字符 | 备注 |
| `notify_url` | string | 否 | ≤ 512，http/https，主机在平台白名单内 | 默认使用商户配置的通知地址；只通知付款商户 |

幂等比较字段：`payee_merchant_id`、`amount`、`notify_url`（缺省按默认值填充后比较）。

处理：双方在同一账户分片时 `mode = LOCAL`，account 一个本地事务完成；否则 `mode = TCC`，payment 作为协调者驱动两个分片的 Try / Confirm / Cancel。请求内执行到结束或遇到结果未知为止，返回当时的转账单；未终结的转账由平台恢复任务推进（TRF-04）。

响应 `data`：`Transfer`（§5.3）。

| 返回状态 | 含义 | code |
|---|---|---|
| `SUCCESS` | 已完成，双方余额已变动 | 0 |
| `TRYING` | 结果暂未确定（如 account 超时），平台会自动继续处理 | 0 |
| `CONFIRMING` | 已决定完成，正在确认，最终为 SUCCESS | 0 |
| `CANCELLING` | 已决定取消，正在撤销，最终为 FAILED | 0 |
| `FAILED` | 已失败，资金未移动。本次请求执行到余额不足时伴随错误码 `40002`；其他原因（`TRY_TIMEOUT`、`ACCOUNT_INVALID`）以及幂等返回已失败的转账单时 `code = 0`，按 `fail_code` 判断 | `40002` 或 0 |

`status` 为数据库原始取值。商户应把 `TRYING`、`CONFIRMING`、`CANCELLING` 统一视为“处理中”，以通知或查询结果为准。

业务错误：

| 码 | 情况 | data |
|---|---|---|
| 10001 | 参数错误，含 `payee_merchant_id` 格式错误 | 空 |
| 10006 | `out_transfer_no` 已存在但 `payee_merchant_id`、`amount` 或 `notify_url` 不同 | 空 |
| 30008 | 收款商户不存在、已停用或与付款商户相同 | 空 |
| 40002 | 付款商户可用余额不足，转账单已为 FAILED（`fail_code = INSUFFICIENT_BALANCE`） | `Transfer` |

超时、`10003`、`10004` 时可用相同参数重试：返回已有转账单，不会重复转账。

示例（`M100001` 在分片 1，`M100002` 在分片 0，跨分片）：

```json
// 请求
{"out_transfer_no": "TR20261012001", "payee_merchant_id": "M100002", "amount": 500,
 "remark": "货款结算", "notify_url": "http://merchant-demo:9100/notify"}

// 响应：成功
{"code": 0, "msg": "ok", "request_id": "0af7651916cd43dd8448eb211c80319c",
 "data": {"transfer_no": "TF0000000123456789020", "out_transfer_no": "TR20261012001",
          "merchant_id": "M100001", "payee_merchant_id": "M100002", "amount": 500,
          "currency": "CNY", "mode": "TCC", "status": "SUCCESS",
          "created_at": "2026-10-12T10:00:00.000+08:00",
          "finished_at": "2026-10-12T10:00:00.042+08:00"}}

// 响应：余额不足
{"code": 40002, "msg": "insufficient balance", "request_id": "4bf92f3577b34da6a3ce929d0e0e4736",
 "data": {"transfer_no": "TF0000000123456789021", "out_transfer_no": "TR20261012002",
          "merchant_id": "M100001", "payee_merchant_id": "M100002", "amount": 9000000,
          "currency": "CNY", "mode": "TCC", "status": "FAILED", "fail_code": "INSUFFICIENT_BALANCE",
          "created_at": "2026-10-12T10:05:00.000+08:00",
          "finished_at": "2026-10-12T10:05:00.031+08:00"}}
```

### 4.9 查询转账

`GET /v1/transfers/{transfer_no}` 或 `GET /v1/transfers?out_transfer_no=<商户转账单号>` · TRF-05

只有付款商户可以查询；收款商户和其他商户查询返回 `30001`（不泄露存在性）。查询串方式必须且只能提供 `out_transfer_no`。

响应 `data`：`Transfer`。

业务错误：`30001`。

## 5. 数据对象

### 5.1 PaymentOrder

| 字段 | 类型 | 出现条件 | 说明 |
|---|---|---|---|
| `pay_no` | string | 总是 | 平台支付单号 |
| `out_trade_no` | string | 总是 | 商户订单号 |
| `amount` | int64 | 总是 | 订单金额 |
| `fee_amount` | int64 | 总是 | 手续费，下单时确定 |
| `refunded_amount` | int64 | 总是 | 已占用的退款额（处理中 + 成功） |
| `currency` | string | 总是 | `CNY` |
| `subject` | string | 总是 | 商品描述 |
| `status` | string | 总是 | `INIT` / `PAYING` / `SUCCESS` / `FAILED` / `CLOSED` |
| `channel` | string | 已发起支付 | 支付渠道 |
| `expire_at` | string | 总是 | 过期时间 |
| `paid_at` | string | SUCCESS | 渠道完成时间 |
| `closed_at` | string | CLOSED | 关闭时间 |
| `fail_code`、`fail_msg` | string | FAILED | 失败原因，`fail_code` 取值见 `docs/03-conventions.md` §5.4 |
| `created_at` | string | 总是 | 创建时间 |

### 5.2 RefundOrder

| 字段 | 类型 | 出现条件 | 说明 |
|---|---|---|---|
| `refund_no` | string | 总是 | 平台退款单号 |
| `out_refund_no` | string | 总是 | 商户退款单号 |
| `pay_no` | string | 总是 | 原支付单号 |
| `amount` | int64 | 总是 | 退款金额 |
| `currency` | string | 总是 | `CNY` |
| `reason` | string | 总是 | 退款原因，可为空串 |
| `status` | string | 总是 | `INIT` / `PROCESSING` / `SUCCESS` / `FAILED` |
| `succeeded_at` | string | SUCCESS | 渠道完成时间 |
| `fail_code`、`fail_msg` | string | FAILED | 失败原因 |
| `created_at` | string | 总是 | 创建时间 |

### 5.3 Transfer

| 字段 | 类型 | 出现条件 | 说明 |
|---|---|---|---|
| `transfer_no` | string | 总是 | 平台转账单号，`TF` 前缀 |
| `out_transfer_no` | string | 总是 | 商户转账单号 |
| `merchant_id` | string | 总是 | 付款商户号 |
| `payee_merchant_id` | string | 总是 | 收款商户号 |
| `amount` | int64 | 总是 | 转账金额 |
| `currency` | string | 总是 | `CNY` |
| `mode` | string | 总是 | `LOCAL`（同分片）/ `TCC`（跨分片） |
| `status` | string | 总是 | `TRYING` / `CONFIRMING` / `CANCELLING` / `SUCCESS` / `FAILED`；前三个为处理中 |
| `fail_code` | string | CANCELLING、FAILED | `INSUFFICIENT_BALANCE` / `TRY_TIMEOUT` / `ACCOUNT_INVALID`，见 `docs/03-conventions.md` §5.4 |
| `created_at` | string | 总是 | 创建时间 |
| `finished_at` | string | SUCCESS、FAILED | 完成时间 |

## 6. 商户异步通知

`POST <notify_url>` · NTF-01、NTF-02

触发：支付成功、退款成功、退款失败、转账成功、转账失败。支付失败和关闭不通知，商户通过查询获得。支付、退款通知发往原支付单的 `notify_url`；转账通知只发给付款商户，地址取转账单的 `notify_url`，收款商户不通知。

请求头：`Content-Type: application/json`、`X-Timestamp`、`X-Nonce`、`X-Signature`（§3.4）。

请求体：

| 字段 | 类型 | 说明 |
|---|---|---|
| `notify_id` | string | 通知单号（`NT` 前缀），同一事件的多次重试相同 |
| `event_type` | string | `payment.succeeded` / `refund.succeeded` / `refund.failed` / `transfer.succeeded` / `transfer.failed` |
| `merchant_id` | string | 接收通知的商户号（转账为付款商户） |
| `data` | object | 见下表 |

| event_type | `data` 字段 |
|---|---|
| `payment.succeeded` | `pay_no`、`out_trade_no`、`amount`、`fee_amount`、`status`（SUCCESS）、`paid_at` |
| `refund.succeeded` | `refund_no`、`out_refund_no`、`pay_no`、`amount`、`status`（SUCCESS）、`succeeded_at` |
| `refund.failed` | `refund_no`、`out_refund_no`、`pay_no`、`amount`、`status`（FAILED）、`fail_code` |
| `transfer.succeeded` | `transfer_no`、`out_transfer_no`、`payee_merchant_id`、`amount`、`status`（SUCCESS）、`finished_at` |
| `transfer.failed` | `transfer_no`、`out_transfer_no`、`payee_merchant_id`、`amount`、`status`（FAILED）、`fail_code`、`finished_at` |

示例：

```json
{"notify_id": "NT0000000123456789099", "event_type": "payment.succeeded", "merchant_id": "M100002",
 "data": {"pay_no": "PA0000000123456789012", "out_trade_no": "T20261012001", "amount": 10000,
          "fee_amount": 60, "status": "SUCCESS", "paid_at": "2026-10-12T10:00:01.234+08:00"}}

{"notify_id": "NT0000000123456789120", "event_type": "transfer.succeeded", "merchant_id": "M100001",
 "data": {"transfer_no": "TF0000000123456789020", "out_transfer_no": "TR20261012001",
          "payee_merchant_id": "M100002", "amount": 500, "status": "SUCCESS",
          "finished_at": "2026-10-12T10:00:00.042+08:00"}}
```

商户应答：HTTP 200，body 为 `SUCCESS`（允许首尾空白）表示接收成功；其他情况按固定间隔 30 秒重试，共发送 5 次（含首次，`NOTIFY_RETRY_INTERVAL`、`NOTIFY_MAX_ATTEMPTS`），仍失败则不再发送。平台不跟随重定向，超时 5 秒，响应体最多读取 1KB。

商户必须：先验签再处理；按 `pay_no` / `refund_no` / `transfer_no` 幂等处理（平台不保证只通知一次）；以查询结果为最终依据。

## 7. 渠道回调入口

`POST /callback/mockpay` · GW-06

调用方为模拟渠道，请求体与签名见 §9.7。不做 nonce 检查（回调处理本身幂等）。

响应为纯文本：

| 情况 | HTTP | body |
|---|---|---|
| 处理成功、重复回调、与本地终态冲突（已记录告警） | 200 | `SUCCESS` |
| 签名正确但 `req_no` 未知（`30001`）或金额与本地不一致（`50002`），订单不变，已告警并交给对账 | 200 | `SUCCESS` |
| 渠道签名错误或时间戳过期 | 401 | `FAIL` |
| 平台内部错误或依赖不可用，需要渠道重试 | 500 | `FAIL` |

## 8. 管理 API

### 8.1 约定

- 地址 `http://127.0.0.1:8090`，鉴权见 §3.6；响应结构同 §2.3，不签名。
- 通用错误：`20005`、`10001`、`10003`、`10004`。
- 写操作（执行对账、事件重放）必须带 `X-Admin-Operator`，并写入审计日志（操作人、接口、参数摘要、结果）。余额核对只读。
- 只提供以下三个接口（GW-07）；对账结果在执行接口的响应中返回，不提供批次、差错、流水的列表查询，也不提供差错人工处理接口。

### 8.2 执行对账

`POST /admin/recon/run` · REC-01、REC-08

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `channel` | string | 是 | `mockpay` |
| `bill_date` | string | 是 | 账期 `YYYY-MM-DD`，不能晚于今天 |
| `force` | bool | 否 | 已完成的批次是否重跑，默认 `false` |

响应 `data`：

| 字段 | 类型 | 说明 |
|---|---|---|
| `batch` | object | `ReconBatch`（§8.5） |
| `diffs` | array | 本批次的 `ReconDiff`，最多 100 条；总数见 `batch.diff_count` |

批次规则（REC-01）：已 DONE 且未指定 `force` → 直接返回已有结果；FAILED 或 `force = true` → 重跑；正在执行（INIT、DOWNLOADED、未超时的 CHECKING）→ 返回当前批次，不重复执行。批次 FAILED 时 `batch.error_msg` 记录原因。

业务错误：`10001`（渠道未配置、日期格式错误或晚于今天）。

### 8.3 事件重放

`POST /admin/outbox/replay` · MQ-05

请求二选一：

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | string | 按事件 ID 重放 |
| `biz_no` + `event_type` | string | 按业务单号重放（支付事件为 `pay_no`，退款事件为 `refund_no`，转账事件为 `transfer_no`）；原事件已清理时按单据当前数据重建 |

响应 `data`：`{"event_id": "EV...", "rebuilt": false}`。事件已是 PENDING（等待投递）时不做修改，同样返回 code 0、`rebuilt = false`。

业务错误：`10002`（事件不存在且不能重建）、`30003`（单据状态与事件类型不符，如 PAYING 订单重建 `payment.succeeded`、非 FAILED 转账单重建 `transfer.failed`）。

### 8.4 余额核对

`POST /admin/accounts/check` · ACC-10

ACC-10 交付进程内 `biz.CheckBalance`。RPC handler 由 ACC-11 转发到该方法。本 HTTP 接口仍是 GW-07，不在 account 进程上监听。本节契约保留。进程内方法不写行，自己在分片连接上打开一致性快照，不用 `dbx.WithTx`。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `merchant_id` | string | 否 | 只核对该商户在所属分片的结算账户；为空时全量核对两个分片 |
| `expect_quiescent` | bool | 否 | 默认 `false`。仅全量模式为 `true` 时额外校验两个分片在途合计为 0，只在全部转账已终结时使用（如 `verify.sh`）。商户模式忽略该参数，不因此返回 `10001` |

核对项：

| `check` | 模式 | 内容 |
|---|---|---|
| `BALANCE_SUM` | 全部 | 账户余额 = 按余额方向汇总的分录金额 |
| `LAST_BALANCE_AFTER` | 全部 | 最后一条分录按 `max(ledger_entry.id)` 取 `balance_after`，不用 `created_at`；等于当前余额。无分录且余额为 0 通过 |
| `FROZEN_SUM` | 全部 | `frozen_amount` = FROZEN 冻结单金额合计 |
| `AVAILABLE_NON_NEGATIVE` | 全部 | 不允许透支的账户可用余额 ≥ 0 |
| `TXN_BALANCED` | 全量 | 每张凭证借贷平衡、分录数 = `entry_count`、分录账户都属于本分片 |
| `GLOBAL_BALANCED` | 全量 | 本分片全部分录借方总额 = 贷方总额 |
| `TRANSIT_EXPECTED` | 全量 | 本分片 `TRANSIT` 子账户余额合计 = 本分片 CONFIRMED 的 `TRANSFER` 冻结金额 − 本分片 CONFIRMED 的收款方分支金额（不变量 11） |
| `TRANSIT_ZERO` | 全量 + `expect_quiescent` | 两个分片 `TRANSIT` 余额合计 = 0 |

每个分片用一条固定连接，按顺序执行 `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`、`START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT`、只读查询、`ROLLBACK`。两个分片是两次独立快照。商户模式只做该结算账户的前四项。`passed = false` 且 `failures` 非空时 error 为 nil，不是错误码；同一次完成调用把进程内无标签计数 `pay_ledger_check_failures_total` 加 1。`/metrics` 暴露仍是 OPS-01。

响应 `data`：

```json
{"passed": false,
 "failures": [{"shard": 1, "account_no": "AC0000000123456789030", "check": "BALANCE_SUM",
               "expected": 9940, "actual": 9941}]}
```

`failures` 中：`shard` 为分片号；`account_no` 为出错账户，分片级核对项（`TXN_BALANCED`、`GLOBAL_BALANCED`、`TRANSIT_EXPECTED`）省略；`TRANSIT_ZERO` 跨分片，`shard` 与 `account_no` 均省略。

业务错误：`40001`（点名的商户结算账户不存在：`MERCHANT` / `MERCHANT_SETTLE` / `CNY` / `sub_no = 0`）。全量模式不因某个商户缺失返回 `40001`。`passed = false` 不是业务错误。`10001`（商户号格式非法，先于 nil 判断）和 `10003` 不进业务错误列。

### 8.5 管理对象

`ReconBatch`：`batch_no`、`channel`、`bill_date`、`status`（INIT / DOWNLOADED / CHECKING / DONE / FAILED）、`channel_count`、`channel_amount`、`local_count`、`local_amount`、`matched_count`、`diff_count`、`error_msg`、`started_at`、`finished_at`。

`ReconDiff`：

| 字段 | 说明 |
|---|---|
| `diff_no`、`batch_no`、`channel`、`bill_date` | 差错与所属批次 |
| `diff_type` | `LOCAL_MISSING` / `CHANNEL_MISSING` / `AMOUNT_MISMATCH` / `STATUS_MISMATCH`（外部对账，REC-03）；`LEDGER_MISSING` / `TRANSFER_MISMATCH`（内部对账，REC-05） |
| `txn_type`、`biz_no` | 匹配键；`txn_type` 为 `PAY` / `REFUND`，`TRANSFER_MISMATCH` 为 `TRANSFER`，`biz_no` 为 `transfer_no` |
| `channel_txn_no`、`local_amount`、`channel_amount`、`local_status`、`channel_status` | 双方数据，无对应方时省略 |
| `status` | `PENDING` / `RESOLVED` / `IGNORED`，只由自动处理推进（REC-06） |
| `handle_action` | `REQUERY`（同步渠道状态）/ `REPLAY`（重放事件，保持 PENDING，下次批次复核凭证后置为 RESOLVED）；未处理时省略 |
| `handler` | 自动处理为 `system` |
| `handle_note` | 处理说明；`LEDGER_MISSING` 金额不等、`TRANSFER_MISMATCH` 时记录各方实际情况 |
| `resolved_at`、`created_at` | 时间 |

`LOCAL_MISSING`、`AMOUNT_MISMATCH`、`STATUS_MISMATCH`（本地已终态）、`TRANSFER_MISMATCH` 不自动处理，保持 PENDING，出现在执行对账的响应与 ERROR 日志中。

## 9. 模拟渠道 API

### 9.1 约定

- 业务地址 `http://channel-mock:9080`，签名见 §3.5。
- 业务结果在 body 的 `result` 字段中，HTTP 状态：业务处理完成 200、验签失败 401、内部错误 500。
- 交易状态：`PROCESSING` / `SUCCESS` / `FAILED` / `CLOSED`。
- 故障场景由金额末两位触发，见需求 CH-06。

### 9.2 支付受理

`POST /mockpay/v1/pay` · CH-01

| 请求字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `req_no` | string | 是 | 平台请求号，即 `pay_no` |
| `amount` | int64 | 是 | 金额 |
| `subject` | string | 是 | 商品描述 |
| `notify_url` | string | 是 | 回调地址 |

| 响应字段 | 说明 |
|---|---|
| `result` | `OK` / `REJECTED` |
| `reason` | `REJECTED` 时：`PARAM_CONFLICT`（同一 req_no 金额不同）、`ORDER_CLOSED`（该 req_no 已关闭） |
| `channel_txn_no` | 渠道流水号（`MC` 前缀），`OK` 时返回 |
| `status` | 当前交易状态 |

### 9.3 查询

`POST /mockpay/v1/query` · CH-02、CH-04

| 请求字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `txn_type` | string | 是 | `PAY` / `REFUND` |
| `req_no` | string | 是 | 平台请求号 |

响应：`result`（`OK` / `NOT_FOUND`）、`status`、`amount`、`channel_txn_no`、`finished_at`（终态时）。

### 9.4 关单

`POST /mockpay/v1/close` · CH-03

请求：`req_no`。响应：`result`（`OK` / `ALREADY_PAID`）、`status`。未知 `req_no` 也返回 `OK / CLOSED`，并阻止之后的受理。

### 9.5 退款受理

`POST /mockpay/v1/refund` · CH-04

| 请求字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `req_no` | string | 是 | 平台请求号，即 `refund_no` |
| `orig_req_no` | string | 是 | 原支付 `pay_no` |
| `amount` | int64 | 是 | 退款金额 |
| `notify_url` | string | 是 | 回调地址 |

响应同 §9.2；`REJECTED` 的 `reason` 增加 `ORIG_NOT_SUCCESS`、`REFUND_EXCEEDED`。先按 `req_no` 幂等返回已有结果，再校验原支付状态与累计额度。

### 9.6 账单下载

`GET /mockpay/v1/bills?date=YYYY-MM-DD` · CH-08

成功：HTTP 200，`Content-Type: text/csv`，响应头 `X-Bill-Sha256`（文件内容小写十六进制 SHA-256）、`X-Bill-Count`（数据行数）。

```
txn_type,req_no,channel_txn_no,amount,status,finished_at
PAY,PA0000000123456789012,MC0000000123456789001,10000,SUCCESS,2026-10-12T10:00:01.234+08:00
REFUND,RF0000000123456789013,MC0000000123456789002,3000,SUCCESS,2026-10-12T11:20:00.500+08:00
```

UTF-8 无 BOM，`\n` 换行，首行为表头，按 `finished_at`、`channel_txn_no` 排序，只包含 SUCCESS 交易。

账单未就绪：HTTP 200，`{"result": "BILL_NOT_READY"}`。

### 9.7 渠道回调

`POST <notify_url>` · CH-05

请求头 `X-Mock-Timestamp`、`X-Mock-Signature`。

| 字段 | 类型 | 说明 |
|---|---|---|
| `notify_id` | string | 本次发送的 ID，每次重试不同 |
| `txn_type` | string | `PAY` / `REFUND` |
| `req_no` | string | 平台请求号 |
| `channel_txn_no` | string | 渠道流水号 |
| `amount` | int64 | 金额 |
| `status` | string | `SUCCESS` / `FAILED` |
| `finished_at` | string | 渠道完成时间 |

平台应答 HTTP 200 且 body 为 `SUCCESS` 视为送达，否则按 5s、15s、30s、60s 重试，共最多 5 次。

### 9.8 模拟渠道管理接口

地址 `http://127.0.0.1:6064`，仅供测试与演练。

`GET /mockpay/admin/config`、`PUT /mockpay/admin/config` · CH-07（P1）

| 字段 | 类型 | 范围 | 说明 |
|---|---|---|---|
| `delay_min_ms`、`delay_max_ms` | int | ≥ 0 | 自动完成延迟 |
| `dup_notify_rate` | float | 0–1 | NORMAL 交易重复回调概率 |
| `drop_notify_rate` | float | 0–1 | NORMAL 交易不回调概率 |
| `accept_error_rate` | float | 0–1 | 受理接口返回 500 的概率 |
| `accept_timeout_rate` | float | 0–1 | 受理接口阻塞 5 秒的概率 |

这里的概率是配置值而非金额，允许使用浮点数。重启后恢复默认值。

`POST /mockpay/admin/bill-inject` · CH-09

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `date` | string | 是 | 账单日期 |
| `txn_type` | string | 是 | `PAY` / `REFUND` |
| `amount` | int64 | 是 | 金额 |
| `req_no` | string | 否 | 默认生成平台不存在的单号 |

响应：`{"req_no": "...", "channel_txn_no": "MC..."}`。

## 10. 内部 RPC

Kitex + Thrift，IDL 位于 `idl/`，以生成后的 IDL 为准；本节列出方法契约。业务错误使用 `BizStatusError`，经 TTHeader 传递，错误码见 `docs/03-conventions.md` §5。服务端和调用方都要启用 TTHeader meta handler，否则业务错误到不了调用方。例外是需要携带对象的错误（`CreateRefund`、`CreateTransfer` 的 `40002`，`30006`），按正常响应返回对象并在响应的 `biz_code` 字段中给出错误码（约定 §5.3 第 5 条）。AccountService 没有这类错误。超时与重试见需求 §5.4.1。HTTP 接口与 RPC 的对应见 §1。

### 10.1 AccountService（:8882）

account RPC 以商户号（或 owner）为入参，由 account 按商户号路由到所属分片（ACC-13），调用方不传账户号和分片号（`OpenAccount` 除外）；每次调用只在一个分片上开启事务。商户号格式非法一律返回 `10001`，下表不再重复。记账 `Post` 是 account 进程内方法（ACC-03，供 ConfirmFreeze、TransferLocal、TransferInConfirm 与事件消费使用），不提供 RPC。ACC-04 交付的冻结同样是进程内方法 `biz.Freeze`，RPC handler 仍是 ACC-11；下表保留 `Freeze` 的契约。ACC-05 交付的解冻同样是进程内方法 `biz.ReleaseFreeze`，RPC handler 仍是 ACC-11；下表保留 `ReleaseFreeze` 的契约。ACC-06 交付的确认同样是进程内方法 `biz.ConfirmFreeze`，RPC handler 仍是 ACC-11；下表保留 `ConfirmFreeze` 的契约。ACC-07 交付的同分片转账已经是进程内方法 `biz.TransferLocal`，RPC handler 仍是 ACC-11；下表保留 `TransferLocal` 的契约。ACC-14 交付的收款方分支已经是进程内方法 `biz.TransferInTry`、`biz.TransferInConfirm`、`biz.TransferInCancel`，各自用 `dbx.WithTx` 提交收款商户所在分片的事务，RPC handler 仍是 ACC-11；下表保留这三行的契约。ACC-10 交付的余额核对是进程内方法 `biz.CheckBalance`，自己打开 REPEATABLE READ 一致性快照，不用 `dbx.WithTx`，不写行，RPC handler 仍是 ACC-11；下表保留 `CheckBalance` 的契约。

| 方法 | 请求 | 响应 | 幂等键 | 业务错误 | 调用方 | 功能点 |
|---|---|---|---|---|---|---|
| `OpenAccount` | `owner_type`、`owner_id`、`account_type`、`shard`、`sub_no` | `Account` | `(owner_type, owner_id, account_type, sub_no)`，分片内唯一 | 10001 | 种子、测试（内部方法） | ACC-01 |
| `GetAccount` | `owner_type`、`owner_id`、`account_type` | `Account` | — | 40001 | gateway（查询余额）、测试 | ACC-02 |
| `Freeze` | `biz_type`（`REFUND` / `TRANSFER`）、`biz_no`（`refund_no` / `transfer_no`）、`merchant_id`、`amount` | `freeze_no`、`status` | `(biz_type, biz_no)`，分片内唯一 | 10001、10006、40001、40002、40004、40005 | payment（REF-02、TRF-03 Try） | ACC-04 |
| `ReleaseFreeze` | `biz_type`、`biz_no`、`merchant_id`、`amount` | `freeze_no`、`status` | `(biz_type, biz_no)` | 10001、10006、40001、40004 | payment（TRF-03 Cancel） | ACC-05 |
| `ConfirmFreeze` | `biz_type`、`biz_no`、`merchant_id`、`channel` | `freeze_no`、`txn_no` | `(biz_type, biz_no)` | 10001、10006、40001、40004、40005 | payment（TRF-03 Confirm） | ACC-06 |
| `TransferLocal` | `transfer_no`、`payer_merchant_id`、`payee_merchant_id`、`amount` | `status`（`SUCCESS` / `FAILED`）、`txn_no`（SUCCESS）、`fail_code`（FAILED） | `(transfer_no, LOCAL)`，分片内唯一 | 10001、10006、40001、40005 | payment（TRF-02） | ACC-07 |
| `TransferInTry` | `transfer_no`、`payee_merchant_id`、`amount` | `status`（`TRIED`，或已有 `CONFIRMED` 且不返回 `txn_no`） | `(transfer_no, PAYEE)`，分片内唯一 | 10006、40001、40004、40005 | payment（TRF-03 Try） | ACC-14 |
| `TransferInConfirm` | `transfer_no`、`payee_merchant_id` | `status`（`CONFIRMED`）、`txn_no` | `(transfer_no, PAYEE)` | 10006、40001、40004、40005 | payment（TRF-03 Confirm） | ACC-14 |
| `TransferInCancel` | `transfer_no`、`payee_merchant_id`、`amount` | `status`（`CANCELLED`） | `(transfer_no, PAYEE)` | 10006、40004 | payment（TRF-03 Cancel） | ACC-14 |
| `CheckBalance` | `merchant_id`（为空时全量）、`expect_quiescent` | `passed`、`failures[{shard, account_no, check, expected, actual}]` | — | 40001 | gateway（管理接口）、测试、`verify.sh` | ACC-10 |
| `Reverse` | `shard`、`txn_no`、`reason` | `reversal_txn_no`、`duplicated` | 原 `txn_no` | 10001、40001、40002 | 内部方法（P1），业务流程不调用 | ACC-08 |

方法语义：

- `GetAccount`：MERCHANT 读取商户所属分片的账户；PLATFORM 读取两个分片全部子账户并求和，`account_no` 为空，`sub_count` 为参与汇总的行数，不在交易链路中调用。
- `Freeze`：只冻结商户结算账户（透支账户 → `10001`）。已存在的冻结单为 FROZEN 或 CONFIRMED，且金额和账户都一致 → 返回已有 `freeze_no`、`status`；RELEASED → `40004`（防悬挂），不再比较金额；FROZEN 或 CONFIRMED 的金额或账户不一致 → `10006`。余额不足 `40002`，不留下冻结单。响应仍是 `freeze_no`、`status`。
- `ReleaseFreeze`：进程内方法，自己用 `dbx.WithTx` 提交本分片事务，RPC handler 仍是 ACC-11。校验与 Freeze 相同，不合法为 `10001`，不写行。FROZEN 且金额、账户都一致 → RELEASED，`frozen_amount` 减少，`balance` 不变，冻结单和账户的 `version` 各加 1；不要求账户 `ACTIVE`，不返回 `40005`。金额或账户不一致 → `10006`，旧行不变。已 RELEASED 幂等成功，不再比较金额。已 CONFIRMED 一律 `40004`，不再比较金额。冻结单不存在时写入 RELEASED 墓碑并返回成功（空回滚），不改账户；账户不存在 → `40001`。账户更新影响 0 行 → `10003` 并回滚，冻结单保持 FROZEN。退款解冻由 account 消费 `refund.failed` 在进程内执行（MQ-04），不经过 RPC。响应仍是 `freeze_no`、`status`。
- `ConfirmFreeze`：进程内方法，自己用 `dbx.WithTx` 提交本分片事务，RPC handler 仍是 ACC-11。请求没有金额，R 只用冻结单 `amount`。`REFUND` 的 `channel` 非空且不超过 32 个码点，贷本分片该渠道 `CHANNEL_RECEIVABLE` 子账户，凭证 `REFUND` / `refund_no`，渠道应收减少 R。`TRANSFER` 的 `channel` 必须是空串，贷本分片 `TRANSIT` 子账户，凭证 `TRANSFER_OUT` / `transfer_no`，在途增加 R；冻结单自己的 `biz_type` 仍是 `TRANSFER`。商户腿把 `balance` 与 `frozen_amount` 同减 R，不要求账户 `ACTIVE`，不返回 `40002`。平台贷方不是 `ACTIVE` → `40005`，回滚后冻结单保持 FROZEN。FROZEN 但账户不是该商户结算账户 → `10006`。本分片平台子账户不存在 → `40001`。第一次成功 `duplicated = false`，返回已有 `freeze_no` 和这次的 `txn_no`。已 CONFIRMED 返回已有 `txn_no`，不再比较 `channel`，不再记账。RELEASED 或不存在 → `40004`，不插入。`duplicated` 不进 RPC，响应仍是 `freeze_no`、`txn_no`。退款确认由 account 消费 `refund.succeeded` 在进程内执行（MQ-04），不经过 RPC。
- `TransferLocal`：进程内方法，自己用 `dbx.WithTx` 提交本分片事务，RPC handler 仍是 ACC-11。参数校验在 nil service / nil ctx 之前，不合法为 `10001`，不写行。`transfer_no` 为 1–32 个码点，不强制 `TF` 前缀；金额必须是正的 int64 分；双方都能被 `shard.Of` 解析、同一分片、且不是同一商户。事务前普通 SELECT 双方 `MERCHANT_SETTLE` / `CNY` / `sub_no = 0`，不加行锁、不比较余额；任一不存在为 `40001`（优先于非 ACTIVE），两个都在但任一不是 `ACTIVE` 为 `40005`。事务 1 插入 `LOCAL` / `SUCCESS`（`txn_no` 空、`version` 0）后调用公开 `Post(TRANSFER, transfer_no, memo 空串)`，借付款商户、贷收款商户，可用余额条件是 `balance - frozen_amount`。`Post` 的 `error` 为 nil 时，无论 `Duplicated` 是 true 还是 false，都回写已有 `txn_no` 并提交 SUCCESS；`Duplicated` true 表示凭证已在、这次没有改余额。回写影响 0 行是 `10003`，回滚，不能留下没有 `txn_no` 的 SUCCESS。只有 `Post` 返回 `40002` 才在事务 1 回滚后开事务 2，插入 `FAILED` 墓碑，`fail_code` 为 `INSUFFICIENT_BALANCE`，成功时 error 为 nil、`txn_no` 为空，不把 `40002` 返回给调用方。`Post` 的其他错误只回滚事务 1，不写墓碑，把该错误返回给调用方。1062 先回滚，再在事务外读已有 `LOCAL` 分支：金额或双方不一致为 `10006`；SUCCESS 且 `txn_no` 非空返回该 `txn_no`，不再记账；SUCCESS 但 `txn_no` 为空是 `10003`；FAILED 返回库里的 `fail_code`，不再看当前余额。读不到行、未知 status、FAILED 却没有 `fail_code` 是 `10003`。不新增字段，响应仍是 `status`、`txn_no`、`fail_code`。
- `TransferInTry`：进程内方法，自己用 `dbx.WithTx` 提交收款商户所在分片的事务，RPC handler 仍是 ACC-11。参数校验在 nil service / nil ctx 之前，不合法为 `10001`，不写行，即使 service 为 nil 或 ctx 已取消。`transfer_no` 为 1–32 个码点，不强制 `TF` 前缀；金额必须是正的 int64 分；收款商户必须能被 `shard.Of` 解析。参数合法后 nil service、nil ctx、nil pools 为 `10003`；已取消或已过截止时间的 ctx 原样返回，不启事务；ShardDB 失败为 `10003`。不选平台子账户，`subCount < 1` 或发号器为空不是参数错误。先普通 SELECT 收款商户 `MERCHANT_SETTLE` / `CNY` / `sub_no = 0`，不加行锁、不读余额。不存在（含非 CNY 选不中）为 `40001`；存在但不是 `ACTIVE` 为 `40005`。都不写行。账户通过后事务里 INSERT `role = PAYEE`、`status = TRIED`、请求金额、`payer_merchant_id` 与 `fail_code` 与 `txn_no` 均为 SQL NULL、`version` 0。不动余额，不调用 Post。1062 先回滚，再在事务外读已有 PAYEE 行：CANCELLED 返回 `40004`，不比较金额和收款商户；TRIED 或 CONFIRMED 且金额或收款商户不一致为 `10006`，不写；一致则返回库里的 status，不记账，CONFIRMED 也不返回 `txn_no`。读不到行或未知 status 为 `10003`，不循环。同一 `transfer_no` 的 LOCAL 行不冲突，不读不改。成功时 `txn_no` 为空。返回错误时 `status` 和 `txn_no` 都是空字符串。不新增字段。
- `TransferInConfirm`：进程内方法，自己用 `dbx.WithTx` 提交同一分片事务，RPC handler 仍是 ACC-11。没有金额。校验同样先于 nil 判断，不合法为 `10001`。参数合法后 nil service、nil ctx、nil pools、发号器为空或 `subCount < 1` 为 `10003`；已取消或已过截止时间的 ctx 原样返回，不调用发号器。子账户号在开事务前用 `shard.Sub(transfer_no, subCount)`，失败为 `10003`，不写。事务内先 `SELECT ... FOR UPDATE` 锁 PAYEE 行，再由公开 Post 按账户 id 升序锁账户。没有行或 CANCELLED 为 `40004`，不插入，不记账。CONFIRMED 且 `txn_no` 非空则返回该 `txn_no`，不再比较收款商户，不再记账。CONFIRMED 但 `txn_no` 为空，或未知 status，为 `10003`，不写。TRIED 但收款商户不一致为 `10006`，不记账。TRIED 且收款商户一致时，金额只用行上的 `amount`。本分片 `TRANSIT`（`PLATFORM` / `PLATFORM` / `CNY` / `sub_no = shard.Sub`）不存在，或商户结算账户此时不存在，为 `40001`，不调用 Post，回滚后分支仍是 TRIED。两个账户 id 相同或任一为 0 是 `10003`。然后 `Post(ctx, tx, TRANSFER_IN, transfer_no, memo 空串)`：借在途、贷收款商户，`FrozenAccountID` 保持 0。在途允许透支，正常行不会因为在途不足返回 `40002`；若在途被改成不许透支且不够扣，`40002` 原样返回，回滚，保持 TRIED，不写墓碑。Post 返回 `40005` 原样返回，回滚，保持 TRIED。本方法不增加 `pay_transfer_inconsistent_total`。Post 的 error 为 nil 时，无论 `Duplicated` 是 true 还是 false，都回写 `PostResult.TxnNo` 并提交 CONFIRMED。`Duplicated` true 表示凭证已在、这次没改余额。这和 ACC-06 不同。回写 WHERE 为 `role = PAYEE AND status = TRIED AND txn_no IS NULL` 且 amount、payee 等于刚读到的值，`version` 加 1。影响 0 行或凭证号为空是 `10003` 并回滚。持有 FOR UPDATE 时 0 行不是并发幂等。`10003` / `10004` 不进上表业务错误列。成功时 `txn_no` 非空。返回错误时 `status` 和 `txn_no` 都是空字符串。不新增字段。
- `TransferInCancel`：进程内方法，自己用 `dbx.WithTx` 提交同一分片事务，RPC handler 仍是 ACC-11。校验与 Try 相同，不合法为 `10001`，即使 service 为 nil 或 ctx 已取消。参数合法后的 nil 依赖、取消的 ctx、ShardDB 失败与 Try 相同。不查账户，不选平台子账户，所以没有 `40001` / `40005`。事务内先 `SELECT ... FOR UPDATE`。没有行：INSERT CANCELLED，金额和收款商户用请求值，payer、fail_code、txn_no 均为 SQL NULL，`version` 0。不改余额。1062 先回滚，再按已有行处理。已 CANCELLED 幂等成功，不再比较金额和收款商户。已 CONFIRMED 为 `40004`，不再比较金额。TRIED 且金额或收款商户不一致为 `10006`，不改行。TRIED 且一致则 CAS 为 CANCELLED，`version` 加 1。余额不变，不调用 Post。持有 FOR UPDATE 时 CAS 影响 0 行是 `10003` 并回滚。未知 status 为 `10003`。1062 之后读到匹配的 TRIED，再开一个事务锁住后 CAS；第二次仍是 1062 则为 `10003`，不空转。成功时 `txn_no` 为空。返回错误时 `status` 和 `txn_no` 都是空字符串。不新增字段。
- `CheckBalance`：进程内方法，RPC handler 仍是 ACC-11。核对项与 `failures` 字段规则见 §8.4。`merchant_id` 为空是全量，不是 `10001`。非空商户号不能被 `shard.Of` 解析时 `10001`，先于 nil service、nil ctx 和已取消的 ctx，不查询。参数合法后 nil service、nil ctx、nil pools、ShardDB 失败、SQL 错误、未知余额方向或金额溢出为 `10003`，`failures` 为空；已取消或已过期的 ctx 原样返回，不开事务。`40001` 只表示点名的商户结算账户不存在。商户模式忽略 `expect_quiescent`，只核对该结算账户的前四项。最后一条分录是 `max(ledger_entry.id)` 的 `balance_after`，不是 `created_at`。`passed = false` 时 error 为 nil，不是错误码；同一次完成调用把进程内无标签计数 `pay_ledger_check_failures_total` 加 1，并打一条 ERROR 日志。成功、`40001`、`10003` 不加计数。不写行，不调用外部系统。不新增字段。
- 平台子账户由 account 按 `crc32(biz_no) % PLATFORM_SUB_ACCOUNTS` 选择，调用方不感知。

超时重试（ACC-11 第 4 条）：payment 使用 `pkg/accountclient`。连接 100ms，调用 500ms。只对以下幂等方法在超时时重试 1 次：`Freeze`、`ReleaseFreeze`、`ConfirmFreeze`、`TransferLocal`、`TransferInTry`、`TransferInConfirm`、`TransferInCancel`、`GetAccount`。`OpenAccount`、`CheckBalance`、`Reverse` 不自动重试。业务错误不重试。gateway 不使用这个客户端。

`Account`：`account_no`、`owner_type`、`owner_id`、`account_type`、`shard`、`sub_no`、`currency`、`balance_direction`、`balance`、`frozen_amount`、`available_amount`、`status`、`sub_count`。PLATFORM 汇总结果中 `account_no` 为空，`shard`、`sub_no` 省略。

### 10.2 PaymentService（:8881）

商户类方法的 `merchant_id` 由网关从验签结果填入；`CreateTransfer` 的 `merchant_id` 即付款商户。

| 方法 | 请求 | 响应 | 业务错误 | 调用方 | 功能点 |
|---|---|---|---|---|---|
| `CreatePayment` | `merchant_id` + §4.1 字段 | `PaymentOrder` | 10001、10006 | gateway | PAY-02 |
| `Pay` | `merchant_id`、`pay_no`、`channel` | `PaymentOrder` | 30001、30003、30004、30006、30007 | gateway | PAY-03 |
| `ClosePayment` | `merchant_id`、`pay_no` | `PaymentOrder` | 30001、30003、10004 | gateway | PAY-06 |
| `QueryPayment` | `merchant_id`、`pay_no` 或 `out_trade_no` | `PaymentOrder` | 30001 | gateway | PAY-08 |
| `CreateRefund` | `merchant_id` + §4.5 字段 | `RefundOrder` | 30001、30003、30005、30006、10006、40002 | gateway | REF-01、REF-02 |
| `QueryRefund` | `merchant_id`、`refund_no` 或 `out_refund_no` | `RefundOrder` | 30001 | gateway | REF-05 |
| `CreateTransfer` | `merchant_id` + §4.8 字段 | `Transfer` | 10001、10006、30008、40002（携带 `Transfer`） | gateway | TRF-01~03 |
| `QueryTransfer` | `merchant_id`、`transfer_no` 或 `out_transfer_no` | `Transfer` | 30001 | gateway | TRF-05 |
| `HandleChannelNotify` | `channel` + §9.7 字段 | `result`：`APPLIED` / `DUPLICATE` / `CONFLICT` | 30001、50002 | gateway | GW-06、PAY-04、REF-03 |
| `GetMerchantCredential` | `app_id` | `merchant_id`、`status`、`app_secret`、`secret_version`、`default_notify_url` | 20004 | gateway | PAY-01 |
| `SyncChannelTxn` | `biz_no` | `local_status`（同步后的本地渠道交易状态）、`channel_status`（`NOT_FOUND` 表示渠道无记录）、`changed` | 30001 | recon（REC-06 `REQUERY`） | PAY-09 |
| `ReplayEvent` | `event_id`，或 `biz_no` + `event_type` | `event_id`、`rebuilt` | 10002、30003 | gateway（§8.3）、recon（REC-06 `REPLAY`） | MQ-05 |

`GetMerchantCredential` 返回明文密钥，只允许网关调用；调用双方都不得记录响应内容。

`CreateTransfer` 的响应码规则同 §4.8：本次请求执行到 FAILED 且 `fail_code = INSUFFICIENT_BALANCE` → `biz_code = 40002` 并携带转账单；其他 FAILED、SUCCESS、处理中状态与幂等命中 → 成功返回。

### 10.3 ReconService（:8883）

| 方法 | 请求 | 响应 | 业务错误 | 调用方 | 功能点 |
|---|---|---|---|---|---|
| `RunBatch` | `channel`、`bill_date`、`force` | `batch`（`ReconBatch`）、`diffs`（`ReconDiff`，≤ 100 条） | 10001 | gateway（§8.2）、recon 定时任务、CLI | REC-01、REC-08 |

## 11. 事件

### 11.1 Topic

| topic | event_type | 生产者 | 消费组 | 分区 |
|---|---|---|---|---|
| `pay.payment.succeeded` | `payment.succeeded` | payment（outbox） | `account-ledger`、`payment-notify` | 3 |
| `pay.refund.succeeded` | `refund.succeeded` | payment（outbox） | `account-ledger`、`payment-notify` | 3 |
| `pay.refund.failed` | `refund.failed` | payment（outbox） | `account-ledger`、`payment-notify` | 3 |
| `pay.transfer.succeeded` | `transfer.succeeded` | payment（outbox） | `payment-notify` | 3 |
| `pay.transfer.failed` | `transfer.failed` | payment（outbox） | `payment-notify` | 3 |
| `pay.dlq` | 原类型 | 消费框架 | 无（排查后通过事件重放修复，MQ-05） | 3 |

| 事件 | 消息 key | outbox `biz_no`（按业务单号重放的键） |
|---|---|---|
| `payment.succeeded` | `pay_no` | `pay_no` |
| `refund.succeeded`、`refund.failed` | 原支付单 `pay_no` | `refund_no` |
| `transfer.succeeded`、`transfer.failed` | `transfer_no` | `transfer_no` |

account 不消费转账事件：转账的两个分支由 payment 同步驱动（TRF-02、TRF-03），转账事件只用于商户通知。

### 11.2 消息头

| 头 | 说明 |
|---|---|
| `event_id` | 同信封 |
| `event_type` | 同信封 |
| `traceparent` | W3C trace context |
| `x-orig-topic`、`x-orig-partition`、`x-orig-offset`、`x-error` | 仅死信消息 |

### 11.3 信封

```json
{"event_id": "EV0000000123456789100", "event_type": "payment.succeeded", "version": 1,
 "occurred_at": "2026-10-12T10:00:01.300+08:00",
 "trace_context": {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
 "data": {}}
```

| 字段 | 说明 |
|---|---|
| `event_id` | 事件 ID，`EV` 前缀 |
| `event_type` | 事件类型 |
| `version` | 信封版本，当前为 1 |
| `occurred_at` | 业务事务提交时间 |
| `trace_context` | 写入 outbox 时的链路上下文 |
| `data` | 见 §11.4 |

### 11.4 data

| 事件 | 字段 |
|---|---|
| `payment.succeeded` | `pay_no`、`merchant_id`、`out_trade_no`、`channel`、`amount`、`fee_amount`、`paid_at` |
| `refund.succeeded` | `refund_no`、`pay_no`、`merchant_id`、`out_refund_no`、`channel`、`amount`、`succeeded_at` |
| `refund.failed` | `refund_no`、`pay_no`、`merchant_id`、`out_refund_no`、`amount`、`fail_code` |
| `transfer.succeeded` | `transfer_no`、`out_transfer_no`、`merchant_id`（付款方）、`payee_merchant_id`、`amount`、`mode`、`status`（SUCCESS）、`finished_at` |
| `transfer.failed` | `transfer_no`、`out_transfer_no`、`merchant_id`（付款方）、`payee_merchant_id`、`amount`、`mode`、`status`（FAILED）、`fail_code`、`finished_at` |

`merchant_id` 是 account 消费时路由分片的依据（ACC-13），支付、退款事件必须携带；`channel` 用于选择本分片的渠道应收子账户。转账事件的 `merchant_id` 决定通知接收方。

### 11.5 投递语义

- 至少一次投递，可能重复、可能跨 topic 乱序；业务正确性不依赖顺序。
- 消费端以业务键幂等：`ledger_txn.uk_biz`、`account_freeze.uk_biz`（均为分片内唯一）、`merchant_notify.uk_event_id`。
- 事件结构只增字段；消费端忽略未知字段；不兼容变更时提升 `version` 并让消费端同时支持新旧版本。
