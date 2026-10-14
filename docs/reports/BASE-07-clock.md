# BASE-07 时钟

| 项 | 内容 |
|---|---|
| 功能点 | BASE-07（P0，第 1 周） |
| 所属链路 | 基础设施 |
| 代码提交 | `15e2022` `[BASE-07] add injectable shanghai clock`（完整 `15e2022fbc26d52483b693b67b173877f41b18c5`，2026-10-15 00:31:05 +0800） |
| 分支 | `feat/BASE-07-clock` |
| 日期 | 2026-10-15 |
| 状态 | DONE |

## 一句话结论

业务当前时间改由可注入的 `Clock` 提供。生产实现 `Real` 用固定东八区（地点名 `Asia/Shanghai`，始终 UTC+8），不读时区文件；测试实现 `Fake` 可以精确把时间拨快或拨慢。`Fake.Advance(10s)` 后时间正好多 10 秒，`pkg/clock` 语句覆盖率 100.0%。

## 1. 背景

### 1.1 系统简介

pay-system 是一个用 Go 编写的小型支付后端，模拟商户收款、退款、商户间转账三条资金链路，重点在数据一致性和性能。系统由 5 个部分组成：gateway（对外 HTTP 入口，负责签名校验和限流）、payment（支付单、退款单、转账单的状态管理和流程推进）、account（账户余额与记账，数据按商户分到 2 个数据库分片）、channel-mock（模拟微信、支付宝这类外部支付渠道）、recon（对账，核对系统内外数据是否一致）。服务之间用 Kitex RPC 同步调用，或通过 Kafka 消息异步通知；数据存在 MySQL 8.4，Redis 用于防重放。所有金额用整数“分”表示，所有记账只追加不修改。当前已完成：工程骨架、本地环境、迁移与种子、整数金额（int64 分）、雪花 ID、错误码、可注入的东八区时钟。账户、支付、退款、转账还没做。

### 1.2 本功能点在系统中的位置

本功能点是一个公共库，不处理支付请求，也不读写数据库。以后的业务代码和定时任务通过它取“现在”；测试换成可拨动的假时钟。发号库 `pkg/idgen` 不走这里，它自己留着一个更窄的时钟。

```mermaid
flowchart LR
  biz["以后的 biz 与 job"] -->|只调用 Now| clk["Clock"]
  clk --> real["Real"]
  clk --> fake["Fake"]
  real --> zone["固定东八区 +8 小时"]
  fake --> mem["进程内存里的一个时间"]
  idgen["pkg/idgen 自己的窄时钟"] -.->|不依赖本包| clk
```

图里的实线是本功能点的调用方向。`biz` 和 `job` 现在还没有业务包，所以图中是以后的调用方。虚线表示发号不改用本包。

### 1.3 为什么需要它

支付过期和账期都按东八区的钟面计算。设一笔支付在东八区 2026-10-15 00:10:00 创建，30 分钟后过期，到期是当天 00:40:00。同一瞬间的 UTC 钟面是 2026-10-14 16:10:00。如果程序用机器本地时区或 UTC 做“今天”，午夜附近会把账期算到前一天。

测试也不能靠真实等待。要证明“到期前 1 秒仍可付、到期后拒绝”，真实时钟得等到 30 分钟，并发测试还会互相改系统时间。`Fake` 把 10 秒一次加完，断言可以立刻比较。

运行镜像没有安装 tzdata（时区数据库）。若生产时钟调用 `time.LoadLocation("Asia/Shanghai")`，容器里会得到未知时区错误，当前时间拿不到。本功能点不改镜像，所以时钟不能依赖那次查找。

## 2. 术语

| 术语 | 含义 |
|---|---|
| gateway | 对外 HTTP 入口，负责签名校验和限流。本功能点不实现它。 |
| 签名 | 商户请求上的校验码，用来确认请求没有被篡改。 |
| 限流 | 限制单个商户单位时间内的请求次数。 |
| payment | 管理支付单、退款单、转账单状态的服务。 |
| account | 管账户余额和记账的服务。数据按商户分到 2 个数据库。 |
| 分片 | 账户数据所在的那一份数据库。这里计划是 2 个，本功能点不动它们。 |
| channel-mock | 模拟外部支付渠道的程序，不是真的微信或支付宝。 |
| recon | 对账服务，核对系统内记录和渠道账单是否一致。 |
| 对账 | 把两边的成功记录逐笔比较，找出多记或少记。 |
| Kitex | 服务之间同步调用使用的 RPC 框架。本功能点不起 RPC。 |
| RPC | 一台进程直接调用另一台进程上的函数，调用方等待结果。 |
| Kafka | 服务之间异步传消息的队列。本功能点不发消息。 |
| MySQL 8.4 | 业务数据库。本功能点不连接它。 |
| Redis | 内存数据库，计划用来挡住短时间里重复提交的随机串。 |
| 防重放 | 同一个已用过的随机串再提交时拒绝，避免把旧请求再执行一次。 |
| 分 | 金额的最小单位。100 分等于 1.00 元。金额用整数，不用小数。 |
| 雪花 ID | 按时间、机器号、序列拼出的 64 位整数单号。由 `pkg/idgen` 生成，本功能点不生成单号。 |
| 东八区 | 比 UTC 早 8 小时的民用时间，中国现行标准时间。 |
| UTC | 协调世界时，本报告用来对照“同一瞬间”的另一套钟面。 |
| `Clock` | 只有一个方法 `Now() time.Time` 的接口。业务通过它取当前时间。 |
| `Real` | 生产用的 `Clock`。返回操作系统此刻，但地点换成固定东八区。 |
| `Fake` | 测试用的 `Clock`。时间由测试设定，不跟着真实时钟走。 |
| `time.FixedZone` | Go 标准库函数。给定地点名和相对 UTC 的固定秒数，得到一个永不变化的时区。 |
| `time.LoadLocation` | Go 标准库函数。按 `Asia/Shanghai` 这类名字去时区数据库里加载规则，包含历史上的夏令时。 |
| zoneinfo / tzdata | 操作系统或程序里的时区数据库。没有它，`LoadLocation("Asia/Shanghai")` 会失败。 |
| 夏令时 | 夏季把钟面拨快的规则。中国自 1991 年起不再使用。 |
| IANA 时区 | 那份带历史规则的标准时区数据。`LoadLocation` 加载的就是它。 |
| 墙钟 | 钟面上的年月日时分秒。同一瞬间，东八区墙钟比 UTC 墙钟早 8 小时。 |
| `biz` | 服务里写业务规则的包。计划只能通过 `Clock` 取时间。 |
| `job` | 定时任务所在的包。同样只能通过 `Clock` 取时间。 |
| 窄时钟 | `pkg/idgen` 自己声明的、只含 `Now` 的小接口。它不是本包的 `Clock`。 |
| 互斥锁 | 同一时刻只放一个 goroutine 进入临界区，用来避免并发把同一次加法盖掉。 |
| goroutine | Go 里的并发执行单元。本报告的并发测试开了 100 个。 |
| forbidigo | 静态检查里的一条禁令：`service/*/biz` 和 `job` 里不允许写 `time.Now`。 |

## 3. 技术讲解

### 3.1 要解决的问题

1. 当前时间的地点名必须是 `Asia/Shanghai`，并且比 UTC 早 8 小时。账期和过期都按这个钟面。
2. 运行镜像没有 tzdata。取当前时间不能调用 `time.LoadLocation`，也不能因此去改镜像。
3. 中国自 1991 年起没有夏令时，所以“现在”不需要历史夏令时规则。
4. 测试要能把时间设到某一刻，并精确前进，例如前进 10 秒后不多也不少。
5. 多个 goroutine 同时拨动同一个 `Fake` 时，不能丢掉其中一次前进。
6. 以后的 `biz` 和 `job` 只能通过注入的 `Clock` 取时间。现在不造空的业务包。
7. 发号继续用自己的窄时钟，不改成依赖本包。

### 3.2 方案

接口只有 `Now() time.Time`。生产用 `Real`，测试用 `Fake`。`Real` 先读操作系统时间，再换到一块名叫 `Asia/Shanghai`、偏移永远是 8 小时的时区上。同一瞬间不变，变的是钟面和地点名。

下面这段代码在构造这块固定时区，并把它套到当前瞬间上。看地点名和 `8*60*60` 这两个常数；`Now` 分成两行，是为了让已有的 `time.Now` 禁令仍能识别这一处调用（写成 `time.Now().In(...)` 时，检查器看到的是 `time.Now().In`，排除规则对不上）。

```go
// pkg/clock/clock.go
const shanghaiOffset = 8 * 60 * 60 // 8 小时，单位秒

var shanghai = time.FixedZone("Asia/Shanghai", shanghaiOffset)

func (Real) Now() time.Time {
	now := time.Now()
	return now.In(shanghai)
}
```

`In` 不改变“这是哪一瞬间”，只改变展示用的时区和钟面。例如同一瞬间：UTC 墙钟是 2026-10-14 16:10:00，东八区墙钟是 2026-10-15 00:10:00，相差正好 8 小时。地点的 `String()` 是 `Asia/Shanghai`，不是 `UTC`，也不是 `Local`。

`Fake` 内部是一把互斥锁加一个 `time.Time`。零值时这个时间是 Go 的零时间（公元 1 年），不是当前时间，使用前要先 `Set`。`Set` 把瞬间和地点整份换成传入值。`Advance(d)` 在锁内做 `now = now.Add(d)`：`d` 是 10 秒就正好加 10 秒，`d` 为负就回拨，不读真实时钟。三个方法都先加锁再读或写，所以并发的前进不会互相覆盖。`Fake` 含锁，不能按值复制，只能以指针放进 `Clock`。

本包不 import `time/tzdata`，生产代码里也没有 `LoadLocation`。数据库连接串里的 `loc=Asia/Shanghai` 仍会让 MySQL 驱动自己去 `LoadLocation`；那条路径继续用已有的编译标签 `-tags timetzdata` 把时区数据编进二进制，本次不改 Dockerfile。

`service/*/biz` 和 `job` 里禁止直接写 `time.Now` 的规则本来就在。本功能点没有新建这两个空目录。`pkg/idgen` 的源码也没有改成 import `pkg/clock`。

### 3.3 为什么这样做

| 方案 | 做法 | 优点 | 缺点 | 结论 |
|---|---|---|---|---|
| 采用：`FixedZone("Asia/Shanghai", 8*60*60)` | 地点名和偏移都写死，不查时区库 | 没有 tzdata 也能得到 `Asia/Shanghai` 和 +8 小时；与 1991 年之后的中国民用时间一致 | 不能重演 1991 年之前的夏令时 | 采用。业务要的是“现在” |
| 备选：`time.LoadLocation("Asia/Shanghai")` | 从 IANA 数据加载，含历史夏令时 | 历史日期的钟面更全 | 镜像没装 tzdata 时失败。本次规定不改 Dockerfile | 不采用 |
| 备选：import `time/tzdata` 再 `LoadLocation` | 把整份时区库编进时钟所在的二进制 | 不依赖镜像里的文件 | 时钟并不需要夏令时规则；数据库驱动那条路已经另有编译标签 | 不采用 |
| 备选：让 `pkg/idgen` 改为依赖 `pkg/clock` | 全仓库只有一个时钟类型 | 类型统一 | 发号要自己观察回拨和等待，和业务注入不是同一次取时；这是无关重构 | 不采用 |

### 3.4 正确性

不涉及资金。时钟不收款、不记账，也没有余额。下面只说明和本包有关的并发；重复请求、崩溃恢复、下游超时都没有对应的数据变化。

| 场景 | 系统行为 | 最终状态 | 证明 |
|---|---|---|---|
| 同一请求重复到达 | 不涉及。没有请求进入本包，也没有写入。`Real.Now` 每次读操作系统时钟；`Fake.Now` 只是把上次设定的时间再返回一次。 | 无数据库行 | 不涉及 |
| 两个冲突操作并发 | 100 个 goroutine 共用一个 `Fake`，每个把时间前进 1 秒，并穿插调用 `Now`。锁把“读出当前值、加上 1 秒、写回”包在同一次持锁里。若不加锁，两个 goroutine 可能都读到 T、都写回 T+1 秒，其中 1 秒丢失。 | 比起点正好多 100 秒 | `TestFakeConcurrentAdvance`（`go test -race`） |
| 进程在中途崩溃 | 不涉及。`Real` 和 `Fake` 都不写磁盘或数据库。进程退出后，内存里的假时间消失；下次启动仍由 `Real` 读操作系统时钟。没有需要补记的账。 | 无持久状态 | 不涉及 |
| 下游调用超时 | 不涉及。`Now`、`Set`、`Advance` 都不访问网络、数据库或别的进程，没有可超时的下游。 | 无外部调用 | 不涉及 |

精确前进的单线程行为：测试先 `Set` 到东八区 2026-10-15 01:02:03.000000004，再 `Advance(10s)`。之后的 `Now` 与起点相差 10 秒，纳秒差是 10000000000，地点名不变。这由 `TestFakeAdvanceTenSeconds` 证明。

“没有调用 `LoadLocation`”由两件独立的事一起证明，都在 `TestRealLocationAsiaShanghai` 里。第一件：用 Go 解析器扫描本包非测试源码，出现 `LoadLocation` 或 import `time/tzdata` 就失败。测试文件自己会调用 `LoadLocation`，只为拿到 IANA 地点来对比，生产文件不调用。第二件：1991-07-01 12:00 UTC 在 IANA 的 `Asia/Shanghai` 上是夏令时，偏移 +9 小时；`Real` 的地点在这一天仍是 +8 小时。只看今天的偏移不够，因为 1991 年之后两者都是 +8。另外，`Now` 与它的 UTC 形式是同一瞬间，墙钟相差 8 小时，`Zone` 偏移是 28800 秒。

没有另起一个删掉 tzdata 的容器。固定时区不打开时区文件，所以上面的扫描和偏移差就是“无时区文件也成立”的证明。

### 3.5 性能

不涉及。这不是性能功能点，本次没有测量耗时或吞吐，也没有可开关的两条路径。

## 4. 已完成内容

### 4.1 改动清单

| 文件 | 新增 / 修改 | 作用 |
|---|---|---|
| `pkg/clock/clock.go` | 新增 | `Clock`、`Real`、`Fake` |
| `pkg/clock/clock_test.go` | 新增 | 验收、并发，以及“未调用 `LoadLocation`”的检查 |
| `docs/02-requirements.md` | 修改 | BASE-07 标为 DONE，写明固定时区口径 |
| `docs/03-conventions.md` | 修改 | 时间约定记为 v2.7：时钟用 `FixedZone`，不依赖 zoneinfo |
| `docs/04-dev-plan.md` | 修改 | 只勾选 BASE-07 |
| `docs/01-tech-decisions.md` | 修改 | 说明本包不改 Dockerfile，也不撤销数据库驱动那条 `-tags timetzdata` |
| `docs/interview-notes.md` | 修改 | 记录为何不用 `LoadLocation`，以及为何要用 1991-07-01 区分固定时区 |

### 4.2 对外变化

| 类别 | 变化 |
|---|---|
| 接口 | 无。没有新的 HTTP 或 RPC。 |
| 表结构 | 无。不访问数据库。 |
| 配置项 | 无。不读环境变量。 |
| 错误码 | 无。不新增错误码，也不改单号前缀。 |
| 指标 / 故障点 | 无。 |

### 4.3 验收对照

| 验收条目 | 测试函数 | 类型 | 结果 |
|---|---|---|---|
| `Fake.Advance(10s)` 后 `Now()` 精确前进 10 秒 | `TestFakeAdvanceTenSeconds` | 单元 | 通过 |
| `Real.Now().Location()` 为 `Asia/Shanghai`，且在无时区文件的容器中也成立 | `TestRealLocationAsiaShanghai` | 单元 | 通过。未启动容器；见 3.4 的扫描和 +8/+9 对比 |

并发规则不是验收条目里的单独一句，由 `TestFakeConcurrentAdvance` 覆盖，结果通过。

### 4.4 验证记录

环境：`go version go1.27.2 darwin/arm64`。下列命令由 lead 于 2026-10-15 在本机执行。

| 命令 | 结果 |
|---|---|
| `gofmt -l .` | 无输出，退出码 0 |
| `make lint` | `0 issues.` |
| `make test`（`go test -race ./...`） | 通过。根包 `ok github.com/yym108/pay-system 2.091s`。`pkg/clock`、`pkg/errno`、`pkg/idgen`、`pkg/money`、`internal/seed` 为 cached ok。其余包没有测试文件 |
| `go test -race -count=1 -cover ./pkg/clock/` | `ok github.com/yym108/pay-system/pkg/clock 1.405s coverage: 100.0% of statements` |
| `go tool cover -func` | `Real.Now`、`Fake.Now`、`Set`、`Advance` 都是 100.0%，语句合计 100.0%。这组数字来自另一次 cover 命令，输出 `ok ... 1.258s` |
| `make test-integration` | 未运行。本功能点不使用 MySQL、Redis 或 Kafka |
| `make gen` | 未运行。没有接口定义要生成 |
| `make cover`（全仓库） | 未运行。不记录全仓库总覆盖率 |

### 4.5 同步的文档

- 需求里 BASE-07 的状态改为 DONE，并写明：用 `time.FixedZone("Asia/Shanghai", 8*60*60)`，不调用 `LoadLocation`，因此不依赖 zoneinfo；不改 Dockerfile；`Fake` 并发安全。
- 全局约定升到 v2.7：业务当前时间走固定东八区；数据库驱动解析 `loc=Asia/Shanghai` 时仍会 `LoadLocation`，继续用 `-tags timetzdata`。
- 开发计划只勾了 BASE-07，配置和日志仍未勾。
- 技术决策补了一段：时钟不读 zoneinfo，不撤销数据库那条编译标签，`pkg/idgen` 仍用自己的窄时钟。

## 5. 未完成与风险

- 1991 年之前的当地钟面不会按当时的夏令时变化。业务上的“现在”在 1991 年之后，固定 +8 与现行东八区一致。不要用这个地点去还原 1991 年夏天的历史钟面。
- `biz` 和 `job` 还没有业务代码。禁令已经生效，但还没有调用方注入 `Clock`。接入留给以后的业务功能点。本功能点故意没有建空包。
- `pkg/idgen` 不会自动改用本包。两处各读各的时间。发号的回拨判断仍看它自己注入的时钟。
- 没有在删掉 tzdata 的容器里再跑一次 `Real.Now`。若以后有人把 `Real` 改成 `LoadLocation`，现有单元测试会失败：源码扫描能看到调用，1991-07-01 的偏移也会从 +8 变成与 IANA 相同的 +9。

## 6. 如何复现

不需要 Docker。在仓库根目录、Go 1.27.2 下执行。耗时会和 2026-10-15 那次不同；通过与否以及 100.0% 应一致。

```bash
go version
gofmt -l .
make lint
make test
go test -race -count=1 -coverprofile=/tmp/clock.out ./pkg/clock/
go tool cover -func=/tmp/clock.out
```

预期：`gofmt -l .` 没有文件名；`make lint` 以 `0 issues.` 结束；`make test` 里各有测试的包为 `ok`；`pkg/clock` 的语句覆盖率是 `100.0% of statements`；`go tool cover -func` 里 `Now`、`Set`、`Advance` 和合计都是 100.0%。不要用这次记录去对照全仓库 `make cover`，那条命令没有跑。

## 7. 讲解要点

1. 问：容器里没有 tzdata，为什么还能叫 `Asia/Shanghai`？答：没有去查时区库。`FixedZone` 自己带上这个名字和 +8 小时。中国自 1991 年起无夏令时，所以“现在”不需要 IANA 规则。
2. 问：今天测到 +8 小时，能不能证明没用 `LoadLocation`？答：不能。1991 年之后 IANA 的上海也是 +8。要看 1991-07-01：IANA 是 +9，固定时区仍是 +8。再加一次源码扫描，确认生产文件没有那个调用。
3. 问：`Advance(10s)` 怎样才算精确，并发加锁加在哪里？答：锁内执行 `time.Add(10s)`，纳秒差是 10000000000，不读真实时钟。100 个 goroutine 各加 1 秒，最终必须正好多 100 秒；锁避免两次都读到旧值、只写回一次。

## 延伸阅读

- `docs/02-requirements.md` 中 BASE-07 的规则、口径和验收
- `docs/03-conventions.md` 的时间一节（v2.7）
- `docs/01-tech-decisions.md` 里 BASE-03 的 `-tags timetzdata` 与 BASE-07 的固定时区说明
- `pkg/clock/clock.go`、`pkg/clock/clock_test.go`

## 修订记录

| 日期 | 提交 | 修订内容 |
|---|---|---|
| 2026-10-15 | `15e2022` | 初稿。对应提交 `[BASE-07] add injectable shanghai clock`（`15e2022fbc26d52483b693b67b173877f41b18c5`，Thu Oct 15 00:31:05 2026 +0800） |
