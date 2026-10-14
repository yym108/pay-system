# BASE-09 配置

| 项 | 内容 |
|---|---|
| 功能点 | BASE-09（P0，第 1 周） |
| 所属链路 | 基础设施 |
| 代码提交 | `cb968f5` `[BASE-09] add env config with redacted startup log` |
| 分支 | `feat/BASE-09-config` |
| 日期 | 2026-10-15 |
| 状态 | DONE |

## 一句话结论

每个程序有自己的配置结构体，启动时只解析调用方传入的环境变量列表，缺必填项或取值非法就带上变量名失败退出。带 `secret:"true"` 的字段在启动输出里变成 `***`。对账和模拟渠道的数据库连接串在进程内按文本规则装配，不读取支付库口令，也不连接数据库。payment 入口的两个测试分别证明：少了主密钥时退出码非 0 且输出含 `SECRET_MASTER_KEY`；成功时输出有 `***`、没有测试构造的密钥明文，普通字段仍可见。

## 1. 背景

### 1.1 系统简介

pay-system 是一个用 Go 编写的小型支付后端，模拟商户收款、退款、商户间转账三条资金链路，重点在数据一致性和性能。系统由 5 个部分组成：gateway（对外 HTTP 入口，负责签名校验和限流）、payment（支付单、退款单、转账单的状态管理和流程推进）、account（账户余额与记账，数据按商户分到 2 个数据库分片）、channel-mock（模拟微信、支付宝这类外部支付渠道）、recon（对账，核对系统内外数据是否一致）。服务之间用 Kitex RPC 同步调用，或通过 Kafka 消息异步通知；数据存在 MySQL 8.4，Redis 用于防重放。所有金额用整数“分”表示，所有记账只追加不修改。当前已完成：工程骨架、本地环境、迁移与种子、整数金额（int64 分）、雪花 ID、错误码、可注入的东八区时钟、按环境变量加载并脱敏的配置。账户、支付、退款、转账的业务还没做。配置加载成功后进程就返回，不监听端口。

### 1.2 本功能点在系统中的位置

配置是进程真正干活之前的第一段。这次只做到“读环境、校验、打印脱敏结果、返回”。后面的 HTTP、RPC 和数据库连接都还没接上。

```mermaid
flowchart LR
  list[调用方传入的环境列表]
  load[按程序结构体解析并校验]
  bad[stderr 写出变量名]
  ok[stdout 写出脱敏配置]
  list --> load
  load -->|缺项或非法| bad
  load -->|通过| ok
```

图里没有端口，也没有 MySQL。gateway、payment、account、recon、channel-mock、merchant-demo、paycli 七个入口都走这条路径。seed 和 bootstrap 只增加了可加载的结构体，它们原来的建库和种子流程没有改。

### 1.3 为什么需要它

本地和容器里，同一个二进制会看到很多环境变量，其中有数据库口令和主密钥。如果启动时直接把整个结构体打印出来，口令会进标准输出。如果缺了主密钥还继续往下走，后面的加密会在更晚的地方以含糊的错误失败，日志里不一定能看出少的是哪一个变量。

另一个具体风险是口令串进程。支付进程的连接串里是支付账号的口令。对账进程如果也读取这同一条变量，对账日志或崩溃转储就可能带上支付口令。对账只应该使用自己的只读账号。

补偿任务的间隔也要能改短。查单默认退避是 10 秒、10 秒、30 秒、1 分钟、2 分钟、5 分钟、10 分钟、30 分钟。测试不能为了走完这条计划去等半小时，所以这些时长必须来自配置，而不是写死在业务代码里。业务本身这次还没做，配置先把入口留出来。

## 2. 术语

| 术语 | 含义 |
|---|---|
| 环境变量 | 进程启动时从外部传入的 `名字=值`。本系统的配置只来自这里，不读配置文件。 |
| DSN | Data Source Name，数据库连接串。文本形如 `用户:口令@tcp(主机:端口)/库名?参数`。 |
| 分片 | 账户数据拆在两个库 `pay_account_0` 和 `pay_account_1`。商户按编号落到其中一个。分片个数固定为 2。 |
| `WORKER_ID` | 发号进程的实例编号，范围 0 到 1023。六个会发号的业务进程必填。seed、bootstrap、paycli 不发号，不读它。 |
| 主密钥 | 环境变量 `SECRET_MASTER_KEY`。标准 base64，解码后必须正好 32 字节，留给以后的 AES-256。 |
| `secret:"true"` | 结构体字段上的标签。打印生效配置时，这个字段整段换成 `***`。 |
| 脱敏 | 这里只指按标签把字段换成 `***`。不扫描卡号或手机号，那是后面的日志组件要做的事。 |
| OTLP | 把链路追踪数据发给收集器的协议。终端地址来自 `OTEL_EXPORTER_OTLP_ENDPOINT`，空字符串表示关闭追踪。 |
| `svc_recon` | 对账使用的数据库用户，只有只读权限。四条对账连接串都用这个用户。 |
| `svc_channel_mock` | 模拟渠道自己的数据库用户，只访问渠道库 `pay_channel_mock`。 |

## 3. 技术讲解

### 3.1 要解决的问题

1. 每个程序只读取自己的变量，并且带上约定的默认值。测试要能把间隔改成 1 毫秒这种短值。
2. 缺必填项或取值不合法时，进程以非 0 退出，错误文本里有变量名，例如 `SECRET_MASTER_KEY`。
3. 启动时打印的生效配置里，密钥、口令、连接串不能出现明文。没标成密钥的字段仍要看得见，方便确认用了哪一个 `WORKER_ID`。
4. 解析不能顺手读走开发者机器上已经导出的环境。测试传入的列表就是全部输入。
5. 对账和模拟渠道不能读取支付进程那条 `MYSQL_DSN`，避免支付口令进入这两个进程。
6. 装配连接串时不能连接数据库。生产代码也不能调用驱动的 `ParseDSN`，因为那个函数会按名字加载时区，没有时区文件的环境会失败。
7. 账户分片数只能是 2。主密钥解码后必须是 32 字节。

### 3.2 方案

依赖是 `github.com/caarlos0/env/v11 v11.4.1`。每个程序一个结构体，字段用 `env` 标签声明变量名、是否必填、默认值。调用方把 `名字=值` 的列表传进来，库只看这份列表。

这段代码在把列表变成 map 后交给库。map 即使为空也不是 nil，库就不会退回去读进程自己的环境。

```go
func parse(dst any, environ []string) (map[string]string, error) {
	values := env.ToMap(environ)
	if values == nil {
		values = map[string]string{}
	}
	if err := env.ParseWithOptions(dst, env.Options{Environment: values}); err != nil {
		return nil, annotate(dst, err)
	}
	return values, nil
}
```

库报“缺变量”时，原文里已经有变量名。类型转换失败时，库只提到 Go 字段名，`annotate` 再按 `env` 标签补上变量名，所以非法值的错误里也有 `WORKER_ID` 这类名字。校验函数自己的错误写成 `invalid 变量名: 原因`，原因里不放口令或密钥原文。

payment 的 `main` 只把进程环境交给 `run`，再按退出码结束。测试调用的是 `run`，不会把测试进程杀掉。

```go
func main() {
	os.Exit(run(os.Environ(), os.Stdout, os.Stderr))
}

func run(environ []string, stdout, stderr io.Writer) int {
	cfg, err := config.LoadPayment(environ)
	return config.WriteResult(stdout, stderr, cfg, err)
}
```

`WriteResult` 失败时把错误写到 stderr 并返回 1。成功时把 `Redact` 的结果写到 stdout 并返回 0。`Redact` 按字段顺序输出 `变量名=值`。标签 `secret:"true"` 的值固定写成三个星号，不看字段里实际是什么。没有这个标签的字段按原值输出。这是本包自己的反射，没有做卡号或手机号扫描。

有两个默认值不能交给库的 `envDefault`。这个库在“变量存在但值是空字符串”并且字段写了默认值时，会把空字符串改回默认值。追踪地址的空字符串表示关闭，所以只有键完全不在列表里才填 `jaeger:4317`。查单退避 `QUERY_BACKOFF` 的空字符串是非法的（至少要有一段正时长），未设置时才填默认的八段。

对账不声明 `MYSQL_DSN`。它必填自己的口令，以及两条账户分片连接串。解析只按文本切：最后一个 `/` 后面是库名和查询参数，最后一个 `@` 后面是 `tcp(主机:端口)`。口令留在 `@` 左边，函数不把口令放进返回的片段，错误文本里也不拼接整条连接串。

四条结果的用户都是 `svc_recon`，口令都是 `MYSQL_RECON_PASSWORD`。主机、端口和查询参数都来自 `ACCOUNT_DB_DSN_0`。库名分别是 `pay_recon`、`pay_payment`、`pay_account_0`、`pay_account_1`。第二条分片连接串只用来确认库名是 `pay_account_1`；就算它指向另一台机器，四条结果仍用第一条的地址。两条库名不对就报对应的变量名。

模拟渠道同样不读 `MYSQL_DSN`。主机默认是 `mysql`，因为这个进程跑在容器网络里；建库程序 bootstrap 的主机默认仍是 `127.0.0.1`，给宿主机上的建库用。端口默认 `3306`。拼出来的用户是 `svc_channel_mock`，库是 `pay_channel_mock`，查询参数固定为 `parseTime=true&loc=Asia%2FShanghai&charset=utf8mb4`。

payment、account、seed 的连接串字段原样保存，不替换用户和口令。这三处以及 recon、channel-mock 装配出来的连接串都标了 `secret:"true"`。

`ACCOUNT_SHARD_COUNT` 默认 2，别的整数直接拒绝，错误里有这个变量名。分片数一变，商户会落到另一个库。`SECRET_MASTER_KEY` 用标准 base64 解码，长度不是 32 字节就拒绝，错误里有变量名，没有密钥原文。`WORKER_ID` 必须在 0 到 1023，0 是合法值，所以不能用“零值表示没填”。

gateway、payment、account、recon、channel-mock、merchant-demo、paycli 的入口都改成加载配置然后返回。paycli 没有专属变量，结构体是空的，不要求 `WORKER_ID`。seed 和 bootstrap 的 `main` 没改。没有新建迁移命令，迁移仍由容器里的迁移镜像做。

### 3.3 为什么这样做

| 方案 | 做法 | 优点 | 缺点 | 结论 |
|---|---|---|---|---|
| 采用：结构体加调用方传入的环境列表 | 库只解析这份列表；密钥字段按标签替换 | 测试不会读到机器上的真实密钥；每个程序的字段是类型，不是散落的 `Getenv` | 空字符串和“未设置”要自己区分 | 采用 |
| 备选：直接读进程环境并 `%+v` 打印结构体 | 代码短 | 和容器环境看起来一致 | 测试会继承开发者导出的密钥；打印会带出口令 | 不采用 |
| 备选：用驱动的 `ParseDSN` 装配连接串 | 解析规则和驱动完全同一份代码 | 少写一个文本解析 | 解析 `loc=Asia/Shanghai` 会按名字加载时区；配置加载就会依赖时区文件 | 不采用。生产代码只切文本。测试再用驱动解析结果，核对用户、库名和口令，失败信息不打印口令 |

### 3.4 正确性

不涉及资金。这里不记账，也不改余额。

不涉及两个请求并发。加载只读调用方传入的那一份列表，不写共享配置，也没有第二个人来抢同一行数据。

不涉及中途崩溃后的恢复。成功路径只是把脱敏文本写到 stdout 然后返回，没有要提交的数据库事务。进程在打印前退出，机器上不会留下半份配置。

不涉及下游超时。装配和校验都不连接数据库，也不访问网络，没有一个会超时的外部调用。

连接串装配由两个测试证明，断言只比较用户、库名、地址和“口令是否等于测试构造的口令”，不把口令写进失败信息。

| 场景 | 系统行为 | 最终状态 | 证明 |
|---|---|---|---|
| 资金变动 | 不涉及 | 无余额、无分录 | 本功能点没有资金代码 |
| 同一输入再加载一次 | 不写库，结果只取决于那份列表 | 无持久化状态 | 加载函数没有写操作 |
| 两个加载并发 | 不涉及，没有共享可变配置 | 无 | 不涉及 |
| 打印前进程退出 | 不涉及恢复。没有半提交的数据 | 无落盘配置 | 不涉及 |
| 下游超时 | 不涉及。不连接数据库、不访问网络 | 无 | 不涉及 |
| 对账装配 | 四条连接串的用户都是 `svc_recon`，口令都是对账口令，库名分别是对账库、支付库和两个账户分片。地址和查询参数来自第一条账户连接串，不用第二条的主机，也不带入支付口令 | 只存在于内存中的四条 secret 字段 | `TestLoadReconAssemblesDSN` |
| 渠道装配 | 默认主机是 `mysql`、端口 3306，用户是 `svc_channel_mock`，库是 `pay_channel_mock`。查询参数含 `parseTime`、东八区地点名和 `utf8mb4`。支付那条连接串里的口令不会出现 | 内存中的一条 secret DSN | `TestLoadChannelMockAssemblesDSN` |

### 3.5 性能

不涉及。这次没有压测，也没有比较开关前后的耗时。

## 4. 已完成内容

### 4.1 改动清单

| 文件 | 新增 / 修改 | 作用 |
|---|---|---|
| `pkg/config/` | 新增 | 各程序的结构体、解析、校验、脱敏、两条 DSN 装配 |
| `cmd/gateway/main.go`、`cmd/payment/main.go`、`cmd/account/main.go`、`cmd/recon/main.go`、`cmd/channel-mock/main.go`、`cmd/merchant-demo/main.go`、`cmd/paycli/main.go` | 修改 | 加载配置；失败写 stderr 并返回非 0；成功写脱敏结果到 stdout |
| 上述七个目录的 `main_test.go` | 新增 | 入口测试。payment 的两个测试覆盖验收 |
| `go.mod`、`go.sum` | 修改 | 直接依赖 `github.com/caarlos0/env/v11 v11.4.1`，没有改其他已锁定版本 |

`cmd/seed` 和 `cmd/bootstrap` 的 `main` 没有改。

### 4.2 对外变化

| 类别 | 变化 |
|---|---|
| 接口 | 无。不监听端口，不提供 HTTP 或 RPC。 |
| 表结构 | 无。 |
| 配置项 | 没有新增变量名。明确了谁读哪些已有变量，以及 recon、channel-mock 如何装配连接串。channel-mock 的 `MYSQL_HOST` 默认是 `mysql`，bootstrap 仍是 `127.0.0.1`。 |
| 错误码 | 无。失败是普通错误加进程退出码，不是业务错误码。 |
| 指标 / 故障点 | 无。`FAULT_POINTS` 只是一个默认为空的字符串，注入逻辑还没做。 |

### 4.3 验收对照

| 验收条目 | 测试函数 | 类型 | 结果 |
|---|---|---|---|
| 缺少 `SECRET_MASTER_KEY` 时 payment 启动失败并提示该变量名 | `TestPaymentMissingSecretMasterKey` | 单元 | 通过 |
| 启动日志不出现密钥明文 | `TestPaymentStartupRedactsSecrets` | 单元 | 通过 |

第一个测试走 payment 的 `run`。列表里放齐其他必填项，只不放主密钥。同时给进程环境放了另一套值。退出码不是 0，合并后的输出含有 `SECRET_MASTER_KEY`，不含测试构造的口令或密钥，也没有把别的必填项报成失败原因。

第二个测试同样走 `run`。列表里的 `WORKER_ID` 是 2，进程环境里是 9。输出含有 `***` 和 `WORKER_ID=2`、`KAFKA_BROKERS=kafka:9092`，不含 `WORKER_ID=9`，也不含测试构造的主密钥、渠道密钥和连接串口令。

### 4.4 验证记录

环境：`go version go1.27.2 darwin/arm64`。lead 于 2026-10-15 在本机运行。下面的数字就是那次输出，没有另跑一次。

| 命令 | 结果 |
|---|---|
| `gofmt -l .` | 无输出，退出码 0 |
| `make lint` | `0 issues.` |
| `make test` | 通过，即 `go test -race ./...`。根包 `ok github.com/yym108/pay-system 2.548s`。`cmd/account`、`cmd/channel-mock`、`cmd/gateway`、`cmd/merchant-demo`、`cmd/paycli`、`cmd/payment`、`cmd/recon`、`internal/seed`、`pkg/clock`、`pkg/config`、`pkg/errno`、`pkg/idgen`、`pkg/money` 为 cached ok。`cmd/bootstrap`、`cmd/seed` 和若干 internal 包、`tools` 没有测试文件 |
| `go test -race -count=1 -cover`（各包分开） | 见下表 |
| `make test-integration` | 未运行。本功能点不连接数据库 |
| `make gen` | 未运行。没有改接口定义 |
| `make cover` | 未运行。没有全仓库总覆盖率，也没有函数级覆盖率 |

| 包 | 耗时 | 语句覆盖率 |
|---|---|---|
| `pkg/config` | `1.254s` | `85.3% of statements` |
| `cmd/payment` | `1.430s` | `66.7%` |
| `cmd/gateway` | `1.415s` | `66.7%` |
| `cmd/account` | `1.427s` | `66.7%` |
| `cmd/recon` | `1.415s` | `66.7%` |
| `cmd/channel-mock` | `1.434s` | `66.7%` |
| `cmd/merchant-demo` | `1.441s` | `66.7%` |
| `cmd/paycli` | `1.450s` | `66.7%` |

七个命令的覆盖率是 66.7%，因为 `main` 调用了 `os.Exit`，测试没有执行 `main`，执行的是 `run`。

### 4.5 同步的文档

- 需求里 BASE-09 标为完成，并写明错误文本要含变量名、`secret:"true"` 输出 `***`、两条 DSN 怎么装配。变更记录加了 2026-10-15 一行。
- 全局约定升到 v2.8，写明装配规则和 channel-mock 的 `MYSQL_HOST` 默认 `mysql`。
- 开发计划只勾了 BASE-09，日志仍未勾。
- 技术决策记下已经直接依赖 `caarlos0/env/v11 v11.4.1`，以及 DSN 装配口径。依赖版本表里的版本号没有改。
- 面试笔记记了两件实现时碰到的事：空字符串会被库改回 `envDefault`；生产代码不调用 `ParseDSN`，避免加载时区。

## 5. 未完成与风险

- 加载成功后进程就返回。监听端口、连数据库、启动 RPC 都还没做。
- seed 和 bootstrap 的 `main` 仍用原来的方式读环境。新结构体还没有接到这两条流程上。
- 连接池的最大连接数、空闲数、存活时间留给后面的数据库访问功能，这次没有这些字段。
- 按字段名扫描卡号、手机号的日志脱敏还没做。这次只替换标了 `secret:"true"` 的整段字段。
- `CHANNEL_MOCKPAY_URL` 这次只放在 payment 上。recon 不读它。对账以后真的要访问渠道时再加。
- 装配不检查 `loc=Asia/Shanghai` 能不能加载。以后真正打开数据库时，仍要靠构建时把时区数据编进二进制。
- 口令按驱动的文本规则放在第一个冒号和最后一个 `@` 之间。测试覆盖了口令里带 `@` 和 `/` 的切分；没有覆盖所有特殊字符。

## 6. 如何复现

在仓库根目录检出 `cb968f5`。不需要 Docker，也不需要准备真实密钥。测试自己构造环境列表。

```bash
gofmt -l .
make lint
make test
go test -race -count=1 -cover ./pkg/config/
go test -race -count=1 -cover ./cmd/payment/ ./cmd/gateway/ ./cmd/account/ ./cmd/recon/ ./cmd/channel-mock/ ./cmd/merchant-demo/ ./cmd/paycli/
```

2026-10-15 在 `go1.27.2 darwin/arm64` 上：`gofmt` 无输出且退出码 0；`make lint` 打印 `0 issues.`；`make test` 通过，根包是 `ok github.com/yym108/pay-system 2.548s`，其余有测试的包为 cached ok。覆盖率命令里 `pkg/config` 是 `1.254s`、`coverage: 85.3% of statements`；七个 `cmd` 包是 `1.415s` 到 `1.450s`，都是 `66.7%`。验收测试是 `TestPaymentMissingSecretMasterKey` 和 `TestPaymentStartupRedactsSecrets`。

## 7. 讲解要点

1. 问：为什么测试不直接读本机环境？答：列表被放进库的 `Options.Environment`，而且这个 map 不是 nil。库就不会合并 `os.Environ()`。进程里另放一个 `WORKER_ID=9`，传入列表是 2，输出只能是 2。
2. 问：为什么对账不读 `MYSQL_DSN`？答：那条变量里是支付账号的口令。对账用 `MYSQL_RECON_PASSWORD` 和第一条账户连接串的地址，自己拼四条 `svc_recon` 连接串。
3. 问：空的追踪地址为什么不是默认的 `jaeger:4317`？答：空字符串表示关闭。库会把“设了空字符串”当成“用默认值”，所以这个字段不写 `envDefault`，只有变量根本没出现时才填默认地址。

## 延伸阅读

- `docs/02-requirements.md` 的 BASE-09 小节和 §8.2
- `docs/03-conventions.md` §8、§8.1 和 §9 的 v2.8
- `docs/01-tech-decisions.md` 的 D12 和 BASE-09 段落

## 修订记录

| 日期 | 提交 | 修订内容 |
|---|---|---|
| 2026-10-15 | `cb968f5` | 初稿。代码提交 `[BASE-09] add env config with redacted startup log`（`cb968f58235d799c0198773198d9cec0fec8b3f6`，2026-10-15 01:21:59 +0800） |
