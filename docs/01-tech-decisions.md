# 技术路线与决策记录

> 最后更新：2026-10-15
> 状态：已决策，第 1 周开工即按本文锁定版本。推翻任一决策需在本文追加新记录，写明原因。
> 2026-10-12：项目聚焦支付、退款、转账三条资金链路，新增 D17–D21（账户分片、跨分片转账 TCC、平台子账户、批量记账、链路延迟优化），调整 D10、D14 与 §4。

## 1. 结论速览

| # | 待决策事项（PROJECT_GOALS §10） | 决策 |
|---|---|---|
| D1 | 多模块 workspace / 单模块 monorepo | 单模块 monorepo，根目录一个 `go.mod` |
| D2 | Thrift / Protobuf | Thrift（Kitex 默认协议，配合 Frugal/FastCodec） |
| D3 | Kafka / RocketMQ | Kafka 4.3（KRaft 单节点），客户端 franz-go |
| D4 | GORM / sqlx / database/sql | sqlx + 手写 SQL，不用 ORM |
| D5 | ID 生成 | 自研雪花算法 `pkg/idgen`，业务单号 = 2 位前缀 + 19 位数字 |
| D6 | 入账走 MQ 还是 RPC | 加钱异步（outbox + Kafka），扣钱同步（RPC 冻结）+ 异步确认 |
| D7 | 调试前端 / Swagger | 不做前端；提供 `paycli` 签名工具、`merchant-demo` 和 OpenAPI 文件（P1） |

补充决策见 §3 的 D8–D21。其中 D17–D21 是三条资金链路的核心设计：

| # | 事项 | 决策 |
|---|---|---|
| D17 | 账户数据如何拆分 | `pay_account_0`、`pay_account_1` 两个分片，按商户号路由；平台账户每个分片各一套 |
| D18 | 跨分片转账如何保证一致 | payment 作为 TCC 协调者；付款方 Try = 冻结，收款方 Try = 预留入账分支；同分片走本地事务 |
| D19 | 热点平台账户 | 平台账户拆为 N 个子账户，按 `hash(biz_no) % N` 选择 |
| D20 | 入账消费吞吐 | 按分区批量消费、按分片合并为一个事务提交，冲突时退化为逐条 |
| D21 | 链路延迟 | 分阶段埋点定位耗时，再做并行化、缓存、减少往返，每项给出前后对比 |

## 2. 版本锁定

2026-10-09 从 Go module proxy 和 Docker Hub 查询并验证。

### 2.1 Go 与依赖

| 组件 | 版本 | 说明 |
|---|---|---|
| Go | 1.27（本机 1.27.2，镜像 `golang:1.27.2-alpine`） | `go.mod` 写 `go 1.27.0`；BASE-01 Dockerfile 锁定 digest `sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d` |
| github.com/cloudwego/hertz | v0.10.6 | 网关、channel_mock、merchant-demo |
| github.com/cloudwego/kitex | v0.16.3 | RPC 运行时，代码生成工具同版本 `kitex/tool/cmd/kitex@v0.16.3` |
| github.com/cloudwego/thriftgo | v0.4.5 | kitex 代码生成依赖 |
| github.com/bytedance/sonic | v1.15.4（必须显式锁定） | 见 D8 |
| github.com/twmb/franz-go | v1.22.1 | Kafka 客户端 |
| github.com/jmoiron/sqlx | v1.4.0 | |
| github.com/go-sql-driver/mysql | v1.10.1 | |
| github.com/redis/go-redis/v9 | v9.23.0 | |
| github.com/caarlos0/env/v11 | v11.4.1 | 环境变量配置解析 |
| go.opentelemetry.io/otel（及 sdk、otlptracegrpc） | v1.47.0 | |
| github.com/hertz-contrib/obs-opentelemetry/tracing | v0.4.1 | 发布于 2024，见 D14 |
| github.com/kitex-contrib/obs-opentelemetry | v0.3.0 | |
| github.com/prometheus/client_golang | v1.25.0 | |
| golang.org/x/sync | v0.23.0 | errgroup、singleflight |
| golang.org/x/time | v0.16.0 | 网关商户级进程内限流 |
| github.com/stretchr/testify | v1.12.1 | |
| github.com/testcontainers/testcontainers-go（modules/mysql） | v0.44.0 | 集成测试 |
| golangci-lint | v2.14.0 | 静态检查，按官方建议安装预编译二进制，不放进 `go.mod` |

BASE-06（2026-10-14）：`go.mod` 直接依赖已锁定的 `github.com/cloudwego/kitex v0.16.3`，只为 `pkg/kerrors` 的 `BizStatusError`。不生成 IDL，不运行 kitex 命令，也不起 RPC 服务。sonic 仍显式锁定 v1.15.4，高于 kitex 自带的 v1.15.0，避免 Go 1.27 回退到 `encoding/json`。kitex 带入的间接依赖：`github.com/bytedance/gopkg` 由 v0.1.3 升到 v0.1.4，`golang.org/x/arch` 升到 v0.14.0，新增 `google.golang.org/protobuf v1.36.11` 与 `google.golang.org/genproto/googleapis/rpc v0.0.0-20260630182238-925bb5da69e7`。后一个模块被 `BizStatusError` 引用的 gRPC status 类型导入；v0.16.3 的 `go.mod` 没有单独 pin 它，`go mod tidy` 解析到这一版并写入 `go.sum`。

BASE-07（2026-10-15）：`pkg/clock` 的生产时钟用 `time.FixedZone("Asia/Shanghai", 8*60*60)`，不调用 `time.LoadLocation`，也不新增依赖。中国自 1991 年起没有夏令时，固定偏移与当前东八区民用时间一致，所以时钟不读 zoneinfo，也不 import `time/tzdata`。这不撤销 BASE-03：MySQL 驱动解析 `loc=Asia/Shanghai` 仍会 `LoadLocation`，构建继续使用 `-tags timetzdata`，Dockerfile 不改。`pkg/idgen` 仍使用自己的窄时钟，不改为依赖 `pkg/clock`。

### 2.2 容器镜像（均已确认提供 arm64 与 amd64）

| 组件 | 镜像 |
|---|---|
| MySQL | `mysql:8.4.11`（8.4 LTS） |
| Redis | `redis:8.10.2` |
| Kafka | `apache/kafka:4.3.1`（KRaft，无 ZooKeeper） |
| 迁移 | `migrate/migrate:v4.20.1` |
| Prometheus | `prom/prometheus:v3.15.0` |
| Jaeger | `jaegertracing/jaeger:2.22.0`（直接接收 OTLP） |
| 压测 | `grafana/k6:2.3.0` |
| BASE-01 运行时 | `alpine:3.22.1` | 最小运行镜像；Dockerfile 锁定 digest `sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1`，使用 UID/GID 65532 的非 root 用户 |

BASE-01 镜像 digest 于 2026-10-13 在本机 Docker 29.4.0 执行 `docker pull` 后用 `docker image inspect` 获取并写入 `deploy/Dockerfile`。运行阶段选择 Alpine 是为了减少占用；程序以 `CGO_ENABLED=0` 的静态二进制运行，不需要额外运行时依赖。

BASE-03（2026-10-14）：`go.mod` 首次直接引用已锁定的 `github.com/go-sql-driver/mysql v1.10.1`、`github.com/golang-migrate/migrate/v4 v4.20.1`、`github.com/testcontainers/testcontainers-go v0.44.0` 与 `modules/mysql`。迁移容器仍是 `migrate/migrate:v4.20.1`，集成测试用同版本库执行同一批 SQL。testcontainers 带入的间接依赖（testify v1.11.1、otel v1.44.0）不改变本表对直接依赖的锁定；业务代码以后直接使用时仍按本表版本。`tools/tools.go` 保留 sonic 的显式引用，避免 `go mod tidy` 把它删掉。bootstrap 与 seed 用已有 `deploy/Dockerfile` 构建本地镜像 `pay-system-bootstrap:local`、`pay-system-seed:local`，不新增基础镜像，运行阶段也不 `apk add`。约定里的 DSN 带 `loc=Asia/Shanghai`，Alpine 没有 zoneinfo；构建命令加 `-tags timetzdata`，把时区数据编进二进制，不依赖未锁定版本的运行时包。

BASE-02（2026-10-13）：本地编排使用上表镜像，不新增 Go 依赖。Kafka 4.3.1 单节点必须把 offsets、transaction state、share coordinator 的副本数和 min ISR 设为 1，否则 broker 按默认值 3 拒绝启动；这是锁定镜像的兼容约束，不是新组件。宿主机只发布 `127.0.0.1:9094`，容器网络使用 `kafka:9092`。`CLUSTER_ID` 固定为 `ZyizIAN2R0ad6PqH5Ne0Gw`，避免数据卷复用时因 ID 变化无法启动。日志保留 168 小时，对齐 NFR-CAP。MySQL 额外固定 `utf8mb4`，与设计稿字符集一致。Jaeger 2.22 使用镜像默认 all-in-one；健康检查访问容器内 13133，不映射到宿主机。应用容器在 OPS-01 之前用已锁定的 `alpine:3.22.1` 占位，不构建空 `main`。`deploy/compose.sh` 把 project directory 设为仓库根，是为了不让 Compose 自动读取 `deploy/.env`；因此 bind mount 相对仓库根，而不是 compose 文件所在目录。

BASE-01（2026-10-13）：根据本机实际安装版本，将本表 Go 本机版本从 1.27.1 更正为 1.27.2；模块语言版本仍按需求锁定为 `go 1.27.0`。

### 2.3 已完成的兼容性验证

- 在 Go 1.27.1 / darwin-arm64 上用 kitex v0.16.3 生成 Thrift 代码，Kitex 服务端 + 客户端调用成功，Hertz 服务正常响应。
- sonic v1.15.0（hertz/kitex 默认拉取的版本）在 Go 1.27 上打印 `sonic/ast only supports go1.20~1.26` 并回退到 `encoding/json`；升级到 v1.15.4 后告警消失。
- 上表全部 Go 依赖在同一模块中 `go build ./... && go vet ./...` 通过，未出现版本降级或冲突。
- 仍有一条 `dynamicgo only support amd64 && go1.17~go1.24` 告警。dynamicgo 只用于 Kitex 泛化调用，本项目不使用，可以忽略。
- 设计稿 DDL 已拆成 `migrations/` 并删除。`make migrate` 在 compose 的 `mysql:8.4.11` 上 up、down、再 up 成功，详见 `docs/database.md` §5。

未验证项（第 1 周验证）：franz-go 与 Kafka 4.3.1 的实际收发、Hertz/Kitex OTel 中间件的链路串联、testcontainers 在 CI 外环境的启动耗时。

## 3. 决策详情

### D1 单模块 monorepo

- 背景：4 个服务 + 1 个对账任务 + 公共包，单人开发。
- 决策：根目录一个 `go.mod`，每个可执行程序放在 `cmd/<name>/main.go`。
- 理由：`pkg/` 和 `kitex_gen/` 跨服务共享，单模块无需维护多个 `go.mod` 的版本同步；`go test ./...` 一次覆盖全仓；面试官浏览代码时结构直观。
- 代价：服务之间没有编译期的依赖隔离。用约定弥补：服务之间只能通过 `kitex_gen` 和 MQ 事件结构交互，禁止 import 其他服务的 `service/<x>/...` 包；第 5 周用 golangci-lint 的 `depguard` 规则固化。

### D2 Thrift

- 决策：RPC IDL 用 Thrift，放在 `idl/*.thrift`；生成代码 `kitex_gen/` 提交入库。
- 理由：Thrift 是 Kitex 的主力协议，字节内部服务大量使用，配套的 Frugal/FastCodec 性能最好；与目标岗位技术栈一致。
- 网关对外是 HTTP JSON，路由在 Hertz 中手写，不用 `hz` 生成。原因是网关很薄，手写路由更容易控制验签、限流中间件顺序。
- 代价：Thrift 生态的工具（lint、breaking change 检查）不如 Protobuf（buf）。约定：字段只增不删，新增字段一律 `optional`。

### D3 Kafka + franz-go

- 决策：Kafka 4.3.1 单节点 KRaft；客户端 franz-go。
- 理由：
  - 一致性方案是本地消息表（outbox），对 MQ 只要求 at-least-once。RocketMQ 的事务消息是 outbox 的替代方案，选 Kafka 反而能在面试中说明“为什么不用事务消息”。
  - Kafka 单容器即可运行；RocketMQ 5 需要 NameServer + Broker + Proxy 三个进程，Go v5 客户端还要经过 gRPC Proxy，本地环境更重。
  - franz-go 持续活跃（2026-09 仍在发版），支持幂等生产者、手动提交 offset、按分区并发消费；segmentio/kafka-go 更新较慢，API 对手动提交支持较弱。
- 代价：Kafka 没有原生延迟消息和消息级重试。重试和延迟由 outbox 的 `next_retry_at` 与通知表的 `next_attempt_at` 在数据库侧实现。
- 回滚条件：第 1 周 franz-go 与 Kafka 4.3.1 联调失败且一天内无法解决，改用 segmentio/kafka-go v0.4.51。

### D4 sqlx + 手写 SQL

- 决策：数据访问层用 sqlx，SQL 全部手写；每个服务一个 `dal` 包，对上暴露 repository 接口。
- 理由：资金代码的核心是条件更新（`WHERE status = ?`、`WHERE balance - frozen_amount >= ?`）、判断影响行数、`FOR UPDATE SKIP LOCKED`、唯一键冲突识别。手写 SQL 让这些语义直接可见，评审和面试时可以逐行解释；GORM 的零值更新、隐式 `updated_at`、钩子容易引入难察觉的问题。
- 代价：sqlx 最近一次发版是 2024-04（v1.4.0），API 稳定但更新慢。它只是 `database/sql` 的薄封装，必要时可替换成原生 `database/sql`，repository 接口不变。
- 约定：
  - 事务统一走 `pkg/dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error)`，禁止在事务中调用 RPC 或 MQ。
  - MySQL 唯一键冲突（错误号 1062）统一转换为 `errno.ErrDuplicate`，由业务层决定是否当作幂等成功。
- 补充（2026-10-14，BASE-06）：`errno.ErrDuplicate` 与 MySQL 1062 映射仍留给 BASE-08，本功能点不实现。`pkg/errno` 只定义约定 §5.2 的业务码（跳过保留的 `30002`），并直接依赖 kitex v0.16.3 的 `BizStatusError` 做双向转换。
  - 隔离级别统一 READ COMMITTED（DSN 设置 `transaction_isolation='READ-COMMITTED'`）。正确性依赖条件更新和唯一索引，不依赖 RR 的间隙锁；RC 可以减少间隙锁导致的死锁。这是一个面试讲解点。

### D5 自研雪花 ID

- 决策：`pkg/idgen` 自研，约 100 行。
  - 布局：1 位符号 + 41 位毫秒时间戳（纪元 2026-01-01 00:00:00 +08:00）+ 10 位 worker ID + 12 位序列号。
  - worker ID 由环境变量 `WORKER_ID` 静态分配（docker-compose 中每个实例固定），启动时校验范围 0–1023。
  - 时钟回拨 ≤ 5ms 时等待，> 5ms 时返回错误，拒绝发号。
  - 业务单号 = 2 位大写前缀 + 19 位定长十进制，共 21 字符。前缀：`PA` 支付单、`RF` 退款单、`TX` 记账凭证、`FZ` 冻结单、`EV` 事件、`NT` 通知、`AC` 账户、`RB` 对账批次、`RD` 差错、`MC` 模拟渠道流水。
- 理由：逻辑简单，能讲清楚时钟回拨、worker 分配、趋势递增对 B+ 树写入的影响；不引入额外依赖。
- 数据库主键仍用自增 `BIGINT`，业务单号走唯一索引。自增主键保证聚簇索引顺序写，业务单号对外暴露、不泄露订单量。
- 补充（2026-10-14，BASE-05）：回拨比较墙上毫秒，不用单调钟。等待可被 context 取消。超过 5ms 返回 `ErrClockRollback`，由调用方映射为已有的 `10004`；本包不依赖 `pkg/errno`，也不实现 `pkg/clock`。前缀以约定 §2.2 全表为准（含后来补上的 `TF`）。worker 分配见约定 §2.5。

### D6 加钱异步、扣钱同步

- 背景：支付成功后要给商户入账；退款要从商户余额扣钱。两者对“失败”的容忍度不同。
- 决策：
  - 支付成功入账：payment 在推进订单为 SUCCESS 的同一事务内写 outbox，经 Kafka 投递给 account 异步入账。入账没有业务失败（不存在“余额不足”），只需要保证最终执行，最适合异步。
  - 退款扣款：创建退款时 payment 通过 Kitex 同步调用 `account.Freeze` 冻结商户余额，余额不足立即失败并返回给商户；退款成功或失败后，再通过 outbox + Kafka 异步通知 account 执行 `ConfirmFreeze`（扣款）或 `ReleaseFreeze`（解冻）。冻结成功后，确认扣款不会因余额不足失败，因此可以异步。
- 理由：这是 TCC 的简化形态（Try = 冻结，Confirm / Cancel = 确认或解冻），既避免“先调渠道退款、事后发现余额不足”的资损，又不需要分布式事务框架。
- 代价：支付成功到入账之间有秒级延迟，商户在入账前发起的退款会因余额不足失败。真实系统有结算周期（D+1），本项目在需求中明确这一边界（REF-02 第 4 条）。
- 补充（2026-10-11）：Freeze 调用超时时结果未知，退款可能在冻结“迟到成功”之前被判为失败。因此 ReleaseFreeze 处理空回滚（找不到冻结单时写入 RELEASED 墓碑记录），迟到的 Freeze 撞上唯一索引后返回 `40004`，不会冻结资金（防悬挂，需求 ACC-05）。

### D7 不做前端

- 决策：不做调试页面。提供：
  - `cmd/paycli`：读取商户密钥，自动计算签名，调用全部商户接口，便于手工调试和演示。
  - `cmd/merchant-demo`：模拟商户后台，接收并验签异步通知，记录收到的通知次数。
  - `docs/api/openapi.yaml`（P1）：对外接口文档。Swagger UI 无法自动签名，因此不部署 UI。
  - `scripts/demo.sh`：一条命令演示“下单 → 支付 → 回调 → 入账 → 退款 → 对账”。

### D8 Go 1.27 + 显式锁定 sonic

- 决策：使用本机已安装的 Go 1.27；`go.mod` 中显式 require `github.com/bytedance/sonic v1.15.4`。
- 理由：sonic 依赖运行时内部结构，旧版本在新 Go 上会回退到 `encoding/json`，性能明显下降（已复现）。
- 规则：升级 Go 小版本前，先跑一次启动检查，确认日志中没有 sonic 告警。

### D9 服务发现

- 决策：不引入注册中心。Kitex 客户端用 `client.WithHostPorts`，地址来自环境变量，docker-compose 内用服务名解析。
- 理由：本地单机部署没有动态扩缩容需求。注册中心（etcd）列为 P2 加分项。

### D10 数据库划分

- 决策：一个 MySQL 实例，按服务划分 schema：`pay_account_0`、`pay_account_1`（account 服务的两个分片，见 D17）、`pay_payment`、`pay_recon`、`pay_channel_mock`。每个服务使用独立的数据库账号，只对自己的 schema 有写权限。
- 对账服务对 `pay_payment`、`pay_account_0`、`pay_account_1` 只有 `SELECT` 权限，模拟真实系统中从只读库或数仓读取数据。这是有意的取舍：对账要扫全天数据，走 RPC 分页反而增加业务服务负担。
- channel_mock 的数据放在独立 schema，与平台数据完全隔离，模拟“渠道是外部系统”。

### D11 签名算法与密钥存储

- 决策：商户请求、平台响应、商户通知、渠道回调统一使用 HMAC-SHA256（算法与测试向量见 `docs/05-api.md` §3，实现要求见 `docs/02-requirements.md` BASE-13）。
- 商户密钥在 `merchant.app_secret_cipher` 中以 AES-256-GCM 加密存储，主密钥来自环境变量 `SECRET_MASTER_KEY`。HMAC 需要平台持有可还原的密钥，所以只能加密，不能哈希。
- 取舍：HMAC 无法提供不可抵赖性，真实支付平台多用 RSA2 或 SM2。非对称签名（Ed25519）列为 P2，签名模块按接口抽象，便于替换。

### D12 配置

- 决策：只用环境变量配置（caarlos0/env），每个服务提供带默认值的配置结构体，仓库提供 `deploy/.env.example`。
- 理由：与 docker-compose 天然配合；测试中可以直接构造配置结构体，把补偿间隔调短。

### D13 日志

- 决策：标准库 `log/slog`，JSON 输出；`pkg/logx` 提供脱敏 Handler，按字段名（`card_no`、`phone`、`id_card`、`app_secret`、`signature` 等）自动脱敏；Hertz 的 hlog 和 Kitex 的 klog 通过适配器写入同一个 slog。每条日志带 `trace_id`。

### D14 可观测

- 决策：
  - Tracing：OTel SDK 通过 OTLP/gRPC 直接上报 Jaeger 2.22，不部署 OTel Collector。
  - Metrics：各服务通过 `prometheus/client_golang` 暴露 `/metrics`，Prometheus 拉取。不部署 Grafana、不配置告警规则（2026-10-12 简化）；压测报告直接用 Prometheus 查询结果和 Jaeger 截图。
  - pprof：每个服务在独立的管理端口暴露 `/debug/pprof`，与业务端口分离，不对外映射。
- 风险：`hertz-contrib/obs-opentelemetry/tracing` 最近发版在 2024-05，已确认能与 otel v1.47 一起编译，但链路串联未验证。如果第 5 周验证失败，改为自写 Hertz 中间件（约 50 行）。

### D15 测试工具

| 层次 | 工具 | 运行方式 |
|---|---|---|
| 单元测试 | 标准库 testing + testify | `make test`，不依赖 Docker |
| 集成测试 | testcontainers-go 启动 MySQL 8.4.11；也可用环境变量 `TEST_MYSQL_DSN` 复用已有实例 | `make test-integration`，文件带 `//go:build integration` |
| 端到端 | docker-compose + `paycli` + Go 测试 | `make e2e` |
| 故障演练 | shell + docker 命令 + 校验 SQL | `make chaos` |
| 压测 | k6（`k6/crypto` 计算 HMAC） | `make load` |

并发用例统一加 `-race` 运行。

### D16 数据库迁移

- 决策：golang-migrate，迁移文件位于 `migrations/<schema>/NNNNNN_<name>.up.sql` / `.down.sql`；docker-compose 中用一次性容器 `migrate/migrate:v4.20.1` 在服务启动前执行。集成测试复用同一批迁移文件。
- 第 1 周已把设计稿拆成 `migrations/<服务>/000001_init.up.sql` 与 `.down.sql`，并删除草稿。迁移文件是唯一来源。
- 两个账户分片共用 `migrations/account/`，迁移容器对每个分片 DSN 各执行一次。建库和最小权限账号由 `cmd/bootstrap` 在迁移前执行，不写进迁移文件：golang-migrate 要求目标库已经存在。
- `down` 使用 CLI 的 `down -all`。不带 `-all` 时镜像会在非交互环境等待确认并失败。

### D17 账户分片（2026-10-12）

- 背景：项目要展示跨库资金操作的一致性。单库内的转账只是一个本地事务，没有讨论空间。
- 决策：
  - account 数据拆为两个 schema `pay_account_0`、`pay_account_1`，表结构完全相同。本地放在同一个 MySQL 实例，但 account 进程为每个分片维护独立的连接池（`ACCOUNT_DB_DSN_0`、`ACCOUNT_DB_DSN_1`），代码中不允许跨分片事务，因此换成两个实例不需要改代码。
  - 路由键是商户号：`shard = 商户号数字部分 % ACCOUNT_SHARD_COUNT`（`M100001` → 1，`M100002` → 0）。路由函数放在 `pkg/shard`，payment 与 account 共用，payment 在转账单上记录双方分片。
  - 平台账户（渠道应收、手续费收入、转账在途）在每个分片各开一套。支付入账、退款扣款只涉及“商户 + 本分片平台账户”，始终是单分片本地事务。
  - 分片数固定为 2，不做在线扩容（扩容需要迁移数据，超出范围）。
- 理由：
  - 用取模而不是 crc32，是为了让演示商户的分片位置可预期，便于构造同分片和跨分片场景。
  - 账户号不编码分片位，所有 account RPC 以商户号为入参，与真实系统“按用户 ID 路由”的做法一致。
- 代价：同一实例上的两个 schema 不能体现网络分区，跨分片的故障用进程崩溃和 RPC 超时模拟（QA-01 C9–C11）。

### D18 跨分片转账：payment 作为 TCC 协调者（2026-10-12）

- 背景：商户间转账的付款方和收款方可能在不同分片，不能用一个本地事务完成。
- 决策：
  - payment 新增 `transfer_order`，作为协调者持久化全局状态：`TRYING → CONFIRMING → SUCCESS`、`TRYING → CANCELLING → FAILED`。同分片转账不走 TCC，由 account 在一个本地事务内完成，转账单 `TRYING → SUCCESS / FAILED`。
  - 付款方分支复用冻结：Try = `Freeze(TRANSFER)`，Confirm = `ConfirmFreeze`（借付款商户、贷本分片“转账在途”），Cancel = `ReleaseFreeze`。空回滚、防悬挂沿用 ACC-05 的墓碑设计。
  - 收款方分支新增 `transfer_branch`：Try 写入 TRIED（校验收款账户可用），Confirm 把 TRIED 改为 CONFIRMED 并记账（借本分片“转账在途”、贷收款商户），Cancel 改为 CANCELLED；Cancel 先到时写 CANCELLED 墓碑，迟到的 Try 返回 `40004`。
  - 两个 Try 并行发起；全部成功后先 CAS 为 CONFIRMING（决策落盘），再并行 Confirm。任一 Try 明确失败或结果未知，CAS 为 CANCELLING 后并行 Cancel。CONFIRMING、CANCELLING 一旦落盘只能前进，恢复任务重试到成功为止。
  - 两个分片的“转账在途”账户余额之和，在没有进行中的转账时恒为 0，是跨分片资金守恒的检查点。
- 理由：
  - 协调者放在 payment，可以复用 outbox、补偿任务和商户通知；account 只提供幂等的分支操作，不感知全局状态。
  - 不用 Seata 等框架：分支逻辑就是现有的冻结和一个分支表，自己实现更能讲清楚空回滚、防悬挂和决策落盘。
  - 不用 XA：两阶段提交期间持有行锁，热点商户会被长时间锁住；而且 MySQL XA 在协调者崩溃时需要人工处理悬挂事务。
- 代价：转账结果可能是“处理中”，商户需要按通知或查询获取最终状态；Confirm 阶段两个分支提交有先后，期间在途账户余额非 0，核对时需要按分支状态计算预期值（ACC-10）。

### D19 热点平台账户拆分子账户（2026-10-12）

- 背景：每笔支付入账都要更新本分片的渠道应收和手续费收入账户，行锁把所有入账事务串行化。
- 决策：平台账户拆为 `PLATFORM_SUB_ACCOUNTS` 个子账户（`account.sub_no`，默认 8），记账时按 `crc32(biz_no) % N` 选择；对外查询和核对按子账户汇总。商户账户不拆分（`sub_no = 0`），因为它要做余额检查。
- 理由：平台账户允许透支，单个子账户余额为负不影响正确性；每个子账户都是独立的账户行，余额不变量逐行成立，核对逻辑不需要特殊处理。
- 对比方式：`PLATFORM_SUB_ACCOUNTS=1` 作为基线，与 8 对比行锁等待和入账吞吐（OPT-03）。
- 未采用：缓冲入账（平台账户余额由后台任务汇总更新）收益更大，但让 `balance_after` 失去意义，列为 P2（OPT-05）。

### D20 批量记账合并提交（2026-10-12）

- 背景：`payment.succeeded` 逐条消费时，每条消息一个事务、一次提交，提交次数和行锁获取次数是吞吐上限。
- 决策：account 消费者把一个分区一次拉取到的 `payment.succeeded`（最多 `LEDGER_BATCH_SIZE` 条）按分片分组，每组一个事务：预查已存在的凭证去重 → 批量插入凭证 → 按账户聚合增量、按账户 id 升序每个账户更新一次 → 批量插入分录 → 提交。所有分片组提交成功后才提交 offset。插入凭证遇唯一键冲突或任一条不可重试错误时，该组回滚并退化为逐条处理，坏消息照常进入 DLQ。
- 理由：一批 100 条消息涉及的账户行数远小于 200（同商户、同平台子账户合并），提交次数降为 1 / 100；幂等仍由 `uk(biz_type, biz_no)` 兜底，不依赖批次边界。
- 开关：`LEDGER_BATCH_ENABLED=false` 为逐条模式，作为对比基线（OPT-02）。只批量处理入账事件；退款确认、解冻仍逐条处理，因为它们要改冻结单状态。

### D21 链路延迟优化方法（2026-10-12）

- 决策：三条链路（支付、退款、转账）的每个阶段都上报 `pay_stage_duration_seconds{chain, stage}`，先用基线压测得到分阶段耗时，再按占比选择优化项。候选项：跨分片转账两个分支并行（`TRANSFER_PARALLEL_BRANCH`）、商户凭证与配置进程内缓存（GW-08）、事务内减少往返（批量插入、合并查询）、连接池参数。每项优化单独开关、单独压测，报告给出前后 P50/P99 对比（OPT-04）。
- 理由：先测量再优化；每项改动都能被单独证明有效或无效，失败的尝试也写进报告。

## 4. Redis 使用边界

Redis 只承担以下职责，任何账务事实不得只存在于 Redis：

| 用途 | Key 示例 | Redis 不可用时 |
|---|---|---|
| nonce 防重放 | `gw:nonce:{app_id}:{nonce}`，TTL 10 分钟 | 写接口拒绝请求（fail-closed，返回 503） |
| 商户级限流 | 不用 Redis：网关进程内按 app_id 的令牌桶（`golang.org/x/time/rate`），多实例时配额按实例数放大（README 说明） | 不涉及 |
| 商户密钥缓存 | 网关进程内缓存，不放 Redis | 不涉及 |
| 订单查询缓存 | 不做 | 订单状态变化频繁，缓存一致性收益太低 |

分布式锁不作为正确性手段：补偿任务用 `FOR UPDATE SKIP LOCKED` 抢占任务，并发安全由数据库保证。
