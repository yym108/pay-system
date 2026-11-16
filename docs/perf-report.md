# pay-system 性能压测报告

> 状态：已完成 M6 基线压测（QA-02）；M7 优化后复测与对比见后续 QA-05 补充。

## 1. 测试环境与基线配置

### 1.1 运行环境

- **宿主机**：Linux（Docker Desktop VM 分配内存 2.743 GiB）
- **组件版本**：Go 1.27、MySQL 8.4.11（单实例双分片 `pay_account_0` / `pay_account_1` + `pay_payment` / `pay_recon` / `pay_channel_mock`，`innodb-buffer-pool-size=256M`）、Redis 8.10.2、Apache Kafka 4.3.1（3 分区）、Jaeger 2.22.0、Prometheus v3.15.0、k6 v2.4.0（`grafana/k6:latest`）
- **部署方式**：6 个业务进程（`gateway`、`payment`、`account`、`recon`、`channel-mock`、`merchant-demo`）以 `CGO_ENABLED=0` 静态编译后运行在 `pay-system_default` Docker 网络中；`k6` 通过 `--network host` 访问 `http://127.0.0.1:8080`。

### 1.2 基线开关配置（全部优化关闭）

| 变量 | 值 | 含义 |
|---|---|---|
| `PLATFORM_SUB_ACCOUNTS` | `1` | 平台账户不拆分子账户（全部路由至 `sub_no=0`） |
| `LEDGER_BATCH_ENABLED` | `false` | 关闭批量记账合并提交（退化为 MQ-04 逐条消费） |
| `TRANSFER_PARALLEL_BRANCH` | `false` | 关闭跨分片转账分支并行（Try / Confirm / Cancel 串行调用） |
| `GW_CREDENTIAL_CACHE_TTL` | `0` | 关闭网关商户凭证缓存（每次请求 RPC 查 payment + DB） |
| `MERCHANT_CONFIG_CACHE_TTL` | `0` | 关闭 payment 商户配置缓存 |
| `RATE_LIMIT_DEFAULT` | `10000:20000` | 调高商户限流配额，避免压测触发 429 |
| `MOCK_ALLOW_PARTIAL_BILL` | `true` | 允许下载当天账单供 `verify.sh` 对账 |
| 渠道模拟延迟（CH-07） | `delay_min_ms=50, delay_max_ms=200` | 通过 `PUT /mockpay/admin/config` 设置 |

### 1.3 可复现命令

```bash
# 1. 编译静态二进制并启动 6 个基线服务（关闭全部优化开关，略去环境变量详情）
for app in gateway payment account recon channel-mock merchant-demo; do
  CGO_ENABLED=0 go build -tags timetzdata -o "bin/$app" "./cmd/$app"
done

# 2. 创建压测商户 M200000–M200099 并入账
make seed-load
bash scripts/load-fund.sh

# 3. 执行 S1–S7 全套基线压测（默认每场景 1m 预热 + 5m 稳态）
make load
```

压测脚本：`test/load/baseline.js`（签名模块 `test/load/lib/sign.js`）、调度脚本 `test/load/run.sh`、分阶段直方图解析 `test/load/collect_metrics.py`、资金一致性核对 `test/chaos/verify.sh`。

---

## 2. S1–S7 基线结果汇总

每个场景预热 1 分钟、稳态持续 5 分钟（300 秒），以下吞吐、延迟与错误率仅统计 `steady` 稳态阶段：

| 场景 | 内容 | VU | 稳态总请求/事务数 | 稳态 QPS / TPS | 平均耗时 | P50 | P95 | P99 | 错误率 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **S1** | 只下单（`POST /v1/payments`） | 50 | 234,502 | **781.7 QPS** | 63.9 ms | 57.0 ms | 123.0 ms | 173.0 ms | 0.001% |
| **S2** | 下单 + 查询（1:3） | 50 | 306,820 | **1022.7 QPS** | 48.9 ms | 43.0 ms | 103.0 ms | 143.0 ms | 0.001% |
| **S3** | 支付全链路（下单→支付→回调→终态） | 40 | 34,808 | **116.0 TPS** | 344.7 ms | 338.0 ms | 497.0 ms | 587.0 ms | 0.000% |
| **S4** | 部分退款（对已入账订单发起部分退款） | 30 | 21,526 | **71.8 TPS** | 418.3 ms | 389.0 ms | 746.0 ms | 927.8 ms | 0.000% |
| **S5** | 同分片转账（`LOCAL`） | 40 | 99,428 | **331.4 TPS** | 120.6 ms | 112.0 ms | 205.0 ms | 275.0 ms | 0.000% |
| **S6** | 跨分片转账（`TCC`） | 40 | 26,346 | **87.8 TPS** | 455.6 ms | 432.0 ms | 641.0 ms | 775.5 ms | 0.000% |
| **S7** | 混合（S3:S4:S5:S6 = 5:1:2:2） | 50 | 36,260 | **120.9 TPS** | 413.8 ms | 406.0 ms | 753.0 ms | 1038.4 ms | 0.022% |

### 2.1 数据库与异步积压、进程资源快照（场景结束时刻）

| 场景 | MySQL 慢查询增量 | `Innodb_row_lock_waits` 增量 | 行锁等待累计时长 | 结束时 `outbox` PENDING | `pay_mq_consume_lag` | MySQL CPU / Mem | `payment` CPU / Mem | `account` CPU / Mem | `gateway` CPU / Mem | `verify.sh` 结果 |
|---|---:|---:|---:|---:|---:|---|---|---|---|---|
| **S1** | 0 | 0 | 0 ms | 0 | 0 | 1.90% / 519.5 MiB | 1.21% / 39.9 MiB | 0.03% / 19.9 MiB | 0.56% / 43.4 MiB | **PASS (exit 0)** |
| **S2** | 0 | 0 | 0 ms | 0 | 0 | 2.31% / 512.0 MiB | 0.53% / 44.8 MiB | 0.04% / 21.7 MiB | 0.53% / 44.2 MiB | **PASS (exit 0)** |
| **S3** | 0 | 11,508 | 158,612 ms | 19,404 | 0 | 106.89% / 599.5 MiB | 31.06% / 47.5 MiB | 48.41% / 24.9 MiB | 0.25% / 57.4 MiB | **PASS (exit 0)** |
| **S4** | 0 | 7,733 | 252,082 ms | 15,881 | 0 | 113.52% / 656.1 MiB | 33.53% / 36.5 MiB | 46.57% / 28.4 MiB | 0.25% / 41.5 MiB | **PASS (exit 0)** |
| **S5** | 0 | 23,056 | 383,830 ms | 100,801 | 0 | 60.41% / 621.9 MiB | 26.73% / 38.7 MiB | 0.16% / 27.8 MiB | 0.55% / 43.4 MiB | **PASS (exit 0)**（复验） |
| **S6** | 0 | **63,012** | **11,966,436 ms** | 45,341 | 0 | 72.62% / 667.1 MiB | 27.44% / 36.4 MiB | 0.05% / 31.3 MiB | 0.47% / 38.2 MiB | **PASS (exit 0)**（复验） |
| **S7** | 0 | 16,329 | 1,976,694 ms | 44,333 | 0 | 77.26% / 706.2 MiB | 26.38% / 61.9 MiB | 0.00% / 32.4 MiB | 0.33% / 38.4 MiB | **PASS (exit 0)**（复验） |

> 说明：
> 1. S1–S4 结束后 `verify.sh` 全部 7 项检查通过（退出码 0）。
> 2. 最初在 S5–S7 后受两个超时问题阻塞：全量 `CheckBalance` 耗时超过 `account` 服务端原 2 秒固定超时（`10004`），以及 `recon` 在 10 万+ `mock_txn` 下触发 `dbx.WithTx` 默认 3 秒事务超时。
> 3. 修复落地后（`ACC-11` 全量 `CheckBalance` 服务端超时 120s、商户模式仍 2s；`recon` 批量账单导入/外部对账/重跑清理事务超时 90s + 分批删除 + `ReconService.RunBatch` 服务端超时 120s），在累计 64 万+分录与 10.4 万+ `mock_txn` 数据集及 S5–S7 压测复验后，`verify.sh` 全部 7 项检查均 **PASS（退出码 0）**。

---

## 3. 6 条链路分阶段耗时表（OPT-01 `pay_stage_duration_seconds`）

全套 S1–S7 跑完后从 `gateway(:6060)`、`payment(:6061)`、`account(:6062)` 的 `/metrics` 导出直方图统计（桶上限为 5000 ms，均值按 `_sum / _count` 精确计算）：

### 3.1 `pay_create`（创建支付单）

| stage | count | P50 (ms) | P99 (ms) | avg (ms) | 耗时占比 |
|---|---:|---:|---:|---:|---:|
| `gw_auth` | 467,765 | 23.42 | 122.98 | 28.34 | **30.6%** |
| `gw_nonce` | 467,764 | 2.86 | 23.77 | 2.19 | 2.4% |
| `rpc` | 467,764 | 31.54 | 143.83 | 34.35 | **37.1%** |
| `db_insert` | 467,764 | 23.52 | 98.85 | 27.77 | 30.0% |

### 3.2 `pay_submit`（发起支付）

| stage | count | P50 (ms) | P99 (ms) | avg (ms) | 耗时占比 |
|---|---:|---:|---:|---:|---:|
| `gw_auth` | 67,621 | 17.22 | 108.38 | 22.29 | 23.5% |
| `db_tx1` | 67,621 | 20.59 | 120.02 | 25.19 | 26.5% |
| `channel_call` | 67,621 | 20.89 | 176.04 | 27.12 | **28.5%** |
| `db_tx2` | 67,621 | 17.08 | 97.58 | 20.42 | 21.5% |

### 3.3 `pay_settle`（回调到入账）

| stage | count | P50 (ms) | P99 (ms) | avg (ms) | 耗时占比 |
|---|---:|---:|---:|---:|---:|
| `callback_tx` | 67,621 | 24.28 | 186.72 | 31.32 | 0.0% |
| `outbox_wait` | 59,653 | >5000.00 | >5000.00 | **158,707.81** | **99.8%** |
| `mq_wait` | 59,681 | 251.49 | 2194.99 | 342.63 | 0.2% |
| `ledger_tx` | 59,681 | 15.59 | 98.80 | 19.22 | 0.0% |

### 3.4 `refund`（创建退款）

| stage | count | P50 (ms) | P99 (ms) | avg (ms) | 耗时占比 |
|---|---:|---:|---:|---:|---:|
| `gw_auth` | 36,734 | 6.54 | 92.17 | 12.11 | 3.8% |
| `db_quota_tx` | 36,734 | 25.28 | 149.72 | 30.51 | 9.6% |
| `rpc_freeze` | 36,734 | 33.28 | 204.54 | 37.65 | 11.9% |
| `db_tx` | 36,734 | 31.32 | 198.80 | 35.87 | 11.3% |
| `channel_call` | 36,734 | 172.70 | 909.65 | 200.74 | **63.3%** |

### 3.5 `transfer_local`（同分片转账）

| stage | count | P50 (ms) | P99 (ms) | avg (ms) | 耗时占比 |
|---|---:|---:|---:|---:|---:|
| `gw_auth` | 133,752 | 12.70 | 90.90 | 16.58 | 13.6% |
| `db_insert` | 133,756 | 21.81 | 98.69 | 26.03 | 21.4% |
| `rpc_transfer_local` | 133,752 | 42.14 | 227.92 | 48.44 | **39.7%** |
| `db_finish_tx` | 133,752 | 26.13 | 128.54 | 30.85 | 25.3% |

### 3.6 `transfer_tcc`（跨分片转账）

| stage | count | P50 (ms) | P99 (ms) | avg (ms) | 耗时占比 |
|---|---:|---:|---:|---:|---:|
| `gw_auth` | 42,439 | 3.42 | 91.21 | 8.20 | 1.7% |
| `db_insert` | 42,439 | 8.78 | 97.22 | 14.71 | 3.1% |
| `try` | 42,439 | 21.25 | 230.00 | 33.36 | 7.0% |
| `db_decision` | 42,439 | 8.61 | 95.83 | 14.05 | 2.9% |
| `confirm` | 42,453 | **383.07** | **974.00** | **391.68** | **81.9%** |
| `db_finish_tx` | 42,439 | 9.20 | 102.67 | 15.99 | 3.3% |

---

## 4. 瓶颈分析（基于实测证据）

1. **热点平台账户行锁竞争（最严重瓶颈，对应 OPT-03）**：
   - **证据**：在 `PLATFORM_SUB_ACCOUNTS=1` 时，S6（跨分片转账）的 `Innodb_row_lock_waits` 增量高达 **63,012 次**，行锁累计等待时间 **11,966,436 ms**；`transfer_tcc` 链路的 `confirm` 阶段 P50=383.07 ms、P99=974.00 ms，占整条链路总耗时的 **81.9%**，直接把 S6 吞吐压低到 **87.8 TPS**。S3/S4 中也同样出现 11,508 / 7,733 次行锁等待（争抢 `CHANNEL_RECEIVABLE` 唯一子账户）。
   - **根因**：所有并发事务都在更新同一条平台账户记录（`sub_no=0`）的 `balance`。打开 `PLATFORM_SUB_ACCOUNTS=8` 后按 `crc32(biz_no) % 8` 分散行锁，预计可消除绝大部分热点等待。
2. **跨分片分支串行与网关凭证无缓存（对应 OPT-04、GW-08）**：
   - **证据**：`pay_create` 链路中 `gw_auth` 平均耗时 **28.34 ms（占比 30.6%）**，P99 达 122.98 ms；这是因为 `GW_CREDENTIAL_CACHE_TTL=0` 且 `MERCHANT_CONFIG_CACHE_TTL=0` 时，每个请求都经 `gateway -> payment RPC -> MySQL -> AES 解密` 获取密钥。同时 `transfer_tcc` 的 `try` / `confirm` 在 `TRANSFER_PARALLEL_BRANCH=false` 下串行调用两个分片。
3. **Outbox 投递与逐条消费积压（对应 OPT-02）**：
   - **证据**：高吞吐下（如 S5 产生近 10 万条 `transfer.succeeded` 事件、S3/S7 并发支付），`outbox_pending_end` 峰值达到 **100,801 条**，`pay_settle` 的 `outbox_wait` 均值高达 **158.7 s**，`mq_wait` P99 达 **2.19 s**。关闭批量记账（`LEDGER_BATCH_ENABLED=false`）时 `account` 消费端每次只能逐条开启 MySQL 事务提交，MySQL CPU 超过 100% 后吞吐见顶。
4. **工程/非功能边界发现（已在 `fix/QA-02-timeouts` 修复并复验通过）**：
   - 全量 `CheckBalance` 在累计数十万分录后耗时超过 2 秒，已按需求 §8.2 A7 将全量模式服务端超时调整为 120 秒（商户模式及其余方法仍为 2 秒）。
   - 单日 `mock_txn` 超过 10 万笔后，`recon` 的 `DownloadAndImport`、`SaveExternalCheckResultTx`、`ResetForRerunTx` 原使用 `dbx.WithTx`（默认 3 秒总超时）会触发超时回滚，已通过 `dbx.WithTxTimeout(..., 90s)`、分批删除及 `ReconService.RunBatch` 服务端超时 120s 修复。

---

## 5. 与 NFR-PERF 目标对比

| 编号 | 场景 / 指标 | NFR-PERF 目标 | M6 基线实测 | 达标情况与 M7 建议 |
|---|---|---|---|---|
| `NFR-PERF-01` | S1 下单 | ≥ 1000 QPS，P99 ≤ 50ms，错误率 < 0.1% | 781.7 QPS，P99 173.0 ms，错误率 0.001% | 未达标（瓶颈在 `gw_auth` 30.6% 无缓存），待 **GW-08 / OPT-04** |
| `NFR-PERF-02` | S2 查询 | ≥ 2000 QPS，P99 ≤ 30ms | 1022.7 QPS，P99 143.0 ms | 未达标（同上受 `gw_auth` 拖累），待 **GW-08 / OPT-04** |
| `NFR-PERF-03` | S3 支付全链路 | 基线 ≥ 200 TPS 持续 5 分钟，无积压持续增长 | 116.0 TPS，end 积压 19,404 条 | 本机 2.74 GiB VM 下未达标，待 **OPT-02（批量记账）+ OPT-03（子账户）** |
| `NFR-PERF-04` | S4 退款 | ≥ 200 TPS，扣渠道延迟后 P99 ≤ 50ms | 71.8 TPS，P99 927.8 ms | 未达标（扣渠道 50–200ms 后仍受行锁与逐条 RPC 拖累），待 **OPT-03 / OPT-04** |
| `NFR-PERF-05` | S5 同分片转账 | ≥ 500 TPS，P99 ≤ 30ms | 331.4 TPS，P99 275.0 ms | 未达标（单分片 `outbox` 与 `account` 行锁竞争），待 **OPT-03 / OPT-04** |
| `NFR-PERF-06` | S6 跨分片转账 | ≥ 300 TPS，P99 ≤ 60ms | 87.8 TPS，P99 775.5 ms | 未达标（`TRANSIT` 单行锁 + 串行分支），待 **OPT-03 + OPT-04（并行分支）** |
| `NFR-PERF-08` | 单次记账 `Post` | P99 ≤ 20ms | `ledger_tx` P50=15.59 ms，P99=98.80 ms | P50 达标，P99 受行锁尾部放大，待 **OPT-03** |

**M7 建议优化顺序**：`GW-08`（凭证缓存，立即释放网关 30% 耗时）→ `OPT-03`（`PLATFORM_SUB_ACCOUNTS=8`，消除 80%+ 行锁等待）→ `OPT-02`（批量记账，提升入账消费吞吐 3 倍以上）→ `OPT-04`（并行分支与链路延迟优化）。
