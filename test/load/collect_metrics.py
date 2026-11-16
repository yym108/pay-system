#!/usr/bin/env python3
"""从 gateway(:6060)、payment(:6061)、account(:6062) 的 /metrics 采集 pay_stage_duration_seconds，计算各链路各 stage 的 P50 / P99 / 占比。"""
import math
import re
import sys
import urllib.request
from collections import defaultdict

ENDPOINTS = [
    ("gateway", "http://127.0.0.1:6060/metrics"),
    ("payment", "http://127.0.0.1:6061/metrics"),
    ("account", "http://127.0.0.1:6062/metrics"),
]

CHAINS = {
    "pay_create": ["gw_auth", "gw_nonce", "rpc", "db_insert"],
    "pay_submit": ["gw_auth", "db_tx1", "channel_call", "db_tx2"],
    "pay_settle": ["callback_tx", "outbox_wait", "mq_wait", "ledger_tx"],
    "refund": ["gw_auth", "db_quota_tx", "rpc_freeze", "db_tx", "channel_call"],
    "transfer_local": ["gw_auth", "db_insert", "rpc_transfer_local", "db_finish_tx"],
    "transfer_tcc": ["gw_auth", "db_insert", "try", "db_decision", "confirm", "cancel", "db_finish_tx"],
}

BUCKET_RE = re.compile(
    r'^pay_stage_duration_seconds_bucket\{([^}]*)\}\s+([0-9eE+\-.]+)$'
)
SUM_RE = re.compile(
    r'^pay_stage_duration_seconds_sum\{([^}]*)\}\s+([0-9eE+\-.]+)$'
)
COUNT_RE = re.compile(
    r'^pay_stage_duration_seconds_count\{([^}]*)\}\s+([0-9eE+\-.]+)$'
)
LAG_RE = re.compile(
    r'^pay_mq_consume_lag(?:\{([^}]*)\})?\s+([0-9eE+\-.]+)$'
)


def parse_labels(s):
    out = {}
    for part in s.split(","):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip().strip('"')
    return out


def hist_quantile(buckets, q):
    """线性插值估算直方图分位数（秒 -> 毫秒）。"""
    if not buckets:
        return 0.0
    sorted_b = sorted(buckets.items(), key=lambda x: float("inf") if x[0] == "+Inf" else float(x[0]))
    total = sorted_b[-1][1]
    if total <= 0:
        return 0.0
    target = total * q
    prev_bound = 0.0
    prev_cnt = 0.0
    for bound_str, cnt in sorted_b:
        bound = float("inf") if bound_str == "+Inf" else float(bound_str)
        if cnt >= target:
            if math.isinf(bound):
                return prev_bound * 1000.0
            span_cnt = cnt - prev_cnt
            if span_cnt <= 0:
                return bound * 1000.0
            frac = (target - prev_cnt) / span_cnt
            return (prev_bound + frac * (bound - prev_bound)) * 1000.0
        prev_bound = bound
        prev_cnt = cnt
    return prev_bound * 1000.0


def main():
    buckets = defaultdict(lambda: defaultdict(float))  # (chain, stage) -> {le: count}
    sums = defaultdict(float)
    counts = defaultdict(float)
    lags = {}

    for svc, url in ENDPOINTS:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                text = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            print(f"[WARN] fetch {svc} {url} failed: {e}", file=sys.stderr)
            continue

        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = BUCKET_RE.match(line)
            if m:
                lbl = parse_labels(m.group(1))
                key = (lbl.get("chain", ""), lbl.get("stage", ""))
                buckets[key][lbl.get("le", "+Inf")] += float(m.group(2))
                continue
            m = SUM_RE.match(line)
            if m:
                lbl = parse_labels(m.group(1))
                sums[(lbl.get("chain", ""), lbl.get("stage", ""))] += float(m.group(2))
                continue
            m = COUNT_RE.match(line)
            if m:
                lbl = parse_labels(m.group(1))
                counts[(lbl.get("chain", ""), lbl.get("stage", ""))] += float(m.group(2))
                continue
            m = LAG_RE.match(line)
            if m:
                lbl = m.group(1) or "default"
                lags[f"{svc}:{lbl}"] = float(m.group(2))

    print("=== pay_stage_duration_seconds ===")
    for chain, stages in CHAINS.items():
        chain_avg_total = 0.0
        stage_avgs = {}
        for st in stages:
            cnt = counts.get((chain, st), 0.0)
            avg_ms = (sums.get((chain, st), 0.0) / cnt * 1000.0) if cnt > 0 else 0.0
            stage_avgs[st] = avg_ms
            chain_avg_total += avg_ms
        print(f"\n[chain: {chain}]")
        print(f"{'stage':<20} {'count':>10} {'p50_ms':>10} {'p99_ms':>10} {'avg_ms':>10} {'share':>8}")
        for st in stages:
            cnt = int(counts.get((chain, st), 0.0))
            if cnt == 0:
                continue
            p50 = hist_quantile(buckets[(chain, st)], 0.50)
            p99 = hist_quantile(buckets[(chain, st)], 0.99)
            avg_ms = stage_avgs[st]
            share = (avg_ms / chain_avg_total * 100.0) if chain_avg_total > 0 else 0.0
            print(f"{st:<20} {cnt:>10d} {p50:>10.2f} {p99:>10.2f} {avg_ms:>10.2f} {share:>7.1f}%")

    if lags:
        print("\n=== pay_mq_consume_lag ===")
        for k, v in sorted(lags.items()):
            print(f"{k}: {v}")


if __name__ == "__main__":
    main()
