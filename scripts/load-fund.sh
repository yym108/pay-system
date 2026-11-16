#!/usr/bin/env bash
# QA-02 压测入账脚本：为 M200000–M200099 通过真实支付链路（下单 -> 支付 -> 回调入账）充值，幂等可重复执行。
set -euo pipefail

GATEWAY_URL=${GATEWAY_URL:-http://127.0.0.1:8080}
DEMO_APP_SECRET_FORMAT=${DEMO_APP_SECRET_FORMAT:-demo_secret_%s_0123456789ab}
TARGET_BALANCE=${TARGET_BALANCE:-20000000} # 默认每个商户至少 200,000.00 元（20,000,000 分）
FUND_PER_ORDER=${FUND_PER_ORDER:-10000000} # 单笔上限 10,000,000 分
MERCHANT_START=${MERCHANT_START:-200000}
MERCHANT_END=${MERCHANT_END:-200099}
CONCURRENCY=${CONCURRENCY:-16}

python3 - <<'PY'
import concurrent.futures
import hashlib
import hmac
import base64
import json
import os
import secrets
import string
import sys
import time
import urllib.error
import urllib.request

gateway_url = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8080").rstrip("/")
secret_fmt = os.environ.get("DEMO_APP_SECRET_FORMAT", "demo_secret_%s_0123456789ab")
target_balance = int(os.environ.get("TARGET_BALANCE", "20000000"))
fund_per_order = int(os.environ.get("FUND_PER_ORDER", "10000000"))
m_start = int(os.environ.get("MERCHANT_START", "200000"))
m_end = int(os.environ.get("MERCHANT_END", "200099"))
concurrency = int(os.environ.get("CONCURRENCY", "16"))

alphabet = string.ascii_letters + string.digits

def make_nonce():
    return "".join(secrets.choice(alphabet) for _ in range(24))

def request_api(method, path, merchant_id, body_obj=None, timeout=10):
    app_id = f"app_{merchant_id.lower()}"
    secret = secret_fmt % merchant_id
    body_str = "" if body_obj is None else json.dumps(body_obj, separators=(",", ":"))
    ts = str(int(time.time()))
    nonce = make_nonce()
    body_hash = hashlib.sha256(body_str.encode("utf-8")).hexdigest()
    canonical = f"{method.upper()}\n{path}\n{ts}\n{nonce}\n{body_hash}"
    sig = base64.b64encode(hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).digest()).decode("ascii")
    headers = {
        "Content-Type": "application/json",
        "X-App-Id": app_id,
        "X-Timestamp": ts,
        "X-Nonce": nonce,
        "X-Signature": sig,
    }
    req = urllib.request.Request(
        f"{gateway_url}{path}",
        data=None if body_obj is None else body_str.encode("utf-8"),
        headers=headers,
        method=method.upper(),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw}

def get_balance(merchant_id):
    status, resp = request_api("GET", "/v1/balance", merchant_id)
    if status != 200 or resp.get("code") != 0:
        raise RuntimeError(f"{merchant_id} get_balance failed: status={status} resp={resp}")
    return int(resp["data"]["available_amount"])

def fund_merchant(merchant_id):
    avail = get_balance(merchant_id)
    if avail >= target_balance:
        return f"{merchant_id}: already={avail} (skip)"
    needed = target_balance - avail
    batches = (needed + fund_per_order - 1) // fund_per_order
    # 先读取已有余额，按幂等批次号充值（支持补充）
    for i in range(1, batches + 1):
        out_trade_no = f"LOAD_FUND_{merchant_id}_{i:03d}"
        amt = min(fund_per_order, target_balance - avail - (i - 1) * fund_per_order)
        if amt <= 0:
            break
        st, resp = request_api("POST", "/v1/payments", merchant_id, {
            "out_trade_no": out_trade_no,
            "amount": amt,
            "subject": f"load fund {merchant_id}",
        })
        if st != 200 or resp.get("code") != 0:
            raise RuntimeError(f"{merchant_id} create payment failed: status={st} resp={resp}")
        pay_no = resp["data"]["pay_no"]
        p_status = resp["data"]["status"]
        if p_status == "INIT":
            st2, resp2 = request_api("POST", f"/v1/payments/{pay_no}/pay", merchant_id, {"channel": "mockpay"})
            if st2 != 200 or resp2.get("code") != 0:
                raise RuntimeError(f"{merchant_id} pay {pay_no} failed: status={st2} resp={resp2}")
    # 轮询等待入账完成
    deadline = time.time() + 60
    while time.time() < deadline:
        cur = get_balance(merchant_id)
        if cur >= target_balance:
            return f"{merchant_id}: funded={cur}"
        time.sleep(0.2)
    cur = get_balance(merchant_id)
    raise RuntimeError(f"{merchant_id} fund timeout: available={cur} < target={target_balance}")

merchants = [f"M{i}" for i in range(m_start, m_end + 1)]
print(f"Funding {len(merchants)} merchants (target_balance={target_balance}) via {gateway_url}...")
errors = []
with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
    futs = {pool.submit(fund_merchant, m): m for m in merchants}
    for fut in concurrent.futures.as_completed(futs):
        m = futs[fut]
        try:
            _ = fut.result()
        except Exception as e:
            errors.append(f"{m}: {e}")

if errors:
    for err in errors[:10]:
        print(f"[ERROR] {err}", file=sys.stderr)
    sys.exit(1)

print(f"All {len(merchants)} merchants funded >= {target_balance}.")
PY
