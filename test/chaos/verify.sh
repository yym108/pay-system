#!/usr/bin/env bash
# QA-02 / QA-01 资金与状态一致性核对脚本。
# 任一项不通过时输出不通过的检查项并退出非 0。
set -euo pipefail

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname "$0")" && pwd)
ROOT_DIR=$(CDPATH='' cd -- "$SCRIPT_DIR/../.." && pwd)
ENV_FILE=${ENV_FILE:-"$ROOT_DIR/deploy/.env"}

# 按需从 ENV_FILE 读取连接与管理鉴权配置（不打印密钥）。
if [ -f "$ENV_FILE" ] && [ "${VERIFY_SKIP_ENV_FILE:-0}" != "1" ]; then
  # shellcheck disable=SC1091
  . "$ROOT_DIR/deploy/load-env.sh"
  load_env_file "$ENV_FILE"
fi

PAYMENT_DB=${PAYMENT_DB:-pay_payment}
ACCOUNT_DB_0=${ACCOUNT_DB_0:-pay_account_0}
ACCOUNT_DB_1=${ACCOUNT_DB_1:-pay_account_1}

MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}

GATEWAY_ADMIN_URL=${GATEWAY_ADMIN_URL:-http://127.0.0.1:8090}
ADMIN_TOKEN=${ADMIN_TOKEN:-}
VERIFY_STALE_SECONDS=${VERIFY_STALE_SECONDS:-60}
ALLOW_NEVER_FINISH=${ALLOW_NEVER_FINISH:-0}
ALLOW_DIFF_COUNT=${ALLOW_DIFF_COUNT:-0}
RECON_BILL_DATE=${RECON_BILL_DATE:-$(TZ=Asia/Shanghai date +%Y-%m-%d)}

if [ -z "$ADMIN_TOKEN" ]; then
  echo "[FAIL] CONFIG: ADMIN_TOKEN is required" >&2
  exit 1
fi

run_sql() {
  local sql="$1"
  if [ -n "$MYSQL_PASSWORD" ]; then
    MYSQL_PWD="$MYSQL_PASSWORD" mysql --protocol=TCP -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -N -B -e "$sql"
  else
    mysql --protocol=TCP -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -N -B -e "$sql"
  fi
}

FAILURES=0

fail_check() {
  local name="$1"
  local detail="$2"
  echo "[FAIL] ${name}: ${detail}" >&2
  FAILURES=$((FAILURES + 1))
}

pass_check() {
  local name="$1"
  local detail="${2:-ok}"
  echo "[PASS] ${name}: ${detail}"
}

# 1. 不存在超过阈值仍为 PROCESSING 的 channel_txn（除 NEVER_FINISH 场景）
never_finish_clause=""
if [ "$ALLOW_NEVER_FINISH" = "1" ]; then
  never_finish_clause="AND ct.biz_no NOT IN (SELECT pay_no FROM ${PAYMENT_DB}.payment_order WHERE subject LIKE '%NEVER_FINISH%' OR out_trade_no LIKE '%NEVER_FINISH%')"
fi
stale_ctxn=$(run_sql "
  SELECT COUNT(*)
  FROM ${PAYMENT_DB}.channel_txn ct
  WHERE ct.status = 'PROCESSING'
    AND ct.created_at < DATE_SUB(NOW(3), INTERVAL ${VERIFY_STALE_SECONDS} SECOND)
    ${never_finish_clause};
")
if [ "${stale_ctxn:-0}" -gt 0 ]; then
  sample=$(run_sql "
    SELECT CONCAT(ct.txn_type, ':', ct.biz_no, ':created=', ct.created_at)
    FROM ${PAYMENT_DB}.channel_txn ct
    WHERE ct.status = 'PROCESSING'
      AND ct.created_at < DATE_SUB(NOW(3), INTERVAL ${VERIFY_STALE_SECONDS} SECOND)
      ${never_finish_clause}
    LIMIT 5;
  " | tr '\n' ';')
  fail_check "STALE_CHANNEL_TXN" "count=${stale_ctxn}, sample=[${sample}]"
else
  pass_check "STALE_CHANNEL_TXN" "count=0"
fi

# 2. 不存在超过阈值仍未终态的 transfer_order
stale_trf=$(run_sql "
  SELECT COUNT(*)
  FROM ${PAYMENT_DB}.transfer_order
  WHERE status NOT IN ('SUCCESS', 'FAILED')
    AND created_at < DATE_SUB(NOW(3), INTERVAL ${VERIFY_STALE_SECONDS} SECOND);
")
if [ "${stale_trf:-0}" -gt 0 ]; then
  sample=$(run_sql "
    SELECT CONCAT(transfer_no, ':', status, ':created=', created_at)
    FROM ${PAYMENT_DB}.transfer_order
    WHERE status NOT IN ('SUCCESS', 'FAILED')
      AND created_at < DATE_SUB(NOW(3), INTERVAL ${VERIFY_STALE_SECONDS} SECOND)
    LIMIT 5;
  " | tr '\n' ';')
  fail_check "STALE_TRANSFER_ORDER" "count=${stale_trf}, sample=[${sample}]"
else
  pass_check "STALE_TRANSFER_ORDER" "count=0"
fi

# 3. 每个 SUCCESS 支付单恰好 1 张 PAY 凭证
bad_pay=$(run_sql "
  SELECT p.pay_no
  FROM ${PAYMENT_DB}.payment_order p
  LEFT JOIN ${ACCOUNT_DB_0}.ledger_txn t0
    ON t0.biz_type = 'PAY' AND t0.biz_no = p.pay_no
   AND (CAST(SUBSTRING(p.merchant_id, 2) AS UNSIGNED) % 2) = 0
  LEFT JOIN ${ACCOUNT_DB_1}.ledger_txn t1
    ON t1.biz_type = 'PAY' AND t1.biz_no = p.pay_no
   AND (CAST(SUBSTRING(p.merchant_id, 2) AS UNSIGNED) % 2) = 1
  WHERE p.status = 'SUCCESS'
    AND (IF(t0.id IS NOT NULL, 1, 0) + IF(t1.id IS NOT NULL, 1, 0)) != 1
  LIMIT 10;
")
if [ -n "$bad_pay" ]; then
  fail_check "PAY_LEDGER_TXN" "mismatched pay_no=[$(echo "$bad_pay" | tr '\n' ',')]"
else
  pass_check "PAY_LEDGER_TXN" "all SUCCESS payment orders have exactly 1 PAY ledger_txn"
fi

# 4. 每个 SUCCESS 退款单恰好 1 张 REFUND 凭证
bad_refund=$(run_sql "
  SELECT r.refund_no
  FROM ${PAYMENT_DB}.refund_order r
  LEFT JOIN ${ACCOUNT_DB_0}.ledger_txn t0
    ON t0.biz_type = 'REFUND' AND t0.biz_no = r.refund_no
   AND (CAST(SUBSTRING(r.merchant_id, 2) AS UNSIGNED) % 2) = 0
  LEFT JOIN ${ACCOUNT_DB_1}.ledger_txn t1
    ON t1.biz_type = 'REFUND' AND t1.biz_no = r.refund_no
   AND (CAST(SUBSTRING(r.merchant_id, 2) AS UNSIGNED) % 2) = 1
  WHERE r.status = 'SUCCESS'
    AND (IF(t0.id IS NOT NULL, 1, 0) + IF(t1.id IS NOT NULL, 1, 0)) != 1
  LIMIT 10;
")
if [ -n "$bad_refund" ]; then
  fail_check "REFUND_LEDGER_TXN" "mismatched refund_no=[$(echo "$bad_refund" | tr '\n' ',')]"
else
  pass_check "REFUND_LEDGER_TXN" "all SUCCESS refund orders have exactly 1 REFUND ledger_txn"
fi

# 5. 转账单满足不变量 12
bad_transfer=$(run_sql "
  SELECT CONCAT(tr.transfer_no, ':', tr.mode, ':', tr.status)
  FROM ${PAYMENT_DB}.transfer_order tr
  LEFT JOIN ${ACCOUNT_DB_0}.ledger_txn l0
    ON l0.biz_type = 'TRANSFER' AND l0.biz_no = tr.transfer_no AND tr.payer_shard = 0
  LEFT JOIN ${ACCOUNT_DB_1}.ledger_txn l1
    ON l1.biz_type = 'TRANSFER' AND l1.biz_no = tr.transfer_no AND tr.payer_shard = 1
  LEFT JOIN ${ACCOUNT_DB_0}.ledger_txn out0
    ON out0.biz_type = 'TRANSFER_OUT' AND out0.biz_no = tr.transfer_no AND tr.payer_shard = 0
  LEFT JOIN ${ACCOUNT_DB_1}.ledger_txn out1
    ON out1.biz_type = 'TRANSFER_OUT' AND out1.biz_no = tr.transfer_no AND tr.payer_shard = 1
  LEFT JOIN ${ACCOUNT_DB_0}.ledger_txn in0
    ON in0.biz_type = 'TRANSFER_IN' AND in0.biz_no = tr.transfer_no AND tr.payee_shard = 0
  LEFT JOIN ${ACCOUNT_DB_1}.ledger_txn in1
    ON in1.biz_type = 'TRANSFER_IN' AND in1.biz_no = tr.transfer_no AND tr.payee_shard = 1
  WHERE
    (tr.mode = 'LOCAL' AND tr.status = 'SUCCESS' AND (IF(l0.id IS NOT NULL, 1, 0) + IF(l1.id IS NOT NULL, 1, 0)) != 1)
    OR (tr.mode = 'LOCAL' AND tr.status = 'FAILED' AND (IF(l0.id IS NOT NULL, 1, 0) + IF(l1.id IS NOT NULL, 1, 0)) != 0)
    OR (tr.mode = 'TCC' AND tr.status = 'SUCCESS' AND (
         (IF(out0.id IS NOT NULL, 1, 0) + IF(out1.id IS NOT NULL, 1, 0)) != 1
      OR (IF(in0.id IS NOT NULL, 1, 0) + IF(in1.id IS NOT NULL, 1, 0)) != 1
    ))
    OR (tr.mode = 'TCC' AND tr.status = 'FAILED' AND (
         (IF(out0.id IS NOT NULL, 1, 0) + IF(out1.id IS NOT NULL, 1, 0)) != 0
      OR (IF(in0.id IS NOT NULL, 1, 0) + IF(in1.id IS NOT NULL, 1, 0)) != 0
    ))
  LIMIT 10;
")
if [ -n "$bad_transfer" ]; then
  fail_check "TRANSFER_INVARIANT_12" "mismatched transfers=[$(echo "$bad_transfer" | tr '\n' ',')]"
else
  pass_check "TRANSFER_INVARIANT_12" "all terminal transfers satisfy invariant 12"
fi

# 6. 全量 CheckBalance(expect_quiescent = true)
check_resp=$(curl -sS --max-time 125 -X POST "${GATEWAY_ADMIN_URL}/admin/accounts/check" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -d '{"expect_quiescent":true}' || true)
if [ -z "$check_resp" ]; then
  fail_check "CheckBalance" "empty response from ${GATEWAY_ADMIN_URL}/admin/accounts/check"
else
  code=$(echo "$check_resp" | jq -r '.code // -1')
  passed=$(echo "$check_resp" | jq -r '.data.passed // false')
  if [ "$code" != "0" ] || [ "$passed" != "true" ]; then
    failures_json=$(echo "$check_resp" | jq -c '.data.failures // .')
    fail_check "CheckBalance" "code=${code}, passed=${passed}, failures=${failures_json}"
  else
    pass_check "CheckBalance" "passed=true (expect_quiescent=true)"
  fi
fi

# 7. 对当天执行一次对账，差错数为 0（允许通过 ALLOW_DIFF_COUNT 放宽）
recon_resp=$(curl -sS --max-time 125 -X POST "${GATEWAY_ADMIN_URL}/admin/recon/run" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: ${ADMIN_TOKEN}" \
  -H "X-Admin-Operator: verify" \
  -d "{\"channel\":\"mockpay\",\"bill_date\":\"${RECON_BILL_DATE}\",\"force\":true}" || true)
if [ -z "$recon_resp" ]; then
  fail_check "RECON_RUN" "empty response from ${GATEWAY_ADMIN_URL}/admin/recon/run"
else
  rcode=$(echo "$recon_resp" | jq -r '.code // -1')
  rstatus=$(echo "$recon_resp" | jq -r '.data.batch.status // ""')
  rdiff=$(echo "$recon_resp" | jq -r '.data.batch.diff_count // -1')
  if [ "$rcode" != "0" ] || [ "$rstatus" != "DONE" ] || [ "$rdiff" -lt 0 ] || [ "$rdiff" -gt "$ALLOW_DIFF_COUNT" ]; then
    fail_check "RECON_RUN" "code=${rcode}, status=${rstatus}, diff_count=${rdiff}, allow=${ALLOW_DIFF_COUNT}, resp=${recon_resp}"
  else
    pass_check "RECON_RUN" "bill_date=${RECON_BILL_DATE}, status=${rstatus}, diff_count=${rdiff}"
  fi
fi

if [ "$FAILURES" -gt 0 ]; then
  echo "VERIFY FAILED: ${FAILURES} check(s) failed" >&2
  exit 1
fi

echo "VERIFY PASSED: all checks passed"
