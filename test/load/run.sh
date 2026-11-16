#!/usr/bin/env bash
# QA-02 基线压测执行脚本（S1–S7）。
set -euo pipefail

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname "$0")" && pwd)
ROOT_DIR=$(CDPATH='' cd -- "$SCRIPT_DIR/../.." && pwd)
ENV_FILE=${ENV_FILE:-"$ROOT_DIR/deploy/.env"}
RESULTS_DIR=${RESULTS_DIR:-"$ROOT_DIR/test/load/results"}

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1091
  . "$ROOT_DIR/deploy/load-env.sh"
  load_env_file "$ENV_FILE"
fi

GATEWAY_URL=${GATEWAY_URL:-http://127.0.0.1:8080}
CHANNEL_ADMIN_URL=${CHANNEL_ADMIN_URL:-http://127.0.0.1:6064}
SCENARIOS=${SCENARIOS:-"S1 S2 S3 S4 S5 S6 S7"}
WARMUP_DURATION=${WARMUP_DURATION:-1m}
STEADY_DURATION=${STEADY_DURATION:-5m}
K6_IMAGE=${K6_IMAGE:-grafana/k6:latest}
MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}
SKIP_VERIFY=${SKIP_VERIFY:-0}

mkdir -p "$RESULTS_DIR"

run_sql() {
  local sql="$1"
  if [ -n "$MYSQL_PASSWORD" ]; then
    MYSQL_PWD="$MYSQL_PASSWORD" mysql --protocol=TCP -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -N -B -e "$sql"
  else
    mysql --protocol=TCP -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -N -B -e "$sql"
  fi
}

echo "=== 检查服务就绪 ==="
for port in 6060 6061 6062 6063 6064; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${port}/readyz" || true)
  if [ "$code" != "200" ]; then
    echo "服务端口 :${port} /readyz 未就绪 (HTTP ${code})，请先启动全部服务并关闭优化开关" >&2
    exit 1
  fi
done
echo "全部服务 /readyz 就绪。"

echo "=== 通过 CH-07 设置 channel_mock 延迟 50-200ms ==="
curl -sS -X PUT "${CHANNEL_ADMIN_URL}/mockpay/admin/config" \
  -H "Content-Type: application/json" \
  -d '{"delay_min_ms":50,"delay_max_ms":200,"dup_notify_rate":0,"drop_notify_rate":0,"accept_error_rate":0,"accept_timeout_rate":0}'
echo ""

wait_drain() {
  local deadline=$((SECONDS + 180))
  while [ "$SECONDS" -lt "$deadline" ]; do
    pending_outbox=$(run_sql "SELECT COUNT(*) FROM pay_payment.outbox WHERE status = 'PENDING';")
    proc_ctxn=$(run_sql "SELECT COUNT(*) FROM pay_payment.channel_txn WHERE status = 'PROCESSING';")
    pending_trf=$(run_sql "SELECT COUNT(*) FROM pay_payment.transfer_order WHERE status NOT IN ('SUCCESS', 'FAILED');")
    if [ "${pending_outbox:-0}" -eq 0 ] && [ "${proc_ctxn:-0}" -eq 0 ] && [ "${pending_trf:-0}" -eq 0 ]; then
      return 0
    fi
    sleep 1
  done
  echo "[WARN] drain timeout: outbox=${pending_outbox} ctxn=${proc_ctxn} trf=${pending_trf}" >&2
}

for sc in $SCENARIOS; do
  echo ""
  echo "========================================"
  echo "=== 开始压测场景 ${sc} (warmup=${WARMUP_DURATION}, steady=${STEADY_DURATION}) ==="
  echo "========================================"
  run_id=$(date +%Y%m%d%H%M%S)
  slow_before=$(run_sql "SHOW GLOBAL STATUS LIKE 'Slow_queries';" | awk '{print $2}')
  lock_wait_before=$(run_sql "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits';" | awk '{print $2}')
  lock_time_before=$(run_sql "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_time';" | awk '{print $2}')

  docker run --rm --network host \
    -v "$SCRIPT_DIR:/scripts:ro" \
    -v "$RESULTS_DIR:/results" \
    -e "GATEWAY_URL=$GATEWAY_URL" \
    -e "SCENARIO=$sc" \
    -e "WARMUP_DURATION=$WARMUP_DURATION" \
    -e "STEADY_DURATION=$STEADY_DURATION" \
    -e "DEMO_APP_SECRET_FORMAT=${DEMO_APP_SECRET_FORMAT:-demo_secret_%s_0123456789ab}" \
    -e "RUN_ID=$run_id" \
    "$K6_IMAGE" run \
    --summary-export="/results/${sc}.summary.json" \
    /scripts/baseline.js | tee "$RESULTS_DIR/${sc}.k6.log"

  slow_after=$(run_sql "SHOW GLOBAL STATUS LIKE 'Slow_queries';" | awk '{print $2}')
  lock_wait_after=$(run_sql "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits';" | awk '{print $2}')
  lock_time_after=$(run_sql "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_time';" | awk '{print $2}')
  outbox_pending=$(run_sql "SELECT COUNT(*) FROM pay_payment.outbox WHERE status = 'PENDING';")
  active_lock_waits=$(run_sql "SELECT COUNT(*) FROM performance_schema.data_lock_waits;")

  echo "--- 场景 ${sc} 系统快照 ---" | tee "$RESULTS_DIR/${sc}.sys.txt"
  echo "slow_queries_delta=$((slow_after - slow_before))" | tee -a "$RESULTS_DIR/${sc}.sys.txt"
  echo "innodb_row_lock_waits_delta=$((lock_wait_after - lock_wait_before))" | tee -a "$RESULTS_DIR/${sc}.sys.txt"
  echo "innodb_row_lock_time_ms_delta=$((lock_time_after - lock_time_before))" | tee -a "$RESULTS_DIR/${sc}.sys.txt"
  echo "outbox_pending_end=${outbox_pending}" | tee -a "$RESULTS_DIR/${sc}.sys.txt"
  echo "data_lock_waits_end=${active_lock_waits}" | tee -a "$RESULTS_DIR/${sc}.sys.txt"
  docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" 2>/dev/null | grep -E "NAME|pay-load|pay-system" | tee -a "$RESULTS_DIR/${sc}.sys.txt" || true
  ps -eo pid,comm,%cpu,rss --sort=-%cpu | grep -E "gateway|payment|account|recon|channel-mock|merchant-demo" | tee -a "$RESULTS_DIR/${sc}.sys.txt" || true

  echo "--- 等待异步排空 ---"
  wait_drain

  if [ "$SKIP_VERIFY" != "1" ]; then
    echo "--- 执行 verify.sh ---"
    ENV_FILE="$ENV_FILE" bash "$ROOT_DIR/test/chaos/verify.sh" | tee "$RESULTS_DIR/${sc}.verify.log"
  fi
done

echo ""
echo "=== 采集分阶段耗时直方图 ==="
python3 "$SCRIPT_DIR/collect_metrics.py" | tee "$RESULTS_DIR/stage_metrics.txt"
