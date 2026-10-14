#!/bin/sh
# 轮询直到列出的长期容器 healthy，且 kafka-init 退出码为 0。
# 用法：wait-ready.sh SECONDS service...
set -eu

timeout=${1:?timeout seconds}
shift
if [ "$#" -eq 0 ]; then
  echo "用法：wait-ready.sh SECONDS service..." >&2
  exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
deadline=$(( $(date +%s) + timeout ))

compose() {
  sh "$script_dir/compose.sh" "$@"
}

while :; do
  now=$(date +%s)
  if [ "$now" -ge "$deadline" ]; then
    echo "等待 healthy 超时 ${timeout}s" >&2
    compose ps -a || true
    compose logs --tail 80 || true
    exit 1
  fi

  # 用 | 分隔。Health 为空时不能用空格分列，否则退出码会错位。
  ps=$(compose ps -a --format '{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}')
  ready=1
  for svc in "$@"; do
    line=$(printf '%s\n' "$ps" | awk -F'|' -v s="$svc" '$1 == s { print; exit }')
    health=$(printf '%s\n' "$line" | awk -F'|' '{ print $3 }')
    if [ "$health" != "healthy" ]; then
      ready=0
      break
    fi
  done

  init_line=$(printf '%s\n' "$ps" | awk -F'|' '$1 == "kafka-init" { print; exit }')
  init_state=$(printf '%s\n' "$init_line" | awk -F'|' '{ print $2 }')
  init_code=$(printf '%s\n' "$init_line" | awk -F'|' '{ print $4 }')
  if [ "$init_state" = "exited" ] && [ "$init_code" != "0" ]; then
    echo "kafka-init 失败，退出码 ${init_code:-<empty>}，原始行：${init_line}" >&2
    compose logs kafka-init || true
    exit 1
  fi
  if [ "$init_state" != "exited" ] || [ "$init_code" != "0" ]; then
    ready=0
  fi

  if [ "$ready" -eq 1 ]; then
    exit 0
  fi
  sleep 2
done
