#!/bin/sh
# 统一的 compose 入口。项目目录是仓库根，避免 Compose 把 deploy/ 当成项目目录后自动读取 deploy/.env。
set -eu

script_dir=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
root=$(CDPATH= cd -- "$script_dir/.." && pwd)
env_file=${ENV_FILE:-"$root/deploy/.env"}
case "$env_file" in
  /*) ;;
  *) env_file="$root/$env_file" ;;
esac

if [ ! -f "$env_file" ]; then
  echo "缺少环境文件 $env_file" >&2
  exit 1
fi

exec docker compose \
  --project-name pay-system \
  --project-directory "$root" \
  --env-file "$env_file" \
  -f "$root/deploy/docker-compose.yml" \
  "$@"
