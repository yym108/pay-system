#!/bin/sh
# make migrate 的入口。先在宿主机重建账号，再用官方 migrate 镜像执行 DDL。
# 不启动 profile apps 里的应用容器。mysql 必须已经由 make up 拉起。
set -eu

action=${1:-up}
if [ "$action" != "up" ] && [ "$action" != "down" ]; then
  echo "usage: deploy/migrate.sh up|down" >&2
  exit 1
fi

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

# shellcheck disable=SC1091
. "$root/deploy/load-env.sh"
load_env_file "$env_file"

if ! sh "$root/deploy/compose.sh" exec -T mysql \
  mysqladmin ping -h 127.0.0.1 -uroot -p"$MYSQL_ROOT_PASSWORD" --silent
then
  echo "mysql 未就绪。请先 make up，并用同一个 ENV_FILE。" >&2
  exit 1
fi

export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306
(cd "$root" && go run ./cmd/bootstrap)

MIGRATE_ACTION="$action" sh "$root/deploy/compose.sh" --profile apps run --rm --no-deps \
  -e "MIGRATE_ACTION=$action" \
  migrate "$action"
