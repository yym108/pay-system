#!/bin/sh
# make seed / make seed-load 的入口。在宿主机执行，把 DSN 里的 mysql 主机改成 127.0.0.1。
# compose 里的 seed 容器不走这个替换，容器网络内主机名就是 mysql。
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

# 调用方已设的 SEED_LOAD（如 make seed-load 设的 1）优先于环境文件。其他变量仍以环境文件为准。
caller_seed_load_set=${SEED_LOAD+x}
caller_seed_load=${SEED_LOAD-}

# shellcheck disable=SC1091
. "$root/deploy/load-env.sh"
load_env_file "$env_file"

if [ -n "$caller_seed_load_set" ]; then
  SEED_LOAD=$caller_seed_load
  export SEED_LOAD
fi

if ! sh "$root/deploy/compose.sh" exec -T mysql \
  mysqladmin ping -h 127.0.0.1 -uroot -p"$MYSQL_ROOT_PASSWORD" --silent
then
  echo "mysql 未就绪。请先 make up && make migrate，并用同一个 ENV_FILE。" >&2
  exit 1
fi

rewrite() {
  printf '%s' "$1" | sed 's/@tcp(mysql:/@tcp(127.0.0.1:/'
}

ACCOUNT_DB_DSN_0=$(rewrite "$ACCOUNT_DB_DSN_0")
ACCOUNT_DB_DSN_1=$(rewrite "$ACCOUNT_DB_DSN_1")
MYSQL_DSN=$(rewrite "$MYSQL_DSN")
export ACCOUNT_DB_DSN_0 ACCOUNT_DB_DSN_1 MYSQL_DSN

cd "$root"
go run ./cmd/seed
