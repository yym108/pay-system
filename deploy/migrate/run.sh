#!/bin/sh
# 在 migrate/migrate:v4.20.1 容器内执行。数据库和账号由 bootstrap 事先建好。
set -eu

action=${1:-up}
pass=${MYSQL_MIGRATOR_PASSWORD:?MYSQL_MIGRATOR_PASSWORD is empty}
host=${MYSQL_HOST:-mysql}

case "$pass" in
  *[@:/?\#\'\"\ ]*)
    echo "migrator password contains an unsafe character" >&2
    exit 1
    ;;
esac

run_one() {
  db=$1
  path=$2
  url="mysql://migrator:${pass}@tcp(${host}:3306)/${db}?multiStatements=true&charset=utf8mb4"
  if [ "$action" = "down" ]; then
    /migrate -path="$path" -database "$url" down -all
  elif [ "$action" = "up" ]; then
    /migrate -path="$path" -database "$url" up
  else
    echo "usage: run.sh up|down" >&2
    exit 1
  fi
}

run_one pay_account_0 /migrations/account
run_one pay_account_1 /migrations/account
run_one pay_payment /migrations/payment
run_one pay_recon /migrations/recon
run_one pay_channel_mock /migrations/channel_mock
