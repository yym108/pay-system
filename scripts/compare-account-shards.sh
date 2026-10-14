#!/bin/sh
# 比较 pay_account_0 与 pay_account_1 的表、列、索引和 CHECK。不比较 AUTO_INCREMENT 当前值。
# 用法：ENV_FILE=deploy/.env.example sh scripts/compare-account-shards.sh
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

# shellcheck disable=SC1091
. "$root/deploy/load-env.sh"
load_env_file "$env_file"

sql=$(cat <<'SQL'
SELECT CONCAT('table ', IFNULL(a.TABLE_NAME, b.TABLE_NAME)) AS diff
  FROM information_schema.TABLES a
  LEFT JOIN information_schema.TABLES b
    ON b.TABLE_SCHEMA = 'pay_account_1' AND b.TABLE_NAME = a.TABLE_NAME
   AND b.TABLE_COLLATION <=> a.TABLE_COLLATION
   AND b.TABLE_COMMENT <=> a.TABLE_COMMENT
 WHERE a.TABLE_SCHEMA = 'pay_account_0' AND b.TABLE_NAME IS NULL
UNION ALL
SELECT CONCAT('column ', IFNULL(a.TABLE_NAME, b.TABLE_NAME), '.', IFNULL(a.COLUMN_NAME, b.COLUMN_NAME))
  FROM information_schema.COLUMNS a
  LEFT JOIN information_schema.COLUMNS b
    ON b.TABLE_SCHEMA = 'pay_account_1' AND b.TABLE_NAME = a.TABLE_NAME AND b.COLUMN_NAME = a.COLUMN_NAME
   AND b.COLUMN_TYPE <=> a.COLUMN_TYPE AND b.IS_NULLABLE <=> a.IS_NULLABLE
   AND b.COLUMN_DEFAULT <=> a.COLUMN_DEFAULT AND b.EXTRA <=> a.EXTRA
   AND b.COLUMN_COMMENT <=> a.COLUMN_COMMENT AND b.ORDINAL_POSITION <=> a.ORDINAL_POSITION
 WHERE a.TABLE_SCHEMA = 'pay_account_0' AND b.COLUMN_NAME IS NULL
UNION ALL
SELECT CONCAT('index ', IFNULL(a.TABLE_NAME, b.TABLE_NAME), '.', IFNULL(a.INDEX_NAME, b.INDEX_NAME))
  FROM information_schema.STATISTICS a
  LEFT JOIN information_schema.STATISTICS b
    ON b.TABLE_SCHEMA = 'pay_account_1' AND b.TABLE_NAME = a.TABLE_NAME
   AND b.INDEX_NAME = a.INDEX_NAME AND b.SEQ_IN_INDEX = a.SEQ_IN_INDEX
   AND b.COLUMN_NAME <=> a.COLUMN_NAME AND b.NON_UNIQUE <=> a.NON_UNIQUE
 WHERE a.TABLE_SCHEMA = 'pay_account_0' AND b.INDEX_NAME IS NULL
UNION ALL
SELECT CONCAT('check ', a.CONSTRAINT_NAME)
  FROM information_schema.CHECK_CONSTRAINTS a
  LEFT JOIN information_schema.CHECK_CONSTRAINTS b
    ON b.CONSTRAINT_SCHEMA = 'pay_account_1' AND b.CONSTRAINT_NAME = a.CONSTRAINT_NAME
   AND b.CHECK_CLAUSE <=> a.CHECK_CLAUSE
 WHERE a.CONSTRAINT_SCHEMA = 'pay_account_0' AND b.CONSTRAINT_NAME IS NULL;
SQL
)

out=$(sh "$root/deploy/compose.sh" exec -T mysql \
  mysql --protocol=TCP -h127.0.0.1 -uroot -p"$MYSQL_ROOT_PASSWORD" --batch --skip-column-names \
  -e "$sql")
if [ -n "$out" ]; then
  echo "账户分片结构不一致：" >&2
  printf '%s\n' "$out" >&2
  exit 1
fi
echo "pay_account_0 与 pay_account_1 结构一致"
