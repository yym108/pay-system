#!/bin/sh
# 按行导出 KEY=VALUE。不能用 shell 的 . 来读，DSN 里的括号会被当成子shell。
# 用法：. deploy/load-env.sh && load_env_file "$env_file"
load_env_file() {
  file=$1
  if [ ! -f "$file" ]; then
    echo "缺少环境文件 $file" >&2
    return 1
  fi
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      ''|\#*) continue ;;
    esac
    key=${line%%=*}
    value=${line#*=}
    case "$key" in
      ''|*[!A-Za-z0-9_]*)
        echo "环境文件含非法变量名" >&2
        return 1
        ;;
    esac
    export "$key=$value"
  done < "$file"
}
