#!/bin/bash
# 创建 docs/05-api.md §11.1 的 topic。可重复执行。
set -eu

bootstrap="${BOOTSTRAP:-kafka:9092}"
topics="pay.payment.succeeded pay.refund.succeeded pay.refund.failed pay.transfer.succeeded pay.transfer.failed pay.dlq"

for topic in $topics; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap" --describe
