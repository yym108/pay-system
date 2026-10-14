// Package testkit 提供集成测试用的 MySQL、Kafka 和夹具。
//
// 业务服务不要 import 本包。MySQL 默认用 testcontainers 启动 mysql:8.4.11；
// TEST_MYSQL_DSN 非空时复用该实例，不再启动容器。
// 每个测试库名带随机后缀，关闭时只删除该库。
// Kafka 使用已锁定的 apache/kafka:4.3.1，只有调用 StartKafka 才会启动。
package testkit
