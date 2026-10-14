//go:build integration

package testkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/wait"
)

const kafkaImage = "apache/kafka:4.3.1"

const kafkaScriptPath = "/tmp/testkit-kafka-start.sh"

// 镜像 configureDefaults 里的示例 cluster id，不是凭据。
const kafkaClusterID = "5L6g3nShT-eMCtK--X86sw"

var (
	kafkaHostRE = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	kafkaPortRE = regexp.MustCompile(`^[0-9]{1,5}$`)
)

// Kafka 是一台 KRaft 单节点，没有 ZooKeeper。Broker 是宿主机上的 host:port。
type Kafka struct {
	Broker string
	ctr    testcontainers.Container
}

// StartKafka 启动 apache/kafka:4.3.1。不要在每个 MySQL 测试里调用。
// 返回前会确认 broker 端口能接受 TCP 连接。不生产、不消费消息。
func StartKafka(ctx context.Context) (*Kafka, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctr, err := testcontainers.Run(ctx, kafkaImage,
		testcontainers.WithLogger(log.NewNoopLogger()),
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID":                                             kafkaClusterID,
			"KAFKA_NODE_ID":                                          "1",
			"KAFKA_PROCESS_ROLES":                                    "broker,controller",
			"KAFKA_LISTENERS":                                        "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":                   "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":                       "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                        "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                         "1@127.0.0.1:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":                 "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":                    "1",
			"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR": "1",
			"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":                 "0",
		}),
		testcontainers.WithEntrypoint("bash"),
		testcontainers.WithCmd("-c", "while [ ! -f "+kafkaScriptPath+" ]; do sleep 0.1; done; exec bash "+kafkaScriptPath),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{startKafkaHook},
		}),
	)
	if err != nil {
		tail := ""
		if ctr != nil {
			tail = containerTail(ctx, ctr)
			terminate(ctx, ctr)
		}
		if tail != "" {
			return nil, errors.New("testkit: start kafka: " + redactString(err.Error()) + " tail: " + tail)
		}
		return nil, errors.New("testkit: start kafka: " + redactString(err.Error()))
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		terminate(ctx, ctr)
		return nil, errors.New("testkit: kafka host: " + redactString(err.Error()))
	}
	mapped, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		terminate(ctx, ctr)
		return nil, errors.New("testkit: kafka port: " + redactString(err.Error()))
	}
	broker := net.JoinHostPort(host, mapped.Port())
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	conn, dialErr := (&net.Dialer{}).DialContext(dialCtx, "tcp", broker)
	cancel()
	if dialErr != nil {
		terminate(ctx, ctr)
		return nil, errors.New("testkit: kafka dial: " + redactString(dialErr.Error()))
	}
	_ = conn.Close()
	return &Kafka{Broker: broker, ctr: ctr}, nil
}

// Close 停掉本助手启动的 Kafka 容器。
func (k *Kafka) Close(ctx context.Context) error {
	if k == nil || k.ctr == nil {
		return nil
	}
	terminate(ctx, k.ctr)
	k.ctr = nil
	return nil
}

func startKafkaHook(ctx context.Context, ctr testcontainers.Container) error {
	if err := wait.ForMappedPort("9092/tcp").WaitUntilReady(ctx, ctr); err != nil {
		return err
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		return err
	}
	mapped, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return err
	}
	port := mapped.Port()
	if !kafkaHostRE.MatchString(host) || !kafkaPortRE.MatchString(port) {
		return errors.New("testkit: kafka endpoint rejected")
	}
	script := fmt.Sprintf(`#!/usr/bin/env bash
set -eu
export KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://%s:%s,BROKER://127.0.0.1:9094'
exec /etc/kafka/docker/run
`, host, port)
	if err := ctr.CopyToContainer(ctx, []byte(script), kafkaScriptPath, 0o755); err != nil {
		return err
	}
	return wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute).WaitUntilReady(ctx, ctr)
}

func containerTail(ctx context.Context, ctr testcontainers.Container) string {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	rc, err := ctr.Logs(readCtx)
	if err != nil {
		return ""
	}
	defer func() { _ = rc.Close() }()
	buf, err := io.ReadAll(io.LimitReader(rc, 64<<10))
	if err != nil && len(buf) == 0 {
		return ""
	}
	text := string(buf)
	if len(text) > 1500 {
		text = text[len(text)-1500:]
	}
	text = strings.ReplaceAll(text, "\n", " | ")
	return redactString(text)
}
