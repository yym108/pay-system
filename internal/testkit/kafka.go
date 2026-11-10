//go:build integration

package testkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"regexp"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaImage = "apache/kafka:4.3.1"

const kafkaScriptPath = "/tmp/testkit-kafka-start.sh"

// 镜像 configureDefaults 里的示例 cluster id，不是凭据。
const kafkaClusterID = "5L6g3nShT-eMCtK--X86sw"

// 钉死在 127.0.0.1。OrbStack 在 Stop/Start 后会改随机宿主机端口，Host() 又常是 localhost，会拨到 ::1。
const kafkaAdvHost = "127.0.0.1"

var kafkaPortRE = regexp.MustCompile(`^[0-9]{1,5}$`)

// Kafka 是一台 KRaft 单节点，没有 ZooKeeper。Broker 是宿主机上的 127.0.0.1:port。
// Stop 之后再 Start，Broker 字符串不变。
type Kafka struct {
	Broker string
	port   string
	ctr    testcontainers.Container
}

// StartKafka 启动 apache/kafka:4.3.1。不要在每个 MySQL 测试里调用。
// 返回前会确认 broker 能回答元数据。不生产、不消费消息。不自动创建 topic。
func StartKafka(ctx context.Context) (*Kafka, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	port, err := freeKafkaPort(ctx)
	if err != nil {
		return nil, errors.New("testkit: kafka port: " + redactString(err.Error()))
	}
	if !kafkaPortRE.MatchString(port) {
		return nil, errors.New("testkit: kafka endpoint rejected")
	}
	broker := net.JoinHostPort(kafkaAdvHost, port)
	ctr, err := testcontainers.Run(ctx, kafkaImage,
		testcontainers.WithLogger(log.NewNoopLogger()),
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("9092/tcp"): []network.PortBinding{{
					HostIP:   netip.MustParseAddr(kafkaAdvHost),
					HostPort: port,
				}},
			}
		}),
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
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                        "false",
		}),
		testcontainers.WithEntrypoint("bash"),
		testcontainers.WithCmd("-c", "while [ ! -f "+kafkaScriptPath+" ]; do sleep 0.1; done; exec bash "+kafkaScriptPath),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, ctr testcontainers.Container) error {
					return writeKafkaScript(ctx, ctr, port)
				},
			},
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
	if err = sameKafkaPort(ctx, ctr, port); err != nil {
		terminate(ctx, ctr)
		return nil, err
	}
	if err = waitKafkaReady(ctx, broker); err != nil {
		terminate(ctx, ctr)
		return nil, err
	}
	return &Kafka{Broker: broker, port: port, ctr: ctr}, nil
}

// Stop 停掉容器但保留它。之后 Start 仍使用原来的 Broker。
func (k *Kafka) Stop(ctx context.Context) error {
	if k == nil || k.ctr == nil {
		return errors.New("testkit: kafka missing")
	}
	timeout := 20 * time.Second
	if err := k.ctr.Stop(ctx, &timeout); err != nil {
		return errors.New("testkit: kafka stop: " + redactString(err.Error()))
	}
	return nil
}

// Start 拉起同一个容器。宿主机端口变了就返回错误，不改 Broker。
// 旧日志里已经有 “Kafka Server started”，这里改成探活，不用那一行判断。
func (k *Kafka) Start(ctx context.Context) error {
	if k == nil || k.ctr == nil {
		return errors.New("testkit: kafka missing")
	}
	if !k.ctr.IsRunning() {
		if err := k.ctr.Start(ctx); err != nil {
			return errors.New("testkit: kafka start: " + redactString(err.Error()))
		}
	}
	if err := sameKafkaPort(ctx, k.ctr, k.port); err != nil {
		return err
	}
	return waitKafkaReady(ctx, k.Broker)
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

func freeKafkaPort(ctx context.Context) (string, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", kafkaAdvHost+":0")
	if err != nil {
		return "", err
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if closeErr := ln.Close(); err == nil {
		err = closeErr
	}
	return port, err
}

func sameKafkaPort(ctx context.Context, ctr testcontainers.Container, port string) error {
	mapped, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return errors.New("testkit: kafka port: " + redactString(err.Error()))
	}
	if mapped.Port() != port {
		return errors.New("testkit: kafka port changed")
	}
	return nil
}

func writeKafkaScript(ctx context.Context, ctr testcontainers.Container, port string) error {
	if err := wait.ForMappedPort("9092/tcp").WaitUntilReady(ctx, ctr); err != nil {
		return err
	}
	mapped, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return err
	}
	if mapped.Port() != port || !kafkaPortRE.MatchString(port) {
		return errors.New("testkit: kafka endpoint rejected")
	}
	// 第一次启动就把地址写进脚本。重启时入口可能先执行已有脚本，钩子再改也是同一行。
	script := fmt.Sprintf(`#!/usr/bin/env bash
set -eu
export KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://%s:%s,BROKER://127.0.0.1:9094'
exec /etc/kafka/docker/run
`, kafkaAdvHost, port)
	return ctr.CopyToContainer(ctx, []byte(script), kafkaScriptPath, 0o755)
}

func waitKafkaReady(ctx context.Context, broker string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return errors.New("testkit: kafka client")
	}
	defer cl.Close()
	for {
		pingCtx, pingCancel := context.WithTimeout(waitCtx, time.Second)
		err = cl.Ping(pingCtx)
		pingCancel()
		if err == nil {
			return nil
		}
		if waitCtx.Err() != nil {
			return errors.New("testkit: kafka not ready")
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return errors.New("testkit: kafka not ready")
		case <-timer.C:
		}
	}
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
