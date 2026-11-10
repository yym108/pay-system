//go:build integration

package mq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/yym108/pay-system/pkg/server"
)

const (
	childEnv     = "MQ03_CHILD"
	childKill    = "kill"
	childReplay  = "replay"
	kafkaImage   = "apache/kafka:4.3.1"
	kafkaScript  = "/tmp/mq03-kafka-start.sh"
	kafkaCluster = "5L6g3nShT-eMCtK--X86sw"
)

var (
	brokerAddr  string
	sharedKafka *testKafka

	kafkaHostRE = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	kafkaPortRE = regexp.MustCompile(`^[0-9]{1,5}$`)

	// 钉死在 127.0.0.1。OrbStack 在 Stop/Start 后会改随机宿主机端口，Host() 又是 localhost，会拨到 ::1。
	kafkaAdvHost = "127.0.0.1"
	kafkaAdvPort string
)

type testKafka struct {
	Broker string
	ctr    testcontainers.Container
}

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) != "" {
		os.Exit(m.Run())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	box, err := startKafka(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "kafka: %v\n", err)
		os.Exit(1)
	}
	brokerAddr = box.Broker
	sharedKafka = box
	code := m.Run()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	_ = box.Close(shutCtx)
	shutCancel()
	os.Exit(code)
}

func TestMQ03Accept1RetryThenCommit(t *testing.T) {
	// MQ-03 验收 1：处理函数前 3 次返回可重试错误、第 4 次成功，只成功处理一次，offset 已提交。
	if os.Getenv(childEnv) != "" {
		t.Skip()
	}
	topic := unique("mq03-a1")
	group := unique("mq03-a1")
	createTopic(t, topic, 1)
	const eventID = "evt-a1"
	var calls atomic.Int32
	got := make(chan Record, 1)
	c := startGroup(t, ConsumerConfig{
		Brokers: []string{brokerAddr},
		Group:   group,
		Topics:  []string{topic},
		Sleeper: fastSleeper,
		Handler: func(_ context.Context, rec Record) error {
			n := calls.Add(1)
			if n < 4 {
				return errors.New("database unavailable")
			}
			got <- rec
			return nil
		},
	})
	produce(t, topic, eventID, []byte("body-a1"))
	var rec Record
	select {
	case rec = <-got:
	case <-time.After(30 * time.Second):
		t.Fatal("handler did not succeed")
	}
	waitUntil(t, 15*time.Second, func() (bool, error) {
		off, err := groupOffset(group, topic, rec.Partition)
		if err != nil {
			return false, err
		}
		return off == rec.Offset+1, nil
	})
	if calls.Load() != 4 {
		t.Fatalf("handler calls = %d, want 4", calls.Load())
	}
	stopGroup(t, c)

	next := unique("mq03-a1-next")
	seen := make(chan string, 4)
	startGroup(t, ConsumerConfig{
		Brokers: []string{brokerAddr},
		Group:   group,
		Topics:  []string{topic},
		Sleeper: fastSleeper,
		Handler: func(_ context.Context, rec Record) error {
			seen <- rec.EventID
			return nil
		},
	})
	produce(t, topic, next, []byte("body-a1-next"))
	waitUntil(t, 30*time.Second, func() (bool, error) {
		select {
		case id := <-seen:
			if id == eventID {
				return false, errors.New("committed record was delivered again")
			}
			return id == next, nil
		default:
			return false, nil
		}
	})
	select {
	case id := <-seen:
		if id == eventID {
			t.Fatal("committed record was delivered again")
		}
	default:
	}
	if calls.Load() != 4 {
		t.Fatalf("first handler calls = %d, want 4", calls.Load())
	}
}

func TestMQ03Accept2Readyz(t *testing.T) {
	// MQ-03 验收 2：Kafka 可用时 /readyz 与 /healthz 为 200；停掉 Kafka 后 /readyz 为 503、/healthz 仍为 200；恢复后 /readyz 回到 200。
	if os.Getenv(childEnv) != "" {
		t.Skip()
	}
	ln := listenLocal(t)
	ctx, cancel := context.WithCancel(context.Background())
	admin := server.New(server.Config{
		Listener:     ln,
		DrainTimeout: time.Second,
		Signals:      make(chan os.Signal, 1),
	})
	admin.RequireDB(func(context.Context) error { return nil }, 0)
	if err := RegisterProducer(admin, brokerAddr); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- admin.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
		}
	})
	base := "http://" + ln.Addr().String()
	waitHTTP(t, 15*time.Second, base+"/healthz", http.StatusOK, "ok")
	waitHTTP(t, 15*time.Second, base+"/readyz", http.StatusOK, "ok")

	stopTimeout := 20 * time.Second
	if err := sharedKafka.ctr.Stop(context.Background(), &stopTimeout); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		restartCtx, restartCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer restartCancel()
		if err := ensureKafka(restartCtx); err != nil {
			t.Errorf("restart kafka: %v", err)
		}
	})
	waitHTTP(t, 20*time.Second, base+"/readyz", http.StatusServiceUnavailable, "unavailable")
	status, body := getHTTP(t, base+"/healthz")
	if status != http.StatusOK || strings.TrimSpace(body) != "ok" {
		t.Fatalf("healthz = %d %q", status, body)
	}
	_, readyBody := getHTTP(t, base+"/readyz")
	if strings.TrimSpace(readyBody) != "unavailable" {
		t.Fatalf("readyz body = %q", readyBody)
	}
	if strings.Contains(readyBody, brokerAddr) || strings.Contains(readyBody, "9092") {
		t.Fatalf("readyz body contains an address: %q", readyBody)
	}

	if err := ensureKafka(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitHTTP(t, 90*time.Second, base+"/readyz", http.StatusOK, "ok")
	waitHTTP(t, 5*time.Second, base+"/healthz", http.StatusOK, "ok")
}

func TestMQ03Accept3DLQ(t *testing.T) {
	// MQ-03 验收 3：不可重试错误进入 pay.dlq，后续消息继续消费。
	if os.Getenv(childEnv) != "" {
		t.Skip()
	}
	createTopic(t, DLQTopic, 3)
	topic := unique("mq03-a3")
	group := unique("mq03-a3")
	createTopic(t, topic, 1)
	const cause = "missing field"
	var mu sync.Mutex
	var order []string
	var bad Record
	done := make(chan struct{})
	c := startGroup(t, ConsumerConfig{
		Brokers: []string{brokerAddr},
		Group:   group,
		Topics:  []string{topic},
		Sleeper: fastSleeper,
		Handler: func(_ context.Context, rec Record) error {
			mu.Lock()
			order = append(order, string(rec.Value))
			n := len(order)
			mu.Unlock()
			if string(rec.Value) == "bad" {
				bad = rec
				return Permanent(errors.New(cause))
			}
			if n >= 2 {
				close(done)
			}
			return nil
		},
	})
	produce(t, topic, "evt-bad", []byte("bad"))
	produce(t, topic, "evt-good", []byte("good"))
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("second record was blocked")
	}
	mu.Lock()
	gotOrder := append([]string(nil), order...)
	mu.Unlock()
	if len(gotOrder) != 2 || gotOrder[0] != "bad" || gotOrder[1] != "good" {
		t.Fatalf("order = %q", gotOrder)
	}
	if c.DLQTotal(topic) != 1 {
		t.Fatalf("dlq total = %d, want 1", c.DLQTotal(topic))
	}
	dlq := readDLQ(t, topic, bad.Offset)
	if got := headerValue(dlq, HeaderOrigTopic); got != topic {
		t.Fatalf("x-orig-topic = %q", got)
	}
	if got := headerValue(dlq, HeaderOrigPartition); got != strconv.FormatInt(int64(bad.Partition), 10) {
		t.Fatalf("x-orig-partition = %q", got)
	}
	if got := headerValue(dlq, HeaderOrigOffset); got != strconv.FormatInt(bad.Offset, 10) {
		t.Fatalf("x-orig-offset = %q", got)
	}
	if got := headerValue(dlq, HeaderError); got != cause {
		t.Fatalf("x-error = %q", got)
	}
	stopGroup(t, c)
}

func TestMQ03Accept4KillBeforeCommit(t *testing.T) {
	// MQ-03 验收 4：处理成功、提交前 kill 进程，重启后重新投递。业务幂等由 MQ-04 吸收，本框架不丢弃重复。Kill 之后不能返回。
	if os.Getenv(childEnv) != "" {
		t.Skip()
	}
	topic := unique("mq03-a4")
	group := unique("mq03-a4")
	createTopic(t, topic, 1)
	const eventID = "evt-a4"
	produce(t, topic, eventID, []byte("body-a4"))

	// os.Args[0] 是当前测试二进制，参数是本测试写死的正则。
	killCtx, killCancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer killCancel()
	kill := exec.CommandContext(killCtx, os.Args[0], "-test.run", "^TestMQ03Accept4Child$", "-test.count=1", "-test.timeout=60s") //nolint:gosec // G204: 见上一行。
	kill.Env = append(os.Environ(),
		childEnv+"="+childKill,
		"MQ03_BROKERS="+brokerAddr,
		"MQ03_TOPIC="+topic,
		"MQ03_GROUP="+group,
	)
	out, err := kill.CombinedOutput()
	if err == nil {
		t.Fatalf("child exited 0: %s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.Error() != "signal: killed" {
		t.Fatalf("child err = %v output = %s", err, out)
	}
	off, ferr := groupOffset(group, topic, 0)
	if ferr != nil {
		t.Fatal(ferr)
	}
	if off >= 0 {
		t.Fatalf("offset committed after kill: %d", off)
	}

	replayCtx, replayCancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer replayCancel()
	replay := exec.CommandContext(replayCtx, os.Args[0], "-test.run", "^TestMQ03Accept4Child$", "-test.count=1", "-test.timeout=60s") //nolint:gosec // G204: 同 kill，二进制是当前测试，参数固定。
	replay.Env = append(os.Environ(),
		childEnv+"="+childReplay,
		"MQ03_BROKERS="+brokerAddr,
		"MQ03_TOPIC="+topic,
		"MQ03_GROUP="+group,
	)
	replayOut, rerr := replay.CombinedOutput()
	if rerr != nil {
		t.Fatalf("replay: %v output = %s", rerr, replayOut)
	}
	if !strings.Contains(string(replayOut), eventID) {
		t.Fatalf("replay did not deliver %s: %s", eventID, replayOut)
	}
}

func TestMQ03Accept4Child(t *testing.T) {
	mode := os.Getenv(childEnv)
	if mode == "" {
		t.Skip()
	}
	topic := os.Getenv("MQ03_TOPIC")
	group := os.Getenv("MQ03_GROUP")
	brokers := os.Getenv("MQ03_BROKERS")
	seen := make(chan struct{}, 1)
	c, err := OpenConsumer(ConsumerConfig{
		Brokers:        strings.Split(brokers, ","),
		Group:          group,
		Topics:         []string{topic},
		Sleeper:        fastSleeper,
		SessionTimeout: 6 * time.Second,
		Handler: func(_ context.Context, rec Record) error {
			if mode == childReplay {
				if _, err := fmt.Fprintln(os.Stdout, rec.EventID); err != nil {
					return err
				}
				select {
				case seen <- struct{}{}:
				default:
				}
			}
			return nil
		},
		BeforeCommit: func(context.Context, Record) {
			if mode == childKill {
				_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
				select {}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Serve() }()
	if mode == childReplay {
		select {
		case <-seen:
		case <-time.After(45 * time.Second):
			t.Fatal("replay did not see the record")
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Stop(stopCtx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
			t.Fatal("serve did not return")
		}
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(45 * time.Second):
	}
}

func TestFinishBeforeRevokeWaitsForCommit(t *testing.T) {
	// MQ-03：撤销分区前等待在途处理并提交。
	if os.Getenv(childEnv) != "" {
		t.Skip()
	}
	topic := unique("mq03-revoke")
	group := unique("mq03-revoke")
	createTopic(t, topic, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var finished atomic.Bool
	c := startGroup(t, ConsumerConfig{
		Brokers: []string{brokerAddr},
		Group:   group,
		Topics:  []string{topic},
		Sleeper: fastSleeper,
		Handler: func(context.Context, Record) error {
			once.Do(func() { close(entered) })
			<-release
			finished.Store(true)
			return nil
		},
	})
	produce(t, topic, "evt-revoke", []byte("revoke"))
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("handler did not start")
	}
	revoked := make(chan struct{})
	go func() {
		c.finishBeforeRevoke(context.Background(), map[string][]int32{topic: {0}})
		close(revoked)
	}()
	waitUntil(t, 10*time.Second, func() (bool, error) {
		c.mu.Lock()
		g := c.gates[partKey{topic, 0}]
		c.mu.Unlock()
		if g == nil {
			return false, nil
		}
		g.mu.Lock()
		n := len(g.waiters)
		g.mu.Unlock()
		return n > 0, nil
	})
	if finished.Load() {
		t.Fatal("handler finished before revoke waited")
	}
	off, err := groupOffset(group, topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if off >= 0 {
		t.Fatalf("offset committed while handler blocked: %d", off)
	}
	close(release)
	select {
	case <-revoked:
	case <-time.After(10 * time.Second):
		t.Fatal("finishBeforeRevoke did not return")
	}
	if !finished.Load() {
		t.Fatal("handler was not finished when revoke returned")
	}
	waitUntil(t, 10*time.Second, func() (bool, error) {
		got, ferr := groupOffset(group, topic, 0)
		if ferr != nil {
			return false, ferr
		}
		return got == 1, nil
	})
	stopGroup(t, c)
}

func TestConsumeLag(t *testing.T) {
	// MQ-03：未消费时延迟大于 0，提交后为 0。
	if os.Getenv(childEnv) != "" {
		t.Skip()
	}
	topic := unique("mq03-lag")
	group := unique("mq03-lag")
	createTopic(t, topic, 1)
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	c := startGroup(t, ConsumerConfig{
		Brokers: []string{brokerAddr},
		Group:   group,
		Topics:  []string{topic},
		Sleeper: fastSleeper,
		Handler: func(context.Context, Record) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		},
	})
	produce(t, topic, "evt-lag", []byte("lag"))
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("handler did not start")
	}
	waitUntil(t, 10*time.Second, func() (bool, error) {
		return c.ConsumeLag(topic, 0) > 0, nil
	})
	close(release)
	waitUntil(t, 15*time.Second, func() (bool, error) {
		return c.ConsumeLag(topic, 0) == 0, nil
	})
	stopGroup(t, c)
}

func fastSleeper(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

type runningGroup struct {
	c   *Consumer
	err chan error
}

func startGroup(t *testing.T, cfg ConsumerConfig) *Consumer {
	t.Helper()
	c, err := OpenConsumer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rg := &runningGroup{c: c, err: make(chan error, 1)}
	go func() { rg.err <- c.Serve() }()
	t.Cleanup(func() { stopRunning(t, rg) })
	runningMu.Lock()
	running[c] = rg
	runningMu.Unlock()
	return c
}

var (
	runningMu sync.Mutex
	running   = map[*Consumer]*runningGroup{}
)

func stopGroup(t *testing.T, c *Consumer) {
	t.Helper()
	runningMu.Lock()
	rg := running[c]
	delete(running, c)
	runningMu.Unlock()
	if rg == nil {
		return
	}
	stopRunning(t, rg)
}

func stopRunning(t *testing.T, rg *runningGroup) {
	t.Helper()
	if rg == nil || rg.c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = rg.c.Stop(ctx)
	select {
	case <-rg.err:
	case <-time.After(15 * time.Second):
		t.Errorf("serve did not return")
	}
	rg.c = nil
}

func produce(t *testing.T, topic, eventID string, body []byte) {
	t.Helper()
	p, err := OpenProducer([]string{brokerAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = p.Produce(ctx, Message{
		Topic:       topic,
		Key:         []byte(eventID),
		Value:       body,
		EventID:     eventID,
		EventType:   "mq.test",
		Traceparent: "",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func createTopic(t *testing.T, name string, partitions int32) {
	t.Helper()
	cl := dialAdmin(t)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Version = 7
	req.TimeoutMillis = 15000
	req.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             name,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range resp.Topics {
		if topic.ErrorCode == 0 {
			continue
		}
		if errors.Is(kerr.ErrorForCode(topic.ErrorCode), kerr.TopicAlreadyExists) {
			continue
		}
		t.Fatalf("create topic %s: code %d", name, topic.ErrorCode)
	}
}

func groupOffset(group, topic string, partition int32) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Version = 7
	req.Group = group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{
		Topic:      topic,
		Partitions: []int32{partition},
	}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, err
	}
	if resp.ErrorCode != 0 {
		return 0, kerr.ErrorForCode(resp.ErrorCode)
	}
	for _, topicResp := range resp.Topics {
		if topicResp.Topic != topic {
			continue
		}
		for _, part := range topicResp.Partitions {
			if part.Partition != partition {
				continue
			}
			if part.ErrorCode != 0 {
				return 0, kerr.ErrorForCode(part.ErrorCode)
			}
			return part.Offset, nil
		}
	}
	return -1, errors.New("offset missing")
}

func readDLQ(t *testing.T, origTopic string, offset int64) *kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumeTopics(DLQTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wantOff := strconv.FormatInt(offset, 10)
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatal("dlq record not found")
		}
		var found *kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) {
			if headerValue(rec, HeaderOrigTopic) == origTopic && headerValue(rec, HeaderOrigOffset) == wantOff {
				found = rec
			}
		})
		if found != nil {
			return found
		}
	}
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func dialAdmin(t *testing.T) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func waitUntil(t *testing.T, timeout time.Duration, fn func() (bool, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var last error
	for {
		ok, err := fn()
		if ok && err == nil {
			return
		}
		if err != nil {
			last = err
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout: %v", last)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("timeout: %v", last)
		case <-timer.C:
		}
	}
}

func waitHTTP(t *testing.T, timeout time.Duration, url string, status int, body string) {
	t.Helper()
	waitUntil(t, timeout, func() (bool, error) {
		gotStatus, gotBody := getHTTP(t, url)
		if gotStatus != status {
			return false, nil
		}
		if strings.TrimSpace(gotBody) != body {
			return false, fmt.Errorf("body %q", gotBody)
		}
		return true, nil
	})
}

func getHTTP(t *testing.T, url string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer func() { _ = resp.Body.Close() }()
	buf, err := io.ReadAll(io.LimitReader(resp.Body, 256))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(buf)
}

var topicSeq atomic.Int64

func unique(prefix string) string {
	return prefix + "-" + strconv.FormatInt(topicSeq.Add(1), 10)
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func ensureKafka(ctx context.Context) error {
	if sharedKafka == nil || sharedKafka.ctr == nil {
		return errors.New("kafka container missing")
	}
	if !sharedKafka.ctr.IsRunning() {
		if err := sharedKafka.ctr.Start(ctx); err != nil {
			return err
		}
	}
	mapped, err := sharedKafka.ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return err
	}
	if mapped.Port() != kafkaAdvPort {
		return errors.New("kafka port changed")
	}
	brokerAddr = net.JoinHostPort(kafkaAdvHost, kafkaAdvPort)
	sharedKafka.Broker = brokerAddr
	return waitKafkaReady(ctx)
}

func startKafka(ctx context.Context) (*testKafka, error) {
	port, err := freePort(ctx)
	if err != nil {
		return nil, err
	}
	kafkaAdvPort = port
	ctr, err := testcontainers.Run(ctx, kafkaImage,
		testcontainers.WithLogger(log.NewNoopLogger()),
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("9092/tcp"): []network.PortBinding{{
					HostIP:   netip.MustParseAddr(kafkaAdvHost),
					HostPort: kafkaAdvPort,
				}},
			}
		}),
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID":                                             kafkaCluster,
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
		testcontainers.WithCmd("-c", "while [ ! -f "+kafkaScript+" ]; do sleep 0.1; done; exec bash "+kafkaScript),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{writeKafkaScript},
		}),
	)
	if err != nil {
		if ctr != nil {
			terminateContainer(ctx, ctr)
		}
		return nil, err
	}
	mapped, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		terminateContainer(ctx, ctr)
		return nil, err
	}
	if mapped.Port() != kafkaAdvPort {
		terminateContainer(ctx, ctr)
		return nil, errors.New("kafka port changed")
	}
	broker := net.JoinHostPort(kafkaAdvHost, kafkaAdvPort)
	return &testKafka{Broker: broker, ctr: ctr}, nil
}

func freePort(ctx context.Context) (string, error) {
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

func waitKafkaReady(ctx context.Context) error {
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	p, err := OpenProducer([]string{net.JoinHostPort(kafkaAdvHost, kafkaAdvPort)})
	if err != nil {
		return err
	}
	defer func() { _ = p.Close() }()
	for {
		pingCtx, pingCancel := context.WithTimeout(waitCtx, time.Second)
		err = p.Ready(pingCtx)
		pingCancel()
		if err == nil {
			return nil
		}
		if waitCtx.Err() != nil {
			return errors.New("kafka not ready")
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return errors.New("kafka not ready")
		case <-timer.C:
		}
	}
}

func (k *testKafka) Close(ctx context.Context) error {
	if k == nil || k.ctr == nil {
		return nil
	}
	terminateContainer(ctx, k.ctr)
	k.ctr = nil
	return nil
}

func writeKafkaScript(ctx context.Context, ctr testcontainers.Container) error {
	if err := wait.ForMappedPort("9092/tcp").WaitUntilReady(ctx, ctr); err != nil {
		return err
	}
	mapped, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return err
	}
	if mapped.Port() != kafkaAdvPort || !kafkaHostRE.MatchString(kafkaAdvHost) || !kafkaPortRE.MatchString(kafkaAdvPort) {
		return errors.New("kafka endpoint rejected")
	}
	script := fmt.Sprintf(`#!/usr/bin/env bash
set -eu
export KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://%s:%s,BROKER://127.0.0.1:9094'
exec /etc/kafka/docker/run
`, kafkaAdvHost, kafkaAdvPort)
	if err := ctr.CopyToContainer(ctx, []byte(script), kafkaScript, 0o755); err != nil {
		return err
	}
	// 容器日志里已经有上一次的 “Kafka Server started”，不能用它判断这次进程。
	return waitKafkaReady(ctx)
}

func terminateContainer(ctx context.Context, ctr testcontainers.Container) {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()
	_ = ctr.Terminate(stopCtx)
}
