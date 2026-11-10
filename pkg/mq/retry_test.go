package mq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yym108/pay-system/pkg/server"
)

// MQ-03：Permanent 是不可重试，其余非 nil 是可重试。
func TestPermanentVsRetryable(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	inner := errors.New("missing field")
	perm := Permanent(inner)
	if !IsPermanent(perm) {
		t.Fatal("Permanent error was classified retryable")
	}
	if !IsPermanent(fmt.Errorf("caller: %w", perm)) {
		t.Fatal("wrapped Permanent error was classified retryable")
	}
	if IsPermanent(errors.New("database unavailable")) {
		t.Fatal("plain error was classified permanent")
	}
	if IsPermanent(fmt.Errorf("wrap: %w", errors.New("network"))) {
		t.Fatal("wrapped plain error was classified permanent")
	}

	var committed bool
	var dlqCause error
	c := &Consumer{
		group: "g-perm",
		cfg:   ConsumerConfig{Handler: func(context.Context, Record) error { return perm }},
		commitFn: func(context.Context, *kgo.Record) error {
			committed = true
			return nil
		},
		dlqFn: func(_ context.Context, _ *kgo.Record, cause error) error {
			dlqCause = cause
			return nil
		},
	}
	c.process(context.Background(), &kgo.Record{Topic: "t-perm", Partition: 1, Offset: 7})
	if !committed {
		t.Fatal("permanent error did not commit")
	}
	if !IsPermanent(dlqCause) {
		t.Fatal("permanent error was not sent to dlq")
	}
	if c.ConsumeErrors("t-perm") != 0 {
		t.Fatalf("permanent error incremented consume errors: %d", c.ConsumeErrors("t-perm"))
	}
	if c.DLQTotal("t-perm") != 1 {
		t.Fatalf("dlq total = %d, want 1", c.DLQTotal("t-perm"))
	}
}

// MQ-03：可重试退避依次等待 100ms、200ms、400ms 后成功。
func TestBackoffThenSuccess(t *testing.T) {
	if RetryBase != 100*time.Millisecond || RetryMax != 5*time.Second {
		t.Fatalf("backoff constants = %s..%s", RetryBase, RetryMax)
	}
	if nextDelay(RetryBase) != 200*time.Millisecond || nextDelay(200*time.Millisecond) != 400*time.Millisecond {
		t.Fatal("delay did not double")
	}
	if nextDelay(3200*time.Millisecond) != RetryMax || nextDelay(RetryMax) != RetryMax {
		t.Fatal("delay did not cap at 5s")
	}

	var waits []time.Duration
	var calls int
	var committed bool
	c := &Consumer{
		group: "g-backoff",
		cfg: ConsumerConfig{
			Sleeper: func(ctx context.Context, d time.Duration) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				waits = append(waits, d)
				return nil
			},
			Handler: func(context.Context, Record) error {
				calls++
				if calls < 4 {
					return errors.New("database unavailable")
				}
				return nil
			},
		},
		commitFn: func(context.Context, *kgo.Record) error {
			committed = true
			return nil
		},
	}
	c.process(context.Background(), &kgo.Record{Topic: "t-backoff", Partition: 0, Offset: 3})
	if calls != 4 {
		t.Fatalf("handler calls = %d, want 4", calls)
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	if len(waits) != len(want) {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("wait %d = %s, want %s", i, waits[i], want[i])
		}
	}
	if !committed {
		t.Fatal("success did not commit")
	}
	if c.ConsumeErrors("t-backoff") != 3 {
		t.Fatalf("consume errors = %d, want 3", c.ConsumeErrors("t-backoff"))
	}
}

// MQ-03：beforeCommit 为 nil 时仍提交 offset。
func TestNilBeforeCommitCommits(t *testing.T) {
	var committed bool
	c := &Consumer{
		group: "g-commit",
		cfg: ConsumerConfig{
			BeforeCommit: nil,
			Handler:      func(context.Context, Record) error { return nil },
		},
		commitFn: func(context.Context, *kgo.Record) error {
			committed = true
			return nil
		},
	}
	c.process(context.Background(), &kgo.Record{Topic: "t-commit", Partition: 0, Offset: 1})
	if !committed {
		t.Fatal("nil beforeCommit skipped commit")
	}
}

func TestCancelSkipsDLQAndCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var committed bool
	var dlq bool
	c := &Consumer{
		group: "g-cancel",
		cfg: ConsumerConfig{
			Sleeper: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
			Handler: func(context.Context, Record) error {
				cancel()
				return errors.New("still down")
			},
		},
		commitFn: func(context.Context, *kgo.Record) error {
			committed = true
			return nil
		},
		dlqFn: func(context.Context, *kgo.Record, error) error {
			dlq = true
			return nil
		},
	}
	c.process(ctx, &kgo.Record{Topic: "t-cancel", Partition: 0, Offset: 1})
	if committed || dlq {
		t.Fatalf("cancel committed=%v dlq=%v", committed, dlq)
	}
}

func TestPanicIsRetryableAndOmitsValue(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	const secret = "super-secret-panic-value"
	var committed bool
	c := &Consumer{
		group: "g-panic",
		cfg: ConsumerConfig{
			Sleeper: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
			Handler: func(context.Context, Record) error { panic(secret) },
		},
		commitFn: func(context.Context, *kgo.Record) error {
			committed = true
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.process(ctx, &kgo.Record{Topic: "t-panic", Partition: 2, Offset: 9})
	if committed {
		t.Fatal("panic committed offset")
	}
	if c.ConsumeErrors("t-panic") != 1 {
		t.Fatalf("consume errors = %d, want 1", c.ConsumeErrors("t-panic"))
	}
	text := buf.String()
	if strings.Contains(text, secret) {
		t.Fatalf("log contains panic value: %s", text)
	}
	if !strings.Contains(text, "ERROR") && !strings.Contains(text, "level=ERROR") {
		t.Fatalf("log missing ERROR: %s", text)
	}
}

func TestDefaultSleeperHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := defaultSleeper(ctx, time.Hour)
	if err == nil {
		t.Fatal("cancelled sleeper returned nil")
	}
	if time.Since(start) > time.Second {
		t.Fatal("default sleeper ignored cancel")
	}
}

func TestOpenProducerRejectsEmptyBrokers(t *testing.T) {
	for _, brokers := range [][]string{nil, {}, {"", "  "}} {
		_, err := OpenProducer(brokers)
		if !errors.Is(err, ErrNoBroker) {
			t.Fatalf("OpenProducer(%q) err = %v", brokers, err)
		}
		if strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), ":") {
			t.Fatalf("error text contains an address: %s", err.Error())
		}
	}
	admin := server.New(server.Config{Addr: "127.0.0.1:0"})
	if _, err := RegisterProducer(admin, " , "); !errors.Is(err, ErrNoBroker) {
		t.Fatalf("RegisterProducer(empty) = %v", err)
	}
}

func TestOpenProducerDoesNotDial(t *testing.T) {
	p, err := OpenProducer([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = p.Ready(ctx)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ready err = %v", err)
	}
	if err.Error() != ErrUnavailable.Error() || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("Ready error text = %q", err.Error())
	}
}

func TestRegisterProducerLazyAndClosed(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sigs := make(chan os.Signal, 1)
	admin := server.New(server.Config{Listener: ln, Signals: sigs, DrainTimeout: time.Second})
	admin.RequireDB(func(context.Context) error { return nil }, 0)
	if _, err := RegisterProducer(admin, " 127.0.0.1:1 "); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := admin.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

// MQ-03：同一批前一条可重试且未提交时，不再处理更高 offset。
func TestRunBatchStopsAfterUncommitted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	const topic = "t-batch-stop"
	var calls []int64
	var commits int
	c := &Consumer{
		group: "g-batch-stop",
		gates: map[partKey]*partGate{{topic: topic, partition: 0}: {}},
		cfg: ConsumerConfig{
			Sleeper: func(ctx context.Context, _ time.Duration) error {
				cancel()
				return ctx.Err()
			},
			Handler: func(_ context.Context, rec Record) error {
				calls = append(calls, rec.Offset)
				return errors.New("retry")
			},
		},
		commitFn: func(context.Context, *kgo.Record) error {
			commits++
			return nil
		},
	}
	c.runBatch(batch{
		ctx: ctx,
		recs: []*kgo.Record{
			{Topic: topic, Partition: 0, Offset: 1},
			{Topic: topic, Partition: 0, Offset: 2},
		},
	})
	if len(calls) != 1 || calls[0] != 1 {
		t.Fatalf("handler offsets = %v, want [1]", calls)
	}
	if commits != 0 {
		t.Fatalf("commits = %d, want 0", commits)
	}
}

// MQ-03：永久错误写 DLQ 失败后只重试写入，成功后再提交原 offset。
func TestPermanentDLQRetriesWithoutRehandle(t *testing.T) {
	const topic = "t-dlq-once"
	var handles int
	var dlqCalls int
	var committed bool
	c := &Consumer{
		group: "g-dlq-once",
		cfg: ConsumerConfig{
			Sleeper: func(context.Context, time.Duration) error { return nil },
			Handler: func(context.Context, Record) error {
				handles++
				return Permanent(errors.New("bad payload"))
			},
		},
		dlqFn: func(context.Context, *kgo.Record, error) error {
			dlqCalls++
			if dlqCalls == 1 {
				return errors.New("dlq down")
			}
			return nil
		},
		commitFn: func(context.Context, *kgo.Record) error {
			committed = true
			return nil
		},
	}
	if !c.process(context.Background(), &kgo.Record{Topic: topic, Partition: 0, Offset: 4}) {
		t.Fatal("process returned uncommitted")
	}
	if handles != 1 {
		t.Fatalf("handler calls = %d, want 1", handles)
	}
	if dlqCalls != 2 {
		t.Fatalf("dlq calls = %d, want 2", dlqCalls)
	}
	if !committed {
		t.Fatal("dlq success did not commit")
	}
}

// MQ-03：世代或成员失效时不 sleep、不提交，同批下一条不处理。
func TestCommitMembershipStopsBatch(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "illegal generation", err: kerr.IllegalGeneration},
		{name: "unknown member", err: kerr.UnknownMemberID},
		{name: "rebalance in progress", err: kerr.RebalanceInProgress},
		{name: "fenced instance", err: kerr.FencedInstanceID},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topic := fmt.Sprintf("t-member-%d", i)
			var sleeps int
			var commits int
			var handles []int64
			c := &Consumer{
				group: fmt.Sprintf("g-member-%d", i),
				gates: map[partKey]*partGate{{topic: topic, partition: 0}: {}},
				cfg: ConsumerConfig{
					Sleeper: func(context.Context, time.Duration) error {
						sleeps++
						return errors.New("sleeper should not run")
					},
					Handler: func(_ context.Context, rec Record) error {
						handles = append(handles, rec.Offset)
						return nil
					},
				},
				commitFn: func(context.Context, *kgo.Record) error {
					commits++
					return tc.err
				},
			}
			rec := &kgo.Record{Topic: topic, Partition: 0, Offset: 1}
			if c.process(context.Background(), rec) {
				t.Fatal("process returned committed")
			}
			if sleeps != 0 {
				t.Fatalf("sleeper calls = %d, want 0", sleeps)
			}
			if commits != 1 {
				t.Fatalf("commit calls = %d, want 1", commits)
			}
			handles = nil
			c.runBatch(batch{
				ctx: context.Background(),
				recs: []*kgo.Record{
					{Topic: topic, Partition: 0, Offset: 10},
					{Topic: topic, Partition: 0, Offset: 11},
				},
			})
			if len(handles) != 1 || handles[0] != 10 {
				t.Fatalf("handler offsets = %v, want [10]", handles)
			}
			if sleeps != 0 {
				t.Fatalf("sleeper calls after batch = %d, want 0", sleeps)
			}
		})
	}
}

// MQ-03：普通提交错误仍按退避重试，不当作世代失效。
func TestCommitRetriesOrdinaryError(t *testing.T) {
	const topic = "t-commit-retry"
	var sleeps []time.Duration
	var commits int
	c := &Consumer{
		group: "g-commit-retry",
		cfg: ConsumerConfig{
			Sleeper: func(_ context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				return nil
			},
			Handler: func(context.Context, Record) error { return nil },
		},
		commitFn: func(context.Context, *kgo.Record) error {
			commits++
			if commits == 1 {
				return errors.New("network")
			}
			return nil
		},
	}
	if !c.process(context.Background(), &kgo.Record{Topic: topic, Partition: 0, Offset: 2}) {
		t.Fatal("process returned uncommitted")
	}
	if commits != 2 {
		t.Fatalf("commit calls = %d, want 2", commits)
	}
	if len(sleeps) != 1 || sleeps[0] != RetryBase {
		t.Fatalf("sleeps = %v, want [%s]", sleeps, RetryBase)
	}
}

// MQ-03：没提交的 offset 挡住同分区更高的记录，并把拉取位置退回到该 offset。
func TestUncommittedHoldsPartitionAcrossBatches(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "rebalance in progress", err: kerr.RebalanceInProgress},
		{name: "illegal generation", err: kerr.IllegalGeneration},
		{name: "unknown member", err: kerr.UnknownMemberID},
		{name: "fenced instance", err: kerr.FencedInstanceID},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const (
				first = int64(4)
				epoch = int32(12)
				hwm   = int64(100)
			)
			partTopic := fmt.Sprintf("t-hold-%d", i)
			var handles []int64
			var commits int
			var rewound []kgo.EpochOffset
			failed := false
			c := &Consumer{
				group: fmt.Sprintf("g-hold-%d", i),
				gates: map[partKey]*partGate{{topic: partTopic, partition: 0}: {}},
				cfg: ConsumerConfig{
					Handler: func(_ context.Context, rec Record) error {
						handles = append(handles, rec.Offset)
						return nil
					},
				},
				commitFn: func(_ context.Context, rec *kgo.Record) error {
					commits++
					if rec.Offset == first && !failed {
						failed = true
						return tc.err
					}
					return nil
				},
				rewindFn: func(set map[string]map[int32]kgo.EpochOffset) {
					rewound = append(rewound, set[partTopic][0])
				},
			}
			c.rewindUncommitted()
			if len(rewound) != 0 {
				t.Fatal("rewind called without a hold")
			}
			c.runBatch(batch{
				ctx: context.Background(),
				hwm: hwm,
				recs: []*kgo.Record{
					{Topic: partTopic, Partition: 0, Offset: first, LeaderEpoch: epoch},
					{Topic: partTopic, Partition: 0, Offset: first + 1, LeaderEpoch: epoch},
				},
			})
			if len(handles) != 1 || handles[0] != first {
				t.Fatalf("first batch handlers = %v, want [%d]", handles, first)
			}
			if commits != 1 {
				t.Fatalf("first batch commits = %d, want 1", commits)
			}
			lag := c.ConsumeLag(partTopic, 0)
			handles = nil
			commits = 0
			c.runBatch(batch{
				ctx: context.Background(),
				hwm: hwm,
				recs: []*kgo.Record{
					{Topic: partTopic, Partition: 0, Offset: first + 10, LeaderEpoch: epoch},
				},
			})
			if len(handles) != 0 || commits != 0 {
				t.Fatalf("later batch handles=%v commits=%d", handles, commits)
			}
			if c.ConsumeLag(partTopic, 0) != lag {
				t.Fatalf("later batch moved lag from %d to %d", lag, c.ConsumeLag(partTopic, 0))
			}
			c.rewindUncommitted()
			if len(rewound) != 1 || rewound[0].Offset != first || rewound[0].Epoch != epoch {
				t.Fatalf("rewind = %+v, want offset %d epoch %d", rewound, first, epoch)
			}
			c.runBatch(batch{
				ctx: context.Background(),
				hwm: hwm,
				recs: []*kgo.Record{
					{Topic: partTopic, Partition: 0, Offset: first, LeaderEpoch: epoch},
					{Topic: partTopic, Partition: 0, Offset: first + 1, LeaderEpoch: epoch},
				},
			})
			if len(handles) != 2 || handles[0] != first || handles[1] != first+1 {
				t.Fatalf("replay handlers = %v", handles)
			}
			if commits != 2 {
				t.Fatalf("replay commits = %d, want 2", commits)
			}
		})
	}
}

// MQ-03：生产失败的错误文本不含 broker 地址。
func TestProduceErrorHidesAddress(t *testing.T) {
	dial := fmt.Errorf("%w, last err: %w", kgo.ErrRecordTimeout, errors.New("unable to dial: dial tcp 127.0.0.1:1: connect: connection refused"))
	err := mapProduceErr(context.Background(), dial)
	if !errors.Is(err, errProduceTimeout) {
		t.Fatalf("timeout err = %v", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatal("timeout matched ErrUnavailable")
	}
	meta := errors.New("metadata request failed: broker 10.0.0.1:9092: dial tcp 10.0.0.1:9092: connect: connection refused")
	other := mapProduceErr(context.Background(), meta)
	if !errors.Is(other, ErrUnavailable) {
		t.Fatalf("metadata err = %v", other)
	}
	if errors.Is(other, errProduceTimeout) {
		t.Fatal("metadata matched produce timeout")
	}
	for _, got := range []error{err, other} {
		text := got.Error()
		for _, banned := range []string{"127.0.0.1", "10.0.0.1", "dial"} {
			if strings.Contains(text, banned) {
				t.Fatalf("error text %q contains %q", text, banned)
			}
		}
	}
	dead := mapProduceErr(context.Background(), context.DeadlineExceeded)
	if !errors.Is(dead, errProduceTimeout) || errors.Is(dead, ErrUnavailable) {
		t.Fatalf("deadline err = %v", dead)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := mapProduceErr(ctx, dial)
	if !errors.Is(got, context.Canceled) || strings.Contains(got.Error(), "127.0.0.1") || strings.Contains(got.Error(), "dial") {
		t.Fatalf("cancelled produce err = %v", got)
	}
}

// MQ-02：错误文本不含 127.0.0.1 和 dial。空批次不拨号。空 broker 仍是 ErrNoBroker。
func TestMQ02ProduceBatchHidesAddress(t *testing.T) {
	var closed *Producer
	if closed.ProduceBatch(context.Background(), nil) != nil {
		t.Fatal("empty batch on nil producer")
	}
	errs := closed.ProduceBatch(context.Background(), []Message{{Topic: "pay.mq02.none"}})
	if len(errs) != 1 || !errors.Is(errs[0], ErrUnavailable) {
		t.Fatalf("nil client errs = %v", errs)
	}
	if strings.Contains(errs[0].Error(), "127.0.0.1") || strings.Contains(errs[0].Error(), "dial") {
		t.Fatal("nil client error text contains an address")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p, err := OpenProducer([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	errs = p.ProduceBatch(ctx, []Message{{
		Topic: "pay.mq02.none", Key: []byte("k"), Value: []byte("v"),
		EventID: "EV0000000000000000001", EventType: "payment.succeeded",
	}})
	if len(errs) != 1 || !errors.Is(errs[0], context.Canceled) {
		t.Fatalf("cancelled batch = %v", errs)
	}
	if strings.Contains(errs[0].Error(), "127.0.0.1") || strings.Contains(errs[0].Error(), "dial") {
		t.Fatal("cancelled batch error text contains an address")
	}
	errs = p.ProduceBatch(context.Background(), []Message{{
		Topic: "pay.mq02.none", Key: []byte("k"), Value: []byte("{}"),
		EventID: "EV0000000000000000001", EventType: "payment.succeeded", Traceparent: "",
	}})
	if len(errs) != 1 || errs[0] == nil {
		t.Fatalf("down broker errs = %v", errs)
	}
	text := errs[0].Error()
	if strings.Contains(text, "127.0.0.1") || strings.Contains(text, "dial") {
		t.Fatal("produce error text contains an address")
	}
	if _, err = OpenProducer(nil); !errors.Is(err, ErrNoBroker) {
		t.Fatalf("empty broker = %v", err)
	}
	if strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), "dial") {
		t.Fatal("empty broker error text contains an address")
	}
}
