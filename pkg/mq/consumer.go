package mq

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerConfig 是一个消费组。BeforeCommit 只给测试设置，生产路径保持 nil。
// SessionTimeout 为 0 时用客户端默认。Sleeper 为 nil 时用 ctx 控制的定时器，间隔仍是 RetryBase 起翻倍。
type ConsumerConfig struct {
	Brokers        []string
	Group          string
	Topics         []string
	Handler        Handler
	BeforeCommit   CommitHook
	Sleeper        Sleeper
	SessionTimeout time.Duration
}

// Consumer 实现 server.Group。Serve 拉取并处理，Stop 停止拉取并等待在途消息结束。
type Consumer struct {
	cfg    ConsumerConfig
	group  string
	client *kgo.Client

	commitFn func(context.Context, *kgo.Record) error
	dlqFn    func(context.Context, *kgo.Record, error) error
	// rewindFn 非 nil 时只交给测试，不调用客户端。
	rewindFn func(map[string]map[int32]kgo.EpochOffset)

	mu       sync.Mutex
	serveCtx context.Context
	cancel   context.CancelFunc
	stopping bool
	done     chan struct{}

	workers map[partKey]chan batch
	gates   map[partKey]*partGate
	cursors map[partKey]*cursor
	// holds 是必须先提交的 offset。更高的 offset 不能处理，否则中间的记录不会再投递。
	holds  map[partKey]holdOffset
	workWG sync.WaitGroup
}

// holdOffset 记下还没提交的记录位置。拉取要退回到 Offset 本身，不是 Offset+1。
type holdOffset struct {
	offset int64
	epoch  int32
}

type partKey struct {
	topic     string
	partition int32
}

type batch struct {
	ctx  context.Context
	recs []*kgo.Record
	hwm  int64
	done func()
}

type cursor struct {
	read      bool
	readAt    int64
	committed bool
	next      int64
	hwm       int64
	hwmOK     bool
}

// partGate 把“正在处理”和“准备撤销”放在同一把锁上。
// 撤销时如果这条还在处理，要等它提交完再返回。
type partGate struct {
	mu       sync.Mutex
	inflight bool
	revoking bool
	waiters  []chan struct{}
}

// OpenConsumer 创建消费组客户端。不在这里拨号，也不在这里开始拉取。
func OpenConsumer(cfg ConsumerConfig) (*Consumer, error) {
	seeds := compactBrokers(cfg.Brokers)
	if len(seeds) == 0 {
		return nil, ErrNoBroker
	}
	if cfg.Group == "" {
		return nil, errors.New("no kafka consumer group")
	}
	if len(cfg.Topics) == 0 {
		return nil, errors.New("no kafka topic")
	}
	if cfg.Handler == nil {
		return nil, errors.New("no kafka handler")
	}
	c := &Consumer{
		cfg:     cfg,
		group:   cfg.Group,
		done:    make(chan struct{}),
		workers: map[partKey]chan batch{},
		gates:   map[partKey]*partGate{},
		cursors: map[partKey]*cursor{},
	}
	opts := append(baseOpts(seeds),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			c.finishBeforeRevoke(ctx, revoked)
		}),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			c.onAssigned(assigned)
		}),
	)
	if cfg.SessionTimeout > 0 {
		heartbeat := cfg.SessionTimeout / 4
		if heartbeat < time.Millisecond {
			heartbeat = time.Millisecond
		}
		opts = append(opts,
			kgo.SessionTimeout(cfg.SessionTimeout),
			kgo.HeartbeatInterval(heartbeat),
		)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errOpenClient
	}
	c.client = cl
	return c, nil
}

// Serve 拉取消息，直到 Stop 取消。返回前在途处理已经结束，成功的 offset 已经提交。
func (c *Consumer) Serve() error {
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.serveCtx = ctx
	c.cancel = cancel
	already := c.stopping
	c.mu.Unlock()
	if already {
		cancel()
	}
	defer func() {
		cancel()
		c.stopWorkers()
		if c.client != nil {
			c.client.Close()
		}
		close(c.done)
	}()
	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := c.client.PollFetches(ctx)
		c.handleFetches(ctx, fetches)
		// AllowRebalance 之后才会撤销。ctx 还活着时先退回拉取位置，并且不与提交并发。
		if ctx.Err() == nil {
			c.rewindUncommitted()
		}
		c.client.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Stop 停止拉取，并等待 Serve 把在途消息处理完。ctx 为 nil 时一直等到 Serve 返回。
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.stopping = true
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if ctx == nil {
		<-c.done
		return nil
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) handleFetches(ctx context.Context, fetches kgo.Fetches) {
	if ctx.Err() == nil {
		fetches.EachError(func(topic string, partition int32, err error) {
			if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
				return
			}
			slog.ErrorContext(ctx, "mq fetch failed", "topic", topic, "partition", partition)
		})
	}
	var wg sync.WaitGroup
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			if p.HighWatermark > 0 {
				c.noteWatermark(p.Topic, p.Partition, p.HighWatermark)
			}
			return
		}
		wg.Add(1)
		c.workerChan(partKey{topic: p.Topic, partition: p.Partition}) <- batch{
			ctx:  ctx,
			recs: p.Records,
			hwm:  p.HighWatermark,
			done: wg.Done,
		}
	})
	wg.Wait()
}

func (c *Consumer) workerChan(key partKey) chan batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gates[key] == nil {
		c.gates[key] = &partGate{}
	}
	ch := c.workers[key]
	if ch == nil {
		ch = make(chan batch)
		c.workers[key] = ch
		c.workWG.Add(1)
		go c.loopWorker(ch)
	}
	return ch
}

func (c *Consumer) loopWorker(ch <-chan batch) {
	defer c.workWG.Done()
	for job := range ch {
		func() {
			defer job.done()
			c.runBatch(job)
		}()
	}
}

func (c *Consumer) runBatch(job batch) {
	for _, rec := range job.recs {
		key := partKey{topic: rec.Topic, partition: rec.Partition}
		// 更小的 offset 还没提交时，不能读、不能处理、不能提交更高的。
		if c.holdBlocks(key, rec.Offset) {
			return
		}
		c.noteRead(key.topic, key.partition, rec.Offset, job.hwm)
		gate := c.gate(key)
		if gate == nil || !gate.begin() {
			return
		}
		committed := false
		func() {
			defer gate.end()
			committed = c.process(job.ctx, rec)
		}()
		// 这一条没提交时停掉本批，避免后面的提交把更早的 offset 标成已消费。
		if !committed {
			c.markHold(key, rec.Offset, rec.LeaderEpoch)
			return
		}
		c.clearHold(key)
	}
}

func (c *Consumer) holdBlocks(key partKey, offset int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, ok := c.holds[key]
	return ok && offset > prev.offset
}

func (c *Consumer) markHold(key partKey, offset int64, epoch int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.holds == nil {
		c.holds = map[partKey]holdOffset{}
	}
	if prev, ok := c.holds[key]; ok && prev.offset < offset {
		return
	}
	c.holds[key] = holdOffset{offset: offset, epoch: epoch}
}

func (c *Consumer) clearHold(key partKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.holds, key)
}

// rewindUncommitted 把没提交的分区退回到记下的 offset。没有标记时不调用。
// 钩子非 nil 时只调钩子。调用点在 PollFetches 之后、AllowRebalance 之前。
func (c *Consumer) rewindUncommitted() {
	c.mu.Lock()
	if len(c.holds) == 0 {
		c.mu.Unlock()
		return
	}
	set := make(map[string]map[int32]kgo.EpochOffset, len(c.holds))
	for key, hold := range c.holds {
		parts := set[key.topic]
		if parts == nil {
			parts = map[int32]kgo.EpochOffset{}
			set[key.topic] = parts
		}
		parts[key.partition] = kgo.EpochOffset{Epoch: hold.epoch, Offset: hold.offset}
	}
	fn := c.rewindFn
	client := c.client
	c.mu.Unlock()
	if fn != nil {
		fn(set)
		return
	}
	if client != nil {
		client.SetOffsets(set)
	}
}

func (c *Consumer) gate(key partKey) *partGate {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gates[key]
}

func (c *Consumer) stopWorkers() {
	c.mu.Lock()
	chans := c.workers
	c.workers = map[partKey]chan batch{}
	c.mu.Unlock()
	for _, ch := range chans {
		close(ch)
	}
	c.workWG.Wait()
}

// finishBeforeRevoke 在撤销这些分区前等待当前这条处理完并提交。
// 测试在处理函数还阻塞时直接调用它，不靠第二个消费组成员。
func (c *Consumer) finishBeforeRevoke(ctx context.Context, revoked map[string][]int32) {
	var gates []*partGate
	c.mu.Lock()
	for topic, parts := range revoked {
		for _, partition := range parts {
			if gate := c.gates[partKey{topic: topic, partition: partition}]; gate != nil {
				gates = append(gates, gate)
			}
		}
	}
	c.mu.Unlock()
	for _, gate := range gates {
		_ = gate.revoke(ctx)
	}
}

func (c *Consumer) onAssigned(assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, parts := range assigned {
		for _, partition := range parts {
			key := partKey{topic: topic, partition: partition}
			gate := c.gates[key]
			if gate == nil {
				gate = &partGate{}
				c.gates[key] = gate
			}
			gate.clearRevoke()
		}
	}
}

func (g *partGate) begin() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.revoking {
		return false
	}
	g.inflight = true
	return true
}

func (g *partGate) end() {
	g.mu.Lock()
	g.inflight = false
	waiters := g.waiters
	g.waiters = nil
	g.mu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

func (g *partGate) revoke(ctx context.Context) error {
	g.mu.Lock()
	g.revoking = true
	if !g.inflight {
		g.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	g.waiters = append(g.waiters, ch)
	g.mu.Unlock()
	if ctx == nil {
		<-ch
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *partGate) clearRevoke() {
	g.mu.Lock()
	g.revoking = false
	g.mu.Unlock()
}

// process 返回这条 offset 是否已经提交。
func (c *Consumer) process(ctx context.Context, rec *kgo.Record) bool {
	delay := RetryBase
	// pinned 记住已经返回的永久错误。写 DLQ 失败后只重试写入，不再调用处理函数。
	var pinned error
	for {
		var err error
		if pinned != nil {
			err = pinned
		} else {
			err = c.safeHandle(ctx, rec)
		}
		if err == nil {
			if c.cfg.BeforeCommit != nil {
				c.cfg.BeforeCommit(ctx, toRecord(rec))
			}
			return c.commitUntil(ctx, rec)
		}
		if IsPermanent(err) && ctx.Err() == nil {
			if derr := c.writeDLQ(ctx, rec, err); derr != nil {
				pinned = err
				c.noteRetry(ctx, rec)
				if c.sleep(ctx, delay) != nil {
					return false
				}
				delay = nextDelay(delay)
				continue
			}
			c.noteDLQ(ctx, rec)
			return c.commitUntil(ctx, rec)
		}
		if ctx.Err() != nil {
			if !IsPermanent(err) {
				c.noteRetry(ctx, rec)
			}
			return false
		}
		c.noteRetry(ctx, rec)
		if c.sleep(ctx, delay) != nil {
			return false
		}
		delay = nextDelay(delay)
	}
}

func (c *Consumer) safeHandle(ctx context.Context, rec *kgo.Record) (err error) {
	defer func() {
		if recover() != nil {
			err = errHandlerFailed
		}
	}()
	if c.cfg.Handler == nil {
		return errHandlerFailed
	}
	return c.cfg.Handler(ctx, toRecord(rec))
}

func (c *Consumer) sleep(ctx context.Context, delay time.Duration) error {
	fn := c.cfg.Sleeper
	if fn == nil {
		fn = defaultSleeper
	}
	return fn(ctx, delay)
}

// commitUntil 返回是否提交成功。世代或成员失效时立刻返回未提交，不再空转。
// 调用方记下该 offset。RebalanceInProgress 和 FencedInstanceID 不会在这条路径上 rejoin。
func (c *Consumer) commitUntil(ctx context.Context, rec *kgo.Record) bool {
	delay := RetryBase
	for {
		err := c.commitOnce(ctx, rec)
		if err == nil {
			c.noteCommit(rec.Topic, rec.Partition, rec.Offset+1)
			return true
		}
		if commitMembershipLost(err) {
			return false
		}
		if c.sleep(ctx, delay) != nil {
			err = c.commitOnce(context.WithoutCancel(ctx), rec)
			if err == nil {
				c.noteCommit(rec.Topic, rec.Partition, rec.Offset+1)
				return true
			}
			return false
		}
		delay = nextDelay(delay)
	}
}

// commitMembershipLost 识别提交时世代或成员已经失效。这类错误原样返回，不写日志。
func commitMembershipLost(err error) bool {
	return errors.Is(err, kerr.IllegalGeneration) ||
		errors.Is(err, kerr.UnknownMemberID) ||
		errors.Is(err, kerr.RebalanceInProgress) ||
		errors.Is(err, kerr.FencedInstanceID)
}

func (c *Consumer) commitOnce(ctx context.Context, rec *kgo.Record) error {
	if c.commitFn != nil {
		return c.commitFn(ctx, rec)
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ProduceTimeout)
	defer cancel()
	return c.client.CommitRecords(cctx, rec)
}

func (c *Consumer) writeDLQ(ctx context.Context, rec *kgo.Record, cause error) error {
	if c.dlqFn != nil {
		return c.dlqFn(ctx, rec, cause)
	}
	pctx, cancel := context.WithTimeout(ctx, ProduceTimeout)
	defer cancel()
	return c.client.ProduceSync(pctx, dlqRecord(rec, cause)).FirstErr()
}

func dlqRecord(rec *kgo.Record, cause error) *kgo.Record {
	msg := []byte(cause.Error())
	if len(msg) > errorHeaderMax {
		msg = msg[:errorHeaderMax]
	}
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers)+4)
	headers = append(headers, rec.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: HeaderOrigTopic, Value: []byte(rec.Topic)},
		kgo.RecordHeader{Key: HeaderOrigPartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
		kgo.RecordHeader{Key: HeaderOrigOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		kgo.RecordHeader{Key: HeaderError, Value: msg},
	)
	return &kgo.Record{
		Topic:   DLQTopic,
		Key:     append([]byte(nil), rec.Key...),
		Value:   append([]byte(nil), rec.Value...),
		Headers: headers,
	}
}

func (c *Consumer) noteRetry(ctx context.Context, rec *kgo.Record) {
	addConsumeError(c.group, rec.Topic)
	view := toRecord(rec)
	slog.ErrorContext(ctx, "mq consume retry",
		"event_id", view.EventID,
		"event_type", view.EventType,
		"topic", rec.Topic,
		"partition", rec.Partition,
		"offset", rec.Offset,
	)
}

func (c *Consumer) noteDLQ(ctx context.Context, rec *kgo.Record) {
	addDLQ(c.group, rec.Topic)
	view := toRecord(rec)
	slog.ErrorContext(ctx, "mq message moved to dlq",
		"event_id", view.EventID,
		"event_type", view.EventType,
		"topic", rec.Topic,
		"partition", rec.Partition,
		"offset", rec.Offset,
	)
}

func (c *Consumer) noteRead(topic string, partition int32, offset, hwm int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.cursor(topic, partition)
	cur.read = true
	cur.readAt = offset
	if !cur.hwmOK || hwm > cur.hwm {
		cur.hwm = hwm
		cur.hwmOK = true
	}
	c.publish(topic, partition, cur)
}

func (c *Consumer) noteWatermark(topic string, partition int32, hwm int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.cursor(topic, partition)
	if !cur.hwmOK || hwm > cur.hwm {
		cur.hwm = hwm
		cur.hwmOK = true
	}
	c.publish(topic, partition, cur)
}

func (c *Consumer) noteCommit(topic string, partition int32, next int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.cursor(topic, partition)
	if !cur.committed || next > cur.next {
		cur.committed = true
		cur.next = next
	}
	c.publish(topic, partition, cur)
}

func (c *Consumer) cursor(topic string, partition int32) *cursor {
	if c.cursors == nil {
		c.cursors = map[partKey]*cursor{}
	}
	key := partKey{topic: topic, partition: partition}
	cur := c.cursors[key]
	if cur == nil {
		cur = &cursor{}
		c.cursors[key] = cur
	}
	return cur
}

func (c *Consumer) publish(topic string, partition int32, cur *cursor) {
	var pos int64
	switch {
	case cur.committed:
		pos = cur.next
	case cur.read:
		pos = cur.readAt
	default:
		pos = cur.hwm
	}
	lag := int64(0)
	if cur.hwmOK && cur.hwm > pos {
		lag = cur.hwm - pos
	}
	setLag(c.group, topic, partition, lag)
}
