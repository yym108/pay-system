package biz

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/money"
)

// TRF-04：退避按加 1 后的 retry_count。第一次 0→1 是 2 秒，n>=6 封顶 60 秒。
func TestTransferRecoverBackoff(t *testing.T) {
	if transferRecoverBatchSize != 100 || transferRecoverConcurrency != 20 {
		t.Fatalf("batch=%d concurrency=%d", transferRecoverBatchSize, transferRecoverConcurrency)
	}
	now := feeAt(12, 0, 0, 0)
	cases := []struct {
		n    int
		want time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 60 * time.Second},
		{7, 60 * time.Second},
		{10, 60 * time.Second},
		{100, 60 * time.Second},
	}
	for _, c := range cases {
		if got := transferRecoverBackoff(c.n); got != c.want {
			t.Fatalf("backoff(%d) = %s, want %s", c.n, got, c.want)
		}
	}
	n, at := nextTransferRetry(now, 0)
	if n != 1 || !at.Equal(now.Add(2*time.Second)) {
		t.Fatalf("first claim = %d %s", n, at)
	}
	n, at = nextTransferRetry(now, 5)
	if n != 6 || !at.Equal(now.Add(60*time.Second)) {
		t.Fatalf("capped claim = %d %s", n, at)
	}
	if _, at = nextTransferRetry(now, -1); !at.Equal(now.Add(2 * time.Second)) {
		t.Fatalf("negative count panicked or drifted: %s", at)
	}
}

type gateLocal struct {
	active  atomic.Int32
	peak    atomic.Int32
	calls   atomic.Int32
	hold    <-chan struct{}
	panicNo string
	seen    func(context.Context)
	err     error
	result  LocalTransferResult
}

func (g *gateLocal) TransferLocal(ctx context.Context, transferNo, _, _ string, _ money.Amount) (LocalTransferResult, error) {
	if g.seen != nil {
		g.seen(ctx)
	}
	if transferNo == g.panicNo {
		panic("boom-secret")
	}
	cur := g.active.Add(1)
	for {
		old := g.peak.Load()
		if cur <= old || g.peak.CompareAndSwap(old, cur) {
			break
		}
	}
	g.calls.Add(1)
	if g.hold != nil {
		<-g.hold
	}
	g.active.Add(-1)
	if g.err != nil {
		return LocalTransferResult{}, g.err
	}
	return g.result, nil
}

func TestDispatchTransferRecoverCapsConcurrency(t *testing.T) {
	tt := newLocalTransferTest(t)
	hold := make(chan struct{})
	local := &gateLocal{hold: hold, err: errno.ErrUnavailable}
	tt.p.UseLocalTransfer(local)
	rows := make([]TransferOrder, 25)
	for i := range rows {
		rows[i] = TransferOrder{
			TransferNo:      "T" + itoa(i),
			MerchantID:      "M100001",
			PayeeMerchantID: "M100003",
			Amount:          20,
			Mode:            transferModeLocal,
			Status:          transferStatusTrying,
			RetryCount:      1,
		}
	}
	done := make(chan struct{})
	go func() {
		tt.p.dispatchTransferRecover(context.Background(), rows)
		close(done)
	}()
	if !pollFor(5*time.Second, func() bool {
		return local.peak.Load() == transferRecoverConcurrency && local.active.Load() == transferRecoverConcurrency
	}) {
		t.Fatalf("peak=%d active=%d calls=%d", local.peak.Load(), local.active.Load(), local.calls.Load())
	}
	if local.calls.Load() != transferRecoverConcurrency {
		t.Fatalf("entered = %d", local.calls.Load())
	}
	close(hold)
	if !pollFor(5*time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("dispatch did not finish")
	}
	if local.calls.Load() != 25 || local.peak.Load() != 20 {
		t.Fatalf("calls=%d peak=%d", local.calls.Load(), local.peak.Load())
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// TRF-04：一个任务 panic 不取消兄弟调用，不把 panic 值写进日志。
func TestTransferRecoverPanicDoesNotCancelSiblings(t *testing.T) {
	tt := newLocalTransferTest(t)
	local := &gateLocal{panicNo: "T0", err: errno.ErrUnavailable}
	tt.p.UseLocalTransfer(local)
	buf := captureLog(t)
	before := transferRecoverPanics.Load()
	rows := make([]TransferOrder, 4)
	for i := range rows {
		rows[i] = TransferOrder{
			TransferNo:      "T" + itoa(i),
			MerchantID:      "M100001",
			PayeeMerchantID: "M100003",
			Amount:          20,
			Mode:            transferModeLocal,
			Status:          transferStatusTrying,
			RetryCount:      1,
		}
	}
	tt.p.dispatchTransferRecover(context.Background(), rows)
	if transferRecoverPanics.Load() != before+1 {
		t.Fatalf("panics = %d", transferRecoverPanics.Load())
	}
	if local.calls.Load() != 3 {
		t.Fatalf("sibling calls = %d", local.calls.Load())
	}
	if strings.Contains(buf.String(), "boom-secret") {
		t.Fatalf("panic value logged: %s", buf.String())
	}
	levels, _ := recoverLogLevels(t, buf, "transfer recover task panicked")
	if len(levels) != 1 || levels[0] != "ERROR" {
		t.Fatalf("panic log = %v", levels)
	}
}

// TRF-04：单笔超时不随批次 ctx 取消。
func TestTransferRecoverTaskWithoutCancel(t *testing.T) {
	tt := newLocalTransferTest(t)
	var saw atomic.Bool
	local := &gateLocal{err: errno.ErrUnavailable, seen: func(ctx context.Context) {
		if ctx.Err() != nil {
			t.Errorf("task ctx canceled: %v", ctx.Err())
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("task ctx has no deadline")
		}
		saw.Store(true)
	}}
	tt.p.UseLocalTransfer(local)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	row := TransferOrder{
		TransferNo: "TFSHORT1", MerchantID: "M100001", PayeeMerchantID: "M100003",
		Amount: 20, Mode: transferModeLocal, Status: transferStatusTrying, RetryCount: 1,
	}
	tt.store.rows = append(tt.store.rows, row)
	tt.p.runTransferRecoverTask(ctx, row)
	if !saw.Load() {
		t.Fatal("local was not called")
	}
}

// nil、无时钟、内存存储都返回 10003，不调用账户。两条都没装要在 MySQL 上区分。
func TestRunTransferRecoverBatchNotReady(t *testing.T) {
	var bare *Payments
	if err := bare.RunTransferRecoverBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("nil = %v", err)
	}
	tt := newLocalTransferTest(t)
	local := &scriptedLocal{err: errno.ErrUnavailable}
	tt.p.UseLocalTransfer(local)
	tt.p.clk = nil
	if err := tt.p.RunTransferRecoverBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("no clock = %v", err)
	}
	tt = newLocalTransferTest(t)
	tt.p.UseLocalTransfer(local)
	if err := tt.p.RunTransferRecoverBatch(context.Background()); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("mem store = %v", err)
	}
	if local.callCount() != 0 {
		t.Fatalf("calls = %d", local.callCount())
	}
	// SA1012 不允许把 nil context 当参数。这里正是要证明 nil ctx 返回 10003，而不是 panic。
	if tt.p.RunTransferRecoverBatch(nil) == nil { //nolint:staticcheck
		t.Fatal("nil ctx")
	}
}

// TRF-04：CAS 影响 0 行且仍是 TRYING 时返回 10003，本轮不再次决策。
func TestAdvanceRecoveredCASMissStillTrying(t *testing.T) {
	tt := newTCCTest(t)
	got, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF04-MISS", 100))
	if err != nil || got.Status != transferStatusTrying {
		t.Fatalf("created = %+v err=%v", got, err)
	}
	tt.store.decideMiss = true
	branch := &scriptedTCC{}
	tt.p.UseTCCTransfer(branch, true)
	out, err := tt.p.advanceRecovered(context.Background(), tt.store.snapshot()[0])
	assertErrno(t, err, errno.ErrInternal)
	if out.Status != transferStatusTrying {
		t.Fatalf("out = %+v", out)
	}
	if len(branch.names()) != 0 {
		t.Fatalf("branch calls = %v", branch.names())
	}
	if rows := tt.store.snapshot(); rows[0].Status != transferStatusTrying || rows[0].Version != 0 {
		t.Fatalf("row = %+v", rows[0])
	}
}

// TRF-04：CAS 影响 0 行后再读到 CONFIRMING，只走一次第 5 步。
func TestAdvanceRecoveredCASMissConfirming(t *testing.T) {
	tt := newTCCTest(t)
	if _, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF04-CFM", 100)); err != nil {
		t.Fatal(err)
	}
	tt.store.hijackDecide = func(row *TransferOrder) {
		row.Status = transferStatusConfirming
		row.FailCode = ""
		row.Version = 1
	}
	tt.store.decideMiss = true
	branch := &scriptedTCC{}
	tt.p.UseTCCTransfer(branch, true)
	out, err := tt.p.advanceRecovered(context.Background(), tt.store.snapshot()[0])
	if err != nil || out.Status != transferStatusSuccess || out.TxnNo != "" {
		t.Fatalf("out = %+v err=%v", out, err)
	}
	names := branch.names()
	if countName(names, "confirm-payer") != 1 || countName(names, "confirm-payee") != 1 || countName(names, "freeze") != 0 {
		t.Fatalf("calls = %v", names)
	}
}

// TRF-04：CAS 影响 0 行后再读到终态，不再调用分支，也不再写终态。
func TestAdvanceRecoveredCASMissTerminal(t *testing.T) {
	tt := newTCCTest(t)
	if _, err := tt.p.CreateTransfer(context.Background(), tccInput("TRF04-DONE", 100)); err != nil {
		t.Fatal(err)
	}
	tt.store.hijackDecide = func(row *TransferOrder) {
		row.Status = transferStatusSuccess
		row.Version = 2
	}
	tt.store.decideMiss = true
	branch := &scriptedTCC{}
	tt.p.UseTCCTransfer(branch, true)
	before := tt.store.snapshot()[0]
	out, err := tt.p.advanceRecovered(context.Background(), before)
	if err != nil || out.Status != transferStatusSuccess {
		t.Fatalf("out = %+v err=%v", out, err)
	}
	if len(branch.names()) != 0 {
		t.Fatalf("branch calls = %v", branch.names())
	}
	if got := tt.store.snapshot()[0]; got.Version != 2 || got.Status != transferStatusSuccess {
		t.Fatalf("row = %+v", got)
	}
}

// TRF-04：CONFIRMING 普通失败仍是 CONFIRMING。retry_count 的增加在抢占里，见 MySQL 测试。
func TestRecoverConfirmingFailureStaysConfirming(t *testing.T) {
	tt := newTCCTest(t)
	row := TransferOrder{
		TransferNo: "TFSHORT1", MerchantID: "M100001", OutTransferNo: "TRF04-STAY",
		PayeeMerchantID: "M100002", Amount: 100, Mode: transferModeTCC,
		Status: transferStatusConfirming, RetryCount: 3, Version: 1, PayerShard: 1,
	}
	tt.store.rows = append(tt.store.rows, row)
	branch := &scriptedTCC{confirmP: errno.ErrInternal}
	tt.p.UseTCCTransfer(branch, true)
	tt.p.recoverTransfer(context.Background(), row)
	got := tt.store.snapshot()[0]
	if got.Status != transferStatusConfirming || got.RetryCount != 3 || got.Version != 1 || got.FailCode != "" {
		t.Fatalf("row = %+v", got)
	}
	if countName(branch.names(), "confirm-payer") != 1 || countName(branch.names(), "freeze") != 0 {
		t.Fatalf("calls = %v", branch.names())
	}
}

// TRF-04：pay_transfer_stuck 是当前去重个数，终态去掉，不到 10 不加。
func TestTransferStuckGauge(t *testing.T) {
	resetTransferStuck()
	t.Cleanup(resetTransferStuck)
	trackTransferStuck("A", 10, false)
	trackTransferStuck("A", 11, false)
	if transferStuckGauge() != 1 {
		t.Fatalf("gauge = %d", transferStuckGauge())
	}
	trackTransferStuck("B", 10, false)
	if transferStuckGauge() != 2 {
		t.Fatalf("gauge = %d", transferStuckGauge())
	}
	trackTransferStuck("C", 9, false)
	if transferStuckGauge() != 2 {
		t.Fatalf("retry 9 entered gauge: %d", transferStuckGauge())
	}
	trackTransferStuck("A", 10, true)
	if transferStuckGauge() != 1 {
		t.Fatalf("terminal not removed: %d", transferStuckGauge())
	}

	tt := newLocalTransferTest(t)
	local := &scriptedLocal{err: errno.ErrUnavailable}
	tt.p.UseLocalTransfer(local)
	row := TransferOrder{
		TransferNo: "TFSHORTG", MerchantID: "M100001", PayeeMerchantID: "M100003",
		Amount: 20, Mode: transferModeLocal, Status: transferStatusTrying, RetryCount: 10,
	}
	tt.store.rows = append(tt.store.rows, row)
	tt.p.recoverTransfer(context.Background(), row)
	if transferStuckGauge() != 2 {
		t.Fatalf("after fail gauge = %d", transferStuckGauge())
	}
	local.err = nil
	local.res = LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TXG"}
	tt.p.recoverTransfer(context.Background(), row)
	if transferStuckGauge() != 1 {
		t.Fatalf("after success gauge = %d", transferStuckGauge())
	}
}

// TRF-04：处理失败打 ERROR，字段只有 transfer_no 和十进制 err_code。终态不打这条。
func TestTransferRecoverErrorLog(t *testing.T) {
	tt := newLocalTransferTest(t)
	buf := captureLog(t)
	local := &scriptedLocal{err: errno.ErrUnavailable}
	tt.p.UseLocalTransfer(local)
	row := TransferOrder{
		TransferNo: "TFSHORT1", MerchantID: "M100001", PayeeMerchantID: "M100003",
		Amount: 20, Mode: transferModeLocal, Status: transferStatusTrying, RetryCount: 1,
	}
	tt.store.rows = append(tt.store.rows, row)
	tt.p.recoverTransfer(context.Background(), row)
	assertRecoverLog(t, buf, "TFSHORT1", "10003")

	buf.Reset()
	tt.store.finishErr = errors.New("secret-dsn password")
	local.err = nil
	local.res = LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX1"}
	tt.p.recoverTransfer(context.Background(), row)
	if strings.Contains(buf.String(), "secret") || strings.Contains(buf.String(), "password") {
		t.Fatalf("secret logged: %s", buf.String())
	}
	assertRecoverLog(t, buf, "TFSHORT1", "10003")

	buf.Reset()
	tt.store.finishErr = errno.ErrUnavailable
	tt.p.recoverTransfer(context.Background(), row)
	assertRecoverLog(t, buf, "TFSHORT1", "10004")

	buf.Reset()
	tt.store.finishErr = nil
	local.res = LocalTransferResult{Status: transferStatusSuccess, TxnNo: "TX1"}
	tt.p.recoverTransfer(context.Background(), row)
	levels, errs := recoverLogLevels(t, buf, "transfer recover")
	if len(levels) != 0 || errs != 0 {
		t.Fatalf("success logged levels=%v errs=%d body=%s", levels, errs, buf.String())
	}

	buf.Reset()
	row.Status = transferStatusTrying
	tt.store.rows[0].Status = transferStatusTrying
	tt.store.rows[0].TxnNo = ""
	tt.store.rows[0].FinishedAt = time.Time{}
	local.res = LocalTransferResult{Status: transferStatusFailed, FailCode: transferFailInsufficient}
	tt.p.recoverTransfer(context.Background(), row)
	levels, errs = recoverLogLevels(t, buf, "transfer recover")
	if len(levels) != 0 || errs != 0 {
		t.Fatalf("business failed logged levels=%v errs=%d body=%s", levels, errs, buf.String())
	}
}

func assertRecoverLog(t *testing.T, buf interface{ String() string }, transferNo, code string) {
	t.Helper()
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Level      string `json:"level"`
			Msg        string `json:"msg"`
			TransferNo string `json:"transfer_no"`
			ErrCode    string `json:"err_code"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec.Msg != "transfer recover" {
			continue
		}
		found = true
		if rec.Level != "ERROR" || rec.TransferNo != transferNo || rec.ErrCode != code {
			t.Fatalf("log = %+v", rec)
		}
	}
	if !found {
		t.Fatalf("missing transfer recover log: %s", buf.String())
	}
}

// TRF-04：恢复不调用注入的 TransferExecutor。
func TestTransferRecoverIgnoresExecutor(t *testing.T) {
	tt := newLocalTransferTest(t)
	exec := &fakeExecutor{}
	tt.p.transferExec = exec
	local := &scriptedLocal{err: errno.ErrUnavailable}
	tt.p.UseLocalTransfer(local)
	row := TransferOrder{
		TransferNo: "TFSHORT1", MerchantID: "M100001", PayeeMerchantID: "M100003",
		Amount: 20, Mode: transferModeLocal, Status: transferStatusTrying,
	}
	tt.store.rows = append(tt.store.rows, row)
	tt.p.recoverTransfer(context.Background(), row)
	if local.callCount() != 1 || len(exec.calls()) != 0 {
		t.Fatalf("local=%d exec=%d", local.callCount(), len(exec.calls()))
	}
}

// TRF-04：并行 Confirm 不经过 transfer.after_payer_confirm；串行经过一次。
func TestTransferParallelSkipsPayerConfirmFault(t *testing.T) {
	var n atomic.Int32
	if err := fault.Init("transfer.after_payer_confirm", fault.WithKill(func() { n.Add(1) })); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Init("") })

	parallel := newTCCTest(t)
	parallel.p.UseTCCTransfer(&scriptedTCC{}, true)
	if _, err := parallel.p.CreateTransfer(context.Background(), tccInput("TRF04-PAR", 100)); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 0 {
		t.Fatalf("parallel hits = %d", n.Load())
	}

	serial := newTCCTest(t)
	serial.p.UseTCCTransfer(&scriptedTCC{}, false)
	got, err := serial.p.CreateTransfer(context.Background(), tccInput("TRF04-SER", 100))
	if err != nil || got.Status != transferStatusSuccess {
		t.Fatalf("serial = %+v err=%v", got, err)
	}
	if n.Load() != 1 {
		t.Fatalf("serial hits = %d", n.Load())
	}
}
