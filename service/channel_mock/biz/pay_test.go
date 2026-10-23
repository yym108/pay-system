package biz

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/service/channel_mock/dal"
)

var shanghaiTZ = time.FixedZone("Asia/Shanghai", 8*3600)

// baseTime 带纳秒，用来确认写库前截到毫秒。
var baseTime = time.Date(2026, 10, 23, 10, 0, 0, 123456789, shanghaiTZ)

type env struct {
	store *memStore
	ids   *seqIDs
	clk   *clock.Fake
	sched *manualSched
	hook  *hookRecorder
	svc   *Service
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		store: newMemStore(),
		ids:   &seqIDs{},
		clk:   &clock.Fake{},
		sched: &manualSched{},
		hook:  &hookRecorder{},
	}
	e.clk.Set(baseTime)
	e.svc = New(e.store, e.ids, e.clk, e.sched, Options{
		DelayMinMS: 200,
		DelayMaxMS: 2000,
		OnFinished: e.hook.fn,
	})
	return e
}

func payReq(reqNo string, amount money.Amount) PayRequest {
	return PayRequest{
		ReqNo:     reqNo,
		Amount:    amount,
		Subject:   "demo",
		NotifyURL: "http://gateway:8080/callback/mockpay",
	}
}

// CH-01 规则 3：新交易 PROCESSING，返回 MC 流水号，scenario 按金额，created_at 取注入时钟并截到毫秒。
func TestPayAcceptsNewTxn(t *testing.T) {
	e := newEnv(t)
	res, err := e.svc.Pay(context.Background(), payReq("PA001", 10000))
	if err != nil {
		t.Fatal(err)
	}
	if res.Result != ResultOK || res.Reason != "" || res.Status != dal.StatusProcessing {
		t.Fatalf("result = %+v", res)
	}
	if !strings.HasPrefix(res.ChannelTxnNo, "MC") {
		t.Fatalf("channel_txn_no = %q", res.ChannelTxnNo)
	}
	row, ok := e.store.row("PA001")
	if !ok {
		t.Fatal("row not inserted")
	}
	wantCreated := baseTime.Truncate(time.Millisecond)
	if row.TxnType != dal.TxnTypePay || row.Amount != 10000 || row.Status != dal.StatusProcessing ||
		row.Scenario != "NORMAL" || row.NotifyURL != "http://gateway:8080/callback/mockpay" ||
		row.ChannelTxnNo != res.ChannelTxnNo || !row.CreatedAt.Equal(wantCreated) ||
		!row.FinishedAt.IsZero() || row.BillDate != "" {
		t.Fatalf("row = %+v", row)
	}
	dues := e.sched.dues()
	if len(dues) != 1 || !dues[0].Equal(wantCreated.Add(Delay("PA001", 200, 2000))) {
		t.Fatalf("dues = %v", dues)
	}
	if len(e.hook.list()) != 0 {
		t.Fatal("hook called before finish")
	}
}

// CH-01 验收 1：同一 req_no 重复受理返回同一 channel_txn_no，状态是当前状态，不重复调度。
func TestPayIdempotentReturnsSameTxnNo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	first, err := e.svc.Pay(ctx, payReq("PA001", 10000))
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.svc.Pay(ctx, payReq("PA001", 10000))
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("second = %+v, first = %+v", second, first)
	}
	if e.store.count() != 1 || e.sched.len() != 1 {
		t.Fatalf("rows = %d, scheduled = %d", e.store.count(), e.sched.len())
	}
	// 先 INSERT 再读：第二次也走了 INSERT，靠唯一键挡住。
	if e.store.inserts != 2 {
		t.Fatalf("inserts = %d, want 2 (insert first, no check-then-insert)", e.store.inserts)
	}

	e.clk.Advance(2 * time.Second)
	e.sched.RunDue(e.clk.Now())
	third, err := e.svc.Pay(ctx, payReq("PA001", 10000))
	if err != nil {
		t.Fatal(err)
	}
	if third.Result != ResultOK || third.ChannelTxnNo != first.ChannelTxnNo || third.Status != dal.StatusSuccess {
		t.Fatalf("after finish = %+v", third)
	}
}

func TestPayConcurrentSameReqNo(t *testing.T) {
	e := newEnv(t)
	const n = 50
	results := make([]PayResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = e.svc.Pay(context.Background(), payReq("PA_CONC", 10000))
		}(i)
	}
	close(start)
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if results[i].Result != ResultOK || results[i].ChannelTxnNo != results[0].ChannelTxnNo {
			t.Fatalf("goroutine %d: %+v vs %+v", i, results[i], results[0])
		}
	}
	if e.store.count() != 1 || e.sched.len() != 1 {
		t.Fatalf("rows = %d, scheduled = %d", e.store.count(), e.sched.len())
	}
}

// CH-01 规则 2：金额不一致 → REJECTED / PARAM_CONFLICT，不带流水号，不改原行。
func TestPayAmountConflict(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.svc.Pay(ctx, payReq("PA001", 10000)); err != nil {
		t.Fatal(err)
	}
	res, err := e.svc.Pay(ctx, payReq("PA001", 10001))
	if err != nil {
		t.Fatal(err)
	}
	want := PayResult{Result: ResultRejected, Reason: ReasonParamConflict, Status: dal.StatusProcessing}
	if res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
	row, _ := e.store.row("PA001")
	if row.Amount != 10000 || row.Scenario != "NORMAL" {
		t.Fatalf("row changed: %+v", row)
	}
	if e.sched.len() != 1 {
		t.Fatalf("scheduled = %d", e.sched.len())
	}
}

// CH-01 规则 2 的优先级：已 CLOSED 时不论金额是否一致都返回 ORDER_CLOSED。
func TestPayClosedRow(t *testing.T) {
	e := newEnv(t)
	e.store.put(dal.Txn{
		ChannelTxnNo: "MC_CLOSED", TxnType: dal.TxnTypePay, ReqNo: "PA_CLOSED",
		Amount: 10000, Status: dal.StatusClosed, Scenario: "NORMAL", CreatedAt: baseTime,
	})
	want := PayResult{Result: ResultRejected, Reason: ReasonOrderClosed, Status: dal.StatusClosed}
	for _, amount := range []money.Amount{10000, 20000} {
		res, err := e.svc.Pay(context.Background(), payReq("PA_CLOSED", amount))
		if err != nil {
			t.Fatal(err)
		}
		if res != want {
			t.Fatalf("amount %d: result = %+v, want %+v", amount, res, want)
		}
	}
	if e.sched.len() != 0 {
		t.Fatal("closed row was scheduled")
	}
	row, _ := e.store.row("PA_CLOSED")
	if row.Status != dal.StatusClosed || row.Amount != 10000 {
		t.Fatalf("row changed: %+v", row)
	}
}

// 已确认的澄清：参数非法返回 ErrInvalidRequest（handler 映射为 400），不写库、不取号。
func TestPayRejectsInvalidRequest(t *testing.T) {
	long := "http://gateway/" + strings.Repeat("a", 512-len("http://gateway/")+1)
	cases := []struct {
		name string
		mod  func(*PayRequest)
	}{
		{"empty req_no", func(r *PayRequest) { r.ReqNo = "" }},
		{"req_no 33 runes", func(r *PayRequest) { r.ReqNo = strings.Repeat("单", 33) }},
		{"req_no invalid utf8", func(r *PayRequest) { r.ReqNo = "PA\xff" }},
		// JSON 里的孤立代理（如 \udc00）被 encoding/json 解码成 U+FFFD，按非法处理。
		{"req_no U+FFFD", func(r *PayRequest) { r.ReqNo = "PA\uFFFD" }},
		{"amount zero", func(r *PayRequest) { r.Amount = 0 }},
		{"amount negative", func(r *PayRequest) { r.Amount = -100 }},
		{"empty subject", func(r *PayRequest) { r.Subject = "" }},
		{"empty notify_url", func(r *PayRequest) { r.NotifyURL = "" }},
		{"relative notify_url", func(r *PayRequest) { r.NotifyURL = "/callback" }},
		{"ftp notify_url", func(r *PayRequest) { r.NotifyURL = "ftp://gateway/callback" }},
		{"notify_url without host", func(r *PayRequest) { r.NotifyURL = "http:///callback" }},
		{"notify_url unparsable", func(r *PayRequest) { r.NotifyURL = "http://gate way/cb" }},
		{"notify_url 513 runes", func(r *PayRequest) { r.NotifyURL = long }},
		{"notify_url invalid utf8", func(r *PayRequest) { r.NotifyURL = "http://gateway/\xff" }},
		{"notify_url U+FFFD", func(r *PayRequest) { r.NotifyURL = "http://gateway/\uFFFD" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			req := payReq("PA001", 10000)
			c.mod(&req)
			_, err := e.svc.Pay(context.Background(), req)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("err = %v, want ErrInvalidRequest", err)
			}
			if e.store.inserts != 0 || e.store.gets != 0 || e.ids.n != 0 || e.sched.len() != 0 {
				t.Fatal("invalid request touched store, id generator or scheduler")
			}
		})
	}
}

func TestPayAcceptsBoundaryValues(t *testing.T) {
	e := newEnv(t)
	req := payReq(strings.Repeat("单", 32), 1)
	req.NotifyURL = "https://gateway/" + strings.Repeat("a", 512-len("https://gateway/"))
	res, err := e.svc.Pay(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result != ResultOK {
		t.Fatalf("result = %+v", res)
	}
	row, _ := e.store.row(req.ReqNo)
	if row.Scenario != "FAIL" {
		t.Fatalf("amount 1 scenario = %s, want FAIL", row.Scenario)
	}
}

// 存储或取号失败是内部错误（handler 映射为 500），不是参数错误，也不调度。
func TestPayInternalErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("id generator", func(t *testing.T) {
		e := newEnv(t)
		e.ids.err = errStore
		_, err := e.svc.Pay(ctx, payReq("PA001", 10000))
		assertInternal(t, err)
		if e.store.inserts != 0 {
			t.Fatal("inserted without channel_txn_no")
		}
	})
	t.Run("insert", func(t *testing.T) {
		e := newEnv(t)
		e.store.insertErr = errStore
		_, err := e.svc.Pay(ctx, payReq("PA001", 10000))
		assertInternal(t, err)
		if !errors.Is(err, errStore) {
			t.Fatalf("err = %v, want the insert error", err)
		}
		if e.sched.len() != 0 {
			t.Fatal("scheduled after failed insert")
		}
		// 非 1062 错误后按 (PAY, req_no) 再读一次，确认本次没有落库。
		if e.store.gets != 1 {
			t.Fatalf("gets = %d, want 1 re-read", e.store.gets)
		}
	})
	t.Run("insert error and re-read error", func(t *testing.T) {
		e := newEnv(t)
		e.store.insertCommitErr = errStore
		e.store.getErr = errors.New("read down")
		_, err := e.svc.Pay(ctx, payReq("PA001", 10000))
		if !errors.Is(err, errStore) {
			t.Fatalf("err = %v, want the insert error", err)
		}
		if e.sched.len() != 0 {
			t.Fatal("scheduled without confirming the row")
		}
	})
	t.Run("insert error and row from another request", func(t *testing.T) {
		e := newEnv(t)
		e.store.put(dal.Txn{ChannelTxnNo: "MC_OTHER", TxnType: dal.TxnTypePay, ReqNo: "PA001",
			Amount: 10000, Status: dal.StatusProcessing, Scenario: "NORMAL", CreatedAt: baseTime})
		e.store.insertErr = errStore
		_, err := e.svc.Pay(ctx, payReq("PA001", 10000))
		if !errors.Is(err, errStore) {
			t.Fatalf("err = %v, want the insert error", err)
		}
		if e.sched.len() != 0 {
			t.Fatal("scheduled a row this request did not write")
		}
	})
	t.Run("get after duplicate", func(t *testing.T) {
		e := newEnv(t)
		if _, err := e.svc.Pay(ctx, payReq("PA001", 10000)); err != nil {
			t.Fatal(err)
		}
		e.store.getErr = errStore
		_, err := e.svc.Pay(ctx, payReq("PA001", 10000))
		assertInternal(t, err)
	})
	t.Run("duplicate but row missing", func(t *testing.T) {
		e := newEnv(t)
		if _, err := e.svc.Pay(ctx, payReq("PA001", 10000)); err != nil {
			t.Fatal(err)
		}
		e.store.hideOnGet = true
		_, err := e.svc.Pay(ctx, payReq("PA001", 10000))
		assertInternal(t, err)
	})
	t.Run("canceled context", func(t *testing.T) {
		e := newEnv(t)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := e.svc.Pay(cctx, payReq("PA001", 10000))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
		if e.store.inserts != 0 {
			t.Fatal("inserted with canceled context")
		}
	})
}

// 审查 P2：INSERT 已提交但调用方拿到非 1062 错误（提交结果不确定、请求被取消）时，
// 再读到本次写入的行（channel_txn_no 一致）就调度它并返回 OK，不会停在 PROCESSING 直到重启。
func TestPayInsertErrorAfterCommitSchedules(t *testing.T) {
	cases := []struct {
		name  string
		setup func(e *env) context.Context
	}{
		{"commit result unknown", func(e *env) context.Context {
			e.store.insertCommitErr = errStore
			return context.Background()
		}},
		{"request canceled after commit", func(e *env) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			e.store.afterInsert = cancel
			e.store.insertCommitErr = context.Canceled
			return ctx
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			ctx := c.setup(e)
			res, err := e.svc.Pay(ctx, payReq("PA001", 10000))
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			row, ok := e.store.row("PA001")
			if !ok {
				t.Fatal("row missing")
			}
			want := PayResult{Result: ResultOK, ChannelTxnNo: row.ChannelTxnNo, Status: dal.StatusProcessing}
			if res != want {
				t.Fatalf("result = %+v, want %+v", res, want)
			}
			dues := e.sched.dues()
			if len(dues) != 1 || !dues[0].Equal(row.CreatedAt.Add(Delay("PA001", 200, 2000))) {
				t.Fatalf("dues = %v", dues)
			}
			e.clk.Advance(2 * time.Second)
			e.sched.RunDue(e.clk.Now())
			if row, _ := e.store.row("PA001"); row.Status != dal.StatusSuccess {
				t.Fatalf("row = %+v", row)
			}
			if calls := e.hook.list(); len(calls) != 1 || calls[0].ID != row.ID {
				t.Fatalf("hook calls = %+v", calls)
			}
		})
	}
}

func assertInternal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("err = nil")
	}
	if errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("internal error reported as invalid request: %v", err)
	}
}
