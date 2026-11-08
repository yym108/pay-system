package biz

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

// mapMerchants 按商户号返回配置。不在表里的商户返回 20004，与 Merchants.MerchantConfig 一致。
type mapMerchants struct {
	mu    sync.Mutex
	cfgs  map[string]MerchantConfig
	err   error
	calls []string
}

func (m *mapMerchants) MerchantConfig(_ context.Context, merchantID string) (MerchantConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, merchantID)
	if m.err != nil {
		return MerchantConfig{}, m.err
	}
	cfg, ok := m.cfgs[merchantID]
	if !ok {
		return MerchantConfig{}, errno.ErrMerchantInvalid
	}
	return cfg, nil
}

func (m *mapMerchants) FeeRateAt(context.Context, string, time.Time) (FeeRate, bool, error) {
	return FeeRate{}, false, errors.New("transfer must not read fee rates")
}

func (m *mapMerchants) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

// memTransfers 模拟 transfer_order 的两个唯一键。
type memTransfers struct {
	mu        sync.Mutex
	rows      []TransferOrder
	inserts   int
	reads     int
	failIns   error
	readErr   error
	finishErr error
	forceDup  bool
	finishes  int
	// beforeInsert 在插入检查唯一键之前调用，用来让并发请求都越过预读。
	beforeInsert func()
	// hijackDecide 在决策比较状态之前改这一行，用来模拟 CAS 影响 0 行。
	hijackDecide func(row *TransferOrder)
}

func (m *memTransfers) transferByMerchantOut(_ context.Context, merchantID, outTransferNo string) (TransferOrder, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	if m.readErr != nil {
		return TransferOrder{}, false, m.readErr
	}
	for _, row := range m.rows {
		if row.MerchantID == merchantID && row.OutTransferNo == outTransferNo {
			return row, true, nil
		}
	}
	return TransferOrder{}, false, nil
}

func (m *memTransfers) insertTransfer(_ context.Context, row TransferOrder) error {
	if m.beforeInsert != nil {
		m.beforeInsert()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inserts++
	if m.failIns != nil {
		return m.failIns
	}
	if m.forceDup {
		return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	}
	for _, ex := range m.rows {
		if ex.TransferNo == row.TransferNo || (ex.MerchantID == row.MerchantID && ex.OutTransferNo == row.OutTransferNo) {
			return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
		}
	}
	m.rows = append(m.rows, row)
	return nil
}

func (m *memTransfers) finishTransfer(_ context.Context, row TransferOrder, status, failCode, txnNo string, at time.Time, _ *idgen.Generator, _ clock.Clock) (TransferOrder, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finishes++
	if m.finishErr != nil {
		return TransferOrder{}, false, m.finishErr
	}
	for i := range m.rows {
		if m.rows[i].TransferNo != row.TransferNo {
			continue
		}
		if m.rows[i].Status != transferStatusTrying {
			return m.rows[i], false, nil
		}
		m.rows[i].Status = status
		m.rows[i].FailCode = failCode
		m.rows[i].TxnNo = txnNo
		m.rows[i].FinishedAt = at
		m.rows[i].UpdatedAt = at
		m.rows[i].Version++
		return m.rows[i], true, nil
	}
	return TransferOrder{}, false, errno.ErrInternal
}

func (m *memTransfers) decideTransfer(_ context.Context, row TransferOrder, status, failCode string, at time.Time) (TransferOrder, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.rows {
		if m.rows[i].TransferNo != row.TransferNo {
			continue
		}
		if m.hijackDecide != nil {
			m.hijackDecide(&m.rows[i])
		}
		if m.rows[i].Status != transferStatusTrying {
			return m.rows[i], false, nil
		}
		m.rows[i].Status = status
		m.rows[i].FailCode = failCode
		m.rows[i].UpdatedAt = at
		m.rows[i].Version++
		return m.rows[i], true, nil
	}
	return TransferOrder{}, false, errno.ErrInternal
}

func (m *memTransfers) finishTCCTransfer(ctx context.Context, row TransferOrder, fromStatus, status, failCode string, at time.Time, ids *idgen.Generator, _ clock.Clock) (TransferOrder, bool, error) {
	if ids != nil {
		// 与 outbox.Add 一样先发事件号。回拨失败时不改行。
		if _, err := ids.NewNo(ctx, "EV"); err != nil {
			return TransferOrder{}, false, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.rows {
		if m.rows[i].TransferNo != row.TransferNo {
			continue
		}
		if m.rows[i].Status != fromStatus {
			return m.rows[i], false, nil
		}
		m.rows[i].Status = status
		m.rows[i].FailCode = failCode
		m.rows[i].TxnNo = ""
		m.rows[i].FinishedAt = at
		m.rows[i].UpdatedAt = at
		m.rows[i].Version++
		return m.rows[i], true, nil
	}
	return TransferOrder{}, false, errno.ErrInternal
}

func (m *memTransfers) finishCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.finishes
}

func (m *memTransfers) snapshot() []TransferOrder {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TransferOrder, len(m.rows))
	copy(out, m.rows)
	return out
}

// fakeExecutor 记录被执行的转账单，并按 result 改写状态返回。
type fakeExecutor struct {
	mu     sync.Mutex
	got    []TransferOrder
	result func(TransferOrder) (TransferOrder, error)
}

func (f *fakeExecutor) ExecuteTransfer(_ context.Context, row TransferOrder) (TransferOrder, error) {
	f.mu.Lock()
	f.got = append(f.got, row)
	result := f.result
	f.mu.Unlock()
	if result == nil {
		return row, nil
	}
	return result(row)
}

func (f *fakeExecutor) calls() []TransferOrder {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]TransferOrder, len(f.got))
	copy(out, f.got)
	return out
}

func transferAt() time.Time {
	return feeAt(9, 30, 0, 123_456_789)
}

func transferMerchants() *mapMerchants {
	return &mapMerchants{cfgs: map[string]MerchantConfig{
		"M100001": {MerchantID: "M100001", Status: payStatusActive},
		"M100002": {MerchantID: "M100002", Status: payStatusActive},
		"M100003": {MerchantID: "M100003", Status: payStatusActive},
		"M100004": {MerchantID: "M100004", Status: "DISABLED"},
	}}
}

type transferTest struct {
	p         *Payments
	merchants *mapMerchants
	store     *memTransfers
	ids       *stubIDs
	clk       *clock.Fake
}

func newTransferTest(t *testing.T) transferTest {
	t.Helper()
	clk := &clock.Fake{}
	clk.Set(transferAt())
	merchants := transferMerchants()
	ids := &stubIDs{prefix: transferNoPrefix, nos: []string{
		"TF0000000000000000001",
		"TF0000000000000000002",
		"TF0000000000000000003",
	}}
	store := &memTransfers{}
	p := newPayments(merchants, ids, clk, "merchant-demo", nil)
	p.transfers = store
	return transferTest{p: p, merchants: merchants, store: store, ids: ids, clk: clk}
}

func transferInput(out, payee string, amount money.Amount) *CreateTransferInput {
	return &CreateTransferInput{
		MerchantID:      "M100001",
		OutTransferNo:   out,
		PayeeMerchantID: payee,
		Amount:          amount,
	}
}

// TRF-01 验收 4：M100001 → M100003 为 LOCAL，M100001 → M100002 为 TCC；新单为 TRYING，next_retry_at = now + 默认 10 秒。
func TestCreateTransferMode(t *testing.T) {
	tt := newTransferTest(t)
	ctx := context.Background()
	now := transferAt().Truncate(time.Millisecond)
	cases := []struct {
		out, payee string
		mode       string
		payeeShard int
	}{
		{"TR_LOCAL", "M100003", transferModeLocal, 1},
		{"TR_TCC", "M100002", transferModeTCC, 0},
	}
	for _, c := range cases {
		got, err := tt.p.CreateTransfer(ctx, transferInput(c.out, c.payee, 500))
		if err != nil {
			t.Fatalf("%s: %v", c.out, err)
		}
		if got.Mode != c.mode || got.PayerShard != 1 || got.PayeeShard != c.payeeShard {
			t.Fatalf("%s: mode=%s payer=%d payee=%d", c.out, got.Mode, got.PayerShard, got.PayeeShard)
		}
		if got.Status != transferStatusTrying || got.Currency != payCurrencyCNY || got.Amount != 500 ||
			got.MerchantID != "M100001" || got.PayeeMerchantID != c.payee || got.OutTransferNo != c.out {
			t.Fatalf("%s: row = %+v", c.out, got)
		}
		if !strings.HasPrefix(got.TransferNo, "TF") || got.FailCode != "" || got.TxnNo != "" ||
			!got.FinishedAt.IsZero() || got.Version != 0 || got.RetryCount != 0 || got.NotifyURL != "" {
			t.Fatalf("%s: defaults = %+v", c.out, got)
		}
		if !got.CreatedAt.Equal(now) || !got.UpdatedAt.Equal(now) || !got.NextRetryAt.Equal(now.Add(10*time.Second)) {
			t.Fatalf("%s: times created=%s next=%s", c.out, got.CreatedAt, got.NextRetryAt)
		}
	}
	if rows := tt.store.snapshot(); len(rows) != 2 || rows[0].Mode != transferModeLocal || rows[1].Mode != transferModeTCC {
		t.Fatalf("stored = %+v", rows)
	}
}

// TRF-01 规则 4：next_retry_at 取 UseTransfer 装入的 TRANSFER_RECOVER_DELAY；0 也照用。负数保留默认值。
func TestCreateTransferRecoverDelay(t *testing.T) {
	for _, c := range []struct {
		name  string
		delay time.Duration
		want  time.Duration
	}{
		{"custom", 3 * time.Second, 3 * time.Second},
		{"zero", 0, 0},
		{"negative keeps default", -time.Second, 10 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			tt := newTransferTest(t)
			tt.p.UseTransfer(c.delay, nil)
			got, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_D", "M100003", 1))
			if err != nil {
				t.Fatal(err)
			}
			if !got.NextRetryAt.Equal(got.CreatedAt.Add(c.want)) {
				t.Fatalf("next=%s created=%s", got.NextRetryAt, got.CreatedAt)
			}
		})
	}
}

// TRF-01 验收 2：相同 out_transfer_no、不同金额返回 10006；收款方或 notify_url 不同也是 10006。一致时返回已有单，不再执行。
func TestCreateTransferIdempotent(t *testing.T) {
	tt := newTransferTest(t)
	exec := &fakeExecutor{}
	tt.p.UseTransfer(10*time.Second, exec)
	ctx := context.Background()
	notify := "http://merchant-demo:9100/notify"
	first := transferInput("TR_SAME", "M100003", 500)
	first.NotifyURL = &notify
	created, err := tt.p.CreateTransfer(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if created.NotifyURL != notify {
		t.Fatalf("notify = %q", created.NotifyURL)
	}
	again, err := tt.p.CreateTransfer(ctx, first)
	if err != nil || again.TransferNo != created.TransferNo {
		t.Fatalf("again = %+v err=%v", again, err)
	}
	if n := len(exec.calls()); n != 1 {
		t.Fatalf("executor calls = %d, want 1", n)
	}
	other := "http://merchant-demo:9100/other"
	conflicts := map[string]*CreateTransferInput{
		"amount":       {MerchantID: "M100001", OutTransferNo: "TR_SAME", PayeeMerchantID: "M100003", Amount: 501, NotifyURL: &notify},
		"payee":        {MerchantID: "M100001", OutTransferNo: "TR_SAME", PayeeMerchantID: "M100002", Amount: 500, NotifyURL: &notify},
		"notify":       {MerchantID: "M100001", OutTransferNo: "TR_SAME", PayeeMerchantID: "M100003", Amount: 500, NotifyURL: &other},
		"notify unset": {MerchantID: "M100001", OutTransferNo: "TR_SAME", PayeeMerchantID: "M100003", Amount: 500},
	}
	for name, in := range conflicts {
		_, err := tt.p.CreateTransfer(ctx, in)
		if !errors.Is(err, errno.ErrIdempotentConflict) {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	// remark 不参与幂等比较。
	remark := "different"
	sameWithRemark := *first
	sameWithRemark.Remark = &remark
	if got, err := tt.p.CreateTransfer(ctx, &sameWithRemark); err != nil || got.TransferNo != created.TransferNo {
		t.Fatalf("remark differs: got=%+v err=%v", got, err)
	}
	if rows := tt.store.snapshot(); len(rows) != 1 || rows[0].Amount != 500 {
		t.Fatalf("rows = %+v", rows)
	}
	if n := len(exec.calls()); n != 1 {
		t.Fatalf("executor calls = %d after idempotent hits", n)
	}
}

// TRF-01 验收 3：转给自己、转给不存在或停用的商户返回 30008，不插入。
func TestCreateTransferPayeeInvalid(t *testing.T) {
	for _, payee := range []string{"M100001", "M100099", "M100004"} {
		t.Run(payee, func(t *testing.T) {
			tt := newTransferTest(t)
			_, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_BAD", payee, 100))
			assertErrno(t, err, errno.ErrPayeeInvalid)
			if rows := tt.store.snapshot(); len(rows) != 0 || tt.ids.calls != 0 {
				t.Fatalf("rows=%d ids=%d", len(rows), tt.ids.calls)
			}
		})
	}
	// 转给自己不读商户配置。
	tt := newTransferTest(t)
	_, _ = tt.p.CreateTransfer(context.Background(), transferInput("TR_SELF", "M100001", 100))
	if n := tt.merchants.callCount(); n != 0 {
		t.Fatalf("self transfer read merchant config %d times", n)
	}
}

// TRF-01 规则 3 先于规则 2：建单后收款商户被停用，相同参数重试仍返回已有单，不读商户配置；
// 参数不同仍是 10006，不是 30008。
func TestCreateTransferPayeeDisabledAfterCreate(t *testing.T) {
	tt := newTransferTest(t)
	exec := &fakeExecutor{}
	tt.p.UseTransfer(10*time.Second, exec)
	ctx := context.Background()
	in := transferInput("TR_LATE", "M100003", 800)
	created, err := tt.p.CreateTransfer(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	tt.merchants.mu.Lock()
	tt.merchants.cfgs["M100003"] = MerchantConfig{MerchantID: "M100003", Status: "DISABLED"}
	tt.merchants.mu.Unlock()
	reads := tt.merchants.callCount()

	again, err := tt.p.CreateTransfer(ctx, in)
	if err != nil || again.TransferNo != created.TransferNo {
		t.Fatalf("retry after payee disabled: got=%+v err=%v", again, err)
	}
	_, err = tt.p.CreateTransfer(ctx, transferInput("TR_LATE", "M100003", 801))
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if n := tt.merchants.callCount(); n != reads {
		t.Fatalf("idempotent hits read merchant config %d times", n-reads)
	}
	if n := len(exec.calls()); n != 1 {
		t.Fatalf("executor calls = %d, want 1", n)
	}
	// 新单号仍要校验收款方。
	_, err = tt.p.CreateTransfer(ctx, transferInput("TR_LATE_2", "M100003", 800))
	assertErrno(t, err, errno.ErrPayeeInvalid)
	if rows := tt.store.snapshot(); len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
}

// TRF-01 规则 3：1062 后再读命中时不做收款方校验，赢者提交后收款方被停用也返回已有单。
func TestCreateTransferDuplicateSkipsPayeeCheck(t *testing.T) {
	tt := newTransferTest(t)
	winner := TransferOrder{
		TransferNo: "TF_WINNER", MerchantID: "M100001", OutTransferNo: "TR_RACE", PayeeMerchantID: "M100003",
		Amount: 300, Currency: payCurrencyCNY, Mode: transferModeLocal, PayerShard: 1, PayeeShard: 1,
		Status: transferStatusTrying,
	}
	tt.store.beforeInsert = func() {
		tt.store.mu.Lock()
		if len(tt.store.rows) == 0 {
			tt.store.rows = append(tt.store.rows, winner)
		}
		tt.store.mu.Unlock()
		tt.merchants.mu.Lock()
		tt.merchants.cfgs["M100003"] = MerchantConfig{MerchantID: "M100003", Status: "DISABLED"}
		tt.merchants.mu.Unlock()
	}
	got, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_RACE", "M100003", 300))
	if err != nil || got.TransferNo != "TF_WINNER" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if n := tt.merchants.callCount(); n != 1 {
		t.Fatalf("merchant config reads = %d, want 1 (before insert only)", n)
	}
}

// TRF-01 规则 2：读商户配置失败不是 30008。超时原样返回，其他错误为 10003。
func TestCreateTransferPayeeLookupError(t *testing.T) {
	tt := newTransferTest(t)
	tt.merchants.err = context.DeadlineExceeded
	_, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_E", "M100002", 100))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	tt.merchants.err = errors.New("db down")
	_, err = tt.p.CreateTransfer(context.Background(), transferInput("TR_E", "M100002", 100))
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("err = %v", err)
	}
	if rows := tt.store.snapshot(); len(rows) != 0 {
		t.Fatalf("rows = %d", len(rows))
	}
}

// TRF-01 规则 1：参数不合法返回 10001，不读商户、不读写转账单。
func TestCreateTransferParams(t *testing.T) {
	long := strings.Repeat("a", 65)
	okNotify := "http://merchant-demo:9100/notify"
	badHost := "http://evil.example/notify"
	ftp := "ftp://merchant-demo/notify"
	empty := ""
	longRemark := strings.Repeat("备", 129)
	badUTF8 := string([]byte{0xff, 0xfe})
	cases := map[string]*CreateTransferInput{
		"nil":                   nil,
		"empty out":             transferInput("", "M100002", 1),
		"long out":              transferInput(long, "M100002", 1),
		"bad out char":          transferInput("TR 1", "M100002", 1),
		"zero amount":           transferInput("TR1", "M100002", 0),
		"negative amount":       transferInput("TR1", "M100002", -1),
		"amount over max":       transferInput("TR1", "M100002", money.MaxPayAmount+1),
		"empty payee":           transferInput("TR1", "", 1),
		"payee lower":           transferInput("TR1", "m100002", 1),
		"payee short":           transferInput("TR1", "M10002", 1),
		"payee long":            transferInput("TR1", "M1000022", 1),
		"payee letters":         transferInput("TR1", "M10000X", 1),
		"payer empty":           {OutTransferNo: "TR1", PayeeMerchantID: "M100002", Amount: 1},
		"payer bad":             {MerchantID: "X100001", OutTransferNo: "TR1", PayeeMerchantID: "M100002", Amount: 1},
		"remark too long":       {MerchantID: "M100001", OutTransferNo: "TR1", PayeeMerchantID: "M100002", Amount: 1, Remark: &longRemark},
		"remark bad utf8":       {MerchantID: "M100001", OutTransferNo: "TR1", PayeeMerchantID: "M100002", Amount: 1, Remark: &badUTF8},
		"notify host":           {MerchantID: "M100001", OutTransferNo: "TR1", PayeeMerchantID: "M100002", Amount: 1, NotifyURL: &badHost},
		"notify scheme":         {MerchantID: "M100001", OutTransferNo: "TR1", PayeeMerchantID: "M100002", Amount: 1, NotifyURL: &ftp},
		"notify explicit empty": {MerchantID: "M100001", OutTransferNo: "TR1", PayeeMerchantID: "M100002", Amount: 1, NotifyURL: &empty},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			tt := newTransferTest(t)
			_, err := tt.p.CreateTransfer(context.Background(), in)
			assertErrno(t, err, errno.ErrInvalidParam)
			if tt.merchants.callCount() != 0 || tt.store.reads != 0 || tt.store.inserts != 0 {
				t.Fatalf("touched storage: cfg=%d reads=%d inserts=%d", tt.merchants.callCount(), tt.store.reads, tt.store.inserts)
			}
		})
	}
	// 边界值合法：64 位单号、金额 1 和 10,000,000、128 字符备注、白名单内的 notify_url。
	tt := newTransferTest(t)
	maxRemark := strings.Repeat("备", 128)
	edge := []*CreateTransferInput{
		{MerchantID: "M100001", OutTransferNo: strings.Repeat("a", 64), PayeeMerchantID: "M100002", Amount: 1, Remark: &maxRemark},
		{MerchantID: "M100001", OutTransferNo: "A-b_9", PayeeMerchantID: "M100003", Amount: money.MaxPayAmount, NotifyURL: &okNotify, Remark: &empty},
	}
	for _, in := range edge {
		if _, err := tt.p.CreateTransfer(context.Background(), in); err != nil {
			t.Fatalf("%+v: %v", in, err)
		}
	}
	rows := tt.store.snapshot()
	if len(rows) != 2 || rows[0].Remark != maxRemark || rows[1].Remark != "" || rows[1].NotifyURL != okNotify {
		t.Fatalf("rows = %+v", rows)
	}
}

// 依赖缺失或 ctx 已结束：10003 或原样返回 ctx 错误，不写行。
func TestCreateTransferNotReady(t *testing.T) {
	var nilP *Payments
	_, err := nilP.CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	assertErrno(t, err, errno.ErrInternal)
	var nilApp *App
	_, err = nilApp.CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	assertErrno(t, err, errno.ErrInternal)
	_, err = NewApp(nil, nil).CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	assertErrno(t, err, errno.ErrInternal)

	noStore := newPayments(transferMerchants(), &stubIDs{prefix: transferNoPrefix}, &clock.Fake{}, "", nil)
	_, err = noStore.CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	assertErrno(t, err, errno.ErrInternal)
	// NewPayments 不给库时同样没有转账存储。
	_, err = NewPayments(transferMerchants(), &stubIDs{prefix: transferNoPrefix}, &clock.Fake{}, "", nil).
		CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	assertErrno(t, err, errno.ErrInternal)

	tt := newTransferTest(t)
	//nolint:staticcheck // 验证 nil ctx 不 panic。
	_, err = tt.p.CreateTransfer(nil, transferInput("TR1", "M100002", 1))
	assertErrno(t, err, errno.ErrInternal)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tt.p.CreateTransfer(ctx, transferInput("TR1", "M100002", 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if rows := tt.store.snapshot(); len(rows) != 0 {
		t.Fatalf("rows = %d", len(rows))
	}
}

// 发号失败：时钟回拨为 10004，其他为 10003。读写转账单失败为 10003。
func TestCreateTransferStoreErrors(t *testing.T) {
	tt := newTransferTest(t)
	tt.ids.err = idgen.ErrClockRollback
	_, err := tt.p.CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	assertErrno(t, err, errno.ErrUnavailable)
	tt.ids.err = errors.New("id broken")
	_, err = tt.p.CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("id err = %v", err)
	}
	tt.ids.err = nil

	tt.store.readErr = errors.New("read broken")
	_, err = tt.p.CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("read err = %v", err)
	}
	tt.store.readErr = nil

	tt.store.failIns = errors.New("insert broken")
	_, err = tt.p.CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("insert err = %v", err)
	}
	tt.store.failIns = nil

	// 1062 后读不到行（例如 transfer_no 撞号）不是幂等命中，返回 10003。
	tt.store.forceDup = true
	_, err = tt.p.CreateTransfer(context.Background(), transferInput("TR1", "M100002", 1))
	assertErrno(t, err, errno.ErrInternal)
	if rows := tt.store.snapshot(); len(rows) != 0 {
		t.Fatalf("rows = %d", len(rows))
	}
}

// TRF-01 规则 3：插入遇 1062 时再读，一致返回已有单（不执行），不一致返回 10006。
func TestCreateTransferDuplicateOnInsert(t *testing.T) {
	tt := newTransferTest(t)
	exec := &fakeExecutor{}
	tt.p.UseTransfer(10*time.Second, exec)
	winner := TransferOrder{
		TransferNo: "TF_WINNER", MerchantID: "M100001", OutTransferNo: "TR_RACE", PayeeMerchantID: "M100002",
		Amount: 300, Currency: payCurrencyCNY, Mode: transferModeTCC, PayerShard: 1, PayeeShard: 0,
		Status: transferStatusFailed, FailCode: transferFailInsufficient,
	}
	// 预读看不到赢者，插入前赢者已提交。
	tt.store.beforeInsert = func() {
		tt.store.mu.Lock()
		if len(tt.store.rows) == 0 {
			tt.store.rows = append(tt.store.rows, winner)
		}
		tt.store.mu.Unlock()
	}
	got, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_RACE", "M100002", 300))
	if err != nil || got.TransferNo != "TF_WINNER" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if n := len(exec.calls()); n != 0 {
		t.Fatalf("loser executed %d times", n)
	}
	_, err = tt.p.CreateTransfer(context.Background(), transferInput("TR_RACE", "M100002", 301))
	assertErrno(t, err, errno.ErrIdempotentConflict)

	// 1062 之后再读失败，返回 10003。
	tt2 := newTransferTest(t)
	tt2.store.beforeInsert = func() {
		tt2.store.mu.Lock()
		tt2.store.rows = append(tt2.store.rows, winner)
		tt2.store.readErr = errors.New("reread broken")
		tt2.store.mu.Unlock()
	}
	_, err = tt2.p.CreateTransfer(context.Background(), transferInput("TR_RACE", "M100002", 300))
	if !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("reread err = %v", err)
	}
}

// TRF-01 验收 1：100 个并发相同请求只生成 1 张转账单，全部返回同一 transfer_no，只执行一次。
func TestCreateTransferConcurrentSame(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(transferAt())
	ids, err := idgen.New(3, clk)
	if err != nil {
		t.Fatal(err)
	}
	store := &memTransfers{}
	const n = 100
	var arrived sync.WaitGroup
	arrived.Add(n)
	// 所有请求都越过预读后才放行插入，迫使 99 个走 1062 分支。
	store.beforeInsert = func() {
		arrived.Done()
		arrived.Wait()
	}
	exec := &fakeExecutor{}
	p := newPayments(transferMerchants(), ids, clk, "", nil)
	p.transfers = store
	p.UseTransfer(10*time.Second, exec)
	got := make([]TransferOrder, n)
	errs := make([]error, n)
	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			got[i], errs[i] = p.CreateTransfer(context.Background(), transferInput("TR_CONC", "M100002", 700))
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("req %d: %v", i, errs[i])
		}
		if got[i].TransferNo != got[0].TransferNo || got[i].TransferNo == "" {
			t.Fatalf("req %d transfer_no %s != %s", i, got[i].TransferNo, got[0].TransferNo)
		}
	}
	if rows := store.snapshot(); len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	if store.inserts != n {
		t.Fatalf("inserts = %d, want every request to reach insert", store.inserts)
	}
	if calls := exec.calls(); len(calls) != 1 || calls[0].TransferNo != got[0].TransferNo {
		t.Fatalf("executor calls = %d", len(calls))
	}
}

// TRF-01 规则 5、6：未装执行器返回 TRYING；执行到 FAILED + INSUFFICIENT_BALANCE 返回 40002 并携带转账单；
// 其他 FAILED、SUCCESS、处理中状态返回 nil error；幂等命中已失败的单返回 nil error。
func TestCreateTransferExecuteResponse(t *testing.T) {
	finished := transferAt().Add(time.Second).Truncate(time.Millisecond)
	set := func(status, failCode string) func(TransferOrder) (TransferOrder, error) {
		return func(row TransferOrder) (TransferOrder, error) {
			row.Status = status
			row.FailCode = failCode
			if status == transferStatusSuccess || status == transferStatusFailed {
				row.FinishedAt = finished
			}
			return row, nil
		}
	}
	cases := []struct {
		name     string
		result   func(TransferOrder) (TransferOrder, error)
		status   string
		wantErr  *errno.Error
		wantCall bool
	}{
		{name: "success", result: set(transferStatusSuccess, ""), status: transferStatusSuccess},
		{name: "insufficient", result: set(transferStatusFailed, transferFailInsufficient), status: transferStatusFailed, wantErr: errno.ErrInsufficientBalance},
		{name: "try timeout", result: set(transferStatusFailed, "TRY_TIMEOUT"), status: transferStatusFailed},
		{name: "account invalid", result: set(transferStatusFailed, "ACCOUNT_INVALID"), status: transferStatusFailed},
		{name: "trying", result: set(transferStatusTrying, ""), status: transferStatusTrying},
		{name: "confirming", result: set("CONFIRMING", ""), status: "CONFIRMING"},
		{name: "cancelling", result: set("CANCELLING", "TRY_TIMEOUT"), status: "CANCELLING"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tt := newTransferTest(t)
			exec := &fakeExecutor{result: c.result}
			tt.p.UseTransfer(10*time.Second, exec)
			got, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_X", "M100002", 100))
			if c.wantErr != nil {
				assertErrno(t, err, c.wantErr)
			} else if err != nil {
				t.Fatal(err)
			}
			if got.Status != c.status || got.TransferNo != "TF0000000000000000001" {
				t.Fatalf("got = %+v", got)
			}
			calls := exec.calls()
			if len(calls) != 1 || calls[0].Status != transferStatusTrying || calls[0].Mode != transferModeTCC {
				t.Fatalf("executor got %+v", calls)
			}
			// 幂等命中返回库里的行，code 0，不再执行。memTransfers 存的是插入时的 TRYING；改成执行后的状态模拟已推进。
			tt.store.mu.Lock()
			tt.store.rows[0] = got
			tt.store.mu.Unlock()
			again, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_X", "M100002", 100))
			if err != nil || again.Status != c.status {
				t.Fatalf("idempotent again=%+v err=%v", again, err)
			}
			if n := len(exec.calls()); n != 1 {
				t.Fatalf("executor calls = %d", n)
			}
		})
	}

	// 未装执行器：返回刚插入的 TRYING。
	tt := newTransferTest(t)
	got, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_NOEXEC", "M100003", 100))
	if err != nil || got.Status != transferStatusTrying || got.Mode != transferModeLocal {
		t.Fatalf("no executor got=%+v err=%v", got, err)
	}

	// 执行器返回错误：转账单已提交，错误按 callErr 映射；超时原样返回。
	for _, c := range []struct {
		err  error
		want error
	}{
		{errors.New("broken"), errno.ErrInternal},
		{context.DeadlineExceeded, context.DeadlineExceeded},
		{errno.ErrUnavailable, errno.ErrUnavailable},
	} {
		tt := newTransferTest(t)
		tt.p.UseTransfer(10*time.Second, &fakeExecutor{result: func(TransferOrder) (TransferOrder, error) {
			return TransferOrder{}, c.err
		}})
		_, err := tt.p.CreateTransfer(context.Background(), transferInput("TR_ERR", "M100002", 100))
		if !errors.Is(err, c.want) {
			t.Fatalf("exec err %v → %v", c.err, err)
		}
		if rows := tt.store.snapshot(); len(rows) != 1 || rows[0].Status != transferStatusTrying {
			t.Fatalf("rows = %+v", rows)
		}
	}
}

// UseTransfer 对 nil 接收者无效果，不 panic。
func TestUseTransferNil(t *testing.T) {
	var p *Payments
	p.UseTransfer(time.Second, &fakeExecutor{})
}

// App.CreateTransfer 转给 Payments。
func TestAppCreateTransfer(t *testing.T) {
	tt := newTransferTest(t)
	got, err := NewApp(nil, tt.p).CreateTransfer(context.Background(), transferInput("TR_APP", "M100002", 9))
	if err != nil || got.Amount != 9 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}
