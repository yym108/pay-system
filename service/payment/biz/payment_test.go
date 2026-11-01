package biz

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

func payInput(notify *string, expire *int32) *CreatePaymentInput {
	return &CreatePaymentInput{
		MerchantID:    "M100002",
		OutTradeNo:    "T20261028001",
		Amount:        10000,
		Subject:       "demo",
		NotifyURL:     notify,
		ExpireMinutes: expire,
	}
}

func payAt() time.Time {
	return feeAt(10, 0, 0, 0)
}

func newPayTest(t *testing.T, hosts string) (*Payments, *fakeMerchants, *memOrders, *stubIDs, *clock.Fake) {
	t.Helper()
	clk := &clock.Fake{}
	clk.Set(payAt())
	merchants := &fakeMerchants{
		cfg:   MerchantConfig{MerchantID: "M100002", Status: "ACTIVE", DefaultNotifyURL: "http://merchant-demo/default"},
		rate:  FeeRate{MerchantID: "M100002", RateBP: 60, EffectiveFrom: payAt().Add(-time.Hour)},
		found: true,
	}
	orders := &memOrders{}
	ids := &stubIDs{prefix: "PA", nos: []string{"PA0000000000000000001"}}
	return newPayments(merchants, ids, clk, hosts, orders), merchants, orders, ids, clk
}

// PAY-02 验收 4：费率 60bp 的商户下单 10000，fee_amount = 60。
func TestCreatePaymentFeeAmount(t *testing.T) {
	p, merchants, orders, _, clk := newPayTest(t, "merchant-demo")
	got, err := p.CreatePayment(context.Background(), payInput(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if got.FeeAmount != 60 || got.Amount != 10000 || got.Status != "INIT" || got.PayNo != "PA0000000000000000001" {
		t.Fatalf("order = %+v", got)
	}
	if got.RefundedAmount != 0 || got.Version != 0 || got.Channel != "" || !got.PaidAt.IsZero() ||
		!got.ClosedAt.IsZero() || got.FailCode != "" || got.FailMsg != "" {
		t.Fatalf("new row defaults = %+v", got)
	}
	if !got.CreatedAt.Equal(clk.Now()) || !got.ExpireAt.Equal(clk.Now().Add(15*time.Minute)) || !got.UpdatedAt.Equal(got.CreatedAt) {
		t.Fatalf("times created=%s expire=%s updated=%s", got.CreatedAt, got.ExpireAt, got.UpdatedAt)
	}
	if got.NotifyURL != "" || got.Currency != "CNY" {
		t.Fatalf("defaults currency=%s notify=%q", got.Currency, got.NotifyURL)
	}
	if merchants.rateCalls != 1 || merchants.lastAt.IsZero() || !merchants.lastAt.Equal(got.CreatedAt) {
		t.Fatalf("fee at %s calls=%d", merchants.lastAt, merchants.rateCalls)
	}
	if len(orders.rows) != 1 || orders.rows[0].FeeAmount != 60 {
		t.Fatalf("stored = %+v", orders.rows)
	}
	// 商户列上的费率不是生效档。假配置没有 fee_rate_bp，手续费只来自 FeeRateAt。
	if merchants.rate.RateBP != 60 {
		t.Fatal("test setup drifted")
	}
}

// PAY-02 验收 2：相同 out_trade_no、不同金额返回 10006。
func TestCreatePaymentIdempotentAmountConflict(t *testing.T) {
	p, _, orders, ids, _ := newPayTest(t, "merchant-demo")
	ctx := context.Background()
	first, err := p.CreatePayment(ctx, payInput(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	second := payInput(nil, nil)
	second.Amount = 10001
	_, err = p.CreatePayment(ctx, second)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	if len(orders.rows) != 1 || orders.rows[0].Amount != 10000 || orders.rows[0].PayNo != first.PayNo {
		t.Fatalf("row changed: %+v", orders.rows)
	}
	if ids.calls != 2 {
		t.Fatalf("id calls = %d", ids.calls)
	}
}

// PAY-02 验收 3：expire_minutes 为 0 和 121、notify_url 不在白名单，返回 10001。
func TestCreatePaymentExpireAndNotifyURL(t *testing.T) {
	p, merchants, orders, _, _ := newPayTest(t, "merchant-demo")
	ctx := context.Background()
	zero := int32(0)
	tooBig := int32(121)
	bad := "http://evil.example/notify"
	for _, in := range []*CreatePaymentInput{
		payInput(nil, &zero),
		payInput(nil, &tooBig),
		payInput(&bad, nil),
	} {
		_, err := p.CreatePayment(ctx, in)
		assertErrno(t, err, errno.ErrInvalidParam)
	}
	if merchants.configCalls != 0 || len(orders.rows) != 0 {
		t.Fatalf("invalid request touched storage config=%d rows=%d", merchants.configCalls, len(orders.rows))
	}
}

func TestCreatePaymentParams(t *testing.T) {
	p, _, _, _, _ := newPayTest(t, "merchant-demo, api.example ")
	ctx := context.Background()
	okURL := "https://API.Example:8443/hook"
	var cnyEmpty string
	cny := "CNY"
	lower := "cny"
	minutes := int32(120)
	one := int32(1)
	neg := int32(-1)
	cases := []struct {
		name string
		in   *CreatePaymentInput
		want *errno.Error
	}{
		{name: "nil", in: nil, want: errno.ErrInvalidParam},
		{name: "empty out", in: mutate(payInput(nil, nil), func(in *CreatePaymentInput) { in.OutTradeNo = "" }), want: errno.ErrInvalidParam},
		{name: "long out", in: mutate(payInput(nil, nil), func(in *CreatePaymentInput) { in.OutTradeNo = strings.Repeat("a", 65) }), want: errno.ErrInvalidParam},
		{name: "bad out", in: mutate(payInput(nil, nil), func(in *CreatePaymentInput) { in.OutTradeNo = "a b" }), want: errno.ErrInvalidParam},
		{name: "amount 0", in: mutate(payInput(nil, nil), func(in *CreatePaymentInput) { in.Amount = 0 }), want: errno.ErrInvalidParam},
		{name: "amount big", in: mutate(payInput(nil, nil), func(in *CreatePaymentInput) { in.Amount = money.MaxPayAmount + 1 }), want: errno.ErrInvalidParam},
		{name: "empty subject", in: mutate(payInput(nil, nil), func(in *CreatePaymentInput) { in.Subject = "" }), want: errno.ErrInvalidParam},
		{name: "long subject", in: mutate(payInput(nil, nil), func(in *CreatePaymentInput) { in.Subject = strings.Repeat("单", 129) }), want: errno.ErrInvalidParam},
		{name: "lower currency", in: mutate(payInput(nil, nil), func(in *CreatePaymentInput) { in.Currency = &lower }), want: errno.ErrInvalidParam},
		{name: "negative expire", in: payInput(nil, &neg), want: errno.ErrInvalidParam},
		{name: "ftp", in: payInput(strPtr("ftp://merchant-demo/a"), nil), want: errno.ErrInvalidParam},
		{name: "empty host", in: payInput(strPtr("http:///a"), nil), want: errno.ErrInvalidParam},
		{name: "too long url", in: payInput(strPtr("http://merchant-demo/"+strings.Repeat("a", 500)), nil), want: errno.ErrInvalidParam},
		{name: "empty url", in: payInput(strPtr(""), nil), want: errno.ErrInvalidParam},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.CreatePayment(ctx, tc.in)
			assertErrno(t, err, tc.want)
		})
	}
	spaced := payInput(nil, &one)
	spaced.Subject = "  demo  "
	spaced.OutTradeNo = strings.Repeat("a", 64)
	spaced.Currency = &cnyEmpty
	got, err := p.CreatePayment(ctx, spaced)
	if err != nil || got.Subject != "  demo  " || got.Currency != "CNY" || !got.ExpireAt.Equal(payAt().Add(time.Minute)) {
		t.Fatalf("edge = %+v err=%v", got, err)
	}
	upper := payInput(&okURL, &minutes)
	upper.Currency = &cny
	upper.OutTradeNo = "T_ok-1"
	got, err = p.CreatePayment(ctx, upper)
	if err != nil || got.NotifyURL != okURL || got.Currency != "CNY" || !got.ExpireAt.Equal(payAt().Add(120*time.Minute)) {
		t.Fatalf("allow = %+v err=%v", got, err)
	}
}

func TestCreatePaymentNotifyURLStoredAsGiven(t *testing.T) {
	p, _, orders, _, _ := newPayTest(t, "merchant-demo")
	ctx := context.Background()
	raw := "http://alice@merchant-demo/default"
	got, err := p.CreatePayment(ctx, payInput(&raw, nil))
	if err != nil {
		t.Fatal(err)
	}
	if got.NotifyURL != raw || orders.rows[0].NotifyURL != raw {
		t.Fatalf("rewrote notify url: %q", got.NotifyURL)
	}
	same := "http://merchant-demo/default"
	got, err = p.CreatePayment(ctx, mutate(payInput(&same, nil), func(in *CreatePaymentInput) { in.OutTradeNo = "T-default" }))
	if err != nil || got.NotifyURL != same {
		t.Fatalf("default-equal url = %q err=%v", got.NotifyURL, err)
	}
	p2, _, orders2, _, _ := newPayTest(t, "")
	_, err = p2.CreatePayment(ctx, payInput(&raw, nil))
	assertErrno(t, err, errno.ErrInvalidParam)
	if len(orders2.rows) != 0 {
		t.Fatal("empty allow list inserted")
	}
	got, err = p2.CreatePayment(ctx, payInput(nil, nil))
	if err != nil || got.NotifyURL != "" {
		t.Fatalf("omitted = %+v err=%v", got, err)
	}
}

func TestCreatePaymentMerchantAndMissingRate(t *testing.T) {
	ctx := context.Background()
	p, merchants, orders, ids, _ := newPayTest(t, "merchant-demo")
	for _, status := range []string{"DISABLED", "active"} {
		merchants.cfg.Status = status
		_, err := p.CreatePayment(ctx, payInput(nil, nil))
		assertErrno(t, err, errno.ErrMerchantInvalid)
	}
	merchants.cfg.Status = "ACTIVE"
	missing := payInput(nil, nil)
	missing.MerchantID = ""
	_, err := p.CreatePayment(ctx, missing)
	assertErrno(t, err, errno.ErrMerchantInvalid)
	merchants.cfgErr = errno.ErrMerchantInvalid
	_, err = p.CreatePayment(ctx, payInput(nil, nil))
	assertErrno(t, err, errno.ErrMerchantInvalid)
	merchants.cfgErr = nil
	merchants.found = false
	_, err = p.CreatePayment(ctx, payInput(nil, nil))
	assertErrno(t, err, errno.ErrInternal)
	if len(orders.rows) != 0 || ids.calls != 0 {
		t.Fatalf("inserted rows=%d ids=%d", len(orders.rows), ids.calls)
	}
	merchants.found = true
	merchants.rate.RateBP = 10001
	_, err = p.CreatePayment(ctx, payInput(nil, nil))
	assertErrno(t, err, errno.ErrInternal)
	if len(orders.rows) != 0 {
		t.Fatal("bad rate inserted")
	}
}

func TestCreatePaymentIdempotentHitDoesNotUpdate(t *testing.T) {
	p, _, orders, _, clk := newPayTest(t, "merchant-demo")
	ctx := context.Background()
	url := "http://merchant-demo/a"
	minutes := int32(15)
	first, err := p.CreatePayment(ctx, payInput(&url, &minutes))
	if err != nil {
		t.Fatal(err)
	}
	orders.rows[0].Status = "PAYING"
	orders.rows[0].FeeAmount = 1
	clk.Advance(time.Hour)
	again := payInput(&url, intPtr(30))
	got, err := p.CreatePayment(ctx, again)
	if err != nil {
		t.Fatal(err)
	}
	if got.PayNo != first.PayNo || got.Status != "PAYING" || got.FeeAmount != 1 || !got.ExpireAt.Equal(first.ExpireAt) {
		t.Fatalf("idempotent = %+v", got)
	}
	if len(orders.rows) != 1 || orders.updates != 0 {
		t.Fatalf("rows=%d updates=%d", len(orders.rows), orders.updates)
	}
	changed := payInput(&url, &minutes)
	changed.Subject = "other"
	_, err = p.CreatePayment(ctx, changed)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	changed = payInput(nil, &minutes)
	_, err = p.CreatePayment(ctx, changed)
	assertErrno(t, err, errno.ErrIdempotentConflict)
	changed = payInput(&url, &minutes)
	changed.Currency = strPtr("CNY")
	got, err = p.CreatePayment(ctx, changed)
	if err != nil || got.PayNo != first.PayNo {
		t.Fatalf("currency fill = %+v err=%v", got, err)
	}
}

func TestCreatePaymentDuplicateWithoutRow(t *testing.T) {
	p, _, orders, _, _ := newPayTest(t, "merchant-demo")
	orders.forceDup = true
	_, err := p.CreatePayment(context.Background(), payInput(nil, nil))
	assertErrno(t, err, errno.ErrInternal)
	if len(orders.rows) != 0 {
		t.Fatal("phantom row")
	}
}

func TestCreatePaymentClockRollback(t *testing.T) {
	p, _, orders, ids, _ := newPayTest(t, "merchant-demo")
	ids.err = idgen.ErrClockRollback
	_, err := p.CreatePayment(context.Background(), payInput(nil, nil))
	assertErrno(t, err, errno.ErrUnavailable)
	if len(orders.rows) != 0 {
		t.Fatal("rollback inserted")
	}
	ids.err = errors.New("idgen broken")
	_, err = p.CreatePayment(context.Background(), payInput(nil, nil))
	assertErrno(t, err, errno.ErrInternal)
	if err.Error() != errno.ErrInternal.Error() {
		t.Fatal("idgen detail leaked")
	}
}

func TestCreatePaymentGuards(t *testing.T) {
	ctx := context.Background()
	var nilP *Payments
	_, err := nilP.CreatePayment(ctx, payInput(nil, nil))
	assertErrno(t, err, errno.ErrInternal)
	p, _, _, _, _ := newPayTest(t, "merchant-demo")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = p.CreatePayment(canceled, payInput(nil, nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	_, err = p.CreatePayment(canceled, payInput(strPtr("ftp://merchant-demo/a"), nil))
	assertErrno(t, err, errno.ErrInvalidParam)
	p.orders = nil
	_, err = p.CreatePayment(ctx, payInput(nil, nil))
	assertErrno(t, err, errno.ErrInternal)
}

type fakeMerchants struct {
	cfg         MerchantConfig
	cfgErr      error
	rate        FeeRate
	found       bool
	rateErr     error
	configCalls int
	rateCalls   int
	lastAt      time.Time
}

func (f *fakeMerchants) MerchantConfig(_ context.Context, merchantID string) (MerchantConfig, error) {
	f.configCalls++
	if merchantID == "" || merchantID != f.cfg.MerchantID {
		return MerchantConfig{}, errno.ErrMerchantInvalid
	}
	if f.cfgErr != nil {
		return MerchantConfig{}, f.cfgErr
	}
	return f.cfg, nil
}

func (f *fakeMerchants) FeeRateAt(_ context.Context, _ string, at time.Time) (FeeRate, bool, error) {
	f.rateCalls++
	f.lastAt = at
	if f.rateErr != nil {
		return FeeRate{}, false, f.rateErr
	}
	return f.rate, f.found, nil
}

type memOrders struct {
	mu         sync.Mutex
	rows       []PaymentOrder
	txns       []channelTxn
	forceDup   bool
	updates    int
	failIns    error
	payNoReads int
	outReads   int
	readErr    error
	forceMiss  bool
	beforeCAS  func()
}

func (m *memOrders) insertPayment(_ context.Context, row PaymentOrder) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failIns != nil {
		return m.failIns
	}
	for _, ex := range m.rows {
		if ex.MerchantID == row.MerchantID && ex.OutTradeNo == row.OutTradeNo {
			return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
		}
	}
	if m.forceDup {
		return &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
	}
	m.rows = append(m.rows, row)
	return nil
}

func (m *memOrders) paymentByOutTradeNo(_ context.Context, merchantID, outTradeNo string) (PaymentOrder, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outReads++
	if m.readErr != nil {
		return PaymentOrder{}, false, m.readErr
	}
	for _, ex := range m.rows {
		if ex.MerchantID == merchantID && ex.OutTradeNo == outTradeNo {
			return ex, true, nil
		}
	}
	return PaymentOrder{}, false, nil
}

func (m *memOrders) paymentByPayNo(_ context.Context, payNo string) (PaymentOrder, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.payNoReads++
	if m.readErr != nil {
		return PaymentOrder{}, false, m.readErr
	}
	for _, ex := range m.rows {
		if ex.PayNo == payNo {
			return ex, true, nil
		}
	}
	return PaymentOrder{}, false, nil
}

func (m *memOrders) queryReads() (payNo, outTrade int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.payNoReads, m.outReads
}

func (m *memOrders) casInitToPaying(_ context.Context, order PaymentOrder, now time.Time) (bool, error) {
	if m.beforeCAS != nil {
		m.beforeCAS()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.forceMiss {
		m.forceMiss = false
		return false, nil
	}
	for i := range m.rows {
		row := &m.rows[i]
		if row.PayNo != order.PayNo || row.MerchantID != order.MerchantID || row.Status != payStatusInit {
			continue
		}
		row.Status = payStatusPaying
		row.Channel = channelMockpay
		row.Version++
		row.UpdatedAt = now
		m.txns = append(m.txns, channelTxn{
			Channel: channelMockpay, TxnType: txnTypePay, BizNo: order.PayNo,
			Amount: order.Amount, Status: txnStatusProcessing,
			NextQueryAt: now.Add(payNextQueryDelay), CreatedAt: now, UpdatedAt: now,
		})
		return true, nil
	}
	return false, nil
}

func (m *memOrders) casInitToClosed(_ context.Context, order PaymentOrder, now time.Time) (bool, error) {
	if m.beforeCAS != nil {
		m.beforeCAS()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.forceMiss {
		m.forceMiss = false
		return false, nil
	}
	for i := range m.rows {
		row := &m.rows[i]
		if row.PayNo != order.PayNo || row.MerchantID != order.MerchantID || row.Status != payStatusInit {
			continue
		}
		row.Status = payStatusClosed
		row.ClosedAt = now
		row.Version++
		row.UpdatedAt = now
		return true, nil
	}
	return false, nil
}

func (m *memOrders) fillChannelTxnNo(_ context.Context, bizNo, txnNo string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.txns {
		if m.txns[i].BizNo != bizNo {
			continue
		}
		if m.txns[i].ChannelTxnNo != "" {
			return 0, nil
		}
		for _, other := range m.txns {
			if other.BizNo != bizNo && other.Channel == m.txns[i].Channel && other.ChannelTxnNo == txnNo {
				return 0, &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"}
			}
		}
		m.txns[i].ChannelTxnNo = txnNo
		return 1, nil
	}
	return 0, nil
}

func (m *memOrders) channelTxnByBizNo(_ context.Context, bizNo string) (channelTxn, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, txn := range m.txns {
		if txn.BizNo == bizNo {
			return txn, true, nil
		}
	}
	return channelTxn{}, false, nil
}

type stubIDs struct {
	mu     sync.Mutex
	prefix string
	nos    []string
	err    error
	calls  int
}

func (s *stubIDs) NewNo(_ context.Context, prefix string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if prefix != s.prefix {
		return "", errors.New("unexpected prefix")
	}
	if s.err != nil {
		return "", s.err
	}
	if len(s.nos) == 0 {
		return "", errors.New("no ids")
	}
	no := s.nos[0]
	if len(s.nos) > 1 {
		s.nos = s.nos[1:]
	}
	return no, nil
}

func strPtr(s string) *string { return &s }

func intPtr(v int32) *int32 { return &v }

func mutate(in *CreatePaymentInput, fn func(*CreatePaymentInput)) *CreatePaymentInput {
	fn(in)
	return in
}
