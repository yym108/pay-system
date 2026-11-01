package biz

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/sign"
)

type countingCloser struct {
	mu       sync.Mutex
	closes   int
	queries  int
	closeOut channelCloseResult
	queryOut channelQueryResult
	closeErr error
	queryErr error
}

func (c *countingCloser) ClosePay(context.Context, string) (channelCloseResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return c.closeOut, c.closeErr
}

func (c *countingCloser) QueryPay(context.Context, string) (channelQueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries++
	return c.queryOut, c.queryErr
}

func (c *countingCloser) calls() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes, c.queries
}

func closeReq(order PaymentOrder) *CloseInput {
	return &CloseInput{MerchantID: order.MerchantID, PayNo: order.PayNo}
}

func seedChannelTxn(orders *memOrders, payNo, txnNo string) {
	orders.mu.Lock()
	defer orders.mu.Unlock()
	orders.txns = append(orders.txns, channelTxn{
		Channel: channelMockpay, TxnType: txnTypePay, BizNo: payNo,
		ChannelTxnNo: txnNo, Status: txnStatusProcessing, Amount: orders.rows[0].Amount,
	})
}

func TestCloseNilAndEmptyPayNo(t *testing.T) {
	p, _, orders, _, _ := newPayTest(t, "merchant-demo")
	if _, err := p.ClosePayment(context.Background(), nil); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("nil = %v", err)
	}
	if _, err := p.ClosePayment(context.Background(), &CloseInput{MerchantID: "M100002"}); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("empty pay_no = %v", err)
	}
	if orders.payNoReads != 0 {
		t.Fatalf("reads = %d", orders.payNoReads)
	}
}

func TestCloseMerchantMismatch(t *testing.T) {
	p, orders, order := seedInit(t)
	before := order.Version
	if _, err := p.ClosePayment(context.Background(), &CloseInput{PayNo: order.PayNo}); !errors.Is(err, errno.ErrOrderNotFound) {
		t.Fatalf("empty merchant = %v", err)
	}
	if _, err := p.ClosePayment(context.Background(), &CloseInput{MerchantID: "M100002x", PayNo: order.PayNo}); !errors.Is(err, errno.ErrOrderNotFound) {
		t.Fatalf("other merchant = %v", err)
	}
	if _, err := p.ClosePayment(context.Background(), &CloseInput{MerchantID: order.MerchantID, PayNo: "PA999"}); !errors.Is(err, errno.ErrOrderNotFound) {
		t.Fatalf("missing = %v", err)
	}
	got := orders.rows[0]
	if got.Status != payStatusInit || got.Version != before {
		t.Fatalf("row changed %+v", got)
	}
}

// INIT 关单不访问渠道。closed_at 和 updated_at 用注入时钟截到毫秒。
func TestCloseInitDoesNotCallChannel(t *testing.T) {
	p, orders, order := seedInit(t)
	clk := &clock.Fake{}
	now := payAt().Add(time.Millisecond + 200*time.Microsecond)
	clk.Set(now)
	p.clk = clk
	ch := &countingCloser{closeOut: channelCloseResult{Outcome: closeClosed}}
	p.UseClose(ch)
	got, err := p.ClosePayment(context.Background(), closeReq(order))
	if err != nil {
		t.Fatal(err)
	}
	wantAt := now.Truncate(time.Millisecond)
	if got.Status != payStatusClosed || got.Version != 1 || !got.ClosedAt.Equal(wantAt) || !got.UpdatedAt.Equal(wantAt) || got.Amount != order.Amount {
		t.Fatalf("got = %+v", got)
	}
	if closes, queries := ch.calls(); closes != 0 || queries != 0 {
		t.Fatalf("channel closes=%d queries=%d", closes, queries)
	}
	stored := orders.rows[0]
	if stored.Status != payStatusClosed || stored.Version != 1 || !stored.ClosedAt.Equal(wantAt) || !stored.UpdatedAt.Equal(wantAt) {
		t.Fatalf("stored = %+v", stored)
	}
}

// CLOSED 再关一次不改 version，也不访问渠道。
func TestCloseClosedAgainKeepsVersion(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingCloser{}
	p.UseClose(ch)
	first, err := p.ClosePayment(context.Background(), closeReq(order))
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.ClosePayment(context.Background(), closeReq(order))
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != payStatusClosed || second.Version != first.Version || orders.rows[0].Version != 1 {
		t.Fatalf("first=%+v second=%+v stored=%+v", first, second, orders.rows[0])
	}
	if !second.ClosedAt.Equal(first.ClosedAt) || !second.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("times changed first=%s second=%s", first.UpdatedAt, second.UpdatedAt)
	}
	if closes, queries := ch.calls(); closes != 0 || queries != 0 {
		t.Fatalf("channel closes=%d queries=%d", closes, queries)
	}
}

// SUCCESS 直接 30003，不访问渠道，也不改行。FAILED 同样。
func TestCloseSuccessReturnsConflictWithoutChannel(t *testing.T) {
	p, orders, order := seedInit(t)
	ch := &countingCloser{closeOut: channelCloseResult{Outcome: closeClosed}}
	p.UseClose(ch)
	for _, status := range []string{payStatusSuccess, payStatusFailed} {
		setRow(orders, func(row *PaymentOrder) {
			row.Status = status
			row.Version = 4
		})
		got, err := p.ClosePayment(context.Background(), closeReq(order))
		if !errors.Is(err, errno.ErrOrderStatusConflict) || got.Status != status || got.Version != 4 {
			t.Fatalf("status %s got=%+v err=%v", status, got, err)
		}
	}
	if closes, queries := ch.calls(); closes != 0 || queries != 0 {
		t.Fatalf("channel closes=%d queries=%d", closes, queries)
	}
	if orders.rows[0].Version != 4 {
		t.Fatalf("version = %d", orders.rows[0].Version)
	}
}

// PAYING 但关单 HTTP 失败时订单仍是 PAYING，返回 10004。
func TestClosePayingHTTPFailureStaysPaying(t *testing.T) {
	p, orders, order := seedInit(t)
	setRow(orders, func(row *PaymentOrder) {
		row.Status = payStatusPaying
		row.Version = 2
		row.Channel = channelMockpay
	})
	seedChannelTxn(orders, order.PayNo, "CH-HTTP")
	clk := testClock()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	client := newTestClient(t, srv.URL, clk, testChannelKey)
	p.UseClose(client)
	p.applier = &recordingApplier{order: order}
	got, err := p.ClosePayment(context.Background(), closeReq(order))
	if !errors.Is(err, errno.ErrUnavailable) || got.PayNo != "" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	stored := orders.rows[0]
	if stored.Status != payStatusPaying || stored.Version != 2 {
		t.Fatalf("stored = %+v", stored)
	}
	if p.applier.(*recordingApplier).calls != 0 {
		t.Fatal("apply ran after a failed close")
	}
}

func TestCloseAlreadyPaidQueryFailureStaysPaying(t *testing.T) {
	p, orders, order := seedInit(t)
	setRow(orders, func(row *PaymentOrder) {
		row.Status = payStatusPaying
		row.Version = 2
	})
	seedChannelTxn(orders, order.PayNo, "CH-PAID")
	ch := &countingCloser{closeOut: channelCloseResult{Outcome: closeAlreadyPaid}}
	p.UseClose(ch)
	applier := &recordingApplier{}
	p.applier = applier
	if _, err := p.ClosePayment(context.Background(), closeReq(order)); !errors.Is(err, errno.ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if closes, queries := ch.calls(); closes != 1 || queries != 1 || applier.calls != 0 || orders.rows[0].Status != payStatusPaying {
		t.Fatalf("closes/queries/apply/status = %d %d %d %s", closes, queries, applier.calls, orders.rows[0].Status)
	}
}

func TestClosePassesApplyAmountMismatch(t *testing.T) {
	p, orders, order := seedInit(t)
	setRow(orders, func(row *PaymentOrder) { row.Status = payStatusPaying })
	seedChannelTxn(orders, order.PayNo, "CH-1")
	finished := payAt()
	ch := &countingCloser{
		closeOut: channelCloseResult{Outcome: closeAlreadyPaid},
		queryOut: channelQueryResult{OK: true, Amount: money.Amount(1), FinishedAt: finished, ChannelTxnNo: "CH-1"},
	}
	p.UseClose(ch)
	p.applier = &recordingApplier{err: errno.ErrChannelAmountMismatch}
	got, err := p.ClosePayment(context.Background(), closeReq(order))
	if !errors.Is(err, errno.ErrChannelAmountMismatch) || got.PayNo != "" || orders.rows[0].Status != payStatusPaying {
		t.Fatalf("got=%+v err=%v status=%s", got, err, orders.rows[0].Status)
	}
	in := p.applier.(*recordingApplier).in
	if in.Result != payResultSuccess || in.Source != paySourceClose || in.Amount == nil || *in.Amount != 1 || !in.FinishedAt.Equal(finished) {
		t.Fatalf("apply in = %+v", in)
	}
}

func TestCloseChannelClassifiesSignedBodies(t *testing.T) {
	clk := testClock()
	finished := payAt().Truncate(time.Millisecond).Format(channelTimeLayout)
	cases := []struct {
		path string
		body string
		want closeOutcome
		ok   bool
		kind queryKind
	}{
		{channelClosePath, `{"result":"OK","status":"CLOSED"}`, closeClosed, false, queryCallFailed},
		{channelClosePath, `{"result":"ALREADY_PAID","status":"SUCCESS"}`, closeAlreadyPaid, false, queryCallFailed},
		{channelClosePath, `{"result":"OK","status":"FAILED"}`, closeTxnFailed, false, queryCallFailed},
		{channelClosePath, `{"result":"OK","status":"PROCESSING"}`, closeCallFailed, false, queryCallFailed},
		{channelQueryPath, `{"result":"OK","status":"SUCCESS","amount":10009,"channel_txn_no":"CH-9","finished_at":"` + finished + `"}`, closeCallFailed, true, queryFound},
		{channelQueryPath, `{"result":"NOT_FOUND"}`, closeCallFailed, false, queryNotFound},
		{channelQueryPath, `{"result":"OK","status":"SUCCESS","amount":10009,"channel_txn_no":"CH-9"}`, closeCallFailed, false, queryFound},
		{channelQueryPath, `{"result":"OK","status":"PROCESSING","amount":10008,"channel_txn_no":"CH-P"}`, closeCallFailed, false, queryFound},
		{channelQueryPath, `{"result":"OK","status":"FAILED","amount":10001,"channel_txn_no":"CH-F","finished_at":"` + finished + `"}`, closeCallFailed, false, queryFound},
		{channelQueryPath, `{"result":"OK","status":"CLOSED","amount":0,"channel_txn_no":"CH-C"}`, closeCallFailed, false, queryFound},
		{channelQueryPath, `{"result":"NOPE"}`, closeCallFailed, false, queryCallFailed},
	}
	for _, c := range cases {
		srv := signedPathServer(t, clk, testChannelKey, http.StatusOK, []byte(c.body))
		client := newTestClient(t, srv.URL, clk, testChannelKey)
		if c.path == channelClosePath {
			got, err := client.ClosePay(context.Background(), "PA0000000000000000001")
			srv.Close()
			if err != nil || got.Outcome != c.want {
				t.Fatalf("close %s = %+v err=%v", c.body, got, err)
			}
			continue
		}
		got, err := client.QueryPay(context.Background(), "PA0000000000000000001")
		srv.Close()
		if err != nil || got.OK != c.ok || got.Kind != c.kind {
			t.Fatalf("query %s = %+v err=%v", c.body, got, err)
		}
		if c.ok && (got.Amount != 10009 || got.ChannelTxnNo != "CH-9") {
			t.Fatalf("query fields = %+v", got)
		}
	}
}

// 没有渠道流水号时先查单。NOT_FOUND 和调用失败不关单。PROCESSING 再关单。已有流水号直接关单。
func TestClosePayingProbesWhenChannelTxnNoMissing(t *testing.T) {
	finished := payAt()
	cases := []struct {
		name       string
		txnNo      string
		omitRow    bool
		query      channelQueryResult
		wantClose  int
		wantQuery  int
		wantErr    error
		wantStatus string
	}{
		{name: "not found", omitRow: true, query: channelQueryResult{Kind: queryNotFound}, wantQuery: 1, wantErr: errno.ErrUnavailable, wantStatus: payStatusPaying},
		{name: "query failed", omitRow: true, query: channelQueryResult{}, wantQuery: 1, wantErr: errno.ErrUnavailable, wantStatus: payStatusPaying},
		{name: "empty txn no not found", query: channelQueryResult{Kind: queryNotFound}, wantQuery: 1, wantErr: errno.ErrUnavailable, wantStatus: payStatusPaying},
		{name: "processing", query: channelQueryResult{Kind: queryFound}, wantClose: 1, wantQuery: 1, wantStatus: payStatusClosed},
		{name: "known txn no", txnNo: "CH-KNOWN", query: channelQueryResult{Kind: queryNotFound}, wantClose: 1, wantStatus: payStatusClosed},
		{name: "known txn no skips success query", txnNo: "CH-KNOWN", query: channelQueryResult{Kind: queryFound, OK: true, Amount: 1, FinishedAt: finished}, wantClose: 1, wantStatus: payStatusClosed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, orders, order := seedInit(t)
			setRow(orders, func(row *PaymentOrder) {
				row.Status = payStatusPaying
				row.Version = 2
			})
			if !c.omitRow {
				seedChannelTxn(orders, order.PayNo, c.txnNo)
			}
			ch := &countingCloser{
				closeOut: channelCloseResult{Outcome: closeClosed},
				queryOut: c.query,
			}
			p.UseClose(ch)
			p.applier = &recordingApplier{order: PaymentOrder{PayNo: order.PayNo, Status: payStatusClosed, Amount: order.Amount}}
			got, err := p.ClosePayment(context.Background(), closeReq(order))
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v", err)
			}
			closes, queries := ch.calls()
			if closes != c.wantClose || queries != c.wantQuery {
				t.Fatalf("closes=%d queries=%d", closes, queries)
			}
			stored := orders.rows[0]
			if c.wantStatus == payStatusPaying {
				if stored.Status != payStatusPaying || stored.Version != 2 || got.PayNo != "" {
					t.Fatalf("got=%+v stored=%+v", got, stored)
				}
				return
			}
			if err != nil || got.Status != payStatusClosed || stored.Status != payStatusPaying {
				t.Fatalf("got=%+v stored=%+v err=%v", got, stored, err)
			}
		})
	}
}

func TestCloseChannelCallFailures(t *testing.T) {
	clk := testClock()
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusInternalServerError} {
		srv := signedPathServer(t, clk, testChannelKey, status, nil)
		client := newTestClient(t, srv.URL, clk, testChannelKey)
		got, err := client.ClosePay(context.Background(), "PA0000000000000000001")
		srv.Close()
		if err != nil || got.Outcome != closeCallFailed {
			t.Fatalf("status %d = %+v err=%v", status, got, err)
		}
	}
	if _, err := newTestClient(t, "http://127.0.0.1", clk, "").ClosePay(context.Background(), "PA1"); !errors.Is(err, sign.ErrEmptyKey) {
		t.Fatalf("empty key err = %v", err)
	}
}

func TestCloseChannelTimeoutUsesParentDeadline(t *testing.T) {
	if CloseChannelTimeout != 1500*time.Millisecond || PayChannelDialTimeout != 500*time.Millisecond {
		t.Fatalf("close timeout=%s dial=%s", CloseChannelTimeout, PayChannelDialTimeout)
	}
	clk := testClock()
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
		case <-timer.C:
		}
	}))
	defer func() {
		hung.CloseClientConnections()
		hung.Close()
	}()
	client := newTestClient(t, hung.URL, clk, testChannelKey)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	done := make(chan error, 1)
	go func() {
		got, err := client.ClosePay(ctx, "PA0000000000000000001")
		if got.Outcome != closeCallFailed {
			err = errors.New("outcome")
		}
		done <- err
	}()
	select {
	case <-timer.C:
		t.Fatal("parent deadline was not applied")
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	}
}

func signedPathServer(t *testing.T, clk clock.Clock, key string, status int, body []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ts := r.Header.Get(sign.HeaderMockTimestamp)
		if err := sign.VerifyChannel([]byte(key), sign.ChannelMessage{Timestamp: ts, Body: raw}, r.Header.Get(sign.HeaderMockSignature)); err != nil {
			t.Errorf("request signature: %v", err)
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		sig, err := sign.SignChannel([]byte(key), sign.ChannelMessage{Timestamp: ts, Body: body})
		if err != nil {
			t.Errorf("sign response: %v", err)
			return
		}
		w.Header().Set(sign.HeaderMockTimestamp, ts)
		w.Header().Set(sign.HeaderMockSignature, sig)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
}
