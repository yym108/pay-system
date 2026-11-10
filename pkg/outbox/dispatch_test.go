package outbox

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/mq"
)

type captureSender struct {
	n int
}

func (s *captureSender) Send(context.Context, []Delivery) []error {
	s.n++
	return nil
}

// MQ-02：依赖为空时在领取前返回固定错误，不拨号。
func TestNilDepsReturnBeforeClaim(t *testing.T) {
	clk := &clock.Fake{}
	clk.Set(time.Date(2026, 11, 10, 8, 0, 0, 0, testZone))
	sender := &captureSender{}
	raw, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:1)/none")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	db := sqlx.NewDb(raw, "mysql")
	cases := []*Dispatcher{
		nil,
		{},
		{sender: sender, clk: clk},
		{db: db, clk: clk},
		{db: db, sender: sender},
	}
	for i, d := range cases {
		got := d.RunBatch(context.Background())
		if !errors.Is(got, errNilDependency) {
			t.Fatalf("case %d err = %v", i, got)
		}
		if strings.Contains(got.Error(), "127.0.0.1") || strings.Contains(got.Error(), "dial") {
			t.Fatalf("case %d error text contains an address", i)
		}
	}
	if sender.n != 0 {
		t.Fatalf("sender calls = %d", sender.n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := NewDispatcher(db, sender, clk).RunBatch(ctx); !errors.Is(got, context.Canceled) || sender.n != 0 {
		t.Fatalf("cancelled err = %v calls = %d", got, sender.n)
	}
	if got := (*Dispatcher)(nil).RefreshGauges(context.Background()); !errors.Is(got, errNilDependency) {
		t.Fatalf("refresh err = %v", got)
	}
	if NewMQSender(nil) != nil {
		t.Fatal("nil producer built a sender")
	}
}

// MQ-02：第 1 次失败间隔 2 秒。
func TestBackoffFirstIsTwoSeconds(t *testing.T) {
	if got := backoff(1); got != 2*time.Second {
		t.Fatalf("backoff(1) = %s", got)
	}
}

// MQ-02：退避封顶 5 分钟。2^9 秒已超过 5 分钟，不再移位。
func TestBackoffCapsAtFiveMinutes(t *testing.T) {
	if got := backoff(8); got != 256*time.Second {
		t.Fatalf("backoff(8) = %s", got)
	}
	if got := backoff(9); got != 5*time.Minute {
		t.Fatalf("backoff(9) = %s", got)
	}
	if got := backoff(20); got != 5*time.Minute {
		t.Fatalf("backoff(20) = %s", got)
	}
}

// MQ-02：第 20 次变为 DEAD，第 19 次仍是 PENDING。
func TestRetryTwentyIsDead(t *testing.T) {
	if statusForRetry(19) != statusPending || statusForRetry(20) != statusDead || statusForRetry(21) != statusDead {
		t.Fatalf("19=%s 20=%s 21=%s", statusForRetry(19), statusForRetry(20), statusForRetry(21))
	}
}

// MQ-02：traceparent 缺失或解析不了仍得到空字符串，能解析时原样返回。
func TestTraceparentMissingStillSends(t *testing.T) {
	cases := []struct {
		payload string
		want    string
	}{
		{payload: `not json`, want: ""},
		{payload: `{}`, want: ""},
		{payload: `{"trace_context":null}`, want: ""},
		{payload: `{"trace_context":{}}`, want: ""},
		{payload: `{"trace_context":[]}`, want: ""},
		{payload: `{"trace_context":{"traceparent":1}}`, want: ""},
		{payload: `{"trace_context":{"traceparent":"00-abc"}}`, want: "00-abc"},
	}
	for _, tc := range cases {
		if got := traceparentOf([]byte(tc.payload)); got != tc.want {
			t.Fatalf("payload %s traceparent = %q", tc.payload, got)
		}
	}
	rows := []claimed{{
		eventID: "EV0000000000000000099", eventType: "payment.succeeded", topic: "pay.payment.succeeded",
		msgKey: "PA0000000123456789012", payload: []byte(`{}`),
	}}
	got := deliveries(rows)
	if len(got) != 1 || got[0].Traceparent != "" || string(got[0].Value) != `{}` || got[0].EventID != rows[0].eventID {
		t.Fatalf("delivery = %+v", got)
	}
}

func TestTruncateLastErrorBytes(t *testing.T) {
	if got := truncateBytes(strings.Repeat("a", 600), lastErrorMax); len(got) != lastErrorMax {
		t.Fatalf("len = %d", len(got))
	}
	raw := strings.Repeat("你", 200)
	got := truncateBytes(raw, lastErrorMax)
	if !utf8.ValidString(got) || len([]byte(got)) > lastErrorMax || len([]byte(got)) == 0 {
		t.Fatalf("truncated len=%d valid=%v", len([]byte(got)), utf8.ValidString(got))
	}
	if truncateBytes("ok", lastErrorMax) != "ok" {
		t.Fatal("short string changed")
	}
}

func TestSendErrAtPadsMissing(t *testing.T) {
	if err := sendErrAt(nil, 0); !errors.Is(err, mq.ErrUnavailable) {
		t.Fatalf("nil errs = %v", err)
	}
	if err := sendErrAt([]error{nil}, 0); err != nil {
		t.Fatalf("success slot = %v", err)
	}
	if err := sendErrAt([]error{nil}, 1); !errors.Is(err, mq.ErrUnavailable) {
		t.Fatalf("short errs = %v", err)
	}
}
