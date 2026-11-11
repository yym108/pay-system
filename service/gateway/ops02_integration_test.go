//go:build integration

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel"

	"github.com/yym108/pay-system/kitex_gen/payment"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/mq"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/service/account/biz"
	"github.com/yym108/pay-system/service/account/consumer"
)

// OPS-02 验收：Jaeger 里按 pay_no 能看到下单、发起支付、回调和入账消费。
// 消费 span 的 link 指向网关下单那条 trace。

const ops02PayNo = "PA0000000123456789012"

type ops02Ledger struct {
	calls int
}

func (f *ops02Ledger) PostPay(context.Context, string, string, string, money.Amount, money.Amount) (biz.PostResult, error) {
	f.calls++
	return biz.PostResult{TxnNo: "TX1"}, nil
}

func (f *ops02Ledger) ConfirmFreeze(context.Context, string, string, string, string) (biz.ConfirmResult, error) {
	return biz.ConfirmResult{}, nil
}

func (f *ops02Ledger) ReleaseFreeze(context.Context, string, string, string, money.Amount) (biz.FreezeResult, error) {
	return biz.FreezeResult{}, nil
}

type jaegerTag struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type jaegerRef struct {
	TraceID string `json:"traceID"`
}

type jaegerSpan struct {
	TraceID       string      `json:"traceID"`
	SpanID        string      `json:"spanID"`
	OperationName string      `json:"operationName"`
	Tags          []jaegerTag `json:"tags"`
	References    []jaegerRef `json:"references"`
}

type jaegerTrace struct {
	Spans []jaegerSpan `json:"spans"`
}

type jaegerResp struct {
	Data []jaegerTrace `json:"data"`
}

func TestOPS02JaegerPayChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := testcontainers.Run(ctx, "jaegertracing/jaeger:2.22.0",
		testcontainers.WithExposedPorts("4317/tcp", "16686/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("16686/tcp").WithStartupTimeout(2*time.Minute)),
	)
	if err != nil {
		t.Skip("jaeger container: " + err.Error())
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer stopCancel()
		_ = ctr.Terminate(stopCtx)
	})
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if host == "localhost" || host == "::1" {
		host = "127.0.0.1"
	}
	otlp, err := ctr.MappedPort(ctx, "4317/tcp")
	if err != nil {
		t.Fatal(err)
	}
	queryPort, err := ctr.MappedPort(ctx, "16686/tcp")
	if err != nil {
		t.Fatal(err)
	}
	queryBase := "http://" + net.JoinHostPort(host, queryPort.Port())

	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	shutdown, err := obs.Install(ctx, "gateway", net.JoinHostPort(host, otlp.Port()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutCancel()
		_ = shutdown(shutCtx)
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	clk := &clock.Fake{}
	clk.Set(time.Unix(1_791_500_000, 0))
	ts := strconv.FormatInt(clk.Now().Unix(), 10)
	pay := &gw02Payment{
		createFn: func(req *payment.CreatePaymentRequest) (*payment.PaymentOrder, error) {
			return &payment.PaymentOrder{
				PayNo: ops02PayNo, OutTradeNo: req.OutTradeNo, Amount: req.Amount,
				Subject: req.Subject, Currency: "CNY", Status: "INIT",
			}, nil
		},
	}
	notify := &gw06Notify{}
	key := []byte("mockpay-plain-key")
	h := NewMerchant(
		WithClock(clk),
		WithMerchantLookup(gw02Lookup()),
		WithPaymentAPI(pay),
		WithNonceStore(NewMemoryNonce()),
		WithChannelKey(key),
		WithNotifyAPI(notify),
	)
	addr := startGW(t, h, nil)

	createBody := []byte(`{"out_trade_no":"T20261009001","amount":10000,"subject":"demo"}`)
	createHdr := gw02Sign(t, http.MethodPost, "/v1/payments", createBody)
	createResp := do(t, http.MethodPost, "http://"+addr+"/v1/payments", createBody, createHdr)
	if createResp.status != http.StatusOK {
		t.Fatalf("create %d %s", createResp.status, createResp.body)
	}
	payBody := []byte(`{"channel":"mockpay"}`)
	payURI := "/v1/payments/" + ops02PayNo + "/pay"
	payHdr := gw02Sign(t, http.MethodPost, payURI, payBody)
	payResp := do(t, http.MethodPost, "http://"+addr+payURI, payBody, payHdr)
	if payResp.status != http.StatusOK {
		t.Fatalf("pay %d %s", payResp.status, payResp.body)
	}
	cb := []byte(`{"notify_id":"CN0000000123456789012","txn_type":"PAY","req_no":"` + ops02PayNo + `","channel_txn_no":"MC0000000123456789012","amount":10000,"status":"SUCCESS","finished_at":"2026-10-26T11:22:33.123+08:00"}`)
	cbResp := gw06Post(t, addr, key, ts, cb)
	if cbResp.status != http.StatusOK {
		t.Fatalf("callback %d %s", cbResp.status, cbResp.body)
	}

	spans, raw, err := waitJaeger(ctx, queryBase, chainReady)
	if errors.Is(err, errJaegerAPI) {
		t.Skip("jaeger query: " + err.Error() + " body=" + raw)
	}
	if err != nil {
		t.Fatalf("jaeger before consume: %v body=%s", err, raw)
	}
	createTrace, createSpan := "", ""
	for _, sp := range spans {
		if sp.OperationName == "/v1/payments" && tagValue(sp, "pay_no") == ops02PayNo {
			createTrace = padHex(sp.TraceID, 32)
			createSpan = padHex(sp.SpanID, 16)
		}
	}
	if createTrace == "" || createSpan == "" {
		t.Fatalf("create span ids missing body=%s", raw)
	}

	ledger := &ops02Ledger{}
	env := []byte(`{"event_id":"EV1","event_type":"payment.succeeded","trace_context":{"traceparent":"00-` +
		createTrace + `-` + createSpan + `-01"},"data":{"pay_no":"` + ops02PayNo +
		`","merchant_id":"M100002","out_trade_no":"OT1","channel":"mockpay","amount":10000,"fee_amount":60,"paid_at":"2026-11-10T00:00:00.000+08:00"}}`)
	if err = consumer.HandleRecord(context.Background(), ledger, mq.Record{
		Topic:     consumer.TopicPaymentSucceeded,
		Value:     env,
		EventID:   "EV1",
		EventType: "payment.succeeded",
	}); err != nil {
		t.Fatal(err)
	}
	if ledger.calls != 1 {
		t.Fatalf("posts = %d", ledger.calls)
	}

	_, raw, err = waitJaeger(ctx, queryBase, func(spans []jaegerSpan) bool {
		for _, sp := range spans {
			if sp.OperationName != "consume" || tagValue(sp, "pay_no") != ops02PayNo {
				continue
			}
			for _, ref := range sp.References {
				if strings.EqualFold(padHex(ref.TraceID, 32), createTrace) {
					return true
				}
			}
		}
		return false
	})
	if errors.Is(err, errJaegerAPI) {
		t.Skip("jaeger query: " + err.Error() + " body=" + raw)
	}
	if err != nil {
		t.Fatalf("consume link missing, create=%s err=%v body=%s", createTrace, err, raw)
	}
}

func tagValue(sp jaegerSpan, key string) string {
	for _, tag := range sp.Tags {
		if tag.Key != key {
			continue
		}
		var s string
		if err := json.Unmarshal(tag.Value, &s); err == nil {
			return s
		}
	}
	return ""
}

var errJaegerAPI = errors.New("jaeger query path unavailable")

func chainReady(spans []jaegerSpan) bool {
	create, pay, callback := false, false, false
	for _, sp := range spans {
		if tagValue(sp, "pay_no") != ops02PayNo {
			continue
		}
		switch {
		case sp.OperationName == "/v1/payments":
			create = true
		case strings.HasSuffix(sp.OperationName, "/pay"):
			pay = true
		case sp.OperationName == "/callback/mockpay":
			callback = true
		}
	}
	return create && pay && callback
}

func padHex(s string, n int) string {
	s = strings.ToLower(s)
	if len(s) >= n {
		return s
	}
	return strings.Repeat("0", n-len(s)) + s
}

func waitJaeger(ctx context.Context, base string, ready func([]jaegerSpan) bool) ([]jaegerSpan, string, error) {
	// Jaeger 2.22 的 /api/traces 返回 404。/api/v3/traces 要带起止时间，否则 400。
	v3 := url.Values{}
	v3.Set("query.service_name", "gateway")
	v3.Set("query.num_traces", "20")
	v3.Set("query.start_time_min", "2020-01-01T00:00:00Z")
	v3.Set("query.start_time_max", "2030-01-01T00:00:00Z")
	paths := []string{
		"/api/traces?service=gateway&limit=20&lookback=1h&tags=" + url.QueryEscape(`{"pay_no":"`+ops02PayNo+`"}`),
		"/api/v3/traces?" + v3.Encode(),
	}
	var last string
	miss := map[string]int{}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		progress := false
		for _, path := range paths {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
			if err != nil {
				return nil, last, err
			}
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			last = string(body)
			if resp.StatusCode == http.StatusNotFound {
				// 查询接口在还没有 span 时也回 404，正文是 No traces found。这不是路径不存在。
				if strings.Contains(last, "No traces found") {
					progress = true
					continue
				}
				miss[path]++
				continue
			}
			progress = true
			if resp.StatusCode != http.StatusOK {
				continue
			}
			spans := parseJaeger(body)
			if ready(spans) {
				return spans, last, nil
			}
		}
		if !progress && miss[paths[0]] >= 4 && miss[paths[1]] >= 4 {
			return nil, last, errJaegerAPI
		}
		select {
		case <-ctx.Done():
			return nil, last, ctx.Err()
		case <-tick.C:
		}
	}
}

func parseJaeger(body []byte) []jaegerSpan {
	if spans := parseJaegerInternal(body); len(spans) > 0 {
		return spans
	}
	return parseJaegerV3(body)
}

func parseJaegerInternal(body []byte) []jaegerSpan {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	var resp jaegerResp
	if err := dec.Decode(&resp); err != nil {
		return nil
	}
	out := make([]jaegerSpan, 0)
	for _, tr := range resp.Data {
		out = append(out, tr.Spans...)
	}
	return out
}

func parseJaegerV3(body []byte) []jaegerSpan {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	var resp struct {
		Result struct {
			ResourceSpans []struct {
				ScopeSpans []struct {
					Spans []struct {
						TraceID    string `json:"traceId"`
						SpanID     string `json:"spanId"`
						Name       string `json:"name"`
						Attributes []struct {
							Key   string `json:"key"`
							Value struct {
								StringValue string `json:"stringValue"`
							} `json:"value"`
						} `json:"attributes"`
						Links []struct {
							TraceID string `json:"traceId"`
						} `json:"links"`
					} `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		} `json:"result"`
	}
	if err := dec.Decode(&resp); err != nil {
		return nil
	}
	out := make([]jaegerSpan, 0)
	for _, rs := range resp.Result.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				tags := make([]jaegerTag, 0, len(sp.Attributes))
				for _, attr := range sp.Attributes {
					raw, err := json.Marshal(attr.Value.StringValue)
					if err != nil {
						continue
					}
					tags = append(tags, jaegerTag{Key: attr.Key, Value: raw})
				}
				refs := make([]jaegerRef, 0, len(sp.Links))
				for _, link := range sp.Links {
					refs = append(refs, jaegerRef{TraceID: link.TraceID})
				}
				out = append(out, jaegerSpan{
					TraceID:       sp.TraceID,
					SpanID:        sp.SpanID,
					OperationName: sp.Name,
					Tags:          tags,
					References:    refs,
				})
			}
		}
	}
	return out
}
