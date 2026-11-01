package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yym108/pay-system/pkg/config"
)

const (
	headerAdminToken    = "X-Admin-Token"
	headerAdminOperator = "X-Admin-Operator"

	errPayNoXOR      = "paycli: provide exactly one of pay_no or out_trade_no"
	errRefundNoXOR   = "paycli: provide exactly one of refund_no or out_refund_no"
	errTransferNoXOR = "paycli: provide exactly one of transfer_no or out_transfer_no"
	errReplayXOR     = "paycli: provide event_id or both biz_no and event_type"
)

var errUsage = errors.New("usage")

func isUsage(err error) bool {
	return errors.Is(err, errUsage)
}

// 超时取需求 §5.4.1 网关调用 payment、account、recon 的调用超时，不另造数值。
const (
	gatewayCallTimeout         = time.Second
	gatewayChannelCallTimeout  = 5 * time.Second
	gatewayTransferCallTimeout = 3 * time.Second
	gatewayBatchCallTimeout    = 120 * time.Second
)

func commandTimeout(name string) time.Duration {
	switch name {
	case "pay", "close", "refund":
		return gatewayChannelCallTimeout
	case "transfer":
		return gatewayTransferCallTimeout
	case "recon-run", "check":
		return gatewayBatchCallTimeout
	default:
		return gatewayCallTimeout
	}
}

type prepared struct {
	name     string
	method   string
	path     string
	body     []byte
	admin    bool
	operator string
	timeout  time.Duration
}

type flagSet struct {
	*flag.FlagSet
	seen map[string]bool
}

func newFlagSet(name string) *flagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return &flagSet{FlagSet: fs, seen: map[string]bool{}}
}

func (f *flagSet) parse(args []string) error {
	if err := f.Parse(args); err != nil {
		return errors.New("paycli: invalid arguments")
	}
	if f.NArg() != 0 {
		return errors.New("paycli: invalid arguments")
	}
	f.Visit(func(fl *flag.Flag) {
		f.seen[fl.Name] = true
	})
	return nil
}

func (f *flagSet) has(name string) bool {
	return f.seen[name]
}

func (f *flagSet) text(name, val string) bool {
	return f.seen[name] && val != ""
}

func splitArgs(args []string) (global []string, cmd string, rest []string, err error) {
	i := 0
	for i < len(args) {
		a := args[i]
		if a == "--" {
			if i+1 < len(args) {
				return global, args[i+1], args[i+2:], nil
			}
			return global, "", nil, nil
		}
		if a == "" || !strings.HasPrefix(a, "-") {
			return global, a, args[i+1:], nil
		}
		name, inline := cutFlag(a)
		if name == "h" || name == "help" {
			return nil, "", nil, errUsage
		}
		if !globalTakesValue(name) {
			return nil, "", nil, errors.New("paycli: invalid arguments")
		}
		global = append(global, a)
		if !inline {
			if i+1 >= len(args) {
				return nil, "", nil, errors.New("paycli: invalid arguments")
			}
			i++
			global = append(global, args[i])
		}
		i++
	}
	return global, "", nil, nil
}

func cutFlag(a string) (name string, inline bool) {
	a = strings.TrimLeft(a, "-")
	name, _, inline = strings.Cut(a, "=")
	return name, inline
}

func globalTakesValue(name string) bool {
	switch name {
	case "gateway", "app-id", "app-secret", "admin-token":
		return true
	default:
		return false
	}
}

func applyGlobals(cfg *config.PayCLI, args []string) error {
	if len(args) == 0 {
		return nil
	}
	fs := newFlagSet("paycli")
	gateway := fs.String("gateway", "", "")
	appID := fs.String("app-id", "", "")
	secret := fs.String("app-secret", "", "")
	token := fs.String("admin-token", "", "")
	if err := fs.parse(args); err != nil {
		return err
	}
	if fs.has("gateway") {
		cfg.GatewayURL = *gateway
	}
	if fs.has("app-id") {
		cfg.AppID = *appID
	}
	if fs.has("app-secret") {
		cfg.AppSecret = *secret
	}
	if fs.has("admin-token") {
		cfg.AdminToken = *token
	}
	return nil
}

func prepare(cmd string, rest []string) (prepared, error) {
	if cmd == "admin" {
		if len(rest) == 0 || strings.HasPrefix(rest[0], "-") {
			return prepared{}, errUsage
		}
		return prepareAdmin(rest[0], rest[1:])
	}
	return prepareMerchant(cmd, rest)
}

func prepareMerchant(cmd string, rest []string) (prepared, error) {
	switch cmd {
	case "create":
		return prepareCreate(rest)
	case "pay":
		return preparePay(rest)
	case "query":
		return prepareQuery(rest)
	case "close":
		return prepareClose(rest)
	case "refund":
		return prepareRefund(rest)
	case "query-refund":
		return prepareQueryRefund(rest)
	case "transfer":
		return prepareTransfer(rest)
	case "query-transfer":
		return prepareQueryTransfer(rest)
	case "balance":
		return prepareBalance(rest)
	default:
		return prepared{}, errUsage
	}
}

type createBody struct {
	OutTradeNo    string `json:"out_trade_no"`
	Amount        int64  `json:"amount"`
	Subject       string `json:"subject"`
	Currency      string `json:"currency,omitempty"`
	NotifyURL     string `json:"notify_url,omitempty"`
	ExpireMinutes *int   `json:"expire_minutes,omitempty"`
}

func prepareCreate(rest []string) (prepared, error) {
	fs := newFlagSet("create")
	outNo := fs.String("out-trade-no", "", "")
	amount := fs.String("amount", "", "")
	subject := fs.String("subject", "", "")
	currency := fs.String("currency", "", "")
	notify := fs.String("notify-url", "", "")
	expire := fs.String("expire-minutes", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	if !fs.text("out-trade-no", *outNo) || !fs.text("subject", *subject) {
		return prepared{}, errors.New("paycli: out_trade_no and subject are required")
	}
	n, err := parseAmount(fs, *amount)
	if err != nil {
		return prepared{}, err
	}
	body := createBody{OutTradeNo: *outNo, Amount: n, Subject: *subject}
	if fs.text("currency", *currency) {
		body.Currency = *currency
	}
	if fs.text("notify-url", *notify) {
		body.NotifyURL = *notify
	}
	if fs.has("expire-minutes") {
		mins, conv := strconv.Atoi(*expire)
		if conv != nil {
			return prepared{}, errors.New("paycli: expire_minutes must be an integer")
		}
		body.ExpireMinutes = &mins
	}
	raw, err := encodeJSON(body)
	if err != nil {
		return prepared{}, err
	}
	return merchantPrepared("create", http.MethodPost, "/v1/payments", raw), nil
}

type payBody struct {
	Channel string `json:"channel"`
}

func preparePay(rest []string) (prepared, error) {
	fs := newFlagSet("pay")
	payNo := fs.String("pay-no", "", "")
	channel := fs.String("channel", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	if !fs.text("pay-no", *payNo) || !fs.text("channel", *channel) {
		return prepared{}, errors.New("paycli: pay_no and channel are required")
	}
	raw, err := encodeJSON(payBody{Channel: *channel})
	if err != nil {
		return prepared{}, err
	}
	return merchantPrepared("pay", http.MethodPost, "/v1/payments/"+url.PathEscape(*payNo)+"/pay", raw), nil
}

func prepareQuery(rest []string) (prepared, error) {
	fs := newFlagSet("query")
	payNo := fs.String("pay-no", "", "")
	outNo := fs.String("out-trade-no", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	hasPay := fs.text("pay-no", *payNo)
	hasOut := fs.text("out-trade-no", *outNo)
	if hasPay == hasOut {
		return prepared{}, errors.New(errPayNoXOR)
	}
	path := "/v1/payments/" + url.PathEscape(*payNo)
	if hasOut {
		q := url.Values{}
		q.Set("out_trade_no", *outNo)
		path = "/v1/payments?" + q.Encode()
	}
	return merchantPrepared("query", http.MethodGet, path, nil), nil
}

func prepareClose(rest []string) (prepared, error) {
	fs := newFlagSet("close")
	payNo := fs.String("pay-no", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	if !fs.text("pay-no", *payNo) {
		return prepared{}, errors.New("paycli: pay_no is required")
	}
	return merchantPrepared("close", http.MethodPost, "/v1/payments/"+url.PathEscape(*payNo)+"/close", nil), nil
}

type refundBody struct {
	PayNo       string `json:"pay_no,omitempty"`
	OutTradeNo  string `json:"out_trade_no,omitempty"`
	OutRefundNo string `json:"out_refund_no"`
	Amount      int64  `json:"amount"`
	Reason      string `json:"reason,omitempty"`
}

func prepareRefund(rest []string) (prepared, error) {
	fs := newFlagSet("refund")
	payNo := fs.String("pay-no", "", "")
	outNo := fs.String("out-trade-no", "", "")
	outRefund := fs.String("out-refund-no", "", "")
	amount := fs.String("amount", "", "")
	reason := fs.String("reason", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	hasPay := fs.text("pay-no", *payNo)
	hasOut := fs.text("out-trade-no", *outNo)
	if hasPay == hasOut {
		return prepared{}, errors.New(errPayNoXOR)
	}
	if !fs.text("out-refund-no", *outRefund) {
		return prepared{}, errors.New("paycli: out_refund_no is required")
	}
	n, err := parseAmount(fs, *amount)
	if err != nil {
		return prepared{}, err
	}
	body := refundBody{OutRefundNo: *outRefund, Amount: n}
	if hasPay {
		body.PayNo = *payNo
	} else {
		body.OutTradeNo = *outNo
	}
	if fs.text("reason", *reason) {
		body.Reason = *reason
	}
	raw, err := encodeJSON(body)
	if err != nil {
		return prepared{}, err
	}
	return merchantPrepared("refund", http.MethodPost, "/v1/refunds", raw), nil
}

func prepareQueryRefund(rest []string) (prepared, error) {
	fs := newFlagSet("query-refund")
	refundNo := fs.String("refund-no", "", "")
	outNo := fs.String("out-refund-no", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	hasNo := fs.text("refund-no", *refundNo)
	hasOut := fs.text("out-refund-no", *outNo)
	if hasNo == hasOut {
		return prepared{}, errors.New(errRefundNoXOR)
	}
	path := "/v1/refunds/" + url.PathEscape(*refundNo)
	if hasOut {
		q := url.Values{}
		q.Set("out_refund_no", *outNo)
		path = "/v1/refunds?" + q.Encode()
	}
	return merchantPrepared("query-refund", http.MethodGet, path, nil), nil
}

type transferBody struct {
	OutTransferNo   string `json:"out_transfer_no"`
	PayeeMerchantID string `json:"payee_merchant_id"`
	Amount          int64  `json:"amount"`
	Remark          string `json:"remark,omitempty"`
	NotifyURL       string `json:"notify_url,omitempty"`
}

func prepareTransfer(rest []string) (prepared, error) {
	fs := newFlagSet("transfer")
	outNo := fs.String("out-transfer-no", "", "")
	payee := fs.String("payee-merchant-id", "", "")
	amount := fs.String("amount", "", "")
	remark := fs.String("remark", "", "")
	notify := fs.String("notify-url", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	if !fs.text("out-transfer-no", *outNo) || !fs.text("payee-merchant-id", *payee) {
		return prepared{}, errors.New("paycli: out_transfer_no and payee_merchant_id are required")
	}
	n, err := parseAmount(fs, *amount)
	if err != nil {
		return prepared{}, err
	}
	body := transferBody{OutTransferNo: *outNo, PayeeMerchantID: *payee, Amount: n}
	if fs.text("remark", *remark) {
		body.Remark = *remark
	}
	if fs.text("notify-url", *notify) {
		body.NotifyURL = *notify
	}
	raw, err := encodeJSON(body)
	if err != nil {
		return prepared{}, err
	}
	return merchantPrepared("transfer", http.MethodPost, "/v1/transfers", raw), nil
}

func prepareQueryTransfer(rest []string) (prepared, error) {
	fs := newFlagSet("query-transfer")
	transferNo := fs.String("transfer-no", "", "")
	outNo := fs.String("out-transfer-no", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	hasNo := fs.text("transfer-no", *transferNo)
	hasOut := fs.text("out-transfer-no", *outNo)
	if hasNo == hasOut {
		return prepared{}, errors.New(errTransferNoXOR)
	}
	path := "/v1/transfers/" + url.PathEscape(*transferNo)
	if hasOut {
		q := url.Values{}
		q.Set("out_transfer_no", *outNo)
		path = "/v1/transfers?" + q.Encode()
	}
	return merchantPrepared("query-transfer", http.MethodGet, path, nil), nil
}

func prepareBalance(rest []string) (prepared, error) {
	fs := newFlagSet("balance")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	return merchantPrepared("balance", http.MethodGet, "/v1/balance", nil), nil
}

func prepareAdmin(sub string, rest []string) (prepared, error) {
	switch sub {
	case "recon-run":
		return prepareRecon(rest)
	case "replay":
		return prepareReplay(rest)
	case "check":
		return prepareCheck(rest)
	default:
		return prepared{}, errUsage
	}
}

type reconBody struct {
	Channel  string `json:"channel"`
	BillDate string `json:"bill_date"`
	Force    *bool  `json:"force,omitempty"`
}

func prepareRecon(rest []string) (prepared, error) {
	fs := newFlagSet("recon-run")
	channel := fs.String("channel", "", "")
	bill := fs.String("bill-date", "", "")
	force := fs.Bool("force", false, "")
	operator := fs.String("operator", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	if !fs.text("channel", *channel) || !fs.text("bill-date", *bill) {
		return prepared{}, errors.New("paycli: channel and bill_date are required")
	}
	op, err := requiredOperator(fs, *operator)
	if err != nil {
		return prepared{}, err
	}
	body := reconBody{Channel: *channel, BillDate: *bill}
	if fs.has("force") && *force {
		v := true
		body.Force = &v
	}
	raw, err := encodeJSON(body)
	if err != nil {
		return prepared{}, err
	}
	return adminPrepared("recon-run", "/admin/recon/run", raw, op), nil
}

type replayEventBody struct {
	EventID string `json:"event_id"`
}

type replayBizBody struct {
	BizNo     string `json:"biz_no"`
	EventType string `json:"event_type"`
}

func prepareReplay(rest []string) (prepared, error) {
	fs := newFlagSet("replay")
	eventID := fs.String("event-id", "", "")
	bizNo := fs.String("biz-no", "", "")
	eventType := fs.String("event-type", "", "")
	operator := fs.String("operator", "", "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	hasEvent := fs.text("event-id", *eventID)
	hasBiz := fs.text("biz-no", *bizNo)
	hasType := fs.text("event-type", *eventType)
	op, err := requiredOperator(fs, *operator)
	if err != nil {
		return prepared{}, err
	}
	var raw []byte
	switch {
	case hasEvent && !hasBiz && !hasType:
		raw, err = encodeJSON(replayEventBody{EventID: *eventID})
	case !hasEvent && hasBiz && hasType:
		raw, err = encodeJSON(replayBizBody{BizNo: *bizNo, EventType: *eventType})
	default:
		return prepared{}, errors.New(errReplayXOR)
	}
	if err != nil {
		return prepared{}, err
	}
	return adminPrepared("replay", "/admin/outbox/replay", raw, op), nil
}

type checkBody struct {
	MerchantID      string `json:"merchant_id,omitempty"`
	ExpectQuiescent bool   `json:"expect_quiescent,omitempty"`
}

func prepareCheck(rest []string) (prepared, error) {
	fs := newFlagSet("check")
	merchant := fs.String("merchant-id", "", "")
	quiet := fs.Bool("expect-quiescent", false, "")
	if err := fs.parse(rest); err != nil {
		return prepared{}, err
	}
	body := checkBody{}
	if fs.text("merchant-id", *merchant) {
		body.MerchantID = *merchant
	}
	if fs.has("expect-quiescent") && *quiet {
		body.ExpectQuiescent = true
	}
	raw, err := encodeJSON(body)
	if err != nil {
		return prepared{}, err
	}
	return adminPrepared("check", "/admin/accounts/check", raw, ""), nil
}

func merchantPrepared(name, method, path string, body []byte) prepared {
	return prepared{name: name, method: method, path: path, body: body, timeout: commandTimeout(name)}
}

func adminPrepared(name, path string, body []byte, operator string) prepared {
	return prepared{
		name: name, method: http.MethodPost, path: path, body: body,
		admin: true, operator: operator, timeout: commandTimeout(name),
	}
}

func parseAmount(fs *flagSet, raw string) (int64, error) {
	if !fs.has("amount") {
		return 0, errors.New("paycli: amount is required")
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, errors.New("paycli: amount must be an integer number of fen")
	}
	return n, nil
}

func requiredOperator(fs *flagSet, raw string) (string, error) {
	if !fs.text("operator", raw) {
		return "", errors.New("paycli: operator is required")
	}
	if strings.ContainsAny(raw, "\r\n") || utf8.RuneCountInString(raw) > 64 {
		return "", errors.New("paycli: operator is invalid")
	}
	return raw, nil
}

func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, errors.New("paycli: invalid arguments")
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func (p prepared) need(cfg config.PayCLI) error {
	if err := checkGateway(cfg.GatewayURL); err != nil {
		return err
	}
	if p.admin {
		if cfg.AdminToken == "" {
			return errors.New("paycli: admin token is required")
		}
		if strings.ContainsAny(cfg.AdminToken, "\r\n") {
			return errors.New("paycli: admin token is invalid")
		}
		return nil
	}
	if cfg.AppID == "" {
		return errors.New("paycli: app_id is required")
	}
	if strings.ContainsAny(cfg.AppID, "\r\n") {
		return errors.New("paycli: app_id is invalid")
	}
	if cfg.AppSecret == "" {
		return errors.New("paycli: app_secret is required")
	}
	return nil
}

func checkGateway(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return errors.New("paycli: gateway address is required")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || strings.ContainsAny(raw, "\r\n") {
		return errors.New("paycli: gateway address is invalid")
	}
	return nil
}
