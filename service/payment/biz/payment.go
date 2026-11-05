package biz

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

const (
	payNoPrefix          = "PA"
	payStatusInit        = "INIT"
	payStatusActive      = "ACTIVE"
	payCurrencyCNY       = "CNY"
	defaultExpireMinutes = 15
	minExpireMinutes     = 1
	maxExpireMinutes     = 120
	maxSubjectRunes      = 128
	maxNotifyURLRunes    = 512
	maxOutTradeNoLen     = 64
)

var outTradeNoPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// MerchantLookup 读取商户状态和按下单时刻生效的费率。
// 状态可以走调用方自己的缓存；费率不得缓存。
type MerchantLookup interface {
	MerchantConfig(ctx context.Context, merchantID string) (MerchantConfig, error)
	FeeRateAt(ctx context.Context, merchantID string, at time.Time) (FeeRate, bool, error)
}

// Numberer 发放业务单号。prefix 必须已在约定 §2.2 登记。
type Numberer interface {
	NewNo(ctx context.Context, prefix string) (string, error)
}

// orderStore 读写支付单和渠道流水。
// 假实现只给不连库的单元测试。状态只通过 casInitToPaying、casInitToClosed 做 CAS，回填不改状态。
type orderStore interface {
	insertPayment(ctx context.Context, row PaymentOrder) error
	paymentByOutTradeNo(ctx context.Context, merchantID, outTradeNo string) (PaymentOrder, bool, error)
	paymentByPayNo(ctx context.Context, payNo string) (PaymentOrder, bool, error)
	casInitToPaying(ctx context.Context, order PaymentOrder, now time.Time) (bool, error)
	casInitToClosed(ctx context.Context, order PaymentOrder, now time.Time) (bool, error)
	fillChannelTxnNo(ctx context.Context, bizNo, txnNo string) (int64, error)
	channelTxnByBizNo(ctx context.Context, bizNo string) (channelTxn, bool, error)
	refundByMerchantOut(ctx context.Context, merchantID, outRefundNo string) (RefundOrder, bool, error)
	occupyRefund(ctx context.Context, pay PaymentOrder, refund RefundOrder) (affected bool, duplicate bool, err error)
	casRefundProcessing(ctx context.Context, row RefundOrder, now time.Time) (bool, error)
	refundByNo(ctx context.Context, refundNo string) (RefundOrder, bool, error)
}

// CreatePaymentInput 是创建支付单的入参。
// 指针为 nil 表示调用方未传该字段。空串和 0 是已传的值，不是缺省。
type CreatePaymentInput struct {
	MerchantID    string
	OutTradeNo    string
	Amount        money.Amount
	Currency      *string
	Subject       string
	NotifyURL     *string
	ExpireMinutes *int32
}

// PaymentOrder 是 payment_order 的一行。金额单位为分。
// CreatedAt、UpdatedAt、ExpireAt 来自同一次注入时钟，已截断到毫秒。
type PaymentOrder struct {
	PayNo          string
	MerchantID     string
	OutTradeNo     string
	Amount         money.Amount
	FeeAmount      money.Amount
	RefundedAmount money.Amount
	Currency       string
	Subject        string
	Status         string
	NotifyURL      string
	Channel        string
	FailCode       string
	FailMsg        string
	Version        uint64
	ExpireAt       time.Time
	PaidAt         time.Time
	ClosedAt       time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Payments 创建支付单。可被多个 goroutine 同时使用。
// 商户状态由 MerchantLookup 决定是否缓存；费率每次按时刻读取，本类型不缓存。
type Payments struct {
	merchants        MerchantLookup
	ids              Numberer
	clk              clock.Clock
	hosts            []string
	orders           orderStore
	channel          ChannelAcceptor
	applier          PayResultApplier
	closer           channelCloser
	channelNotifyURL string
	// channelTimeout 大于 0 时替换渠道受理总超时。生产保持 0，用 PayChannelTimeout。
	channelTimeout time.Duration
	refundFreezer  RefundFreezer
	refundChannel  ChannelRefunder
	refundApplier  RefundResultApplier
}

// App 把商户读取和支付单创建合成 handler 使用的服务。
// 子对象为 nil 时，对应方法返回 10003。
type App struct {
	merchants *Merchants
	payments  *Payments
}

// NewApp 组装支付服务。任一参数为 nil 时，对应方法返回 10003。
func NewApp(merchants *Merchants, payments *Payments) *App {
	return &App{merchants: merchants, payments: payments}
}

// GetMerchantCredential 转给商户读取。行为与 Merchants.GetMerchantCredential 相同。
func (a *App) GetMerchantCredential(ctx context.Context, appID string) (MerchantCredential, error) {
	if a == nil || a.merchants == nil {
		return MerchantCredential{}, errno.ErrInternal
	}
	return a.merchants.GetMerchantCredential(ctx, appID)
}

// CreatePayment 转给支付单创建。
func (a *App) CreatePayment(ctx context.Context, in *CreatePaymentInput) (PaymentOrder, error) {
	if a == nil || a.payments == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	return a.payments.CreatePayment(ctx, in)
}

// Pay 转给发起支付。
func (a *App) Pay(ctx context.Context, in *PayInput) (PaymentOrder, error) {
	if a == nil || a.payments == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	return a.payments.Pay(ctx, in)
}

// ClosePayment 转给商户关单。
func (a *App) ClosePayment(ctx context.Context, in *CloseInput) (PaymentOrder, error) {
	if a == nil || a.payments == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	return a.payments.ClosePayment(ctx, in)
}

// QueryPayment 转给支付单查询。
func (a *App) QueryPayment(ctx context.Context, in *QueryInput) (PaymentOrder, error) {
	if a == nil || a.payments == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	return a.payments.QueryPayment(ctx, in)
}

// ApplyChannelNotify 转给渠道回调。
func (a *App) ApplyChannelNotify(ctx context.Context, in PayResult) (string, error) {
	if a == nil || a.payments == nil {
		return "", errno.ErrInternal
	}
	return a.payments.ApplyChannelNotify(ctx, in)
}

// ApplyRefundNotify 转给退款回调。
func (a *App) ApplyRefundNotify(ctx context.Context, in RefundResult) (string, error) {
	if a == nil || a.payments == nil {
		return "", errno.ErrInternal
	}
	return a.payments.ApplyRefundNotify(ctx, in)
}

func newPayments(merchants MerchantLookup, ids Numberer, clk clock.Clock, allowHosts string, orders orderStore) *Payments {
	return &Payments{
		merchants: merchants,
		ids:       ids,
		clk:       clk,
		hosts:     parseAllowHosts(allowHosts),
		orders:    orders,
	}
}

// NewPayments 使用调用方传入的库写入支付单。
// db 为 nil 时创建支付单返回 10003。allowHosts 是逗号分隔的通知主机白名单。
func NewPayments(merchants MerchantLookup, ids Numberer, clk clock.Clock, allowHosts string, db *sqlx.DB) *Payments {
	var orders orderStore
	if db != nil {
		orders = sqlOrderStore{db: db}
	}
	return newPayments(merchants, ids, clk, allowHosts, orders)
}

func parseAllowHosts(raw string) []string {
	var hosts []string
	for _, part := range strings.Split(raw, ",") {
		host := strings.TrimSpace(part)
		if host == "" {
			continue
		}
		hosts = append(hosts, host)
	}
	return hosts
}

// CreatePayment 插入一条 INIT 支付单。
// 幂等键是 (merchant_id, out_trade_no)。命中已有行时只读，不更新。
// 未传 notify_url 存空串。商户不是 ACTIVE 返回 20004。没有生效费率档返回 10003。
func (p *Payments) CreatePayment(ctx context.Context, in *CreatePaymentInput) (PaymentOrder, error) {
	if p == nil {
		return PaymentOrder{}, errno.ErrInternal
	}
	if in == nil {
		return PaymentOrder{}, errno.ErrInvalidParam
	}
	filled, err := p.normalize(in)
	if err != nil {
		return PaymentOrder{}, err
	}
	if err = p.ready(ctx); err != nil {
		return PaymentOrder{}, err
	}
	// DATETIME(3) 的四舍五入会和取档时刻错开，先截到毫秒再取档、再写入。
	now := p.clk.Now().Truncate(time.Millisecond)
	cfg, err := p.merchants.MerchantConfig(ctx, filled.merchantID)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if cfg.Status != payStatusActive {
		return PaymentOrder{}, errno.ErrMerchantInvalid
	}
	rate, found, err := p.merchants.FeeRateAt(ctx, filled.merchantID, now)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	if !found {
		slog.ErrorContext(ctx, "no effective fee rate", "merchant_id", filled.merchantID)
		return PaymentOrder{}, errno.ErrInternal
	}
	fee, err := money.Fee(filled.amount, rate.RateBP)
	if err != nil {
		return PaymentOrder{}, callErr(err)
	}
	payNo, err := p.ids.NewNo(ctx, payNoPrefix)
	if err != nil {
		if errors.Is(err, idgen.ErrClockRollback) {
			return PaymentOrder{}, errno.ErrUnavailable
		}
		return PaymentOrder{}, callErr(err)
	}
	row := PaymentOrder{
		PayNo:      payNo,
		MerchantID: filled.merchantID,
		OutTradeNo: filled.outTradeNo,
		Amount:     filled.amount,
		FeeAmount:  fee,
		Currency:   filled.currency,
		Subject:    filled.subject,
		Status:     payStatusInit,
		NotifyURL:  filled.notifyURL,
		ExpireAt:   now.Add(time.Duration(filled.expireMinutes) * time.Minute),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err = p.orders.insertPayment(ctx, row); err != nil {
		return p.onInsertErr(ctx, row, err)
	}
	return row, nil
}

func (p *Payments) ready(ctx context.Context) error {
	if p.merchants == nil || p.ids == nil || p.clk == nil || p.orders == nil {
		return errno.ErrInternal
	}
	if ctx == nil {
		return errno.ErrInternal
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

type filledPayment struct {
	merchantID    string
	outTradeNo    string
	amount        money.Amount
	currency      string
	subject       string
	notifyURL     string
	expireMinutes int32
}

func (p *Payments) normalize(in *CreatePaymentInput) (filledPayment, error) {
	if in.OutTradeNo == "" || len(in.OutTradeNo) > maxOutTradeNoLen || !utf8.ValidString(in.OutTradeNo) || !outTradeNoPattern.MatchString(in.OutTradeNo) {
		return filledPayment{}, errno.ErrInvalidParam
	}
	if err := money.ValidatePayAmount(in.Amount); err != nil {
		return filledPayment{}, errno.ErrInvalidParam
	}
	if in.Subject == "" || !utf8.ValidString(in.Subject) || utf8.RuneCountInString(in.Subject) > maxSubjectRunes {
		return filledPayment{}, errno.ErrInvalidParam
	}
	currency := payCurrencyCNY
	if in.Currency != nil && *in.Currency != "" {
		if !utf8.ValidString(*in.Currency) || *in.Currency != payCurrencyCNY {
			return filledPayment{}, errno.ErrInvalidParam
		}
	}
	minutes := int32(defaultExpireMinutes)
	if in.ExpireMinutes != nil {
		minutes = *in.ExpireMinutes
		if minutes < minExpireMinutes || minutes > maxExpireMinutes {
			return filledPayment{}, errno.ErrInvalidParam
		}
	}
	notify := ""
	if in.NotifyURL != nil {
		if err := p.checkNotifyURL(*in.NotifyURL); err != nil {
			return filledPayment{}, err
		}
		notify = *in.NotifyURL
	}
	return filledPayment{
		merchantID:    in.MerchantID,
		outTradeNo:    in.OutTradeNo,
		amount:        in.Amount,
		currency:      currency,
		subject:       in.Subject,
		notifyURL:     notify,
		expireMinutes: minutes,
	}, nil
}

func (p *Payments) checkNotifyURL(raw string) error {
	if raw == "" || !utf8.ValidString(raw) || utf8.RuneCountInString(raw) > maxNotifyURLRunes {
		return errno.ErrInvalidParam
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return errno.ErrInvalidParam
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errno.ErrInvalidParam
	}
	host := parsed.Hostname()
	if host == "" || !p.hostAllowed(host) {
		return errno.ErrInvalidParam
	}
	return nil
}

func (p *Payments) hostAllowed(host string) bool {
	for _, allowed := range p.hosts {
		if strings.EqualFold(allowed, host) {
			return true
		}
	}
	return false
}

func (p *Payments) onInsertErr(ctx context.Context, row PaymentOrder, err error) (PaymentOrder, error) {
	if !dbx.IsDuplicate(err) {
		return PaymentOrder{}, callErr(err)
	}
	// 1062 之后在这条自动提交的插入之外读取，才能看见已提交的赢者。
	existing, ok, readErr := p.orders.paymentByOutTradeNo(ctx, row.MerchantID, row.OutTradeNo)
	if readErr != nil {
		return PaymentOrder{}, callErr(readErr)
	}
	if !ok || existing.MerchantID != row.MerchantID || existing.OutTradeNo != row.OutTradeNo {
		return PaymentOrder{}, errno.ErrInternal
	}
	if existing.Amount == row.Amount && existing.Currency == row.Currency &&
		existing.Subject == row.Subject && existing.NotifyURL == row.NotifyURL {
		return existing, nil
	}
	return PaymentOrder{}, errno.ErrIdempotentConflict
}
