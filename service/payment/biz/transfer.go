package biz

import (
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/shard"
)

const (
	transferNoPrefix           = "TF"
	transferStatusTrying       = "TRYING"
	transferStatusSuccess      = "SUCCESS"
	transferStatusFailed       = "FAILED"
	transferModeLocal          = "LOCAL"
	transferModeTCC            = "TCC"
	transferStatusConfirming   = "CONFIRMING"
	transferStatusCancelling   = "CANCELLING"
	transferFailInsufficient   = "INSUFFICIENT_BALANCE"
	transferFailAccountInvalid = "ACCOUNT_INVALID"
	transferFailTryTimeout     = "TRY_TIMEOUT"
	transferBizType            = "TRANSFER"
	maxRemarkRunes             = 128
	// defaultTransferRecoverDelay 与 TRANSFER_RECOVER_DELAY 的默认值一致，未调用 UseTransfer 时使用。
	defaultTransferRecoverDelay = 10 * time.Second
)

// CreateTransferInput 是创建转账的入参。MerchantID 是付款商户，只取自验签结果。
// Remark、NotifyURL 为 nil 表示未传。空串是已传的值：notify_url 传空串是 10001，remark 传空串按未传存 NULL。
type CreateTransferInput struct {
	MerchantID      string
	OutTransferNo   string
	PayeeMerchantID string
	Amount          money.Amount
	Remark          *string
	NotifyURL       *string
}

// TransferOrder 是 transfer_order 的一行。金额单位为分。时间已截断到毫秒。
// Remark、FailCode、TxnNo 的 SQL NULL 读成空串，FinishedAt 的 NULL 读成零值。
type TransferOrder struct {
	TransferNo      string
	MerchantID      string
	OutTransferNo   string
	PayeeMerchantID string
	Amount          money.Amount
	Currency        string
	Remark          string
	NotifyURL       string
	Mode            string
	PayerShard      int
	PayeeShard      int
	Status          string
	FailCode        string
	TxnNo           string
	RetryCount      int
	NextRetryAt     time.Time
	FinishedAt      time.Time
	Version         uint64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// TransferExecutor 推进一张刚提交的 TRYING 转账单：LOCAL 走 TRF-02，TCC 走 TRF-03。
// 返回执行结束时的转账单。结果未知时返回仍处于处理中的转账单和 nil error，由 TRF-04 接着推进。
// 返回 error 只表示执行本身出错，转账单已经提交，商户用相同参数重试会拿到已有单。
type TransferExecutor interface {
	ExecuteTransfer(ctx context.Context, row TransferOrder) (TransferOrder, error)
}

// transferStore 读写转账单。TRF-02 用 finishTransfer 把 TRYING 收成终态。
// TRF-03 用 decideTransfer 落盘决策，再用 finishTCCTransfer 收 TCC 终态。TRF-04 不在这里。
type transferStore interface {
	transferByMerchantOut(ctx context.Context, merchantID, outTransferNo string) (TransferOrder, bool, error)
	// transferByNo 按 transfer_no 读，不比较商户号；调用方按自己的规则比较（TRF-05）。
	transferByNo(ctx context.Context, transferNo string) (TransferOrder, bool, error)
	insertTransfer(ctx context.Context, row TransferOrder) error
	// finishTransfer 把仍是 TRYING 的行收成 SUCCESS 或 FAILED，并在同一次提交里写 outbox。
	// applied 为 false 表示 CAS 影响 0 行，current 是再读到的当前行，没有写 outbox。
	finishTransfer(ctx context.Context, row TransferOrder, status, failCode, txnNo string, at time.Time, ids *idgen.Generator, clk clock.Clock) (current TransferOrder, applied bool, err error)
	// decideTransfer 把 TRYING 收成 CONFIRMING 或 CANCELLING。不写 outbox。
	// CANCELLING 必须带 failCode。applied 为 false 时 current 是再读到的行。
	decideTransfer(ctx context.Context, row TransferOrder, status, failCode string, at time.Time) (current TransferOrder, applied bool, err error)
	// finishTCCTransfer 把 CONFIRMING 收成 SUCCESS，或把 CANCELLING 收成 FAILED。
	// SUCCESS 不写 txn_no。FAILED 不改已经写下的 fail_code。命中时同事务写 outbox。
	finishTCCTransfer(ctx context.Context, row TransferOrder, fromStatus, status, failCode string, at time.Time, ids *idgen.Generator, clk clock.Clock) (current TransferOrder, applied bool, err error)
}

// UseTransfer 装上转账恢复延迟和执行器。
// recoverDelay 是 TRANSFER_RECOVER_DELAY，写进新单的 next_retry_at；负数时保留默认的 10 秒。
// exec 为 nil 时不使用这份执行器。LOCAL 改看 LocalTransferrer，TCC 改看 TCCBrancher。
// 两条都没装时，CreateTransfer 返回刚插入的 TRYING 转账单。
func (p *Payments) UseTransfer(recoverDelay time.Duration, exec TransferExecutor) {
	if p == nil {
		return
	}
	if recoverDelay >= 0 {
		p.transferRecoverDelay = recoverDelay
	}
	p.transferExec = exec
}

// UseLocalTransfer 装上同分片执行（TRF-02）。
// local 为 nil 时不执行 LOCAL。TCC 不走这条路径。已装 TransferExecutor 时执行器优先，不调用 local。
func (p *Payments) UseLocalTransfer(local LocalTransferrer) {
	if p == nil {
		return
	}
	p.localTransfer = local
}

// UseTCCTransfer 装上跨分片协调（TRF-03）。
// branch 为 nil 时不执行 TCC。parallel 为 true 时两个分支并行，否则先付款方。
// 已装 TransferExecutor 时执行器优先，不调用 branch。
func (p *Payments) UseTCCTransfer(branch TCCBrancher, parallel bool) {
	if p == nil {
		return
	}
	p.tccTransfer = branch
	p.tccParallel = parallel
}

// CreateTransfer 转给转账创建。payments 为 nil 时返回 10003。
func (a *App) CreateTransfer(ctx context.Context, in *CreateTransferInput) (TransferOrder, error) {
	if a == nil || a.payments == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	return a.payments.CreateTransfer(ctx, in)
}

// CreateTransfer 插入一张 TRYING 转账单，提交后在同一请求内交给执行器（TRF-01）。
// 幂等键是 (merchant_id, out_transfer_no)。命中且 payee_merchant_id、amount、notify_url 一致时只读，
// 不论当前状态都原样返回，不再执行，也不再校验收款方；不一致返回 10006。没有已有单才校验收款方（30008）。
// 本次执行到 FAILED 且 fail_code = INSUFFICIENT_BALANCE 时返回该转账单和 40002；其他结果 error 为 nil。
func (p *Payments) CreateTransfer(ctx context.Context, in *CreateTransferInput) (TransferOrder, error) {
	if p == nil {
		return TransferOrder{}, errno.ErrInternal
	}
	req, err := p.normalizeTransfer(in)
	if err != nil {
		return TransferOrder{}, err
	}
	if err := p.transferReady(ctx); err != nil {
		return TransferOrder{}, err
	}
	// 幂等读在收款方校验之前：建单后收款方被停用，相同参数重试仍应拿到已有单。
	existing, ok, err := p.transfers.transferByMerchantOut(ctx, req.merchantID, req.outTransferNo)
	if err != nil {
		return TransferOrder{}, callErr(err)
	}
	if ok {
		return sameTransfer(existing, req)
	}
	if err := p.checkPayee(ctx, req.merchantID, req.payee); err != nil {
		return TransferOrder{}, err
	}
	transferNo, err := p.ids.NewNo(ctx, transferNoPrefix)
	if err != nil {
		if errors.Is(err, idgen.ErrClockRollback) {
			return TransferOrder{}, errno.ErrUnavailable
		}
		return TransferOrder{}, callErr(err)
	}
	now := p.clk.Now().Truncate(time.Millisecond)
	mode := transferModeTCC
	if req.payerShard == req.payeeShard {
		mode = transferModeLocal
	}
	row := TransferOrder{
		TransferNo:      transferNo,
		MerchantID:      req.merchantID,
		OutTransferNo:   req.outTransferNo,
		PayeeMerchantID: req.payee,
		Amount:          req.amount,
		Currency:        payCurrencyCNY,
		Remark:          req.remark,
		NotifyURL:       req.notifyURL,
		Mode:            mode,
		PayerShard:      req.payerShard,
		PayeeShard:      req.payeeShard,
		Status:          transferStatusTrying,
		NextRetryAt:     now.Add(p.transferRecoverDelay).Truncate(time.Millisecond),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := p.transfers.insertTransfer(ctx, row); err != nil {
		return p.onTransferInsertErr(ctx, req, err)
	}
	return p.executeTransfer(ctx, row)
}

type transferRequest struct {
	merchantID    string
	outTransferNo string
	payee         string
	amount        money.Amount
	remark        string
	notifyURL     string
	payerShard    int
	payeeShard    int
}

// normalizeTransfer 校验参数并按默认值填充。只做格式校验，不读库。
func (p *Payments) normalizeTransfer(in *CreateTransferInput) (transferRequest, error) {
	if in == nil {
		return transferRequest{}, errno.ErrInvalidParam
	}
	if in.OutTransferNo == "" || len(in.OutTransferNo) > maxOutTradeNoLen || !outTradeNoPattern.MatchString(in.OutTransferNo) {
		return transferRequest{}, errno.ErrInvalidParam
	}
	if err := money.ValidatePayAmount(in.Amount); err != nil {
		return transferRequest{}, errno.ErrInvalidParam
	}
	// 付款方来自验签结果，格式不对说明调用方传错，同样是参数错误。
	payerShard, err := shard.Of(in.MerchantID)
	if err != nil {
		return transferRequest{}, errno.ErrInvalidParam
	}
	payeeShard, err := shard.Of(in.PayeeMerchantID)
	if err != nil {
		return transferRequest{}, errno.ErrInvalidParam
	}
	remark := ""
	if in.Remark != nil {
		if !utf8.ValidString(*in.Remark) || utf8.RuneCountInString(*in.Remark) > maxRemarkRunes {
			return transferRequest{}, errno.ErrInvalidParam
		}
		remark = *in.Remark
	}
	notify := ""
	if in.NotifyURL != nil {
		if err := p.checkNotifyURL(*in.NotifyURL); err != nil {
			return transferRequest{}, err
		}
		notify = *in.NotifyURL
	}
	return transferRequest{
		merchantID:    in.MerchantID,
		outTransferNo: in.OutTransferNo,
		payee:         in.PayeeMerchantID,
		amount:        in.Amount,
		remark:        remark,
		notifyURL:     notify,
		payerShard:    payerShard,
		payeeShard:    payeeShard,
	}, nil
}

func (p *Payments) transferReady(ctx context.Context) error {
	if p.merchants == nil || p.ids == nil || p.clk == nil || p.transfers == nil {
		return errno.ErrInternal
	}
	if ctx == nil {
		return errno.ErrInternal
	}
	return ctx.Err()
}

// checkPayee 要求收款商户存在、ACTIVE 且不是付款商户本身（TRF-01 规则 2）。
// 商户配置可以来自 MERCHANT_CONFIG_CACHE_TTL 内的缓存。读库失败不是 30008。
func (p *Payments) checkPayee(ctx context.Context, payer, payee string) error {
	if payer == payee {
		return errno.ErrPayeeInvalid
	}
	cfg, err := p.merchants.MerchantConfig(ctx, payee)
	if errors.Is(err, errno.ErrMerchantInvalid) {
		return errno.ErrPayeeInvalid
	}
	if err != nil {
		return callErr(err)
	}
	if cfg.MerchantID != payee || cfg.Status != payStatusActive {
		return errno.ErrPayeeInvalid
	}
	return nil
}

func sameTransfer(existing TransferOrder, req transferRequest) (TransferOrder, error) {
	if existing.PayeeMerchantID == req.payee && existing.Amount == req.amount && existing.NotifyURL == req.notifyURL {
		return existing, nil
	}
	return TransferOrder{}, errno.ErrIdempotentConflict
}

// onTransferInsertErr 处理插入失败。1062 之后在这条自动提交的插入之外再读，才能看见已提交的赢者。
// 赢者负责执行，这里只返回它的当前状态，不再校验收款方。
func (p *Payments) onTransferInsertErr(ctx context.Context, req transferRequest, err error) (TransferOrder, error) {
	if !dbx.IsDuplicate(err) {
		return TransferOrder{}, callErr(err)
	}
	existing, ok, readErr := p.transfers.transferByMerchantOut(ctx, req.merchantID, req.outTransferNo)
	if readErr != nil {
		return TransferOrder{}, callErr(readErr)
	}
	if !ok {
		// 冲突的是 uk_transfer_no 而不是幂等键，说明发号撞号，不能当作幂等命中。
		return TransferOrder{}, errno.ErrInternal
	}
	return sameTransfer(existing, req)
}

// executeTransfer 在插入提交之后执行转账。事务外调用账户，CAS 在各自的事务里。
// 注入的执行器优先，用来保住 TRF-01 的测试。没有执行器时，LOCAL 走 LocalTransferrer，TCC 走 TCCBrancher。
func (p *Payments) executeTransfer(ctx context.Context, row TransferOrder) (TransferOrder, error) {
	var got TransferOrder
	var err error
	switch {
	case p.transferExec != nil:
		got, err = p.transferExec.ExecuteTransfer(ctx, row)
	case row.Mode == transferModeLocal && p.localTransfer != nil:
		got, err = p.executeLocal(ctx, row)
	case row.Mode == transferModeTCC && p.tccTransfer != nil:
		got, err = p.executeTCC(ctx, row)
	default:
		return row, nil
	}
	if err != nil {
		return TransferOrder{}, callErr(err)
	}
	// 只有本次请求执行到余额不足才带 40002；幂等命中已失败的单走 sameTransfer，返回 nil error。
	if got.Status == transferStatusFailed && got.FailCode == transferFailInsufficient {
		return got, errno.ErrInsufficientBalance
	}
	return got, nil
}
