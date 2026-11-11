package biz

import (
	"context"
	"sort"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/money"
	"github.com/yym108/pay-system/pkg/obs"
	"github.com/yym108/pay-system/pkg/shard"
)

// PostPay 在商户所属分片提交一笔支付成功凭证，然后返回。
// 手续费为 0 时不写手续费分录。手续费等于金额时省略金额为 0 的商户分录，因为 Post 拒绝金额 0。
// 这时先按账户 id 升序锁住渠道应收、手续费收入和商户，再看商户是不是 ACTIVE。
// 商户结算账户不存在返回 40001。平台子账户不存在返回 10003，留给消费端重试。
// 本方法自己提交事务。不调用 Kafka，不打故障点。不改 Post 的资金规则。
func (s *Service) PostPay(ctx context.Context, payNo, merchantID, channel string, amount, fee money.Amount) (PostResult, error) {
	shardNo, err := validatePostPay(payNo, merchantID, channel, amount, fee)
	if err != nil {
		return PostResult{}, err
	}
	if s == nil {
		return PostResult{}, errno.ErrInternal
	}
	if ctx == nil {
		return PostResult{}, errno.ErrInternal
	}
	if err = ctx.Err(); err != nil {
		return PostResult{}, err
	}
	if s.ids == nil || s.pools == nil || s.subCount < 1 {
		return PostResult{}, errno.ErrInternal
	}
	db, err := s.pools.ShardDB(shardNo)
	if err != nil {
		return PostResult{}, errno.Wrap(errno.ErrInternal, err)
	}
	var out PostResult
	endLedger := obs.Stage(ctx, obs.ChainPaySettle, obs.StageLedgerTx)
	err = dbx.WithTx(ctx, db, func(tx *sqlx.Tx) error {
		var applyErr error
		out, applyErr = applyPostPay(ctx, s, sqlFreezeTx{tx: tx}, tx, payNo, merchantID, channel, amount, fee)
		return applyErr
	})
	endLedger()
	if err != nil {
		return PostResult{}, internalErr(err)
	}
	return out, nil
}

func validatePostPay(payNo, merchantID, channel string, amount, fee money.Amount) (int, error) {
	if n := utf8.RuneCountInString(payNo); n == 0 || n > maxBizNoRunes {
		return 0, errno.ErrInvalidParam
	}
	shardNo, err := shard.Of(merchantID)
	if err != nil {
		return 0, errno.ErrInvalidParam
	}
	if !validChannelOwner(channel) {
		return 0, errno.ErrInvalidParam
	}
	if amount <= 0 || fee < 0 || fee > amount {
		return 0, errno.ErrInvalidParam
	}
	return shardNo, nil
}

func applyPostPay(ctx context.Context, s *Service, db sqlFreezeTx, tx *sqlx.Tx, payNo, merchantID, channel string, amount, fee money.Amount) (PostResult, error) {
	acct, found, err := db.selectMerchant(ctx, merchantID)
	if err != nil {
		return PostResult{}, internalErr(err)
	}
	if !found {
		return PostResult{}, errno.ErrAccountNotFound
	}
	subNo, err := shard.Sub(payNo, s.subCount)
	if err != nil {
		return PostResult{}, errno.Wrap(errno.ErrInternal, err)
	}
	channelID, found, err := db.selectOwnerAccount(ctx, ownerPlatform, channel, typeChannelReceivable, subNo)
	if err != nil {
		return PostResult{}, internalErr(err)
	}
	if !found || channelID == acct.ID {
		// 平台子账户缺失可在开户后重试。不要写成 40001，那是商户账户不存在。
		return PostResult{}, errno.ErrInternal
	}
	var feeID uint64
	if fee > 0 {
		var feeFound bool
		var feeErr error
		feeID, feeFound, feeErr = db.selectOwnerAccount(ctx, ownerPlatform, platformOwnerID, typeFeeIncome, subNo)
		if feeErr != nil {
			return PostResult{}, internalErr(feeErr)
		}
		if !feeFound || feeID == acct.ID || feeID == channelID {
			return PostResult{}, errno.ErrInternal
		}
	}
	entries := []Entry{{
		AccountID: channelID,
		Direction: directionDebit,
		Amount:    amount,
	}}
	if fee < amount {
		merchantAmount, subErr := money.Sub(amount, fee)
		if subErr != nil || merchantAmount <= 0 {
			return PostResult{}, errno.ErrInvalidParam
		}
		entries = append(entries, Entry{
			AccountID: acct.ID,
			Direction: directionCredit,
			Amount:    merchantAmount,
		})
	} else {
		// 没有商户分录时 Post 不会锁商户。先把三户按 id 升序锁住，避免先锁商户再让 Post 锁更小的 id。
		status, lockErr := lockEqualFeeAccounts(ctx, tx, acct.ID, channelID, feeID)
		if lockErr != nil {
			return PostResult{}, lockErr
		}
		if status != statusActive {
			return PostResult{}, errno.ErrAccountInactive
		}
	}
	if fee > 0 {
		entries = append(entries, Entry{
			AccountID: feeID,
			Direction: directionCredit,
			Amount:    fee,
		})
	}
	return s.Post(ctx, tx, bizPay, payNo, "", entries)
}

// lockEqualFeeAccounts 在同一事务里按账户 id 升序锁住三户，返回锁内读到的商户状态。
// 顺序与 Post 相同。先锁较大的商户 id，再让 Post 去锁较小的渠道 id，会和普通记账死锁。
func lockEqualFeeAccounts(ctx context.Context, tx *sqlx.Tx, merchantID, channelID, feeID uint64) (string, error) {
	ids := []uint64{channelID, feeID, merchantID}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	locks := sqlPostTx{tx: tx}
	var status string
	var lockedMerchant bool
	var prev uint64
	for i, id := range ids {
		if i > 0 && id == prev {
			return "", errno.ErrInternal
		}
		prev = id
		row, found, err := locks.lockAccount(ctx, id)
		if err != nil {
			return "", internalErr(err)
		}
		if !found || row.ID != id {
			return "", errno.ErrInternal
		}
		if id == merchantID {
			status = row.Status
			lockedMerchant = true
		}
	}
	if !lockedMerchant {
		return "", errno.ErrInternal
	}
	return status, nil
}
