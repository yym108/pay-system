//go:build integration

package biz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	accountbiz "github.com/yym108/pay-system/service/account/biz"
	accountdal "github.com/yym108/pay-system/service/account/dal"

	"github.com/yym108/pay-system/internal/testkit"
	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/errno"
	"github.com/yym108/pay-system/pkg/fault"
	"github.com/yym108/pay-system/pkg/idgen"
	"github.com/yym108/pay-system/pkg/money"
)

const (
	transferCrashChildEnv = "PAY_TRF04_CHILD"
	transferCrashNow      = "PAY_TRF04_NOW"
	transferCrashOut      = "PAY_TRF04_OUT"
	transferCrashParallel = "PAY_TRF04_PARALLEL"
	trf04Amount           = money.Amount(100)
)

// runTransferCrashChild 是被 kill 的 payment：FAULT_POINTS 在子进程里打开，空配置是空操作。
// DSN 从标准输入读，依次是支付库、账户分片 0、账户分片 1。
func runTransferCrashChild() int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fault.Init(os.Getenv("FAULT_POINTS")); err != nil {
		fmt.Fprintf(os.Stderr, "fault init: %v\n", err)
		return 1
	}
	now, err := time.Parse(time.RFC3339Nano, os.Getenv(transferCrashNow))
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse now: %v\n", err)
		return 1
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read stdin: %v\n", err)
		return 1
	}
	dsns := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(dsns) != 3 {
		fmt.Fprintf(os.Stderr, "want 3 dsn lines, got %d\n", len(dsns))
		return 1
	}
	var dbs [3]*sqlx.DB
	for i, dsn := range dsns {
		if dbs[i], err = dbx.Open(ctx, dsn, dbx.PoolConfig{}); err != nil {
			fmt.Fprintf(os.Stderr, "open dsn %d failed\n", i)
			return 1
		}
	}
	clk := &clock.Fake{}
	clk.Set(now)
	ids, err := idgen.New(13, clk)
	if err != nil {
		fmt.Fprintf(os.Stderr, "idgen: %v\n", err)
		return 1
	}
	accIDs, err := idgen.New(14, clk)
	if err != nil {
		fmt.Fprintf(os.Stderr, "idgen: %v\n", err)
		return 1
	}
	accounts := accountbiz.New(accountdal.New(dbs[1], dbs[2]), accIDs, 8)
	p := NewPayments(NewMerchants(dbs[0], nil, clk, 0), ids, clk, "merchant-demo", dbs[0])
	p.UseTransfer(10*time.Second, nil)
	p.UseTCCTransfer(accountBizTCC{svc: accounts}, os.Getenv(transferCrashParallel) == "true")
	got, err := p.CreateTransfer(ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: os.Getenv(transferCrashOut),
		PayeeMerchantID: "M100002", Amount: trf04Amount,
	})
	fmt.Fprintf(os.Stderr, "not killed: status=%s err=%v\n", got.Status, err)
	return childNotKilledExit
}

func filterChildEnv(extra ...string) []string {
	drop := []string{"FAULT_POINTS=", transferCrashChildEnv + "=", transferCrashNow + "=", transferCrashOut + "=", transferCrashParallel + "="}
	var out []string
	for _, item := range os.Environ() {
		skip := false
		for _, prefix := range drop {
			if strings.HasPrefix(item, prefix) {
				skip = true
				break
			}
		}
		if !skip {
			out = append(out, item)
		}
	}
	return append(out, extra...)
}

type transferCrashWorld struct {
	env      payEnv
	p        *Payments
	accounts *accountbiz.Service
	s1       *sqlx.DB
	s0       *sqlx.DB
	schema0  *testkit.Schema
	schema1  *testkit.Schema
	now      time.Time
}

func openTransferCrash(t *testing.T, seedBiz string) transferCrashWorld {
	t.Helper()
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
	keepSchemas(t, schema0, schema1)
	openTCCBooks(t, env.ctx, accounts)
	fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 500, seedBiz)
	return transferCrashWorld{env: env, p: p, accounts: accounts, s1: s1, s0: schema0.DB, schema0: schema0, schema1: schema1, now: now}
}

func killTransferChild(t *testing.T, w transferCrashWorld, point, out string, parallel bool) {
	t.Helper()
	par := "false"
	if parallel {
		par = "true"
	}
	cmd := exec.CommandContext(w.env.ctx, os.Args[0]) //nolint:gosec // 重新执行当前测试二进制，不带额外参数。
	cmd.Env = filterChildEnv(
		transferCrashChildEnv+"=1",
		"FAULT_POINTS="+point,
		transferCrashNow+"="+w.now.Format(time.RFC3339Nano),
		transferCrashOut+"="+out,
		transferCrashParallel+"="+par,
	)
	cmd.Stdin = strings.NewReader(strings.Join(
		[]string{w.env.schema.DSN(), w.schema0.DSN(), w.schema1.DSN()}, "\n"))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil || !strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("child err=%v stderr=%s", err, testkit.Redact(errors.New(stderr.String())))
	}
}

func transferNoByOut(t *testing.T, env payEnv, merchantID, out string) string {
	t.Helper()
	var no string
	err := env.db.QueryRowContext(env.ctx,
		`SELECT transfer_no FROM transfer_order WHERE merchant_id = ? AND out_transfer_no = ?`, merchantID, out).Scan(&no)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return no
}

func runTransferBatch(t *testing.T, ctx context.Context, p *Payments) {
	t.Helper()
	if err := p.RunTransferRecoverBatch(ctx); err != nil {
		t.Fatalf("RunTransferRecoverBatch: %v", err)
	}
}

func wireTransferRecover(p *Payments, accounts *accountbiz.Service, parallel bool) {
	p.UseLocalTransfer(accountBizLocal{svc: accounts})
	p.UseTCCTransfer(accountBizTCC{svc: accounts}, parallel)
}

func countVoucher(t *testing.T, ctx context.Context, db *sqlx.DB, bizType, bizNo string) int {
	t.Helper()
	return countBizTxn(t, ctx, db, bizType, bizNo)
}

func transitBoth(t *testing.T, ctx context.Context, s0, s1 *sqlx.DB) int64 {
	t.Helper()
	return transitSum(t, ctx, s0) + transitSum(t, ctx, s1)
}

func loadUpdatedAt(t *testing.T, env payEnv, transferNo string) time.Time {
	t.Helper()
	var at time.Time
	if err := env.db.QueryRowContext(env.ctx, `SELECT updated_at FROM transfer_order WHERE transfer_no = ?`, transferNo).Scan(&at); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return at
}

func insertStuckTransfer(t *testing.T, env payEnv, no, out, merchant, payee, mode, status, failCode string, payerShard, payeeShard, retry int, amount int64, next, created time.Time, version int) {
	t.Helper()
	var fail any
	if failCode != "" {
		fail = failCode
	}
	_, err := env.db.ExecContext(env.ctx, `
INSERT INTO transfer_order (
  transfer_no, merchant_id, out_transfer_no, payee_merchant_id, amount, currency, remark, notify_url,
  mode, payer_shard, payee_shard, status, fail_code, txn_no, retry_count, next_retry_at, finished_at,
  version, created_at, updated_at
) VALUES (
  ?, ?, ?, ?, ?, 'CNY', NULL, '',
  ?, ?, ?, ?, ?, NULL, ?, ?, NULL,
  ?, ?, ?
)`, no, merchant, out, payee, amount, mode, payerShard, payeeShard, status, fail, retry, next, version, created, created)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
}

// TRF-04 验收 1：transfer.after_try 杀掉 payment 后，恢复任务把转账做成 FAILED，两个分支都已取消，冻结已释放。
func TestTransferRecoverAfterTryKill(t *testing.T) {
	w := openTransferCrash(t, "PAY-TRF04-C9")
	killTransferChild(t, w, faultTransferAfterTry, "TRF04-C9", true)
	no := transferNoByOut(t, w.env, "M100001", "TRF04-C9")
	row := loadTransferRow(t, w.env, no)
	if row.status != transferStatusTrying || row.version != 0 || row.failCode.Valid || row.mode != transferModeTCC {
		t.Fatalf("after kill row = %+v", row)
	}
	if freezeStatus(t, w.env.ctx, w.s1, no) != "FROZEN" {
		t.Fatal("freeze not held")
	}
	if payeeBranchStatus(t, w.env.ctx, w.s0, no) != "TRIED" {
		t.Fatal("payee not tried")
	}
	if countVoucher(t, w.env.ctx, w.s1, "TRANSFER_OUT", no) != 0 || countVoucher(t, w.env.ctx, w.s0, "TRANSFER_IN", no) != 0 {
		t.Fatal("voucher before recover")
	}
	wireTransferRecover(w.p, w.accounts, true)
	paymentsClock(t, w.p).Set(w.now.Add(10 * time.Second))
	runTransferBatch(t, w.env.ctx, w.p)
	row = loadTransferRow(t, w.env, no)
	if row.status != transferStatusFailed || !row.failCode.Valid || row.failCode.String != transferFailTryTimeout ||
		row.version != 2 || row.txnNo.Valid || !row.finishedAt.Valid {
		t.Fatalf("after recover row = %+v", row)
	}
	if freezeStatus(t, w.env.ctx, w.s1, no) != "RELEASED" {
		t.Fatal("freeze not released")
	}
	if payeeBranchStatus(t, w.env.ctx, w.s0, no) != "CANCELLED" {
		t.Fatal("payee not cancelled")
	}
	if countVoucher(t, w.env.ctx, w.s1, "TRANSFER_OUT", no) != 0 || countVoucher(t, w.env.ctx, w.s0, "TRANSFER_IN", no) != 0 ||
		countVoucher(t, w.env.ctx, w.s1, "TRANSFER", no) != 0 {
		t.Fatal("unexpected voucher")
	}
	box := loadOutbox(t, w.env, no)
	if len(box) != 1 || box[0].eventType != "transfer.failed" {
		t.Fatalf("outbox = %+v", box)
	}
	requireCheck(t, w.env.ctx, w.accounts)
}

// TRF-04 验收 2：transfer.after_decision 杀掉后，恢复任务确认为 SUCCESS，TRANSFER_OUT 与 TRANSFER_IN 各 1 张。
func TestTransferRecoverAfterDecisionKill(t *testing.T) {
	w := openTransferCrash(t, "PAY-TRF04-C10")
	killTransferChild(t, w, faultTransferAfterDecision, "TRF04-C10", true)
	no := transferNoByOut(t, w.env, "M100001", "TRF04-C10")
	row := loadTransferRow(t, w.env, no)
	if row.status != transferStatusConfirming || row.version != 1 || row.failCode.Valid {
		t.Fatalf("after kill row = %+v", row)
	}
	if countVoucher(t, w.env.ctx, w.s1, "TRANSFER_OUT", no) != 0 || countVoucher(t, w.env.ctx, w.s0, "TRANSFER_IN", no) != 0 {
		t.Fatal("voucher before confirm")
	}
	wireTransferRecover(w.p, w.accounts, true)
	paymentsClock(t, w.p).Set(w.now.Add(10 * time.Second))
	runTransferBatch(t, w.env.ctx, w.p)
	row = loadTransferRow(t, w.env, no)
	if row.status != transferStatusSuccess || row.version != 2 || row.txnNo.Valid || row.failCode.Valid || !row.finishedAt.Valid {
		t.Fatalf("after recover row = %+v", row)
	}
	if countVoucher(t, w.env.ctx, w.s1, "TRANSFER_OUT", no) != 1 || countVoucher(t, w.env.ctx, w.s0, "TRANSFER_IN", no) != 1 {
		t.Fatal("voucher count")
	}
	if transitBoth(t, w.env.ctx, w.s0, w.s1) != 0 {
		t.Fatal("transit not zero")
	}
	box := loadOutbox(t, w.env, no)
	if len(box) != 1 || box[0].eventType != "transfer.succeeded" {
		t.Fatalf("outbox = %+v", box)
	}
	requireCheck(t, w.env.ctx, w.accounts)
}

// TRF-04 验收 3：串行模式在 transfer.after_payer_confirm 杀掉。重启前在途为 T，恢复后 SUCCESS 且在途为 0。
func TestTransferRecoverAfterPayerConfirmKill(t *testing.T) {
	w := openTransferCrash(t, "PAY-TRF04-C11")
	killTransferChild(t, w, faultTransferAfterPayerConfirm, "TRF04-C11", false)
	no := transferNoByOut(t, w.env, "M100001", "TRF04-C11")
	row := loadTransferRow(t, w.env, no)
	if row.status != transferStatusConfirming || row.version != 1 || row.failCode.Valid {
		t.Fatalf("after kill row = %+v", row)
	}
	if freezeStatus(t, w.env.ctx, w.s1, no) != "CONFIRMED" {
		t.Fatal("freeze not confirmed")
	}
	if payeeBranchStatus(t, w.env.ctx, w.s0, no) != "TRIED" {
		t.Fatal("payee not tried")
	}
	if countVoucher(t, w.env.ctx, w.s1, "TRANSFER_OUT", no) != 1 || countVoucher(t, w.env.ctx, w.s0, "TRANSFER_IN", no) != 0 {
		t.Fatal("vouchers before recover")
	}
	if got := transitBoth(t, w.env.ctx, w.s0, w.s1); got != int64(trf04Amount) {
		t.Fatalf("transit before = %d", got)
	}
	wireTransferRecover(w.p, w.accounts, false)
	paymentsClock(t, w.p).Set(w.now.Add(10 * time.Second))
	runTransferBatch(t, w.env.ctx, w.p)
	row = loadTransferRow(t, w.env, no)
	if row.status != transferStatusSuccess || row.version != 2 || row.txnNo.Valid {
		t.Fatalf("after recover row = %+v", row)
	}
	if countVoucher(t, w.env.ctx, w.s1, "TRANSFER_OUT", no) != 1 || countVoucher(t, w.env.ctx, w.s0, "TRANSFER_IN", no) != 1 {
		t.Fatal("vouchers after")
	}
	if transitBoth(t, w.env.ctx, w.s0, w.s1) != 0 {
		t.Fatal("transit after")
	}
	requireCheck(t, w.env.ctx, w.accounts)
}

type downTCC struct {
	next         accountBizTCC
	down         atomic.Bool
	cancelBefore atomic.Bool
}

func (d *downTCC) Freeze(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	if d.cancelBefore.Load() {
		return context.Canceled
	}
	if d.down.Load() {
		return errno.ErrUnavailable
	}
	return d.next.Freeze(ctx, transferNo, payer, amount)
}

func (d *downTCC) TryPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	if d.down.Load() {
		return errno.ErrUnavailable
	}
	return d.next.TryPayee(ctx, transferNo, payee, amount)
}

func (d *downTCC) ConfirmPayer(ctx context.Context, transferNo, payer string) error {
	if d.down.Load() {
		return errno.ErrUnavailable
	}
	return d.next.ConfirmPayer(ctx, transferNo, payer)
}

func (d *downTCC) ConfirmPayee(ctx context.Context, transferNo, payee string) error {
	if d.down.Load() {
		return errno.ErrUnavailable
	}
	return d.next.ConfirmPayee(ctx, transferNo, payee)
}

func (d *downTCC) CancelPayer(ctx context.Context, transferNo, payer string, amount money.Amount) error {
	if d.down.Load() {
		return errno.ErrUnavailable
	}
	return d.next.CancelPayer(ctx, transferNo, payer, amount)
}

func (d *downTCC) CancelPayee(ctx context.Context, transferNo, payee string, amount money.Amount) error {
	if d.down.Load() {
		return errno.ErrUnavailable
	}
	return d.next.CancelPayee(ctx, transferNo, payee, amount)
}

func vouchersOf(t *testing.T, ctx context.Context, s0, s1 *sqlx.DB, no string) (outN, inN int) {
	t.Helper()
	return countVoucher(t, ctx, s1, "TRANSFER_OUT", no), countVoucher(t, ctx, s0, "TRANSFER_IN", no)
}

// TRF-04 验收 4：account 停止 1 分钟期间的 TCC 转账，恢复后全部终态，已终态的不再多出凭证。
func TestTransferRecoverAccountOutage(t *testing.T) {
	w := openTransferCrash(t, "PAY-TRF04-DOWN")
	branch := &downTCC{next: accountBizTCC{svc: w.accounts}}
	w.p.UseTCCTransfer(branch, true)
	okOrder, err := w.p.CreateTransfer(w.env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF04-UP", PayeeMerchantID: "M100002", Amount: trf04Amount,
	})
	if err != nil || okOrder.Status != transferStatusSuccess {
		t.Fatalf("up = %+v err=%v", okOrder, err)
	}
	if outN, inN := vouchersOf(t, w.env.ctx, w.s0, w.s1, okOrder.TransferNo); outN != 1 || inN != 1 {
		t.Fatalf("success vouchers out=%d in=%d", outN, inN)
	}

	branch.cancelBefore.Store(true)
	_, err = w.p.CreateTransfer(w.env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF04-CANCEL", PayeeMerchantID: "M100002", Amount: trf04Amount,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel-before err=%v", err)
	}
	tryingNo := transferNoByOut(t, w.env, "M100001", "TRF04-CANCEL")
	tryingRow := loadTransferRow(t, w.env, tryingNo)
	if tryingRow.status != transferStatusTrying || tryingRow.version != 0 {
		t.Fatalf("trying = %+v", tryingRow)
	}

	branch.cancelBefore.Store(false)
	branch.down.Store(true)
	cancelling, err := w.p.CreateTransfer(w.env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF04-UNAVAIL", PayeeMerchantID: "M100002", Amount: trf04Amount,
	})
	if err != nil || cancelling.Status != transferStatusCancelling || cancelling.FailCode != transferFailTryTimeout || cancelling.Version != 1 {
		t.Fatalf("down = %+v err=%v", cancelling, err)
	}
	if outN, inN := vouchersOf(t, w.env.ctx, w.s0, w.s1, cancelling.TransferNo); outN != 0 || inN != 0 {
		t.Fatalf("down vouchers out=%d in=%d", outN, inN)
	}

	clk := paymentsClock(t, w.p)
	t0 := w.now
	deadline := t0.Add(time.Minute)
	at := t0.Add(10 * time.Second)
	rounds := 0
	for !at.After(deadline) {
		clk.Set(at)
		runTransferBatch(t, w.env.ctx, w.p)
		rounds++
		var nextAt time.Time
		for _, no := range []string{tryingNo, cancelling.TransferNo} {
			row := loadTransferRow(t, w.env, no)
			if row.status == transferStatusSuccess || row.status == transferStatusFailed {
				t.Fatalf("terminal during outage: %+v", row)
			}
			if outN, inN := vouchersOf(t, w.env.ctx, w.s0, w.s1, no); outN != 0 || inN != 0 {
				t.Fatalf("voucher during outage %s", no)
			}
			if row.nextRetryAt.After(nextAt) {
				nextAt = row.nextRetryAt
			}
		}
		at = nextAt
	}
	if rounds < 2 {
		t.Fatalf("rounds = %d", rounds)
	}
	branch.down.Store(false)
	clk.Set(at)
	runTransferBatch(t, w.env.ctx, w.p)
	for _, no := range []string{tryingNo, cancelling.TransferNo} {
		row := loadTransferRow(t, w.env, no)
		if row.status != transferStatusFailed || row.version != 2 {
			t.Fatalf("after up row = %+v", row)
		}
		if outN, inN := vouchersOf(t, w.env.ctx, w.s0, w.s1, no); outN != 0 || inN != 0 {
			t.Fatalf("vouchers after up %s out=%d in=%d", no, outN, inN)
		}
	}
	if outN, inN := vouchersOf(t, w.env.ctx, w.s0, w.s1, okOrder.TransferNo); outN != 1 || inN != 1 {
		t.Fatalf("success vouchers changed out=%d in=%d", outN, inN)
	}
	requireCheck(t, w.env.ctx, w.accounts)
	clk.Advance(time.Hour)
	runTransferBatch(t, w.env.ctx, w.p)
	if outN, inN := vouchersOf(t, w.env.ctx, w.s0, w.s1, okOrder.TransferNo); outN != 1 || inN != 1 {
		t.Fatalf("second voucher out=%d in=%d", outN, inN)
	}
	if n := len(loadOutbox(t, w.env, okOrder.TransferNo)); n != 1 {
		t.Fatalf("success outbox = %d", n)
	}
}

type switchLocal struct {
	inner accountBizLocal
	down  atomic.Bool
}

func (s *switchLocal) TransferLocal(ctx context.Context, transferNo, payer, payee string, amount money.Amount) (LocalTransferResult, error) {
	if s.down.Load() {
		return LocalTransferResult{}, errno.ErrUnavailable
	}
	return s.inner.TransferLocal(ctx, transferNo, payer, payee, amount)
}

// TRF-04 验收 5：LOCAL 在 account 不可用期间保持 TRYING 且没有凭证，恢复后恰好 1 张 TRANSFER。
func TestTransferRecoverLocalAfterAccountUp(t *testing.T) {
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
	keepSchemas(t, schema0, schema1)
	if _, err := accounts.OpenAccount(env.ctx, "MERCHANT", "M100001", "MERCHANT_SETTLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := accounts.OpenAccount(env.ctx, "MERCHANT", "M100003", "MERCHANT_SETTLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if _, err := accounts.OpenAccount(env.ctx, "PLATFORM", "mockpay", "CHANNEL_RECEIVABLE", 1, 0); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 500, "PAY-TRF04-LOCAL")
	local := &switchLocal{inner: accountBizLocal{svc: accounts}}
	local.down.Store(true)
	p.UseLocalTransfer(local)
	got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
		MerchantID: "M100001", OutTransferNo: "TRF04-LOCAL", PayeeMerchantID: "M100003", Amount: trf04Amount,
	})
	if err != nil || got.Status != transferStatusTrying || got.Mode != transferModeLocal {
		t.Fatalf("created = %+v err=%v", got, err)
	}
	if countTransferVouchers(t, env.ctx, s1, got.TransferNo) != 0 {
		t.Fatal("voucher while down")
	}
	clk := paymentsClock(t, p)
	clk.Set(now.Add(10 * time.Second))
	runTransferBatch(t, env.ctx, p)
	row := loadTransferRow(t, env, got.TransferNo)
	if row.status != transferStatusTrying || row.retryCount != 1 || row.version != 0 {
		t.Fatalf("still down row = %+v", row)
	}
	if countTransferVouchers(t, env.ctx, s1, got.TransferNo) != 0 {
		t.Fatal("voucher after recover while down")
	}
	local.down.Store(false)
	clk.Set(now.Add(12 * time.Second))
	runTransferBatch(t, env.ctx, p)
	row = loadTransferRow(t, env, got.TransferNo)
	if row.status != transferStatusSuccess || row.version != 1 || !row.txnNo.Valid || row.failCode.Valid {
		t.Fatalf("up row = %+v", row)
	}
	if countTransferVouchers(t, env.ctx, s1, got.TransferNo) != 1 {
		t.Fatal("want 1 TRANSFER")
	}
	if n := len(loadOutbox(t, env, got.TransferNo)); n != 1 {
		t.Fatalf("outbox = %d", n)
	}
	clk.Advance(time.Hour)
	runTransferBatch(t, env.ctx, p)
	if countTransferVouchers(t, env.ctx, s1, got.TransferNo) != 1 || len(loadOutbox(t, env, got.TransferNo)) != 1 {
		t.Fatal("second recover duplicated")
	}
	requireCheck(t, env.ctx, accounts)
}

// TRF-04 验收 6：付款方 Confirm 失败一次停在 CONFIRMING，本任务推进为 SUCCESS，两种凭证各 1 张。
func TestTransferRecoverConfirmFail(t *testing.T) {
	forTCCModes(t, func(t *testing.T, parallel bool) {
		env := openPay(t)
		p := openTransfers(t, env, feeAt(12, 0, 0, 0))
		accounts, s1, schema0, schema1 := openAccountPair(t, env.ctx)
		keepSchemas(t, schema0, schema1)
		openTCCBooks(t, env.ctx, accounts)
		fundMerchant(t, env.ctx, s1, accounts, "M100001", 1, 500, "PAY-TRF04-CFM")
		var once atomic.Bool
		p.UseTCCTransfer(hookedTCC{
			next: accountBizTCC{svc: accounts},
			confirmPayer: func(ctx context.Context, transferNo, payer string) error {
				if once.CompareAndSwap(false, true) {
					return errno.ErrInternal
				}
				return accountBizTCC{svc: accounts}.ConfirmPayer(ctx, transferNo, payer)
			},
		}, parallel)
		got, err := p.CreateTransfer(env.ctx, &CreateTransferInput{
			MerchantID: "M100001", OutTransferNo: "TRF04-CFM", PayeeMerchantID: "M100002", Amount: 100,
		})
		if err != nil || got.Status != transferStatusConfirming || got.Version != 1 {
			t.Fatalf("created = %+v err=%v", got, err)
		}
		if transitBoth(t, env.ctx, schema0.DB, s1) == 0 {
			t.Fatal("transit is zero while confirming")
		}
		check, err := accounts.CheckBalance(env.ctx, "", false)
		if err != nil || !check.Passed {
			t.Fatalf("check = %+v err=%v", check, err)
		}
		paymentsClock(t, p).Set(feeAt(12, 0, 0, 0).Add(10 * time.Second))
		runTransferBatch(t, env.ctx, p)
		row := loadTransferRow(t, env, got.TransferNo)
		if row.status != transferStatusSuccess || row.version != 2 || row.txnNo.Valid || row.failCode.Valid {
			t.Fatalf("row = %+v", row)
		}
		if countVoucher(t, env.ctx, s1, "TRANSFER_OUT", got.TransferNo) != 1 ||
			countVoucher(t, env.ctx, schema0.DB, "TRANSFER_IN", got.TransferNo) != 1 {
			t.Fatal("voucher count")
		}
		if transitBoth(t, env.ctx, schema0.DB, s1) != 0 {
			t.Fatal("transit after")
		}
		requireCheck(t, env.ctx, accounts)
	})
}

// TRF-04：一轮最多抢 100 行。
func TestTransferRecoverClaimsAtMost100(t *testing.T) {
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	local := &scriptedLocal{err: errno.ErrUnavailable}
	p.UseLocalTransfer(local)
	const n = transferRecoverBatchSize + 1
	base := now
	for i := 0; i < n; i++ {
		insertStuckTransfer(t, env, fmt.Sprintf("TF%019d", i), fmt.Sprintf("TRF04L%03d", i),
			"M100001", "M100003", transferModeLocal, transferStatusTrying, "",
			1, 1, 0, 1, base.Add(time.Duration(i)*time.Millisecond), base, 0)
	}
	paymentsClock(t, p).Set(base.Add(time.Duration(transferRecoverBatchSize) * time.Millisecond))
	runTransferBatch(t, env.ctx, p)
	if local.callCount() != transferRecoverBatchSize {
		t.Fatalf("calls = %d", local.callCount())
	}
	var bumped, left int
	err := env.db.QueryRowContext(env.ctx, `
SELECT COALESCE(SUM(retry_count = 1), 0), COALESCE(SUM(retry_count = 0), 0)
FROM transfer_order`).Scan(&bumped, &left)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	if bumped != transferRecoverBatchSize || left != 1 {
		t.Fatalf("bumped=%d left=%d", bumped, left)
	}
}

// TRF-04：CONFIRMING 失败后仍是 CONFIRMING，retry_count 增加，updated_at 是注入时钟。
func TestTransferRecoverConfirmingRetryMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	branch := &scriptedTCC{confirmP: errno.ErrInternal}
	p.UseTCCTransfer(branch, true)
	insertStuckTransfer(t, env, "TFCONFIRMB00000000001", "TRF04-STAY",
		"M100001", "M100002", transferModeTCC, transferStatusConfirming, "",
		1, 0, 0, 100, now, now, 1)
	runTransferBatch(t, env.ctx, p)
	row := loadTransferRow(t, env, "TFCONFIRMB00000000001")
	if row.status != transferStatusConfirming || row.retryCount != 1 || row.version != 1 || row.failCode.Valid {
		t.Fatalf("row = %+v", row)
	}
	if !row.nextRetryAt.Equal(now.Add(2*time.Second)) || !loadUpdatedAt(t, env, row.transferNo).Equal(now) {
		t.Fatalf("next=%s updated=%s", row.nextRetryAt, loadUpdatedAt(t, env, row.transferNo))
	}
	runTransferBatch(t, env.ctx, p)
	if again := loadTransferRow(t, env, row.transferNo); again.retryCount != 1 {
		t.Fatalf("claimed again: %+v", again)
	}
	paymentsClock(t, p).Set(now.Add(2 * time.Second))
	runTransferBatch(t, env.ctx, p)
	row = loadTransferRow(t, env, row.transferNo)
	if row.status != transferStatusConfirming || row.retryCount != 2 || !row.nextRetryAt.Equal(now.Add(6*time.Second)) {
		t.Fatalf("second = %+v", row)
	}
}

// TRF-04：retry_count 从 9 到 10 进入 gauge，终态后去掉。
func TestTransferRecoverStuckGaugeMySQL(t *testing.T) {
	resetTransferStuck()
	t.Cleanup(resetTransferStuck)
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	branch := &scriptedTCC{confirmP: errno.ErrInternal}
	p.UseTCCTransfer(branch, true)
	const no = "TFSTUCKB000000000001"
	insertStuckTransfer(t, env, no, "TRF04-STUCK",
		"M100001", "M100002", transferModeTCC, transferStatusConfirming, "",
		1, 0, 9, 100, now, now, 1)
	runTransferBatch(t, env.ctx, p)
	row := loadTransferRow(t, env, no)
	if row.retryCount != 10 || row.status != transferStatusConfirming || transferStuckGauge() != 1 {
		t.Fatalf("row=%+v gauge=%d", row, transferStuckGauge())
	}
	branch.confirmP = nil
	paymentsClock(t, p).Set(now.Add(60 * time.Second))
	runTransferBatch(t, env.ctx, p)
	row = loadTransferRow(t, env, no)
	if row.status != transferStatusSuccess || row.version != 2 || transferStuckGauge() != 0 {
		t.Fatalf("row=%+v gauge=%d", row, transferStuckGauge())
	}
}

// MySQL 上两条都没装时返回 10003 且不抢占；装上之后 ctx 已取消则原样返回，不改 retry_count。
func TestRunTransferRecoverBatchNotWiredMySQL(t *testing.T) {
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	insertStuckTransfer(t, env, "TFUNWIRED000000000001", "TRF04-WIRE",
		"M100001", "M100003", transferModeLocal, transferStatusTrying, "",
		1, 1, 0, 1, now, now, 0)
	if err := p.RunTransferRecoverBatch(env.ctx); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("unwired = %v", err)
	}
	canceled, cancel := context.WithCancel(env.ctx)
	cancel()
	if err := p.RunTransferRecoverBatch(canceled); !errors.Is(err, errno.ErrInternal) {
		t.Fatalf("unwired canceled = %v", err)
	}
	if row := loadTransferRow(t, env, "TFUNWIRED000000000001"); row.retryCount != 0 {
		t.Fatalf("claimed unwired: %+v", row)
	}
	p.UseLocalTransfer(&scriptedLocal{err: errno.ErrUnavailable})
	if err := p.RunTransferRecoverBatch(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	if row := loadTransferRow(t, env, "TFUNWIRED000000000001"); row.retryCount != 0 {
		t.Fatalf("claimed canceled: %+v", row)
	}
}

// 只装 LOCAL 不抢 TCC；只装 TCC 不抢 LOCAL。
func TestTransferRecoverWiringFilter(t *testing.T) {
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	insertStuckTransfer(t, env, "TFONLYLOCAL0000000001", "TRF04-OL",
		"M100001", "M100003", transferModeLocal, transferStatusTrying, "",
		1, 1, 0, 1, now, now, 0)
	insertStuckTransfer(t, env, "TFONLYTCC000000000001", "TRF04-OT",
		"M100001", "M100002", transferModeTCC, transferStatusTrying, "",
		1, 0, 0, 1, now, now, 0)
	insertStuckTransfer(t, env, "TFONLYCFM000000000001", "TRF04-OC",
		"M100001", "M100002", transferModeTCC, transferStatusConfirming, "",
		1, 0, 0, 1, now, now, 1)
	local := &scriptedLocal{err: errno.ErrUnavailable}
	p.UseLocalTransfer(local)
	runTransferBatch(t, env.ctx, p)
	if local.callCount() != 1 {
		t.Fatalf("local calls = %d", local.callCount())
	}
	if loadTransferRow(t, env, "TFONLYLOCAL0000000001").retryCount != 1 ||
		loadTransferRow(t, env, "TFONLYTCC000000000001").retryCount != 0 ||
		loadTransferRow(t, env, "TFONLYCFM000000000001").retryCount != 0 {
		t.Fatal("local wiring claimed the wrong rows")
	}
	insertStuckTransfer(t, env, "TFONLYLOCAL2000000001", "TRF04-OL2",
		"M100001", "M100003", transferModeLocal, transferStatusTrying, "",
		1, 1, 0, 1, now, now, 0)
	p.localTransfer = nil
	branch := &scriptedTCC{confirmP: errno.ErrInternal, cancelP: errno.ErrInternal}
	p.UseTCCTransfer(branch, true)
	runTransferBatch(t, env.ctx, p)
	if loadTransferRow(t, env, "TFONLYLOCAL0000000001").retryCount != 1 ||
		loadTransferRow(t, env, "TFONLYLOCAL2000000001").retryCount != 0 {
		t.Fatal("tcc wiring claimed local")
	}
	if loadTransferRow(t, env, "TFONLYTCC000000000001").retryCount != 1 ||
		loadTransferRow(t, env, "TFONLYCFM000000000001").retryCount != 1 {
		t.Fatal("tcc rows not claimed")
	}
}

type countLocal struct {
	mu  sync.Mutex
	by  map[string]int
	err error
}

func (c *countLocal) TransferLocal(_ context.Context, transferNo, _, _ string, _ money.Amount) (LocalTransferResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.by == nil {
		c.by = map[string]int{}
	}
	c.by[transferNo]++
	return LocalTransferResult{}, c.err
}

// dueTransferNos 读出最早到期的 10 个单号。返回前关闭 rows，同一事务才能接着加锁。
func dueTransferNos(t *testing.T, tx *sqlx.Tx, ctx context.Context, now time.Time) []string {
	t.Helper()
	rows, err := tx.QueryContext(ctx, `
SELECT transfer_no FROM transfer_order
WHERE status IN ('TRYING', 'CONFIRMING', 'CANCELLING') AND next_retry_at <= ?
ORDER BY next_retry_at
LIMIT 10`, now)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	var nos []string
	for rows.Next() {
		var no string
		if err = rows.Scan(&no); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		nos = append(nos, no)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return nos
}

// lockTransferNos 按主键锁住这些单并读回单号。返回前关闭 rows。
func lockTransferNos(t *testing.T, tx *sqlx.Tx, ctx context.Context, nos []string) []string {
	t.Helper()
	lockSQL, args, err := sqlx.In(`SELECT transfer_no FROM transfer_order WHERE transfer_no IN (?) FOR UPDATE`, nos)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, tx.Rebind(lockSQL), args...)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = rows.Close() }()
	var locked []string
	for rows.Next() {
		var no string
		if err = rows.Scan(&no); err != nil {
			t.Fatal(testkit.Redact(err))
		}
		locked = append(locked, no)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(testkit.Redact(err))
	}
	return locked
}

// TRF-04：另一个连接持有最早 10 行的锁时，本实例用 SKIP LOCKED 拿走其余的行。
func TestTransferRecoverSkipLockedSplitsRows(t *testing.T) {
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	local := &countLocal{err: errno.ErrUnavailable}
	p.UseLocalTransfer(local)
	// 20 行都已到期，next_retry_at 彼此不同，持锁的查询按这个顺序拿走最早的 10 行。
	base := now.Add(-time.Minute)
	for i := 0; i < 20; i++ {
		insertStuckTransfer(t, env, fmt.Sprintf("TFSKIP%019d", i), fmt.Sprintf("TRF04S%02d", i),
			"M100001", "M100003", transferModeLocal, transferStatusTrying, "",
			1, 1, 0, 1, base.Add(time.Duration(i)*time.Millisecond), now, 0)
	}
	tx, err := env.db.BeginTxx(env.ctx, nil)
	if err != nil {
		t.Fatal(testkit.Redact(err))
	}
	defer func() { _ = tx.Rollback() }()
	// 先读出最早的 10 个单号再按主键加锁。ORDER BY LIMIT FOR UPDATE 在 filesort 时会锁住扫描到的全部行。
	nos := dueTransferNos(t, tx, env.ctx, now)
	if len(nos) != 10 {
		t.Fatalf("candidates = %d", len(nos))
	}
	held := lockTransferNos(t, tx, env.ctx, nos)
	locked := map[string]struct{}{}
	for _, no := range held {
		locked[no] = struct{}{}
	}
	if len(locked) != 10 {
		t.Fatalf("locked = %d", len(locked))
	}
	ctx, cancel := context.WithTimeout(env.ctx, 5*time.Second)
	err = p.RunTransferRecoverBatch(ctx)
	cancel()
	if err != nil {
		t.Fatalf("batch while locked = %v", err)
	}
	for i := 0; i < 20; i++ {
		no := fmt.Sprintf("TFSKIP%019d", i)
		row := loadTransferRow(t, env, no)
		_, held := locked[no]
		if held && row.retryCount != 0 {
			t.Fatalf("locked row bumped: %s", no)
		}
		if !held && row.retryCount != 1 {
			t.Fatalf("unlocked row not bumped: %s retry=%d", no, row.retryCount)
		}
	}
	_ = tx.Rollback()
	runTransferBatch(t, env.ctx, p)
	var calls int
	local.mu.Lock()
	for no, n := range local.by {
		calls += n
		if n != 1 {
			t.Fatalf("%s calls = %d", no, n)
		}
	}
	local.mu.Unlock()
	if calls != 20 {
		t.Fatalf("calls = %d", calls)
	}
	for i := 0; i < 20; i++ {
		row := loadTransferRow(t, env, fmt.Sprintf("TFSKIP%019d", i))
		if row.retryCount != 1 {
			t.Fatalf("final retry %s = %d", row.transferNo, row.retryCount)
		}
	}
}

// TRF-04：两个实例同时抢，每行只被处理一次。
func TestTransferRecoverTwoInstances(t *testing.T) {
	env := openPay(t)
	now := feeAt(12, 0, 0, 0)
	p := openTransfers(t, env, now)
	clk2 := &clock.Fake{}
	clk2.Set(now)
	ids, err := idgen.New(15, clk2)
	if err != nil {
		t.Fatal(err)
	}
	p2 := NewPayments(NewMerchants(env.db, nil, clk2, 0), ids, clk2, "merchant-demo", env.db)
	local := &countLocal{err: errno.ErrUnavailable}
	p.UseLocalTransfer(local)
	p2.UseLocalTransfer(local)
	for i := 0; i < 20; i++ {
		insertStuckTransfer(t, env, fmt.Sprintf("TFINST%019d", i), fmt.Sprintf("TRF04I%02d", i),
			"M100001", "M100003", transferModeLocal, transferStatusTrying, "",
			1, 1, 0, 1, now, now, 0)
	}
	var g errgroup.Group
	g.Go(func() error { return p.RunTransferRecoverBatch(env.ctx) })
	g.Go(func() error { return p2.RunTransferRecoverBatch(env.ctx) })
	if err = g.Wait(); err != nil {
		t.Fatal(err)
	}
	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.by) != 20 {
		t.Fatalf("distinct = %d", len(local.by))
	}
	for no, n := range local.by {
		if n != 1 {
			t.Fatalf("%s calls = %d", no, n)
		}
		row := loadTransferRow(t, env, no)
		if row.retryCount != 1 {
			t.Fatalf("%s retry = %d", no, row.retryCount)
		}
	}
}
