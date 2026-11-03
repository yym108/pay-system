// Package dal 读写 pay_channel_mock.mock_txn。
//
// SQL 只用 ? 占位符。唯一键冲突原样返回，由调用方用 dbx.IsDuplicate 判断。
// 状态推进只用 CAS（WHERE id = ? AND status = 'PROCESSING'），表上没有 version 列。
package dal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/money"
)

// txn_type 取值，与 ck_mock_type 一致。
const (
	TxnTypePay    = "PAY"
	TxnTypeRefund = "REFUND"
)

// status 取值，与 ck_mock_status 一致。
const (
	StatusProcessing = "PROCESSING"
	StatusSuccess    = "SUCCESS"
	StatusFailed     = "FAILED"
	StatusClosed     = "CLOSED"
)

// billDateLayout 是 Txn.BillDate 的格式。
const billDateLayout = "2006-01-02"

// shanghai 与连接串的 loc 一致，用来把 DATE 列还原成东八区日期串。
var shanghai = time.FixedZone("Asia/Shanghai", 8*60*60)

// Txn 是 mock_txn 的一行。可空列用零值表示 NULL：FinishedAt 为零时间、BillDate 为空串。
type Txn struct {
	ID              uint64
	ChannelTxnNo    string
	TxnType         string
	ReqNo           string
	OrigReqNo       string
	Amount          money.Amount
	Status          string
	Scenario        string
	NotifyURL       string
	NotifySentCount int
	FinishedAt      time.Time
	// BillDate 是 YYYY-MM-DD，空串表示 NULL。
	BillDate  string
	CreatedAt time.Time
}

// Finish 是一次自动完成要写的终态。BillDate 为空串时写 NULL。
type Finish struct {
	ID         uint64
	Status     string
	FinishedAt time.Time
	BillDate   string
}

// DB 是 mock_txn 的存取。并发安全。
type DB struct {
	db *sqlx.DB
}

// New 包装连接池。连接池由调用方关闭。
func New(db *sqlx.DB) *DB {
	return &DB{db: db}
}

const txnColumns = `id, channel_txn_no, txn_type, req_no, orig_req_no, amount, status, scenario, notify_url,
  notify_sent_count, finished_at, bill_date, created_at`

type txnRow struct {
	ID              uint64         `db:"id"`
	ChannelTxnNo    string         `db:"channel_txn_no"`
	TxnType         string         `db:"txn_type"`
	ReqNo           string         `db:"req_no"`
	OrigReqNo       sql.NullString `db:"orig_req_no"`
	Amount          int64          `db:"amount"`
	Status          string         `db:"status"`
	Scenario        string         `db:"scenario"`
	NotifyURL       string         `db:"notify_url"`
	NotifySentCount int            `db:"notify_sent_count"`
	FinishedAt      sql.NullTime   `db:"finished_at"`
	BillDate        sql.NullTime   `db:"bill_date"`
	CreatedAt       time.Time      `db:"created_at"`
}

func (r txnRow) txn() Txn {
	t := Txn{
		ID:              r.ID,
		ChannelTxnNo:    r.ChannelTxnNo,
		TxnType:         r.TxnType,
		ReqNo:           r.ReqNo,
		Amount:          money.Amount(r.Amount),
		Status:          r.Status,
		Scenario:        r.Scenario,
		NotifyURL:       r.NotifyURL,
		NotifySentCount: r.NotifySentCount,
		CreatedAt:       r.CreatedAt,
	}
	if r.OrigReqNo.Valid {
		t.OrigReqNo = r.OrigReqNo.String
	}
	if r.FinishedAt.Valid {
		t.FinishedAt = r.FinishedAt.Time
	}
	if r.BillDate.Valid {
		t.BillDate = r.BillDate.Time.In(shanghai).Format(billDateLayout)
	}
	return t
}

// Insert 写入一笔新交易并返回 id。created_at 取 t.CreatedAt（调用方的注入时钟），不用列默认值。
// (txn_type, req_no) 或 channel_txn_no 冲突时返回驱动的 1062 错误。
func (d *DB) Insert(ctx context.Context, t Txn) (uint64, error) {
	var orig any
	if t.OrigReqNo != "" {
		orig = t.OrigReqNo
	}
	res, err := d.db.ExecContext(ctx, `
INSERT INTO mock_txn (channel_txn_no, txn_type, req_no, orig_req_no, amount, status, scenario, notify_url, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ChannelTxnNo, t.TxnType, t.ReqNo, orig, int64(t.Amount), t.Status, t.Scenario, t.NotifyURL, t.CreatedAt)
	if err != nil {
		return 0, fmt.Errorf("insert mock_txn: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("insert mock_txn: last insert id: %w", err)
	}
	if id <= 0 {
		return 0, errors.New("insert mock_txn: invalid last insert id")
	}
	return uint64(id), nil
}

// Get 按 (txn_type, req_no) 读一行。不存在时返回 ok = false、err = nil。
func (d *DB) Get(ctx context.Context, txnType, reqNo string) (Txn, bool, error) {
	var r txnRow
	err := d.db.GetContext(ctx, &r,
		`SELECT `+txnColumns+` FROM mock_txn WHERE txn_type = ? AND req_no = ?`, txnType, reqNo)
	if errors.Is(err, sql.ErrNoRows) {
		return Txn{}, false, nil
	}
	if err != nil {
		return Txn{}, false, fmt.Errorf("get mock_txn: %w", err)
	}
	return r.txn(), true, nil
}

// GetByID 按主键读一行。不存在时返回 ok = false、err = nil。
// 用于完成写库结果不确定后确认那次是否已生效。
func (d *DB) GetByID(ctx context.Context, id uint64) (Txn, bool, error) {
	var r txnRow
	err := d.db.GetContext(ctx, &r, `SELECT `+txnColumns+` FROM mock_txn WHERE id = ?`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Txn{}, false, nil
	}
	if err != nil {
		return Txn{}, false, fmt.Errorf("get mock_txn by id: %w", err)
	}
	return r.txn(), true, nil
}

// Finish 把 PROCESSING 推进到终态（CAS）。返回 true 表示本次生效且事务已提交；
// false 表示该行已不是 PROCESSING（例如已被关单），什么也没改。
// 这条 SQL 会写 finished_at。普通关单不要用它，用 Close。
func (d *DB) Finish(ctx context.Context, f Finish) (bool, error) {
	var bill any
	if f.BillDate != "" {
		bill = f.BillDate
	}
	applied := false
	err := dbx.WithTx(ctx, d.db, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `
UPDATE mock_txn SET status = ?, finished_at = ?, bill_date = ?
WHERE id = ? AND status = 'PROCESSING'`,
			f.Status, f.FinishedAt, bill, f.ID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		applied = n == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("finish mock_txn: %w", err)
	}
	return applied, nil
}

// Close 把 PROCESSING 置为 CLOSED（CAS）。不写 finished_at、不写 bill_date。
// 返回 true 表示本次生效且事务已提交；false 表示该行已不是 PROCESSING，什么也没改。
func (d *DB) Close(ctx context.Context, id uint64) (bool, error) {
	applied := false
	err := dbx.WithTx(ctx, d.db, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `
UPDATE mock_txn SET status = ?
WHERE id = ? AND status = 'PROCESSING'`, StatusClosed, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		applied = n == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("close mock_txn: %w", err)
	}
	return applied, nil
}

// AddNotifySent 在单独的短事务里把 notify_sent_count 条件加一。
// expected 是这次发送前读到的计数。返回 true 表示本次占到发送名额且事务已提交，调用方再发 HTTP。
// 不改 status，也不写其他列。影响 0 行时返回 false、err = nil，不覆盖状态。
func (d *DB) AddNotifySent(ctx context.Context, id uint64, expected int) (bool, error) {
	applied := false
	err := dbx.WithTx(ctx, d.db, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `
UPDATE mock_txn SET notify_sent_count = notify_sent_count + 1
WHERE id = ? AND notify_sent_count = ?`, id, expected)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		applied = n == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("add mock_txn notify_sent_count: %w", err)
	}
	return applied, nil
}

// AddDupNotifySent 在单独的短事务里把 notify_sent_count 加一。
// 条件是当前值小于 limit，不是等于某个期望值：同一波并发发送不能共用同一个期望计数。
// 返回 true 表示本次占到名额且事务已提交，调用方再发 HTTP。
// 返回 false 表示已经达到 limit，什么也没改。不改 status，也不写其他列。
func (d *DB) AddDupNotifySent(ctx context.Context, id uint64, limit int) (bool, error) {
	applied := false
	err := dbx.WithTx(ctx, d.db, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `
UPDATE mock_txn SET notify_sent_count = notify_sent_count + 1
WHERE id = ? AND notify_sent_count < ?`, id, limit)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		applied = n == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("add mock_txn dup notify_sent_count: %w", err)
	}
	return applied, nil
}

// ListProcessing 按 id 升序返回 id > afterID 的该类型 PROCESSING 行，最多 limit 行。
// 表上没有 status 索引，这里走主键范围扫描；只在进程启动时调用。
func (d *DB) ListProcessing(ctx context.Context, txnType string, afterID uint64, limit int) ([]Txn, error) {
	var rows []txnRow
	err := d.db.SelectContext(ctx, &rows, `SELECT `+txnColumns+` FROM mock_txn
WHERE id > ? AND txn_type = ? AND status = 'PROCESSING' ORDER BY id LIMIT ?`, afterID, txnType, limit)
	if err != nil {
		return nil, fmt.Errorf("list processing mock_txn: %w", err)
	}
	out := make([]Txn, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.txn())
	}
	return out, nil
}

// RefundLook 在原支付行被 FOR UPDATE 锁住时调用。reject 非空则回滚且不插入。
type RefundLook func(pay Txn, found bool, activeSum money.Amount) (reject string)

// InsertRefund 在一个事务里锁住原支付、让 look 决定是否拒绝，通过后插入退款行。
// 1062 原样返回。look 返回的 reject 不是 error。
func (d *DB) InsertRefund(ctx context.Context, t Txn, look RefundLook) (uint64, string, error) {
	if look == nil {
		return 0, "", errors.New("insert refund: nil look")
	}
	var id uint64
	var reject string
	err := dbx.WithTx(ctx, d.db, func(tx *sqlx.Tx) error {
		var row txnRow
		qerr := tx.GetContext(ctx, &row, `SELECT `+txnColumns+` FROM mock_txn WHERE txn_type = ? AND req_no = ? FOR UPDATE`, TxnTypePay, t.OrigReqNo)
		found := true
		if errors.Is(qerr, sql.ErrNoRows) {
			found = false
		} else if qerr != nil {
			return fmt.Errorf("lock pay: %w", qerr)
		}
		var sum int64
		if found {
			if serr := tx.GetContext(ctx, &sum, `SELECT COALESCE(SUM(amount), 0) FROM mock_txn WHERE txn_type = ? AND orig_req_no = ? AND status <> ?`, TxnTypeRefund, t.OrigReqNo, StatusFailed); serr != nil {
				return fmt.Errorf("sum refunds: %w", serr)
			}
		}
		reject = look(row.txn(), found, money.Amount(sum))
		if reject != "" {
			return nil
		}
		res, ierr := tx.ExecContext(ctx, `
INSERT INTO mock_txn (channel_txn_no, txn_type, req_no, orig_req_no, amount, status, scenario, notify_url, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			t.ChannelTxnNo, TxnTypeRefund, t.ReqNo, t.OrigReqNo, int64(t.Amount), t.Status, t.Scenario, t.NotifyURL, t.CreatedAt)
		if ierr != nil {
			return fmt.Errorf("insert mock_txn: %w", ierr)
		}
		last, lerr := res.LastInsertId()
		if lerr != nil {
			return fmt.Errorf("insert mock_txn: last insert id: %w", lerr)
		}
		if last <= 0 {
			return errors.New("insert mock_txn: invalid last insert id")
		}
		id = uint64(last)
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	return id, reject, nil
}
