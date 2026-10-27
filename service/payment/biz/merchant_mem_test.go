package biz

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/errno"
)

func init() {
	sql.Register("paymerchant-mem", merchantMemDriver{})
}

var merchantMemByName sync.Map

type merchantMemDriver struct{}

func (merchantMemDriver) Open(name string) (driver.Conn, error) {
	v, ok := merchantMemByName.Load(name)
	if !ok {
		return nil, errors.New("unknown merchant mem")
	}
	return &merchantMemConn{st: v.(*merchantMem)}, nil
}

type merchantMemConn struct {
	st *merchantMem
}

func (c *merchantMemConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("merchant mem prepare")
}

func (c *merchantMemConn) Close() error { return nil }

func (c *merchantMemConn) Begin() (driver.Tx, error) {
	return nil, errors.New("merchant reads must not open a transaction")
}

func (c *merchantMemConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.st.query(query, args)
}

type merchantMemRow struct {
	id, appID, status, notify string
	cipher                    []byte
	version                   int64
}

// merchantMem 按 SQL 文本区分语句，驱动只给单元测试覆盖 sqlMerchantStore 的扫描与参数。
type merchantMem struct {
	mu        sync.Mutex
	rows      []merchantMemRow
	rates     []FeeRate
	queryFail string
	badScan   bool
	queries   []string
}

func openMerchantMem(t *testing.T, st *merchantMem) *sqlx.DB {
	t.Helper()
	name := fmt.Sprintf("merchant-mem-%d", memSeq.Add(1))
	merchantMemByName.Store(name, st)
	raw, err := sql.Open("paymerchant-mem", name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return sqlx.NewDb(raw, "paymerchant-mem")
}

func (st *merchantMem) query(query string, args []driver.NamedValue) (driver.Rows, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.queries = append(st.queries, query)
	if st.queryFail != "" && strings.Contains(query, st.queryFail) {
		return nil, errors.New("merchant mem query")
	}
	switch {
	case strings.Contains(query, "WHERE app_id = ?"):
		key, err := oneString(args)
		if err != nil {
			return nil, err
		}
		rows := &memRows{cols: []string{"app_id", "merchant_id", "status", "app_secret_cipher", "secret_version", "notify_url"}}
		for _, row := range st.rows {
			// 模拟 utf8mb4_0900_ai_ci：比较不区分大小写。
			if strings.EqualFold(row.appID, key) {
				version := driver.Value(row.version)
				if st.badScan {
					version = "bad"
				}
				rows.data = [][]driver.Value{{row.appID, row.id, row.status, row.cipher, version, row.notify}}
			}
		}
		return rows, nil
	case strings.Contains(query, "FROM merchant_fee_rate"):
		if len(args) != 2 {
			return nil, fmt.Errorf("fee rate args %d", len(args))
		}
		id, err := argString(args[0].Value)
		if err != nil {
			return nil, err
		}
		at, err := argTime(args[1].Value)
		if err != nil {
			return nil, err
		}
		rows := &memRows{cols: rateCols()}
		var best FeeRate
		found := false
		for _, row := range st.rates {
			if row.MerchantID == id && !row.EffectiveFrom.After(at) && (!found || row.EffectiveFrom.After(best.EffectiveFrom)) {
				best, found = row, true
			}
		}
		if found {
			rows.data = [][]driver.Value{rateValues(best, st.badScan)}
		}
		return rows, nil
	case strings.Contains(query, "WHERE merchant_id = ?"):
		key, err := oneString(args)
		if err != nil {
			return nil, err
		}
		rows := &memRows{cols: []string{"merchant_id", "status", "notify_url"}}
		for _, row := range st.rows {
			if strings.EqualFold(row.id, key) {
				status := driver.Value(row.status)
				if st.badScan {
					status = nil
				}
				rows.data = [][]driver.Value{{row.id, status, row.notify}}
			}
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("unexpected query %s", query)
	}
}

// PAY-01：真实 SQL 适配经内存驱动读出凭证、配置和费率档，列与参数顺序与表结构一致。
func TestMemMerchantStore(t *testing.T) {
	box := testBox(t, 0)
	st := &merchantMem{
		rows: []merchantMemRow{
			{id: "M100001", appID: "app_m100001", status: "ACTIVE", notify: "http://merchant-demo/n", cipher: seal(t, box, demoSecret), version: 2},
		},
		rates: []FeeRate{
			{MerchantID: "M100001", RateBP: 0, EffectiveFrom: feeAt(9, 0, 0, 0)},
			{MerchantID: "M100001", RateBP: 60, EffectiveFrom: feeAt(11, 0, 0, 0)},
		},
	}
	m := NewMerchants(openMerchantMem(t, st), box, fakeClock(), time.Minute)
	ctx := context.Background()

	cred, err := m.GetMerchantCredential(ctx, "app_m100001")
	if err != nil {
		t.Fatal(err)
	}
	if cred.MerchantID != "M100001" || cred.Status != "ACTIVE" || cred.AppSecret.Reveal() != demoSecret ||
		cred.SecretVersion != 2 || cred.DefaultNotifyURL != "http://merchant-demo/n" {
		t.Fatalf("credential = %s", cred)
	}
	_, err = m.GetMerchantCredential(ctx, "app_m100099")
	assertErrno(t, err, errno.ErrMerchantInvalid)
	// 库的排序规则不区分大小写，只认完全相同的 app_id。
	_, err = m.GetMerchantCredential(ctx, "APP_M100001")
	assertErrno(t, err, errno.ErrMerchantInvalid)

	cfg, err := m.MerchantConfig(ctx, "M100001")
	if err != nil || cfg != (MerchantConfig{MerchantID: "M100001", Status: "ACTIVE", DefaultNotifyURL: "http://merchant-demo/n"}) {
		t.Fatalf("config = %+v err=%v", cfg, err)
	}
	_, err = m.MerchantConfig(ctx, "M100099")
	assertErrno(t, err, errno.ErrMerchantInvalid)
	_, err = m.MerchantConfig(ctx, "m100001")
	assertErrno(t, err, errno.ErrMerchantInvalid)

	rate, ok, err := m.FeeRateAt(ctx, "M100001", feeAt(10, 0, 0, 0))
	if err != nil || !ok || rate.RateBP != 0 {
		t.Fatalf("rate 10:00 = %+v ok=%v err=%v", rate, ok, err)
	}
	rate, ok, err = m.FeeRateAt(ctx, "M100001", feeAt(11, 0, 0, 0))
	if err != nil || !ok || rate.RateBP != 60 || !rate.EffectiveFrom.Equal(feeAt(11, 0, 0, 0)) {
		t.Fatalf("rate 11:00 = %+v ok=%v err=%v", rate, ok, err)
	}
	_, ok, err = m.FeeRateAt(ctx, "M100001", feeAt(8, 0, 0, 0))
	if err != nil || ok {
		t.Fatalf("before first tier ok=%v err=%v", ok, err)
	}
	st.mu.Lock()
	queries := append([]string(nil), st.queries...)
	st.mu.Unlock()
	for _, q := range queries {
		if strings.Contains(q, "fee_rate_bp") && !strings.Contains(q, "FROM merchant_fee_rate") {
			t.Fatalf("effective rate must not come from merchant.fee_rate_bp: %s", q)
		}
	}
}

func TestMemMerchantStoreErrors(t *testing.T) {
	box := testBox(t, 0)
	cases := []struct {
		name string
		set  func(*merchantMem)
		run  func(*Merchants) error
	}{
		{"credential query", func(st *merchantMem) { st.queryFail = "app_id" }, func(m *Merchants) error {
			_, err := m.GetMerchantCredential(context.Background(), "app_m100001")
			return err
		}},
		{"credential scan", func(st *merchantMem) { st.badScan = true }, func(m *Merchants) error {
			_, err := m.GetMerchantCredential(context.Background(), "app_m100001")
			return err
		}},
		{"config query", func(st *merchantMem) { st.queryFail = "WHERE merchant_id = ?" }, func(m *Merchants) error {
			_, err := m.MerchantConfig(context.Background(), "M100001")
			return err
		}},
		{"config scan", func(st *merchantMem) { st.badScan = true }, func(m *Merchants) error {
			_, err := m.MerchantConfig(context.Background(), "M100001")
			return err
		}},
		{"rate query", func(st *merchantMem) { st.queryFail = "merchant_fee_rate" }, func(m *Merchants) error {
			_, _, err := m.FeeRateAt(context.Background(), "M100001", feeAt(12, 0, 0, 0))
			return err
		}},
		{"rate scan", func(st *merchantMem) { st.badScan = true }, func(m *Merchants) error {
			_, _, err := m.FeeRateAt(context.Background(), "M100001", feeAt(12, 0, 0, 0))
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &merchantMem{
				rows:  []merchantMemRow{{id: "M100001", appID: "app_m100001", status: "ACTIVE", cipher: seal(t, box, demoSecret), version: 1}},
				rates: []FeeRate{{MerchantID: "M100001", RateBP: 60, EffectiveFrom: feeAt(9, 0, 0, 0)}},
			}
			tc.set(st)
			err := tc.run(NewMerchants(openMerchantMem(t, st), box, fakeClock(), time.Minute))
			assertErrno(t, err, errno.ErrInternal)
		})
	}
}
