package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym108/pay-system/pkg/errno"
)

// transferByNo 按 transfer_no 逐字节读，与 SQL 实现一样不比较商户号，商户号由 QueryTransfer 比较。
func (m *memTransfers) transferByNo(_ context.Context, transferNo string) (TransferOrder, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	if m.readErr != nil {
		return TransferOrder{}, false, m.readErr
	}
	for _, row := range m.rows {
		if row.TransferNo == transferNo {
			return row, true, nil
		}
	}
	return TransferOrder{}, false, nil
}

func (m *memTransfers) readCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reads
}

// transferQuerySeed 放一张 M100001 → M100002 的 TCC 转账单。
func transferQuerySeed(t *testing.T) (transferTest, TransferOrder) {
	t.Helper()
	tt := newTransferTest(t)
	created := transferAt().Truncate(time.Millisecond)
	row := TransferOrder{
		TransferNo: "TF0000000000000000001", MerchantID: "M100001", OutTransferNo: "TR1",
		PayeeMerchantID: "M100002", Amount: 500, Currency: payCurrencyCNY, Remark: "货款",
		Mode: transferModeTCC, PayerShard: 1, PayeeShard: 0, Status: transferStatusTrying,
		NextRetryAt: created.Add(10 * time.Second), Version: 0, CreatedAt: created, UpdatedAt: created,
	}
	tt.store.rows = append(tt.store.rows, row)
	return tt, row
}

// TRF-05：两个单号都空、都非空，或请求为 nil，返回 10001，不读库。空串算没提供，不 trim。
func TestQueryTransferRejectsBadKeysWithoutRead(t *testing.T) {
	tt, row := transferQuerySeed(t)
	var nilP *Payments
	if _, err := nilP.QueryTransfer(context.Background(), nil); !errors.Is(err, errno.ErrInvalidParam) {
		t.Fatalf("nil payments with nil input = %v", err)
	}
	for _, in := range []*TransferQueryInput{
		nil,
		{MerchantID: row.MerchantID},
		{MerchantID: row.MerchantID, TransferNo: "", OutTransferNo: ""},
		{MerchantID: row.MerchantID, TransferNo: row.TransferNo, OutTransferNo: row.OutTransferNo},
		{MerchantID: row.MerchantID, TransferNo: " ", OutTransferNo: " "},
	} {
		got, err := tt.p.QueryTransfer(context.Background(), in)
		assertErrno(t, err, errno.ErrInvalidParam)
		if got.TransferNo != "" {
			t.Fatalf("in=%+v got=%+v", in, got)
		}
	}
	if n := tt.store.readCount(); n != 0 {
		t.Fatalf("invalid query read the store %d times", n)
	}
}

// TRF-05：付款商户按 transfer_no 与 out_transfer_no 都能查到同一行，原样返回，不改行。五种状态都原样返回。
func TestQueryTransferReadsPayerTransfer(t *testing.T) {
	tt, row := transferQuerySeed(t)
	ctx := context.Background()
	byNo, err := tt.p.QueryTransfer(ctx, &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
	if err != nil || byNo != row {
		t.Fatalf("by transfer_no = %+v err=%v", byNo, err)
	}
	byOut, err := tt.p.QueryTransfer(ctx, &TransferQueryInput{MerchantID: row.MerchantID, OutTransferNo: row.OutTransferNo})
	if err != nil || byOut != row {
		t.Fatalf("by out_transfer_no = %+v err=%v", byOut, err)
	}
	finished := transferAt().Add(time.Second).Truncate(time.Millisecond)
	for _, st := range []struct {
		status, failCode string
		at               time.Time
	}{
		{transferStatusTrying, "", time.Time{}},
		{"CONFIRMING", "", time.Time{}},
		{"CANCELLING", transferFailInsufficient, time.Time{}},
		{transferStatusSuccess, "", finished},
		{transferStatusFailed, transferFailInsufficient, finished},
	} {
		tt.store.mu.Lock()
		tt.store.rows[0].Status = st.status
		tt.store.rows[0].FailCode = st.failCode
		tt.store.rows[0].FinishedAt = st.at
		tt.store.mu.Unlock()
		got, qerr := tt.p.QueryTransfer(ctx, &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
		if qerr != nil || got.Status != st.status || got.FailCode != st.failCode || !got.FinishedAt.Equal(st.at) || got.Version != row.Version {
			t.Fatalf("status %s got=%+v err=%v", st.status, got, qerr)
		}
	}
	rows := tt.store.snapshot()
	if len(rows) != 1 || rows[0].Version != row.Version || !rows[0].UpdatedAt.Equal(row.UpdatedAt) || tt.store.inserts != 0 {
		t.Fatalf("query changed the store: %+v inserts=%d", rows, tt.store.inserts)
	}
	if tt.merchants.callCount() != 0 {
		t.Fatal("query read merchant config")
	}
}

// TRF-05 验收 1（单元）：收款商户查询返回 30001，与其他商户、不存在的转账单是同一个错误，不带转账单。
// 空商户号、大小写不同、未 trim 的空格也分不出转账单在不在。
func TestQueryTransferPayeeAndOthersSameAsMissing(t *testing.T) {
	tt, row := transferQuerySeed(t)
	ctx := context.Background()
	payee, errPayee := tt.p.QueryTransfer(ctx, &TransferQueryInput{MerchantID: row.PayeeMerchantID, TransferNo: row.TransferNo})
	missing, errMissing := tt.p.QueryTransfer(ctx, &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: "TF9999999999999999999"})
	assertErrno(t, errPayee, errno.ErrOrderNotFound)
	assertErrno(t, errMissing, errno.ErrOrderNotFound)
	if errPayee.Error() != errMissing.Error() || payee.TransferNo != "" || missing.TransferNo != "" {
		t.Fatalf("payee=%+v %v missing=%+v %v", payee, errPayee, missing, errMissing)
	}
	for _, in := range []*TransferQueryInput{
		{MerchantID: row.PayeeMerchantID, OutTransferNo: row.OutTransferNo},
		{MerchantID: "M100003", TransferNo: row.TransferNo},
		{MerchantID: "M100003", OutTransferNo: row.OutTransferNo},
		{MerchantID: "", TransferNo: row.TransferNo},
		{MerchantID: "", OutTransferNo: row.OutTransferNo},
		{MerchantID: "m100001", TransferNo: row.TransferNo},
		{MerchantID: row.MerchantID + " ", TransferNo: row.TransferNo},
		{MerchantID: row.MerchantID, TransferNo: row.TransferNo + " "},
		{MerchantID: row.MerchantID, OutTransferNo: row.OutTransferNo + " "},
		{MerchantID: row.MerchantID, OutTransferNo: "NO-SUCH"},
	} {
		got, qerr := tt.p.QueryTransfer(ctx, in)
		assertErrno(t, qerr, errno.ErrOrderNotFound)
		if got.TransferNo != "" || qerr.Error() != errMissing.Error() {
			t.Fatalf("in=%+v got=%+v err=%v", in, got, qerr)
		}
	}
	if rows := tt.store.snapshot(); len(rows) != 1 || rows[0] != row {
		t.Fatalf("lookup changed the row: %+v", rows)
	}
}

// 存储层返回字节不同的行（模拟排序规则放宽匹配）时仍是 30001。
func TestQueryTransferRechecksStoredKeys(t *testing.T) {
	tt, row := transferQuerySeed(t)
	tt.p.transfers = &looseTransferStore{memTransfers: tt.store}
	for _, in := range []*TransferQueryInput{
		{MerchantID: row.MerchantID, TransferNo: "tf0000000000000000001"},
		{MerchantID: "m100001", OutTransferNo: row.OutTransferNo},
		{MerchantID: row.MerchantID, OutTransferNo: "tr1"},
	} {
		_, err := tt.p.QueryTransfer(context.Background(), in)
		assertErrno(t, err, errno.ErrOrderNotFound)
	}
}

// looseTransferStore 不论单号与商户号，都返回第一张转账单，模拟大小写不敏感的排序规则。
type looseTransferStore struct {
	*memTransfers
}

func (l *looseTransferStore) transferByNo(_ context.Context, _ string) (TransferOrder, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rows[0], true, nil
}

func (l *looseTransferStore) transferByMerchantOut(_ context.Context, _, _ string) (TransferOrder, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rows[0], true, nil
}

// 读库失败返回 10003；ctx 已取消时不读库，原样返回 ctx 错误；依赖未装上或 nil ctx 返回 10003。
func TestQueryTransferStoreErrorAndCanceledContext(t *testing.T) {
	tt, row := transferQuerySeed(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tt.p.QueryTransfer(ctx, &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	if n := tt.store.readCount(); n != 0 {
		t.Fatalf("canceled query read the store %d times", n)
	}
	tt.store.mu.Lock()
	tt.store.readErr = errors.New("db down")
	tt.store.mu.Unlock()
	_, err := tt.p.QueryTransfer(context.Background(), &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
	assertErrno(t, err, errno.ErrInternal)
	_, err = tt.p.QueryTransfer(context.Background(), &TransferQueryInput{MerchantID: row.MerchantID, OutTransferNo: row.OutTransferNo})
	assertErrno(t, err, errno.ErrInternal)

	noStore := newPayments(nil, nil, nil, "", nil)
	_, err = noStore.QueryTransfer(context.Background(), &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
	assertErrno(t, err, errno.ErrInternal)
	var nilP *Payments
	_, err = nilP.QueryTransfer(context.Background(), &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
	assertErrno(t, err, errno.ErrInternal)
	//nolint:staticcheck // 故意传 nil ctx，确认返回 10003 而不是 panic。
	_, err = tt.p.QueryTransfer(nil, &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
	assertErrno(t, err, errno.ErrInternal)
}

// App 转给 Payments；子对象为 nil 时返回 10003。
func TestAppQueryTransfer(t *testing.T) {
	tt, row := transferQuerySeed(t)
	got, err := NewApp(nil, tt.p).QueryTransfer(context.Background(), &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
	if err != nil || got != row {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	_, err = NewApp(nil, nil).QueryTransfer(context.Background(), &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
	assertErrno(t, err, errno.ErrInternal)
	var nilApp *App
	_, err = nilApp.QueryTransfer(context.Background(), &TransferQueryInput{MerchantID: row.MerchantID, TransferNo: row.TransferNo})
	assertErrno(t, err, errno.ErrInternal)
}
