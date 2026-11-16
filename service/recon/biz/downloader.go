package biz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jmoiron/sqlx"

	"github.com/yym108/pay-system/pkg/clock"
	"github.com/yym108/pay-system/pkg/dbx"
	"github.com/yym108/pay-system/pkg/sign"
	"github.com/yym108/pay-system/service/recon/dal"
)

const (
	billDownloadTimeout       = 30 * time.Second
	billDownloadDialTimeout   = 5 * time.Second
	billDownloadRetryInterval = 10 * time.Second
	billDownloadMaxRetries    = 3
	billDownloadPath          = "/mockpay/v1/bills"
	billCSVTimestampLayout    = "2006-01-02T15:04:05.000Z07:00"
	maxBillResponseBytes      = 256 << 20 // 256 MiB
)

var (
	errBillClientConfig  = errors.New("bill downloader config")
	errDownloadedCASLost = errors.New("bill downloader: batch taken over")
	expectedBillHeader   = []string{"txn_type", "req_no", "channel_txn_no", "amount", "status", "finished_at"}
)

// BillDownloaderConfig 是账单下载器配置。
type BillDownloaderConfig struct {
	BaseURL       string
	Key           []byte
	Clock         clock.Clock
	Timeout       time.Duration
	RetryInterval time.Duration
	MaxRetries    int
	HTTPClient    *http.Client
	SleepFunc     func(context.Context, time.Duration) error
}

// BillDownloader 实现 REC-02 账单下载、校验、严格解析与事务批量导入。
type BillDownloader struct {
	db            *sqlx.DB
	billURL       string
	key           []byte
	clk           clock.Clock
	http          *http.Client
	timeout       time.Duration
	retryInterval time.Duration
	maxRetries    int
	sleep         func(context.Context, time.Duration) error
}

// NewBillDownloader 创建账单下载器。错误文本不含 URL 与密钥。
func NewBillDownloader(db *sqlx.DB, cfg BillDownloaderConfig) (*BillDownloader, error) {
	u, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("%w: invalid base url", errBillClientConfig)
	}
	u.Path = strings.TrimRight(u.Path, "/") + billDownloadPath
	u.RawQuery = ""
	u.Fragment = ""

	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = billDownloadTimeout
	}
	retryInterval := cfg.RetryInterval
	if retryInterval <= 0 {
		retryInterval = billDownloadRetryInterval
	}
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = billDownloadMaxRetries
	}
	client := cfg.HTTPClient
	if client == nil {
		dialer := &net.Dialer{Timeout: billDownloadDialTimeout}
		transport := &http.Transport{
			Proxy:       http.ProxyFromEnvironment,
			DialContext: dialer.DialContext,
		}
		client = &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	sleepFn := cfg.SleepFunc
	if sleepFn == nil {
		sleepFn = sleepWithContext
	}
	return &BillDownloader{
		db:            db,
		billURL:       u.String(),
		key:           bytes.Clone(cfg.Key),
		clk:           clk,
		http:          client,
		timeout:       timeout,
		retryInterval: retryInterval,
		maxRetries:    maxRetries,
		sleep:         sleepFn,
	}, nil
}

// DownloadAndImport 下载账单、校验哈希与行数、严格解析并在同一事务内批量插入与置 DOWNLOADED。
func (d *BillDownloader) DownloadAndImport(ctx context.Context, _ sqlx.ExtContext, batch *dal.Batch) (DownloadResult, error) {
	if d.db == nil {
		return DownloadResult{}, errors.New("bill downloader: nil db")
	}
	rows, fileHash, err := d.DownloadAndParse(ctx, batch.BillDate)
	if err != nil {
		return DownloadResult{}, err
	}
	for i := range rows {
		rows[i].BatchNo = batch.BatchNo
		rows[i].Channel = batch.Channel
		rows[i].BillDate = batch.BillDate
		rows[i].MatchStatus = dal.BillMatchUnchecked
	}

	dlNow := d.clk.Now().In(shanghai).Truncate(time.Millisecond)
	err = dbx.WithTx(ctx, d.db, func(tx *sqlx.Tx) error {
		if err := dal.InsertChannelBillsBatch(ctx, tx, rows); err != nil {
			return err
		}
		won, err := dal.MarkDownloadedCAS(ctx, tx, batch.ID, fileHash, batch.UpdatedAt, dlNow)
		if err != nil {
			return err
		}
		if !won {
			return errDownloadedCASLost
		}
		return nil
	})
	if errors.Is(err, errDownloadedCASLost) {
		return DownloadResult{Transitioned: true, Won: false}, nil
	}
	if err != nil {
		return DownloadResult{}, err
	}
	return DownloadResult{
		FileHash:     fileHash,
		Transitioned: true,
		Won:          true,
		UpdatedAt:    dlNow,
	}, nil
}

// DownloadAndParse 下载指定账期的账单、校验响应头并严格解析为 ChannelBill 切片。
func (d *BillDownloader) DownloadAndParse(ctx context.Context, billDate string) ([]dal.ChannelBill, string, error) {
	var lastErr error
	for attempt := 0; attempt <= d.maxRetries; attempt++ {
		if attempt > 0 {
			if err := d.sleep(ctx, d.retryInterval); err != nil {
				return nil, "", err
			}
		}
		res, retryable, err := d.fetchOnce(ctx, billDate)
		if err != nil {
			if !retryable {
				return nil, "", err
			}
			lastErr = err
			continue
		}
		if isBillNotReadyResponse(res.contentType, res.body) {
			return nil, "", ErrBillNotReady
		}
		return verifyAndParseBill(res.body, res.sha256Header, res.countHeader, billDate)
	}
	if lastErr == nil {
		lastErr = errors.New("bill download failed")
	}
	return nil, "", lastErr
}

type fetchedBill struct {
	contentType  string
	sha256Header string
	countHeader  string
	body         []byte
}

func (d *BillDownloader) fetchOnce(ctx context.Context, billDate string) (fetchedBill, bool, error) {
	reqCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	target := d.billURL + "?date=" + url.QueryEscape(billDate)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target, nil)
	if err != nil {
		return fetchedBill{}, false, fmt.Errorf("build bill request: %w", err)
	}
	if len(d.key) > 0 {
		ts := sign.FormatTimestamp(d.clk.Now())
		sig, serr := sign.SignChannel(d.key, sign.ChannelMessage{Timestamp: ts, Body: nil})
		if serr != nil {
			return fetchedBill{}, false, fmt.Errorf("sign bill request: %w", serr)
		}
		req.Header.Set(sign.HeaderMockTimestamp, ts)
		req.Header.Set(sign.HeaderMockSignature, sig)
	}

	resp, err := d.http.Do(req)
	if err != nil {
		return fetchedBill{}, true, fmt.Errorf("bill http request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fetchedBill{}, true, fmt.Errorf("bill http status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBillResponseBytes+1))
	if err != nil {
		return fetchedBill{}, true, fmt.Errorf("read bill response: %w", err)
	}
	if len(body) > maxBillResponseBytes {
		return fetchedBill{}, false, errors.New("bill response exceeds size limit")
	}

	return fetchedBill{
		contentType:  resp.Header.Get("Content-Type"),
		sha256Header: resp.Header.Get("X-Bill-Sha256"),
		countHeader:  resp.Header.Get("X-Bill-Count"),
		body:         body,
	}, false, nil
}

func isBillNotReadyResponse(contentType string, body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if !strings.Contains(strings.ToLower(contentType), "application/json") && !bytes.HasPrefix(trimmed, []byte("{")) {
		return false
	}
	var msg struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(trimmed, &msg); err != nil {
		return false
	}
	return msg.Result == "BILL_NOT_READY"
}

func verifyAndParseBill(body []byte, shaHeader, countHeader, billDate string) ([]dal.ChannelBill, string, error) {
	if shaHeader == "" {
		return nil, "", errors.New("missing X-Bill-Sha256 header")
	}
	sum := sha256.Sum256(body)
	actualHash := hex.EncodeToString(sum[:])
	if !strings.EqualFold(shaHeader, actualHash) || shaHeader != actualHash {
		return nil, "", fmt.Errorf("bill sha256 mismatch: header=%s actual=%s", shaHeader, actualHash)
	}
	if countHeader == "" {
		return nil, "", errors.New("missing X-Bill-Count header")
	}
	wantCount, err := strconv.Atoi(countHeader)
	if err != nil || wantCount < 0 {
		return nil, "", fmt.Errorf("invalid X-Bill-Count header %q", countHeader)
	}
	rows, err := ParseBillCSV(body, "", "", billDate)
	if err != nil {
		return nil, "", err
	}
	if len(rows) != wantCount {
		return nil, "", fmt.Errorf("bill count mismatch: header=%d actual=%d", wantCount, len(rows))
	}
	return rows, actualHash, nil
}

// ParseBillCSV 按 docs/05-api.md §9.6 严格解析账单 CSV，任一行错误返回带行号的错误。
func ParseBillCSV(data []byte, batchNo, channel, billDate string) ([]dal.ChannelBill, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = len(expectedBillHeader)
	r.ReuseRecord = true

	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("bill csv line 1: %w", err)
	}
	for i, col := range expectedBillHeader {
		if i >= len(header) || header[i] != col {
			return nil, fmt.Errorf("bill csv line 1: invalid header column %d", i+1)
		}
	}

	var rows []dal.ChannelBill
	lineNum := 1
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		lineNum++
		if err != nil {
			return nil, fmt.Errorf("bill csv line %d: %w", lineNum, err)
		}
		row, perr := parseBillCSVRow(rec, lineNum, batchNo, channel, billDate)
		if perr != nil {
			return nil, perr
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseBillCSVRow(rec []string, lineNum int, batchNo, channel, billDate string) (dal.ChannelBill, error) {
	txnType := rec[0]
	if txnType != "PAY" && txnType != "REFUND" {
		return dal.ChannelBill{}, fmt.Errorf("bill csv line %d: invalid txn_type %q", lineNum, txnType)
	}
	reqNo := rec[1]
	if reqNo == "" || utf8.RuneCountInString(reqNo) > 32 {
		return dal.ChannelBill{}, fmt.Errorf("bill csv line %d: invalid req_no", lineNum)
	}
	channelTxnNo := rec[2]
	if channelTxnNo == "" || utf8.RuneCountInString(channelTxnNo) > 64 {
		return dal.ChannelBill{}, fmt.Errorf("bill csv line %d: invalid channel_txn_no", lineNum)
	}
	amount, err := strconv.ParseInt(rec[3], 10, 64)
	if err != nil || amount <= 0 {
		return dal.ChannelBill{}, fmt.Errorf("bill csv line %d: invalid amount %q", lineNum, rec[3])
	}
	status := rec[4]
	if status == "" || utf8.RuneCountInString(status) > 16 {
		return dal.ChannelBill{}, fmt.Errorf("bill csv line %d: invalid status %q", lineNum, status)
	}
	finishedAt, err := parseBillTimestamp(rec[5])
	if err != nil {
		return dal.ChannelBill{}, fmt.Errorf("bill csv line %d: invalid finished_at %q", lineNum, rec[5])
	}
	return dal.ChannelBill{
		BatchNo:           batchNo,
		Channel:           channel,
		BillDate:          billDate,
		TxnType:           txnType,
		ChannelTxnNo:      channelTxnNo,
		BizNo:             reqNo,
		Amount:            amount,
		ChannelStatus:     status,
		ChannelFinishedAt: finishedAt,
		MatchStatus:       dal.BillMatchUnchecked,
	}, nil
}

func parseBillTimestamp(s string) (time.Time, error) {
	if t, err := time.Parse(billCSVTimestampLayout, s); err == nil {
		return t.In(shanghai).Truncate(time.Millisecond), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.In(shanghai).Truncate(time.Millisecond), nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
