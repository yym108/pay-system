package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/yym108/pay-system/pkg/config"
	"github.com/yym108/pay-system/pkg/sign"
)

const (
	// maxNotifyBody 与网关请求体上限相同。再多 1 字节就拒绝，恰好等于上限的请求继续处理。
	maxNotifyBody = 64 * 1024

	pathNotify = "/notify"
	pathStats  = "/stats"

	bodySuccess = "SUCCESS"
	bodyFail    = "FAIL"

	contentText = "text/plain; charset=utf-8"
	contentJSON = "application/json; charset=utf-8"
)

// demo 在内存里按单号计数。进程退出后清空。没有管理端口。
type demo struct {
	failFirstN   int
	secretFormat string
	log          *slog.Logger

	mu    sync.Mutex
	stats map[string]*bizStat
}

type bizStat struct {
	count int
	last  []byte
}

type statsView struct {
	Count    int    `json:"count"`
	LastBody string `json:"last_body"`
}

func newDemo(cfg config.MerchantDemo) *demo {
	return &demo{
		failFirstN:   cfg.FailFirstN,
		secretFormat: cfg.AppSecretFormat,
		log:          slog.Default(),
		stats:        make(map[string]*bizStat),
	}
}

func (d *demo) register(srv *server.Hertz) {
	srv.POST(pathNotify, d.handleNotify)
	srv.GET(pathStats, d.handleStats)
}

func (d *demo) handleNotify(ctx context.Context, c *app.RequestContext) {
	body := append([]byte(nil), c.Request.Body()...)
	if len(body) > maxNotifyBody {
		writeRaw(c, http.StatusRequestEntityTooLarge, contentText, nil)
		return
	}
	merchantID, eventType, data, ok := parseNotify(body)
	if !ok {
		writeRaw(c, http.StatusUnauthorized, contentText, nil)
		return
	}
	// 格式是 merchant_id 的纯函数。伪造的商户号对不上签名时仍是 401。
	// 密钥为空是本端配置问题，按 BASE-13 返回 500，不把空密钥送进 HMAC。
	secret := fmt.Sprintf(d.secretFormat, merchantID)
	if secret == "" {
		d.log.ErrorContext(ctx, "merchant demo app secret empty")
		writeRaw(c, http.StatusInternalServerError, contentText, nil)
		return
	}
	err := sign.VerifyPlatform([]byte(secret), sign.PlatformMessage{
		Timestamp: string(c.GetHeader(sign.HeaderTimestamp)),
		Nonce:     string(c.GetHeader(sign.HeaderNonce)),
		Body:      body,
	}, string(c.GetHeader(sign.HeaderSignature)))
	if errors.Is(err, sign.ErrEmptyKey) {
		d.log.ErrorContext(ctx, "merchant demo app secret empty")
		writeRaw(c, http.StatusInternalServerError, contentText, nil)
		return
	}
	if err != nil {
		// 对端签名问题是 401。其余验签错误不当成商户签错。日志不写密钥、签名和拼接串。
		if !peerSignErr(err) {
			d.log.ErrorContext(ctx, "merchant demo verify failed")
			writeRaw(c, http.StatusInternalServerError, contentText, nil)
			return
		}
		writeRaw(c, http.StatusUnauthorized, contentText, nil)
		return
	}
	bizNo, ok := bizNoFrom(eventType, data)
	if !ok {
		writeRaw(c, http.StatusBadRequest, contentText, nil)
		return
	}
	n := d.record(bizNo, body)
	if n <= d.failFirstN {
		writeRaw(c, http.StatusInternalServerError, contentText, []byte(bodyFail))
		return
	}
	writeRaw(c, http.StatusOK, contentText, []byte(bodySuccess))
}

func (d *demo) handleStats(_ context.Context, c *app.RequestContext) {
	// 缺少参数和空串都不查内存。Peek 在键不存在时为 nil，和空值一样是空串。
	bizNo := string(c.QueryArgs().Peek("biz_no"))
	if bizNo == "" {
		writeRaw(c, http.StatusBadRequest, contentText, nil)
		return
	}
	d.mu.Lock()
	view := statsView{}
	if st := d.stats[bizNo]; st != nil {
		view.Count = st.count
		view.LastBody = string(st.last)
	}
	d.mu.Unlock()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(view); err != nil {
		d.log.Error("merchant demo stats encode failed")
		writeRaw(c, http.StatusInternalServerError, contentText, nil)
		return
	}
	// Encoder 会加换行。去掉，让 body 只有一个 JSON 对象。
	raw := bytes.TrimRight(buf.Bytes(), "\n")
	writeRaw(c, http.StatusOK, contentJSON, raw)
}

func (d *demo) record(bizNo string, body []byte) int {
	saved := append([]byte(nil), body...)
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.stats[bizNo]
	if st == nil {
		st = &bizStat{}
		d.stats[bizNo] = st
	}
	st.count++
	st.last = saved
	return st.count
}

func peerSignErr(err error) bool {
	return errors.Is(err, sign.ErrMalformedSignature) ||
		errors.Is(err, sign.ErrSignatureMismatch) ||
		errors.Is(err, sign.ErrInvalidField)
}

func parseNotify(body []byte) (merchantID, eventType string, data json.RawMessage, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var raw struct {
		EventType  string          `json:"event_type"`
		MerchantID json.RawMessage `json:"merchant_id"`
		Data       json.RawMessage `json:"data"`
	}
	if err := dec.Decode(&raw); err != nil || dec.More() {
		return "", "", nil, false
	}
	if len(raw.MerchantID) == 0 {
		return "", "", nil, false
	}
	if err := json.Unmarshal(raw.MerchantID, &merchantID); err != nil {
		return "", "", nil, false
	}
	return merchantID, raw.EventType, raw.Data, true
}

func bizNoFrom(eventType string, data json.RawMessage) (string, bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var obj map[string]json.RawMessage
	if err := dec.Decode(&obj); err != nil || dec.More() {
		return "", false
	}
	keys := []string{"pay_no", "refund_no", "transfer_no"}
	switch eventType {
	case "payment.succeeded":
		keys = []string{"pay_no"}
	case "refund.succeeded", "refund.failed":
		keys = []string{"refund_no"}
	case "transfer.succeeded", "transfer.failed":
		keys = []string{"transfer_no"}
	}
	for _, key := range keys {
		raw, exists := obj[key]
		if !exists {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil || s == "" {
			if len(keys) == 1 {
				return "", false
			}
			continue
		}
		return s, true
	}
	return "", false
}

func writeRaw(c *app.RequestContext, status int, contentType string, body []byte) {
	c.SetStatusCode(status)
	c.Response.Header.Set("Content-Type", contentType)
	if body == nil {
		body = []byte{}
	}
	c.Response.SetBodyRaw(body)
}
