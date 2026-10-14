package config

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	testPayPass     = "payment-plain-password"
	testMockKey     = "mockpay-plain-key"
	testAcctPass    = "account-plain-password"
	testReconPass   = "recon-plain-password"
	testChannelPass = "channel-mock-plain-password"
	testAdminToken  = "0123456789abcdef0123456789abcdef"
	testQuery       = "parseTime=true&loc=Asia%2FShanghai&charset=utf8mb4"
)

func testMasterKey() string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x3c}, 32))
}

func tcpDSN(user, pass, addr, db, query string) string {
	dsn := user + ":" + pass + "@tcp(" + addr + ")/" + db
	if query != "" {
		dsn += "?" + query
	}
	return dsn
}

func paymentDSN() string {
	return tcpDSN("svc_payment", testPayPass, "mysql:3306", "pay_payment", testQuery)
}

func accountDSN(db string) string {
	return tcpDSN("svc_account", testAcctPass, "mysql:3306", db, testQuery)
}

func environ(kv map[string]string) []string {
	out := make([]string, 0, len(kv))
	for k, v := range kv {
		out = append(out, k+"="+v)
	}
	return out
}

func cloneEnv(kv map[string]string) map[string]string {
	out := make(map[string]string, len(kv))
	for k, v := range kv {
		out[k] = v
	}
	return out
}

func paymentEnv() map[string]string {
	return map[string]string{
		"WORKER_ID":           "2",
		"MYSQL_DSN":           paymentDSN(),
		"SECRET_MASTER_KEY":   testMasterKey(),
		"CHANNEL_MOCKPAY_KEY": testMockKey,
	}
}

func assertInvalid(t *testing.T, err error, name string, secrets ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Load succeeded, want error containing %s", name)
	}
	msg := err.Error()
	if !strings.Contains(msg, name) {
		t.Fatalf("error %q does not contain %s", msg, name)
	}
	for _, secret := range secrets {
		if secret != "" && strings.Contains(msg, secret) {
			t.Fatalf("error contains secret material for %s", name)
		}
	}
}

func assertNoMaterial(t *testing.T, out, label string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(out, secret) {
			t.Fatalf("%s contains secret material", label)
		}
	}
}

func parsedDSN(t *testing.T, dsn string) *mysql.Config {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("assembled dsn is not valid")
	}
	return cfg
}

func assertDSN(t *testing.T, dsn, user, pass, addr, db string) {
	t.Helper()
	cfg := parsedDSN(t, dsn)
	if cfg.User != user {
		t.Fatalf("dsn user = %s, want %s", cfg.User, user)
	}
	if cfg.Addr != addr {
		t.Fatalf("dsn addr = %s, want %s", cfg.Addr, addr)
	}
	if cfg.DBName != db {
		t.Fatalf("dsn db = %s, want %s", cfg.DBName, db)
	}
	if cfg.Passwd != pass {
		t.Fatal("dsn password mismatch")
	}
}

// BASE-09：加载只用调用方给出的环境，不读取进程里的密钥。
func TestLoadPaymentIgnoresProcessEnv(t *testing.T) {
	t.Setenv("WORKER_ID", "9")
	t.Setenv("MYSQL_DSN", paymentDSN())
	t.Setenv("SECRET_MASTER_KEY", testMasterKey())
	t.Setenv("CHANNEL_MOCKPAY_KEY", testMockKey)
	_, err := LoadPayment(nil)
	assertInvalid(t, err, "SECRET_MASTER_KEY")
	assertInvalid(t, err, "WORKER_ID")
}

func TestLoadPaymentDefaultsAndShorterDurations(t *testing.T) {
	t.Parallel()
	env := paymentEnv()
	env["QUERY_BACKOFF"] = "1ms,2ms"
	env["NOTIFY_RETRY_INTERVAL"] = "1ms"
	env["TRANSFER_RECOVER_DELAY"] = "1ms"
	env["TRANSFER_RECOVER_INTERVAL"] = "2ms"
	env["MERCHANT_CONFIG_CACHE_TTL"] = "0s"
	env["TRANSFER_PARALLEL_BRANCH"] = "false"
	env["NOTIFY_MAX_ATTEMPTS"] = "1"
	env["OTEL_EXPORTER_OTLP_ENDPOINT"] = ""
	cfg, err := LoadPayment(environ(env))
	if err != nil {
		t.Fatalf("LoadPayment: %v", err)
	}
	if cfg.WorkerID != 2 || cfg.AccountShardCount != 2 || cfg.TransferParallelBranch {
		t.Fatalf("worker/shard/parallel = %d %d %v", cfg.WorkerID, cfg.AccountShardCount, cfg.TransferParallelBranch)
	}
	if cfg.MySQLDSN != paymentDSN() {
		t.Fatal("MYSQL_DSN was rewritten")
	}
	if cfg.TransferRecoverDelay != time.Millisecond || cfg.TransferRecoverInterval != 2*time.Millisecond {
		t.Fatalf("recover durations = %s %s", cfg.TransferRecoverDelay, cfg.TransferRecoverInterval)
	}
	if cfg.MerchantConfigCacheTTL != 0 {
		t.Fatalf("cache ttl = %s, want 0", cfg.MerchantConfigCacheTTL)
	}
	if cfg.KafkaBrokers != "kafka:9092" || cfg.ChannelMockpayURL != "http://channel-mock:9080" {
		t.Fatalf("defaults brokers=%s url=%s", cfg.KafkaBrokers, cfg.ChannelMockpayURL)
	}
	if cfg.ChannelNotifyURL != "http://gateway:8080/callback/mockpay" || cfg.NotifyURLAllowHosts != "merchant-demo" {
		t.Fatalf("notify defaults %s %s", cfg.ChannelNotifyURL, cfg.NotifyURLAllowHosts)
	}
	if cfg.OTELExporterOTLPEndpoint != "" {
		t.Fatalf("empty otel = %q, want disabled", cfg.OTELExporterOTLPEndpoint)
	}
	if cfg.NotifyRetryInterval != time.Millisecond || cfg.NotifyMaxAttempts != 1 {
		t.Fatalf("notify retry = %s attempts %d", cfg.NotifyRetryInterval, cfg.NotifyMaxAttempts)
	}
	want := DurationList{time.Millisecond, 2 * time.Millisecond}
	if len(cfg.QueryBackoff) != len(want) || cfg.QueryBackoff[0] != want[0] || cfg.QueryBackoff[1] != want[1] {
		t.Fatalf("backoff = %s", cfg.QueryBackoff)
	}
	if cfg.SecretMasterKey != testMasterKey() || cfg.ChannelMockpayKey != testMockKey {
		t.Fatal("secret fields were modified")
	}
}

func TestLoadPaymentDefaultBackoffAndOTEL(t *testing.T) {
	t.Parallel()
	cfg, err := LoadPayment(environ(paymentEnv()))
	if err != nil {
		t.Fatalf("LoadPayment: %v", err)
	}
	want := []time.Duration{
		10 * time.Second, 10 * time.Second, 30 * time.Second,
		time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute,
	}
	if len(cfg.QueryBackoff) != len(want) {
		t.Fatalf("backoff len = %d", len(cfg.QueryBackoff))
	}
	for i, d := range want {
		if cfg.QueryBackoff[i] != d {
			t.Fatalf("backoff[%d] = %s, want %s", i, cfg.QueryBackoff[i], d)
		}
	}
	if cfg.OTELExporterOTLPEndpoint != "jaeger:4317" {
		t.Fatalf("otel = %q", cfg.OTELExporterOTLPEndpoint)
	}
	if cfg.NotifyRetryInterval != 30*time.Second || cfg.NotifyMaxAttempts != 5 {
		t.Fatalf("notify defaults %s %d", cfg.NotifyRetryInterval, cfg.NotifyMaxAttempts)
	}
	if cfg.TransferRecoverDelay != 10*time.Second || cfg.TransferRecoverInterval != time.Second {
		t.Fatalf("recover defaults %s %s", cfg.TransferRecoverDelay, cfg.TransferRecoverInterval)
	}
	if !cfg.TransferParallelBranch || cfg.MerchantConfigCacheTTL != time.Minute {
		t.Fatalf("parallel %v cache %s", cfg.TransferParallelBranch, cfg.MerchantConfigCacheTTL)
	}
}

func TestLoadPaymentRejectsInvalid(t *testing.T) {
	t.Parallel()
	shortKey := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x3c}, 16))
	cases := []struct {
		name   string
		key    string
		value  string
		secret string
	}{
		{name: "worker high", key: "WORKER_ID", value: "1024"},
		{name: "worker negative", key: "WORKER_ID", value: "-1"},
		{name: "worker empty", key: "WORKER_ID", value: ""},
		{name: "shard", key: "ACCOUNT_SHARD_COUNT", value: "1"},
		{name: "master key", key: "SECRET_MASTER_KEY", value: "%%%not-a-key%%%", secret: "%%%not-a-key%%%"},
		{name: "master key short", key: "SECRET_MASTER_KEY", value: shortKey, secret: shortKey},
		{name: "attempts", key: "NOTIFY_MAX_ATTEMPTS", value: "0"},
		{name: "backoff", key: "QUERY_BACKOFF", value: "-1s"},
		{name: "backoff empty", key: "QUERY_BACKOFF", value: ""},
		{name: "delay", key: "TRANSFER_RECOVER_DELAY", value: "-1s"},
		{name: "mysql dsn empty", key: "MYSQL_DSN", value: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := cloneEnv(paymentEnv())
			env[tc.key] = tc.value
			_, err := LoadPayment(environ(env))
			assertInvalid(t, err, tc.key, tc.secret, testPayPass, testMockKey)
		})
	}
}

func TestLoadPaymentWorkerBounds(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"0", "1023"} {
		env := paymentEnv()
		env["WORKER_ID"] = id
		cfg, err := LoadPayment(environ(env))
		if err != nil {
			t.Fatalf("WORKER_ID=%s: %v", id, err)
		}
		if id == "0" && cfg.WorkerID != 0 {
			t.Fatalf("worker = %d", cfg.WorkerID)
		}
		if id == "1023" && cfg.WorkerID != 1023 {
			t.Fatalf("worker = %d", cfg.WorkerID)
		}
	}
}

func TestRedactPaymentHidesSecrets(t *testing.T) {
	t.Parallel()
	cfg, err := LoadPayment(environ(paymentEnv()))
	if err != nil {
		t.Fatalf("LoadPayment: %v", err)
	}
	out := Redact(cfg)
	if !strings.Contains(out, "***") {
		t.Fatal("redacted config has no ***")
	}
	assertNoMaterial(t, out, "redacted payment", testMasterKey(), testMockKey, testPayPass, paymentDSN())
	if !strings.Contains(out, "WORKER_ID=2") || !strings.Contains(out, "KAFKA_BROKERS=kafka:9092") {
		t.Fatal("non-secret fields are not visible")
	}
	if !strings.Contains(out, "CHANNEL_MOCKPAY_URL=http://channel-mock:9080") {
		t.Fatal("channel url is not visible")
	}
}

func TestLoadGateway(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"WORKER_ID":               "1",
		"ADMIN_TOKEN":             testAdminToken,
		"CHANNEL_MOCKPAY_KEY":     testMockKey,
		"RATE_LIMIT_OVERRIDES":    "demo:5:9,other:1:2",
		"GW_CREDENTIAL_CACHE_TTL": "0s",
	}
	cfg, err := LoadGateway(environ(env))
	if err != nil {
		t.Fatalf("LoadGateway: %v", err)
	}
	if cfg.WorkerID != 1 || cfg.RedisAddr != "redis:6379" || cfg.OTELExporterOTLPEndpoint != "jaeger:4317" {
		t.Fatalf("gateway defaults %+v", cfg.RedisAddr)
	}
	if cfg.CredentialCacheTTL != 0 || cfg.RateLimitDefault.Rate() != 100 || cfg.RateLimitDefault.Burst() != 200 {
		t.Fatalf("rate default %s ttl %s", cfg.RateLimitDefault, cfg.CredentialCacheTTL)
	}
	if len(cfg.RateLimitOverrides) != 2 || cfg.RateLimitOverrides[0].AppID != "demo" || cfg.RateLimitOverrides[0].Rate != 5 || cfg.RateLimitOverrides[0].Burst != 9 {
		t.Fatalf("overrides = %+v", cfg.RateLimitOverrides)
	}
	if cfg.RateLimitOverrides[1].AppID != "other" || cfg.RateLimitOverrides[1].Burst != 2 {
		t.Fatalf("second override = %+v", cfg.RateLimitOverrides[1])
	}
	out := Redact(cfg)
	assertNoMaterial(t, out, "redacted gateway", testAdminToken, testMockKey)
	if !strings.Contains(out, "***") || !strings.Contains(out, "REDIS_ADDR=redis:6379") || !strings.Contains(out, "RATE_LIMIT_DEFAULT=100:200") {
		t.Fatal("gateway redact missing non-secret or mask")
	}
	if !strings.Contains(out, "demo:5:9") {
		t.Fatal("override is not visible")
	}
}

func TestLoadGatewayRejectsInvalid(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"WORKER_ID":           "1",
		"ADMIN_TOKEN":         testAdminToken,
		"CHANNEL_MOCKPAY_KEY": testMockKey,
	}
	cases := []struct {
		name   string
		key    string
		value  string
		secret string
	}{
		{name: "token short", key: "ADMIN_TOKEN", value: "short-token", secret: "short-token"},
		{name: "token missing", key: "ADMIN_TOKEN", value: "\x00missing"},
		{name: "rate", key: "RATE_LIMIT_DEFAULT", value: "0:200"},
		{name: "rate format", key: "RATE_LIMIT_DEFAULT", value: "100"},
		{name: "override", key: "RATE_LIMIT_OVERRIDES", value: "demo:1"},
		{name: "override zero", key: "RATE_LIMIT_OVERRIDES", value: "demo:0:1"},
		{name: "key empty", key: "CHANNEL_MOCKPAY_KEY", value: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := cloneEnv(base)
			if tc.name == "token missing" {
				delete(env, "ADMIN_TOKEN")
				_, err := LoadGateway(environ(env))
				assertInvalid(t, err, "ADMIN_TOKEN", testMockKey)
				return
			}
			env[tc.key] = tc.value
			_, err := LoadGateway(environ(env))
			assertInvalid(t, err, tc.key, tc.secret, testMockKey)
		})
	}
	env := cloneEnv(base)
	env["RATE_LIMIT_OVERRIDES"] = ""
	if _, err := LoadGateway(environ(env)); err != nil {
		t.Fatalf("empty overrides: %v", err)
	}
}

func TestLoadAccount(t *testing.T) {
	t.Parallel()
	dsn0 := accountDSN("pay_account_0")
	dsn1 := accountDSN("pay_account_1")
	env := map[string]string{
		"WORKER_ID":             "3",
		"ACCOUNT_DB_DSN_0":      dsn0,
		"ACCOUNT_DB_DSN_1":      dsn1,
		"LEDGER_BATCH_SIZE":     "1",
		"PLATFORM_SUB_ACCOUNTS": "1",
		"LEDGER_BATCH_ENABLED":  "false",
	}
	cfg, err := LoadAccount(environ(env))
	if err != nil {
		t.Fatalf("LoadAccount: %v", err)
	}
	if cfg.AccountDBDSN0 != dsn0 || cfg.AccountDBDSN1 != dsn1 {
		t.Fatal("account DSN was rewritten")
	}
	if cfg.AccountShardCount != 2 || cfg.PlatformSubAccounts != 1 || cfg.LedgerBatchSize != 1 || cfg.LedgerBatchEnabled {
		t.Fatalf("account values shard=%d subs=%d batch=%d enabled=%v", cfg.AccountShardCount, cfg.PlatformSubAccounts, cfg.LedgerBatchSize, cfg.LedgerBatchEnabled)
	}
	if cfg.KafkaBrokers != "kafka:9092" {
		t.Fatalf("brokers = %s", cfg.KafkaBrokers)
	}
	out := Redact(cfg)
	assertNoMaterial(t, out, "redacted account", testAcctPass, dsn0, dsn1)
	if !strings.Contains(out, "***") || !strings.Contains(out, "WORKER_ID=3") {
		t.Fatal("account redact missing mask or worker")
	}

	bad := cloneEnv(env)
	bad["PLATFORM_SUB_ACCOUNTS"] = "0"
	_, err = LoadAccount(environ(bad))
	assertInvalid(t, err, "PLATFORM_SUB_ACCOUNTS", testAcctPass)
	bad["PLATFORM_SUB_ACCOUNTS"] = "1"
	bad["LEDGER_BATCH_SIZE"] = "0"
	_, err = LoadAccount(environ(bad))
	assertInvalid(t, err, "LEDGER_BATCH_SIZE", testAcctPass)
	bad["LEDGER_BATCH_SIZE"] = "100"
	bad["ACCOUNT_SHARD_COUNT"] = "3"
	_, err = LoadAccount(environ(bad))
	assertInvalid(t, err, "ACCOUNT_SHARD_COUNT", testAcctPass)
}

func TestLoadReconAssemblesDSN(t *testing.T) {
	t.Parallel()
	pass0 := "p@ss/w:rd"
	dsn0 := tcpDSN("svc_account", pass0, "db.internal:3307", "pay_account_0", testQuery)
	dsn1 := tcpDSN("svc_account", "other-account-password", "elsewhere:3308", "pay_account_1", "parseTime=true")
	paymentPassword := "payment-password-must-not-enter-recon"
	env := map[string]string{
		"WORKER_ID":                   "4",
		"MYSQL_RECON_PASSWORD":        testReconPass,
		"ACCOUNT_DB_DSN_0":            dsn0,
		"ACCOUNT_DB_DSN_1":            dsn1,
		"MYSQL_DSN":                   tcpDSN("svc_payment", paymentPassword, "mysql:3306", "pay_payment", testQuery),
		"ACCOUNT_SHARD_COUNT":         "2",
		"RECON_RUN_AT":                "00:15",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "",
	}
	cfg, err := LoadRecon(environ(env))
	if err != nil {
		t.Fatalf("LoadRecon: %v", err)
	}
	if cfg.ReconRunAt != "00:15" || cfg.OTELExporterOTLPEndpoint != "" || cfg.AccountShardCount != 2 {
		t.Fatalf("recon run=%s otel=%q shard=%d", cfg.ReconRunAt, cfg.OTELExporterOTLPEndpoint, cfg.AccountShardCount)
	}
	assertDSN(t, cfg.DSNPayRecon, "svc_recon", testReconPass, "db.internal:3307", "pay_recon")
	assertDSN(t, cfg.DSNPayPayment, "svc_recon", testReconPass, "db.internal:3307", "pay_payment")
	assertDSN(t, cfg.DSNPayAccount0, "svc_recon", testReconPass, "db.internal:3307", "pay_account_0")
	assertDSN(t, cfg.DSNPayAccount1, "svc_recon", testReconPass, "db.internal:3307", "pay_account_1")
	for _, dsn := range []string{cfg.DSNPayRecon, cfg.DSNPayPayment, cfg.DSNPayAccount0, cfg.DSNPayAccount1} {
		if !strings.Contains(dsn, testQuery) {
			t.Fatal("assembled dsn dropped the query from ACCOUNT_DB_DSN_0")
		}
		if strings.Contains(dsn, paymentPassword) || strings.Contains(dsn, pass0) || strings.Contains(dsn, "other-account-password") {
			t.Fatal("assembled dsn kept an input password")
		}
	}
	out := Redact(cfg)
	assertNoMaterial(t, out, "redacted recon", testReconPass, pass0, paymentPassword, "other-account-password", dsn0, dsn1)
	if !strings.Contains(out, "***") || !strings.Contains(out, "RECON_RUN_AT=00:15") || !strings.Contains(out, "WORKER_ID=4") {
		t.Fatal("recon redact missing mask or plain fields")
	}
}

func TestLoadReconRejectsInvalid(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"WORKER_ID":            "4",
		"MYSQL_RECON_PASSWORD": testReconPass,
		"ACCOUNT_DB_DSN_0":     accountDSN("pay_account_0"),
		"ACCOUNT_DB_DSN_1":     accountDSN("pay_account_1"),
	}
	cases := []struct {
		name   string
		key    string
		value  string
		secret string
	}{
		{name: "clock", key: "RECON_RUN_AT", value: "24:00"},
		{name: "clock short", key: "RECON_RUN_AT", value: "9:00"},
		{name: "shard", key: "ACCOUNT_SHARD_COUNT", value: "1"},
		{name: "bad dsn", key: "ACCOUNT_DB_DSN_0", value: "not-a-dsn-unique-bad-marker", secret: "unique-bad-marker"},
		{name: "wrong db", key: "ACCOUNT_DB_DSN_1", value: accountDSN("pay_account_0"), secret: testAcctPass},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := cloneEnv(base)
			env[tc.key] = tc.value
			_, err := LoadRecon(environ(env))
			assertInvalid(t, err, tc.key, tc.secret, testReconPass, testAcctPass)
		})
	}
}

func TestLoadChannelMockAssemblesDSN(t *testing.T) {
	t.Parallel()
	paymentPassword := "payment-password-must-not-enter-channel-mock"
	env := map[string]string{
		"WORKER_ID":                   "5",
		"CHANNEL_MOCKPAY_KEY":         testMockKey,
		"MYSQL_CHANNEL_MOCK_PASSWORD": testChannelPass,
		"MYSQL_DSN":                   tcpDSN("svc_payment", paymentPassword, "mysql:3306", "pay_payment", testQuery),
		"MOCK_DELAY_MIN_MS":           "0",
		"MOCK_DELAY_MAX_MS":           "0",
		"MOCK_ALLOW_PARTIAL_BILL":     "true",
	}
	cfg, err := LoadChannelMock(environ(env))
	if err != nil {
		t.Fatalf("LoadChannelMock: %v", err)
	}
	if cfg.MySQLHost != "mysql" || cfg.MySQLPort != 3306 {
		t.Fatalf("host=%s port=%d", cfg.MySQLHost, cfg.MySQLPort)
	}
	if cfg.MockDelayMinMS != 0 || cfg.MockDelayMaxMS != 0 || !cfg.MockAllowPartialBill {
		t.Fatalf("delay %d %d partial %v", cfg.MockDelayMinMS, cfg.MockDelayMaxMS, cfg.MockAllowPartialBill)
	}
	assertDSN(t, cfg.DSN, "svc_channel_mock", testChannelPass, "mysql:3306", "pay_channel_mock")
	cfgParsed := parsedDSN(t, cfg.DSN)
	if !cfgParsed.ParseTime || cfgParsed.Loc == nil || cfgParsed.Loc.String() != "Asia/Shanghai" {
		t.Fatal("channel dsn query is not the fixed loc and parseTime")
	}
	if strings.Contains(cfg.DSN, paymentPassword) {
		t.Fatal("channel dsn contains the payment password")
	}
	if !strings.Contains(cfg.DSN, "charset=utf8mb4") || !strings.Contains(cfg.DSN, "loc=Asia%2FShanghai") {
		t.Fatal("channel dsn query does not match the fixed text")
	}
	out := Redact(cfg)
	assertNoMaterial(t, out, "redacted channel-mock", testChannelPass, testMockKey, paymentPassword, cfg.DSN)
	if !strings.Contains(out, "***") || !strings.Contains(out, "MYSQL_HOST=mysql") || !strings.Contains(out, "WORKER_ID=5") {
		t.Fatal("channel redact missing mask or host")
	}

	custom := cloneEnv(env)
	custom["MYSQL_HOST"] = "127.0.0.1"
	custom["MYSQL_PORT"] = "13306"
	custom["MOCK_DELAY_MIN_MS"] = "200"
	custom["MOCK_DELAY_MAX_MS"] = "2000"
	got, err := LoadChannelMock(environ(custom))
	if err != nil {
		t.Fatalf("custom host: %v", err)
	}
	assertDSN(t, got.DSN, "svc_channel_mock", testChannelPass, "127.0.0.1:13306", "pay_channel_mock")
	if got.MockDelayMinMS != 200 || got.MockDelayMaxMS != 2000 || got.MockAllowPartialBill != true {
		t.Fatalf("custom delay %d %d", got.MockDelayMinMS, got.MockDelayMaxMS)
	}
}

func TestLoadChannelMockRejectsInvalid(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"WORKER_ID":                   "5",
		"CHANNEL_MOCKPAY_KEY":         testMockKey,
		"MYSQL_CHANNEL_MOCK_PASSWORD": testChannelPass,
	}
	cases := []struct {
		name string
		key  string
		val  string
	}{
		{name: "min above max", key: "MOCK_DELAY_MIN_MS", val: "3000"},
		{name: "negative", key: "MOCK_DELAY_MAX_MS", val: "-1"},
		{name: "port", key: "MYSQL_PORT", val: "0"},
		{name: "port high", key: "MYSQL_PORT", val: "65536"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := cloneEnv(base)
			env[tc.key] = tc.val
			if tc.key == "MOCK_DELAY_MIN_MS" {
				env["MOCK_DELAY_MAX_MS"] = "2000"
			}
			_, err := LoadChannelMock(environ(env))
			assertInvalid(t, err, tc.key, testChannelPass, testMockKey)
		})
	}
}

func TestLoadMerchantDemo(t *testing.T) {
	t.Parallel()
	cfg, err := LoadMerchantDemo(environ(map[string]string{"WORKER_ID": "6"}))
	if err != nil {
		t.Fatalf("LoadMerchantDemo: %v", err)
	}
	if cfg.WorkerID != 6 || cfg.FailFirstN != 0 || cfg.OTELExporterOTLPEndpoint != "jaeger:4317" || cfg.FaultPoints != "" {
		t.Fatalf("merchant demo defaults worker=%d fail=%d otel=%q fault=%q", cfg.WorkerID, cfg.FailFirstN, cfg.OTELExporterOTLPEndpoint, cfg.FaultPoints)
	}
	env := map[string]string{"WORKER_ID": "0", "FAIL_FIRST_N": "4", "FAULT_POINTS": "drop_notify", "OTEL_EXPORTER_OTLP_ENDPOINT": ""}
	cfg, err = LoadMerchantDemo(environ(env))
	if err != nil {
		t.Fatalf("custom: %v", err)
	}
	if cfg.FailFirstN != 4 || cfg.FaultPoints != "drop_notify" || cfg.OTELExporterOTLPEndpoint != "" {
		t.Fatalf("custom merchant demo %+v", cfg.FaultPoints)
	}
	_, err = LoadMerchantDemo(environ(map[string]string{"WORKER_ID": "6", "FAIL_FIRST_N": "-1"}))
	assertInvalid(t, err, "FAIL_FIRST_N")
	_, err = LoadMerchantDemo(nil)
	assertInvalid(t, err, "WORKER_ID")
}

func TestLoadSeed(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"SECRET_MASTER_KEY": testMasterKey(),
		"ACCOUNT_DB_DSN_0":  accountDSN("pay_account_0"),
		"ACCOUNT_DB_DSN_1":  accountDSN("pay_account_1"),
		"MYSQL_DSN":         paymentDSN(),
	}
	cfg, err := LoadSeed(environ(env))
	if err != nil {
		t.Fatalf("LoadSeed: %v", err)
	}
	if cfg.PlatformSubAccounts != 8 || cfg.SeedLoad != 0 {
		t.Fatalf("subs=%d load=%d", cfg.PlatformSubAccounts, cfg.SeedLoad)
	}
	if cfg.DemoAppSecretFormat != "demo_secret_%s_0123456789ab" {
		t.Fatal("secret format default changed")
	}
	if cfg.MySQLDSN != paymentDSN() || cfg.AccountDBDSN0 != accountDSN("pay_account_0") {
		t.Fatal("seed DSN was rewritten")
	}
	out := Redact(cfg)
	assertNoMaterial(t, out, "redacted seed", testMasterKey(), testPayPass, testAcctPass, "demo_secret_%s_0123456789ab")
	if !strings.Contains(out, "***") || !strings.Contains(out, "PLATFORM_SUB_ACCOUNTS=8") || !strings.Contains(out, "SEED_LOAD=0") {
		t.Fatal("seed redact missing mask or plain fields")
	}

	env["SEED_LOAD"] = "1"
	env["DEMO_APP_SECRET_FORMAT"] = "custom-secret-format-%s"
	env["PLATFORM_SUB_ACCOUNTS"] = "9"
	cfg, err = LoadSeed(environ(env))
	if err != nil {
		t.Fatalf("custom seed: %v", err)
	}
	if cfg.SeedLoad != 1 || cfg.PlatformSubAccounts != 9 || cfg.DemoAppSecretFormat != "custom-secret-format-%s" {
		t.Fatalf("custom seed load=%d", cfg.SeedLoad)
	}
	out = Redact(cfg)
	assertNoMaterial(t, out, "redacted custom seed", "custom-secret-format-%s")

	env["SEED_LOAD"] = "2"
	_, err = LoadSeed(environ(env))
	assertInvalid(t, err, "SEED_LOAD", "custom-secret-format-%s", testMasterKey())
	env["SEED_LOAD"] = "0"
	env["SECRET_MASTER_KEY"] = "%%%not-a-key%%%"
	_, err = LoadSeed(environ(env))
	assertInvalid(t, err, "SECRET_MASTER_KEY", "%%%not-a-key%%%")
}

func TestLoadBootstrap(t *testing.T) {
	t.Parallel()
	rootMaterial := "root-plain-material"
	migratorMaterial := "migrator-plain-material"
	env := map[string]string{
		"MYSQL_ROOT_PASSWORD":         rootMaterial,
		"ACCOUNT_DB_DSN_0":            accountDSN("pay_account_0"),
		"ACCOUNT_DB_DSN_1":            accountDSN("pay_account_1"),
		"MYSQL_DSN":                   paymentDSN(),
		"MYSQL_RECON_PASSWORD":        testReconPass,
		"MYSQL_CHANNEL_MOCK_PASSWORD": testChannelPass,
		"MYSQL_MIGRATOR_PASSWORD":     migratorMaterial,
	}
	cfg, err := LoadBootstrap(environ(env))
	if err != nil {
		t.Fatalf("LoadBootstrap: %v", err)
	}
	if cfg.MySQLHost != "127.0.0.1" || cfg.MySQLPort != 3306 {
		t.Fatalf("bootstrap addr %s %d", cfg.MySQLHost, cfg.MySQLPort)
	}
	if cfg.AccountDBDSN0 != accountDSN("pay_account_0") || cfg.MySQLDSN != paymentDSN() {
		t.Fatal("bootstrap DSN was rewritten")
	}
	out := Redact(cfg)
	assertNoMaterial(t, out, "redacted bootstrap", rootMaterial, testReconPass, testChannelPass, migratorMaterial, testPayPass, testAcctPass)
	if !strings.Contains(out, "***") || !strings.Contains(out, "MYSQL_HOST=127.0.0.1") || !strings.Contains(out, "MYSQL_PORT=3306") {
		t.Fatal("bootstrap redact missing mask or host")
	}
	_, err = LoadBootstrap(nil)
	assertInvalid(t, err, "MYSQL_ROOT_PASSWORD")
}

func TestLoadPayCLI(t *testing.T) {
	t.Parallel()
	cfg, err := LoadPayCLI(nil)
	if err != nil {
		t.Fatalf("LoadPayCLI: %v", err)
	}
	if Redact(cfg) != "" {
		t.Fatalf("empty paycli redact = %q", Redact(cfg))
	}
	cfg, err = LoadPayCLI(environ(map[string]string{"WORKER_ID": "nope", "MYSQL_DSN": paymentDSN()}))
	if err != nil {
		t.Fatalf("paycli should ignore unknown vars: %v", err)
	}
	_ = cfg
}
