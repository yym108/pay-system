package config

import "time"

// Payment 是 payment 进程的配置。MYSQL_DSN 原样保存，不在这里改用户或口令。
type Payment struct {
	WorkerID                 int           `env:"WORKER_ID,required,notEmpty"`
	MySQLDSN                 string        `env:"MYSQL_DSN,required,notEmpty" secret:"true"`
	AccountShardCount        int           `env:"ACCOUNT_SHARD_COUNT" envDefault:"2"`
	TransferParallelBranch   bool          `env:"TRANSFER_PARALLEL_BRANCH" envDefault:"true"`
	TransferRecoverDelay     time.Duration `env:"TRANSFER_RECOVER_DELAY" envDefault:"10s"`
	TransferRecoverInterval  time.Duration `env:"TRANSFER_RECOVER_INTERVAL" envDefault:"1s"`
	MerchantConfigCacheTTL   time.Duration `env:"MERCHANT_CONFIG_CACHE_TTL" envDefault:"60s"`
	KafkaBrokers             string        `env:"KAFKA_BROKERS" envDefault:"kafka:9092"`
	OTELExporterOTLPEndpoint string        `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	SecretMasterKey          string        `env:"SECRET_MASTER_KEY,required,notEmpty" secret:"true"`
	ChannelMockpayKey        string        `env:"CHANNEL_MOCKPAY_KEY,required,notEmpty" secret:"true"`
	ChannelMockpayURL        string        `env:"CHANNEL_MOCKPAY_URL" envDefault:"http://channel-mock:9080"`
	ChannelNotifyURL         string        `env:"CHANNEL_NOTIFY_URL" envDefault:"http://gateway:8080/callback/mockpay"`
	NotifyURLAllowHosts      string        `env:"NOTIFY_URL_ALLOW_HOSTS" envDefault:"merchant-demo"`
	QueryBackoff             DurationList  `env:"QUERY_BACKOFF"`
	NotifyRetryInterval      time.Duration `env:"NOTIFY_RETRY_INTERVAL" envDefault:"30s"`
	NotifyMaxAttempts        int           `env:"NOTIFY_MAX_ATTEMPTS" envDefault:"5"`
	FaultPoints              string        `env:"FAULT_POINTS" envDefault:""`
}

// LoadPayment 加载 payment 配置。environ 是完整环境，不会再读取进程环境。
func LoadPayment(environ []string) (Payment, error) {
	var cfg Payment
	values, err := parse(&cfg, environ)
	if err != nil {
		return Payment{}, err
	}
	applyOTEL(values, &cfg.OTELExporterOTLPEndpoint)
	if err := applyQueryBackoff(values, &cfg.QueryBackoff); err != nil {
		return Payment{}, err
	}
	if err := cfg.validate(); err != nil {
		return Payment{}, err
	}
	return cfg, nil
}

func (cfg Payment) validate() error {
	if err := checkWorkerID(cfg.WorkerID); err != nil {
		return err
	}
	if err := checkShardCount(cfg.AccountShardCount); err != nil {
		return err
	}
	if err := checkNonNegative("TRANSFER_RECOVER_DELAY", cfg.TransferRecoverDelay); err != nil {
		return err
	}
	if err := checkNonNegative("TRANSFER_RECOVER_INTERVAL", cfg.TransferRecoverInterval); err != nil {
		return err
	}
	if err := checkNonNegative("MERCHANT_CONFIG_CACHE_TTL", cfg.MerchantConfigCacheTTL); err != nil {
		return err
	}
	if err := checkNonNegative("NOTIFY_RETRY_INTERVAL", cfg.NotifyRetryInterval); err != nil {
		return err
	}
	if err := checkMasterKey(cfg.SecretMasterKey); err != nil {
		return err
	}
	return checkMin("NOTIFY_MAX_ATTEMPTS", cfg.NotifyMaxAttempts, 1)
}
