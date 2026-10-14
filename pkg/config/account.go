package config

// Account 是 account 进程的配置。两个分片 DSN 原样保存。
type Account struct {
	WorkerID                 int    `env:"WORKER_ID,required,notEmpty"`
	AccountDBDSN0            string `env:"ACCOUNT_DB_DSN_0,required,notEmpty" secret:"true"`
	AccountDBDSN1            string `env:"ACCOUNT_DB_DSN_1,required,notEmpty" secret:"true"`
	AccountShardCount        int    `env:"ACCOUNT_SHARD_COUNT" envDefault:"2"`
	PlatformSubAccounts      int    `env:"PLATFORM_SUB_ACCOUNTS" envDefault:"8"`
	LedgerBatchEnabled       bool   `env:"LEDGER_BATCH_ENABLED" envDefault:"true"`
	LedgerBatchSize          int    `env:"LEDGER_BATCH_SIZE" envDefault:"100"`
	KafkaBrokers             string `env:"KAFKA_BROKERS" envDefault:"kafka:9092"`
	OTELExporterOTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	FaultPoints              string `env:"FAULT_POINTS" envDefault:""`
}

// LoadAccount 加载 account 配置。environ 是完整环境，不会再读取进程环境。
func LoadAccount(environ []string) (Account, error) {
	var cfg Account
	values, err := parse(&cfg, environ)
	if err != nil {
		return Account{}, err
	}
	applyOTEL(values, &cfg.OTELExporterOTLPEndpoint)
	if err := cfg.validate(); err != nil {
		return Account{}, err
	}
	return cfg, nil
}

func (cfg Account) validate() error {
	if err := checkWorkerID(cfg.WorkerID); err != nil {
		return err
	}
	if err := checkShardCount(cfg.AccountShardCount); err != nil {
		return err
	}
	if err := checkMin("PLATFORM_SUB_ACCOUNTS", cfg.PlatformSubAccounts, 1); err != nil {
		return err
	}
	return checkMin("LEDGER_BATCH_SIZE", cfg.LedgerBatchSize, 1)
}
