package config

// Recon 是对账进程的配置。
// 不读取 MYSQL_DSN。四条生效 DSN 的用户都是 svc_recon，口令都是 MYSQL_RECON_PASSWORD。
type Recon struct {
	WorkerID                 int    `env:"WORKER_ID,required,notEmpty"`
	AccountShardCount        int    `env:"ACCOUNT_SHARD_COUNT" envDefault:"2"`
	OTELExporterOTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	ReconRunAt               string `env:"RECON_RUN_AT" envDefault:"10:00"`
	FaultPoints              string `env:"FAULT_POINTS" envDefault:""`
	MySQLReconPassword       string `env:"MYSQL_RECON_PASSWORD,required,notEmpty" secret:"true"`
	AccountDBDSN0            string `env:"ACCOUNT_DB_DSN_0,required,notEmpty" secret:"true"`
	AccountDBDSN1            string `env:"ACCOUNT_DB_DSN_1,required,notEmpty" secret:"true"`
	DSNPayRecon              string `env:"-" secret:"true"`
	DSNPayPayment            string `env:"-" secret:"true"`
	DSNPayAccount0           string `env:"-" secret:"true"`
	DSNPayAccount1           string `env:"-" secret:"true"`
}

// LoadRecon 加载对账配置，并装配四条只读 DSN。不连接数据库。
func LoadRecon(environ []string) (Recon, error) {
	var cfg Recon
	values, err := parse(&cfg, environ)
	if err != nil {
		return Recon{}, err
	}
	applyOTEL(values, &cfg.OTELExporterOTLPEndpoint)
	if err := cfg.validate(); err != nil {
		return Recon{}, err
	}
	cfg.DSNPayRecon, cfg.DSNPayPayment, cfg.DSNPayAccount0, cfg.DSNPayAccount1, err = assembleRecon(
		cfg.MySQLReconPassword, cfg.AccountDBDSN0, cfg.AccountDBDSN1,
	)
	if err != nil {
		return Recon{}, err
	}
	return cfg, nil
}

func (cfg Recon) validate() error {
	if err := checkWorkerID(cfg.WorkerID); err != nil {
		return err
	}
	if err := checkShardCount(cfg.AccountShardCount); err != nil {
		return err
	}
	return checkReconRunAt(cfg.ReconRunAt)
}
