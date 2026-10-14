package config

// MerchantDemo 是商户演示进程的配置。
type MerchantDemo struct {
	WorkerID                 int    `env:"WORKER_ID,required,notEmpty"`
	OTELExporterOTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	FailFirstN               int    `env:"FAIL_FIRST_N" envDefault:"0"`
	FaultPoints              string `env:"FAULT_POINTS" envDefault:""`
}

// LoadMerchantDemo 加载商户演示配置。environ 是完整环境，不会再读取进程环境。
func LoadMerchantDemo(environ []string) (MerchantDemo, error) {
	var cfg MerchantDemo
	values, err := parse(&cfg, environ)
	if err != nil {
		return MerchantDemo{}, err
	}
	applyOTEL(values, &cfg.OTELExporterOTLPEndpoint)
	if err := checkWorkerID(cfg.WorkerID); err != nil {
		return MerchantDemo{}, err
	}
	if err := checkMin("FAIL_FIRST_N", cfg.FailFirstN, 0); err != nil {
		return MerchantDemo{}, err
	}
	return cfg, nil
}
