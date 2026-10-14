package config

// ChannelMock 是模拟渠道进程的配置。
// 不读取 MYSQL_DSN。MYSQL_HOST 默认 mysql，因为渠道进程跑在 compose 网络里。
type ChannelMock struct {
	WorkerID                 int    `env:"WORKER_ID,required,notEmpty"`
	ChannelMockpayKey        string `env:"CHANNEL_MOCKPAY_KEY,required,notEmpty" secret:"true"`
	MockDelayMinMS           int    `env:"MOCK_DELAY_MIN_MS" envDefault:"200"`
	MockDelayMaxMS           int    `env:"MOCK_DELAY_MAX_MS" envDefault:"2000"`
	MockAllowPartialBill     bool   `env:"MOCK_ALLOW_PARTIAL_BILL" envDefault:"false"`
	OTELExporterOTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	FaultPoints              string `env:"FAULT_POINTS" envDefault:""`
	MySQLHost                string `env:"MYSQL_HOST" envDefault:"mysql"`
	MySQLPort                int    `env:"MYSQL_PORT" envDefault:"3306"`
	MySQLChannelMockPassword string `env:"MYSQL_CHANNEL_MOCK_PASSWORD,required,notEmpty" secret:"true"`
	DSN                      string `env:"-" secret:"true"`
}

// LoadChannelMock 加载渠道配置，并装配 svc_channel_mock 的 DSN。不连接数据库。
func LoadChannelMock(environ []string) (ChannelMock, error) {
	var cfg ChannelMock
	values, err := parse(&cfg, environ)
	if err != nil {
		return ChannelMock{}, err
	}
	applyOTEL(values, &cfg.OTELExporterOTLPEndpoint)
	if err := cfg.validate(); err != nil {
		return ChannelMock{}, err
	}
	cfg.DSN, err = assembleChannelMock(cfg.MySQLHost, cfg.MySQLPort, cfg.MySQLChannelMockPassword)
	if err != nil {
		return ChannelMock{}, err
	}
	return cfg, nil
}

func (cfg ChannelMock) validate() error {
	if err := checkWorkerID(cfg.WorkerID); err != nil {
		return err
	}
	return checkDelays(cfg.MockDelayMinMS, cfg.MockDelayMaxMS)
}
