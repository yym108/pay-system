package config

import "time"

// Gateway 是网关进程的配置。
type Gateway struct {
	WorkerID                 int                `env:"WORKER_ID,required,notEmpty"`
	RedisAddr                string             `env:"REDIS_ADDR" envDefault:"redis:6379"`
	OTELExporterOTLPEndpoint string             `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	CredentialCacheTTL       time.Duration      `env:"GW_CREDENTIAL_CACHE_TTL" envDefault:"60s"`
	AdminToken               string             `env:"ADMIN_TOKEN,required,notEmpty" secret:"true"`
	RateLimitDefault         RateLimit          `env:"RATE_LIMIT_DEFAULT" envDefault:"100:200"`
	RateLimitOverrides       RateLimitOverrides `env:"RATE_LIMIT_OVERRIDES" envDefault:""`
	ChannelMockpayKey        string             `env:"CHANNEL_MOCKPAY_KEY,required,notEmpty" secret:"true"`
	FaultPoints              string             `env:"FAULT_POINTS" envDefault:""`
}

// LoadGateway 加载网关配置。environ 是完整环境，不会再读取进程环境。
func LoadGateway(environ []string) (Gateway, error) {
	var cfg Gateway
	values, err := parse(&cfg, environ)
	if err != nil {
		return Gateway{}, err
	}
	applyOTEL(values, &cfg.OTELExporterOTLPEndpoint)
	if err := cfg.validate(); err != nil {
		return Gateway{}, err
	}
	return cfg, nil
}

func (cfg Gateway) validate() error {
	if err := checkWorkerID(cfg.WorkerID); err != nil {
		return err
	}
	if err := checkNonNegative("GW_CREDENTIAL_CACHE_TTL", cfg.CredentialCacheTTL); err != nil {
		return err
	}
	if cfg.RateLimitDefault.Rate() <= 0 || cfg.RateLimitDefault.Burst() <= 0 {
		return invalid("RATE_LIMIT_DEFAULT", "rate and burst must be positive integers")
	}
	return checkAdminToken(cfg.AdminToken)
}
