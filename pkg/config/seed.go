package config

// Seed 是种子程序的配置。DSN 原样保存。本结构体不接入 cmd/seed 的加密和写库流程。
type Seed struct {
	SecretMasterKey     string `env:"SECRET_MASTER_KEY,required,notEmpty" secret:"true"`
	PlatformSubAccounts int    `env:"PLATFORM_SUB_ACCOUNTS" envDefault:"8"`
	AccountDBDSN0       string `env:"ACCOUNT_DB_DSN_0,required,notEmpty" secret:"true"`
	AccountDBDSN1       string `env:"ACCOUNT_DB_DSN_1,required,notEmpty" secret:"true"`
	MySQLDSN            string `env:"MYSQL_DSN,required,notEmpty" secret:"true"`
	DemoAppSecretFormat string `env:"DEMO_APP_SECRET_FORMAT" envDefault:"demo_secret_%s_0123456789ab" secret:"true"`
	SeedLoad            int    `env:"SEED_LOAD" envDefault:"0"`
}

// LoadSeed 加载种子配置。environ 是完整环境，不会再读取进程环境。
func LoadSeed(environ []string) (Seed, error) {
	var cfg Seed
	if _, err := parse(&cfg, environ); err != nil {
		return Seed{}, err
	}
	if err := checkMasterKey(cfg.SecretMasterKey); err != nil {
		return Seed{}, err
	}
	if err := checkMin("PLATFORM_SUB_ACCOUNTS", cfg.PlatformSubAccounts, 1); err != nil {
		return Seed{}, err
	}
	if err := checkSeedLoad(cfg.SeedLoad); err != nil {
		return Seed{}, err
	}
	return cfg, nil
}
