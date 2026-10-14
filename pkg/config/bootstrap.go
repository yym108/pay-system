package config

// Bootstrap 是建库授权程序的配置。
// MYSQL_HOST 默认 127.0.0.1，供宿主机执行；与 channel-mock 的默认 mysql 不同。
// 本结构体不接入 cmd/bootstrap 的建号流程。
type Bootstrap struct {
	MySQLHost                string `env:"MYSQL_HOST" envDefault:"127.0.0.1"`
	MySQLPort                int    `env:"MYSQL_PORT" envDefault:"3306"`
	MySQLRootPassword        string `env:"MYSQL_ROOT_PASSWORD,required,notEmpty" secret:"true"`
	AccountDBDSN0            string `env:"ACCOUNT_DB_DSN_0,required,notEmpty" secret:"true"`
	AccountDBDSN1            string `env:"ACCOUNT_DB_DSN_1,required,notEmpty" secret:"true"`
	MySQLDSN                 string `env:"MYSQL_DSN,required,notEmpty" secret:"true"`
	MySQLReconPassword       string `env:"MYSQL_RECON_PASSWORD,required,notEmpty" secret:"true"`
	MySQLChannelMockPassword string `env:"MYSQL_CHANNEL_MOCK_PASSWORD,required,notEmpty" secret:"true"`
	MySQLMigratorPassword    string `env:"MYSQL_MIGRATOR_PASSWORD,required,notEmpty" secret:"true"`
}

// LoadBootstrap 加载建库配置。environ 是完整环境，不会再读取进程环境。
func LoadBootstrap(environ []string) (Bootstrap, error) {
	var cfg Bootstrap
	if _, err := parse(&cfg, environ); err != nil {
		return Bootstrap{}, err
	}
	if err := checkHost("MYSQL_HOST", cfg.MySQLHost); err != nil {
		return Bootstrap{}, err
	}
	if err := checkPort("MYSQL_PORT", cfg.MySQLPort); err != nil {
		return Bootstrap{}, err
	}
	return cfg, nil
}
