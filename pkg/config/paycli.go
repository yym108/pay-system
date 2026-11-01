package config

// PayCLI 是命令行工具的配置。
// 四个字段都可选：商户命令才要 app_id 和 app_secret，管理命令才要 Token，加载时不因空值失败。
type PayCLI struct {
	GatewayURL string `env:"PAYCLI_GATEWAY_URL"`
	AppID      string `env:"PAYCLI_APP_ID"`
	AppSecret  string `env:"PAYCLI_APP_SECRET" secret:"true"`
	AdminToken string `env:"PAYCLI_ADMIN_TOKEN" secret:"true"`
}

// LoadPayCLI 加载 paycli 配置。未知环境变量被忽略，也不要求 WORKER_ID。
func LoadPayCLI(environ []string) (PayCLI, error) {
	var cfg PayCLI
	if _, err := parse(&cfg, environ); err != nil {
		return PayCLI{}, err
	}
	return cfg, nil
}
