package config

// PayCLI 是命令行工具的配置。约定没有专属环境变量，因此结构体为空。
// 不读取 WORKER_ID，也不定义 TL-01 的命令行参数。
type PayCLI struct{}

// LoadPayCLI 加载空配置。未知环境变量被忽略。
func LoadPayCLI(environ []string) (PayCLI, error) {
	var cfg PayCLI
	if _, err := parse(&cfg, environ); err != nil {
		return PayCLI{}, err
	}
	return cfg, nil
}
