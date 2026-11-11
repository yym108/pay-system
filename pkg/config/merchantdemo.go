package config

import "strings"

// MerchantDemo 是商户演示进程的配置。
type MerchantDemo struct {
	WorkerID                 int    `env:"WORKER_ID,required,notEmpty"`
	OTELExporterOTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	FailFirstN               int    `env:"FAIL_FIRST_N" envDefault:"0"`
	FaultPoints              string `env:"FAULT_POINTS" envDefault:""`
	// AppSecretFormat 与 seed 共用 DEMO_APP_SECRET_FORMAT。%s 换成通知 body 里的 merchant_id 就是验签密钥。
	AppSecretFormat string `env:"DEMO_APP_SECRET_FORMAT" envDefault:"demo_secret_%s_0123456789ab" secret:"true"`
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
	if err := checkDemoAppSecretFormat(cfg.AppSecretFormat); err != nil {
		return MerchantDemo{}, err
	}
	return cfg, nil
}

// checkDemoAppSecretFormat 只约束 merchant-demo。seed 仍只保存原文字符串。
// 恰好一个 %s，且没有其他 %：避免 %%、%d 或第二个 %s 把密钥算成别的东西。
// 错误文本不含格式原文。
func checkDemoAppSecretFormat(format string) error {
	if strings.Count(format, "%s") != 1 || strings.Count(format, "%") != 1 {
		return invalid("DEMO_APP_SECRET_FORMAT", "must contain exactly one %s and no other %")
	}
	return nil
}
