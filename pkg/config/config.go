// Package config 从环境变量加载各程序的配置。
//
// 环境只来自调用方传入的列表，不合并进程环境。启动校验失败时，错误文本包含变量名。
// 带 secret:"true" 的字段在生效配置里输出为 ***，不扫描卡号或手机号。
package config

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/caarlos0/env/v11"
)

const (
	// 空字符串表示关闭 trace。未设置时才用这个默认值；显式空串不能被默认值盖掉。
	defaultOTELEndpoint = "jaeger:4317"
	otelEnv             = "OTEL_EXPORTER_OTLP_ENDPOINT"

	defaultQueryBackoff = "10s,10s,30s,1m,2m,5m,10m,30m"
	queryBackoffEnv     = "QUERY_BACKOFF"

	// 分片数只能是 2。改动会改变商户路由（D17）。
	allowedShardCount = 2
	// 主密钥解码后必须是 32 字节，供 AES-256 使用（BASE-14）。
	masterKeyLen = 32
	workerIDMax  = 1023
)

// WriteResult 把加载结果写成进程入口的退出码。
// 失败时把错误写到 stderr 并返回 1。成功时把脱敏后的生效配置写到 stdout 并返回 0。
func WriteResult(stdout, stderr io.Writer, cfg any, err error) int {
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	if _, werr := fmt.Fprintln(stdout, Redact(cfg)); werr != nil {
		_, _ = fmt.Fprintf(stderr, "write config: %v\n", werr)
		return 1
	}
	return 0
}

// Redact 按结构体字段顺序输出生效配置。secret:"true" 的字段固定为 ***。
func Redact(cfg any) string {
	if cfg == nil {
		return ""
	}
	v := reflect.ValueOf(cfg)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	t := v.Type()
	var b strings.Builder
	first := true
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		if !first {
			b.WriteByte('\n')
		}
		first = false
		fmt.Fprintf(&b, "%s=%s", fieldLabel(sf), redactField(sf, v.Field(i)))
	}
	return b.String()
}

func redactField(sf reflect.StructField, v reflect.Value) string {
	if sf.Tag.Get("secret") == "true" {
		return "***"
	}
	return formatPlain(v)
}

func formatPlain(v reflect.Value) string {
	if !v.IsValid() {
		return ""
	}
	if v.Kind() == reflect.String {
		return v.String()
	}
	if v.CanInterface() {
		if s, ok := v.Interface().(fmt.Stringer); ok {
			return s.String()
		}
		return fmt.Sprint(v.Interface())
	}
	return ""
}

func fieldLabel(sf reflect.StructField) string {
	name, _, _ := strings.Cut(sf.Tag.Get("env"), ",")
	if name != "" && name != "-" {
		return name
	}
	return sf.Name
}

func parse(dst any, environ []string) (map[string]string, error) {
	values := env.ToMap(environ)
	if values == nil {
		values = map[string]string{}
	}
	// Environment 非 nil 时库不会回退到 os.Environ。
	if err := env.ParseWithOptions(dst, env.Options{Environment: values}); err != nil {
		return nil, annotate(dst, err)
	}
	return values, nil
}

func annotate(dst any, err error) error {
	var agg env.AggregateError
	if !errors.As(err, &agg) {
		return err
	}
	names := fieldEnvNames(reflect.TypeOf(dst))
	parts := make([]string, 0, len(agg.Errors))
	for _, item := range agg.Errors {
		var pe env.ParseError
		if errors.As(item, &pe) {
			key := names[pe.Name]
			if key == "" {
				key = pe.Name
			}
			parts = append(parts, fmt.Sprintf("invalid %s: %s", key, pe.Error()))
			continue
		}
		parts = append(parts, item.Error())
	}
	return fmt.Errorf("env: %s", strings.Join(parts, "; "))
}

func fieldEnvNames(t reflect.Type) map[string]string {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	names := make(map[string]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("env"), ",")
		if name == "" || name == "-" {
			continue
		}
		names[f.Name] = name
	}
	return names
}

func invalid(name, reason string) error {
	return fmt.Errorf("invalid %s: %s", name, reason)
}

func applyOTEL(values map[string]string, dst *string) {
	if _, ok := values[otelEnv]; !ok {
		*dst = defaultOTELEndpoint
	}
}

func applyQueryBackoff(values map[string]string, dst *DurationList) error {
	if _, ok := values[queryBackoffEnv]; ok {
		if len(*dst) == 0 {
			return invalid(queryBackoffEnv, "at least one duration is required")
		}
		return nil
	}
	if err := dst.UnmarshalText([]byte(defaultQueryBackoff)); err != nil {
		return invalid(queryBackoffEnv, "default is invalid")
	}
	return nil
}

func checkWorkerID(id int) error {
	if id < 0 || id > workerIDMax {
		return invalid("WORKER_ID", "must be between 0 and 1023")
	}
	return nil
}

func checkShardCount(n int) error {
	if n != allowedShardCount {
		return invalid("ACCOUNT_SHARD_COUNT", "only 2 is allowed")
	}
	return nil
}

func checkMasterKey(key string) error {
	raw, err := decodeMasterKey(key)
	if err != nil || len(raw) != masterKeyLen {
		return invalid("SECRET_MASTER_KEY", "must be standard base64 decoding to 32 bytes")
	}
	return nil
}

func checkMin(name string, n, min int) error {
	if n < min {
		return invalid(name, fmt.Sprintf("must be >= %d", min))
	}
	return nil
}
