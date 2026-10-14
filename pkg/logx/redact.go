package logx

import (
	"log/slog"
	"strings"
)

// fullyRedacted 是整段替换的占位。短于保留长度时也用它，避免前后重叠把原文露出来。
const fullyRedacted = "***"

type fieldClass int

const (
	classNone fieldClass = iota
	classFull
	classCard
	classPhone
	classID
)

// 先整段替换，再卡号、手机号、证件号。名字包含片段即匹配，比较前已转成小写。
var (
	fullFragments  = []string{"secret", "password", "token", "signature", "master_key"}
	cardFragments  = []string{"card_no", "bank_card"}
	phoneFragments = []string{"phone", "mobile"}
	idFragments    = []string{"id_card"}
)

func classOf(key string) fieldClass {
	folded := strings.ToLower(key)
	switch {
	case containsAny(folded, fullFragments):
		return classFull
	case containsAny(folded, cardFragments):
		return classCard
	case containsAny(folded, phoneFragments):
		return classPhone
	case containsAny(folded, idFragments):
		return classID
	default:
		return classNone
	}
}

func containsAny(s string, parts []string) bool {
	for _, part := range parts {
		if strings.Contains(s, part) {
			return true
		}
	}
	return false
}

// redactAttr 按字段名脱敏，再对字符串和 error 做兜底扫描。
// 整段替换在 Resolve 之前返回：slog 的 ReplaceAttr 不会看到已展开的分组键，
// 敏感键如果先变成 group，子字段就会漏出去。
func redactAttr(a slog.Attr) slog.Attr {
	class := classOf(a.Key)
	if class == classFull {
		return slog.String(a.Key, fullyRedacted)
	}
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		if class != classNone {
			return slog.String(a.Key, fullyRedacted)
		}
		members := v.Group()
		redacted := make([]slog.Attr, len(members))
		for i, member := range members {
			redacted[i] = redactAttr(member)
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(redacted...)}
	}
	switch class {
	case classCard:
		return slog.String(a.Key, maskKeep(v.String(), 6, 4))
	case classPhone:
		return slog.String(a.Key, maskKeep(v.String(), 3, 4))
	case classID:
		return slog.String(a.Key, maskKeep(v.String(), 1, 1))
	}
	if text, ok := textForScan(v); ok {
		return slog.String(a.Key, scanDigits(text))
	}
	return slog.Attr{Key: a.Key, Value: v}
}

func textForScan(v slog.Value) (string, bool) {
	switch v.Kind() {
	case slog.KindString:
		return v.String(), true
	case slog.KindAny:
		err, ok := v.Any().(error)
		if !ok {
			return "", false
		}
		if err == nil {
			return "", true
		}
		return err.Error(), true
	default:
		return "", false
	}
}

// maskKeep 按码点保留首尾。长度不够时整段换成 ***。
func maskKeep(s string, head, tail int) string {
	rs := []rune(s)
	if len(rs) < head+tail {
		return fullyRedacted
	}
	stars := len(rs) - head - tail
	var b strings.Builder
	b.Grow(len(s))
	b.WriteString(string(rs[:head]))
	for range stars {
		b.WriteByte('*')
	}
	b.WriteString(string(rs[len(rs)-tail:]))
	return b.String()
}

// scanDigits 只认 ASCII 连续数字。11 位且以 1 开头是手机号，不是卡号。
// 短于 11 位、12 位、以及长于 19 位的连续数字保持原样。
func scanDigits(s string) string {
	var b strings.Builder
	changed := false
	for i := 0; i < len(s); {
		if s[i] < '0' || s[i] > '9' {
			if changed {
				b.WriteByte(s[i])
			}
			i++
			continue
		}
		j := i + 1
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		run := s[i:j]
		masked, ok := maskDigitRun(run)
		if !ok {
			if changed {
				b.WriteString(run)
			}
			i = j
			continue
		}
		if !changed {
			b.Grow(len(s))
			b.WriteString(s[:i])
			changed = true
		}
		b.WriteString(masked)
		i = j
	}
	if !changed {
		return s
	}
	return b.String()
}

func maskDigitRun(run string) (string, bool) {
	n := len(run)
	switch {
	case n >= 13 && n <= 19:
		return maskKeep(run, 6, 4), true
	case n == 11 && run[0] == '1':
		return maskKeep(run, 3, 4), true
	default:
		return "", false
	}
}
