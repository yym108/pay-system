package testkit

import (
	"net/url"
	"sort"
	"strings"
	"sync"
)

// redactor 记住本进程见过的 DSN 和口令，失败信息里换成 [redacted]。
type redactor struct {
	mu        sync.Mutex
	texts     []string
	passwords []string
}

var secrets redactor

func rememberText(s string) {
	if len(s) < 8 {
		return
	}
	secrets.mu.Lock()
	defer secrets.mu.Unlock()
	for _, existing := range secrets.texts {
		if existing == s {
			return
		}
	}
	secrets.texts = append(secrets.texts, s)
}

func rememberPassword(s string) {
	if s == "" {
		return
	}
	secrets.mu.Lock()
	defer secrets.mu.Unlock()
	for _, existing := range secrets.passwords {
		if existing == s {
			return
		}
	}
	secrets.passwords = append(secrets.passwords, s)
}

// Redact 去掉错误文本中的 DSN 和口令。err 为 nil 时返回空字符串。
func Redact(err error) string {
	if err == nil {
		return ""
	}
	return redactString(err.Error())
}

func redactString(msg string) string {
	secrets.mu.Lock()
	texts := append([]string(nil), secrets.texts...)
	passwords := append([]string(nil), secrets.passwords...)
	secrets.mu.Unlock()
	sort.Slice(texts, func(i, j int) bool { return len(texts[i]) > len(texts[j]) })
	for _, s := range texts {
		msg = strings.ReplaceAll(msg, s, "[redacted]")
	}
	for _, p := range passwords {
		msg = strings.ReplaceAll(msg, ":"+p+"@", ":[redacted]@")
		if esc := url.QueryEscape(p); esc != p {
			msg = strings.ReplaceAll(msg, ":"+esc+"@", ":[redacted]@")
		}
		if len(p) >= 8 {
			msg = strings.ReplaceAll(msg, p, "[redacted]")
		}
	}
	return msg
}
