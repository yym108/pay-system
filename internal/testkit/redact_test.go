package testkit

import (
	"errors"
	"strings"
	"testing"
)

func TestRedactHidesDSNAndPassword(t *testing.T) {
	const dsn = "root:super-secret-pass@tcp(127.0.0.1:1)/db"
	rememberText(dsn)
	rememberPassword("super-secret-pass")
	got := Redact(errors.New("dial " + dsn + " failed"))
	if strings.Contains(got, "super-secret-pass") || strings.Contains(got, dsn) {
		t.Fatal("redact left a password or dsn")
	}
}
