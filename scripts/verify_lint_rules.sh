#!/bin/sh

set -eu

time_violation="service/payment/biz/base01_lint_violation.go"
cross_violation="service/payment/handler/base01_cross_service.go"
same_service_file="service/account/handler/base01_same_service.go"
pkg_violation="pkg/base01_verify/base01_service_import.go"
account_dependency="service/account/base01_verify/base01_dependency.go"
payment_dependency="service/payment/base01_verify/base01_dependency.go"
internal_violation="pkg/base03_verify/internal_import.go"
output="$(mktemp)"

cleanup() {
	rm -f "$time_violation" "$cross_violation" "$same_service_file" "$pkg_violation" "$account_dependency" "$payment_dependency" "$internal_violation" "$output"
	rmdir pkg/base01_verify pkg/base03_verify service/account/base01_verify service/payment/base01_verify 2>/dev/null || true
	rmdir pkg service/account/handler service/account service/payment/handler service/payment/biz service/payment 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

mkdir -p service/payment/biz
cat > "$time_violation" <<'EOF'
package biz

import "time"

var _ = time.Now()
EOF

if golangci-lint run ./service/payment/biz >"$output" 2>&1; then
	echo "expected golangci-lint to reject time.Now()" >&2
	cat "$output" >&2
	exit 1
fi

if ! grep -q "time.Now" "$output"; then
	echo "golangci-lint failed without reporting the time.Now() violation" >&2
cat "$output" >&2
exit 1
fi

echo "golangci-lint rejected time.Now() in service/payment/biz"

mkdir -p service/account/base01_verify service/account/handler service/payment/base01_verify service/payment/handler pkg/base01_verify
cat > "$account_dependency" <<'EOF'
package base01_verify

const Marker = "account"
EOF
cat > "$payment_dependency" <<'EOF'
package base01_verify

const Marker = "payment"
EOF
cat > "$same_service_file" <<'EOF'
package handler

import accountverify "github.com/yym108/pay-system/service/account/base01_verify"

var _ = accountverify.Marker
EOF
cat > "$cross_violation" <<'EOF'
package handler

import accountverify "github.com/yym108/pay-system/service/account/base01_verify"

var _ = accountverify.Marker
EOF
cat > "$pkg_violation" <<'EOF'
package base01_verify

import accountverify "github.com/yym108/pay-system/service/account/base01_verify"

var _ = accountverify.Marker
EOF

if ! golangci-lint run ./service/account/handler >"$output" 2>&1; then
	echo "same-service import was incorrectly rejected" >&2
	cat "$output" >&2
	exit 1
fi
echo "golangci-lint allowed service/account to import service/account"

if golangci-lint run ./service/payment/handler >"$output" 2>&1; then
	echo "expected cross-service import to be rejected" >&2
	cat "$output" >&2
	exit 1
fi
if ! grep -q "service/account/base01_verify" "$output"; then
	echo "cross-service rejection did not identify the imported account package" >&2
	cat "$output" >&2
	exit 1
fi
echo "golangci-lint rejected cross-service import"

if golangci-lint run ./pkg/base01_verify >"$output" 2>&1; then
	echo "expected pkg import of service to be rejected" >&2
	cat "$output" >&2
	exit 1
fi
if ! grep -q "service/account/base01_verify" "$output"; then
	echo "pkg service import rejection did not identify the imported package" >&2
	cat "$output" >&2
	exit 1
fi
echo "golangci-lint rejected pkg import of service"

mkdir -p pkg/base03_verify
cat > "$internal_violation" <<'EOF'
package base03_verify

import seed "github.com/yym108/pay-system/internal/seed"

var _ = seed.CipherVersion
EOF

if golangci-lint run ./pkg/base03_verify >"$output" 2>&1; then
	echo "expected pkg import of internal to be rejected" >&2
	cat "$output" >&2
	exit 1
fi
if ! grep -q "internal/seed" "$output"; then
	echo "internal import rejection did not identify internal/seed" >&2
	cat "$output" >&2
	exit 1
fi
echo "golangci-lint rejected pkg import of internal"
