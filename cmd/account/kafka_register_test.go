package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/yym108/pay-system/pkg/mq"
)

// MQ-03 验收 2：account 的 serve 使用与 payment 相同的 mq.RegisterProducer。
func TestServeUsesMQRegisterProducer(t *testing.T) {
	if mq.ErrNoBroker == nil {
		t.Fatal("register sentinel missing")
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "serve" || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if ok && pkg.Name == "mq" && sel.Sel.Name == "RegisterProducer" {
				found = true
			}
			return true
		})
	}
	if !found {
		t.Fatal("serve does not call mq.RegisterProducer")
	}
}

// MQ-04：account 的 serve 在 RegisterProducer 之后启动 account-ledger。
func TestServeStartsAccountLedger(t *testing.T) {
	body, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(body)
	if !strings.Contains(src, "consumer.Open") || !strings.Contains(src, "RegisterGroup") {
		t.Fatal("serve does not start the ledger consumer")
	}
	if strings.Contains(src, "BeforeCommit") || strings.Contains(src, "LEDGER_BATCH_ENABLED") {
		t.Fatal("serve sets a test hook or the batch switch")
	}
}

// MQ-04：payment 不启动 account-ledger。
func TestPaymentServeDoesNotStartLedger(t *testing.T) {
	body, err := os.ReadFile("../payment/main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(body)
	if strings.Contains(src, "account-ledger") || strings.Contains(src, "service/account/consumer") || strings.Contains(src, "consumer.Open") {
		t.Fatal("payment serve starts the account ledger group")
	}
}
