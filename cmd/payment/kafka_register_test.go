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

// MQ-03 验收 2：payment 的 serve 使用与 account 相同的 mq.RegisterProducer。
func TestServeUsesMQRegisterProducer(t *testing.T) {
	if mq.ErrNoBroker == nil {
		t.Fatal("register sentinel missing")
	}
	assertServeCalls(t, "main.go")
}

func assertServeCalls(t *testing.T, path string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
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

func TestServeStartsPaymentNotify(t *testing.T) {
	body, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(body)
	if !strings.Contains(src, "payment-notify") || !strings.Contains(src, "notify.Open") {
		t.Fatal("serve does not start payment-notify")
	}
	if strings.Contains(src, "account-ledger") || strings.Contains(src, "service/account/consumer") {
		t.Fatal("payment serve starts the account ledger group")
	}
}
