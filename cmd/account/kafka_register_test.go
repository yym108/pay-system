package main

import (
	"go/ast"
	"go/parser"
	"go/token"
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
