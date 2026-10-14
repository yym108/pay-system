package base

import (
	"os"
	"regexp"
	"testing"
)

// BASE-12 验收 1：接口 §10 三张表的方法都在对应 IDL 的 service 里，且没有 Post。
func TestBASE12IDLMethods(t *testing.T) {
	t.Parallel()
	cases := []struct {
		file    string
		service string
		methods []string
	}{
		{
			file:    "idl/account.thrift",
			service: "AccountService",
			methods: []string{
				"OpenAccount", "GetAccount", "Freeze", "ReleaseFreeze", "ConfirmFreeze",
				"TransferLocal", "TransferInTry", "TransferInConfirm", "TransferInCancel",
				"CheckBalance", "Reverse",
			},
		},
		{
			file:    "idl/payment.thrift",
			service: "PaymentService",
			methods: []string{
				"CreatePayment", "Pay", "ClosePayment", "QueryPayment",
				"CreateRefund", "QueryRefund", "CreateTransfer", "QueryTransfer",
				"HandleChannelNotify", "GetMerchantCredential", "SyncChannelTxn", "ReplayEvent",
			},
		},
		{
			file:    "idl/recon.thrift",
			service: "ReconService",
			methods: []string{"RunBatch"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.service, func(t *testing.T) {
			t.Parallel()
			got := serviceMethods(t, tc.file, tc.service)
			assertMethods(t, tc.service, got, tc.methods)
		})
	}
}

func serviceMethods(t *testing.T, path, service string) map[string]struct{} {
	t.Helper()
	content, err := os.ReadFile(path) //nolint:gosec // path is a fixed IDL path from the test table.
	if err != nil {
		t.Fatalf("读取 %s：%v", path, err)
	}
	re := regexp.MustCompile(`(?s)service\s+` + service + `\s*\{(.*?)\n\}`)
	match := re.FindSubmatch(content)
	if match == nil {
		t.Fatalf("%s 中没有 service %s", path, service)
	}
	methodRe := regexp.MustCompile(`(?m)^[ \t]*(?:oneway[ \t]+)?(?:[\w.]+[ \t]+)+(\w+)[ \t]*\(`)
	found := make(map[string]struct{})
	for _, name := range methodRe.FindAllSubmatch(match[1], -1) {
		found[string(name[1])] = struct{}{}
	}
	return found
}

func assertMethods(t *testing.T, service string, got map[string]struct{}, want []string) {
	t.Helper()
	wantSet := make(map[string]struct{}, len(want))
	for _, name := range want {
		wantSet[name] = struct{}{}
		if _, ok := got[name]; !ok {
			t.Errorf("%s 缺少方法 %s", service, name)
		}
	}
	for name := range got {
		if name == "Post" {
			t.Errorf("%s 不应包含 Post 方法", service)
		}
		if _, ok := wantSet[name]; !ok {
			t.Errorf("%s 有文档未列出的方法 %s", service, name)
		}
	}
}
