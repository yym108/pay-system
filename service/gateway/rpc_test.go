package gateway

import (
	"reflect"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

func TestRPCTimeoutTable(t *testing.T) {
	cases := []struct {
		method string
		want   time.Duration
	}{
		{method: "Pay", want: 5 * time.Second},
		{method: "ClosePayment", want: 5 * time.Second},
		{method: "CreateRefund", want: 5 * time.Second},
		{method: "CreateTransfer", want: 3 * time.Second},
		{method: "RunBatch", want: 120 * time.Second},
		{method: "CheckBalance", want: 120 * time.Second},
		{method: "GetAccount", want: time.Second},
		{method: "QueryPayment", want: time.Second},
		{method: "HandleChannelNotify", want: time.Second},
		{method: "", want: time.Second},
	}
	for _, tc := range cases {
		if got := RPCTimeout(tc.method); got != tc.want {
			t.Errorf("%s: got %s want %s", tc.method, got, tc.want)
		}
	}
}

func TestClientAddrsAndNoRetry(t *testing.T) {
	if AddrPayment != "payment:8881" || AddrAccount != "account:8882" || AddrRecon != "recon:8883" {
		t.Fatalf("addrs %s %s %s", AddrPayment, AddrAccount, AddrRecon)
	}
	opts := dialOptions(AddrPayment, DefaultRPCTimeout)
	o := client.Options{Configs: rpcinfo.NewRPCConfig()}
	rv := reflect.ValueOf(&o).Elem()
	locks := rv.FieldByName("Locks")
	if locks.IsValid() && locks.CanSet() && locks.Kind() == reflect.Pointer && locks.IsNil() {
		locks.Set(reflect.New(locks.Type().Elem()))
	}
	for _, opt := range opts {
		opt.F(&o, &o.DebugInfo)
	}
	if o.UnaryOptions.RetryMethodPolicies != nil || o.UnaryOptions.RetryContainer != nil {
		t.Fatal("dial options enable retry")
	}
	if o.Configs.ConnectTimeout() != ConnectTimeout || o.Configs.RPCTimeout() != DefaultRPCTimeout {
		t.Fatalf("timeouts connect %s rpc %s", o.Configs.ConnectTimeout(), o.Configs.RPCTimeout())
	}
	if len(o.MetaHandlers) == 0 {
		t.Fatal("missing meta handler")
	}
}

func TestNewClientsWithoutPeer(t *testing.T) {
	clients, err := NewClients()
	if err != nil {
		t.Fatal(err)
	}
	if clients.Payment == nil || clients.Account == nil || clients.Recon == nil {
		t.Fatal("nil client")
	}
}
