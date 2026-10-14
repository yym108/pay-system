//go:build integration

package testkit

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// BASE-11：Kafka 助手返回 broker 地址，端口可以连通。不生产、不消费消息。
func TestKafkaBrokerPortOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	k, err := StartKafka(ctx)
	if err != nil {
		t.Fatal(Redact(err))
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := k.Close(cctx); err != nil {
			t.Errorf("close kafka: %s", Redact(err))
		}
	})
	if k.Broker == "" || strings.ContainsAny(k.Broker, " /") {
		t.Fatalf("broker %q", k.Broker)
	}
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", k.Broker)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}
