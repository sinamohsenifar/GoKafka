package kfake_test

import (
	"context"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/kfake"
)

// TestRebootstrapAfterBrokerReplacement verifies KIP-1102-style resilience: when
// the broker the client knows about is replaced by a new one at the same
// bootstrap address (a full cluster rotation behind a stable endpoint), the
// client re-dials its bootstrap address and recovers, rather than failing
// permanently. GoKafka always refreshes metadata via the bootstrap seeds (which
// re-resolve DNS on failure), so it never "forgets" the bootstrap servers.
func TestRebootstrapAfterBrokerReplacement(t *testing.T) {
	b1, err := kfake.NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	addr := b1.Addr()
	b1.AddTopic("before", 1)

	cfg, err := gokafka.NewConfig([]string{addr})
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	topics, err := cli.Admin().ListTopics(ctx)
	if err != nil || !contains(topics, "before") {
		t.Fatalf("initial ListTopics: topics=%v err=%v", topics, err)
	}

	// Kill the broker the client is connected to, then bring up a fresh broker at
	// the SAME bootstrap address. A just-closed loopback port can briefly linger,
	// so retry the bind.
	b1.Close()
	var b2 *kfake.Broker
	for i := 0; i < 60; i++ {
		b2, err = kfake.NewBrokerAt(addr)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("rebind %s: %v", addr, err)
	}
	defer b2.Close()
	b2.AddTopic("after", 1)

	// The client must re-dial its bootstrap address and recover — now seeing b2's
	// distinct topic, proving it reconnected to the replacement (not stale cache).
	var got []string
	for i := 0; i < 60; i++ {
		got, err = cli.Admin().ListTopics(ctx)
		if err == nil && contains(got, "after") {
			return // rebootstrapped successfully
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("client did not rebootstrap to the replacement broker: topics=%v err=%v", got, err)
}
