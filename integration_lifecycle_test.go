//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func goroutineBaseline() int {
	runtime.GC()
	time.Sleep(250 * time.Millisecond)
	runtime.GC()
	return runtime.NumGoroutine()
}

// TestIntegrationClientCloseNoLeak connects and closes many clients and asserts
// no goroutine/connection leak, and that Close is idempotent (audit #1/#10).
func TestIntegrationClientCloseNoLeak(t *testing.T) {
	brokers := integrationBrokers(t)
	cfg, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}

	warm, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	warm.Close()
	warm.Close() // idempotent: must not panic

	base := goroutineBaseline()
	for i := 0; i < 30; i++ {
		c, err := gokafka.NewClient(cfg)
		if err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
		_, _ = c.Admin().ListTopics(context.Background()) // open a real broker connection
		c.Close()
	}
	after := goroutineBaseline()
	if after > base+5 {
		t.Fatalf("goroutine leak after 30 client connect/close cycles: base=%d after=%d", base, after)
	}
}

// TestIntegrationConsumerLeaveNoLeak joins/leaves a group repeatedly and asserts
// the background heartbeat goroutines are cleaned up (no leak).
func TestIntegrationConsumerLeaveNoLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := integrationBrokers(t)

	topic := fmt.Sprintf("gokafka-leak-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-leak-grp-%d", time.Now().UnixNano())
	acfg, _ := gokafka.NewConfig(brokers)
	adm, err := gokafka.NewClient(acfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adm.Close()
	if err := adm.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, adm.Admin(), topic, 1)
	t.Cleanup(func() { _ = adm.Admin().DeleteTopics(context.Background(), topic) })

	base := goroutineBaseline()
	for i := 0; i < 8; i++ {
		cfg, _ := gokafka.NewConfig(brokers,
			gokafka.WithConsumerGroup(group),
			gokafka.WithConsumeFromBeginning(true),
		)
		cli, err := gokafka.NewClient(cfg)
		if err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
		cons := cli.Consumer([]string{topic})
		if _, err := cons.Poll(ctx); err != nil { // join + start heartbeat
			t.Fatalf("poll %d: %v", i, err)
		}
		if err := cons.Leave(ctx); err != nil { // stop heartbeat
			t.Fatalf("leave %d: %v", i, err)
		}
		cli.Close()
	}
	after := goroutineBaseline()
	if after > base+5 {
		t.Fatalf("goroutine leak after 8 join/leave cycles: base=%d after=%d", base, after)
	}
}
