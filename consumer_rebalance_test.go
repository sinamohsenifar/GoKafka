package gokafka

import (
	"context"
	"testing"
)

func TestRebalanceRequiresConsumerGroup(t *testing.T) {
	cfg, err := NewConfig([]string{"localhost:9092"})
	if err != nil {
		t.Fatal(err)
	}
	c := &Consumer{client: &Client{cfg: cfg}}
	if err := c.Rebalance(context.Background()); err != ErrNoConsumerGroup {
		t.Fatalf("got %v want %v", err, ErrNoConsumerGroup)
	}
}
