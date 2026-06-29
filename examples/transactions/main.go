// Command transactions demonstrates exactly-once produce (EOS) with GoKafka's
// transactional producer: begin, produce, commit (or abort on error).
package main

import (
	"context"
	"log"
	"os"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	topic := env("KAFKA_TOPIC", "gokafka-txn-demo")

	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-txn-example"),
		// A transactional id is required; it fences zombie producers.
		gokafka.WithTransaction(gokafka.TransactionConfig{Enabled: true, TransactionalID: "gokafka-txn-1"}),
	)
	if err != nil {
		log.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	txn, err := client.Producer().BeginTransaction(ctx)
	if err != nil {
		log.Fatal(err)
	}

	err = txn.ProduceWithinTxn(ctx,
		gokafka.Record{Topic: topic, Key: []byte("a"), Value: []byte(`{"n":1}`)},
		gokafka.Record{Topic: topic, Key: []byte("b"), Value: []byte(`{"n":2}`)},
	)
	if err != nil {
		// On a produce error, roll the whole transaction back — nothing is visible
		// to read_committed consumers.
		_ = txn.Abort(ctx)
		log.Fatalf("produce within txn: %v", err)
	}

	if err := txn.Commit(ctx); err != nil {
		log.Fatalf("commit: %v", err)
	}
	log.Printf("transaction committed: both records are now visible to read_committed consumers")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
