// Command sharegroup demonstrates a KIP-932 share-group consumer (queue
// semantics): records are acquired, processed, and acknowledged. Requires a
// Kafka 4.1+ broker with share.version enabled.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	topic := env("KAFKA_TOPIC", "gokafka-share-demo")
	group := env("KAFKA_SHARE_GROUP", "gokafka-share-1")

	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithShareGroup(group),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		log.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	share := client.ShareConsumer([]string{topic})
	defer share.Leave(context.Background())

	for {
		records, err := share.Poll(ctx)
		if err != nil {
			log.Fatalf("poll: %v", err)
		}
		for _, r := range records {
			log.Printf("acquired %s[%d]@%d: %s", r.Topic, r.Partition, r.Offset, r.Value)
		}
		if len(records) > 0 {
			// Accept the records so they are not redelivered. Release returns them
			// to the group; Reject drops poison messages; Renew extends the lock.
			if err := share.Acknowledge(ctx, records...); err != nil {
				log.Fatalf("acknowledge: %v", err)
			}
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
