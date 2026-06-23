package main

import (
	"context"
	"log"
	"os"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	cfg := gokafka.DefaultConfig([]string{env("KAFKA_BROKERS", "localhost:9092")})
	cfg.ClientID = "gokafka-producer"

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := env("KAFKA_TOPIC", "gokafka-demo")
	results, err := client.Producer().ProduceSyncResult(context.Background(), gokafka.Record{
		Topic: topic,
		Key:   []byte("key-1"),
		Value: []byte(`{"hello":"gokafka"}`),
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(results) > 0 {
		log.Printf("produced to %s partition=%d offset=%d", results[0].Topic, results[0].Partition, results[0].Offset)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
