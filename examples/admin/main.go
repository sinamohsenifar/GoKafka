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
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	admin := client.Admin()
	topic := env("KAFKA_TOPIC", "gokafka-demo")

	if err := admin.CreateTopic(ctx, topic, 3, 1); err != nil {
		log.Printf("create topic: %v (may already exist)", err)
	}

	topics, err := admin.ListTopics(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("topics (%d):", len(topics))
	for _, t := range topics {
		log.Printf("  - %s", t)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
