package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	cfg := gokafka.DefaultConfig([]string{env("KAFKA_BROKERS", "localhost:9092")})
	cfg.ClientID = "gokafka-consumer"
	cfg.ConsumerGroup = env("KAFKA_GROUP", "gokafka-demo-group")

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := env("KAFKA_TOPIC", "gokafka-demo")
	consumer := client.Consumer([]string{topic})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		recs, err := consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Fatal(err)
		}
		if len(recs) == 0 {
			continue
		}
		for _, r := range recs {
			fmt.Printf("partition=%d offset=%d key=%s value=%s\n", r.Partition, r.Offset, r.Key, r.Value)
		}
		if err := consumer.Commit(ctx, recs...); err != nil {
			log.Fatal(err)
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
