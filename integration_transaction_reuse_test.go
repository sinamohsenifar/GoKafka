//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

// TestIntegrationTransactionReuse runs several sequential transactions on a
// single transactional producer. Under KIP-890 TV2 (transaction.version >= 2,
// the default on Kafka 4.x) EndTxn v5 returns the server-bumped producer epoch,
// which the next BeginTransaction reuses without a fresh InitProducerID — so the
// producer id stays constant while the epoch increases monotonically. On a
// pre-TV2 cluster each transaction re-initializes; this test then only asserts
// correctness (all committed records visible), not the epoch progression.
func TestIntegrationTransactionReuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	txnID := fmt.Sprintf("gokafka-txnreuse-%d", time.Now().UnixNano())
	topic := fmt.Sprintf("gokafka-txnreuse-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-txnreuse-grp-%d", time.Now().UnixNano())

	setup, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	sclient, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	if err := sclient.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, sclient.Admin(), topic)
	t.Cleanup(func() {
		_ = sclient.Admin().DeleteTopics(context.Background(), topic)
		sclient.Close()
	})

	pcfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithTransaction(gokafka.TransactionConfig{Enabled: true, TransactionalID: txnID}),
	)
	if err != nil {
		t.Fatal(err)
	}
	pclient, err := gokafka.NewClient(pcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pclient.Close()

	tv2 := false
	if lvl, ok := pclient.BrokerFeature("transaction.version"); ok && lvl >= 2 {
		tv2 = true
	}
	producer := pclient.Producer()

	const txns = 3
	var firstID int64
	var prevEpoch int16 = -1
	for i := 0; i < txns; i++ {
		txn, err := producer.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("txn %d begin: %v", i, err)
		}
		val := []byte(fmt.Sprintf("reuse-%d", i))
		if err := txn.ProduceWithinTxn(ctx, gokafka.Record{Topic: topic, Value: val}); err != nil {
			t.Fatalf("txn %d produce: %v", i, err)
		}
		if err := txn.Commit(ctx); err != nil {
			t.Fatalf("txn %d commit: %v", i, err)
		}
		id, epoch := txn.ProducerID()
		if i == 0 {
			firstID = id
		} else if tv2 {
			if id != firstID {
				t.Fatalf("txn %d: producer id changed %d -> %d (TV2 should reuse)", i, firstID, id)
			}
			if epoch <= prevEpoch {
				t.Fatalf("txn %d: epoch did not advance (%d -> %d); EndTxn v5 epoch adoption broken", i, prevEpoch, epoch)
			}
		}
		prevEpoch = epoch
	}
	if tv2 {
		t.Logf("TV2 reuse verified: producer id %d constant, epoch advanced to %d across %d txns", firstID, prevEpoch, txns)
	}

	// All committed records must be visible to a read_committed consumer.
	ccfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup(group),
		gokafka.WithConsumeFromBeginning(true),
		gokafka.WithConsumer(gokafka.ConsumerConfig{IsolationLevel: gokafka.IsolationReadCommitted}),
	)
	if err != nil {
		t.Fatal(err)
	}
	cclient, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cclient.Close()

	consumer := cclient.Consumer([]string{topic})
	seen := map[string]bool{}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && len(seen) < txns {
		recs, err := consumer.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			seen[string(r.Value)] = true
		}
	}
	for i := 0; i < txns; i++ {
		if !seen[fmt.Sprintf("reuse-%d", i)] {
			t.Fatalf("committed record reuse-%d not consumed (seen=%v)", i, seen)
		}
	}
}
