//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationListDescribeTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	topic := fmt.Sprintf("gokafka-txnadmin-%d", time.Now().UnixNano())
	txnID := fmt.Sprintf("gokafka-txnadmin-id-%d", time.Now().UnixNano())

	setup, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	adminClient, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	admin := adminClient.Admin()
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, admin, topic, 1)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

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

	txn, err := pclient.Producer().BeginTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.ProduceWithinTxn(ctx, gokafka.Record{Topic: topic, Value: []byte("in-flight")}); err != nil {
		t.Fatal(err)
	}
	defer txn.Abort(context.Background())

	// DescribeTransactions should report the ongoing transaction.
	descs, err := admin.DescribeTransactions(ctx, txnID)
	if err != nil {
		t.Fatalf("describe transactions: %v", err)
	}
	if len(descs) != 1 || descs[0].TransactionalID != txnID {
		t.Fatalf("unexpected describe result: %+v", descs)
	}
	if _, ok := descs[0].Partitions[topic]; !ok {
		t.Fatalf("expected topic %s in transaction partitions, got %v", topic, descs[0].Partitions)
	}

	// ListTransactions should include our transactional id.
	listings, err := admin.ListTransactions(ctx, nil, nil)
	if err != nil {
		t.Fatalf("list transactions: %v", err)
	}
	found := false
	for _, l := range listings {
		if l.TransactionalID == txnID {
			found = true
		}
	}
	if !found {
		t.Fatalf("transactional id %s not found in ListTransactions", txnID)
	}
}
