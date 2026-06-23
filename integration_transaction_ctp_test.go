//go:build integration

package gokafka_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationTransactionAbort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	txnID := fmt.Sprintf("gokafka-abort-%d", time.Now().UnixNano())
	topic := fmt.Sprintf("gokafka-abort-%d", time.Now().UnixNano())

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

	txn, err := pclient.Producer().BeginTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.ProduceWithinTxn(ctx, gokafka.Record{Topic: topic, Value: []byte("aborted-record")}); err != nil {
		t.Fatal(err)
	}
	if err := txn.Abort(ctx); !errors.Is(err, gokafka.ErrTransactionAborted) {
		t.Fatalf("Abort: %v", err)
	}
	if err := txn.Commit(ctx); !errors.Is(err, gokafka.ErrTransactionAborted) {
		t.Fatalf("Commit after abort: %v", err)
	}
}

func TestIntegrationTransactionSendOffsets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	txnID := fmt.Sprintf("gokafka-ctp-%d", time.Now().UnixNano())
	topic := fmt.Sprintf("gokafka-ctp-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-ctp-grp-%d", time.Now().UnixNano())

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

	plain, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	plainClient, err := gokafka.NewClient(plain)
	if err != nil {
		t.Fatal(err)
	}
	defer plainClient.Close()

	first := []byte("first")
	second := []byte("second")
	if err := plainClient.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: first}); err != nil {
		t.Fatal(err)
	}
	if err := plainClient.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: second}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	ccfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup(group),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	cclient, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}

	consumer := cclient.Consumer([]string{topic})
	var gotFirst bool
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		recs, err := consumer.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if string(r.Value) == string(first) {
				gotFirst = true
				break
			}
		}
		if gotFirst {
			break
		}
	}
	if !gotFirst {
		t.Fatal("first record not consumed")
	}
	if err := consumer.Leave(ctx); err != nil {
		t.Fatal(err)
	}
	cclient.Close()
	time.Sleep(500 * time.Millisecond)

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
	offsets := map[string]map[int32]int64{
		topic: {0: 1},
	}
	if err := txn.SendOffsetsToTxn(ctx, group, offsets); err != nil {
		if strings.Contains(err.Error(), "EOF") {
			t.Skip("SendOffsetsToTxn unavailable on this broker:", err)
		}
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	verifyCfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup(group),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	verifyClient, err := gokafka.NewClient(verifyCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer verifyClient.Close()

	verify := verifyClient.Consumer([]string{topic})
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		recs, err := verify.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if string(r.Value) == string(first) {
				t.Fatal("group offset was not committed via SendOffsetsToTxn")
			}
			if string(r.Value) == string(second) {
				return
			}
		}
	}
	t.Fatal("second record not consumed after SendOffsetsToTxn commit")
}
