//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestIntegrationShareConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)
	setup, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := probe.NegotiatedAPIVersion(protocol.APIShareGroupHeartbeat); !ok || v == 0 {
		probe.Close()
		t.Skip("broker does not support KIP-932 ShareGroupHeartbeat (Kafka 4.1+ with share.version=1)")
	}
	probe.Close()

	topic := fmt.Sprintf("gokafka-share-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-share-grp-%d", time.Now().UnixNano())

	adminCfg, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	adminClient, err := gokafka.NewClient(adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := adminClient.Admin().CreateTopic(ctx, topic, 2, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, adminClient.Admin(), topic, 2)
	t.Cleanup(func() {
		_ = adminClient.Admin().DeleteTopics(context.Background(), topic)
		adminClient.Close()
	})

	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithShareGroup(group),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	c, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	prod := c.Producer()
	if err := prod.ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte("share-msg")}); err != nil {
		t.Fatal(err)
	}

	share := c.ShareConsumer([]string{topic})
	recs, err := share.Poll(ctx)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("expected share-acquired records")
	}
	if err := share.Acknowledge(ctx, recs...); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := share.Leave(ctx); err != nil {
		t.Fatalf("leave: %v", err)
	}
}

// TestIntegrationShareRenew exercises KIP-1222 Renew acknowledgements
// (ShareAcknowledge v2). It renews the acquisition lock on in-flight records
// before accepting them. Skips on brokers below ShareAcknowledge v2 (Kafka 4.3+).
func TestIntegrationShareRenew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)
	setup, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	shareAckV, _ := probe.NegotiatedAPIVersion(protocol.APIShareAcknowledge)
	hbV, hbOK := probe.NegotiatedAPIVersion(protocol.APIShareGroupHeartbeat)
	probe.Close()
	if !hbOK || hbV == 0 {
		t.Skip("broker does not support KIP-932 share groups")
	}
	if shareAckV < 2 {
		t.Skipf("broker ShareAcknowledge v%d < 2: KIP-1222 Renew needs Kafka 4.3+", shareAckV)
	}

	topic := fmt.Sprintf("gokafka-renew-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-renew-grp-%d", time.Now().UnixNano())

	cfg, err := gokafka.NewConfig(brokers, gokafka.WithShareGroup(group), gokafka.WithConsumeFromBeginning(true))
	if err != nil {
		t.Fatal(err)
	}
	c, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, c.Admin(), topic, 1)
	t.Cleanup(func() { _ = c.Admin().DeleteTopics(context.Background(), topic) })

	if err := c.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte("renew-msg")}); err != nil {
		t.Fatal(err)
	}

	share := c.ShareConsumer([]string{topic})
	recs, err := share.Poll(ctx)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("expected share-acquired records")
	}
	// Renew the acquisition lock (still processing), then accept. A broker may
	// advertise ShareAcknowledge v2 yet gate Renew behind share.version=2; treat
	// an UNSUPPORTED_VERSION rejection as "feature not enabled" and skip.
	if err := share.Renew(ctx, recs...); err != nil {
		var ke *gokafka.KafkaError
		if gokafka.AsKafkaError(err, &ke) && ke.Code == gokafka.ErrCodeUnsupportedVersion {
			t.Skipf("broker advertises ShareAcknowledge v2 but Renew is not enabled (share.version<2): %v", err)
		}
		t.Fatalf("renew: %v", err)
	}
	if err := share.Acknowledge(ctx, recs...); err != nil {
		t.Fatalf("ack after renew: %v", err)
	}
	if err := share.Leave(ctx); err != nil {
		t.Fatalf("leave: %v", err)
	}
}
