package gokafka

import (
	"context"
	"testing"

	"github.com/sinamohsenifar/gokafka/kfake"
)

// An idempotent producer must advance its per-partition sequence by the NUMBER
// OF RECORDS in each batch, not by one per batch. A batch of N records claims
// wire sequences base..base+N-1, so the broker expects the next batch to start
// at base+N. Reserving only 1 leaves the client N-1 sequences behind, and every
// later batch on that partition is rejected OUT_OF_ORDER_SEQUENCE (and, before
// the fix, re-sent under a fresh producer id — silent duplication). This is a
// white-box regression test for the produce-path sequence reservation.
func TestProducerIdempotentSequenceAdvancesByRecordCount(t *testing.T) {
	broker, err := kfake.NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	broker.AddTopic("seqtest", 1)

	cfg, err := NewConfig([]string{broker.Addr()},
		WithProducer(ProducerConfig{Idempotent: true, Acks: AcksAll}))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx := context.Background()
	prod := cli.Producer()

	mk := func(n int) []Record {
		recs := make([]Record, n)
		for i := range recs {
			recs[i] = Record{Topic: "seqtest", Value: []byte("v")}
		}
		return recs
	}

	if err := prod.ProduceSync(ctx, mk(3)...); err != nil {
		t.Fatalf("first produce (3 records): %v", err)
	}
	if prod.idState == nil {
		t.Fatal("idempotent producer should have initialized idState after produce")
	}
	if got := prod.idState.ReserveSequence("seqtest", 0); got != 3 {
		t.Fatalf("after 3 records the next sequence = %d, want 3 (reserved 1 per batch instead of per record?)", got)
	}
	if err := prod.ProduceSync(ctx, mk(2)...); err != nil {
		t.Fatalf("second produce (2 records): %v", err)
	}
	if got := prod.idState.ReserveSequence("seqtest", 0); got != 5 {
		t.Fatalf("after 3+2 records the next sequence = %d, want 5", got)
	}
}
