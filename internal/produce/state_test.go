package produce_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/produce"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestSequenceReserveRollback(t *testing.T) {
	st := produce.NewState(protocol.ProducerID{ID: 1, Epoch: 0})
	base := st.ReserveBlock("t", 0, 2)
	if base != 0 {
		t.Fatalf("base=%d", base)
	}
	st.RollbackBlock("t", 0, 2)
	if st.ReserveSequence("t", 0) != 0 {
		t.Fatal("expected sequence reset after rollback")
	}
}

func TestSequenceConfirmOnSuccess(t *testing.T) {
	st := produce.NewState(protocol.ProducerID{ID: 1, Epoch: 0})
	_ = st.ReserveBlock("t", 0, 1)
	if st.ReserveSequence("t", 0) != 1 {
		t.Fatal("sequence should advance after successful reserve block")
	}
}
