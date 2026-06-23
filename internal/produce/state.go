package produce

import (
	"fmt"
	"sync"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// State tracks idempotent producer sequence numbers per partition.
type State struct {
	mu        sync.Mutex
	pid       protocol.ProducerID
	sequences map[string]int32
}

func NewState(pid protocol.ProducerID) *State {
	return &State{pid: pid, sequences: map[string]int32{}}
}

func topicKey(topic string, part int32) string {
	return fmt.Sprintf("%s:%d", topic, part)
}

// ReserveSequence returns the next sequence to send without advancing (safe for retries).
func (s *State) ReserveSequence(topic string, part int32) int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sequences[topicKey(topic, part)]
}

// ConfirmSequence advances the sequence after broker ack (zero duplicate on retry).
func (s *State) ConfirmSequence(topic string, part int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicKey(topic, part)
	s.sequences[key]++
}

// ReserveBlock reserves consecutive sequences for a multi-record batch on one partition.
func (s *State) ReserveBlock(topic string, part int32, count int) (base int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicKey(topic, part)
	base = s.sequences[key]
	s.sequences[key] += int32(count)
	return base
}

// RollbackBlock undoes ReserveBlock when the send fails before acknowledgement.
func (s *State) RollbackBlock(topic string, part int32, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicKey(topic, part)
	s.sequences[key] -= int32(count)
	if s.sequences[key] < 0 {
		s.sequences[key] = 0
	}
}

// NextSequence advances immediately (legacy); prefer Reserve/Confirm for idempotent sends.
func (s *State) NextSequence(topic string, part int32) int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := topicKey(topic, part)
	seq := s.sequences[key]
	s.sequences[key]++
	return seq
}

func (s *State) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequences = map[string]int32{}
}
