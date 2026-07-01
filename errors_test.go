package gokafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// Locks in the retriable broker-error classification (coordinator/leader
// transients, transaction concurrency, share sessions).
func TestKafkaErrorRetriable(t *testing.T) {
	retriable := []ErrorCode{
		ErrCodeLeaderNotAvail, ErrCodeNotLeaderForPart, ErrCodeRequestTimedOut,
		ErrCodeNetworkException, ErrCodeCoordinatorLoad, ErrCodeCoordinatorNotAvailable,
		ErrCodeNotCoordinator, ErrCodeNotEnoughReplicas, ErrCodeNotEnoughReplicasAfterAppend,
		ErrCodeRebalanceInProg,
		ErrCodeConcurrentTransactions, ErrCodeShareSessionNotFound, ErrCodeInvalidShareSessionEpoch,
	}
	for _, c := range retriable {
		if !(&KafkaError{Code: c}).Retriable() {
			t.Errorf("code %d should be retriable", c)
		}
	}
	// OUT_OF_ORDER_SEQUENCE and INVALID_PRODUCER_EPOCH must NOT be retriable:
	// retrying (with a producer-id reset + resend) duplicates committed records
	// on an idempotent producer. They are surfaced to the caller as fatal.
	if (&KafkaError{Code: ErrCodeOutOfOrderSequence}).Retriable() {
		t.Error("OUT_OF_ORDER_SEQUENCE (45) must not be retriable — resend would duplicate")
	}
	if (&KafkaError{Code: ErrCodeInvalidProducerEpoch}).Retriable() {
		t.Error("INVALID_PRODUCER_EPOCH (47) must not be retriable — abortable/fatal")
	}
	if (&KafkaError{Code: ErrCodeInvalidTxnState}).Retriable() {
		t.Error("INVALID_TXN_STATE (48) must not be retriable")
	}
	if (&KafkaError{Code: ErrCodeUnknownTopic}).Retriable() {
		t.Error("UNKNOWN_TOPIC (3) must not be retriable")
	}
}

// Transport/connection failures (broker dying mid-request) must be retriable so
// the client refreshes metadata and routes to the new leader (audit #5/#8/#10).
func TestIsRetriableTransport(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"eof", io.EOF, true},
		{"unexpected-eof", io.ErrUnexpectedEOF, true},
		{"net-closed", net.ErrClosed, true},
		{"net-op-error", &net.OpError{Op: "read", Err: errors.New("connection reset")}, true},
		{"wrapped-eof", fmt.Errorf("transport: read size: %w", io.EOF), true},
		{"wrapped-kafka-not-leader", fmt.Errorf("fetch: %w", &KafkaError{Code: ErrCodeNotLeaderForPart}), true},
		{"wrapped-kafka-nonretriable", fmt.Errorf("x: %w", &KafkaError{Code: ErrCodeInvalidTxnState}), false},
		{"plain-error", errors.New("nope"), false},
		{"context-canceled", context.Canceled, false},
	}
	for _, c := range cases {
		if got := IsRetriable(c.err); got != c.want {
			t.Errorf("IsRetriable(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// AsKafkaError must unwrap a wrapped *KafkaError (errors.As semantics), so
// IsRetriable/shouldResetProducerID work on errors wrapped with %w.
func TestAsKafkaErrorUnwraps(t *testing.T) {
	base := &KafkaError{Code: ErrCodeNotCoordinator, Msg: "init producer id failed"}
	wrapped := fmt.Errorf("layer1: %w", fmt.Errorf("layer2: %w", base))
	var ke *KafkaError
	if !AsKafkaError(wrapped, &ke) || ke.Code != ErrCodeNotCoordinator {
		t.Fatalf("AsKafkaError should unwrap a doubly-wrapped *KafkaError, got %+v", ke)
	}
}

// Coordinator/leader startup retry must be patient enough to outlast a leader
// election / coordinator load, but cap per-attempt backoff (audit #6/#7).
func TestCoordinatorRetryPatient(t *testing.T) {
	r := coordinatorRetry(RetryConfig{MaxAttempts: 3, Backoff: 100 * time.Millisecond, MaxBackoff: 10 * time.Second})
	if r.MaxAttempts < 25 {
		t.Errorf("coordinatorRetry should raise attempts to >=25, got %d", r.MaxAttempts)
	}
	if r.MaxBackoff > time.Second {
		t.Errorf("coordinatorRetry should cap MaxBackoff at 1s, got %v", r.MaxBackoff)
	}
}
