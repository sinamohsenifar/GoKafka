package protocol

import (
	"errors"
	"fmt"
)

var ErrUnknownTopic = errors.New("protocol: unknown topic or partition")

// ErrRebalanceInProgress indicates the consumer group is rebalancing.
var ErrRebalanceInProgress = errors.New("protocol: rebalance in progress")

// ErrMemberIDRequired is returned when the broker assigns a member id (KIP-394); retry JoinGroup with that id.
var ErrMemberIDRequired = errors.New("protocol: member id required")

// ErrorCodeMemberIDRequired is Kafka error code 79 (MEMBER_ID_REQUIRED).
const ErrorCodeMemberIDRequired int16 = 79

const (
	ErrorCodeCoordinatorLoadInProgress int16 = 14
	ErrorCodeCoordinatorNotAvailable   int16 = 15
)

// CoordinatorRetriable reports whether FindCoordinator should be retried.
func CoordinatorRetriable(code int16) bool {
	return code == ErrorCodeCoordinatorLoadInProgress || code == ErrorCodeCoordinatorNotAvailable
}

// APIError is a non-zero Kafka error code from a protocol response.
type APIError struct {
	Op   string
	Code int16
}

func (e *APIError) Error() string {
	return fmt.Sprintf("protocol: %s error %d", e.Op, e.Code)
}

func apiError(op string, code int16) error {
	return &APIError{Op: op, Code: code}
}
