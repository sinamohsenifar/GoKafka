package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// TransactionListing summarizes an ongoing transaction from ListTransactions.
type TransactionListing struct {
	TransactionalID string
	ProducerID      int64
	State           string
}

// ListTransactions lists ongoing transactions across all brokers. stateFilters
// (e.g. "Ongoing", "PrepareCommit") and producerIDFilters are optional; empty
// means no filtering.
func (a *Admin) ListTransactions(ctx context.Context, stateFilters []string, producerIDFilters []int64) ([]TransactionListing, error) {
	ver := a.client.cluster.NegotiatedVersion(protocol.APIListTransactions, protocol.VerListTransactions)
	if ver < 0 {
		ver = protocol.VerListTransactions
	}
	body := protocol.EncodeListTransactionsRequest(ver, stateFilters, producerIDFilters)

	var targets []int32
	for _, b := range a.client.cluster.Metadata().Brokers {
		targets = append(targets, b.NodeID)
	}
	seen := map[string]struct{}{}
	var out []TransactionListing
	for _, node := range targets {
		resp, err := a.client.cluster.Request(ctx, node, protocol.APIListTransactions, ver, body)
		if err != nil {
			return nil, err
		}
		code, listings, err := protocol.DecodeListTransactionsResponse(ver, resp)
		if err != nil {
			return nil, err
		}
		if code != 0 {
			return nil, newKafkaError(code, "", 0, "list transactions failed")
		}
		for _, l := range listings {
			if _, dup := seen[l.TransactionalID]; dup {
				continue
			}
			seen[l.TransactionalID] = struct{}{}
			out = append(out, TransactionListing{TransactionalID: l.TransactionalID, ProducerID: l.ProducerID, State: l.State})
		}
	}
	return out, nil
}

// TransactionDescription is the detailed state of a transactional id.
type TransactionDescription struct {
	TransactionalID string
	State           string
	ProducerID      int64
	ProducerEpoch   int16
	TimeoutMs       int32
	StartTimeMs     int64
	Partitions      map[string][]int32 // topics/partitions in the current transaction
}

// DescribeTransactions returns detailed state for each transactional id, routing
// each request to that id's transaction coordinator.
func (a *Admin) DescribeTransactions(ctx context.Context, transactionalIDs ...string) ([]TransactionDescription, error) {
	if len(transactionalIDs) == 0 {
		return nil, nil
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIDescribeTransactions, protocol.VerDescribeTransactions)
	if ver < 0 {
		ver = protocol.VerDescribeTransactions
	}
	// Group ids by their transaction coordinator.
	byCoord := map[int32][]string{}
	for _, id := range transactionalIDs {
		coord, err := a.client.cluster.TransactionCoordinator(ctx, id)
		if err != nil {
			return nil, err
		}
		byCoord[coord] = append(byCoord[coord], id)
	}
	var out []TransactionDescription
	for coord, ids := range byCoord {
		body := protocol.EncodeDescribeTransactionsRequest(ids)
		resp, err := a.client.cluster.Request(ctx, coord, protocol.APIDescribeTransactions, ver, body)
		if err != nil {
			return nil, err
		}
		descs, err := protocol.DecodeDescribeTransactionsResponse(resp)
		if err != nil {
			return nil, err
		}
		for _, d := range descs {
			if d.ErrorCode != 0 {
				return nil, newKafkaError(d.ErrorCode, "", 0, "describe transaction "+d.TransactionalID+" failed")
			}
			out = append(out, TransactionDescription{
				TransactionalID: d.TransactionalID, State: d.State,
				ProducerID: d.ProducerID, ProducerEpoch: d.ProducerEpoch,
				TimeoutMs: d.TimeoutMs, StartTimeMs: d.StartTimeMs, Partitions: d.Topics,
			})
		}
	}
	return out, nil
}
