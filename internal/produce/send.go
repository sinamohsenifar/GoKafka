package produce

import (
	"context"
	"fmt"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/broker"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// Send dispatches records to partition leaders.
func Send(ctx context.Context, cl *broker.Cluster, records []protocol.ProduceRecord, settings protocol.ProduceSettings, st *State) ([]protocol.ProduceResult, error) {
	byBroker := map[int32][]protocol.ProduceRecord{}
	meta := cl.Metadata()
	for _, r := range records {
		leader := leaderFor(meta, r.Topic, r.Partition)
		if leader < 0 {
			return nil, protocol.ErrUnknownTopic
		}
		byBroker[leader] = append(byBroker[leader], r)
	}
	var all []protocol.ProduceResult
	for node, batch := range byBroker {
		body, err := protocol.EncodeProduceRequest(batch, settings)
		if err != nil {
			return nil, err
		}
		rb, err := cl.Request(ctx, node, protocol.APIProduce, protocol.VerProduce, body)
		if err != nil {
			return nil, err
		}
		results, err := protocol.DecodeProduceResponse(rb)
		if err != nil {
			return nil, err
		}
		all = append(all, results...)
	}
	return all, nil
}

func leaderFor(meta protocol.MetadataResponse, topic string, part int32) int32 {
	for _, t := range meta.Topics {
		if t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			if p.Partition == part {
				return p.Leader
			}
		}
	}
	return -1
}

// ToProtocol converts public records with resolved partitions.
func ToProtocol(records []RecordInput) []protocol.ProduceRecord {
	out := make([]protocol.ProduceRecord, len(records))
	now := time.Now()
	for i, r := range records {
		out[i] = protocol.ProduceRecord{
			Topic: r.Topic, Partition: r.Partition, Key: r.Key, Value: r.Value,
			Timestamp: now,
		}
	}
	return out
}

type RecordInput struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
}

var ErrUnknownTopic = protocol.ErrUnknownTopic

func topicKeyPublic(topic string, part int32) string {
	return fmt.Sprintf("%s:%d", topic, part)
}
