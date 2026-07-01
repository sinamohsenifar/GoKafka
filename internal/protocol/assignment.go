package protocol

import (
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// TopicPartitionAssignment is a consumer group partition assignment.
type TopicPartitionAssignment struct {
	Topic      string
	Partitions []int32
}

// ParseMemberAssignment decodes SyncGroup assignment bytes (range/roundrobin protocol).
func ParseMemberAssignment(raw []byte) ([]TopicPartitionAssignment, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	buf := wire.FromBytes(raw)
	version, err := buf.ReadInt16()
	if err != nil {
		return nil, err
	}
	switch version {
	case 0, 1:
		return parseLegacyAssignment(buf)
	default:
		return parseCompactAssignment(buf)
	}
}

func parseLegacyAssignment(buf *wire.Buffer) ([]TopicPartitionAssignment, error) {
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	out := make([]TopicPartitionAssignment, 0, safePrealloc(int(nTopics)))
	for i := int32(0); i < nTopics; i++ {
		topic, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		parts := make([]int32, 0, safePrealloc(int(nParts)))
		for j := int32(0); j < nParts; j++ {
			p, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
		}
		out = append(out, TopicPartitionAssignment{Topic: topic, Partitions: parts})
	}
	return out, nil
}

func parseCompactAssignment(buf *wire.Buffer) ([]TopicPartitionAssignment, error) {
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]TopicPartitionAssignment, 0, safePrealloc(int(nTopics)-1))
	for i := 1; i < int(nTopics); i++ {
		topic, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		parts := make([]int32, 0, safePrealloc(int(nParts)-1))
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			parts = append(parts, part)
		}
		out = append(out, TopicPartitionAssignment{Topic: topic, Partitions: parts})
	}
	return out, nil
}

// AssignorName returns the Kafka assignor protocol name for consumer config.
func AssignorName(strategy PartitionAssignor) string {
	switch strategy {
	case AssignorRoundRobin:
		return "roundrobin"
	case AssignorSticky:
		return "sticky"
	default:
		return "range"
	}
}

// PartitionAssignor is a consumer group partition assignor.
type PartitionAssignor int

const (
	AssignorRange PartitionAssignor = iota
	AssignorRoundRobin
	AssignorSticky
)

func (a PartitionAssignor) String() string {
	return AssignorName(a)
}

func ParseAssignor(s string) (PartitionAssignor, error) {
	switch s {
	case "range", "":
		return AssignorRange, nil
	case "roundrobin":
		return AssignorRoundRobin, nil
	case "sticky", "cooperative-sticky":
		return AssignorSticky, nil
	default:
		return AssignorRange, fmt.Errorf("protocol: unknown assignor %q", s)
	}
}
