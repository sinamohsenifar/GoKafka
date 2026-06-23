package protocol

import (
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

const (
	APIInitProducerID   int16 = 22
	APIAddOffsetsToTxn  int16 = 25
	APITxnOffsetCommit  int16 = 28
	APIAddPartitionsTxn int16 = 24
	APIEndTxn           int16 = 26

	VerInitProducerID   int16 = 0
	VerAddOffsetsToTxn  int16 = 3
	VerTxnOffsetCommit  int16 = 3
	VerAddPartitionsTxn int16 = 1
	VerEndTxn           int16 = 2
)

// ProducerID is allocated by InitProducerId for idempotent/transactional produce.
type ProducerID struct {
	ID    int64
	Epoch int16
}

func EncodeInitProducerID(txnID *string, txnTimeoutMs int32) []byte {
	buf := wire.NewBuffer(32)
	if VerInitProducerID >= 2 {
		buf.WriteCompactNullableString(txnID)
		buf.WriteInt32(txnTimeoutMs)
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	buf.WriteNullableString(txnID)
	buf.WriteInt32(txnTimeoutMs)
	return buf.Bytes()
}

func DecodeInitProducerID(body []byte) (ProducerID, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return ProducerID{}, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return ProducerID{}, err
	}
	if errCode != 0 {
		return ProducerID{}, apiError("init producer id", errCode)
	}
	id, err := buf.ReadInt64()
	if err != nil {
		return ProducerID{}, err
	}
	epoch, err := buf.ReadInt16()
	if err != nil {
		return ProducerID{}, err
	}
	if VerInitProducerID >= 2 {
		if err := buf.SkipTagSection(); err != nil {
			return ProducerID{}, err
		}
	}
	return ProducerID{ID: id, Epoch: epoch}, nil
}

func EncodeEndTxn(txnID string, producerID int64, epoch int16, commit bool) []byte {
	if VerEndTxn >= 3 {
		return encodeEndTxnFlex(txnID, producerID, epoch, commit)
	}
	return encodeEndTxnLegacy(txnID, producerID, epoch, commit)
}

func encodeEndTxnLegacy(txnID string, producerID int64, epoch int16, commit bool) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteString(txnID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	buf.WriteBool(commit)
	return buf.Bytes()
}

func encodeEndTxnFlex(txnID string, producerID int64, epoch int16, commit bool) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactString(txnID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	if commit {
		buf.WriteInt8(0)
	} else {
		buf.WriteInt8(1)
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeEndTxn(body []byte) (int16, error) {
	if VerEndTxn >= 3 {
		return decodeEndTxnFlex(body)
	}
	return decodeEndTxnLegacy(body)
}

func decodeEndTxnLegacy(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, err
	}
	return code, nil
}

func decodeEndTxnFlex(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, err
	}
	if err := buf.SkipTagSection(); err != nil {
		return 0, err
	}
	return code, nil
}

// TxnTopicPartitions groups partitions to register with a transaction.
type TxnTopicPartitions struct {
	Topic      string
	Partitions []int32
}

func EncodeAddPartitionsToTxn(txnID string, producerID int64, epoch int16, topics []TxnTopicPartitions) []byte {
	if VerAddPartitionsTxn >= 3 {
		return encodeAddPartitionsToTxnFlex(txnID, producerID, epoch, topics)
	}
	return encodeAddPartitionsToTxnLegacy(txnID, producerID, epoch, topics)
}

func encodeAddPartitionsToTxnLegacy(txnID string, producerID int64, epoch int16, topics []TxnTopicPartitions) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteString(txnID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	buf.WriteInt32(int32(len(topics)))
	for _, tp := range topics {
		buf.WriteString(tp.Topic)
		buf.WriteInt32(int32(len(tp.Partitions)))
		for _, p := range tp.Partitions {
			buf.WriteInt32(p)
		}
	}
	return buf.Bytes()
}

func encodeAddPartitionsToTxnFlex(txnID string, producerID int64, epoch int16, topics []TxnTopicPartitions) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactString(txnID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	buf.WriteCompactArrayLen(len(topics))
	for _, tp := range topics {
		buf.WriteCompactString(tp.Topic)
		buf.WriteCompactArrayLen(len(tp.Partitions))
		for _, p := range tp.Partitions {
			buf.WriteInt32(p)
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeAddPartitionsToTxn(body []byte) (int16, error) {
	if VerAddPartitionsTxn >= 3 {
		return decodeAddPartitionsToTxnFlex(body)
	}
	return decodeAddPartitionsToTxnLegacy(body)
}

func decodeAddPartitionsToTxnLegacy(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return 0, err
	}
	for i := int32(0); i < nTopics; i++ {
		if _, err := buf.ReadString(); err != nil {
			return 0, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return 0, err
		}
		for j := int32(0); j < nParts; j++ {
			if _, err := buf.ReadInt32(); err != nil {
				return 0, err
			}
			code, err := buf.ReadInt16()
			if err != nil {
				return 0, err
			}
			if code != 0 {
				return code, nil
			}
		}
	}
	return 0, nil
}

func decodeAddPartitionsToTxnFlex(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for i := 1; i < int(nTopics); i++ {
		if _, err := buf.ReadCompactString(); err != nil {
			return 0, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return 0, err
		}
		for j := 1; j < int(nParts); j++ {
			if _, err := buf.ReadInt32(); err != nil {
				return 0, err
			}
			code, err := buf.ReadInt16()
			if err != nil {
				return 0, err
			}
			if code != 0 {
				return code, nil
			}
			if err := buf.SkipTagSection(); err != nil {
				return 0, err
			}
		}
		if err := buf.SkipTagSection(); err != nil {
			return 0, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return 0, err
	}
	return 0, nil
}

// TxnGroupOffsets registers consumer group partitions with a transaction.
type TxnGroupOffsets struct {
	GroupID string
	Topics  []TxnTopicPartitions
}

func EncodeAddOffsetsToTxn(txnID string, producerID int64, epoch int16, groups []TxnGroupOffsets) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactString(txnID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	buf.WriteCompactArrayLen(len(groups))
	for _, g := range groups {
		buf.WriteCompactString(g.GroupID)
		buf.WriteCompactArrayLen(len(g.Topics))
		for _, tp := range g.Topics {
			buf.WriteCompactString(tp.Topic)
			buf.WriteCompactArrayLen(len(tp.Partitions))
			for _, p := range tp.Partitions {
				buf.WriteInt32(p)
			}
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeAddOffsetsToTxn(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	nGroups, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for i := 1; i < int(nGroups); i++ {
		if _, err := buf.ReadCompactString(); err != nil {
			return 0, err
		}
		nTopics, err := buf.ReadUvarint()
		if err != nil {
			return 0, err
		}
		for j := 1; j < int(nTopics); j++ {
			if _, err := buf.ReadCompactString(); err != nil {
				return 0, err
			}
			nParts, err := buf.ReadUvarint()
			if err != nil {
				return 0, err
			}
			for k := 1; k < int(nParts); k++ {
				if _, err := buf.ReadInt32(); err != nil {
					return 0, err
				}
				code, err := buf.ReadInt16()
				if err != nil {
					return 0, err
				}
				if code != 0 {
					return code, nil
				}
			}
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return 0, err
	}
	return 0, nil
}

// TxnCommittedOffset is a partition offset sent with TxnOffsetCommit.
type TxnCommittedOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

func EncodeTxnOffsetCommit(txnID, groupID string, producerID int64, epoch int16, offsets []TxnCommittedOffset) []byte {
	byTopic := map[string][]TxnCommittedOffset{}
	order := make([]string, 0)
	for _, o := range offsets {
		if _, ok := byTopic[o.Topic]; !ok {
			order = append(order, o.Topic)
		}
		byTopic[o.Topic] = append(byTopic[o.Topic], o)
	}
	buf := wire.NewBuffer(128)
	buf.WriteCompactString(txnID)
	buf.WriteCompactString(groupID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	buf.WriteCompactArrayLen(len(order))
	for _, topic := range order {
		buf.WriteCompactString(topic)
		parts := byTopic[topic]
		buf.WriteCompactArrayLen(len(parts))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteInt64(p.Offset)
			buf.WriteCompactNullableString(nil)
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeTxnOffsetCommit(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for i := 1; i < int(nTopics); i++ {
		if _, err := buf.ReadCompactString(); err != nil {
			return 0, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return 0, err
		}
		for j := 1; j < int(nParts); j++ {
			if _, err := buf.ReadInt32(); err != nil {
				return 0, err
			}
			code, err := buf.ReadInt16()
			if err != nil {
				return 0, err
			}
			if code != 0 {
				return code, nil
			}
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return 0, err
	}
	return 0, nil
}
