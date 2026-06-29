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
	VerEndTxn           int16 = 5
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

func EncodeEndTxn(ver int16, txnID string, producerID int64, epoch int16, commit bool) []byte {
	if ver <= 0 {
		ver = VerEndTxn
	}
	if ver >= 3 {
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
	// committed: true = commit, false = abort (encoded as a 1/0 boolean byte).
	buf.WriteBool(commit)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// EndTxnResult is the decoded EndTxn response. ProducerID/ProducerEpoch are the
// server-bumped values returned by EndTxn v5+ (KIP-890 TV2); for older versions
// they are -1 (not present in the response).
type EndTxnResult struct {
	Code          int16
	ProducerID    int64
	ProducerEpoch int16
}

// DecodeEndTxn parses the EndTxn response at the given version. v5+ additionally
// carries the bumped producer id and epoch (after error_code, before the tag
// section); earlier versions do not, so those fields come back as -1.
func DecodeEndTxn(version int16, body []byte) (EndTxnResult, error) {
	if version <= 0 {
		version = VerEndTxn
	}
	res := EndTxnResult{ProducerID: -1, ProducerEpoch: -1}
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return res, err
	}
	code, err := buf.ReadInt16()
	if err != nil {
		return res, err
	}
	res.Code = code
	if version >= 5 {
		pid, err := buf.ReadInt64()
		if err != nil {
			return res, err
		}
		epoch, err := buf.ReadInt16()
		if err != nil {
			return res, err
		}
		res.ProducerID = pid
		res.ProducerEpoch = epoch
	}
	if version >= 3 {
		if err := buf.SkipTagSection(); err != nil {
			return res, err
		}
	}
	return res, nil
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

// TxnGroupOffsets registers a consumer group with a transaction (legacy helper name).
type TxnGroupOffsets struct {
	GroupID string
	Topics  []TxnTopicPartitions
}

// EncodeAddOffsetsToTxn registers a consumer group id with the open transaction.
func EncodeAddOffsetsToTxn(ver int16, txnID string, producerID int64, epoch int16, groupID string) []byte {
	if ver <= 0 {
		ver = VerAddOffsetsToTxn
	}
	if ver >= 3 {
		buf := wire.NewBuffer(64)
		buf.WriteCompactString(txnID)
		buf.WriteInt64(producerID)
		buf.WriteInt16(epoch)
		buf.WriteCompactString(groupID)
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	buf := wire.NewBuffer(64)
	buf.WriteString(txnID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	buf.WriteString(groupID)
	return buf.Bytes()
}

func DecodeAddOffsetsToTxn(ver int16, body []byte) (int16, error) {
	if ver <= 0 {
		ver = VerAddOffsetsToTxn
	}
	if ver >= 3 {
		return decodeAddOffsetsToTxnFlex(body)
	}
	code, err := decodeAddOffsetsToTxnLegacy(body)
	if err == nil || code != 0 {
		return code, err
	}
	return decodeAddOffsetsToTxnFlex(body)
}

func decodeAddOffsetsToTxnLegacy(body []byte) (int16, error) {
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

func decodeAddOffsetsToTxnFlex(body []byte) (int16, error) {
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

// TxnOffsetCommitMeta is consumer group metadata for TxnOffsetCommit v3+.
type TxnOffsetCommitMeta struct {
	Generation      int32
	MemberID        string
	GroupInstanceID string
}

// TxnCommittedOffset is a partition offset sent with TxnOffsetCommit.
type TxnCommittedOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

func EncodeTxnOffsetCommit(ver int16, txnID, groupID string, producerID int64, epoch int16, meta TxnOffsetCommitMeta, offsets []TxnCommittedOffset) []byte {
	if ver <= 0 {
		ver = VerTxnOffsetCommit
	}
	if ver >= 3 {
		return encodeTxnOffsetCommitFlex(ver, txnID, groupID, producerID, epoch, meta, offsets)
	}
	return encodeTxnOffsetCommitLegacy(ver, txnID, groupID, producerID, epoch, offsets)
}

func encodeTxnOffsetCommitLegacy(ver int16, txnID, groupID string, producerID int64, epoch int16, offsets []TxnCommittedOffset) []byte {
	byTopic := txnOffsetsByTopic(offsets)
	order := txnOffsetTopicOrder(byTopic)
	buf := wire.NewBuffer(128)
	buf.WriteString(txnID)
	buf.WriteString(groupID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	buf.WriteInt32(int32(len(order)))
	for _, topic := range order {
		buf.WriteString(topic)
		parts := byTopic[topic]
		buf.WriteInt32(int32(len(parts)))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteInt64(p.Offset)
			if ver >= 2 {
				buf.WriteInt32(-1)
			}
			buf.WriteNullableString(nil)
		}
	}
	return buf.Bytes()
}

func encodeTxnOffsetCommitFlex(ver int16, txnID, groupID string, producerID int64, epoch int16, meta TxnOffsetCommitMeta, offsets []TxnCommittedOffset) []byte {
	byTopic := txnOffsetsByTopic(offsets)
	order := txnOffsetTopicOrder(byTopic)
	buf := wire.NewBuffer(128)
	buf.WriteCompactString(txnID)
	buf.WriteCompactString(groupID)
	buf.WriteInt64(producerID)
	buf.WriteInt16(epoch)
	if ver >= 3 {
		buf.WriteInt32(meta.Generation)
		buf.WriteCompactString(meta.MemberID)
		if meta.GroupInstanceID == "" {
			buf.WriteUvarint(0)
		} else {
			buf.WriteCompactString(meta.GroupInstanceID)
		}
	}
	buf.WriteCompactArrayLen(len(order))
	for _, topic := range order {
		buf.WriteCompactString(topic)
		parts := byTopic[topic]
		buf.WriteCompactArrayLen(len(parts))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteInt64(p.Offset)
			if ver >= 2 {
				buf.WriteInt32(-1)
			}
			buf.WriteCompactNullableString(nil)
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func txnOffsetsByTopic(offsets []TxnCommittedOffset) map[string][]TxnCommittedOffset {
	byTopic := map[string][]TxnCommittedOffset{}
	for _, o := range offsets {
		byTopic[o.Topic] = append(byTopic[o.Topic], o)
	}
	return byTopic
}

func txnOffsetTopicOrder(byTopic map[string][]TxnCommittedOffset) []string {
	order := make([]string, 0, len(byTopic))
	for topic := range byTopic {
		order = append(order, topic)
	}
	return order
}

func DecodeTxnOffsetCommit(ver int16, body []byte) (int16, error) {
	if ver <= 0 {
		ver = VerTxnOffsetCommit
	}
	if ver >= 3 {
		return decodeTxnOffsetCommitFlex(body)
	}
	code, err := decodeTxnOffsetCommitLegacy(body)
	if err == nil || code != 0 {
		return code, err
	}
	return decodeTxnOffsetCommitFlex(body)
}

func decodeTxnOffsetCommitLegacy(body []byte) (int16, error) {
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

func decodeTxnOffsetCommitFlex(body []byte) (int16, error) {
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
