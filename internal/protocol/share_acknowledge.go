package protocol

import (
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// ShareAcknowledgeRequest is KIP-932 ShareAcknowledge (API 79 v1).
type ShareAcknowledgeRequest struct {
	GroupID           string
	MemberID          string
	ShareSessionEpoch int32
	Partitions        []ShareFetchPartition
}

// EncodeShareAcknowledgeRequest encodes API 79 flex v1/v2. v2 (KIP-1222) adds a
// top-level IsRenewAck flag set when any batch carries a Renew (type 4)
// acknowledgement.
func EncodeShareAcknowledgeRequest(ver int16, req ShareAcknowledgeRequest) []byte {
	if ver <= 0 {
		ver = VerShareAcknowledge
	}
	byTopic := map[wire.UUID][]ShareFetchPartition{}
	order := make([]wire.UUID, 0)
	renew := false
	for _, p := range req.Partitions {
		if _, ok := byTopic[p.TopicID]; !ok {
			order = append(order, p.TopicID)
		}
		byTopic[p.TopicID] = append(byTopic[p.TopicID], p)
		for _, ab := range p.AckBatches {
			if ab.Type == ShareAckRenew {
				renew = true
			}
		}
	}

	buf := wire.NewBuffer(256)
	buf.WriteCompactString(req.GroupID)
	buf.WriteCompactString(req.MemberID)
	buf.WriteInt32(req.ShareSessionEpoch)
	if ver >= 2 {
		buf.WriteBool(renew) // is_renew_ack (KIP-1222) — precedes the topics array
	}
	buf.WriteCompactArrayLen(len(order))
	for _, tid := range order {
		buf.WriteUUID(tid)
		parts := byTopic[tid]
		buf.WriteCompactArrayLen(len(parts))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteCompactArrayLen(len(p.AckBatches))
			for _, ab := range p.AckBatches {
				buf.WriteInt64(ab.FirstOffset)
				buf.WriteInt64(ab.LastOffset)
				buf.WriteCompactArrayLen(1)
				buf.WriteInt8(int8(ab.Type))
				buf.WriteEmptyTagSection() // acknowledgement_batch tags
			}
			buf.WriteEmptyTagSection() // acknowledge_partition tags
		}
		buf.WriteEmptyTagSection() // acknowledge_topic tags
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeShareAcknowledgeResponse decodes API 79 flex response (v1/v2) and returns
// the first non-zero error code — top-level OR per-partition. The per-partition
// codes (e.g. INVALID_RECORD_STATE when an acquisition lock has expired) are the
// ones that actually report a failed acknowledgement; ignoring them would let a
// rejected Accept look successful and the record be redelivered. The layout
// mirrors ShareFetch (Responses[]→Partitions[]) — verified against a real broker
// response in the unit test. Per-topic structure: topic_id, partitions; each
// partition: index, error_code, error_message, current_leader{id,epoch,tag}, tag.
func DecodeShareAcknowledgeResponse(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, err
	}
	topErr, err := buf.ReadInt16()
	if err != nil {
		return 0, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
		return topErr, err
	}
	if topErr != 0 {
		return topErr, apiError("share acknowledge", topErr)
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for i := 1; i < int(nTopics); i++ {
		tid, err := buf.ReadUUID()
		if err != nil {
			return 0, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return 0, err
		}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return 0, err
			}
			code, err := buf.ReadInt16()
			if err != nil {
				return 0, err
			}
			if _, err := buf.ReadCompactNullableString(); err != nil { // partition error_message
				return 0, err
			}
			if _, err := buf.ReadInt32(); err != nil { // current_leader.leader_id
				return 0, err
			}
			if _, err := buf.ReadInt32(); err != nil { // current_leader.leader_epoch
				return 0, err
			}
			if err := buf.SkipTagSection(); err != nil { // current_leader tags
				return 0, err
			}
			if err := buf.SkipTagSection(); err != nil { // partition tags
				return 0, err
			}
			if code != 0 {
				return code, fmt.Errorf("protocol: share acknowledge partition %s-%d error %w", tid, part, apiError("share acknowledge", code))
			}
		}
		if err := buf.SkipTagSection(); err != nil { // topic tags
			return 0, err
		}
	}
	// node_endpoints[] (leader-discovery hints) then response tags.
	nEnd, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for i := 1; i < int(nEnd); i++ {
		if _, err := buf.ReadInt32(); err != nil { // node_id
			return 0, err
		}
		if _, err := buf.ReadCompactString(); err != nil { // host
			return 0, err
		}
		if _, err := buf.ReadInt32(); err != nil { // port
			return 0, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // rack
			return 0, err
		}
		if err := buf.SkipTagSection(); err != nil {
			return 0, err
		}
	}
	if err := buf.SkipTagSection(); err != nil { // response tags
		return 0, err
	}
	return 0, nil
}
