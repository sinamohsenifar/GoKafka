package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

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

// DecodeShareAcknowledgeResponse decodes API 79 flex response v1 (errors only).
func DecodeShareAcknowledgeResponse(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, err
	}
	if code != 0 {
		return code, apiError("share acknowledge", code)
	}
	return 0, nil
}
