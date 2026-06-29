package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// TransactionListing is a row of ListTransactions.
type TransactionListing struct {
	TransactionalID string
	ProducerID      int64
	State           string
}

func EncodeListTransactionsRequest(version int16, stateFilters []string, producerIDFilters []int64) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactArrayLen(len(stateFilters))
	for _, s := range stateFilters {
		buf.WriteCompactString(s)
	}
	buf.WriteCompactArrayLen(len(producerIDFilters))
	for _, p := range producerIDFilters {
		buf.WriteInt64(p)
	}
	if version >= 1 {
		buf.WriteInt64(-1) // duration_filter: -1 = disabled
	}
	if version >= 2 {
		buf.WriteCompactNullableString(nil) // transactional_id_pattern
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeListTransactionsResponse(version int16, body []byte) (int16, []TransactionListing, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle
		return 0, nil, err
	}
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, nil, err
	}
	// unknown_state_filters
	nUnknown, err := buf.ReadUvarint()
	if err != nil {
		return code, nil, err
	}
	for i := 1; i < int(nUnknown); i++ {
		if _, err := buf.ReadCompactString(); err != nil {
			return code, nil, err
		}
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return code, nil, err
	}
	out := make([]TransactionListing, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		var tl TransactionListing
		if tl.TransactionalID, err = buf.ReadCompactString(); err != nil {
			return code, nil, err
		}
		if tl.ProducerID, err = buf.ReadInt64(); err != nil {
			return code, nil, err
		}
		if tl.State, err = buf.ReadCompactString(); err != nil {
			return code, nil, err
		}
		if err := buf.SkipTagSection(); err != nil {
			return code, nil, err
		}
		out = append(out, tl)
	}
	if err := buf.SkipTagSection(); err != nil {
		return code, nil, err
	}
	return code, out, nil
}

// TransactionDescription is the detailed state of one transactional id.
type TransactionDescription struct {
	ErrorCode       int16
	TransactionalID string
	State           string
	TimeoutMs       int32
	StartTimeMs     int64
	ProducerID      int64
	ProducerEpoch   int16
	Topics          map[string][]int32
}

func EncodeDescribeTransactionsRequest(transactionalIDs []string) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactArrayLen(len(transactionalIDs))
	for _, id := range transactionalIDs {
		buf.WriteCompactString(id)
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeDescribeTransactionsResponse(body []byte) ([]TransactionDescription, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]TransactionDescription, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		var d TransactionDescription
		if d.ErrorCode, err = buf.ReadInt16(); err != nil {
			return nil, err
		}
		if d.TransactionalID, err = buf.ReadCompactString(); err != nil {
			return nil, err
		}
		if d.State, err = buf.ReadCompactString(); err != nil {
			return nil, err
		}
		if d.TimeoutMs, err = buf.ReadInt32(); err != nil {
			return nil, err
		}
		if d.StartTimeMs, err = buf.ReadInt64(); err != nil {
			return nil, err
		}
		if d.ProducerID, err = buf.ReadInt64(); err != nil {
			return nil, err
		}
		if d.ProducerEpoch, err = buf.ReadInt16(); err != nil {
			return nil, err
		}
		nTopics, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		if nTopics > 1 {
			d.Topics = make(map[string][]int32, int(nTopics)-1)
		}
		for j := 1; j < int(nTopics); j++ {
			topic, err := buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
			nParts, err := buf.ReadUvarint()
			if err != nil {
				return nil, err
			}
			parts := make([]int32, 0, safePrealloc(int(nParts)-1))
			for k := 1; k < int(nParts); k++ {
				p, err := buf.ReadInt32()
				if err != nil {
					return nil, err
				}
				parts = append(parts, p)
			}
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
			d.Topics[topic] = parts
		}
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}
