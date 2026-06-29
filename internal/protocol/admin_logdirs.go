package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// LogDirPartition is per-partition storage info from DescribeLogDirs.
type LogDirPartition struct {
	Topic     string
	Partition int32
	Size      int64
	OffsetLag int64
	IsFuture  bool
}

// LogDirResult is a single log directory's storage report.
type LogDirResult struct {
	ErrorCode   int16
	LogDir      string
	TotalBytes  int64 // -1 if unsupported (broker < v4)
	UsableBytes int64 // -1 if unsupported (broker < v4)
	IsCordoned  bool
	Partitions  []LogDirPartition
}

// EncodeDescribeLogDirsRequest encodes API 35. nil topics requests all log dirs.
func EncodeDescribeLogDirsRequest(version int16, topics map[string][]int32) []byte {
	if version >= 2 {
		buf := wire.NewBuffer(32)
		if topics == nil {
			buf.WriteCompactArrayLen(-1)
		} else {
			buf.WriteCompactArrayLen(len(topics))
			for topic, parts := range topics {
				buf.WriteCompactString(topic)
				buf.WriteCompactArrayLen(len(parts))
				for _, p := range parts {
					buf.WriteInt32(p)
				}
				buf.WriteEmptyTagSection()
			}
		}
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	buf := wire.NewBuffer(32)
	if topics == nil {
		buf.WriteInt32(-1)
	} else {
		buf.WriteInt32(int32(len(topics)))
		for topic, parts := range topics {
			buf.WriteString(topic)
			buf.WriteInt32(int32(len(parts)))
			for _, p := range parts {
				buf.WriteInt32(p)
			}
		}
	}
	return buf.Bytes()
}

// DecodeDescribeLogDirsResponse decodes API 35 response (flexible v2+).
func DecodeDescribeLogDirsResponse(version int16, body []byte) (int16, []LogDirResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, nil, err
	}
	var topErr int16
	if version >= 3 {
		var err error
		if topErr, err = buf.ReadInt16(); err != nil {
			return 0, nil, err
		}
	}
	nResults, err := buf.ReadUvarint()
	if err != nil {
		return topErr, nil, err
	}
	out := make([]LogDirResult, 0, safePrealloc(int(nResults)-1))
	for i := 1; i < int(nResults); i++ {
		r := LogDirResult{TotalBytes: -1, UsableBytes: -1}
		if r.ErrorCode, err = buf.ReadInt16(); err != nil {
			return topErr, nil, err
		}
		if r.LogDir, err = buf.ReadCompactString(); err != nil {
			return topErr, nil, err
		}
		nTopics, err := buf.ReadUvarint()
		if err != nil {
			return topErr, nil, err
		}
		for j := 1; j < int(nTopics); j++ {
			name, err := buf.ReadCompactString()
			if err != nil {
				return topErr, nil, err
			}
			nParts, err := buf.ReadUvarint()
			if err != nil {
				return topErr, nil, err
			}
			for k := 1; k < int(nParts); k++ {
				p := LogDirPartition{Topic: name}
				if p.Partition, err = buf.ReadInt32(); err != nil {
					return topErr, nil, err
				}
				if p.Size, err = buf.ReadInt64(); err != nil {
					return topErr, nil, err
				}
				if p.OffsetLag, err = buf.ReadInt64(); err != nil {
					return topErr, nil, err
				}
				if p.IsFuture, err = buf.ReadBool(); err != nil {
					return topErr, nil, err
				}
				if err := buf.SkipTagSection(); err != nil {
					return topErr, nil, err
				}
				r.Partitions = append(r.Partitions, p)
			}
			if err := buf.SkipTagSection(); err != nil {
				return topErr, nil, err
			}
		}
		if version >= 4 {
			if r.TotalBytes, err = buf.ReadInt64(); err != nil {
				return topErr, nil, err
			}
			if r.UsableBytes, err = buf.ReadInt64(); err != nil {
				return topErr, nil, err
			}
		}
		if version >= 5 {
			if r.IsCordoned, err = buf.ReadBool(); err != nil {
				return topErr, nil, err
			}
		}
		if err := buf.SkipTagSection(); err != nil {
			return topErr, nil, err
		}
		out = append(out, r)
	}
	if err := buf.SkipTagSection(); err != nil {
		return topErr, nil, err
	}
	return topErr, out, nil
}
