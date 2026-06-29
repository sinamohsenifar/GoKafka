package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// ApiVersion describes a supported API key/version pair.
type ApiVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// EncodeApiVersionsRequest builds an ApiVersions request body.
func EncodeApiVersionsRequest(softwareName, softwareVersion string) []byte {
	if VerApiVersions >= 3 {
		buf := wire.NewBuffer(32)
		if softwareName == "" {
			softwareName = "gokafka"
		}
		buf.WriteCompactString(softwareName)
		buf.WriteCompactString(softwareVersion)
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	return nil
}

// DecodeApiVersionsResponse parses broker API version ranges.
// FinalizedFeature is a cluster-finalized feature level from ApiVersions
// (e.g. transaction.version, metadata.version), used for feature negotiation
// such as KIP-890 TV2.
type FinalizedFeature struct {
	Name     string
	MinLevel int16
	MaxLevel int16
}

func DecodeApiVersionsResponse(version int16, body []byte) ([]ApiVersion, []FinalizedFeature, int16, error) {
	if version >= 3 {
		return decodeApiVersionsResponseFlex(body)
	}
	v, code, err := decodeApiVersionsResponseLegacy(version, body)
	return v, nil, code, err
}

func decodeApiVersionsResponseLegacy(version int16, body []byte) ([]ApiVersion, int16, error) {
	buf := wire.FromBytes(body)
	errCode, err := buf.ReadInt16()
	if err != nil {
		return nil, 0, err
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, errCode, err
	}
	out := make([]ApiVersion, 0, safePrealloc(int(n)))
	for i := 0; i < int(n); i++ {
		key, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		min, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		max, err := buf.ReadInt16()
		if err != nil {
			return nil, errCode, err
		}
		out = append(out, ApiVersion{APIKey: key, MinVersion: min, MaxVersion: max})
	}
	if version >= 2 {
		if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
			return nil, errCode, err
		}
	}
	return out, errCode, nil
}

func decodeApiVersionsResponseFlex(body []byte) ([]ApiVersion, []FinalizedFeature, int16, error) {
	buf := wire.FromBytes(body)
	errCode, err := buf.ReadInt16()
	if err != nil {
		return nil, nil, 0, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, nil, errCode, err
	}
	out := make([]ApiVersion, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		key, err := buf.ReadInt16()
		if err != nil {
			return nil, nil, errCode, err
		}
		min, err := buf.ReadInt16()
		if err != nil {
			return nil, nil, errCode, err
		}
		max, err := buf.ReadInt16()
		if err != nil {
			return nil, nil, errCode, err
		}
		out = append(out, ApiVersion{APIKey: key, MinVersion: min, MaxVersion: max})
		if err := buf.SkipTagSection(); err != nil {
			return nil, nil, errCode, err
		}
	}
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return nil, nil, errCode, err
	}
	feats, err := readApiVersionsFinalizedFeatures(buf)
	if err != nil {
		return nil, nil, errCode, err
	}
	return out, feats, errCode, nil
}

// readApiVersionsFinalizedFeatures parses the response-level tag section and
// extracts FinalizedFeatures (tag 2): each is {Name, MaxVersionLevel int16,
// MinVersionLevel int16, tags}. Unknown tags are skipped by length.
func readApiVersionsFinalizedFeatures(buf *wire.Buffer) ([]FinalizedFeature, error) {
	n, err := buf.ReadUvarint()
	if err != nil || n == 0 {
		return nil, err
	}
	var feats []FinalizedFeature
	for i := uint(0); i < n; i++ {
		tag, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		size, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		end := buf.I + int(size)
		if size > uint(len(buf.B)) || end > len(buf.B) {
			return nil, wire.ErrShortBuffer
		}
		if tag == 2 { // FinalizedFeatures
			m, err := buf.ReadUvarint()
			if err != nil {
				return nil, err
			}
			for j := 1; j < int(m); j++ {
				name, err := buf.ReadCompactString()
				if err != nil {
					return nil, err
				}
				maxL, err := buf.ReadInt16()
				if err != nil {
					return nil, err
				}
				minL, err := buf.ReadInt16()
				if err != nil {
					return nil, err
				}
				if err := buf.SkipTagSection(); err != nil {
					return nil, err
				}
				feats = append(feats, FinalizedFeature{Name: name, MinLevel: minL, MaxLevel: maxL})
			}
		}
		buf.I = end // advance past this tagged field regardless
	}
	return feats, nil
}

// NegotiateVersion picks the highest mutually supported version.
func NegotiateVersion(versions []ApiVersion, apiKey, clientMax int16) int16 {
	if clientMax <= 0 {
		return 0
	}
	for _, v := range versions {
		if v.APIKey != apiKey {
			continue
		}
		if clientMax < v.MinVersion {
			return 0
		}
		if clientMax > v.MaxVersion {
			return v.MaxVersion
		}
		return clientMax
	}
	return clientMax
}
