package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// CoordinatorResponse is the broker coordinator for a group or transactional id.
type CoordinatorResponse struct {
	ThrottleTime int32
	ErrorCode    int16
	NodeID       int32
	Host         string
	Port         int32
}

func EncodeFindCoordinatorRequest(key string, keyType int8) []byte {
	if VerFindCoordinator >= 3 {
		return encodeFindCoordinatorRequestFlex(key, keyType)
	}
	buf := wire.NewBuffer(32)
	buf.WriteString(key)
	buf.WriteInt8(keyType)
	return buf.Bytes()
}

func encodeFindCoordinatorRequestFlex(key string, keyType int8) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactString(key)
	buf.WriteInt8(keyType)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeFindCoordinatorResponse(body []byte) (CoordinatorResponse, error) {
	if VerFindCoordinator >= 3 {
		return decodeFindCoordinatorResponseFlex(body)
	}
	return decodeFindCoordinatorResponseLegacy(body)
}

func decodeFindCoordinatorResponseLegacy(body []byte) (CoordinatorResponse, error) {
	buf := wire.FromBytes(body)
	throttle, err := buf.ReadInt32()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	if VerFindCoordinator >= 1 {
		if _, err := readNullableString(buf); err != nil {
			return CoordinatorResponse{}, err
		}
	}
	nodeID, err := buf.ReadInt32()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	host, err := buf.ReadString()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	port, err := buf.ReadInt32()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	return CoordinatorResponse{ThrottleTime: throttle, ErrorCode: errCode, NodeID: nodeID, Host: host, Port: port}, nil
}

func decodeFindCoordinatorResponseFlex(body []byte) (CoordinatorResponse, error) {
	buf := wire.FromBytes(body)
	throttle, err := buf.ReadInt32()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil {
		return CoordinatorResponse{}, err
	}
	nodeID, err := buf.ReadInt32()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	host, err := buf.ReadCompactString()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	port, err := buf.ReadInt32()
	if err != nil {
		return CoordinatorResponse{}, err
	}
	if err := buf.SkipTagSection(); err != nil {
		return CoordinatorResponse{}, err
	}
	return CoordinatorResponse{ThrottleTime: throttle, ErrorCode: errCode, NodeID: nodeID, Host: host, Port: port}, nil
}
