package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

const (
	APIDescribeCluster int16 = 60
	VerDescribeCluster int16 = 1
)

// ClusterBroker is a broker entry from DescribeCluster.
type ClusterBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

// ClusterDescription is metadata returned by DescribeCluster.
type ClusterDescription struct {
	ClusterID    string
	ControllerID int32
	Brokers      []ClusterBroker
	ErrorCode    int16
}

func EncodeDescribeClusterRequest() []byte {
	buf := wire.NewBuffer(8)
	buf.WriteBool(false) // include_cluster_authorized_operations
	buf.WriteInt8(0)     // endpoint_type: BROKER
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeDescribeClusterResponse(body []byte) (ClusterDescription, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return ClusterDescription{}, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return ClusterDescription{}, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
		return ClusterDescription{}, err
	}
	if _, err := buf.ReadInt8(); err != nil { // endpoint_type
		return ClusterDescription{}, err
	}
	clusterID, err := buf.ReadCompactString()
	if err != nil {
		return ClusterDescription{}, err
	}
	controller, err := buf.ReadInt32()
	if err != nil {
		return ClusterDescription{}, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return ClusterDescription{}, err
	}
	desc := ClusterDescription{ErrorCode: errCode, ClusterID: clusterID, ControllerID: controller}
	for i := 1; i < int(n); i++ {
		nodeID, err := buf.ReadInt32()
		if err != nil {
			return ClusterDescription{}, err
		}
		host, err := buf.ReadCompactString()
		if err != nil {
			return ClusterDescription{}, err
		}
		port, err := buf.ReadInt32()
		if err != nil {
			return ClusterDescription{}, err
		}
		rack, err := buf.ReadCompactNullableString()
		if err != nil {
			return ClusterDescription{}, err
		}
		desc.Brokers = append(desc.Brokers, ClusterBroker{NodeID: nodeID, Host: host, Port: port, Rack: rack})
		if err := buf.SkipTagSection(); err != nil {
			return ClusterDescription{}, err
		}
	}
	if _, err := buf.ReadInt32(); err != nil { // cluster_authorized_operations
		return ClusterDescription{}, err
	}
	if err := buf.SkipTagSection(); err != nil {
		return ClusterDescription{}, err
	}
	return desc, nil
}
