package gokafka

import (
	"context"
)

// ClusterBroker describes a broker from DescribeCluster.
type ClusterBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

// ClusterDescription is cluster-wide metadata from the controller/broker.
type ClusterDescription struct {
	ClusterID    string
	ControllerID int32
	Brokers      []ClusterBroker
	ErrorCode    ErrorCode
}

// DescribeCluster returns cluster id, controller, and registered brokers (Metadata API).
func (a *Admin) DescribeCluster(ctx context.Context) (ClusterDescription, error) {
	if err := a.client.requireOpen(); err != nil {
		return ClusterDescription{}, err
	}
	return a.describeClusterFromMetadata(ctx)
}

func (a *Admin) describeClusterFromMetadata(ctx context.Context) (ClusterDescription, error) {
	if err := a.client.cluster.Refresh(ctx, nil); err != nil {
		return ClusterDescription{}, err
	}
	meta := a.client.cluster.Metadata()
	out := ClusterDescription{
		ClusterID: meta.ClusterID, ControllerID: meta.Controller,
	}
	for _, b := range meta.Brokers {
		out.Brokers = append(out.Brokers, ClusterBroker{
			NodeID: b.NodeID, Host: b.Host, Port: b.Port, Rack: b.Rack,
		})
	}
	return out, nil
}
