package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// ApiVersion describes a broker API key version range.
type ApiVersion = protocol.ApiVersion

// ApiVersions returns broker API version ranges (useful for compatibility checks).
func (c *Client) ApiVersions(ctx context.Context) ([]ApiVersion, error) {
	if err := c.requireOpen(); err != nil {
		return nil, err
	}
	body := protocol.EncodeApiVersionsRequest(c.cfg.ClientID, Version)
	rb, err := c.cluster.RequestViaSeed(ctx, protocol.APIApiVersions, protocol.VerApiVersions, body)
	if err != nil {
		return nil, err
	}
	versions, code, err := protocol.DecodeApiVersionsResponse(protocol.VerApiVersions, rb)
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, newKafkaError(code, "", 0, "api versions failed")
	}
	return versions, nil
}

// NegotiatedAPIVersion returns the version negotiated at connect for an API key.
func (c *Client) NegotiatedAPIVersion(apiKey int16) (int16, bool) {
	if err := c.requireOpen(); err != nil {
		return 0, false
	}
	v := c.cluster.NegotiatedVersion(apiKey, 0)
	return v, v > 0
}

// NegotiatedAPIVersions returns API versions negotiated at connect.
func (c *Client) NegotiatedAPIVersions() map[int16]int16 {
	if err := c.requireOpen(); err != nil {
		return nil
	}
	return c.cluster.NegotiatedVersions()
}

// SupportsAPI reports whether the cluster advertises an API key at least at minVersion.
func (c *Client) SupportsAPI(ctx context.Context, apiKey, minVersion int16) (bool, error) {
	versions, err := c.ApiVersions(ctx)
	if err != nil {
		return false, err
	}
	for _, v := range versions {
		if v.APIKey == apiKey && v.MaxVersion >= minVersion {
			return true, nil
		}
	}
	return false, nil
}
