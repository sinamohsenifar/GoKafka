//go:build integration && oauth

package gokafka_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka"
)

// Requires a broker with OAUTHBEARER on a dedicated listener (see docs/TESTING.md).
const testOAuthToken = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhZG1pbiJ9."

func TestIntegrationSecurityOAuthBearer(t *testing.T) {
	brokers := integrationBrokerEnv(t, "KAFKA_BROKERS_OAUTH", "")
	if brokers == "" {
		t.Skip("KAFKA_BROKERS_OAUTH not set")
	}
	integrationProduceConsume(t, []string{brokers}, gokafka.WithSecurity(gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySASLPlaintext,
		SASL: gokafka.SASLConfig{
			Mechanism: gokafka.SASLOAuth,
			Token:     testOAuthToken,
		},
	}))
}
