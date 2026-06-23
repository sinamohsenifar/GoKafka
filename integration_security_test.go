//go:build integration

package gokafka_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationSecurityPlaintext(t *testing.T) {
	brokers := integrationBrokerEnv(t, "KAFKA_BROKERS_PLAINTEXT", "127.0.0.1:9092")
	integrationProduceConsume(t, []string{brokers})
}

func TestIntegrationSecuritySSL(t *testing.T) {
	brokers := integrationBrokerEnv(t, "KAFKA_BROKERS_SSL", "127.0.0.1:9093")
	integrationProduceConsume(t, []string{brokers}, gokafka.WithSecurity(gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySSL,
		TLS:      testTLSConfig(t),
	}))
}

func TestIntegrationSecuritySASLPlain(t *testing.T) {
	brokers := integrationBrokerEnv(t, "KAFKA_BROKERS_SASL_PLAINTEXT", "127.0.0.1:9094")
	integrationProduceConsume(t, []string{brokers}, gokafka.WithSecurity(gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySASLPlaintext,
		SASL: gokafka.SASLConfig{
			Mechanism: gokafka.SASLPlain,
			Username:  "gokafka",
			Password:  "gokafka-secret",
		},
	}))
}

func TestIntegrationSecuritySASLSCRAM256(t *testing.T) {
	brokers := integrationBrokerEnv(t, "KAFKA_BROKERS_SASL_PLAINTEXT", "127.0.0.1:9094")
	integrationProduceConsume(t, []string{brokers}, gokafka.WithSecurity(gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySASLPlaintext,
		SASL: gokafka.SASLConfig{
			Mechanism: gokafka.SASLSCRAM256,
			Username:  "gokafka",
			Password:  "gokafka-secret",
		},
	}))
}

func TestIntegrationSecuritySASLSCRAM512(t *testing.T) {
	brokers := integrationBrokerEnv(t, "KAFKA_BROKERS_SASL_PLAINTEXT", "127.0.0.1:9094")
	integrationProduceConsume(t, []string{brokers}, gokafka.WithSecurity(gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySASLPlaintext,
		SASL: gokafka.SASLConfig{
			Mechanism: gokafka.SASLSCRAM512,
			Username:  "gokafka",
			Password:  "gokafka-secret",
		},
	}))
}

func TestIntegrationSecuritySASLSSL(t *testing.T) {
	brokers := integrationBrokerEnv(t, "KAFKA_BROKERS_SASL_SSL", "127.0.0.1:9095")
	integrationProduceConsume(t, []string{brokers}, gokafka.WithSecurity(gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySASLSSL,
		TLS:      testTLSConfig(t),
		SASL: gokafka.SASLConfig{
			Mechanism: gokafka.SASLSCRAM512,
			Username:  "gokafka",
			Password:  "gokafka-secret",
		},
	}))
}

func TestIntegrationSecurityMTLS(t *testing.T) {
	brokers := integrationBrokerEnv(t, "KAFKA_BROKERS_SSL", "127.0.0.1:9093")
	// Broker runs with ssl.client.auth=none; mTLS exercises client certificate loading + TLS handshake.
	integrationProduceConsume(t, []string{brokers}, gokafka.WithSecurity(gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySSL,
		TLS:      testMTLSConfig(t),
	}))
}
