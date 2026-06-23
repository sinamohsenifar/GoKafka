#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
USER="${SCRAM_USER:-gokafka}"
PASS="${SCRAM_PASSWORD:-gokafka-secret}"

echo "Waiting for Kafka at $BOOTSTRAP..."
for i in $(seq 1 60); do
  if /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Creating SCRAM credentials for user $USER..."
/opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
  --alter --add-config "SCRAM-SHA-256=[password=${PASS}]" \
  --entity-type users --entity-name "$USER"

/opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
  --alter --add-config "SCRAM-SHA-512=[password=${PASS}]" \
  --entity-type users --entity-name "$USER"

echo "SCRAM users ready."
