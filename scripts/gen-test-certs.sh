#!/usr/bin/env bash
# Generate self-signed TLS material for local integration tests only.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SECRETS="$ROOT/docker/secrets"
DAYS=825
STORE_PASS="${KAFKA_SSL_STORE_PASS:-testpass}"

mkdir -p "$SECRETS"

gen_ca() {
  openssl req -new -x509 -days "$DAYS" -nodes \
    -subj "/CN=GoKafka Test CA" \
    -keyout "$SECRETS/ca.key" \
    -out "$SECRETS/ca.crt"
}

gen_cert() {
  local name="$1"
  local cn="$2"
  local extfile="$SECRETS/${name}.ext"

  cat >"$extfile" <<EOF
subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
EOF

  openssl req -new -nodes \
    -subj "/CN=${cn}" \
    -keyout "$SECRETS/${name}.key" \
    -out "$SECRETS/${name}.csr"

  openssl x509 -req -days "$DAYS" \
    -in "$SECRETS/${name}.csr" \
    -CA "$SECRETS/ca.crt" -CAkey "$SECRETS/ca.key" -CAcreateserial \
    -out "$SECRETS/${name}.crt" \
    -extfile "$extfile"
}

gen_jks() {
  rm -f "$SECRETS/kafka.keystore.jks" "$SECRETS/kafka.truststore.jks" "$SECRETS/broker.p12"

  # Broker keystore (PKCS12 intermediate -> JKS for Kafka docker entrypoint).
  openssl pkcs12 -export \
    -in "$SECRETS/broker.crt" \
    -inkey "$SECRETS/broker.key" \
    -out "$SECRETS/broker.p12" \
    -name kafka-broker \
    -password "pass:${STORE_PASS}" \
    -CAfile "$SECRETS/ca.crt"

  keytool -importkeystore -noprompt \
    -srckeystore "$SECRETS/broker.p12" -srcstoretype PKCS12 -srcstorepass "$STORE_PASS" \
    -destkeystore "$SECRETS/kafka.keystore.jks" -deststoretype JKS -deststorepass "$STORE_PASS"

  keytool -import -noprompt \
    -alias ca -file "$SECRETS/ca.crt" \
    -keystore "$SECRETS/kafka.truststore.jks" -storepass "$STORE_PASS"

  printf '%s' "$STORE_PASS" >"$SECRETS/kafka_keystore_creds"
  printf '%s' "$STORE_PASS" >"$SECRETS/kafka_ssl_key_creds"
  printf '%s' "$STORE_PASS" >"$SECRETS/kafka_truststore_creds"
}

gen_ca
gen_cert broker "kafka-broker"
gen_cert client "gokafka-client"
cp "$SECRETS/broker.crt" "$SECRETS/broker-chain.pem"
gen_jks

chmod 0644 "$SECRETS"/*.crt "$SECRETS"/*.pem "$SECRETS"/*.jks 2>/dev/null || true
chmod 0600 "$SECRETS"/*.key "$SECRETS"/*_creds 2>/dev/null || true

echo "Generated test TLS material in $SECRETS"
