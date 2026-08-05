#!/bin/sh
# Generates a throwaway CA + server + client certificate/key set for the
# docker-compose.test.yml Kafka SASL_SSL listener (localhost-only, live
# integration testing). NOT for any real deployment — self-signed, 1-year
# validity, no passphrase, checked-out fresh into a gitignored directory.
#
# Usage: scripts/gen-kafka-test-certs.sh [output-dir]
# Default output dir: .kafka-certs (repo root, gitignored).
set -eu

OUT="${1:-.kafka-certs}"
mkdir -p "$OUT"
cd "$OUT"

echo "Generating Kafka test TLS materials in $(pwd) ..."

# --- CA ---
openssl genrsa -out ca.key 2048 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 \
  -subj "/CN=flogo-datapipeline-test-ca" -out ca.crt 2>/dev/null

# --- Server cert (CN=localhost, SAN=localhost+kafka, signed by the CA above) ---
openssl genrsa -out server.key 2048 2>/dev/null
openssl req -new -key server.key -subj "/CN=localhost" -out server.csr 2>/dev/null
cat > server.ext <<EOF
subjectAltName = DNS:localhost,DNS:kafka,IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 365 -sha256 -extfile server.ext 2>/dev/null
# Kafka's PEM keystore type reads a single file: private key followed by the
# certificate — concatenate them.
cat server.key server.crt > server.keystore.pem

# --- Client cert (mutual TLS: the sarama client authenticates with this) ---
openssl genrsa -out client.key 2048 2>/dev/null
openssl req -new -key client.key -subj "/CN=flogo-datapipeline-test-client" -out client.csr 2>/dev/null
echo "extendedKeyUsage = clientAuth" > client.ext
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 365 -sha256 -extfile client.ext 2>/dev/null

rm -f server.csr server.ext client.csr client.ext ca.srl
chmod 644 ca.crt server.crt server.key server.keystore.pem client.crt client.key

echo "Done. Files: ca.crt server.key server.crt server.keystore.pem client.key client.crt"
