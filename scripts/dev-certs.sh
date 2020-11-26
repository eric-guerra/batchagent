#!/usr/bin/env bash
# Generates a throwaway CA plus server and client certs for local testing. Never use in production.
set -euo pipefail
out=${1:-certs}
mkdir -p "$out" && cd "$out"

openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=batchagent-dev-ca" -keyout ca.key -out ca.crt

issue() {
  local name=$1 cn=$2 ext=$3
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$cn/O=Retail Dev" -keyout "$name.key.rsa" -out "$name.csr"
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 -out "$name.crt" -extfile <(printf "%s" "$ext")
  # Netty wants PKCS#8 keys
  openssl pkcs8 -topk8 -nocrypt -in "$name.key.rsa" -out "$name.key"
  rm "$name.key.rsa" "$name.csr"
}

issue agent store-s042 "subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth"
issue scheduler dagrunner-scheduler "extendedKeyUsage=clientAuth"
issue monitoring monitoring "extendedKeyUsage=clientAuth"
echo "certs written to $(pwd)"
