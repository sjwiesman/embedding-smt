#!/bin/sh
set -eu

CONNECT_URL="http://connect:8083"

until curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; do
  echo "waiting for Kafka Connect..."
  sleep 2
done

curl -fsS -X POST \
  -H "Content-Type: application/json" \
  --data @/connect/connector-config.json \
  "$CONNECT_URL/connectors"

echo
echo "Kafka Connect connector registered."
