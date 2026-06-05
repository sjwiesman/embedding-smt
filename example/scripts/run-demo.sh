#!/bin/sh
set -eu

MZ_URL="postgresql://materialize@materialize:6875/materialize"
CONNECT_URL="http://connect:8083/connectors/articles-elasticsearch-sink/status"

until psql "$MZ_URL" -c "SELECT 1" >/dev/null 2>&1; do
  echo "waiting for Materialize..."
  sleep 2
done

until wget -q -O - "$CONNECT_URL" 2>/dev/null | grep -q '"state":"RUNNING"'; do
  echo "waiting for Kafka Connect connector to become RUNNING..."
  sleep 2
done

echo "Inserting article..."
psql "$MZ_URL" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO articles VALUES (1, 'Hello world', 'First body text', 10);"
sleep 8

echo "Updating views only..."
psql "$MZ_URL" -v ON_ERROR_STOP=1 -c \
  "UPDATE articles SET views = 42 WHERE id = 1;"
sleep 8

echo "Updating body text..."
psql "$MZ_URL" -v ON_ERROR_STOP=1 -c \
  "UPDATE articles SET body = 'First body text updated' WHERE id = 1;"
sleep 8

echo "Demo mutations complete."
