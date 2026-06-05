#!/bin/sh
set -eu

MZ_URL="postgresql://materialize@materialize:6875/materialize"

until psql "$MZ_URL" -c "SELECT 1" >/dev/null 2>&1; do
  echo "waiting for Materialize..."
  sleep 2
done

psql "$MZ_URL" -v ON_ERROR_STOP=1 -f /materialize/setup.sql

echo "Materialize bootstrap complete."
