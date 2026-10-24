#!/usr/bin/env bash
# Download one month of NYC TLC yellow-taxi trips plus the zone lookup table.
# Usage: bash scripts/download_tlc_data.sh 2024-01
set -euo pipefail

MONTH="${1:-2024-01}"
BASE="https://d37ci6vzurychx.cloudfront.net"
OUT="data/raw"

mkdir -p "$OUT"
curl -fL --retry 3 -o "$OUT/yellow_tripdata_${MONTH}.parquet" \
  "$BASE/trip-data/yellow_tripdata_${MONTH}.parquet"
curl -fL --retry 3 -o "$OUT/taxi_zone_lookup.csv" \
  "$BASE/misc/taxi_zone_lookup.csv"

echo "Downloaded $MONTH to $OUT/"
