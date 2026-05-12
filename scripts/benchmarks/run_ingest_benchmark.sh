#!/bin/bash
set -euo pipefail

mkdir -p data/sample output/benchmarks

if [ ! -f data/VED_171101_week.csv ]; then
  echo "Missing data/VED_171101_week.csv. Run stage1.sh first."
  exit 1
fi

head -n 100001 data/VED_171101_week.csv > data/sample/trips_sample_100k.csv

spark-submit --master yarn scripts/benchmarks/ingest_benchmark.py \
  2>&1 | tee output/benchmarks/ingest_benchmark_run.log

echo "Ingest benchmark completed."
cat output/benchmarks/ingest_benchmark.csv
