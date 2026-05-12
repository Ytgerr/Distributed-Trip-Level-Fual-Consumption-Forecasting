#!/bin/bash
set -euo pipefail

echo "Stage 1.1 - Prepare raw data"
bash scripts/stage1_prepare_data.sh

echo "Stage 1.2 - Raw data audit"
rm -rf data/sample
python3 scripts/stage1_data_audit.py

echo "Stage 1.3 - Ingest benchmark on sample"
bash scripts/benchmarks/run_ingest_benchmark.sh

echo "Stage 1.4 - Final PostgreSQL + Sqoop ingest"
bash scripts/stage1_final_ingest.sh

echo "Stage 1 completed."
