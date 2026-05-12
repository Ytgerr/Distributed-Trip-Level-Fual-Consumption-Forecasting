#!/bin/bash
set -euo pipefail

bash scripts/benchmarks/run_storage_benchmark.sh
bash scripts/benchmarks/run_processing_benchmark.sh
bash scripts/benchmarks/run_ml_benchmark.sh

echo "All benchmark results:"
ls -lh output/benchmarks
