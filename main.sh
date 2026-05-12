#!/bin/bash
set -euo pipefail

echo "Stage 1 - Data preparation, audit, ingest benchmark, final ingest"
bash scripts/stage1.sh

echo "Stage 2.5 - Data profiling"
bash scripts/stage2_data_profiling.sh

echo "Stage 2 - Spark EDA and trip-level dataset"
bash scripts/stage2.sh

echo "Benchmark - Storage formats"
bash scripts/benchmarks/run_storage_benchmark.sh

echo "Benchmark - Processing stages"
bash scripts/benchmarks/run_processing_benchmark.sh

echo "Stage 3.5 - Feature selection and train-test split"
bash scripts/stage3_feature_selection.sh

echo "Stage 3.6 - Train three Spark ML models"
bash scripts/stage3_train_models.sh

echo "Stage 4 - Dashboard artifacts and Hive tables"
bash scripts/stage4.sh

echo "Pipeline completed successfully."
