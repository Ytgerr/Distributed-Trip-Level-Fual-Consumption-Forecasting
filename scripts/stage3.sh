#!/bin/bash
set -euo pipefail

echo "============================================================"
echo "Stage 3: Spark ML – trip-level fuel consumption forecasting"
echo "============================================================"

# ------------------------------------------------------------------
# Pre-flight: verify that Stage 2 output exists in HDFS
# ------------------------------------------------------------------
echo "Checking HDFS input (trip_features)..."
hdfs dfs -ls /user/team15/project/analytics/trip_features >/dev/null

# ------------------------------------------------------------------
# Clean previous Stage 3 HDFS outputs
# ------------------------------------------------------------------
echo "Cleaning previous Stage 3 HDFS outputs..."
hdfs dfs -rm -r -f project/data/train       || true
hdfs dfs -rm -r -f project/data/test        || true
hdfs dfs -rm -r -f project/models/model1    || true
hdfs dfs -rm -r -f project/models/model2    || true
hdfs dfs -rm -r -f project/output/model1_predictions || true
hdfs dfs -rm -r -f project/output/model2_predictions || true
hdfs dfs -rm -r -f project/output/evaluation         || true

# ------------------------------------------------------------------
# Clean previous local outputs
# ------------------------------------------------------------------
echo "Cleaning previous local outputs..."
rm -f  data/train.json data/test.json
rm -rf models/model1 models/model2
rm -f  output/model1_predictions.csv output/model2_predictions.csv output/evaluation.csv

mkdir -p data output models

# ------------------------------------------------------------------
# Run Spark ML job on YARN
# ------------------------------------------------------------------
echo "Submitting Spark ML job to YARN..."
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.executor.memory=4g \
  --conf spark.driver.memory=2g \
  scripts/stage3_spark_ml.py

# ------------------------------------------------------------------
# Verify local outputs were created by the Python script
# ------------------------------------------------------------------
echo ""
echo "Verifying local output files..."

for f in \
  data/train.json \
  data/test.json \
  output/model1_predictions.csv \
  output/model2_predictions.csv \
  output/evaluation.csv
do
  if [ -f "$f" ]; then
    echo "  OK  $f"
  else
    echo "  MISSING  $f"
  fi
done

for d in models/model1 models/model2; do
  if [ -d "$d" ]; then
    echo "  OK  $d/"
  else
    echo "  MISSING  $d/"
  fi
done

echo ""
echo "============================================================"
echo "Stage 3 completed successfully."
echo "Local artefacts:"
echo "  data/train.json, data/test.json"
echo "  models/model1/, models/model2/"
echo "  output/model1_predictions.csv"
echo "  output/model2_predictions.csv"
echo "  output/evaluation.csv"
echo "HDFS artefacts:"
echo "  project/data/train, project/data/test"
echo "  project/models/model1, project/models/model2"
echo "  project/output/model1_predictions"
echo "  project/output/model2_predictions"
echo "  project/output/evaluation"
echo "============================================================"
