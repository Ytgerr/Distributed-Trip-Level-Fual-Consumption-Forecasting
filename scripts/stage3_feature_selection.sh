#!/bin/bash
set -euo pipefail

HDFS_OUT="/user/team15/project/ml_prepared"

mkdir -p output

hdfs dfs -rm -r -f "${HDFS_OUT}"

spark-submit --master yarn scripts/stage3_feature_selection.py \
  2>&1 | tee output/stage3_feature_selection_run.log

hdfs dfs -put -f output/feature_selection.csv "${HDFS_OUT}/feature_selection.csv"
hdfs dfs -put -f output/train_test_metadata.csv "${HDFS_OUT}/train_test_metadata.csv"

echo "Feature selection completed."
