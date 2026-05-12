#!/bin/bash
set -euo pipefail

STAGE4_HDFS="/user/team15/project/stage4"
LOCAL_OUTPUT="output"

echo "Cleaning old Stage 4 HDFS output..."
hdfs dfs -rm -r -f "${STAGE4_HDFS}"
hdfs dfs -mkdir -p "${STAGE4_HDFS}"

put_table() {
  local table_name="$1"
  local local_file="$2"

  if [ ! -f "${local_file}" ]; then
    echo "WARN: missing ${local_file}, skipping ${table_name}"
    return 0
  fi

  hdfs dfs -mkdir -p "${STAGE4_HDFS}/${table_name}"
  hdfs dfs -put -f "${local_file}" "${STAGE4_HDFS}/${table_name}/data.csv"

  echo "Uploaded ${table_name}: ${local_file}"
}

echo "Preparing model prediction sample from best model..."
BEST_MODEL_INDEX="$(tail -n +2 output/best_model.csv | cut -d',' -f1 | head -1)"
head -101 "output/model${BEST_MODEL_INDEX}_predictions.csv" > output/model_prediction_samples.csv

echo "Uploading Stage 4 dashboard tables..."

put_table "data_audit" "output/data_audit.csv"

put_table "table_overview" "output/profiling/table_overview.csv"
put_table "column_types" "output/profiling/column_types.csv"
put_table "missingness" "output/profiling/missingness.csv"
put_table "numeric_summary" "output/profiling/numeric_summary.csv"
put_table "numeric_quantiles" "output/profiling/numeric_quantiles.csv"
put_table "categorical_summary" "output/profiling/categorical_summary.csv"

put_table "feature_selection" "output/feature_selection.csv"
put_table "train_test_metadata" "output/train_test_metadata.csv"

put_table "model_evaluation" "output/evaluation.csv"
put_table "best_model" "output/best_model.csv"
put_table "model_training_log" "output/model_training_log.csv"
put_table "model_cv_results" "output/model_cv_results.csv"
put_table "specific_prediction" "output/specific_prediction.csv"
put_table "model_prediction_samples" "output/model_prediction_samples.csv"

put_table "ingest_benchmark" "output/benchmarks/ingest_benchmark.csv"
put_table "storage_benchmark" "output/benchmarks/storage_benchmark.csv"
put_table "processing_benchmark" "output/benchmarks/processing_benchmark.csv"

echo "Creating Hive external tables..."
beeline -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001" -n team15 -p "$(cat secrets/.psql.pass)" -f sql/stage4_superset_tables.hql

echo "Stage 4 completed."
