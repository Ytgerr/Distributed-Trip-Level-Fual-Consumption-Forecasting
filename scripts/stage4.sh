#!/bin/bash
set -euo pipefail

HIVE_DB="team15_projectdb"
HDFS_STAGE4="/user/team15/project/stage4"
LOCAL_STAGE4="output/stage4"

BEELINE_URL="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/default"
BEELINE_USER="team15"
BEELINE_PASS_FILE="secrets/.psql.pass"

TABLES="
stage4_psql_table_counts
stage4_psql_column_datatypes
stage4_psql_vehicles_sample
stage4_psql_trips_sample
stage4_data_cleaning_summary
stage4_feature_extraction_characteristics
stage4_feature_group_summary
stage4_hyperparameter_optimization
stage4_model_evaluation
stage4_model_comparison
stage4_best_model
stage4_prediction_samples
stage4_model1_predictions
stage4_model2_predictions
"

echo "Stage 4: prepare Hive tables for Superset"

echo "checking local inputs"
for f in output/evaluation.csv output/model1_predictions.csv output/model2_predictions.csv "$BEELINE_PASS_FILE"; do
  if [ ! -f "$f" ]; then
    echo "missing: $f"
    exit 1
  fi
  echo "ok: $f"
done

echo "checking python file"
python -m py_compile scripts/stage4_prepare_dashboard_data.py

if command -v pylint >/dev/null 2>&1; then
  echo "running pylint"
  pylint \
    --disable=missing-function-docstring,too-many-locals,too-many-statements \
    scripts/stage4_prepare_dashboard_data.py || true
else
  echo "pylint not found, skip"
fi

rm -rf "$LOCAL_STAGE4"
mkdir -p "$LOCAL_STAGE4"

echo "building local stage4 files"
python scripts/stage4_prepare_dashboard_data.py

echo "local files"
ls -lh "$LOCAL_STAGE4"

echo "uploading files to hdfs"
hdfs dfs -rm -r -f "$HDFS_STAGE4" || true
hdfs dfs -mkdir -p "$HDFS_STAGE4"

for table in $TABLES; do
  file="$LOCAL_STAGE4/${table}.tsv"

  if [ ! -f "$file" ]; then
    echo "missing generated file: $file"
    exit 1
  fi

  echo "upload: $table"
  hdfs dfs -mkdir -p "$HDFS_STAGE4/$table"
  hdfs dfs -put -f "$file" "$HDFS_STAGE4/$table/data.tsv"
done

password=$(head -n 1 "$BEELINE_PASS_FILE")

echo "creating hive tables"
beeline \
  -u "$BEELINE_URL" \
  -n "$BEELINE_USER" \
  -p "$password" \
  -f sql/stage4_superset_tables.hql

echo "checking hive tables"
for table in $TABLES; do
  beeline \
    -u "$BEELINE_URL" \
    -n "$BEELINE_USER" \
    -p "$password" \
    -e "USE $HIVE_DB; SELECT '$table' AS table_name, COUNT(*) AS rows_count FROM $table;"
done

echo "stage4 done"
