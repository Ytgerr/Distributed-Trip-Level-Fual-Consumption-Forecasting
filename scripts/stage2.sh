#!/bin/bash
set -euo pipefail

echo "Stage 2: Spark EDA and feature engineering"

echo "Checking HDFS inputs..."
hdfs dfs -ls /user/team15/project/warehouse/trips >/dev/null
hdfs dfs -ls /user/team15/project/warehouse/vehicles >/dev/null

echo "Cleaning old analytics outputs..."
hdfs dfs -rm -r -f /user/team15/project/analytics || true
hdfs dfs -rm -r -f /user/team15/project/analytics_csv || true

echo "Running Spark job on YARN..."
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=32 \
  scripts/stage2_spark_eda.py

echo "Downloading small CSV outputs for report..."
rm -rf output/eda
mkdir -p output/eda

for name in \
  vehicles_raw \
  eda_data_characteristics \
  eda_missingness \
  insight_01_vehicle_type_distribution \
  insight_02_fuel_by_vehicle_type \
  insight_03_fuel_by_speed_bin \
  insight_04_stop_go_vs_fuel \
  insight_05_temperature_hvac_fuel \
  insight_06_engine_displacement_fuel \
  insight_07_geo_activity \
  trip_features_sample
do
  echo "Exporting ${name}.csv"
  hdfs dfs -getmerge "/user/team15/project/analytics_csv/${name}" "output/eda/${name}.csv"
done

echo "Stage 2 completed successfully."
echo "Local CSV files are in output/eda"
echo "HDFS parquet files are in /user/team15/project/analytics"
