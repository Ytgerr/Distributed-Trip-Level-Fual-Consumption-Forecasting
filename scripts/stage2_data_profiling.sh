#!/bin/bash
set -euo pipefail

HDFS_OUT="/user/team15/project/profiling"
LOCAL_OUT="output/profiling"

mkdir -p "${LOCAL_OUT}"

hdfs dfs -rm -r -f "${HDFS_OUT}"

spark-submit --master yarn scripts/stage2_data_profiling.py \
  2>&1 | tee "${LOCAL_OUT}/stage2_data_profiling_run.log"

for name in table_overview column_types missingness numeric_summary numeric_quantiles categorical_summary; do
    rm -f "${LOCAL_OUT}/${name}.csv"

    hdfs dfs -getmerge \
      "${HDFS_OUT}/${name}/*.csv" \
      "${LOCAL_OUT}/${name}.csv"

    echo "Saved ${LOCAL_OUT}/${name}.csv"
done
