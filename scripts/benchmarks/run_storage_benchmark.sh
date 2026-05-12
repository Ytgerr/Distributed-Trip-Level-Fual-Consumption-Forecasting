#!/bin/bash
set -euo pipefail

mkdir -p output/benchmarks

spark-submit \
  --master yarn \
  --packages org.apache.spark:spark-avro_2.12:3.2.4 \
  scripts/benchmarks/storage_benchmark.py

hdfs dfs -getmerge \
  /user/team15/project/benchmarks/results/storage_benchmark/*.csv \
  output/benchmarks/storage_benchmark_raw.csv

echo "format,compression,hdfs_size_mb,hdfs_path" > output/benchmarks/storage_sizes.csv

for path in $(hdfs dfs -ls /user/team15/project/benchmarks/storage | awk '{print $8}'); do
    name=$(basename "$path")
    fmt=$(echo "$name" | cut -d_ -f1)
    codec=$(echo "$name" | cut -d_ -f2-)
    size_bytes=$(hdfs dfs -du -s "$path" | awk '{print $1}')
    size_mb=$(python - <<PY
print(round(${size_bytes} / 1024 / 1024, 3))
PY
)
    echo "${fmt},${codec},${size_mb},${path}" >> output/benchmarks/storage_sizes.csv
done

python - <<'PY'
import pandas as pd

raw = pd.read_csv("output/benchmarks/storage_benchmark_raw.csv")
sizes = pd.read_csv("output/benchmarks/storage_sizes.csv")

df = raw.merge(sizes, on=["format", "compression", "hdfs_path"], how="left")
df = df.sort_values(["read_time_sec", "hdfs_size_mb"])
df.to_csv("output/benchmarks/storage_benchmark.csv", index=False)

print(df)
PY
