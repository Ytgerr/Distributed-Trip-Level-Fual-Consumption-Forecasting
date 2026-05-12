#!/bin/bash
set -euo pipefail

mkdir -p output/benchmarks

spark-submit --master yarn scripts/benchmarks/ml_benchmark.py

hdfs dfs -getmerge \
  /user/team15/project/benchmarks/results/ml_benchmark/*.csv \
  output/benchmarks/ml_benchmark.csv

cat output/benchmarks/ml_benchmark.csv
