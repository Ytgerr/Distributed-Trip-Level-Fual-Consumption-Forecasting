#!/bin/bash
set -euo pipefail

mkdir -p output/benchmarks

spark-submit --master yarn scripts/benchmarks/processing_benchmark.py

hdfs dfs -getmerge \
  /user/team15/project/benchmarks/results/processing_benchmark/*.csv \
  output/benchmarks/processing_benchmark.csv

cat output/benchmarks/processing_benchmark.csv
