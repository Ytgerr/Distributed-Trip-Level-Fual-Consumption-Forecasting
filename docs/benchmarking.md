# Benchmarking

## TL;DR

The repository contains benchmark scripts for ingest, storage formats, processing stages, and ML experiments. The current full pipeline runs ingest, storage, and processing benchmarks; the ML benchmark script exists but is not called by `main.sh`.

## Benchmark scripts

| Script | Purpose | Main output |
|---|---|---|
| `scripts/benchmarks/run_ingest_benchmark.sh` | Benchmarks local CSV scan, HDFS upload, Spark CSV read, Parquet write/read on a 100k-row sample. | `output/benchmarks/ingest_benchmark.csv` |
| `scripts/benchmarks/run_storage_benchmark.sh` | Compares storage formats and compression by write time, read time, row count, and HDFS size. | `output/benchmarks/storage_benchmark.csv` |
| `scripts/benchmarks/run_processing_benchmark.sh` | Measures runtime for selected processing stages. | `output/benchmarks/processing_benchmark.csv` |
| `scripts/benchmarks/run_ml_benchmark.sh` | Runs ML benchmark cases and merges HDFS results locally. | `output/benchmarks/ml_benchmark.csv` |
| `scripts/benchmarks/run_all_benchmarks.sh` | Runs storage, processing, and ML benchmarks. | Multiple files in `output/benchmarks/` |

## HDFS locations

```text
/user/team15/project/benchmarks/ingest_sample
/user/team15/project/benchmarks/storage
/user/team15/project/benchmarks/results/storage_benchmark
/user/team15/project/benchmarks/results/processing_benchmark
/user/team15/project/benchmarks/results/ml_benchmark
```

## Local outputs

```text
output/benchmarks/ingest_benchmark.csv
output/benchmarks/storage_benchmark_raw.csv
output/benchmarks/storage_sizes.csv
output/benchmarks/storage_benchmark.csv
output/benchmarks/processing_benchmark.csv
output/benchmarks/ml_benchmark.csv
```

## How to run

Run benchmark scripts from the repository root:

```bash
bash scripts/benchmarks/run_ingest_benchmark.sh
bash scripts/benchmarks/run_storage_benchmark.sh
bash scripts/benchmarks/run_processing_benchmark.sh
bash scripts/benchmarks/run_ml_benchmark.sh
```

Run the grouped benchmark script:

```bash
bash scripts/benchmarks/run_all_benchmarks.sh
```

## Notes

- `main.sh` runs ingest, storage, and processing benchmarks.
- `main.sh` does not currently run `run_ml_benchmark.sh`.
- `run_storage_benchmark.sh` uses the Spark Avro package `org.apache.spark:spark-avro_2.12:3.2.4`.
- Benchmark outputs are generated artifacts. If a benchmark output is missing locally, rerun the corresponding benchmark script.
