# Scripts Directory

## TL;DR

This directory contains the executable Bash and Python scripts for the project pipeline. Run scripts from the repository root.

Example:

```bash
bash scripts/stage1.sh
```

## Full pipeline

The current full pipeline is defined in:

```text
main.sh
```

Run it from the repository root:

```bash
bash main.sh
```

## Stage entrypoints

| Script | Stage | Description |
|---|---|---|
| `stage1.sh` | Stage 1 | Runs data preparation, raw-data audit, ingest benchmark, and final PostgreSQL/Sqoop ingest. |
| `stage2_data_profiling.sh` | Stage 2.5 | Profiles HDFS warehouse tables and exports profiling CSVs. |
| `stage2.sh` | Stage 2 | Runs Spark EDA and trip-level feature engineering on YARN. |
| `stage3_feature_selection.sh` | Stage 3.5 | Selects features and creates train/test Parquet datasets. |
| `stage3_train_models.sh` | Stage 3.6 | Trains Linear Regression, Random Forest, and GBT models. |
| `stage4.sh` | Stage 4 | Uploads dashboard-ready CSVs and creates Hive external tables. |

Legacy entrypoint:

| Script | Description |
|---|---|
| `stage3.sh` | Older two-model Stage 3 pipeline using `stage3_spark_ml.py`. Kept for compatibility only. |

## Stage 1 helper scripts

| Script | Purpose |
|---|---|
| `stage1_prepare_data.sh` | Downloads raw data and prepares `vehicles.csv` if missing. |
| `data_collection.sh` | Downloads and unpacks the raw dataset. Requires `yadisk-direct`. |
| `preprocess_dataset.py` | Converts static vehicle metadata into `data/vehicles.csv`. |
| `stage1_data_audit.py` | Audits downloaded files and writes `output/data_audit.csv`. |
| `stage1_final_ingest.sh` | Loads PostgreSQL and imports `vehicles`/`trips` into HDFS with Sqoop. |
| `build_projectdb.py` | Executes SQL scripts and loads CSV files into PostgreSQL. |

## Stage 2 helper scripts

| Script | Purpose |
|---|---|
| `stage2_data_profiling.py` | Creates table overview, data types, missingness, numeric summaries, quantiles, and categorical summaries. |
| `stage2_spark_eda.py` | Spark job for EDA, fuel-rate approximation, trip-level aggregation, and analytical outputs. |
| `build_stage2_charts.py` | Builds local figures from Stage 2 CSV outputs. |

## Stage 3 helper scripts

| Script | Purpose |
|---|---|
| `stage3_feature_selection.py` | Runs `SelectKBest(mutual_info_regression)` for numeric features and writes train/test Parquet datasets. |
| `stage3_train_models.py` | Trains three Spark ML regression models with 3-fold cross-validation. |
| `stage3_spark_ml.py` | Legacy two-model Spark ML script. |

## Benchmark scripts

| Script | Purpose |
|---|---|
| `benchmarks/run_ingest_benchmark.sh` | Ingest benchmark on a 100k-row sample. |
| `benchmarks/run_storage_benchmark.sh` | Storage format/compression benchmark. |
| `benchmarks/run_processing_benchmark.sh` | Processing runtime benchmark. |
| `benchmarks/run_ml_benchmark.sh` | Optional ML benchmark. |
| `benchmarks/run_all_benchmarks.sh` | Runs storage, processing, and ML benchmarks. |

## Execution requirements

Spark scripts must be submitted to YARN through `spark-submit`, not run as plain local Python scripts:

```bash
spark-submit --master yarn scripts/stage2_spark_eda.py
spark-submit --master yarn scripts/stage3_feature_selection.py
spark-submit --master yarn scripts/stage3_train_models.py
```

Required secret file:

```text
secrets/.psql.pass
```

## Output convention

| Output type | Location |
|---|---|
| HDFS warehouse | `/user/team15/project/warehouse` |
| HDFS analytics | `/user/team15/project/analytics` |
| HDFS prepared ML data | `/user/team15/project/ml_prepared` |
| HDFS models | `/user/team15/project/models` |
| Local generated files | `output/` |
| Local model copies | `models/` |
