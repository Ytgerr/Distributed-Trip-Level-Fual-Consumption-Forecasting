# Scripts Directory

This directory contains executable Bash and Python scripts for the project pipeline.

Run scripts from the repository root, for example:

```bash
bash scripts/stage1.sh
```

## Stage scripts

| Script | Stage | Description |
|---|---|---|
| `stage1.sh` | Stage 1 | Downloads data if missing, preprocesses vehicle metadata, builds PostgreSQL tables, imports PostgreSQL tables into HDFS via Sqoop. |
| `stage2.sh` | Stage 2 | Runs Spark EDA and trip-level feature engineering on YARN, then exports small CSV outputs from HDFS to `output/eda/`. |
| `stage3.sh` | Stage 3 | Runs Spark ML on YARN, saves train/test splits, models, predictions, and evaluation outputs. |
| `stage4.sh` | Stage 4 | Placeholder for the presentation stage. Superset dashboard creation and publishing are manual. |

## Helper scripts

| Script | Purpose |
|---|---|
| `data_collection.sh` | Downloads and unpacks the raw dataset from Yandex Disk into `data/`. Requires `yadisk-direct`. |
| `preprocess_dataset.py` | Converts static vehicle metadata into `data/vehicles.csv`. |
| `build_projectdb.py` | Executes SQL scripts and loads `vehicles.csv` and telemetry CSV files into PostgreSQL. |
| `stage2_spark_eda.py` | Spark job for EDA, fuel-rate approximation, trip-level aggregation, and analytical outputs. |
| `build_stage2_charts.py` | Builds local figures from Stage 2 CSV outputs. |
| `stage3_spark_ml.py` | Spark ML pipeline for trip-level fuel consumption regression. |
| `preprocess.sh` | Optional pre-processing hook called by `main.sh`. |
| `postprocess.sh` | Optional post-processing hook called by `main.sh`. |
| `docker-compose.yml` | Auxiliary local service configuration if needed during development. |

## Execution requirements

The Spark scripts must be submitted to YARN through `spark-submit`, not executed as plain local Python scripts:

```bash
spark-submit --master yarn scripts/stage2_spark_eda.py
spark-submit --master yarn scripts/stage3_spark_ml.py
```

Required secret files:

```text
secrets/.psql.pass
```

